// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table.Partitioning;
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Reads Change Data Feed (CDC) to return row-level changes between versions.
/// For each version, if CDC files exist they are used; otherwise changes are
/// inferred from add/remove actions.
/// </summary>
internal static class CdfReader
{
    // The schema view the feed is resolved against — the table's LOGICAL schema (with column-mapping metadata
    // and partition columns). Both _change_data files and inferred-from-data-file rows are stored in the
    // PHYSICAL layout (physical names + field ids, partition columns absent — the data-file convention), so the
    // reader maps them back to logical names and re-materializes the partition columns from the action's
    // partitionValues. A single schema is used for the whole range (column mapping keeps field ids stable across
    // a rename, so this is correct unless the schema's SHAPE changes mid-range — the same simplification the
    // rest of the read path makes).
    private sealed record CdfSchemaContext(
        Apache.Arrow.Schema LogicalArrowSchema,
        DeltaStructType DeltaSchema,
        ColumnMappingMode MappingMode,
        IReadOnlyList<string> PartitionColumns,
        IReadOnlyDictionary<string, string> LogicalToPhysical,
        string? MaterializedRowIdName,
        string? MaterializedRowVersionName,
        bool EmitRowTracking,
        string EmittedRowIdName,
        string EmittedRowVersionName)
    {
        /// <summary>True when the table DECLARES its hidden materialized column names — the gate for
        /// touching them at all, so a table without row tracking is read exactly as before.</summary>
        public bool HasMaterializedColumns =>
            MaterializedRowIdName is not null || MaterializedRowVersionName is not null;
    }

    /// <summary>
    /// Reads changes from <paramref name="startVersion"/> to <paramref name="endVersion"/> (inclusive).
    /// Each batch includes <c>_change_type</c>, <c>_commit_version</c>, and <c>_commit_timestamp</c> columns.
    /// <para>The table's hidden materialized row-tracking columns (named by
    /// <paramref name="materializedRowIdName"/> / <paramref name="materializedRowVersionName"/>) are stripped
    /// from every batch — a change file and a data file both store them inline, and they are not part of the
    /// feed a caller asked for. With <paramref name="emitRowTracking"/> they come back as the spec's two
    /// GENERATED columns instead, resolved per row; see <see cref="ResolveRowTracking"/>.</para>
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        ITableFileSystem fs,
        TransactionLog log,
        long startVersion,
        long endVersion,
        ParquetReadOptions? readOptions,
        Apache.Arrow.Schema logicalArrowSchema,
        DeltaStructType deltaSchema,
        ColumnMappingMode mappingMode,
        IReadOnlyList<string> partitionColumns,
        string? materializedRowIdName = null,
        string? materializedRowVersionName = null,
        bool emitRowTracking = false,
        string? emittedRowIdName = null,
        string? emittedRowVersionName = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var ctx = new CdfSchemaContext(
            logicalArrowSchema, deltaSchema, mappingMode, partitionColumns,
            mappingMode == ColumnMappingMode.None
                ? new Dictionary<string, string>()
                : ColumnMapping.BuildLogicalToPhysicalMap(deltaSchema, mappingMode),
            materializedRowIdName, materializedRowVersionName, emitRowTracking,
            emittedRowIdName ?? DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName,
            emittedRowVersionName
                ?? DeltaLake.RowTracking.RowTrackingConfig.RowCommitVersionColumnName);

        // Stateless over the file system — one instance serves every action in the range.
        var dvReader = new DeletionVectorReader(fs);

        for (long version = startVersion; version <= endVersion; version++)
        {
            IReadOnlyList<DeltaAction> actions;
            try
            {
                actions = await log.ReadCommitAsync(version, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                continue; // Skip missing versions
            }

            // Get commit timestamp
            long? commitTimestamp = InCommitTimestamp.GetTimestampFromActions(actions);

            // Check if this version has CDC files
            var cdcFiles = actions.OfType<CdcFile>().ToList();

            if (cdcFiles.Count > 0)
            {
                // Read from CDC files — they have _change_type already
                foreach (var cdcFile in cdcFiles)
                {
                    await foreach (var batch in ReadCdcFileAsync(
                        fs, cdcFile, version, commitTimestamp, readOptions, ctx,
                        cancellationToken).ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }
            }
            else
            {
                // Infer changes from add/remove actions
                var adds = actions.OfType<AddFile>()
                    .Where(a => a.DataChange).ToList();
                var removes = actions.OfType<RemoveFile>()
                    .Where(r => r.DataChange).ToList();

                // Removed files → "delete" rows. The action's DELETION VECTOR is honored: rows it already
                // marked deleted are not live at this version — they were reported as deletes when the DV
                // itself committed, so materializing them again double-counts (e.g. an overwrite removing a
                // file an earlier DV-delete had soft-deleted rows in). Same on the add side: a DV-carrying
                // add's marked rows are not live inserts, which is also what makes a DV-delete visible at all
                // under inference (remove reports the pre-image rows, add reports only the survivors).
                foreach (var remove in removes)
                {
                    await foreach (var batch in ReadDataFileAsChangesAsync(
                        fs, remove.Path, remove.PartitionValues, remove.DeletionVector,
                        CdfConfig.Delete, version,
                        commitTimestamp, readOptions, ctx, dvReader,
                        remove.BaseRowId, remove.DefaultRowCommitVersion, cancellationToken)
                        .ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }

                // Added files → "insert" rows
                foreach (var add in adds)
                {
                    await foreach (var batch in ReadDataFileAsChangesAsync(
                        fs, add.Path, add.PartitionValues, add.DeletionVector,
                        CdfConfig.Insert, version,
                        commitTimestamp, readOptions, ctx, dvReader,
                        add.BaseRowId, add.DefaultRowCommitVersion, cancellationToken)
                        .ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }
            }
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadCdcFileAsync(
        ITableFileSystem fs,
        CdcFile cdcFile,
        long commitVersion,
        long? commitTimestamp,
        ParquetReadOptions? readOptions,
        CdfSchemaContext ctx,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var file = await fs.OpenReadAsync(
            EngineeredWood.DeltaLake.DeltaPath.Decode(cdcFile.Path), cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        await foreach (var batch in reader.ReadAllAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            // The file already carries _change_type (per-row); split it off so the physical data columns can be
            // mapped to logical + interleaved with the re-materialized partition columns, then re-attach it.
            var (physicalData, changeType) = SplitChangeType(batch);

            // The hidden row-tracking columns come off next, before anything positional runs: the partition
            // interleave walks the TABLE schema and pulls data columns in order, so a column the schema does
            // not know about either falls off the end or shifts the ones after it. Gated on the table actually
            // DECLARING the names — the strip also recognizes legacy internal ones, which on a table that does
            // not track rows would mean dropping a user column that merely shares the name.
            var cleanPhysical = physicalData;
            Int64Array? matIds = null, matVers = null;
            if (ctx.HasMaterializedColumns)
            {
                (cleanPhysical, matIds, matVers) = RowTracking.RowTrackingWriter.StripMaterializedColumns(
                    physicalData, ctx.MaterializedRowIdName, ctx.MaterializedRowVersionName);
            }

            var logical = MapToLogicalWithPartitions(
                cleanPhysical, (IReadOnlyDictionary<string, string>?)cdcFile.PartitionValues, ctx);
            var withChangeType = changeType is null
                ? logical
                : AppendColumn(logical,
                    new Field(CdfConfig.ChangeTypeColumn, StringType.Default, false), changeType);
            var withMetadata = AddMetadataColumns(withChangeType, commitVersion, commitTimestamp);

            // A cdc action carries no baseRowId / defaultRowCommitVersion, so the materialized value is the
            // whole story for the id. The commit version falls back to this commit — that is what a null
            // means in a change file Spark wrote, and it is the version the row belongs to by definition.
            yield return ctx.EmitRowTracking
                ? ResolveRowTracking(
                    withMetadata, matIds, matVers,
                    baseRowId: null, defaultRowCommitVersion: commitVersion, absolutePositions: null,
                    ctx.EmittedRowIdName, ctx.EmittedRowVersionName)
                : withMetadata;
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadDataFileAsChangesAsync(
        ITableFileSystem fs,
        string path,
        IReadOnlyDictionary<string, string>? partitionValues,
        EngineeredWood.DeltaLake.Actions.DeletionVector? deletionVector,
        string changeType,
        long commitVersion,
        long? commitTimestamp,
        ParquetReadOptions? readOptions,
        CdfSchemaContext ctx,
        DeletionVectorReader dvReader,
        long? baseRowId,
        long? defaultRowCommitVersion,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // path is an add.path (URL-encoded on-disk relative path) — decode to the on-disk name.
        string diskPath = EngineeredWood.DeltaLake.DeltaPath.Decode(path);

        // Try to read the file — it may have been deleted already
        if (!await fs.ExistsAsync(diskPath, cancellationToken).ConfigureAwait(false))
            yield break;

        // Rows the action's DV marks deleted are not part of this change (see the call sites).
        HashSet<long>? dvRows = deletionVector is null
            ? null
            : await dvReader.ReadAsync(deletionVector, cancellationToken).ConfigureAwait(false);

        await using var file = await fs.OpenReadAsync(diskPath, cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        // DV positions are absolute within the file, and batches arrive in file order. The running offset is
        // also what a row's positional id is derived from, so it advances on every batch, not only when a
        // deletion vector made it necessary.
        long batchStartRow = 0;
        await foreach (var rawBatch in reader.ReadAllAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            long thisBatchStart = batchStartRow;
            batchStartRow += rawBatch.Length;

            // Hidden row-tracking columns off first — a data file carries them for every row a rewrite moved,
            // and they are indexed by RAW position, so they must be captured before the DV filter renumbers.
            var rawBatch2 = rawBatch;
            Int64Array? rawMatIds = null, rawMatVers = null;
            if (ctx.HasMaterializedColumns)
            {
                (rawBatch2, rawMatIds, rawMatVers) = RowTracking.RowTrackingWriter.StripMaterializedColumns(
                    rawBatch, ctx.MaterializedRowIdName, ctx.MaterializedRowVersionName);
            }

            // Source indices of the rows that survive DV filtering, in emit order — the map back to the raw
            // arrays above, and to each row's absolute in-file position.
            List<int>? survivorSrc = ctx.EmitRowTracking ? new List<int>(rawBatch2.Length) : null;
            if (survivorSrc is not null)
            {
                for (int i = 0; i < rawBatch2.Length; i++)
                    if (dvRows is null || !dvRows.Contains(thisBatchStart + i))
                        survivorSrc.Add(i);
            }

            var batch = rawBatch2;
            if (dvRows is not null)
            {
                batch = DeletionVectorFilter.Filter(batch, dvRows, thisBatchStart);
                if (batch.Length == 0)
                    continue; // every row of this batch was already deleted
            }

            // Strip a _change_type column if the data file happens to carry one (defensive), map physical → logical
            // + re-materialize partition columns, then add the constant _change_type for this add/remove.
            var cleanBatch = StripChangeTypeColumn(batch);
            var logical = MapToLogicalWithPartitions(cleanBatch, partitionValues, ctx);
            var withChangeType = CdfWriter.AddChangeTypeColumn(logical, changeType);
            var withMetadata = AddMetadataColumns(withChangeType, commitVersion, commitTimestamp);

            if (!ctx.EmitRowTracking)
            {
                yield return withMetadata;
                continue;
            }

            // Inferred from a data file, so the full resolution applies: the materialized value where the file
            // has one, else the action's baseRowId + the row's absolute position / its defaultRowCommitVersion.
            var positions = new long[survivorSrc!.Count];
            for (int k = 0; k < survivorSrc.Count; k++)
                positions[k] = thisBatchStart + survivorSrc[k];

            yield return ResolveRowTracking(
                withMetadata,
                Gather(rawMatIds, survivorSrc), Gather(rawMatVers, survivorSrc),
                baseRowId, defaultRowCommitVersion, positions,
                ctx.EmittedRowIdName, ctx.EmittedRowVersionName);
        }
    }

    /// <summary>Picks <paramref name="src"/>'s values at <paramref name="indices"/> (the DV survivors), so a
    /// raw-position-indexed array lines up with the emitted batch. Null in, null out.</summary>
    private static Int64Array? Gather(Int64Array? src, List<int> indices)
    {
        if (src is null)
            return null;
        var b = new Int64Array.Builder().Reserve(indices.Count);
        foreach (int i in indices)
        {
            if (i < src.Length && !src.IsNull(i))
                b.Append(src.GetValue(i)!.Value);
            else
                b.AppendNull();
        }
        return b.Build();
    }

    /// <summary>
    /// Appends the Delta spec's two GENERATED row-tracking columns — <c>_metadata.row_id</c> and
    /// <c>_metadata.row_commit_version</c> — resolving each row the way a conformant reader does: the
    /// MATERIALIZED value when the source carries one, else the positional default.
    ///
    /// <para>The default differs by source, which is why it is a parameter rather than a rule here. From a
    /// data file (the inferred feed) it is <c>baseRowId + absolute position</c> and the action's
    /// <c>defaultRowCommitVersion</c>, matching a <c>DeltaRowMetadata.RowTracking</c> read exactly. From a
    /// change file there is no <c>baseRowId</c> to add to — a <c>cdc</c> action has no such field — so an
    /// unmaterialized id stays NULL, while the version defaults to the commit that produced the file.</para>
    ///
    /// <para>Both columns are nullable: a change touching a file that predates row tracking on the table has
    /// no identity to report, and reporting a fabricated one would be worse than reporting none.</para>
    /// </summary>
    private static RecordBatch ResolveRowTracking(
        RecordBatch batch,
        Int64Array? materializedIds,
        Int64Array? materializedVersions,
        long? baseRowId,
        long? defaultRowCommitVersion,
        IReadOnlyList<long>? absolutePositions,
        string emittedRowIdName,
        string emittedRowVersionName)
    {
        var idb = new Int64Array.Builder().Reserve(batch.Length);
        var vrb = new Int64Array.Builder().Reserve(batch.Length);

        for (int i = 0; i < batch.Length; i++)
        {
            long? mid = materializedIds is not null && i < materializedIds.Length && !materializedIds.IsNull(i)
                ? materializedIds.GetValue(i) : null;
            long? id = mid ?? (baseRowId is { } br && absolutePositions is not null && i < absolutePositions.Count
                ? br + absolutePositions[i] : (long?)null);
            if (id is { } iv) idb.Append(iv); else idb.AppendNull();

            long? mv = materializedVersions is not null && i < materializedVersions.Length
                       && !materializedVersions.IsNull(i)
                ? materializedVersions.GetValue(i) : null;
            long? ver = mv ?? defaultRowCommitVersion;
            if (ver is { } vv) vrb.Append(vv); else vrb.AppendNull();
        }

        return RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(
            batch, idb.Build(), vrb.Build(),
            emittedRowIdName, emittedRowVersionName, nullable: true);
    }

    // Maps a PHYSICAL-layout data batch (physical names, partition columns absent) to the LOGICAL schema and
    // interleaves the partition columns from the action's partitionValues. A no-op for a plain, unpartitioned
    // table (logical == physical, no partitions), so the common path is byte-identical to before.
    private static RecordBatch MapToLogicalWithPartitions(
        RecordBatch physicalData,
        IReadOnlyDictionary<string, string>? partitionValues,
        CdfSchemaContext ctx)
    {
        var logical = ColumnMappingRecursive.ToLogical(physicalData, ctx.DeltaSchema, ctx.MappingMode);
        if (ctx.PartitionColumns.Count == 0)
            return logical;
        return PartitionUtils.AddPartitionColumns(
            logical, ctx.LogicalArrowSchema, partitionValues ?? EmptyPartitionValues,
            ctx.PartitionColumns, ctx.LogicalToPhysical);
    }

    private static readonly IReadOnlyDictionary<string, string> EmptyPartitionValues =
        new Dictionary<string, string>();

    private static RecordBatch AddMetadataColumns(
        RecordBatch batch, long commitVersion, long? commitTimestamp)
    {
        // Both added columns are constant down the whole batch — one commit contributes one version and one
        // timestamp — so they are tiled rather than appended a row at a time. Repeat allocates no validity
        // buffer, which is what lets Arrow skip the per-element validity check downstream; the absent
        // timestamp is the one case that needs one, and every row of it is null rather than a sentinel.
        var versionColumn = ArrowCompute.Repeat(
            Int64Type.Default, BitConverter.GetBytes(commitVersion), batch.Length);
        var timestampColumn = commitTimestamp is { } ts
            ? ArrowCompute.Repeat(Int64Type.Default, BitConverter.GetBytes(ts), batch.Length)
            : ArrowCompute.MakeNullArray(Int64Type.Default, batch.Length);

        var columns = new IArrowArray[batch.ColumnCount + 2];
        var fields = new List<Field>(batch.ColumnCount + 2);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = versionColumn;
        fields.Add(new Field(CdfConfig.CommitVersionColumn, Int64Type.Default, false));

        columns[batch.ColumnCount + 1] = timestampColumn;
        fields.Add(new Field(CdfConfig.CommitTimestampColumn, Int64Type.Default, true));

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    // Splits a batch into (everything except _change_type, the _change_type array) — null array when the column
    // is absent. Keeps the per-row change-type values (a _change_data file could carry more than one type).
    private static (RecordBatch Data, IArrowArray? ChangeType) SplitChangeType(RecordBatch batch)
    {
        int ctIdx = batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
        if (ctIdx < 0)
            return (batch, null);

        var changeType = batch.Column(ctIdx);
        var columns = new IArrowArray[batch.ColumnCount - 1];
        var fields = new List<Field>(batch.ColumnCount - 1);
        int outIdx = 0;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (i == ctIdx) continue;
            columns[outIdx++] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);
        return (new RecordBatch(schema.Build(), columns, batch.Length), changeType);
    }

    private static RecordBatch AppendColumn(RecordBatch batch, Field field, IArrowArray array)
    {
        var columns = new IArrowArray[batch.ColumnCount + 1];
        var fields = new List<Field>(batch.ColumnCount + 1);
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }
        columns[batch.ColumnCount] = array;
        fields.Add(field);

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);
        return new RecordBatch(schema.Build(), columns, batch.Length);
    }

    private static RecordBatch StripChangeTypeColumn(RecordBatch batch)
    {
        int ctIdx = batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
        if (ctIdx < 0)
            return batch;

        var columns = new IArrowArray[batch.ColumnCount - 1];
        var fields = new List<Field>(batch.ColumnCount - 1);
        int outIdx = 0;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (i == ctIdx) continue;
            columns[outIdx++] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, batch.Length);
    }
}
