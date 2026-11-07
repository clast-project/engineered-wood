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
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using EngineeredWood.Scans;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Reads Change Data Feed (CDC) to return row-level changes between versions.
/// For each version, if CDC files exist they are used; otherwise changes are
/// inferred from add/remove actions.
/// </summary>
internal static class CdfReader
{
    // The schema view the feed is resolved against: the LOGICAL schema (with column-mapping metadata and
    // partition columns) of the snapshot the caller picked. Both _change_data files and inferred-from-data-file
    // rows are stored in the PHYSICAL layout (physical names + field ids, partition columns absent — the
    // data-file convention), so the reader maps them back to logical names and then reconciles them exactly as
    // a scan does: partition columns from the action's partitionValues, columns the file predates as NULL,
    // widened types widened. One schema serves the whole range, so every batch of one feed has the same shape.
    private sealed record CdfSchemaContext(
        Snapshot.Snapshot SchemaSnapshot,
        ColumnMappingMode MappingMode,
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
    /// <para>Every batch is reconciled to <paramref name="schemaSnapshot"/>'s schema; which snapshot that is
    /// (Spark's rule depends on column mapping) is the caller's decision.</para>
    /// <para>The files the feed reads (change files, and the data files changes are inferred from) are read
    /// up to <paramref name="maxConcurrentReads"/> at once, holding at most
    /// <paramref name="maxBufferedReadBytes"/> of decoded batches the caller has not taken, as a scan does
    /// (<see cref="OrderedScan"/>). Batches come in the order a serial read gives: by version, then file. On an
    /// object store each file costs a few round trips, and read one after another they set the feed's time.</para>
    /// <para>Which commits the range has comes from ONE listing of the log, as delta-spark's does (measured
    /// against delta-spark 4.0.0): a version with no commit file inside the range is skipped (unless the caller
    /// could not build <paramref name="schemaSnapshot"/> across it, under column mapping); a range that starts
    /// before the earliest commit file left (log cleanup removed it), or whose end has no commit file, is refused
    /// with <see cref="DeltaErrorCodes.TruncatedTransactionLog"/>; and every listed commit must read, so a read
    /// that fails fails the feed. An end past <paramref name="latestVersion"/> is read as that version: commits made
    /// after the handle's snapshot are not read until the handle is refreshed.
    /// The listed commits are read up to <paramref name="maxConcurrentReads"/> ahead, in order (#724).</para>
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        ITableFileSystem fs,
        TransactionLog log,
        long startVersion,
        long endVersion,
        ParquetReadOptions? readOptions,
        Snapshot.Snapshot schemaSnapshot,
        string? materializedRowIdName = null,
        string? materializedRowVersionName = null,
        bool emitRowTracking = false,
        string? emittedRowIdName = null,
        string? emittedRowVersionName = null,
        int maxConcurrentReads = 1,
        long maxBufferedReadBytes = DefaultMaxBufferedReadBytes,
        long latestVersion = -1,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var versions = await CommitsInRangeAsync(log, startVersion, endVersion, latestVersion, cancellationToken)
            .ConfigureAwait(false);

        var mappingMode = ColumnMapping.GetMode(schemaSnapshot.Metadata.Configuration);
        var ctx = new CdfSchemaContext(
            schemaSnapshot, mappingMode,
            mappingMode == ColumnMappingMode.None
                ? new Dictionary<string, string>()
                : ColumnMapping.BuildLogicalToPhysicalMap(schemaSnapshot.Schema, mappingMode),
            materializedRowIdName, materializedRowVersionName, emitRowTracking,
            emittedRowIdName ?? DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName,
            emittedRowVersionName
                ?? DeltaLake.RowTracking.RowTrackingConfig.RowCommitVersionColumnName);

        // Stateless over the file system — one instance serves every action in the range.
        var dvReader = new DeletionVectorReader(fs);

        // The units come from ONE enumeration, in order. It reads the commits ahead of itself, and under
        // OrderedScan those reads overlap the files already dispatched. Every other request is made by a file's
        // read.
        var units = UnitsAsync(log, versions, Math.Max(1, maxConcurrentReads), dvReader, cancellationToken);

        IAsyncEnumerable<RecordBatch> batches = maxConcurrentReads <= 1
            ? SeriallyAsync(fs, units, readOptions, ctx, dvReader, cancellationToken)
            : OrderedScan.RunAsync(
                units,
                (unit, reservation, ct) => ReadUnitAsync(fs, unit, readOptions, ctx, dvReader, reservation, ct),
                static b => RollingDataFileWriter.EstimatedBytes(b),
                static b => b.Dispose(),
                maxConcurrentReads,
                new ScanMemoryBudget(maxBufferedReadBytes),
                cancellationToken: cancellationToken);

        await foreach (var batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return batch;
    }

    /// <summary><see cref="DeltaTableOptions.MaxBufferedReadBytes"/>'s default, for a caller that gives none.</summary>
    internal const long DefaultMaxBufferedReadBytes = 256L * 1024 * 1024;

    /// <summary>
    /// One file the feed reads, with everything its read needs: a <c>_change_data</c> file
    /// (<see cref="IsChangeFile"/>), or a data file whose rows, as <see cref="Filter"/> selects them, are reported
    /// as <see cref="ChangeType"/>. <see cref="Size"/> is the size the action records, null when it records none.
    /// </summary>
    private sealed record CdfUnit(
        bool IsChangeFile,
        string Path,
        long? Size,
        IReadOnlyDictionary<string, string>? PartitionValues,
        RowFilter Filter,
        string ChangeType,
        long Version,
        long? CommitTimestamp,
        long? BaseRowId,
        long? DefaultRowCommitVersion);

    /// <summary>
    /// The versions of [<paramref name="startVersion"/>, <paramref name="endVersion"/>] that have a commit file,
    /// ascending, from one listing of the log. The end is clamped to <paramref name="latestVersion"/> (the
    /// handle's snapshot; the newest commit file when it is negative); a start past it gives no versions.
    /// Refused, as delta-spark refuses them: a start before the earliest commit file (the commits it needs were
    /// removed by log cleanup), and a clamped end with no commit file of its own. A version missing in
    /// between is left out, which is delta-spark's behaviour too.
    /// </summary>
    private static async ValueTask<List<long>> CommitsInRangeAsync(
        TransactionLog log, long startVersion, long endVersion, long latestVersion,
        CancellationToken cancellationToken)
    {
        var listed = new List<long>();
        await foreach (long version in log.ListVersionsAsync(0, cancellationToken).ConfigureAwait(false))
            listed.Add(version);

        // The handle's snapshot bounds the feed, as it bounds every read on the handle: a commit made after it may
        // carry a schema or protocol the feed's schema snapshot does not know. Only with no bound given does the
        // newest commit file stand in for it.
        long latest = latestVersion >= 0 ? latestVersion : (listed.Count > 0 ? listed[listed.Count - 1] : -1);
        long end = Math.Min(endVersion, latest);
        var inRange = new List<long>();
        if (startVersion > end)
            return inRange;

        if (listed.Count == 0 || listed[0] > startVersion)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.TruncatedTransactionLog,
                listed.Count == 0
                    ? $"Cannot read changes from version {startVersion}: the log holds no commit files."
                    : $"Cannot read changes from version {startVersion}: the earliest commit left in the log is "
                      + $"version {listed[0]}; the ones before it were removed (log cleanup, under "
                      + "delta.logRetentionDuration).");
        }
        if (listed.BinarySearch(end) < 0)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.TruncatedTransactionLog,
                $"Cannot read changes up to version {end}: the log has no commit file for it.");
        }

        foreach (long version in listed)
        {
            if (version >= startVersion && version <= end)
                inRange.Add(version);
        }
        return inRange;
    }

    /// <summary>
    /// The files of every listed version, in the order the feed reports them: a version's change files when it
    /// has any; else its removed data files, then its added ones. Up to <paramref name="readAhead"/> commits are
    /// read at once, the next ones started as each is taken, so the reads run only that far ahead of the units.
    /// A commit that fails to read fails the feed at its position.
    /// </summary>
    private static async IAsyncEnumerable<CdfUnit> UnitsAsync(
        TransactionLog log,
        IReadOnlyList<long> versions,
        int readAhead,
        DeletionVectorReader dvReader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pending = new Queue<(long Version, Task<IReadOnlyList<DeltaAction>> Read)>();
        int next = 0;
        try
        {
            while (true)
            {
                while (pending.Count < readAhead && next < versions.Count)
                {
                    long v = versions[next++];
                    pending.Enqueue((v, log.ReadCommitAsync(v, stop.Token).AsTask()));
                }
                if (pending.Count == 0)
                    yield break;

                var (version, read) = pending.Dequeue();
                var actions = await read.ConfigureAwait(false);

                // Get commit timestamp
                long? commitTimestamp = InCommitTimestamp.GetTimestampFromActions(actions);

                // Check if this version has CDC files
                var cdcFiles = actions.OfType<CdcFile>().ToList();

                if (cdcFiles.Count > 0)
                {
                    // Read from CDC files — they have _change_type already
                    foreach (var cdcFile in cdcFiles)
                    {
                        yield return new CdfUnit(
                            IsChangeFile: true, cdcFile.Path, cdcFile.Size,
                            (IReadOnlyDictionary<string, string>?)cdcFile.PartitionValues,
                            Filter: default, ChangeType: "", version, commitTimestamp,
                            BaseRowId: null, DefaultRowCommitVersion: null);
                    }
                    continue;
                }

                // Infer changes from add/remove actions
                var adds = actions.OfType<AddFile>()
                    .Where(a => a.DataChange).ToList();
                var removes = actions.OfType<RemoveFile>()
                    .Where(r => r.DataChange).ToList();

                // A remove and an add of the SAME file in one version, either carrying a deletion vector, is a
                // vector edit rather than a file replaced: what changed is the difference between the two
                // vectors, not every row of the file. Read whole, the pair reported every survivor deleted and
                // re-inserted. Spark's reader pairs them the same way (CDCReader.processDeletionVectorActions).
                // Pairing needs only the actions. The vectors are read, and the difference taken, by the file's own
                // read (RowFilter.ResolveEditAsync), so the dispatcher sends no request for them.
                var vectorEdits = PairVectorEdits(removes, adds, dvReader);

                // Removed files → "delete" rows. The action's DELETION VECTOR is honored: rows it already
                // marked deleted are not live at this version — they were reported as deletes when the DV
                // itself committed, so materializing them again double-counts (e.g. an overwrite removing a
                // file an earlier DV-delete had soft-deleted rows in). Same on the add side: a DV-carrying
                // add's marked rows are not live inserts.
                foreach (var remove in removes)
                {
                    RowFilter filter;
                    if (vectorEdits.TryGetValue(remove.Path, out var edit))
                    {
                        // The rows the new vector hides that the old one did not.
                        filter = RowFilter.Edited(edit, reinserted: false);
                    }
                    else
                    {
                        filter = RowFilter.Exclude(remove.DeletionVector);
                    }

                    yield return new CdfUnit(
                        IsChangeFile: false, remove.Path, remove.Size ?? edit?.Add.Size,
                        remove.PartitionValues ?? edit?.Add.PartitionValues, filter,
                        CdfConfig.Delete, version, commitTimestamp,
                        remove.BaseRowId ?? edit?.Add.BaseRowId,
                        remove.DefaultRowCommitVersion ?? edit?.Add.DefaultRowCommitVersion);
                }

                // Added files → "insert" rows
                foreach (var add in adds)
                {
                    RowFilter filter;
                    if (vectorEdits.TryGetValue(add.Path, out var edit))
                    {
                        // The rows the old vector hid that the new one no longer does — a RESTORE un-deleting
                        // them. An ordinary vector DELETE has none.
                        filter = RowFilter.Edited(edit, reinserted: true);
                    }
                    else
                    {
                        filter = RowFilter.Exclude(add.DeletionVector);
                    }

                    yield return new CdfUnit(
                        IsChangeFile: false, add.Path, add.Size, add.PartitionValues, filter,
                        CdfConfig.Insert, version, commitTimestamp,
                        add.BaseRowId, add.DefaultRowCommitVersion);
                }
            }
        }
        finally
        {
            // Stop the reads still running ahead (an early exit, or a failure) and wait for them, so none is left
            // unobserved or still running once the feed is gone.
            stop.Cancel();
            while (pending.Count > 0)
            {
                try
                {
                    await pending.Dequeue().Read.ConfigureAwait(false);
                }
                catch
                {
                    // Cancelled, or failed after the feed had stopped needing it.
                }
            }
        }
    }

    private static async IAsyncEnumerable<RecordBatch> SeriallyAsync(
        ITableFileSystem fs,
        IAsyncEnumerable<CdfUnit> units,
        ParquetReadOptions? readOptions,
        CdfSchemaContext ctx,
        DeletionVectorReader dvReader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var unit in units.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await foreach (var batch in ReadUnitAsync(
                fs, unit, readOptions, ctx, dvReader, ScanReservation.None, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    private static IAsyncEnumerable<RecordBatch> ReadUnitAsync(
        ITableFileSystem fs,
        CdfUnit unit,
        ParquetReadOptions? readOptions,
        CdfSchemaContext ctx,
        DeletionVectorReader dvReader,
        ScanReservation reservation,
        CancellationToken cancellationToken) =>
        unit.IsChangeFile
            ? ReadCdcFileAsync(fs, unit, readOptions, ctx, reservation, cancellationToken)
            : ReadDataFileAsChangesAsync(fs, unit, readOptions, ctx, dvReader, reservation, cancellationToken);

    /// <summary>
    /// Opens <paramref name="diskPath"/> and reads its footer, which is where a missing file shows up. The size
    /// the action records is passed on (<see cref="KnownLengthRandomAccessFile"/>), so nothing asks the store
    /// about the file before its first read: on an object store that was a HEAD per file, and a data file paid
    /// a second one to check that it still existed. With <paramref name="missingMeansVacuumed"/>, a failure on
    /// a file that turns out not to exist (VACUUM removed it) returns null instead. The existence check runs
    /// only then, never for a file that opens.
    /// </summary>
    private static async ValueTask<OpenedFile?> OpenAsync(
        ITableFileSystem fs,
        string diskPath,
        long? size,
        ParquetReadOptions? readOptions,
        bool missingMeansVacuumed,
        CancellationToken cancellationToken)
    {
        IRandomAccessFile? file = null;
        ParquetFileReader? reader = null;
        try
        {
            file = await KnownLengthRandomAccessFile.OpenAsync(fs, diskPath, size, cancellationToken)
                .ConfigureAwait(false);
            reader = new ParquetFileReader(file, ownsFile: false, readOptions);
            var footer = await DataFileFooter.ReadAsync(reader, file, diskPath, cancellationToken).ConfigureAwait(false);
            return new OpenedFile(file, reader, footer);
        }
        catch (Exception) when (missingMeansVacuumed && !cancellationToken.IsCancellationRequested)
        {
            reader?.Dispose();
            if (file is not null)
                await file.DisposeAsync().ConfigureAwait(false);
            if (await fs.ExistsAsync(diskPath, cancellationToken).ConfigureAwait(false))
                throw;
            return null;
        }
        catch
        {
            reader?.Dispose();
            if (file is not null)
                await file.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>An open file, its reader, and its footer. Disposing it disposes the reader, then the file.</summary>
    private sealed class OpenedFile(IRandomAccessFile file, ParquetFileReader reader, Parquet.Metadata.FileMetaData footer)
        : IAsyncDisposable
    {
        public ParquetFileReader Reader { get; } = reader;
        public Parquet.Metadata.FileMetaData Footer { get; } = footer;

        public async ValueTask DisposeAsync()
        {
            Reader.Dispose();
            await file.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>A Parquet read's batches, each with the file position of its first row.</summary>
    private static async IAsyncEnumerable<(RecordBatch Batch, long FirstRow)> PositionedAsync(
        IAsyncEnumerable<PositionedRecordBatch> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var positioned in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return (positioned.Batch, positioned.FirstRow);
    }

    private static async IAsyncEnumerable<RecordBatch> ReadCdcFileAsync(
        ITableFileSystem fs,
        CdfUnit unit,
        ParquetReadOptions? readOptions,
        CdfSchemaContext ctx,
        ScanReservation reservation,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long commitVersion = unit.Version;
        long? commitTimestamp = unit.CommitTimestamp;

        // A missing change file is an error, as it was before.
        await using var opened = (await OpenAsync(
            fs, EngineeredWood.DeltaLake.DeltaPath.Decode(unit.Path), unit.Size, readOptions,
            missingMeansVacuumed: false, cancellationToken).ConfigureAwait(false))!;
        var reader = opened.Reader;

        // Every column of every row group is read, so the largest row group bounds the first reservation.
        if (reservation != ScanReservation.None)
            reservation.SetInitialEstimate(DeltaTable.LargestRowGroupBytes(opened.Footer, null, null));

        await foreach (var (batch, _) in DeltaTable.Reserving(
            PositionedAsync(reader.ReadWithPositionsAsync(null, null, cancellationToken), cancellationToken),
            reservation, cancellationToken).ConfigureAwait(false))
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

            var logical = ToTableShape(
                cleanPhysical, unit.PartitionValues, ctx);
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

    /// <summary>
    /// Reads back a change file this table's writer wrote under <paramref name="snapshot"/>, for rewriting it: each
    /// batch's rows in the table's LOGICAL shape (the shape <see cref="CdfWriter.PrepareRows"/> takes), its
    /// <c>_change_type</c> values, and the materialized row-tracking columns exactly as stored (a NULL commit version
    /// stays NULL, so it still resolves to whichever version the file is finally committed in).
    /// </summary>
    internal static async IAsyncEnumerable<(RecordBatch Rows, StringArray ChangeTypes, Int64Array? RowIds,
        Int64Array? RowCommitVersions)> ReadWrittenChangeFileAsync(
        ITableFileSystem fs,
        Snapshot.Snapshot snapshot,
        CdcFile file,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var (rowIdName, rowVersionName) = DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        var ctx = new CdfSchemaContext(
            snapshot, mappingMode,
            mappingMode == ColumnMappingMode.None
                ? new Dictionary<string, string>()
                : ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode),
            rowIdName, rowVersionName, EmitRowTracking: false,
            DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName,
            DeltaLake.RowTracking.RowTrackingConfig.RowCommitVersionColumnName);

        await using var opened = (await OpenAsync(
            fs, EngineeredWood.DeltaLake.DeltaPath.Decode(file.Path), file.Size, readOptions: null,
            missingMeansVacuumed: false, cancellationToken).ConfigureAwait(false))!;
        await foreach (var batch in opened.Reader.ReadAllAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            var (physicalData, changeType) = SplitChangeType(batch);
            if (changeType is not StringArray changeTypes)
            {
                throw new InvalidOperationException(
                    $"The change file '{file.Path}' has no string {CdfConfig.ChangeTypeColumn} column "
                    + $"(read as {changeType?.Data.DataType.Name ?? "absent"}).");
            }
            var cleanPhysical = physicalData;
            Int64Array? ids = null, versions = null;
            if (ctx.HasMaterializedColumns)
            {
                (cleanPhysical, ids, versions) = RowTracking.RowTrackingWriter.StripMaterializedColumns(
                    physicalData, ctx.MaterializedRowIdName, ctx.MaterializedRowVersionName);
            }
            yield return (ToTableShape(cleanPhysical, file.PartitionValues, ctx), changeTypes, ids, versions);
        }
    }

    /// <summary>
    /// A remove/add pair of one file whose vectors differ. The positions that changed are worked out once, on
    /// first use, and shared by the pair's two reads (the delete side and the insert side), which may run at
    /// the same time.
    /// </summary>
    private sealed class VectorEdit(RemoveFile remove, AddFile add, DeletionVectorReader dvReader)
    {
        private readonly object _gate = new();
        private Task<(DeletionVectorBitmap NewlyDeleted, DeletionVectorBitmap Reinserted)>? _diff;

        public AddFile Add => add;

        /// <summary>The rows the new vector hides that the old one did not, and the rows the old one hid that
        /// the new one no longer does. Reads both vectors, so it fails when VACUUM has removed them.</summary>
        public Task<(DeletionVectorBitmap NewlyDeleted, DeletionVectorBitmap Reinserted)> DiffAsync(
            CancellationToken cancellationToken)
        {
            lock (_gate)
                return _diff ??= ComputeAsync(cancellationToken);
        }

        private async Task<(DeletionVectorBitmap NewlyDeleted, DeletionVectorBitmap Reinserted)> ComputeAsync(
            CancellationToken cancellationToken)
        {
            var oldRows = remove.DeletionVector is null
                ? DeletionVectorBitmap.Empty
                : await dvReader.ReadAsync(remove.DeletionVector, cancellationToken).ConfigureAwait(false);
            var newRows = add.DeletionVector is null
                ? DeletionVectorBitmap.Empty
                : await dvReader.ReadAsync(add.DeletionVector, cancellationToken).ConfigureAwait(false);
            return (newRows.Except(oldRows), oldRows.Except(newRows));
        }
    }

    /// <summary>
    /// The version's vector edits by path: each file both removed and added in it, with a deletion vector on
    /// at least one side. A pair with neither is a file replaced by itself and is read whole as before; a path
    /// removed or added twice is left unpaired, since there is no one pair to diff. Nothing is read here.
    /// </summary>
    private static Dictionary<string, VectorEdit> PairVectorEdits(
        List<RemoveFile> removes, List<AddFile> adds, DeletionVectorReader dvReader)
    {
        var edits = new Dictionary<string, VectorEdit>(StringComparer.Ordinal);
        if (removes.Count == 0 || adds.Count == 0)
            return edits;

        var removeByPath = UniqueByPath(removes, r => r.Path);
        var addByPath = UniqueByPath(adds, a => a.Path);
        foreach (var (path, remove) in removeByPath)
        {
            if (!addByPath.TryGetValue(path, out var add)
                || (remove.DeletionVector is null && add.DeletionVector is null))
            {
                continue;
            }
            edits[path] = new VectorEdit(remove, add, dvReader);
        }
        return edits;
    }

    // Null marks a path that occurs more than once, so the caller can skip it.
    private static Dictionary<string, T> UniqueByPath<T>(List<T> actions, Func<T, string> path)
        where T : class
    {
        var byPath = new Dictionary<string, T?>(StringComparer.Ordinal);
        foreach (var action in actions)
            byPath[path(action)] = byPath.ContainsKey(path(action)) ? null : action;
        return byPath.Where(kv => kv.Value is not null)
            .ToDictionary(kv => kv.Key, kv => kv.Value!, StringComparer.Ordinal);
    }

    /// <summary>Which rows of a data file belong to a change: all but <see cref="Positions"/> (a deletion
    /// vector's hidden rows), or only <see cref="Positions"/> (the rows a vector edit changed). The vectors are
    /// read on the file's own read. An excluding one is read by <see cref="ResolveAsync"/>, once the data file is
    /// known to exist (VACUUM removes a file's vector along with it). A vector edit's two are read by
    /// <see cref="ResolveEditAsync"/> before the file is opened, so that a side with nothing to report opens
    /// nothing.</summary>
    private readonly record struct RowFilter(
        DeletionVectorBitmap? Positions, bool KeepOnlyPositions,
        EngineeredWood.DeltaLake.Actions.DeletionVector? Unread = null,
        VectorEdit? Edit = null, bool ReinsertedSide = false)
    {
        public static RowFilter KeepOnly(DeletionVectorBitmap positions) => new(positions, true);

        public static RowFilter Exclude(EngineeredWood.DeltaLake.Actions.DeletionVector? deletionVector) =>
            new(null, false, deletionVector);

        /// <summary>One side of a vector edit: its newly deleted rows, or (<paramref name="reinserted"/>) the rows
        /// it un-deletes.</summary>
        public static RowFilter Edited(VectorEdit edit, bool reinserted) =>
            new(null, true, null, edit, reinserted);

        /// <summary>A vector edit's side as the positions it keeps; any other filter unchanged.</summary>
        public async ValueTask<RowFilter> ResolveEditAsync(CancellationToken cancellationToken)
        {
            if (Edit is null)
                return this;
            var (newlyDeleted, reinserted) = await Edit.DiffAsync(cancellationToken).ConfigureAwait(false);
            return KeepOnly(ReinsertedSide ? reinserted : newlyDeleted);
        }

        public async ValueTask<RowFilter> ResolveAsync(
            DeletionVectorReader dvReader, CancellationToken cancellationToken) =>
            Unread is null
                ? this
                : new(await dvReader.ReadAsync(Unread, cancellationToken).ConfigureAwait(false), false);

        /// <summary>
        /// The row groups a read can skip unread, since the filter keeps none of their rows: those the vector deletes
        /// in full when it excludes, those holding none of its positions when it keeps only them. Null when nothing
        /// can be skipped (no positions to exclude).
        /// </summary>
        public RowSkipPredicate? SkipRows => Positions switch
        {
            null => null,
            { IsEmpty: true } => KeepOnlyPositions ? (_, _) => true : null,
            var positions => KeepOnlyPositions
                ? (start, rows) => !positions.Intersects(start, rows)
                : positions.ContainsRange,
        };

        /// <summary>The batch-relative indices, ascending, of the rows of [<paramref name="start"/>,
        /// <paramref name="start"/> + <paramref name="rows"/>) the filter keeps.</summary>
        public List<int> Selected(long start, int rows)
        {
            var selected = new List<int>(rows);
            if (Positions is null)
            {
                if (!KeepOnlyPositions)
                    for (int i = 0; i < rows; i++)
                        selected.Add(i);
                return selected;
            }

            int words = SelectionMask.WordCount(rows);
            var rented = System.Buffers.ArrayPool<ulong>.Shared.Rent(Math.Max(words, 1));
            var indices = System.Buffers.ArrayPool<int>.Shared.Rent(Math.Max(rows, 1));
            try
            {
                var selection = rented.AsSpan(0, words);
                SelectionMask.SelectAll(selection, rows);
                Positions.ClearDeleted(start, rows, selection); // now: the rows the vector does NOT hold
                if (KeepOnlyPositions)
                    SelectionMask.Invert(selection, rows);
                int n = SelectionMask.ToIndices(selection, indices);
                for (int k = 0; k < n; k++)
                    selected.Add(indices[k]);
                return selected;
            }
            finally
            {
                System.Buffers.ArrayPool<int>.Shared.Return(indices);
                System.Buffers.ArrayPool<ulong>.Shared.Return(rented);
            }
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadDataFileAsChangesAsync(
        ITableFileSystem fs,
        CdfUnit unit,
        ParquetReadOptions? readOptions,
        CdfSchemaContext ctx,
        DeletionVectorReader dvReader,
        ScanReservation reservation,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var partitionValues = unit.PartitionValues;
        string changeType = unit.ChangeType;
        long commitVersion = unit.Version;
        long? commitTimestamp = unit.CommitTimestamp;
        long? baseRowId = unit.BaseRowId;
        long? defaultRowCommitVersion = unit.DefaultRowCommitVersion;

        // path is an add.path (URL-encoded on-disk relative path) — decode to the on-disk name.
        string diskPath = EngineeredWood.DeltaLake.DeltaPath.Decode(unit.Path);

        // A vector edit's side first: when it changed no rows (an ordinary vector DELETE un-deletes none), the
        // file is never opened. Its vectors failing to read may mean VACUUM removed them with the file; only
        // then is the file's existence checked, and a file that is still there makes the failure an error.
        var filter = unit.Filter;
        if (filter.Edit is not null)
        {
            bool vacuumed = false;
            try
            {
                filter = await filter.ResolveEditAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                if (await fs.ExistsAsync(diskPath, cancellationToken).ConfigureAwait(false))
                    throw;
                vacuumed = true;
            }
            if (vacuumed || filter.Positions!.IsEmpty)
                yield break;
        }

        // The file may have been deleted already (VACUUM); then it contributes nothing. An excluding vector is
        // read only after the file is known to exist, since VACUUM removes a file's vector along with it.
        await using var opened = await OpenAsync(
            fs, diskPath, unit.Size, readOptions, missingMeansVacuumed: true, cancellationToken).ConfigureAwait(false);
        if (opened is null)
            yield break;
        var reader = opened.Reader;

        filter = await filter.ResolveAsync(dvReader, cancellationToken).ConfigureAwait(false);

        // Every column is read. A row group the vector deletes in full is skipped unread, so it needs no
        // reservation; one a keep-only filter skips is still counted, which can only overestimate.
        if (reservation != ScanReservation.None)
        {
            reservation.SetInitialEstimate(DeltaTable.LargestRowGroupBytes(
                opened.Footer, null, filter.KeepOnlyPositions ? null : filter.Positions));
        }

        // DV positions are absolute within the file, and so is a row's positional id: each batch's first row comes
        // from the reader, which counts the row groups the filter lets it skip unread (#618).
        await foreach (var (rawBatch, thisBatchStart) in DeltaTable.Reserving(
            PositionedAsync(reader.ReadWithPositionsAsync(null, null, filter.SkipRows, cancellationToken), cancellationToken),
            reservation, cancellationToken).ConfigureAwait(false))
        {

            // Hidden row-tracking columns off first — a data file carries them for every row a rewrite moved,
            // and they are indexed by RAW position, so they must be captured before the DV filter renumbers.
            var rawBatch2 = rawBatch;
            Int64Array? rawMatIds = null, rawMatVers = null;
            if (ctx.HasMaterializedColumns)
            {
                (rawBatch2, rawMatIds, rawMatVers) = RowTracking.RowTrackingWriter.StripMaterializedColumns(
                    rawBatch, ctx.MaterializedRowIdName, ctx.MaterializedRowVersionName);
            }

            // Source indices of the rows the filter keeps, in emit order — the map back to the raw arrays above,
            // and to each row's absolute in-file position.
            bool filtered = filter.Positions is not null;
            List<int>? survivorSrc = ctx.EmitRowTracking || filtered
                ? filter.Selected(thisBatchStart, rawBatch2.Length)
                : null;

            var batch = rawBatch2;
            if (filtered && survivorSrc!.Count < rawBatch2.Length)
            {
                if (survivorSrc.Count == 0)
                    continue; // no row of this batch is part of the change
                var columns = new IArrowArray[rawBatch2.ColumnCount];
                for (int col = 0; col < rawBatch2.ColumnCount; col++)
                    columns[col] = ArrowCompute.Take(rawBatch2.Column(col), survivorSrc);
                batch = new RecordBatch(rawBatch2.Schema, columns, survivorSrc.Count);
            }

            // Strip a _change_type column if the data file happens to carry one (defensive), map physical → logical
            // + reconcile to the feed's schema, then add the constant _change_type for this add/remove.
            var cleanBatch = StripChangeTypeColumn(batch);
            var logical = ToTableShape(cleanBatch, partitionValues, ctx);
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

    // Maps a PHYSICAL-layout data batch (physical names, partition columns absent) to the LOGICAL schema, then
    // brings it to the shape a scan of the schema snapshot returns: partition columns interleaved from the
    // action's partitionValues, a column the file predates backfilled as NULL, a dropped one removed, widened
    // types widened. Without the reconcile, a range spanning an ADD COLUMN gave batches of two schemas.
    private static RecordBatch ToTableShape(
        RecordBatch physicalData,
        IReadOnlyDictionary<string, string>? partitionValues,
        CdfSchemaContext ctx)
    {
        var snapshot = ctx.SchemaSnapshot;
        // As read from a change or data file, so in name mode a dropped column's field is removed before the names
        // are mapped (in id mode ToLogical drops by id itself).
        var physical = ctx.MappingMode == ColumnMappingMode.Name
            ? ColumnMappingRecursive.DropStaleFields(physicalData, snapshot.Schema, ctx.MappingMode)
            : physicalData;
        var logical = ColumnMappingRecursive.StripParquetFieldIds(
            ColumnMappingRecursive.ToLogical(physical, snapshot.Schema, ctx.MappingMode));
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        return DeltaTable.ReconcileToTableShape(
            logical, snapshot, partitionValues, columns: null, partitionColumns, partitionColumns.Count > 0,
            ctx.LogicalToPhysical);
    }

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
