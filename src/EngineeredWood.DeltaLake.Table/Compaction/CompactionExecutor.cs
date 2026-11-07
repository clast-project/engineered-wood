// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.Parquet;
using EngineeredWood.Scans;

namespace EngineeredWood.DeltaLake.Table.Compaction;

/// <summary>
/// The output of a compaction: the files are already written, and these are the actions that would
/// publish them. Nothing here is committed — see <see cref="CompactionExecutor.ExecuteAsync"/> for why
/// the commit is the caller's.
/// </summary>
/// <param name="Actions">The removes of every rewritten file and the adds of what replaced them, with
/// the row-tracking high-water-mark advance when the table tracks rows. No <c>commitInfo</c> — the
/// committer writes that.</param>
/// <param name="AssignsRowIds">Whether the adds carry a <c>baseRowId</c> that a rebase has to
/// re-derive against the version it lands on.</param>
internal readonly record struct CompactionPlan(
    IReadOnlyList<DeltaAction> Actions,
    bool AssignsRowIds);

/// <summary>
/// Executes file compaction: reads small files and rewrites them as larger files, returning the
/// add/remove actions that publish the result.
/// </summary>
internal static class CompactionExecutor
{
    /// <summary>
    /// A field stripped to name/type/nullability for the clean rebuild before a re-write: reader-carried
    /// metadata (e.g. the source file's own <c>PARQUET:field_id</c>) malforms the footer when the writer
    /// re-stamps ids, so it is dropped — EXCEPT the <c>ARROW:extension:*</c> transport markers, which type
    /// the column for a pluggable host codec (see <see cref="IDataFileReader"/>) and must survive every
    /// rewrite, or the host loses the column's representation on compaction.
    /// </summary>
    internal static Field CleanField(Field f)
    {
        Dictionary<string, string>? kept = null;
        if (f.Metadata is { } md)
        {
            foreach (var kv in md)
            {
                if (kv.Key.StartsWith("ARROW:extension:", StringComparison.Ordinal))
                    (kept ??= new Dictionary<string, string>())[kv.Key] = kv.Value;
            }
        }
        return new Field(f.Name, f.DataType, f.IsNullable, kept);
    }

    /// <summary>
    /// Selects files eligible for compaction and rewrites them. Returns the actions that publish the
    /// rewrite, or null if no files were compacted.
    ///
    /// <para><b>It does not commit, deliberately.</b> OPTIMIZE used to make one attempt at
    /// <c>snapshot.Version + 1</c> from here, so any concurrent commit threw away a rewrite of the whole
    /// candidate set (#109). Committing is now the caller's, through the optimistic-concurrency loop that
    /// every other write path uses — see <see cref="DeltaTable.CompactAsync"/>, which also explains why a
    /// rewrite is safe to rebase and what refuses to.</para>
    /// </summary>
    public static async ValueTask<CompactionPlan?> ExecuteAsync(
        ITableFileSystem fs,
        DeltaSnapshot snapshot,
        CompactionOptions options,
        ParquetWriteOptions parquetOptions,
        ParquetReadOptions parquetReadOptions,
        CancellationToken cancellationToken,
        IDataFileWriter? dataFileWriter = null,
        IDataFileReader? dataFileReader = null,
        WrittenFileLedger? written = null,
        int maxConcurrentReads = 1,
        long maxBufferedReadBytes = long.MaxValue,
        EngineeredWood.Memory.MemoryBudget? memoryBudget = null)
    {
        // Select small files as compaction candidates and group them BY PARTITION: a data file belongs to
        // exactly ONE partition (its add.partitionValues), so each group must compact independently. Mixing
        // partitions into one file stamped with a single partition's values silently corrupted the partition
        // column of every other row. Unpartitioned tables form a single group (behaviour unchanged).
        // Canonical keys tolerate mixed logical/physical partitionValues vintages under column mapping.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        // One table, one selection: built once for every file this operation writes.
        var statsSelection = Stats.StatsColumnSelection.For(snapshot.Schema, snapshot.Metadata);
        var logicalToPhysical = mappingMode != ColumnMappingMode.None
            ? ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode)
            : null;
        var groups = snapshot.ActiveFiles.Values
            .Where(f => f.Size < options.MinFileSize)
            .OrderBy(f => f.Size)
            .Take(options.MaxFilesPerCommit)
            .GroupBy(f => DeltaTable.CanonicalPartitionKey(
                f.PartitionValues, snapshot.Metadata.PartitionColumns, logicalToPhysical, snapshot.Schema))
            .Select(g => g.ToList())
            .Where(g => g.Count >= 2) // a partition with a single small file is not worth compacting
            .ToList();

        if (groups.Count == 0)
            return null;

        // Build target schema for type widening during compaction — EXCLUDING partition columns: per the
        // Delta layout, data files do not carry them (values live in add.partitionValues; readers re-add
        // them). Backfilling them as all-NULL columns wrote junk columns into the compacted file and
        // misaligned the batches against the file's real column set.
        var tableSchema = DeltaSchemaSerializer.Parse(snapshot.Metadata.SchemaString);
        if (snapshot.Metadata.PartitionColumns.Count > 0)
        {
            var partSet = new HashSet<string>(snapshot.Metadata.PartitionColumns, StringComparer.Ordinal);
            tableSchema = new EngineeredWood.DeltaLake.Schema.StructType
            {
                Fields = tableSchema.Fields.Where(f => !partSet.Contains(f.Name)).ToList(),
            };
        }

        // Column mapping: the data files store PHYSICAL column names (both name and id mode) at EVERY depth, and
        // the compacted file must keep them + re-stamp each column's parquet field_id — readers resolve by
        // physicalName/field_id, so a compacted file without them reads as all-NULL. Widening and backfill
        // therefore match on a target schema that is physical all the way down: one physical only at the top
        // level matched no struct child on disk, so a file whose struct lacked a field ADDed later was
        // "reconciled" to all-NULL children and every nested value in the compacted file was lost.
        var targetSchema = SchemaConverter.ToArrowSchema(ColumnMapping.ToPhysicalSchema(tableSchema, mappingMode));

        var dvReader = new DeletionVectors.DeletionVectorReader(fs);
        var actions = new List<DeltaAction>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bool assignRowIds = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsSupported(snapshot.Protocol);
        bool rowTrackingEnabled = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(snapshot.Metadata.Configuration);
        long nextRowId = assignRowIds ? snapshot.RowIdHighWaterMark : 0;

        // Row tracking through compaction: rows from several source files mix into one compacted file, so the
        // compacted add's single baseRowId / defaultRowCommitVersion cannot represent them all. Materialize each
        // surviving row's ORIGINAL id + commit version (from the source's own materialized column, else its
        // baseRowId + physical position / defaultRowCommitVersion) into the declared hidden columns.
        var (matRowIdName, matRowVerName) = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig
            .TryGetMaterializedColumnNames(snapshot.Metadata.Configuration);
        bool materialize = rowTrackingEnabled && matRowIdName is not null && matRowVerName is not null;
        bool anyAdds = false;

        foreach (var group in groups)
        {
            (bool compacted, nextRowId) = await CompactGroupAsync(
                fs, snapshot, options, parquetOptions, parquetReadOptions, group, targetSchema, mappingMode,
                dvReader, actions, now, assignRowIds, nextRowId, materialize, matRowIdName, matRowVerName,
                dataFileWriter, dataFileReader, cancellationToken, written, statsSelection,
                maxConcurrentReads, maxBufferedReadBytes, memoryBudget).ConfigureAwait(false);
            anyAdds |= compacted;
        }

        if (!anyAdds)
            return null;

        // Row tracking: OPTIMIZE assigns fresh row ids to the compacted files, so it advances the
        // delta.rowTracking high-water mark too (same reasoning as the write path).
        if (assignRowIds && nextRowId > snapshot.RowIdHighWaterMark)
        {
            actions.Add(EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig
                .BuildHighWaterMarkAction(nextRowId));
        }

        return new CompactionPlan(actions, assignRowIds);
    }

    /// <summary>
    /// The file's raw batches with the position of each one's first row. A row group <paramref name="deletedRows"/>
    /// deletes in full is skipped unread (#618): none of its rows survive the compaction, and the reader still
    /// counts them towards the positions after it, which the survivors' row ids are derived from.
    /// </summary>
    private static async IAsyncEnumerable<(RecordBatch Batch, long FirstRow)> Positioned(
        ParquetFileReader reader, DeletionVectors.DeletionVectorBitmap? deletedRows,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        RowSkipPredicate? skip = deletedRows is { IsEmpty: false } ? deletedRows.ContainsRange : null;
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, null, skip, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return (positioned.Batch, positioned.FirstRow);
        }
    }

    /// <summary>A pluggable reader's batches, which come whole and in file order, positioned by counting them.</summary>
    private static async IAsyncEnumerable<(RecordBatch Batch, long FirstRow)> InFileOrder(
        IAsyncEnumerable<RecordBatch> source,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long next = 0;
        await foreach (var batch in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return (batch, next);
            next += batch.Length;
        }
    }

    /// <summary>One raw batch of a source file, with what the rewrite needs to know about that file.</summary>
    private readonly record struct SourceBatch(
        AddFile File, DeletionVectors.DeletionVectorBitmap? DeletedRows, RecordBatch Batch, long FirstRow);

    /// <summary>
    /// The raw batches of every file of <paramref name="group"/>, in group order then file order. With
    /// <paramref name="maxConcurrentReads"/> above 1 the files are read that many at once through
    /// <see cref="OrderedScan"/>, holding at most <paramref name="maxBufferedReadBytes"/> (within <paramref name="memoryBudget"/>, when given) of decoded batches the
    /// writer has not taken yet, as a table scan does; with 1, one after another.
    /// </summary>
    private static IAsyncEnumerable<SourceBatch> ReadGroupAsync(
        ITableFileSystem fs,
        IReadOnlyList<AddFile> group,
        DeletionVectors.DeletionVectorReader dvReader,
        ParquetReadOptions parquetReadOptions,
        IDataFileReader? dataFileReader,
        int maxConcurrentReads,
        long maxBufferedReadBytes,
        EngineeredWood.Memory.MemoryBudget? memoryBudget,
        CancellationToken cancellationToken)
    {
        if (maxConcurrentReads <= 1 || group.Count == 1)
            return SeriallyAsync(fs, group, dvReader, parquetReadOptions, dataFileReader, cancellationToken);

        return OrderedScan.RunAsync(
            UnitsAsync(group),
            (addFile, reservation, ct) => ReadSourceAsync(
                fs, addFile, dvReader, parquetReadOptions, dataFileReader, reservation, ct),
            static b => RollingDataFileWriter.EstimatedBytes(b.Batch),
            static b => b.Batch.Dispose(),
            maxConcurrentReads,
            new ScanMemoryBudget(maxBufferedReadBytes, memoryBudget),
            cancellationToken: cancellationToken);
    }

    private static async IAsyncEnumerable<SourceBatch> SeriallyAsync(
        ITableFileSystem fs,
        IReadOnlyList<AddFile> group,
        DeletionVectors.DeletionVectorReader dvReader,
        ParquetReadOptions parquetReadOptions,
        IDataFileReader? dataFileReader,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var addFile in group)
        {
            await foreach (var batch in ReadSourceAsync(
                               fs, addFile, dvReader, parquetReadOptions, dataFileReader, ScanReservation.None,
                               cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

#pragma warning disable CS1998 // an async iterator with nothing to await: the units are already in hand
    private static async IAsyncEnumerable<AddFile> UnitsAsync(IReadOnlyList<AddFile> group)
#pragma warning restore CS1998
    {
        foreach (var addFile in group)
            yield return addFile;
    }

    /// <summary>
    /// One source file's raw batches with their positions, and its deletion vector: what the rewrite loop used
    /// to read inline, now run on the file's own producer when the group is read in parallel. Under a parallel
    /// read the first batch reserves the file's largest row group (over every column, since compaction reads
    /// them all), and each later one what the batch before it measured, as a scan's file does.
    /// </summary>
    private static async IAsyncEnumerable<SourceBatch> ReadSourceAsync(
        ITableFileSystem fs,
        AddFile addFile,
        DeletionVectors.DeletionVectorReader dvReader,
        ParquetReadOptions parquetReadOptions,
        IDataFileReader? dataFileReader,
        ScanReservation reservation,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var deletedRows = addFile.DeletionVector is not null
            ? await dvReader.ReadAsync(addFile.DeletionVector, cancellationToken).ConfigureAwait(false)
            : null;

        // Pluggable read half: raw physical batches in file order (positions drive the DV exclusion) — the same
        // contract as the built-in reader.
        IRandomAccessFile? file = null;
        ParquetFileReader? reader = null;
        try
        {
            IAsyncEnumerable<(RecordBatch Batch, long FirstRow)> rawBatches;
            if (dataFileReader is not null)
            {
                rawBatches = InFileOrder(dataFileReader.ReadAsync(
                    EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), null, cancellationToken), cancellationToken);
            }
            else
            {
                file = await fs.OpenReadAsync(
                    EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), cancellationToken)
                    .ConfigureAwait(false);
                reader = new ParquetFileReader(file, ownsFile: false, parquetReadOptions);
                if (reservation != ScanReservation.None)
                {
                    var footer = await reader.ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
                    reservation.SetInitialEstimate(DeltaTable.LargestRowGroupBytes(footer, null, deletedRows));
                }
                rawBatches = Positioned(reader, deletedRows, cancellationToken);
            }

            await foreach (var (batch, firstRow) in DeltaTable.Reserving(rawBatches, reservation, cancellationToken)
                .ConfigureAwait(false))
            {
                yield return new SourceBatch(addFile, deletedRows, batch, firstRow);
            }
        }
        finally
        {
            reader?.Dispose();
            if (file is not null)
                await file.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Compacts ONE partition group's candidate files (the whole table when unpartitioned): reads the live
    /// rows, appends the group's remove + add actions (adds carry the group's partitionValues and land in
    /// the group's Hive directory), and returns whether anything was compacted plus the advanced
    /// row-tracking id cursor.
    /// </summary>
    private static async ValueTask<(bool Compacted, long NextRowId)> CompactGroupAsync(
        ITableFileSystem fs,
        DeltaSnapshot snapshot,
        CompactionOptions options,
        ParquetWriteOptions parquetOptions,
        ParquetReadOptions parquetReadOptions,
        IReadOnlyList<AddFile> group,
        Apache.Arrow.Schema targetSchema,
        ColumnMappingMode mappingMode,
        DeletionVectors.DeletionVectorReader dvReader,
        List<DeltaAction> actions,
        long now,
        bool assignRowIds,
        long nextRowId,
        bool materialize,
        string? matRowIdName,
        string? matRowVerName,
        IDataFileWriter? dataFileWriter,
        IDataFileReader? dataFileReader,
        CancellationToken cancellationToken,
        WrittenFileLedger? written,
        Stats.StatsColumnSelection statsSelection,
        int maxConcurrentReads,
        long maxBufferedReadBytes,
        EngineeredWood.Memory.MemoryBudget? memoryBudget)
    {
        // The group's Hive directory: one partition = one directory, so the compacted file joins its
        // sources' directory. The add keeps the ENCODED prefix (reused verbatim from a source path, so it
        // is never double-encoded); the physical write path is the decoded form. Empty for an
        // unpartitioned table (files at the table root).
        string encodedDir = "";
        int dirSlash = group[0].Path.LastIndexOf('/');
        if (dirSlash >= 0)
            encodedDir = group[0].Path.Substring(0, dirSlash + 1);
        string physicalDir = EngineeredWood.DeltaLake.DeltaPath.Decode(encodedDir).TrimEnd('/');

        // The live rows stream straight into the output (#572). The rolling writer joins consecutive batches into
        // row groups of up to RowGroupMaxRows rows / RowGroupMaxBytes, however the sources were cut, and closes a
        // file once its ACTUAL size reaches the target. Writing one row group per source batch carried every
        // source file's row groups over unchanged, and cutting files by a bytes-per-row estimate from the sources
        // went wrong whenever they were heavily deleted or encoded differently. Under a host writer the batches
        // go through as they came and the host picks its own row groups. Every file is recorded in the ledger
        // before its first byte, so a compaction that fails part-way takes back what it wrote.
        IReadOnlyList<RolledDataFile> rolled;
        await using (var output = new RollingDataFileWriter(
            fs, dataFileWriter, parquetOptions, options.TargetFileSize, physicalDir, written,
            collectStats: true, statsSelection,
                VariantShreddingConfig.AllowsShreddedWrites(snapshot.Protocol, snapshot.Metadata.Configuration)))
        {
            // Read all LIVE data from the group's files, widening types if needed. A candidate may carry a
            // deletion vector (DELETE marks rows rather than rewriting), and those rows MUST be excluded —
            // compacting the raw parquet would RESURRECT every deleted row. The files are read up to
            // maxConcurrentReads ahead (each open, deletion vector and decode is round trips against an object
            // store), but their batches arrive here in group order, so what is written is what a serial read wrote.
            await foreach (var source in ReadGroupAsync(
                               fs, group, dvReader, parquetReadOptions, dataFileReader,
                               maxConcurrentReads, maxBufferedReadBytes, memoryBudget, cancellationToken).ConfigureAwait(false))
            {
                var (batch, batchStartRow, deletedRows) = (source.Batch, source.FirstRow, source.DeletedRows);
                long baseId = source.File.BaseRowId ?? 0;
                long commitVer = source.File.DefaultRowCommitVersion ?? 0;

                // Strip the hidden materialized row-tracking columns (declared physical names) before widening
                // and column mapping, which expect only user columns. Capture the source's own materialized
                // ids/versions (present when the source was itself a rewrite output).
                var (userBatch, srcMatIds, srcMatVers) = materialize
                    ? RowTracking.RowTrackingWriter.StripMaterializedColumns(batch, matRowIdName, matRowVerName)
                    : (batch, null, null);

                // The rows the deletion vector leaves, computed once from a selection mask: they drive both the
                // filter below and the survivor id/version arrays, which therefore stay aligned with it.
                using var kept = deletedRows is null
                    ? default
                    : DeletionVectors.KeptRows.For(deletedRows, batchStartRow, batch.Length);
                int survivors = deletedRows is null ? batch.Length : kept.Count;
                if (survivors == 0)
                    continue; // every row in this batch was deleted: nothing to map, gather or write

                Int64Array? survivorIds = null, survivorVers = null;
                if (materialize)
                {
                    var idb = new Int64Array.Builder().Reserve(survivors);
                    var vrb = new Int64Array.Builder().Reserve(survivors);
                    for (int n = 0; n < survivors; n++)
                    {
                        int i = deletedRows is null ? n : kept[n];
                        long id = srcMatIds is not null && !srcMatIds.IsNull(i)
                            ? srcMatIds.GetValue(i)!.Value
                            : baseId + batchStartRow + i;
                        long ver = srcMatVers is not null && !srcMatVers.IsNull(i)
                            ? srcMatVers.GetValue(i)!.Value
                            : commitVer;
                        idb.Append(id);
                        vrb.Append(ver);
                    }
                    survivorIds = idb.Build();
                    survivorVers = vrb.Build();
                }

                // The source file's own PARQUET:field_id metadata must not ride into the compacted file: a
                // mapped table re-stamps every id from the schema below, and an unmapped one has none to write.
                // Id mode resolves a file's columns by field id, and its physical names need not be the schema's
                // (a converted table keeps each file's names from when it was written). Everything below matches
                // by NAME, so first map the batch to the schema's own physical names by id: logical by id (which
                // also drops columns whose id the table no longer has), then back to canonical physical.
                if (mappingMode == ColumnMappingMode.Id)
                {
                    userBatch = ColumnMappingRecursive.ToPhysical(
                        ColumnMappingRecursive.ToLogical(userBatch, snapshot.Schema, mappingMode),
                        snapshot.Schema, mappingMode);
                }
                var liveBatch = ColumnMappingRecursive.StripParquetFieldIds(userBatch);
                if (deletedRows is not null && !kept.AllKept)
                    liveBatch = ArrowCompute.Take(liveBatch, liveBatch.Schema, kept.Indices);
                if (liveBatch.Length == 0)
                    continue; // every row in this batch was deleted

                // Widen values from old files to match current schema
                var outBatch = TypeWidening.ValueWidener.WidenBatch(liveBatch, targetSchema);

                // Then reconcile the COLUMN SET. ADD/DROP COLUMN are metadata-only commits, so the files being
                // compacted span vintages with different column sets (one predating an ADD lacks the column,
                // one predating a DROP still carries it). Every batch must be reconciled to the current
                // schema's (physical-named, partition-less) columns — absent ones typed all-NULL, dropped ones
                // removed — because the parquet writer fixes the file's schema on its FIRST row group: a later
                // batch of a different shape either fails the write or lands columns under the wrong names.
                outBatch = SchemaEvolution.BackfillMissingColumns(outBatch, targetSchema.FieldsList);
                if (mappingMode != ColumnMappingMode.None)
                {
                    // Rebuild with a CLEAN schema (drop the reader-carried field metadata, e.g. the file's own
                    // PARQUET:field_id) before re-stamping, then apply the mapping recursively so nested struct
                    // children keep their physical names + ids too. The batch is already physical-named, so it is
                    // matched physical-first (a renamed column's old name may be a newer column's logical name) and
                    // renames nothing — it only stamps the ids.
                    var cleanFields = new List<Field>(outBatch.Schema.FieldsList.Count);
                    foreach (var f in outBatch.Schema.FieldsList)
                        cleanFields.Add(CleanField(f));
                    var cleanArrays = new List<IArrowArray>(outBatch.ColumnCount);
                    for (int c = 0; c < outBatch.ColumnCount; c++)
                        cleanArrays.Add(outBatch.Column(c));
                    outBatch = new RecordBatch(
                        new Apache.Arrow.Schema(cleanFields, null), cleanArrays, outBatch.Length);
                    outBatch = ColumnMappingRecursive.ToPhysical(
                        outBatch, snapshot.Schema, mappingMode, inputIsPhysical: true);
                }

                // Canonical Arrow forms (WriteTypeNormalization). The rows come from several files, each read in
                // whatever form its own reader produced, and one row group cannot mix them; a layout that still
                // differs would start a new output file. Not under a host writer: the codec seam is value-blind,
                // and the host owns the physical representation.
                if (dataFileWriter is null)
                    outBatch = WriteTypeNormalization.Normalize(outBatch);
                // Nullability from the table schema, so sources whose Arrow flags disagree (an EW append's
                // non-nullable id beside a Spark file's all-optional columns) still share a file.
                outBatch = TableNullability.Apply(outBatch, targetSchema.FieldsList);
                // The append path's guard: a foreign TIMESTAMP(NANOS) reads back as a nanosecond column, which no
                // writer can store under a microsecond Delta timestamp — refused under a host writer too.
                SchemaConverter.ThrowIfUnwritableType(outBatch.Schema, convertibleTypesAllowed: dataFileWriter is not null);
                // When materializing, append the ORIGINAL id + commit-version columns to the WRITTEN batch only:
                // the internal columns must not appear in Delta stats, which cover the user columns.
                var writeBatch = materialize
                    ? RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(
                        outBatch, survivorIds!, survivorVers!, matRowIdName!, matRowVerName!)
                    : outBatch;
                await output.AddAsync(writeBatch, outBatch, cancellationToken).ConfigureAwait(false);
            }

            rolled = await output.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }

        if (rolled.Count == 0)
            return (false, nextRowId); // every live row DV-deleted — leave the group's files alone

        // Remove the group's old files (with dataChange: false since this is rearrangement)
        foreach (var oldFile in group)
        {
            actions.Add(new RemoveFile
            {
                Path = oldFile.Path,
                DeletionTimestamp = now,
                DataChange = false,
                ExtendedFileMetadata = true,
                PartitionValues = oldFile.PartitionValues,
                Size = oldFile.Size,
                // Keyed by (path, deletionVector) — a remove omitting the DV leaves the compacted-away file
                // active and duplicates its rows.
                DeletionVector = oldFile.DeletionVector,
                // A remove carries the row-tracking fields of the add it removes (spec; Spark 4.0.1 does).
                BaseRowId = oldFile.BaseRowId,
                DefaultRowCommitVersion = oldFile.DefaultRowCommitVersion,
            });
        }

        // Earliest defaultRowCommitVersion from source files (preserved through compaction)
        long? earliestCommitVersion = group
            .Where(c => c.DefaultRowCommitVersion.HasValue)
            .Select(c => c.DefaultRowCommitVersion!.Value)
            .DefaultIfEmpty(-1)
            .Min();
        if (earliestCommitVersion == -1) earliestCommitVersion = null;
        // A file that got a baseRowId needs a defaultRowCommitVersion too. When no source has one (files
        // written before the table supported row tracking, then restored into it), the compacted file takes
        // this commit's version, as Spark's commit fills a missing one. The rebase moves it with the commit.
        if (assignRowIds && earliestCommitVersion is null)
            earliestCommitVersion = snapshot.Version + 1;

        // The compacted files, in the order they were written: each reserves the next run of row ids.
        foreach (var file in rolled)
        {
            string baseName = file.FileName.Substring(file.FileName.LastIndexOf('/') + 1);
            actions.Add(new AddFile
            {
                Path = encodedDir + baseName,
                PartitionValues = group[0].PartitionValues,
                Size = file.Size,
                ModificationTime = now,
                DataChange = false,
                Stats = file.Stats,
                BaseRowId = assignRowIds ? nextRowId : null,
                DefaultRowCommitVersion = earliestCommitVersion,
            });
            if (assignRowIds)
                nextRowId += file.Rows;
        }

        return (true, nextRowId);
    }
}
