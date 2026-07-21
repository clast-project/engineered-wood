// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using DeltaSnapshot = EngineeredWood.DeltaLake.Snapshot.Snapshot;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Compaction;

/// <summary>
/// Executes file compaction: reads small files, rewrites them as larger files,
/// and commits the add/remove actions.
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
    /// Selects files eligible for compaction and rewrites them.
    /// Returns the new version number, or null if no files were compacted.
    /// </summary>
    public static async ValueTask<long?> ExecuteAsync(
        ITableFileSystem fs,
        TransactionLog log,
        DeltaSnapshot snapshot,
        CompactionOptions options,
        ParquetWriteOptions parquetOptions,
        ParquetReadOptions parquetReadOptions,
        CancellationToken cancellationToken,
        IDataFileWriter? dataFileWriter = null,
        IDataFileReader? dataFileReader = null)
    {
        // Select small files as compaction candidates and group them BY PARTITION: a data file belongs to
        // exactly ONE partition (its add.partitionValues), so each group must compact independently. Mixing
        // partitions into one file stamped with a single partition's values silently corrupted the partition
        // column of every other row. Unpartitioned tables form a single group (behaviour unchanged).
        // Canonical keys tolerate mixed logical/physical partitionValues vintages under column mapping.
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        var logicalToPhysical = mappingMode != ColumnMappingMode.None
            ? ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode)
            : null;
        var groups = snapshot.ActiveFiles.Values
            .Where(f => f.Size < options.MinFileSize)
            .OrderBy(f => f.Size)
            .Take(options.MaxFilesPerCommit)
            .GroupBy(f => DeltaTable.CanonicalPartitionKey(f.PartitionValues, logicalToPhysical))
            .Select(g => g.ToList())
            .Where(g => g.Count >= 2) // a partition with a single small file is not worth compacting
            .ToList();

        if (groups.Count == 0)
            return null;

        // Build target schema for type widening during compaction — EXCLUDING partition columns: per the
        // Delta layout, data files do not carry them (values live in add.partitionValues; readers re-add
        // them). Backfilling them as all-NULL columns wrote junk columns into the compacted file and
        // misaligned the batches against the file's real column set.
        var targetSchema = SchemaConverter.ToArrowSchema(
            DeltaSchemaSerializer.Parse(snapshot.Metadata.SchemaString));
        if (snapshot.Metadata.PartitionColumns.Count > 0)
        {
            var partSet = new HashSet<string>(snapshot.Metadata.PartitionColumns, StringComparer.Ordinal);
            targetSchema = new Apache.Arrow.Schema(
                targetSchema.FieldsList.Where(f => !partSet.Contains(f.Name)).ToList(), null);
        }

        // Column mapping: the data files store PHYSICAL column names (both name and id mode), and the compacted
        // file must keep them + re-stamp each column's parquet field_id — readers resolve by
        // physicalName/field_id, so a compacted file without them reads as all-NULL. Widening therefore has to
        // match on the physical-renamed target schema (the logical-named one matches nothing on disk, which
        // silently skipped widening under mapping).
        if (mappingMode != ColumnMappingMode.None)
        {
            var physFields = new List<Field>(targetSchema.FieldsList.Count);
            foreach (var f in targetSchema.FieldsList)
            {
                physFields.Add(logicalToPhysical!.TryGetValue(f.Name, out var p) && p != f.Name
                    ? new Field(p, f.DataType, f.IsNullable)
                    : f);
            }
            targetSchema = new Apache.Arrow.Schema(physFields, null);
        }

        var dvReader = new DeletionVectors.DeletionVectorReader(fs);
        var actions = new List<DeltaAction>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bool rowTrackingEnabled = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

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
                dvReader, actions, now, rowTrackingEnabled, nextRowId, materialize, matRowIdName, matRowVerName,
                dataFileWriter, dataFileReader, cancellationToken).ConfigureAwait(false);
            anyAdds |= compacted;
        }

        if (!anyAdds)
            return null;

        // Row tracking: OPTIMIZE assigns fresh row ids to the compacted files, so it advances the
        // delta.rowTracking high-water mark too (same reasoning as the write path).
        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
        {
            actions.Add(EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig
                .BuildHighWaterMarkAction(nextRowId));
        }

        // Commit — with the always-on commitInfo (operation + timestamp) every other commit path writes.
        long newVersion = snapshot.Version + 1;
        var commitActions = InCommitTimestamp.EnsureCommitInfo(
            actions, snapshot.Metadata.Configuration, "OPTIMIZE");
        await log.WriteCommitAsync(newVersion, commitActions, cancellationToken)
            .ConfigureAwait(false);

        return newVersion;
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
        bool rowTrackingEnabled,
        long nextRowId,
        bool materialize,
        string? matRowIdName,
        string? matRowVerName,
        IDataFileWriter? dataFileWriter,
        IDataFileReader? dataFileReader,
        CancellationToken cancellationToken)
    {
        // Read all LIVE data from the group's files, widening types if needed. A candidate may carry a
        // deletion vector (DELETE marks rows rather than rewriting), and those rows MUST be excluded —
        // compacting the raw parquet would RESURRECT every deleted row.
        var allBatches = new List<RecordBatch>();
        var batchIds = materialize ? new List<Int64Array>() : null;   // aligned 1:1 with allBatches
        var batchVers = materialize ? new List<Int64Array>() : null;  // aligned 1:1 with allBatches
        foreach (var addFile in group)
        {
            var deletedRows = addFile.DeletionVector is not null
                ? await dvReader.ReadAsync(addFile.DeletionVector, cancellationToken).ConfigureAwait(false)
                : null;
            long baseId = addFile.BaseRowId ?? 0;
            long commitVer = addFile.DefaultRowCommitVersion ?? 0;

            // Pluggable read half: raw physical batches in file order (positions drive the DV exclusion
            // below) — the same contract as the built-in reader.
            IRandomAccessFile? file = null;
            ParquetFileReader? reader = null;
            IAsyncEnumerable<RecordBatch> rawBatches;
            if (dataFileReader is not null)
            {
                rawBatches = dataFileReader.ReadAsync(
                    EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), null, cancellationToken);
            }
            else
            {
                file = await fs.OpenReadAsync(
                    EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), cancellationToken)
                    .ConfigureAwait(false);
                reader = new ParquetFileReader(file, ownsFile: false, parquetReadOptions);
                rawBatches = reader.ReadAllAsync(cancellationToken: cancellationToken);
            }

            try
            {
            long batchStartRow = 0;
            await foreach (var batch in rawBatches.ConfigureAwait(false))
            {
                // Strip the hidden materialized row-tracking columns (declared physical names) before widening
                // and column mapping, which expect only user columns. Capture the source's own materialized
                // ids/versions (present when the source was itself a rewrite output).
                var (userBatch, srcMatIds, srcMatVers) = materialize
                    ? RowTracking.RowTrackingWriter.StripMaterializedColumns(batch, matRowIdName, matRowVerName)
                    : (batch, null, null);
                long physicalRows = batch.Length;

                // Build the survivor id/version arrays in the SAME order DeletionVectorFilter keeps (ascending
                // physical index, skipping DV-deleted rows) so they stay aligned with the filtered batch.
                Int64Array? survivorIds = null, survivorVers = null;
                if (materialize)
                {
                    var idb = new Int64Array.Builder();
                    var vrb = new Int64Array.Builder();
                    for (int i = 0; i < physicalRows; i++)
                    {
                        if (deletedRows is not null && deletedRows.Contains(batchStartRow + i))
                            continue;
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

                var liveBatch = userBatch;
                if (deletedRows is not null)
                {
                    liveBatch = DeletionVectors.DeletionVectorFilter.Filter(
                        liveBatch, deletedRows, batchStartRow);
                }
                batchStartRow += physicalRows;
                if (liveBatch.Length == 0)
                    continue; // every row in this batch was deleted

                // Widen values from old files to match current schema
                var outBatch = TypeWidening.ValueWidener.WidenBatch(liveBatch, targetSchema);
                if (mappingMode != ColumnMappingMode.None)
                {
                    // Rebuild with a CLEAN schema (drop the reader-carried field metadata, e.g. the file's own
                    // PARQUET:field_id) before re-stamping, then apply the mapping recursively so nested struct
                    // children keep their physical names + ids too. The batch is already physical-named, so the
                    // tolerant matching renames nothing — it only stamps the ids.
                    var cleanFields = new List<Field>(outBatch.Schema.FieldsList.Count);
                    foreach (var f in outBatch.Schema.FieldsList)
                        cleanFields.Add(CleanField(f));
                    var cleanArrays = new List<IArrowArray>(outBatch.ColumnCount);
                    for (int c = 0; c < outBatch.ColumnCount; c++)
                        cleanArrays.Add(outBatch.Column(c));
                    outBatch = new RecordBatch(
                        new Apache.Arrow.Schema(cleanFields, null), cleanArrays, outBatch.Length);
                    outBatch = ColumnMappingRecursive.ToPhysical(outBatch, snapshot.Schema, mappingMode);
                }
                allBatches.Add(outBatch);
                if (materialize)
                {
                    batchIds!.Add(survivorIds!);
                    batchVers!.Add(survivorVers!);
                }
            }
            }
            finally
            {
                reader?.Dispose();
                if (file is not null)
                {
                    await file.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        if (allBatches.Count == 0)
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
            });
        }

        // Earliest defaultRowCommitVersion from source files (preserved through compaction)
        long? earliestCommitVersion = group
            .Where(c => c.DefaultRowCommitVersion.HasValue)
            .Select(c => c.DefaultRowCommitVersion!.Value)
            .DefaultIfEmpty(-1)
            .Min();
        if (earliestCommitVersion == -1) earliestCommitVersion = null;

        // The group's Hive directory: one partition = one directory, so the compacted file joins its
        // sources' directory. The add keeps the ENCODED prefix (reused verbatim from a source path, so it
        // is never double-encoded); the physical write path is the decoded form. Empty for an
        // unpartitioned table (files at the table root).
        string encodedDir = "";
        int dirSlash = group[0].Path.LastIndexOf('/');
        if (dirSlash >= 0)
            encodedDir = group[0].Path.Substring(0, dirSlash + 1);
        string physicalDir = EngineeredWood.DeltaLake.DeltaPath.Decode(encodedDir);

        // Write new compacted file(s)
        // Group batches to target file size (approximate by row count)
        long totalRows = allBatches.Sum(b => (long)b.Length);
        long totalBytes = group.Sum(f => f.Size);
        double bytesPerRow = totalRows > 0 ? (double)totalBytes / totalRows : 0;
        long rowsPerFile = bytesPerRow > 0
            ? Math.Max(1, (long)(options.TargetFileSize / bytesPerRow))
            : totalRows;

        int batchIdx = 0;
        long currentRowCount = 0;
        var currentBatches = new List<RecordBatch>();   // USER columns only (stats collected over these)
        var currentWrite = new List<RecordBatch>();     // what gets written (== currentBatches unless materialize)

        while (batchIdx < allBatches.Count)
        {
            var addBatch = allBatches[batchIdx];
            currentBatches.Add(addBatch);
            // When materializing, append the ORIGINAL id + commit-version columns to the WRITTEN batch (the
            // internal columns must not appear in Delta stats, which cover the user columns only).
            currentWrite.Add(materialize
                ? RowTracking.RowTrackingWriter.AddRowIdAndCommitVersionColumns(
                    addBatch, batchIds![batchIdx], batchVers![batchIdx], matRowIdName!, matRowVerName!)
                : addBatch);
            currentRowCount += addBatch.Length;
            batchIdx++;

            if (currentRowCount >= rowsPerFile || batchIdx == allBatches.Count)
            {
                string baseName = $"{Guid.NewGuid():N}.parquet";
                string fileName = physicalDir + baseName;
                long fileSize;
                long fileBaseRowId = nextRowId;

                if (dataFileWriter is not null)
                {
                    // Keep the host codec's output quality (bloom filters, stats, footer) through an OPTIMIZE
                    // instead of reverting to the built-in writer for compacted files.
                    fileSize = await dataFileWriter.WriteAsync(
                        currentWrite.ToAsyncEnumerable(), fileName, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await using var outFile = await fs.CreateAsync(
                        fileName, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await using var writer = new ParquetFileWriter(
                        outFile, ownsFile: false, parquetOptions);
                    foreach (var batch in currentWrite)
                    {
                        await writer.WriteRowGroupAsync(batch, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    await writer.DisposeAsync().ConfigureAwait(false);
                    fileSize = outFile.Position;
                }

                if (rowTrackingEnabled)
                    nextRowId += currentRowCount;

                string? stats = Stats.StatsCollector.Collect(currentBatches);

                actions.Add(new AddFile
                {
                    Path = encodedDir + baseName,
                    PartitionValues = group[0].PartitionValues,
                    Size = fileSize,
                    ModificationTime = now,
                    DataChange = false,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = earliestCommitVersion,
                });

                currentBatches.Clear();
                currentWrite.Clear();
                currentRowCount = 0;
            }
        }

        return (true, nextRowId);
    }
}
