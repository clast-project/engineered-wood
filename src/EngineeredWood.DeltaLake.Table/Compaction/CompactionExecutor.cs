// Copyright (c) Curt Hagenlocher. All rights reserved.
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
        CancellationToken cancellationToken)
    {
        // Select small files as compaction candidates
        var candidates = snapshot.ActiveFiles.Values
            .Where(f => f.Size < options.MinFileSize)
            .OrderBy(f => f.Size)
            .Take(options.MaxFilesPerCommit)
            .ToList();

        if (candidates.Count < 2)
            return null; // Not worth compacting a single file

        // Build target schema for type widening during compaction
        var targetSchema = SchemaConverter.ToArrowSchema(
            DeltaSchemaSerializer.Parse(snapshot.Metadata.SchemaString));

        // Column mapping: the data files store PHYSICAL column names (both name and id mode), and the compacted
        // file must keep them + re-stamp each column's parquet field_id — readers resolve by
        // physicalName/field_id, so a compacted file without them reads as all-NULL. Widening therefore has to
        // match on the physical-renamed target schema (the logical-named one matches nothing on disk, which
        // silently skipped widening under mapping).
        var mappingMode = ColumnMapping.GetMode(snapshot.Metadata.Configuration);
        if (mappingMode != ColumnMappingMode.None)
        {
            var logicalToPhysical = ColumnMapping.BuildLogicalToPhysicalMap(snapshot.Schema, mappingMode);
            var physFields = new List<Field>(targetSchema.FieldsList.Count);
            foreach (var f in targetSchema.FieldsList)
            {
                physFields.Add(logicalToPhysical.TryGetValue(f.Name, out var p) && p != f.Name
                    ? new Field(p, f.DataType, f.IsNullable)
                    : f);
            }
            targetSchema = new Apache.Arrow.Schema(physFields, null);
        }

        // Read all data from candidate files, widening types if needed
        var allBatches = new List<RecordBatch>();
        foreach (var addFile in candidates)
        {
            await using var file = await fs.OpenReadAsync(
                EngineeredWood.DeltaLake.DeltaPath.Decode(addFile.Path), cancellationToken)
                .ConfigureAwait(false);
            using var reader = new ParquetFileReader(file, ownsFile: false, parquetReadOptions);

            await foreach (var batch in reader.ReadAllAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                // Widen values from old files to match current schema
                var outBatch = TypeWidening.ValueWidener.WidenBatch(batch, targetSchema);
                if (mappingMode != ColumnMappingMode.None)
                {
                    // Rebuild with a CLEAN schema (drop the reader-carried field metadata, e.g. the file's own
                    // PARQUET:field_id) before re-stamping, then apply the mapping recursively so nested struct
                    // children keep their physical names + ids too. The batch is already physical-named, so the
                    // tolerant matching renames nothing — it only stamps the ids.
                    var cleanFields = new List<Field>(outBatch.Schema.FieldsList.Count);
                    foreach (var f in outBatch.Schema.FieldsList)
                        cleanFields.Add(new Field(f.Name, f.DataType, f.IsNullable));
                    var cleanArrays = new List<IArrowArray>(outBatch.ColumnCount);
                    for (int c = 0; c < outBatch.ColumnCount; c++)
                        cleanArrays.Add(outBatch.Column(c));
                    outBatch = new RecordBatch(
                        new Apache.Arrow.Schema(cleanFields, null), cleanArrays, outBatch.Length);
                    outBatch = ColumnMappingRecursive.ToPhysical(outBatch, snapshot.Schema, mappingMode);
                }
                allBatches.Add(outBatch);
            }
        }

        if (allBatches.Count == 0)
            return null;

        // Write compacted data into new files
        var actions = new List<DeltaAction>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Remove old files (with dataChange: false since this is rearrangement)
        foreach (var oldFile in candidates)
        {
            actions.Add(new RemoveFile
            {
                Path = oldFile.Path,
                DeletionTimestamp = now,
                DataChange = false,
                ExtendedFileMetadata = true,
                PartitionValues = oldFile.PartitionValues,
                Size = oldFile.Size,
            });
        }

        // Row tracking state
        bool rowTrackingEnabled = EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.IsEnabled(
            snapshot.Metadata.Configuration);
        long nextRowId = rowTrackingEnabled ? snapshot.RowIdHighWaterMark : 0;

        // Earliest defaultRowCommitVersion from source files (preserved through compaction)
        long? earliestCommitVersion = candidates
            .Where(c => c.DefaultRowCommitVersion.HasValue)
            .Select(c => c.DefaultRowCommitVersion!.Value)
            .DefaultIfEmpty(-1)
            .Min();
        if (earliestCommitVersion == -1) earliestCommitVersion = null;

        // Write new compacted file(s)
        // Group batches to target file size (approximate by row count)
        long totalRows = allBatches.Sum(b => (long)b.Length);
        long totalBytes = candidates.Sum(f => f.Size);
        double bytesPerRow = totalRows > 0 ? (double)totalBytes / totalRows : 0;
        long rowsPerFile = bytesPerRow > 0
            ? Math.Max(1, (long)(options.TargetFileSize / bytesPerRow))
            : totalRows;

        int batchIdx = 0;
        long currentRowCount = 0;
        var currentBatches = new List<RecordBatch>();

        while (batchIdx < allBatches.Count)
        {
            currentBatches.Add(allBatches[batchIdx]);
            currentRowCount += allBatches[batchIdx].Length;
            batchIdx++;

            if (currentRowCount >= rowsPerFile || batchIdx == allBatches.Count)
            {
                string fileName = $"{Guid.NewGuid():N}.parquet";
                long fileSize;
                long fileBaseRowId = nextRowId;

                await using (var outFile = await fs.CreateAsync(
                    fileName, cancellationToken: cancellationToken).ConfigureAwait(false))
                {
                    await using var writer = new ParquetFileWriter(
                        outFile, ownsFile: false, parquetOptions);
                    foreach (var batch in currentBatches)
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
                    Path = EngineeredWood.DeltaLake.DeltaPath.Encode(fileName),
                    PartitionValues = candidates[0].PartitionValues,
                    Size = fileSize,
                    ModificationTime = now,
                    DataChange = false,
                    Stats = stats,
                    BaseRowId = rowTrackingEnabled ? fileBaseRowId : null,
                    DefaultRowCommitVersion = earliestCommitVersion,
                });

                currentBatches.Clear();
                currentRowCount = 0;
            }
        }

        // Row tracking: OPTIMIZE assigns fresh row ids to the compacted files, so it advances the
        // delta.rowTracking high-water mark too (same reasoning as the write path).
        if (rowTrackingEnabled && nextRowId > snapshot.RowIdHighWaterMark)
        {
            actions.Add(EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig
                .BuildHighWaterMarkAction(nextRowId));
        }

        // Commit
        long newVersion = snapshot.Version + 1;
        await log.WriteCommitAsync(newVersion, actions, cancellationToken)
            .ConfigureAwait(false);

        return newVersion;
    }
}
