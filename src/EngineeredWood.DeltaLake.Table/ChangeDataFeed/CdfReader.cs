// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.ChangeDataFeed;

/// <summary>
/// Reads Change Data Feed (CDC) to return row-level changes between versions.
/// For each version, if CDC files exist they are used; otherwise changes are
/// inferred from add/remove actions.
/// </summary>
internal static class CdfReader
{
    /// <summary>
    /// Reads changes from <paramref name="startVersion"/> to <paramref name="endVersion"/> (inclusive).
    /// Each batch includes <c>_change_type</c>, <c>_commit_version</c>, and <c>_commit_timestamp</c> columns.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadChangesAsync(
        ITableFileSystem fs,
        TransactionLog log,
        long startVersion,
        long endVersion,
        ParquetReadOptions? readOptions,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
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
                        fs, cdcFile, version, commitTimestamp, readOptions,
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

                // Removed files → "delete" rows
                foreach (var remove in removes)
                {
                    await foreach (var batch in ReadDataFileAsChangesAsync(
                        fs, remove.Path, CdfConfig.Delete, version,
                        commitTimestamp, readOptions, cancellationToken)
                        .ConfigureAwait(false))
                    {
                        yield return batch;
                    }
                }

                // Added files → "insert" rows
                foreach (var add in adds)
                {
                    await foreach (var batch in ReadDataFileAsChangesAsync(
                        fs, add.Path, CdfConfig.Insert, version,
                        commitTimestamp, readOptions, cancellationToken)
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
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var file = await fs.OpenReadAsync(
            EngineeredWood.DeltaLake.DeltaPath.Decode(cdcFile.Path), cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        await foreach (var batch in reader.ReadAllAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            // CDC files already have _change_type — add version and timestamp columns
            yield return AddMetadataColumns(batch, commitVersion, commitTimestamp);
        }
    }

    private static async IAsyncEnumerable<RecordBatch> ReadDataFileAsChangesAsync(
        ITableFileSystem fs,
        string path,
        string changeType,
        long commitVersion,
        long? commitTimestamp,
        ParquetReadOptions? readOptions,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // path is an add.path (URL-encoded on-disk relative path) — decode to the on-disk name.
        string diskPath = EngineeredWood.DeltaLake.DeltaPath.Decode(path);

        // Try to read the file — it may have been deleted already
        if (!await fs.ExistsAsync(diskPath, cancellationToken).ConfigureAwait(false))
            yield break;

        await using var file = await fs.OpenReadAsync(diskPath, cancellationToken)
            .ConfigureAwait(false);
        using var reader = new ParquetFileReader(file, ownsFile: false, readOptions);

        await foreach (var batch in reader.ReadAllAsync(
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            // Strip _change_type column if present in the data file
            var cleanBatch = StripChangeTypeColumn(batch);

            // Add _change_type, _commit_version, _commit_timestamp
            var withChangeType = CdfWriter.AddChangeTypeColumn(cleanBatch, changeType);
            yield return AddMetadataColumns(withChangeType, commitVersion, commitTimestamp);
        }
    }

    private static RecordBatch AddMetadataColumns(
        RecordBatch batch, long commitVersion, long? commitTimestamp)
    {
        var versionBuilder = new Int64Array.Builder();
        var timestampBuilder = new Int64Array.Builder();

        for (int i = 0; i < batch.Length; i++)
        {
            versionBuilder.Append(commitVersion);
            if (commitTimestamp.HasValue)
                timestampBuilder.Append(commitTimestamp.Value);
            else
                timestampBuilder.AppendNull();
        }

        var columns = new IArrowArray[batch.ColumnCount + 2];
        var fields = new List<Field>(batch.ColumnCount + 2);

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            columns[i] = batch.Column(i);
            fields.Add(batch.Schema.FieldsList[i]);
        }

        columns[batch.ColumnCount] = versionBuilder.Build();
        fields.Add(new Field(CdfConfig.CommitVersionColumn, Int64Type.Default, false));

        columns[batch.ColumnCount + 1] = timestampBuilder.Build();
        fields.Add(new Field(CdfConfig.CommitTimestampColumn, Int64Type.Default, true));

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
