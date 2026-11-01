// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The <c>commitInfo.operationMetrics</c> a DELETE, UPDATE or MERGE records, under Spark's names
/// (delta-spark's <c>DeltaOperationMetrics</c>) and as Spark writes them: every value a string.
/// </summary>
/// <remarks>
/// <para>The file, byte and deletion-vector counts are read off the actions the commit writes, so they are
/// computed again on every attempt and describe what actually landed. The row counts come from the operation,
/// apart from the copied rows of a DELETE or UPDATE, which are the rows of the files it added that it did not
/// change, by those files' <c>numRecords</c>.</para>
///
/// <para>An <c>add</c> and a <c>remove</c> of the same path are one file given a new deletion vector, not a file
/// removed and another added, which is how Spark counts a deletion-vector DELETE: no file added or removed,
/// one vector added (or updated, when the file already had one).</para>
///
/// <para>Spark's timing metrics (<c>executionTimeMs</c>, <c>scanTimeMs</c>, <c>rewriteTimeMs</c>, and MERGE's
/// <c>materializeSourceTimeMs</c>) are not recorded.</para>
/// </remarks>
internal static class DmlOperationMetrics
{
    /// <summary>The counts read off a commit's actions.</summary>
    private readonly record struct FileCounts(
        long AddedFiles,
        long RemovedFiles,
        long AddedChangeFiles,
        long AddedBytes,
        long RemovedBytes,
        long DeletionVectorsAdded,
        long DeletionVectorsRemoved,
        long DeletionVectorsUpdated,
        long? RowsInAddedFiles);

    private static FileCounts Count(IReadOnlyList<DeltaAction> actions)
    {
        var removes = new Dictionary<string, RemoveFile>(StringComparer.Ordinal);
        foreach (var action in actions)
        {
            if (action is RemoveFile { DataChange: true } remove)
                removes[remove.Path] = remove;
        }

        long addedFiles = 0, addedChangeFiles = 0, addedBytes = 0, removedBytes = 0;
        long dvAdded = 0, dvRemoved = 0, dvUpdated = 0;
        long? rowsInAdded = 0;
        var readded = new HashSet<string>(StringComparer.Ordinal);
        foreach (var action in actions)
        {
            switch (action)
            {
                case AddFile { DataChange: true } add when removes.TryGetValue(add.Path, out var replaced):
                    // The same file under a new deletion vector.
                    readded.Add(add.Path);
                    if (add.DeletionVector is not null)
                    {
                        if (replaced.DeletionVector is null)
                            dvAdded++;
                        else
                            dvUpdated++;
                    }
                    else if (replaced.DeletionVector is not null)
                    {
                        dvRemoved++;
                    }
                    break;
                case AddFile { DataChange: true } add:
                    addedFiles++;
                    addedBytes += add.Size;
                    rowsInAdded = add.GetNumRecords() is { } records ? rowsInAdded + records : null;
                    if (add.DeletionVector is not null)
                        dvAdded++;
                    break;
                case CdcFile:
                    addedChangeFiles++;
                    break;
            }
        }

        long removedFiles = 0;
        foreach (var remove in removes.Values)
        {
            if (readded.Contains(remove.Path))
                continue;
            removedFiles++;
            removedBytes += remove.Size ?? 0;
            if (remove.DeletionVector is not null)
                dvRemoved++;
        }

        return new FileCounts(
            addedFiles, removedFiles, addedChangeFiles, addedBytes, removedBytes, dvAdded, dvRemoved, dvUpdated,
            rowsInAdded);
    }

    /// <summary>A DELETE's metrics. Its added files hold only rows it kept, so all of them are copied rows.</summary>
    public static IReadOnlyDictionary<string, string> ForDelete(IReadOnlyList<DeltaAction> actions, long deletedRows)
    {
        var c = Count(actions);
        var metrics = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["numDeletedRows"] = Text(deletedRows),
        };
        if (c.RowsInAddedFiles is { } copied)
            metrics["numCopiedRows"] = Text(copied);
        AddFileCounts(metrics, c);
        return metrics;
    }

    /// <summary>An UPDATE's metrics. Its added files hold the rows it changed and the rows it copied.</summary>
    public static IReadOnlyDictionary<string, string> ForUpdate(IReadOnlyList<DeltaAction> actions, long updatedRows)
    {
        var c = Count(actions);
        var metrics = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["numUpdatedRows"] = Text(updatedRows),
        };
        if (c.RowsInAddedFiles is { } rows)
            metrics["numCopiedRows"] = Text(Math.Max(0, rows - updatedRows));
        AddFileCounts(metrics, c);
        return metrics;
    }

    /// <summary>A MERGE's metrics: its own row counts, and the file counts of what it committed.</summary>
    public static IReadOnlyDictionary<string, string> ForMerge(IReadOnlyList<DeltaAction> actions, MergeMetrics m)
    {
        var c = Count(actions);
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["numSourceRows"] = Text(m.NumSourceRows),
            ["numTargetRowsInserted"] = Text(m.NumTargetRowsInserted),
            ["numTargetRowsUpdated"] = Text(m.NumTargetRowsUpdated),
            ["numTargetRowsMatchedUpdated"] = Text(m.NumTargetRowsMatchedUpdated),
            // This library's not-matched-by-source clause can only delete.
            ["numTargetRowsNotMatchedBySourceUpdated"] = "0",
            ["numTargetRowsDeleted"] = Text(m.NumTargetRowsDeleted),
            ["numTargetRowsMatchedDeleted"] = Text(m.NumTargetRowsMatchedDeleted),
            ["numTargetRowsNotMatchedBySourceDeleted"] = Text(m.NumTargetRowsNotMatchedBySourceDeleted),
            ["numTargetRowsCopied"] = Text(m.NumTargetRowsCopied),
            ["numOutputRows"] = Text(m.NumOutputRows),
            ["numTargetFilesAdded"] = Text(c.AddedFiles),
            ["numTargetFilesRemoved"] = Text(c.RemovedFiles),
            ["numTargetChangeFilesAdded"] = Text(c.AddedChangeFiles),
            ["numTargetBytesAdded"] = Text(c.AddedBytes),
            ["numTargetBytesRemoved"] = Text(c.RemovedBytes),
            ["numTargetDeletionVectorsAdded"] = Text(c.DeletionVectorsAdded),
            ["numTargetDeletionVectorsRemoved"] = Text(c.DeletionVectorsRemoved),
            ["numTargetDeletionVectorsUpdated"] = Text(c.DeletionVectorsUpdated),
        };
    }

    /// <summary>Adds several operations' metrics key by key, for a transaction that staged more than one.</summary>
    public static MergeMetrics Sum(MergeMetrics a, MergeMetrics b) => new(
        a.NumSourceRows + b.NumSourceRows,
        a.NumTargetRowsInserted + b.NumTargetRowsInserted,
        a.NumTargetRowsUpdated + b.NumTargetRowsUpdated,
        a.NumTargetRowsMatchedDeleted + b.NumTargetRowsMatchedDeleted,
        a.NumTargetRowsNotMatchedBySourceDeleted + b.NumTargetRowsNotMatchedBySourceDeleted,
        a.NumTargetRowsCopied + b.NumTargetRowsCopied,
        a.NumTargetFilesAdded + b.NumTargetFilesAdded,
        a.NumTargetFilesRemoved + b.NumTargetFilesRemoved);

    private static void AddFileCounts(Dictionary<string, string> metrics, FileCounts c)
    {
        metrics["numAddedFiles"] = Text(c.AddedFiles);
        metrics["numRemovedFiles"] = Text(c.RemovedFiles);
        metrics["numAddedChangeFiles"] = Text(c.AddedChangeFiles);
        metrics["numAddedBytes"] = Text(c.AddedBytes);
        metrics["numRemovedBytes"] = Text(c.RemovedBytes);
        metrics["numDeletionVectorsAdded"] = Text(c.DeletionVectorsAdded);
        metrics["numDeletionVectorsRemoved"] = Text(c.DeletionVectorsRemoved);
        metrics["numDeletionVectorsUpdated"] = Text(c.DeletionVectorsUpdated);
    }

    private static string Text(long value) => value.ToString(CultureInfo.InvariantCulture);
}
