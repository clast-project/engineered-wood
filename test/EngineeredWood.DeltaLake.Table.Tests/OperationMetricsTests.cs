// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>commitInfo.operationMetrics</c> for DELETE, UPDATE and MERGE (#577), on every path that commits one, under
/// Spark's names (<see cref="Interop.OperationMetricsInteropTests"/> compares them with Spark's own). The table
/// starts as two files, (1, 2, 3) and (4, 5).
/// </summary>
public class OperationMetricsTests : IDisposable
{
    private readonly string _tempDir;

    public OperationMetricsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_opmetrics_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        var value = new StringArray.Builder();
        foreach (long i in ids)
        {
            id.Append(i);
            value.Append($"v{i}");
        }
        return new RecordBatch(TableSchema, [id.Build(), value.Build()], ids.Length);
    }

    private static RecordBatch Renamed(RecordBatch batch)
    {
        var value = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            value.Append("x");
        return new RecordBatch(batch.Schema, [batch.Column(0), value.Build()], batch.Length);
    }

    private async Task<DeltaTable> CreateAsync(bool deletionVectors = false, bool changeDataFeed = false)
    {
        var configuration = new Dictionary<string, string>();
        if (changeDataFeed)
            configuration["delta.enableChangeDataFeed"] = "true";
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema, new DeltaTableOptions { CheckpointInterval = 0 },
            enableDeletionVectors: deletionVectors, configuration: configuration);
        await table.WriteAsync([Rows(1, 2, 3)]);
        await table.WriteAsync([Rows(4, 5)]);
        return table;
    }

    private static async Task<IReadOnlyDictionary<string, string>?> MetricsAsync(DeltaTable table, long version)
    {
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version == version)
                return entry.OperationMetrics;
        }
        throw new InvalidOperationException($"No version {version}.");
    }

    private static void AssertCounts(IReadOnlyDictionary<string, string>? metrics, params (string Key, long Value)[] expected)
    {
        Assert.NotNull(metrics);
        foreach (var (key, value) in expected)
            Assert.True(metrics!.TryGetValue(key, out string? actual) && actual == value.ToString(),
                $"{key}: expected {value}, got {(actual ?? "nothing")}");
    }

    private static void AssertBytes(IReadOnlyDictionary<string, string>? metrics, string key, bool nonZero)
    {
        Assert.True(long.TryParse(metrics![key], out long bytes), $"{key} is '{metrics[key]}'");
        Assert.Equal(nonZero, bytes > 0);
    }

    /// <summary>
    /// A copy-on-write DELETE removes the file it emptied and the one it rewrote, and copies the rewritten file's
    /// survivors into the one file it adds.
    /// </summary>
    [Fact]
    public async Task ACopyOnWriteDelete_CountsRowsAndFiles()
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.DeleteAsync(Ex.In("id", LiteralValue.Of(2L), LiteralValue.Of(4L), LiteralValue.Of(5L)));

        var metrics = await MetricsAsync(table, version);
        AssertCounts(metrics,
            ("numDeletedRows", 3), ("numCopiedRows", 2), ("numAddedFiles", 1), ("numRemovedFiles", 2),
            ("numAddedChangeFiles", 0), ("numDeletionVectorsAdded", 0), ("numDeletionVectorsRemoved", 0),
            ("numDeletionVectorsUpdated", 0));
        AssertBytes(metrics, "numAddedBytes", nonZero: true);
        AssertBytes(metrics, "numRemovedBytes", nonZero: true);
    }

    /// <summary>
    /// A DELETE that empties a file removes it whole, rewriting nothing, and counts that file's size as removed
    /// bytes, in both modes.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWholeFileDelete_CountsTheRemovedFilesBytes(bool deletionVectors)
    {
        await using var table = await CreateAsync(deletionVectors);
        long size = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.GetNumRecords() == 2).Size;

        var (_, version) = await table.DeleteAsync(Ex.In("id", LiteralValue.Of(4L), LiteralValue.Of(5L)));

        AssertCounts(await MetricsAsync(table, version),
            ("numDeletedRows", 2), ("numCopiedRows", 0), ("numAddedFiles", 0), ("numRemovedFiles", 1),
            ("numRemovedBytes", size), ("numAddedBytes", 0));
    }

    /// <summary>
    /// A deletion-vector DELETE adds no file and removes none for the file it marks: it adds a vector, and a
    /// second DELETE from the same file updates it. Only a file emptied whole counts as removed, with its vector.
    /// </summary>
    [Fact]
    public async Task ADeletionVectorDelete_CountsVectorsNotFiles()
    {
        await using var table = await CreateAsync(deletionVectors: true);

        var (_, first) = await table.DeleteAsync(Ex.Equal("id", 2L));
        AssertCounts(await MetricsAsync(table, first),
            ("numDeletedRows", 1), ("numCopiedRows", 0), ("numAddedFiles", 0), ("numRemovedFiles", 0),
            ("numDeletionVectorsAdded", 1), ("numDeletionVectorsUpdated", 0), ("numDeletionVectorsRemoved", 0));

        var (_, second) = await table.DeleteAsync(Ex.Equal("id", 3L));
        AssertCounts(await MetricsAsync(table, second),
            ("numDeletedRows", 1), ("numDeletionVectorsAdded", 0), ("numDeletionVectorsUpdated", 1));

        var (_, third) = await table.DeleteAsync(Ex.Equal("id", 1L));
        var metrics = await MetricsAsync(table, third);
        AssertCounts(metrics,
            ("numDeletedRows", 1), ("numRemovedFiles", 1), ("numDeletionVectorsRemoved", 1),
            ("numDeletionVectorsAdded", 0), ("numDeletionVectorsUpdated", 0));
        AssertBytes(metrics, "numAddedBytes", nonZero: false);
    }

    /// <summary>
    /// An UPDATE's rewritten file holds the row it changed and the two it copied. On a CDF table it writes its
    /// pre-images and post-images to a change file each.
    /// </summary>
    [Fact]
    public async Task AnUpdate_CountsUpdatedAndCopiedRows()
    {
        await using var table = await CreateAsync(changeDataFeed: true);
        var (_, version) = await table.UpdateAsync(Ex.Equal("id", 3L), Renamed);

        AssertCounts(await MetricsAsync(table, version),
            ("numUpdatedRows", 1), ("numCopiedRows", 2), ("numAddedFiles", 1), ("numRemovedFiles", 1),
            ("numAddedChangeFiles", 2));
    }

    /// <summary>The host-driven DELETE and UPDATE by row position record the same metrics.</summary>
    [Fact]
    public async Task TheRowLevelHostPaths_RecordMetrics()
    {
        await using var table = await CreateAsync();
        string small = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.GetNumRecords() == 2).Path;
        string large = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.GetNumRecords() == 3).Path;

        var (_, deleted) = await table.DeleteRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>> { [small] = [0L] }),
            RowDeleteMode.CopyOnWrite);
        AssertCounts(await MetricsAsync(table, deleted),
            ("numDeletedRows", 1), ("numCopiedRows", 1), ("numAddedFiles", 1), ("numRemovedFiles", 1));

        long updated = await table.UpdateRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>> { [large] = [0L, 2L] }),
            (_, batches, _) => [.. batches.Select(Renamed)]);
        AssertCounts(await MetricsAsync(table, updated),
            ("numUpdatedRows", 2), ("numCopiedRows", 1), ("numAddedFiles", 1), ("numRemovedFiles", 1));
    }

    /// <summary>A MERGE records the counts it returns, under the same names, beside its file counts.</summary>
    [Fact]
    public async Task AMerge_RecordsTheMetricsItReturns()
    {
        await using var table = await CreateAsync();
        var (m, version) = await table.MergeAsync([Rows(2, 6)], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenNotMatched = MergeAction.Insert,
            WhenNotMatchedBySource = MergeAction.Delete,
            WhenNotMatchedBySourceCondition = Ex.Equal(
                UnboundReference.FromParts(["target", "id"]), Ex.Lit(LiteralValue.Of(5L))),
        });

        AssertCounts(await MetricsAsync(table, version),
            ("numSourceRows", m.NumSourceRows), ("numTargetRowsInserted", m.NumTargetRowsInserted),
            ("numTargetRowsUpdated", m.NumTargetRowsUpdated), ("numTargetRowsMatchedUpdated", m.NumTargetRowsUpdated),
            ("numTargetRowsNotMatchedBySourceUpdated", 0), ("numTargetRowsDeleted", m.NumTargetRowsDeleted),
            ("numTargetRowsMatchedDeleted", m.NumTargetRowsMatchedDeleted),
            ("numTargetRowsNotMatchedBySourceDeleted", m.NumTargetRowsNotMatchedBySourceDeleted),
            ("numTargetRowsCopied", m.NumTargetRowsCopied), ("numOutputRows", m.NumOutputRows),
            ("numTargetFilesAdded", m.NumTargetFilesAdded), ("numTargetFilesRemoved", m.NumTargetFilesRemoved));
        // 2 updated and 6 inserted; 5 deleted by source; 1 and 3 copied from the first file, 4 from the second.
        Assert.Equal((2L, 1L, 1L, 1L, 3L), (m.NumSourceRows, m.NumTargetRowsInserted, m.NumTargetRowsUpdated,
            m.NumTargetRowsNotMatchedBySourceDeleted, m.NumTargetRowsCopied));
    }

    /// <summary>
    /// A transaction of two DELETEs sums them. One that also appends, or that records a different operation,
    /// records no metrics: Spark's describe one operation, and those of a DELETE would misdescribe the commit.
    /// </summary>
    [Fact]
    public async Task ATransaction_SumsOneKindOfOperation_AndOtherwiseRecordsNone()
    {
        await using var table = await CreateAsync();

        long summed;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 1L));
            await txn.DeleteAsync(Ex.Equal("id", 4L));
            summed = await txn.CommitAsync();
        }
        AssertCounts(await MetricsAsync(table, summed),
            ("numDeletedRows", 2), ("numCopiedRows", 3), ("numAddedFiles", 2), ("numRemovedFiles", 2));

        long mixed;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 2L));
            await txn.WriteAsync([Rows(9)]);
            mixed = await txn.CommitAsync();
        }
        Assert.Null(await MetricsAsync(table, mixed));

        long renamed;
        await using (var txn = table.StartTransaction())
        {
            txn.Operation = "PURGE";
            await txn.DeleteAsync(Ex.Equal("id", 3L));
            renamed = await txn.CommitAsync();
        }
        Assert.Null(await MetricsAsync(table, renamed));
    }

    /// <summary>
    /// Work staged as raw actions is not one of the operation's counts. A data-changing add staged beside a DELETE
    /// would otherwise turn up as copied rows, and a commitInfo naming another operation is what the commit
    /// records, so DELETE metrics would misdescribe it. Either records none. A raw action that changes no data
    /// leaves the metrics alone.
    /// </summary>
    [Fact]
    public async Task RawStagedWork_TheMetricsCannotDescribe_RecordsNone()
    {
        await using var table = await CreateAsync();

        // A data-changing add of a new file (a copy of an existing one): its rows are no staging call's.
        var donor = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.GetNumRecords() == 2);
        string copy = "raw-" + Path.GetFileName(donor.Path);
        File.Copy(Path.Combine(_tempDir, donor.Path), Path.Combine(_tempDir, copy));
        long withRawAdd;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 1L));
            txn.StageActions([donor with { Path = copy, DataChange = true }]);
            withRawAdd = await txn.CommitAsync();
        }
        Assert.Null(await MetricsAsync(table, withRawAdd));

        long withHostCommitInfo;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 4L));
            txn.StageActions([new Actions.CommitInfo
            {
                Values = new Dictionary<string, JsonElement>
                {
                    ["operation"] = JsonDocument.Parse("\"PURGE\"").RootElement.Clone(),
                },
            }]);
            withHostCommitInfo = await txn.CommitAsync();
        }
        Assert.Null(await MetricsAsync(table, withHostCommitInfo));

        long withTxnId;
        await using (var txn = table.StartTransaction())
        {
            // id 2 is in no file but the first one's rewrite; the raw copy above holds another 4 and 5.
            await txn.DeleteAsync(Ex.Equal("id", 2L));
            txn.StageActions([new Actions.TransactionId { AppId = "app", Version = 1, LastUpdated = 0 }]);
            withTxnId = await txn.CommitAsync();
        }
        AssertCounts(await MetricsAsync(table, withTxnId), ("numDeletedRows", 1));
    }

    /// <summary>Every value is a JSON string, as Spark writes its <c>Map[String, String]</c>; a plain write records none.</summary>
    [Fact]
    public async Task MetricsAreStrings_AndAWriteRecordsNone()
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.DeleteAsync(Ex.Equal("id", 2L));

        string path = Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");
        var info = File.ReadAllLines(path)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .Single(e => e.TryGetProperty("commitInfo", out _))
            .GetProperty("commitInfo");
        var metrics = info.GetProperty("operationMetrics");
        Assert.NotEmpty(metrics.EnumerateObject());
        Assert.All(metrics.EnumerateObject(), p => Assert.Equal(JsonValueKind.String, p.Value.ValueKind));

        Assert.Null(await MetricsAsync(table, 1));
    }
}
