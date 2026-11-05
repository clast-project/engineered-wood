// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A MERGE on a table with deletion vectors (#574): the rows it updates or deletes are masked by the file's deletion
/// vector and the post-images written to a new file, as Spark's MERGE does
/// (<see cref="Interop.MergeDeletionVectorInteropTests"/> compares the two).
/// </summary>
public class MergeDeletionVectorTests : IDisposable
{
    private readonly string _tempDir;

    public MergeDeletionVectorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_merge_dv_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private Task<DeltaTable> CreateAsync(
        bool deletionVectors = true, bool cdf = false, bool rowTracking = false,
        IReadOnlyList<string>? partitionColumns = null)
    {
        var configuration = new Dictionary<string, string>();
        if (cdf)
            configuration[CdfConfig.EnableKey] = "true";
        return DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema, partitionColumns: partitionColumns,
            enableDeletionVectors: deletionVectors, enableRowTracking: rowTracking,
            configuration: configuration).AsTask();
    }

    private static RecordBatch Rows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }
        return new RecordBatch(TableSchema, [ids.Build(), values.Build()], rows.Length);
    }

    private static readonly RecordBatch Five = Rows((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"));

    private static async Task<List<(long Id, string Value)>> ReadAsync(DeltaTable table)
    {
        var rows = new List<(long, string)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, values.GetString(i)));
        }
        return rows.OrderBy(r => r.Item1).ToList();
    }

    /// <summary>Update id 2, delete id 3, insert id 9: the probe Spark's own MERGE was measured on.</summary>
    private static MergeOptions UpsertWithDelete(RowDeleteMode? mode = null) => new()
    {
        Keys = ["id"],
        WhenMatched =
        [
            MergeAction.Delete.When(SparkSqlParser.ParsePredicate("source.value = 'DEL'")),
            MergeAction.Update,
        ],
        WhenNotMatched = [MergeAction.Insert],
        DeleteMode = mode,
    };

    private static readonly RecordBatch Source = Rows((2, "B"), (3, "DEL"), (9, "z"));

    private static readonly List<(long, string)> Merged = [(1, "a"), (2, "B"), (4, "d"), (5, "e"), (9, "z")];

    private async Task<IReadOnlyDictionary<string, string>> LastMetricsAsync(DeltaTable table)
    {
        IReadOnlyDictionary<string, string>? metrics = null;
        long version = -1;
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version > version)
            {
                version = entry.Version;
                metrics = entry.OperationMetrics;
            }
        }
        return metrics!;
    }

    private static Actions.AddFile FileAt(DeltaTable table, string path) =>
        table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Path == path);

    private int FileCount(string pattern) =>
        Directory.GetFiles(_tempDir, pattern, SearchOption.AllDirectories).Length;

    [Fact]
    public async Task OnADeletionVectorTable_TheChangedRowsAreMaskedAndOnlyThePostImagesWritten()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single();

        var (metrics, _) = await table.MergeAsync([Source], UpsertWithDelete());

        Assert.Equal(Merged, await ReadAsync(table));
        var files = table.CurrentSnapshot.ActiveFiles.Values.ToList();
        // The original file stays, masking the updated and the deleted row; a post-image file and an insert file join it.
        var masked = files.Single(f => f.Path == original.Path);
        Assert.Equal(2, masked.DeletionVector!.Cardinality);
        Assert.Contains("\"tightBounds\":false", masked.Stats);
        Assert.Equal(3, files.Count);
        Assert.All(files.Where(f => f.Path != original.Path), f => Assert.Null(f.DeletionVector));

        Assert.Equal(1, metrics.NumTargetRowsMatchedUpdated);
        Assert.Equal(1, metrics.NumTargetRowsMatchedDeleted);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
        Assert.Equal(0, metrics.NumTargetRowsCopied);
        Assert.Equal(0, metrics.NumTargetFilesRemoved);
        Assert.Equal(2, metrics.NumTargetFilesAdded);
        Assert.Equal(2, metrics.NumOutputRows);

        var committed = await LastMetricsAsync(table);
        Assert.Equal("1", committed["numTargetDeletionVectorsAdded"]);
        Assert.Equal("0", committed["numTargetFilesRemoved"]);
        Assert.Equal("2", committed["numTargetFilesAdded"]);
        Assert.Equal("0", committed["numTargetRowsCopied"]);
    }

    [Fact]
    public async Task AFileWhoseEveryLiveRowChanges_IsRemovedWhole()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((2, "b"), (3, "c"))]);
        var touched = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;
        await table.WriteAsync([Rows((1, "a"), (4, "d"), (5, "e"))]);

        var (metrics, _) = await table.MergeAsync([Source], UpsertWithDelete());

        Assert.Equal(Merged, await ReadAsync(table));
        Assert.DoesNotContain(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == touched);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal(1, metrics.NumTargetFilesRemoved);
        Assert.Equal(0, metrics.NumTargetRowsCopied);
        Assert.Equal("0", (await LastMetricsAsync(table))["numTargetDeletionVectorsAdded"]);
    }

    [Fact]
    public async Task AFileWithADeletionVector_HasItUnionedWithTheChangedRows()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(5L)));
        var path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        await table.MergeAsync([Source], UpsertWithDelete());

        Assert.Equal([(1, "a"), (2, "B"), (4, "d"), (9, "z")], await ReadAsync(table));
        Assert.Equal(3, FileAt(table, path).DeletionVector!.Cardinality);
        Assert.Equal("1", (await LastMetricsAsync(table))["numTargetDeletionVectorsUpdated"]);
    }

    [Fact]
    public async Task ARowAnEarlierDeletionVectorHid_IsNotMatched()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2L)));

        // Id 2 is gone, so its source row inserts rather than updating a hidden row.
        var (metrics, _) = await table.MergeAsync([Rows((2, "B"), (4, "D"))], UpsertWithDelete());

        Assert.Equal([(1, "a"), (2, "B"), (3, "c"), (4, "D"), (5, "e")], await ReadAsync(table));
        Assert.Equal(1, metrics.NumTargetRowsInserted);
        Assert.Equal(1, metrics.NumTargetRowsMatchedUpdated);
    }

    [Fact]
    public async Task CopyOnWrite_RewritesTheFileEvenWithDeletionVectors()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        var (metrics, _) = await table.MergeAsync([Source], UpsertWithDelete(RowDeleteMode.CopyOnWrite));

        Assert.Equal(Merged, await ReadAsync(table));
        Assert.DoesNotContain(table.CurrentSnapshot.ActiveFiles.Values, f => f.Path == original);
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal(3, metrics.NumTargetRowsCopied);
        Assert.Equal(1, metrics.NumTargetFilesRemoved);
    }

    [Fact]
    public async Task WithoutDeletionVectors_TheTableIsRewritten()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Five]);

        var (metrics, _) = await table.MergeAsync([Source], UpsertWithDelete());

        Assert.Equal(Merged, await ReadAsync(table));
        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal(3, metrics.NumTargetRowsCopied);
    }

    [Fact]
    public async Task DeletionVectorsAskedForOnATableWithoutThem_AreRefusedAndWriteNothing()
    {
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Five]);
        long version = table.CurrentSnapshot.Version;
        int parquet = FileCount("*.parquet");

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.MergeAsync([Source], UpsertWithDelete(RowDeleteMode.DeletionVector)).AsTask());

        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal(parquet, FileCount("*.parquet"));
        Assert.Equal(0, FileCount("*.bin"));
        Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task DeletionVectorsAskedForOnATableWithoutThem_StillRemoveAWholeFile()
    {
        // No vector is needed when every live row of the file changes, as for a DELETE.
        await using var table = await CreateAsync(deletionVectors: false);
        await table.WriteAsync([Rows((2, "b"), (3, "c"))]);

        await table.MergeAsync([Source], UpsertWithDelete(RowDeleteMode.DeletionVector));

        Assert.Equal([(2, "B"), (9, "z")], await ReadAsync(table));
    }

    [Fact]
    public async Task OnARowTrackingTable_EveryRowKeepsItsStableId()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([Five]);
        var before = await RowTrackingAsync(table);

        await table.MergeAsync([Source], UpsertWithDelete());

        var after = await RowTrackingAsync(table);
        long mergeVersion = table.CurrentSnapshot.Version;
        foreach (long id in new long[] { 1, 2, 4, 5 })
            Assert.Equal(before[id].RowId, after[id].RowId);
        // The updated row's commit version advances; an untouched one keeps its own.
        Assert.Equal(mergeVersion, after[2].Version);
        Assert.Equal(before[1].Version, after[1].Version);
        Assert.Equal(mergeVersion, after[9].Version);
        Assert.Equal(5, after.Values.Select(v => v.RowId).Distinct().Count());
        Assert.DoesNotContain(after[9].RowId, before.Values.Select(v => v.RowId));
    }

    private static async Task<Dictionary<long, (long RowId, long Version)>> RowTrackingAsync(DeltaTable table)
    {
        var result = new Dictionary<long, (long, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rowIds = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var versions = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
            for (int i = 0; i < batch.Length; i++)
                result[ids.GetValue(i)!.Value] = (rowIds.GetValue(i)!.Value, versions.GetValue(i)!.Value);
        }
        return result;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheChangeFeed_IsTheSameAsACopyOnWriteMergesFeed(bool rowTracking)
    {
        await using var table = await CreateAsync(cdf: true, rowTracking: rowTracking);
        await table.WriteAsync([Five]);

        var (_, version) = await table.MergeAsync([Source], UpsertWithDelete());

        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.DeletionVector is not null));
        var changes = new List<(string Type, long Id, string Value)>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((types.GetString(i), ids.GetValue(i)!.Value, values.GetString(i)));
        }

        Assert.Equal(
            [("delete", 3L, "c"), ("insert", 9L, "z"), ("update_postimage", 2L, "B"), ("update_preimage", 2L, "b")],
            changes.OrderBy(c => c.Type, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task OnAPartitionedTable_AnUpdateThatChangesThePartitionMovesOnlyThatRow()
    {
        await using var table = await CreateAsync(partitionColumns: ["value"]);
        await table.WriteAsync([Rows((1, "p"), (2, "p"), (3, "p"))]);
        var original = table.CurrentSnapshot.ActiveFiles.Values.Single();

        await table.MergeAsync([Rows((2, "q"))], new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update] });

        Assert.Equal([(1, "p"), (2, "q"), (3, "p")], await ReadAsync(table));
        Assert.Equal(1, FileAt(table, original.Path).DeletionVector!.Cardinality);
        Assert.Equal("q", table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Path != original.Path).PartitionValues["value"]);
    }

    [Fact]
    public async Task NotMatchedBySourceClauses_MaskTheRowsTheyChange()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);
        var path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        var (metrics, _) = await table.MergeAsync([Rows((2, "B"))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.Update],
            WhenNotMatchedBySource =
            [
                MergeAction.Delete.When(Ex.GreaterThan("id", LiteralValue.Of(4L))),
                MergeAction.UpdateSet(("value", SparkSqlParser.ParseExpression("'nm'"))).When(Ex.Equal("id", LiteralValue.Of(1L))),
            ],
        });

        Assert.Equal([(1, "nm"), (2, "B"), (3, "c"), (4, "d")], await ReadAsync(table));
        Assert.Equal(3, FileAt(table, path).DeletionVector!.Cardinality);
        Assert.Equal(1, metrics.NumTargetRowsNotMatchedBySourceDeleted);
        Assert.Equal(1, metrics.NumTargetRowsNotMatchedBySourceUpdated);
    }

    [Fact]
    public async Task AnInsertOnlyMerge_WritesNoVector()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await table.MergeAsync([Source], new MergeOptions { Keys = ["id"], WhenNotMatched = [MergeAction.Insert] });

        Assert.All(table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
        Assert.Equal(6, (await ReadAsync(table)).Count);
    }

    [Fact]
    public async Task InATransaction_ASecondOperationOnAFileTheMergeMasked_IsRefused()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using var txn = table.StartTransaction();
        await txn.MergeAsync([Source], UpsertWithDelete());
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            txn.DeleteAsync(Ex.Equal("id", LiteralValue.Of(4L))).AsTask());
    }

    [Fact]
    public async Task InATransaction_AMergeOfAFileAnEarlierDeleteMasked_IsRefused()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using var txn = table.StartTransaction();
        await txn.DeleteAsync(Ex.Equal("id", LiteralValue.Of(4L)));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            txn.MergeAsync([Source], UpsertWithDelete()).AsTask());
    }

    [Fact]
    public async Task AnAbortedMerge_TakesBackItsVectorAndPostImages()
    {
        await using var table = await CreateAsync();
        table.DeletionVectorInlineThreshold = 0; // a vector in a file of its own, for the abort to take back
        await table.WriteAsync([Five]);
        int parquet = FileCount("*.parquet");

        await using (var txn = table.StartTransaction())
        {
            await txn.MergeAsync([Source], UpsertWithDelete());
            Assert.Equal(1, FileCount("*.bin"));
            await txn.AbortAsync();
        }

        Assert.Equal(parquet, FileCount("*.parquet"));
        Assert.Equal(0, FileCount("*.bin"));
        Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], await ReadAsync(table));
    }

    [Fact]
    public async Task AConcurrentDeleteOfAnotherRowOfTheMaskedFile_Conflicts()
    {
        // An update cannot be rebased row by row: had the concurrent delete removed the updated row, a union of the
        // vectors would bring its post-image back. So the MERGE conflicts on the file, as a rewrite does.
        await using var table = await CreateAsync();
        await table.WriteAsync([Five]);

        await using var txn = table.StartTransaction();
        await txn.MergeAsync([Source], UpsertWithDelete());
        await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(4L)));

        await Assert.ThrowsAnyAsync<DeltaConflictException>(() => txn.CommitAsync().AsTask());
        Assert.Equal([(1, "a"), (2, "b"), (3, "c"), (5, "e")], await ReadAsync(table));
    }
}
