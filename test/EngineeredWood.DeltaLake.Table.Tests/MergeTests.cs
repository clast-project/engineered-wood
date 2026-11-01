// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>The key-based MERGE (#563).</summary>
public class MergeTests : IDisposable
{
    private readonly string _tempDir;

    public MergeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_merge_{Guid.NewGuid():N}");
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
        .Field(new Field("ts", Int64Type.Default, true))
        .Build();

    private Task<DeltaTable> CreateAsync(
        bool cdf = false, bool rowTracking = false, IReadOnlyList<string>? partitionColumns = null,
        Apache.Arrow.Schema? schema = null, string? constraint = null)
    {
        var configuration = new Dictionary<string, string>();
        if (cdf)
            configuration[CdfConfig.EnableKey] = "true";
        if (constraint is not null)
            configuration["delta.constraints.c"] = constraint;
        return DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema ?? TableSchema, partitionColumns: partitionColumns,
            enableRowTracking: rowTracking, configuration: configuration).AsTask();
    }

    private static RecordBatch Rows(params (long? Id, string? Value, long? Ts)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        var ts = new Int64Array.Builder();
        foreach (var (id, value, t) in rows)
        {
            if (id is { } i) ids.Append(i); else ids.AppendNull();
            if (value is null) values.AppendNull(); else values.Append(value);
            if (t is { } tv) ts.Append(tv); else ts.AppendNull();
        }
        return new RecordBatch(TableSchema, [ids.Build(), values.Build(), ts.Build()], rows.Length);
    }

    private static async Task<List<(long? Id, string? Value, long? Ts)>> ReadAsync(DeltaTable table)
    {
        var rows = new List<(long?, string?, long?)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            var ts = (Int64Array)batch.Column("ts");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i), values.GetString(i), ts.GetValue(i)));
        }
        return rows.OrderBy(r => r.Item1 ?? long.MinValue).ThenBy(r => r.Item2, StringComparer.Ordinal).ToList();
    }

    private static MergeOptions Upsert => new()
    {
        Keys = ["id"],
        WhenMatched = MergeAction.Update,
        WhenNotMatched = MergeAction.Insert,
    };

    private static Predicate Source(string column, string op, string targetColumn) => op switch
    {
        ">" => Ex.GreaterThan(UnboundReference.FromParts(["source", column]), UnboundReference.FromParts(["target", targetColumn])),
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };

    [Fact]
    public async Task AnUpsert_UpdatesTheMatchedRowsAndInsertsTheRest()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);

        var (metrics, version) = await table.MergeAsync([Rows((2, "B", 21), (4, "D", 40))], Upsert);

        Assert.Equal(2, version);
        Assert.Equal(
            [(1L, "a", 10L), (2L, "B", 21L), (3L, "c", 30L), (4L, "D", 40L)],
            (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
        Assert.Equal(2, metrics.NumSourceRows);
        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
        Assert.Equal(0, metrics.NumTargetRowsDeleted);
        Assert.Equal(2, metrics.NumTargetRowsCopied);
        Assert.Equal(1, metrics.NumTargetFilesRemoved);
        Assert.Equal(2, metrics.NumTargetFilesAdded);
        Assert.Equal(4, metrics.NumOutputRows);

        string? operation = null;
        await foreach (var entry in table.GetHistoryAsync())
            operation = entry.Operation;
        Assert.Equal("MERGE", operation);
    }

    [Fact]
    public async Task UpdateColumns_LeavesTheOtherColumnsAsTheTargetHadThem()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        await table.MergeAsync([Rows((2, "B", 99))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.UpdateColumns("value"),
        });

        Assert.Equal([(1L, "a", 10L), (2L, "B", 20L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task ColumnLists_LeaveAColumnTheSourceLacks_OnUpdate_AndInsertItAsNull()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await table.MergeAsync([NarrowSource()], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.UpdateColumns("value"),
            WhenNotMatched = MergeAction.InsertColumns("id", "value"),
        });

        Assert.Equal([(1L, "A", (long?)10L), (2L, "B", null)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts)));
    }

    /// <summary>Spark: <c>UPDATE SET *</c> and <c>INSERT *</c> need every table column in the source
    /// (<c>DELTA_MERGE_UNRESOLVED_EXPRESSION</c>).</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AStarAction_NeedsEveryColumnInTheSource(bool update)
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => table.MergeAsync([NarrowSource()], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = update ? MergeAction.Update : null,
            WhenNotMatched = update ? null : MergeAction.Insert,
        }).AsTask());

        Assert.Contains("'ts'", ex.Message);
        Assert.Equal(1, table.CurrentSnapshot.Version);
    }

    private static RecordBatch NarrowSource()
    {
        var narrow = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        return new RecordBatch(narrow,
            [new Int64Array.Builder().Append(1).Append(2).Build(), new StringArray.Builder().Append("A").Append("B").Build()], 2);
    }

    [Fact]
    public async Task AMatchedDelete_DeletesTheMatchedRows()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);

        var (metrics, _) = await table.MergeAsync([Rows((1, null, null), (3, null, null))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Delete,
        });

        Assert.Equal([2L], (await ReadAsync(table)).Select(r => r.Id!.Value));
        Assert.Equal(2, metrics.NumTargetRowsMatchedDeleted);
        Assert.Equal(2, metrics.NumTargetRowsDeleted);
    }

    [Fact]
    public async Task NotMatchedBySource_DeletesTheRowsTheSourceNoLongerHas()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        await table.WriteAsync([Rows((3, "c", 30))]);

        var (metrics, _) = await table.MergeAsync([Rows((2, "B", 20), (4, "d", 40))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenNotMatched = MergeAction.Insert,
            WhenNotMatchedBySource = MergeAction.Delete,
        });

        Assert.Equal([(2L, "B"), (4L, "d")], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value)));
        Assert.Equal(2, metrics.NumTargetRowsNotMatchedBySourceDeleted);
        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
    }

    [Fact]
    public async Task NotMatchedBySource_OnlyDeletesWhereItsConditionHolds()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);

        await table.MergeAsync([Rows((2, "b", 20))], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatchedBySource = MergeAction.Delete,
            WhenNotMatchedBySourceCondition = Ex.GreaterThan(
                UnboundReference.FromParts(["target", "ts"]), Ex.Lit(LiteralValue.Of(15L))),
        });

        Assert.Equal([1L, 2L], (await ReadAsync(table)).Select(r => r.Id!.Value));
    }

    [Fact]
    public async Task ANullKey_MatchesNothing()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((null, "target-null", 1), (1, "a", 10))]);

        var (metrics, _) = await table.MergeAsync([Rows((null, "source-null", 2), (1, "A", 11))], Upsert);

        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
        Assert.Equal(
            [(null, "source-null"), (null, "target-null"), (1L, "A")],
            (await ReadAsync(table)).Select(r => (r.Id, r.Value)));
    }

    [Fact]
    public async Task TwoSourceRowsUpdatingOneTargetRow_AreRefused_AndNothingIsWritten()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);
        var before = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length;

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() =>
            table.MergeAsync([Rows((1, "x", 1), (1, "y", 2))], Upsert).AsTask());

        Assert.Equal(DeltaTableErrorCodes.MultipleSourceRowsMatchingTargetRow, ex.ErrorCode);
        Assert.Equal(1, table.CurrentSnapshot.Version);
        Assert.Equal(before, Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length);
    }

    /// <summary>delta-spark counts the ON matches alone (<c>ClassicMergeExecutor.findTouchedFiles</c>), so a
    /// condition that would pick one source row, or none, does not make two matches acceptable; nor does having no
    /// matched clause at all. Each case checked against Spark 4.0 / Delta 4.0.</summary>
    [Theory]
    [InlineData("conditionHoldsForOne")]
    [InlineData("conditionHoldsForNone")]
    [InlineData("conditionalDelete")]
    [InlineData("notMatchedBySourceOnly")]
    public async Task TwoSourceRowsMatchingOneTargetRow_AreRefused_WhateverTheConditions(string which)
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        var options = which switch
        {
            "conditionHoldsForOne" => new MergeOptions
            {
                Keys = ["id"], WhenMatched = MergeAction.Update, WhenMatchedCondition = Source("ts", ">", "ts"),
            },
            "conditionHoldsForNone" => new MergeOptions
            {
                Keys = ["id"], WhenMatched = MergeAction.Update,
                WhenMatchedCondition = Ex.GreaterThan(UnboundReference.FromParts(["source", "ts"]), Ex.Lit(LiteralValue.Of(100L))),
            },
            "conditionalDelete" => new MergeOptions
            {
                Keys = ["id"], WhenMatched = MergeAction.Delete,
                WhenMatchedCondition = Ex.GreaterThan(UnboundReference.FromParts(["source", "ts"]), Ex.Lit(LiteralValue.Of(0L))),
            },
            _ => new MergeOptions { Keys = ["id"], WhenNotMatchedBySource = MergeAction.Delete },
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() =>
            table.MergeAsync([Rows((1, "old", 5), (1, "new", 15))], options).AsTask());

        Assert.Equal(DeltaTableErrorCodes.MultipleSourceRowsMatchingTargetRow, ex.ErrorCode);
        Assert.Equal(1, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task AnInsertOnlyMerge_MayMatchARowMoreThanOnce()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await table.MergeAsync([Rows((1, "x", 1), (1, "y", 2), (3, "c", 3))], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatched = MergeAction.Insert,
        });

        Assert.Equal([(1L, "a"), (3L, "c")], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value)));
    }

    [Fact]
    public async Task AnUnconditionalDelete_MayMatchARowMoreThanOnce()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        var (metrics, _) = await table.MergeAsync([Rows((1, null, null), (1, null, null))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Delete,
        });

        Assert.Equal(1, metrics.NumTargetRowsDeleted);
        Assert.Equal([2L], (await ReadAsync(table)).Select(r => r.Id!.Value));
    }

    [Fact]
    public async Task TheMatchedCondition_SeesBothRows()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        // Only a newer source row wins: 1's is newer, 2's is older.
        var (metrics, _) = await table.MergeAsync([Rows((1, "A", 11), (2, "B", 19))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenMatchedCondition = Source("ts", ">", "ts"),
            WhenNotMatched = MergeAction.Insert,
        });

        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(0, metrics.NumTargetRowsInserted); // 2 matched; its condition failing does not make it an insert
        Assert.Equal([(1L, "A"), (2L, "b")], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value)));
    }

    [Fact]
    public async Task TheNotMatchedCondition_FiltersTheInserts()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await table.MergeAsync([Rows((2, "keep", 1), (3, "drop", -1))], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatched = MergeAction.Insert,
            WhenNotMatchedCondition = Ex.GreaterThan(
                UnboundReference.FromParts(["source", "ts"]), Ex.Lit(LiteralValue.Of(0L))),
        });

        Assert.Equal([1L, 2L], (await ReadAsync(table)).Select(r => r.Id!.Value));
    }

    [Fact]
    public async Task DuplicateUnmatchedSourceRows_AreAllInserted()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await table.MergeAsync([Rows((5, "x", 1), (5, "y", 2))], Upsert);

        Assert.Equal([(1L, "a"), (5L, "x"), (5L, "y")], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value)));
    }

    [Fact]
    public async Task AFileOutsideTheSourceKeyRange_IsNeitherReadNorRewritten()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        await table.WriteAsync([Rows((100, "x", 1), (200, "y", 2))]);
        var far = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Stats!.Contains("\"minValues\":{\"id\":100"));

        await using var txn = table.StartTransaction();
        await txn.MergeAsync([Rows((2, "B", 21), (3, "c", 30))], Upsert);
        Assert.DoesNotContain(far.Path, txn.DeclaredReadPaths);
        Assert.DoesNotContain(far.Path, txn.RemovedPaths);
        await txn.CommitAsync();

        Assert.Contains(far.Path, table.CurrentSnapshot.ActiveFiles.Keys);
        Assert.Equal([1L, 2L, 3L, 100L, 200L], (await ReadAsync(table)).Select(r => r.Id!.Value));
    }

    [Fact]
    public async Task AMergeThatChangesNothing_CommitsNothing()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        var (metrics, version) = await table.MergeAsync([Rows((1, "A", 0))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenMatchedCondition = Source("ts", ">", "ts"),
        });

        Assert.Equal(1, version);
        Assert.Equal(0, metrics.NumTargetRowsUpdated);
    }

    [Fact]
    public async Task CompositeKeys_MatchOnEveryKey()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (1, "b", 20))]);

        await table.MergeAsync([Rows((1, "b", 99), (1, "c", 30))], new MergeOptions
        {
            Keys = ["id", "value"],
            WhenMatched = MergeAction.Update,
            WhenNotMatched = MergeAction.Insert,
        });

        Assert.Equal([(1L, "a", 10L), (1L, "b", 99L), (1L, "c", 30L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task TheChangeFeed_RecordsEachKindOfChange()
    {
        await using var table = await CreateAsync(cdf: true);
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);

        var (_, version) = await table.MergeAsync([Rows((1, "A", 11), (4, "d", 40))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenNotMatched = MergeAction.Insert,
            WhenNotMatchedBySource = MergeAction.Delete,
            WhenNotMatchedBySourceCondition = Ex.Equal("id", LiteralValue.Of(3L)),
        });

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
            [("delete", 3L, "c"), ("insert", 4L, "d"), ("update_postimage", 1L, "A"), ("update_preimage", 1L, "a")],
            changes.OrderBy(c => c.Type, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task OnARowTrackingTable_AnUpdatedRowKeepsItsStableId()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        var before = await RowIdsAsync(table);

        await table.MergeAsync([Rows((2, "B", 21), (3, "c", 30))], Upsert);

        var after = await RowIdsAsync(table);
        Assert.Equal(before[1], after[1]);
        Assert.Equal(before[2], after[2]);
        Assert.Equal(3, after.Values.Distinct().Count());
    }

    private static async Task<Dictionary<long, long>> RowIdsAsync(DeltaTable table)
    {
        var result = new Dictionary<long, long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rowIds = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                result[ids.GetValue(i)!.Value] = rowIds.GetValue(i)!.Value;
        }
        return result;
    }

    [Fact]
    public async Task OnAPartitionedTable_AnUpdateThatChangesThePartitionMovesTheRow()
    {
        await using var table = await CreateAsync(partitionColumns: ["value"]);
        await table.WriteAsync([Rows((1, "p", 10), (2, "p", 20))]);

        await table.MergeAsync([Rows((2, "q", 21))], Upsert);

        Assert.Equal([(1L, "p"), (2L, "q")], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value)));
        Assert.Contains(table.CurrentSnapshot.ActiveFiles.Values, f => f.PartitionValues["value"] == "q");
    }

    [Fact]
    public async Task OnAColumnMappingTable_TheMergeNamesColumnsLogically()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema, columnMappingMode: EngineeredWood.DeltaLake.Schema.ColumnMappingMode.Name);
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        await table.WriteAsync([Rows((50, "x", 1))]);

        var (metrics, _) = await table.MergeAsync([Rows((2, "B", 21), (3, "c", 30))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenMatchedCondition = Source("ts", ">", "ts"),
            WhenNotMatched = MergeAction.Insert,
        });

        Assert.Equal(1, metrics.NumTargetFilesRemoved); // the id=50 file is outside the key range
        Assert.Equal(
            [(1L, "a", 10L), (2L, "B", 21L), (3L, "c", 30L), (50L, "x", 1L)],
            (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task APartitionKey_PrunesTheOtherPartitions()
    {
        await using var table = await CreateAsync(partitionColumns: ["value"]);
        await table.WriteAsync([Rows((1, "p", 10), (2, "q", 20))]);
        var other = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.PartitionValues["value"] == "q");

        await using var txn = table.StartTransaction();
        await txn.MergeAsync([Rows((1, "p", 11), (3, "p", 30))], new MergeOptions
        {
            Keys = ["value", "id"],
            WhenMatched = MergeAction.Update,
            WhenNotMatched = MergeAction.Insert,
        });
        Assert.DoesNotContain(other.Path, txn.DeclaredReadPaths);
        Assert.DoesNotContain(other.Path, txn.RemovedPaths);
        await txn.CommitAsync();

        Assert.Equal([(1L, "p", 11L), (2L, "q", 20L), (3L, "p", 30L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task InATransaction_ASecondOperationOnAFileTheMergeRewrote_IsRefused()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        await using var txn = table.StartTransaction();
        await txn.MergeAsync([Rows((1, "A", 11))], Upsert);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            txn.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2L))).AsTask());
    }

    [Fact]
    public async Task AnAbortedMerge_TakesBackTheFilesItWrote()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);
        var before = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length;

        await using (var txn = table.StartTransaction())
        {
            await txn.MergeAsync([Rows((1, "A", 11), (2, "b", 20))], Upsert);
            Assert.True(Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length > before);
            await txn.AbortAsync();
        }

        Assert.Equal(before, Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length);
    }

    [Fact]
    public async Task AConcurrentAppendOfASourceKey_Conflicts()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await using var txn = table.StartTransaction(IsolationLevel.Serializable);
        await txn.MergeAsync([Rows((1, "A", 11), (2, "b", 20))], Upsert);
        await table.WriteAsync([Rows((2, "concurrent", 0))]);

        await Assert.ThrowsAnyAsync<DeltaConflictException>(() => txn.CommitAsync().AsTask());
    }

    [Fact]
    public async Task AConcurrentAppendOutsideTheSourceKeyRange_DoesNotConflict()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await using var txn = table.StartTransaction(IsolationLevel.Serializable);
        await txn.MergeAsync([Rows((1, "A", 11), (2, "b", 20))], Upsert);
        await table.WriteAsync([Rows((500, "concurrent", 0))]);
        await txn.CommitAsync();

        Assert.Equal([1L, 2L, 500L], (await ReadAsync(table)).Select(r => r.Id!.Value));
    }

    [Fact]
    public async Task ACheckConstraintTheUpdateBreaks_IsRefused_AndNothingCommits()
    {
        await using var table = await CreateAsync(constraint: "ts > 0");
        await table.WriteAsync([Rows((1, "a", 10))]);

        await Assert.ThrowsAnyAsync<Exception>(() =>
            table.MergeAsync([Rows((1, "A", -5))], Upsert).AsTask());
        Assert.Equal([(1L, "a", 10L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task ASourceColumnTheTableLacks_IsRefused()
    {
        await using var table = await CreateAsync();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("extra", Int64Type.Default, true))
            .Build();
        var source = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(1).Build()], 1);

        await Assert.ThrowsAsync<ArgumentException>(() => table.MergeAsync([source], Upsert).AsTask());
    }

    private static readonly TimestampType Micros = new(TimeUnit.Microsecond, "UTC");
    private static readonly TimestampType Millis = new(TimeUnit.Millisecond, "UTC");

    private static TimestampArray Timestamps(TimestampType type, params long[] values)
    {
        var data = new ArrowBuffer.Builder<long>().AppendRange(values).Build();
        return new TimestampArray(type, data, ArrowBuffer.Empty, values.Length, 0, 0);
    }

    private static RecordBatch TimestampRows(TimestampType kType, long[] k, IArrowArray v, IArrowType vType)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", kType, true)).Field(new Field("v", vType, true)).Build();
        return new RecordBatch(schema, [Timestamps(kType, k), v], k.Length);
    }

    [Fact]
    public async Task ATimestampKey_MatchesTheSameInstantInAnotherUnit()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", Micros, true)).Field(new Field("v", StringType.Default, true)).Build();
        await using var table = await CreateAsync(schema: schema);
        // One second, and one millisecond.
        await table.WriteAsync([TimestampRows(Micros, [1_000_000, 1_000],
            new StringArray.Builder().Append("second").Append("milli").Build(), StringType.Default)]);

        // In milliseconds: 1000 is the first row's instant (not the second row's raw count), 7 neither's.
        var (metrics, _) = await table.MergeAsync(
            [TimestampRows(Millis, [1_000, 7], new StringArray.Builder().Append("S").Append("new").Build(), StringType.Default)],
            new MergeOptions { Keys = ["k"], WhenMatched = MergeAction.Update, WhenNotMatched = MergeAction.Insert });

        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
        var rows = new SortedDictionary<long, string?>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var k = (TimestampArray)batch.Column("k");
            Assert.Equal(TimeUnit.Microsecond, ((TimestampType)k.Data.DataType).Unit);
            for (int i = 0; i < batch.Length; i++)
                rows[k.GetValue(i)!.Value] = ((StringArray)batch.Column("v")).GetString(i);
        }
        Assert.Equal(new Dictionary<long, string?> { [1_000] = "milli", [7_000] = "new", [1_000_000] = "S" }, rows);
    }

    [Fact]
    public async Task AnUpdateFromAnotherArrowForm_IsWrittenBesideTheCopiedRowsInTheTablesForm()
    {
        // A file with one updated row and one copied: the source's millisecond timestamps and large strings
        // must reach it as the table's microseconds and strings.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", StringType.Default, true))
            .Field(new Field("at", Micros, true))
            .Build();
        await using var table = await CreateAsync(schema: schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(),
             new StringArray.Builder().Append("a").Append("b").Build(),
             Timestamps(Micros, 10, 20)], 2)]);

        var sourceSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", LargeStringType.Default, true))
            .Field(new Field("at", Millis, true))
            .Build();
        await table.MergeAsync([new RecordBatch(sourceSchema,
            [new Int64Array.Builder().Append(1).Build(),
             new LargeStringArray.Builder().Append("A").Build(),
             Timestamps(Millis, 5)], 1)],
            new MergeOptions { Keys = ["id"], WhenMatched = MergeAction.Update });

        var rows = new List<(long, string?, long)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            for (int i = 0; i < batch.Length; i++)
            {
                rows.Add((((Int64Array)batch.Column("id")).GetValue(i)!.Value,
                    ((StringArray)batch.Column("v")).GetString(i),
                    ((TimestampArray)batch.Column("at")).GetValue(i)!.Value));
            }
        }
        Assert.Equal([(1L, "A", 5_000L), (2L, "b", 20L)], rows.OrderBy(r => r.Item1));
    }

    [Fact]
    public async Task SourceBatchesInDifferentForms_AreEachConvertedBeforeTheyAreCombined()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", Micros, true)).Field(new Field("v", StringType.Default, true)).Build();
        await using var table = await CreateAsync(schema: schema);
        await table.WriteAsync([TimestampRows(Micros, [1_000_000],
            new StringArray.Builder().Append("a").Build(), StringType.Default)]);

        var (metrics, _) = await table.MergeAsync(
            [
                TimestampRows(Millis, [1_000], new LargeStringArray.Builder().Append("A").Build(), LargeStringType.Default),
                TimestampRows(Micros, [2_000_000], new StringArray.Builder().Append("b").Build(), StringType.Default),
            ],
            new MergeOptions { Keys = ["k"], WhenMatched = MergeAction.Update, WhenNotMatched = MergeAction.Insert });

        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
    }

    [Fact]
    public async Task ADecimalKeyAtAnotherScale_IsRefusedAsMistyped()
    {
        // decimal(10,3) is another Delta type than the table's decimal(10,2), so it never reaches the matching.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("k", new Decimal128Type(10, 2), true)).Field(new Field("v", StringType.Default, true)).Build();
        await using var table = await CreateAsync(schema: schema);
        var sourceType = new Decimal128Type(10, 3);
        var source = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("k", sourceType, true)).Field(new Field("v", StringType.Default, true)).Build(),
            [new Decimal128Array.Builder(sourceType).Append(1.000m).Build(), new StringArray.Builder().Append("A").Build()], 1);

        await Assert.ThrowsAsync<ArgumentException>(() => table.MergeAsync([source], new MergeOptions
        {
            Keys = ["k"], WhenMatched = MergeAction.Update, WhenNotMatched = MergeAction.Insert,
        }).AsTask());
    }

    [Fact]
    public void DecimalKeys_EncodeByValue_WhateverTheirWidthOrScale()
    {
        // A host's codec hands batches through unconverted, so a key may arrive at another width or scale.
        static byte[] Key(IArrowArray column) => MergeKeys.Encode([column], 0)!;
        var twoPlaces = new Decimal128Type(10, 2);
        var threePlaces = new Decimal128Type(10, 3);
        byte[] oneAtTwo = Key(new Decimal128Array.Builder(twoPlaces).Append(1.00m).Build());
        byte[] oneAtThree = Key(new Decimal128Array.Builder(threePlaces).Append(1.000m).Build());
        byte[] oneAt256 = Key(new Decimal256Array.Builder(new Decimal256Type(10, 2)).Append(1.00m).Build());
        byte[] tenthAtThree = Key(new Decimal128Array.Builder(threePlaces).Append(0.100m).Build());
        byte[] cents = Key(new Decimal128Array.Builder(twoPlaces).Append(0.01m).Build());
        byte[] mils = Key(new Decimal128Array.Builder(threePlaces).Append(0.010m).Build());

        Assert.Equal(oneAtTwo, oneAtThree, MergeKeyComparer.Instance);
        Assert.Equal(oneAtTwo, oneAt256, MergeKeyComparer.Instance);
        Assert.Equal(cents, mils, MergeKeyComparer.Instance);
        Assert.NotEqual(oneAtTwo, tenthAtThree, MergeKeyComparer.Instance);
        Assert.NotEqual(cents, oneAtTwo, MergeKeyComparer.Instance);
    }

    [Fact]
    public async Task AMergeThatReadNothing_StillRecordsANonBlindCommit()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        // Every source key NULL: nothing can match, so nothing is scanned, and the row is only inserted.
        var (_, version) = await table.MergeAsync([Rows((null, "n", 0))], Upsert);

        string commit = File.ReadAllText(Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json"));
        Assert.Contains("\"isBlindAppend\":false", commit);
    }

    private Task<DeltaTable> CreateAppendOnlyAsync() => DeltaTable.CreateAsync(
        new LocalTableFileSystem(_tempDir), TableSchema,
        configuration: new Dictionary<string, string> { ["delta.appendOnly"] = "true" }).AsTask();

    [Fact]
    public async Task OnAnAppendOnlyTable_AMergeThatChangesNoRow_Lands()
    {
        await using var table = await CreateAppendOnlyAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        // Insert-only, and an upsert whose matched clause matches nothing: neither removes a file (as Spark).
        await table.MergeAsync([Rows((1, "x", 0), (2, "b", 20))], new MergeOptions
        {
            Keys = ["id"], WhenNotMatched = MergeAction.Insert,
        });
        await table.MergeAsync([Rows((3, "c", 30))], Upsert);

        Assert.Equal([(1L, "a", 10L), (2L, "b", 20L), (3L, "c", 30L)],
            (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OnAnAppendOnlyTable_AMergeThatUpdatesOrDeletes_IsRefused(bool update)
    {
        await using var table = await CreateAppendOnlyAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);
        var before = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length;

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync(
            [Rows((1, "A", 11), (2, "b", 20))], new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = update ? MergeAction.Update : MergeAction.Delete,
                WhenNotMatched = MergeAction.Insert,
            }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.CannotModifyAppendOnly, e.ErrorCode);
        Assert.Equal([(1L, "a", 10L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
        Assert.Equal(before, Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Length);
    }

    [Fact]
    public async Task TwoInsertingMergesInATransaction_TakeDistinctIdentityValues()
    {
        var idMeta = Schema.IdentityColumn.CreateMetadata(start: 1, step: 1, allowExplicitInsert: false);
        string schemaString = "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":"
            + System.Text.Json.JsonSerializer.Serialize(idMeta) + "},"
            + "{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
        var fs = new LocalTableFileSystem(_tempDir);
        await new Log.TransactionLog(fs).WriteCommitAsync(0, new List<Actions.DeltaAction>
        {
            new Actions.ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new Actions.MetadataAction
            {
                Id = "merge-identity", Format = Actions.Format.Parquet, SchemaString = schemaString, PartitionColumns = [],
            },
        });
        await using var table = await DeltaTable.OpenAsync(fs);

        static RecordBatch Values(params string[] values)
        {
            var builder = new StringArray.Builder();
            foreach (string v in values)
                builder.Append(v);
            return new RecordBatch(
                new Apache.Arrow.Schema.Builder().Field(new Field("value", StringType.Default, true)).Build(),
                [builder.Build()], values.Length);
        }

        var insert = new MergeOptions { Keys = ["value"], WhenNotMatched = MergeAction.InsertColumns("value") };
        await using (var txn = table.StartTransaction())
        {
            await txn.MergeAsync([Values("a", "b")], insert);
            await txn.MergeAsync([Values("c")], insert);
            await txn.CommitAsync();
        }
        // The committed mark carries both: the next write continues after them.
        await table.MergeAsync([Values("d")], insert);

        var ids = new Dictionary<string, long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            for (int i = 0; i < batch.Length; i++)
                ids[((StringArray)batch.Column("value")).GetString(i)] = ((Int64Array)batch.Column("id")).GetValue(i)!.Value;
        }
        Assert.Equal(new long[] { 1, 2, 3, 4 }, ids.OrderBy(kv => kv.Key).Select(kv => kv.Value));
    }

    [Theory]
    [InlineData("noKeys")]
    [InlineData("noClauses")]
    [InlineData("insertWhenMatched")]
    [InlineData("conditionWithoutClause")]
    public async Task InvalidOptions_AreRefused(string which)
    {
        await using var table = await CreateAsync();
        var options = which switch
        {
            "noKeys" => new MergeOptions { WhenNotMatched = MergeAction.Insert },
            "noClauses" => new MergeOptions { Keys = ["id"] },
            "insertWhenMatched" => new MergeOptions { Keys = ["id"], WhenMatched = MergeAction.Insert },
            _ => new MergeOptions
            {
                Keys = ["id"], WhenNotMatched = MergeAction.Insert, WhenMatchedCondition = Ex.True,
            },
        };

        await Assert.ThrowsAsync<ArgumentException>(() => table.MergeAsync([Rows((1, "a", 1))], options).AsTask());
    }
}
