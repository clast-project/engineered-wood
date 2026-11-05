// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// MERGE with several clauses of a kind (#575) and assigned values (#576). Each behaviour asserted here was measured
/// against Spark 4.0 / Delta 4.0 first; <c>MergeInteropTests</c> pins the combination against Spark itself.
/// </summary>
public class MergeClauseTests : IDisposable
{
    private readonly string _tempDir;

    public MergeClauseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_merge_clauses_{Guid.NewGuid():N}");
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

    private static Expression E(string sql) => SparkSqlParser.ParseExpression(sql);
    private static Predicate P(string sql) => SparkSqlParser.ParsePredicate(sql);

    private Task<DeltaTable> CreateAsync(
        Apache.Arrow.Schema? schema = null, bool cdf = false, bool rowTracking = false,
        IReadOnlyList<string>? partitionColumns = null)
    {
        var configuration = new Dictionary<string, string>();
        if (cdf)
            configuration[CdfConfig.EnableKey] = "true";
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
            if (t is { } x) ts.Append(x); else ts.AppendNull();
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
        return [.. rows.OrderBy(r => r.Item1 ?? long.MinValue)];
    }

    // ── #575: several clauses of a kind ──

    [Fact]
    public async Task AMatchedDeleteBeforeAnUpdate_DeletesOnlyTheRowsItsConditionHolds()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);

        var (metrics, _) = await table.MergeAsync([Rows((1, "del", 11), (2, "upd", 21))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.Delete.When(P("source.value = 'del'")), MergeAction.Update],
        });

        Assert.Equal([(2L, "upd", 21L), (3L, "c", 30L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
        Assert.Equal(1, metrics.NumTargetRowsMatchedDeleted);
        Assert.Equal(1, metrics.NumTargetRowsMatchedUpdated);
        Assert.Equal(1, metrics.NumTargetRowsCopied);
    }

    [Fact]
    public async Task ARowTakesTheFirstClauseWhoseConditionHolds()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        await table.MergeAsync([Rows((1, "x", 0), (2, "y", 0))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched =
            [
                MergeAction.UpdateSet(("value", E("'first'"))).When(P("target.id >= 1")),
                MergeAction.UpdateSet(("value", E("'second'"))).When(P("target.id >= 2")),
            ],
        });

        Assert.Equal(["first", "first"], (await ReadAsync(table)).Select(r => r.Value));
    }

    [Fact]
    public async Task AMatchedRowNoClauseTakes_IsKeptAsItWas()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        var (metrics, _) = await table.MergeAsync([Rows((1, "x", 0), (2, "y", 0))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched =
            [
                MergeAction.UpdateSet(("value", E("'one'"))).When(P("source.value = 'x'")),
                MergeAction.Delete.When(P("source.value = 'zz'")),
            ],
        });

        Assert.Equal(["one", "b"], (await ReadAsync(table)).Select(r => r.Value));
        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(0, metrics.NumTargetRowsDeleted);
        Assert.Equal(1, metrics.NumTargetRowsCopied);
    }

    [Fact]
    public async Task ALaterClausesCondition_IsNotEvaluatedForARowAnEarlierClauseTook()
    {
        // CAST('del' AS BIGINT) raises CAST_INVALID_INPUT, so evaluating the second clause's condition for the row
        // the first clause took would fail the MERGE. As in Spark's CASE WHEN, it is never asked.
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        await table.MergeAsync([Rows((1, "del", 0), (2, "5", 0))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched =
            [
                MergeAction.Delete.When(P("source.value = 'del'")),
                MergeAction.UpdateSet(("ts", E("CAST(source.value AS BIGINT)"))).When(P("CAST(source.value AS BIGINT) > 0")),
            ],
        });

        Assert.Equal([(2L, "b", 5L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task SeveralInsertClauses_InsertEachRowByTheFirstThatHolds()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        var (metrics, _) = await table.MergeAsync([Rows((5, "five", 50), (6, "six", 60))], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatched =
            [
                MergeAction.InsertValues(("id", E("source.id")), ("value", E("'by-first'"))).When(P("source.id = 5")),
                MergeAction.Insert,
            ],
        });

        Assert.Equal(
            [(1L, "a", (long?)10L), (5L, "by-first", null), (6L, "six", 60L)],
            (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts)));
        Assert.Equal(2, metrics.NumTargetRowsInserted);
    }

    [Fact]
    public async Task ANotMatchedBySourceUpdate_ThenDelete()
    {
        await using var table = await CreateAsync(cdf: true);
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);

        var (metrics, version) = await table.MergeAsync([Rows((1, "x", 0))], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatchedBySource =
            [
                MergeAction.UpdateSet(("value", E("'orphan'")), ("ts", E("target.ts * 10"))).When(P("target.id = 2")),
                MergeAction.Delete,
            ],
        });

        Assert.Equal([(1L, "a", 10L), (2L, "orphan", 200L)], (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
        Assert.Equal(1, metrics.NumTargetRowsNotMatchedBySourceUpdated);
        Assert.Equal(0, metrics.NumTargetRowsMatchedUpdated);
        Assert.Equal(1, metrics.NumTargetRowsUpdated);
        Assert.Equal(1, metrics.NumTargetRowsNotMatchedBySourceDeleted);

        var feed = new List<(string, long?, string?)>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            for (int i = 0; i < batch.Length; i++)
            {
                feed.Add((((StringArray)batch.Column("_change_type")).GetString(i)!,
                    ((Int64Array)batch.Column("id")).GetValue(i), ((StringArray)batch.Column("value")).GetString(i)));
            }
        }
        Assert.Equal(
            [("delete", 3L, "c"), ("update_postimage", 2L, "orphan"), ("update_preimage", 2L, "b")],
            feed.OrderBy(f => f.Item1, StringComparer.Ordinal));

        IReadOnlyDictionary<string, string>? committed = null;
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version == version)
                committed = entry.OperationMetrics;
        }
        Assert.Equal("1", committed!["numTargetRowsNotMatchedBySourceUpdated"]);
    }

    [Fact]
    public async Task ANotMatchedBySourceUpdate_KeepsTheRowsStableId()
    {
        await using var table = await CreateAsync(rowTracking: true);
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        var before = await RowIdsAsync(table);

        await table.MergeAsync([Rows((1, "x", 0))], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatchedBySource = [MergeAction.UpdateSet(("value", E("'orphan'")))],
        });

        Assert.Equal(before, await RowIdsAsync(table));

        static async Task<Dictionary<long, long>> RowIdsAsync(DeltaTable table)
        {
            var ids = new Dictionary<long, long>();
            await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
            {
                var id = (Int64Array)batch.Column("id");
                var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
                for (int i = 0; i < batch.Length; i++)
                    ids[id.GetValue(i)!.Value] = rowId.GetValue(i)!.Value;
            }
            return ids;
        }
    }

    [Theory]
    [InlineData("conditionalThenUnconditionalDelete", true)]
    [InlineData("conditionsNeverHold", true)]
    [InlineData("onlyNotMatchedBySource", true)]
    [InlineData("unconditionalDeleteAndNotMatchedBySource", false)]
    public async Task TwoSourceRowsOnOneTargetRow_AreRefusedAsSparkRefusesThem(string shape, bool refused)
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);
        var options = shape switch
        {
            "conditionalThenUnconditionalDelete" => new MergeOptions
            {
                Keys = ["id"], WhenMatched = [MergeAction.Delete.When(P("source.value = 'x'")), MergeAction.Delete],
            },
            "conditionsNeverHold" => new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = [MergeAction.Update.When(P("source.value = 'zz'")), MergeAction.Delete.When(P("source.value = 'qq'"))],
            },
            "onlyNotMatchedBySource" => new MergeOptions { Keys = ["id"], WhenNotMatchedBySource = [MergeAction.Delete] },
            _ => new MergeOptions
            {
                Keys = ["id"], WhenMatched = [MergeAction.Delete], WhenNotMatchedBySource = [MergeAction.Delete],
            },
        };

        var merge = () => table.MergeAsync([Rows((1, "x", 0), (1, "y", 0))], options).AsTask();
        if (refused)
        {
            var e = await Assert.ThrowsAsync<DeltaFormatException>(merge);
            Assert.Equal(DeltaTableErrorCodes.MultipleSourceRowsMatchingTargetRow, e.ErrorCode);
        }
        else
        {
            await merge();
            Assert.Empty(await ReadAsync(table));
        }
    }

    // ── #576: assigned values ──

    [Fact]
    public async Task AnUpdateMayReadBothRows_AndAnInsertWritesItsOwnValues()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20))]);

        await table.MergeAsync([Rows((1, "x", 5), (3, "z", 7))], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.UpdateSet(("ts", E("target.ts + source.ts")), ("value", E("concat(target.value, source.value)")))],
            WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), ("ts", E("0")))],
        });

        Assert.Equal(
            [(1L, "ax", 15L), (2L, "b", 20L), (3L, null, 0L)],
            (await ReadAsync(table)).Select(r => (r.Id!.Value, r.Value, r.Ts!.Value)));
    }

    [Fact]
    public async Task EveryAssignment_ReadsTheRowAsItWasBeforeTheUpdate()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("a", Int64Type.Default, true))
            .Field(new Field("b", Int64Type.Default, true))
            .Build();
        await using var table = await CreateAsync(schema);
        await table.WriteAsync([Batch(schema, [1], [10], [20])]);

        await table.MergeAsync([Batch(schema, [1], [0], [0])], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.UpdateSet(("a", E("target.b")), ("b", E("target.a")))],
        });

        await foreach (var batch in table.ReadAllAsync())
        {
            Assert.Equal(20L, ((Int64Array)batch.Column("a")).GetValue(0));
            Assert.Equal(10L, ((Int64Array)batch.Column("b")).GetValue(0));
        }
    }

    [Fact]
    public async Task AnAssignmentNamesItsColumnIgnoringCase()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        await table.MergeAsync([Rows((1, "x", 5))], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("TS", E("99")))],
        });

        Assert.Equal(99L, (await ReadAsync(table)).Single().Ts);
    }

    // As measured: a MERGE assignment of another type is an ANSI CAST to the column's type.
    [Theory]
    [InlineData("i", "'42'", "42")]
    [InlineData("i", "CAST(7 AS BIGINT)", "7")]
    [InlineData("i", "2.7D", "2")]
    [InlineData("i", "true", "1")]
    [InlineData("l", "5", "5")]
    [InlineData("s", "12", "12")]
    [InlineData("s", "DATE'2021-02-03'", "2021-02-03")]
    [InlineData("d", "'2022-03-04'", "2022-03-04")]
    [InlineData("d", "TIMESTAMP'2022-03-04 12:00:00'", "2022-03-04")]
    [InlineData("dec", "1.235BD", "1.24")]
    [InlineData("dec", "7", "7.00")]
    [InlineData("dec", "1.005D", "1.01")]
    [InlineData("b", "1", "True")]
    [InlineData("b", "'true'", "True")]
    [InlineData("f", "'1.5'", "1.5")]
    [InlineData("i", "NULL", "")]
    [InlineData("s", "NULL", "")]
    public async Task AnAssignedValue_IsCastToTheColumnsType(string column, string value, string expected)
    {
        await using var table = await CreateTypedAsync();

        await table.MergeAsync([IdOnly(1)], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, E(value)))],
        });

        Assert.Equal(expected, await ReadTypedAsync(table, column));
    }

    [Theory]
    [InlineData("i", "3000000000L")]
    [InlineData("dec", "123456789012.5BD")]
    public async Task AValueTheColumnCannotHold_IsACastOverflowInTableWrite(string column, string value)
    {
        await using var table = await CreateTypedAsync();

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([IdOnly(1)], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, E(value)))],
        }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.CastOverflowInTableWrite, e.ErrorCode);
    }

    [Fact]
    public async Task AnInsertedValueTheColumnCannotHold_IsACastOverflowInTableWrite()
    {
        await using var table = await CreateTypedAsync();

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([IdOnly(2)], new MergeOptions
        {
            Keys = ["id"], WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), ("i", E("3000000000L")))],
        }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.CastOverflowInTableWrite, e.ErrorCode);
    }

    [Theory]
    [InlineData("i", "'abc'")]
    [InlineData("d", "'nope'")]
    public async Task AMalformedValue_IsCastInvalidInput(string column, string value)
    {
        await using var table = await CreateTypedAsync();

        var e = await Assert.ThrowsAsync<SparkEvaluationException>(() => table.MergeAsync([IdOnly(1)], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, E(value)))],
        }).AsTask());

        Assert.Equal("CAST_INVALID_INPUT", e.ErrorClass);
    }

    [Fact]
    public async Task AnArithmeticOverflow_InTheValueItself_IsAnError()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((1, "a", 10))]);

        var e = await Assert.ThrowsAsync<SparkEvaluationException>(() => table.MergeAsync(
            [Rows((1, "x", long.MaxValue))], new MergeOptions
            {
                Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("ts", E("target.ts + source.ts")))],
            }).AsTask());

        Assert.Equal("ARITHMETIC_OVERFLOW", e.ErrorClass);
        Assert.Equal(10L, (await ReadAsync(table)).Single().Ts);
    }

    [Theory]
    [InlineData("insertReadsTarget")]
    [InlineData("notMatchedBySourceReadsSource")]
    [InlineData("neitherRowHasIt")]
    public async Task AValueReadingAColumnItsClauseCannotSee_IsRefusedBeforeAnyRowIsRead(string shape)
    {
        // An empty table and a source no clause will reach: Spark refuses at analysis, so must this.
        await using var table = await CreateAsync();
        var options = shape switch
        {
            "insertReadsTarget" => new MergeOptions
            {
                Keys = ["id"], WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), ("ts", E("target.ts")))],
            },
            "notMatchedBySourceReadsSource" => new MergeOptions
            {
                Keys = ["id"], WhenNotMatchedBySource = [MergeAction.UpdateSet(("ts", E("source.ts")))],
            },
            _ => new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("ts", E("source.nope")))] },
        };

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([Rows()], options).AsTask());

        Assert.Equal(DeltaTableErrorCodes.MergeUnresolvedExpression, e.ErrorCode);
    }

    [Fact]
    public async Task AnAssignmentToAColumnTheTableLacks_IsRefused()
    {
        await using var table = await CreateAsync();

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([Rows((1, "x", 1))], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("nope", E("1")))],
        }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ColumnNotFound, e.ErrorCode);
    }

    [Fact]
    public void AColumnAssignedTwice_IsRefused()
    {
        Assert.Throws<ArgumentException>(() => MergeAction.UpdateSet(("ts", E("1")), ("TS", E("2"))));
        Assert.Throws<ArgumentException>(() => MergeAction.InsertValues(("id", E("1")), ("id", E("2"))));
    }

    [Fact]
    public void ASecondCondition_IsRefused() =>
        Assert.Throws<InvalidOperationException>(() => MergeAction.Delete.When(P("id = 1")).When(P("id = 2")));

    [Fact]
    public async Task AnAssignmentToANotNullColumn_OfNull_IsRefused()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("x", Int64Type.Default, false))
            .Build();
        await using var table = await CreateAsync(schema);
        await table.WriteAsync([Batch(schema, [1], [1])]);

        var e = await Assert.ThrowsAnyAsync<DeltaFormatException>(() => table.MergeAsync([Batch(schema, [1], [1])], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("x", E("NULL")))],
        }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.NotNullConstraintViolated, e.ErrorCode);
    }

    [Fact]
    public async Task AnAssignmentToAnIdentityColumn_IsRefused()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        var fs = new LocalTableFileSystem(_tempDir);
        string idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(1, 1, true));
        string schemaString = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":true,""metadata"":{idMeta}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";
        await new TransactionLog(fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction { Id = "identity-merge", Format = Format.Parquet, SchemaString = schemaString, PartitionColumns = [] },
        });
        await using var table = await DeltaTable.OpenAsync(fs);
        await table.WriteAsync([new RecordBatch(new Apache.Arrow.Schema.Builder().Field(new Field("value", StringType.Default, true)).Build(),
            [new StringArray.Builder().Append("a").Build()], 1)]);

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync(
            [new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build(), new StringArray.Builder().Append("x").Build()], 1)],
            new MergeOptions { Keys = ["value"], WhenMatched = [MergeAction.UpdateSet(("id", E("source.id + 100")))] }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.IdentityColumnUpdateNotSupported, e.ErrorCode);
    }

    [Fact]
    public async Task AnAssignmentToAPartitionColumn_MovesTheRow()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("p", StringType.Default, true))
            .Build();
        await using var table = await CreateAsync(schema, partitionColumns: ["p"]);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Append(2).Build(), new StringArray.Builder().Append("a").Append("a").Build()], 2)]);

        await table.MergeAsync(
            [new RecordBatch(new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Build(),
                [new Int64Array.Builder().Append(1).Build()], 1)],
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("p", E("concat(target.p, 'b')")))] });

        Assert.Equal(
            ["a", "ab"],
            table.CurrentSnapshot.ActiveFiles.Values.Select(f => f.PartitionValues["p"]).OrderBy(p => p, StringComparer.Ordinal));
    }

    [Fact]
    public async Task ATimestampNtzColumn_TakesOnlyAValueOfItsOwnType()
    {
        var ntz = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("t", ntz, true))
            .Build();
        await using var table = await CreateAsync(schema);
        var row = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(),
             new TimestampArray(ntz, new ArrowBuffer.Builder<long>().Append(1_000_000).Build(), ArrowBuffer.Empty, 1, 0, 0)], 1);
        await table.WriteAsync([row]);

        // Its own type: copied from the source row.
        await table.MergeAsync([row], new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("t", E("source.t")))] });

        // Another: the cast cannot target timestamp_ntz, so it is refused rather than reinterpreted.
        await Assert.ThrowsAsync<ArgumentException>(() => table.MergeAsync([row], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("t", E("'2020-01-01 00:00:00'")))],
        }).AsTask());
    }

    // ── generated columns: an assigned one is checked, an omitted one recomputed ──

    private static readonly Apache.Arrow.Schema GeneratedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, true))
        .Field(new Field("x", Int32Type.Default, true))
        .Field(new Field("g", Int32Type.Default, true,
            new Dictionary<string, string> { ["delta.generationExpression"] = "x * 2" }))
        .Build();

    private Task<DeltaTable> CreateGeneratedAsync() => DeltaTable.CreateAsync(
        new LocalTableFileSystem(_tempDir), GeneratedSchema).AsTask();

    private static RecordBatch Generated(int id, int x, int? g = null)
    {
        var fields = new List<Field> { GeneratedSchema.FieldsList[0], GeneratedSchema.FieldsList[1] };
        var columns = new List<IArrowArray>
        {
            new Int32Array.Builder().Append(id).Build(), new Int32Array.Builder().Append(x).Build(),
        };
        if (g is { } value)
        {
            fields.Add(GeneratedSchema.FieldsList[2]);
            columns.Add(new Int32Array.Builder().Append(value).Build());
        }
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, 1);
    }

    private static async Task<(int X, int G)> ReadGeneratedAsync(DeltaTable table)
    {
        await foreach (var batch in table.ReadAllAsync())
            return (((Int32Array)batch.Column("x")).GetValue(0)!.Value, ((Int32Array)batch.Column("g")).GetValue(0)!.Value);
        throw new InvalidOperationException("empty");
    }

    [Theory]
    [InlineData("setG")]
    [InlineData("star")]
    [InlineData("columns")]
    public async Task AnAssignedGeneratedColumn_ThatDisagrees_IsRefused(string how)
    {
        await using var table = await CreateGeneratedAsync();
        await table.WriteAsync([Generated(1, 1)]);
        var action = how switch
        {
            "setG" => MergeAction.UpdateSet(("g", E("999"))),
            "star" => MergeAction.Update,
            _ => MergeAction.UpdateColumns("x", "g"),
        };

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync(
            [Generated(1, 5, g: 999)], new MergeOptions { Keys = ["id"], WhenMatched = [action] }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.GeneratedColumnMismatch, e.ErrorCode);
        Assert.Equal((1, 2), await ReadGeneratedAsync(table));
    }

    [Fact]
    public async Task AnAssignedGeneratedColumn_ThatAgrees_IsKept()
    {
        await using var table = await CreateGeneratedAsync();
        await table.WriteAsync([Generated(1, 1)]);

        await table.MergeAsync([Generated(1, 0)], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("x", E("3")), ("g", E("6")))],
        });

        Assert.Equal((3, 6), await ReadGeneratedAsync(table));
    }

    [Fact]
    public async Task AnOmittedGeneratedColumn_IsRecomputed()
    {
        await using var table = await CreateGeneratedAsync();
        await table.WriteAsync([Generated(1, 1)]);

        await table.MergeAsync([Generated(1, 5)], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("x", E("target.x + source.x")))],
        });

        Assert.Equal((6, 12), await ReadGeneratedAsync(table));
    }

    [Fact]
    public async Task AnInsertedGeneratedColumn_IsCheckedOrComputed()
    {
        await using var table = await CreateGeneratedAsync();

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([Generated(1, 5)], new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), ("x", E("source.x")), ("g", E("999")))],
        }).AsTask());
        Assert.Equal(DeltaTableErrorCodes.GeneratedColumnMismatch, e.ErrorCode);

        await table.MergeAsync([Generated(1, 5)], new MergeOptions
        {
            Keys = ["id"], WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), ("x", E("source.x")))],
        });
        Assert.Equal((5, 10), await ReadGeneratedAsync(table));
    }

    // ── helpers ──

    private static RecordBatch Batch(Apache.Arrow.Schema schema, params long[][] columns) =>
        new(schema, columns.Select(c => (IArrowArray)new Int64Array.Builder().AppendRange(c).Build()).ToList(), columns[0].Length);

    private static readonly Apache.Arrow.Schema TypedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, true))
        .Field(new Field("i", Int32Type.Default, true))
        .Field(new Field("l", Int64Type.Default, true))
        .Field(new Field("s", StringType.Default, true))
        .Field(new Field("d", Date32Type.Default, true))
        .Field(new Field("dec", new Decimal128Type(10, 2), true))
        .Field(new Field("b", BooleanType.Default, true))
        .Field(new Field("f", DoubleType.Default, true))
        .Build();

    private async Task<DeltaTable> CreateTypedAsync()
    {
        var table = await CreateAsync(TypedSchema);
        await table.WriteAsync([new RecordBatch(TypedSchema,
        [
            new Int32Array.Builder().Append(1).Build(),
            new Int32Array.Builder().Append(1).Build(),
            new Int64Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("a").Build(),
            new Date32Array.Builder().Append(new DateTime(2020, 1, 1)).Build(),
            new Decimal128Array.Builder(new Decimal128Type(10, 2)).Append(1.00m).Build(),
            new BooleanArray.Builder().Append(false).Build(),
            new DoubleArray.Builder().Append(1.0).Build(),
        ], 1)]);
        return table;
    }

    private static RecordBatch IdOnly(int id) => new(
        new Apache.Arrow.Schema.Builder().Field(new Field("id", Int32Type.Default, true)).Build(),
        [new Int32Array.Builder().Append(id).Build()], 1);

    private static async Task<string> ReadTypedAsync(DeltaTable table, string column)
    {
        await foreach (var batch in table.ReadAllAsync())
        {
            var array = batch.Column(column);
            if (array.IsNull(0))
                return "";
            return array switch
            {
                Int32Array a => a.GetValue(0)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Int64Array a => a.GetValue(0)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                StringArray a => a.GetString(0)!,
                Date32Array a => a.GetDateTime(0)!.Value.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
                Decimal128Array a => a.GetSqlDecimal(0)!.Value.ToString(),
                BooleanArray a => a.GetValue(0)!.Value.ToString(),
                DoubleArray a => a.GetValue(0)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                _ => throw new NotSupportedException(array.GetType().Name),
            };
        }
        throw new InvalidOperationException("empty");
    }
}
