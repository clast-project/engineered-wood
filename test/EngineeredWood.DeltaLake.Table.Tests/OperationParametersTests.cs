// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>commitInfo.operationParameters</c> for DELETE, UPDATE, MERGE and replaceWhere (#591): the condition each was
/// given, as Spark SQL, under Spark's keys and in Spark's shapes (<c>OperationParametersInteropTests</c>
/// checks both against Spark). The table starts as two files, (1, 2, 3) and (4, 5).
/// </summary>
public class OperationParametersTests : IDisposable
{
    private readonly string _tempDir;

    public OperationParametersTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_opparams_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
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
            value.Append(i == 2 ? "it's" : $"v{i}");
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

    private async Task<DeltaTable> CreateAsync(bool deletionVectors = false)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema, new DeltaTableOptions { CheckpointInterval = 0 },
            enableDeletionVectors: deletionVectors);
        await table.WriteAsync([Rows(1, 2, 3)]);
        await table.WriteAsync([Rows(4, 5)]);
        return table;
    }

    /// <summary>The commit's operationParameters, each value as the string Spark writes it.</summary>
    private static async Task<Dictionary<string, string>> ParametersAsync(DeltaTable table, long version)
    {
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version != version)
                continue;
            var result = new Dictionary<string, string>(StringComparer.Ordinal);
            if (entry.OperationParameters is { } json)
            {
                using var document = JsonDocument.Parse(json);
                foreach (var property in document.RootElement.EnumerateObject())
                    result[property.Name] = property.Value.GetString()!;
            }
            return result;
        }
        throw new InvalidOperationException($"No version {version}.");
    }

    private static string[] Conditions(string json) => JsonSerializer.Deserialize<string[]>(json)!;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADelete_RecordsItsConditionAsSql(bool deletionVectors)
    {
        await using var table = await CreateAsync(deletionVectors);
        var (_, version) = await table.DeleteAsync(Ex.In("id", LiteralValue.Of(2L), LiteralValue.Of(4L)));

        var parameters = await ParametersAsync(table, version);
        Assert.Equal(["predicate"], parameters.Keys);
        Assert.Equal("[\"(id IN (2L, 4L))\"]", parameters["predicate"]);
    }

    /// <summary>The recorded text is the condition the DELETE was given: it parses back to the same tree, a quoted
    /// string included (the reason <c>Predicate.ToString()</c> could not be recorded).</summary>
    [Fact]
    public async Task ARecordedCondition_ParsesBackToTheSameCondition()
    {
        await using var table = await CreateAsync();
        var condition = Ex.Or(Ex.Equal("value", "it's"), Ex.Not(Ex.GreaterThan("id", 3L)));
        var (deleted, version) = await table.DeleteAsync(condition);
        Assert.Equal(3, deleted);

        string sql = Assert.Single(Conditions((await ParametersAsync(table, version))["predicate"]));
        Assert.Equal(@"((value = 'it\'s') OR (NOT (id > 3L)))", sql);
        Assert.Equal(condition, SparkSqlParser.ParsePredicate(sql));
    }

    [Fact]
    public async Task AnUpdate_RecordsItsCondition()
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.UpdateAsync(Ex.Equal("id", 3L), Renamed);

        Assert.Equal("[\"(id = 3L)\"]", (await ParametersAsync(table, version))["predicate"]);
    }

    /// <summary>A delegate has no text. Nothing is recorded rather than a placeholder, which would read as a
    /// condition: for an UPDATE, Spark's <c>[]</c> means it had no WHERE clause.</summary>
    [Fact]
    public async Task ADelegateCondition_RecordsNoParameters()
    {
        await using var table = await CreateAsync();

        var (_, deleted) = await table.DeleteAsync(batch => (BooleanArray)new BooleanArray.Builder()
            .AppendRange(Enumerable.Range(0, batch.Length).Select(i => ((Int64Array)batch.Column(0)).GetValue(i) == 1))
            .Build());
        Assert.DoesNotContain("predicate", (await ParametersAsync(table, deleted)).Keys);

        var (_, updated) = await table.UpdateAsync(
            batch => (BooleanArray)new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, batch.Length)).Build(),
            Renamed);
        Assert.DoesNotContain("predicate", (await ParametersAsync(table, updated)).Keys);
    }

    /// <summary>A condition with no Spark SQL records nothing too, and the DELETE itself still lands.</summary>
    [Fact]
    public async Task AConditionWithNoSql_RecordsNoParameters_AndStillCommits()
    {
        await using var table = await CreateAsync();
        var (deleted, version) = await table.DeleteAsync(Ex.Equal("id", LiteralValue.Of(2u)));

        Assert.Equal(1, deleted);
        Assert.DoesNotContain("predicate", (await ParametersAsync(table, version)).Keys);
    }

    [Fact]
    public async Task AMerge_RecordsItsOnConditionAndClauses()
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.MergeAsync([Rows(2, 6)], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched =
            [
                MergeAction.Update.When(Ex.NotEqual(UnboundReference.FromParts(["source", "value"]), Ex.Lit("x"))),
                MergeAction.Delete,
            ],
            WhenNotMatched = [MergeAction.Insert],
            WhenNotMatchedBySource =
            [
                MergeAction.Delete.When(Ex.GreaterThan(UnboundReference.FromParts(["target", "id"]), Ex.Lit(5L))),
                MergeAction.UpdateSet(new MergeAssignment("value", Ex.Lit("gone"))),
            ],
        });

        var parameters = await ParametersAsync(table, version);
        Assert.Equal(
            new Dictionary<string, string>
            {
                ["predicate"] = "[\"(target.id = source.id)\"]",
                ["matchedPredicates"] =
                    "[{\"predicate\":\"(source.value <> 'x')\",\"actionType\":\"update\"},{\"actionType\":\"delete\"}]",
                ["notMatchedPredicates"] = "[{\"actionType\":\"insert\"}]",
                ["notMatchedBySourcePredicates"] =
                    "[{\"predicate\":\"(target.id > 5L)\",\"actionType\":\"delete\"},{\"actionType\":\"update\"}]",
            },
            parameters);
    }

    /// <summary>The parameters are taken when the MERGE is staged. Its options hold lists the caller still owns, and
    /// changing them before the commit must not change what is recorded for the rows already staged.</summary>
    [Fact]
    public async Task AMerge_RecordsTheOptionsItRanWith_NotLaterChangesToThem()
    {
        await using var table = await CreateAsync();
        var keys = new List<string> { "id" };
        var whenMatched = new List<MergeAction> { MergeAction.Delete };

        long version;
        await using (var txn = table.StartTransaction())
        {
            await txn.MergeAsync([Rows(2)], new MergeOptions { Keys = keys, WhenMatched = whenMatched });
            keys[0] = "value";
            whenMatched[0] = MergeAction.Update.When(Ex.Equal("id", 99L));
            version = await txn.CommitAsync();
        }

        var parameters = await ParametersAsync(table, version);
        Assert.Equal("[\"(target.id = source.id)\"]", parameters["predicate"]);
        Assert.Equal("[{\"actionType\":\"delete\"}]", parameters["matchedPredicates"]);
    }

    /// <summary>A MERGE on several keys equates each, and an empty clause list is recorded empty, as Spark does.</summary>
    [Fact]
    public async Task AMergeOnSeveralKeys_RecordsTheirConjunction()
    {
        await using var table = await CreateAsync();
        var (_, version) = await table.MergeAsync([Rows(6)], new MergeOptions
        {
            Keys = ["id", "value"],
            WhenNotMatched = [MergeAction.Insert],
        });

        var parameters = await ParametersAsync(table, version);
        Assert.Equal("[\"((target.id = source.id) AND (target.value = source.value))\"]", parameters["predicate"]);
        Assert.Equal("[]", parameters["matchedPredicates"]);
        Assert.Equal("[]", parameters["notMatchedBySourcePredicates"]);
    }

    [Fact]
    public async Task AReplaceWhere_RecordsSparksWriteParameters()
    {
        await using var table = await CreateAsync();
        long version = await table.OverwriteWhereAsync(
            [Rows(2)], Ex.And(Ex.GreaterThanOrEqual("id", 2L), Ex.LessThan("id", 4L)));

        Assert.Equal(
            new Dictionary<string, string>
            {
                ["mode"] = "Overwrite",
                ["partitionBy"] = "[]",
                ["predicate"] = "((id >= 2L) AND (id < 4L))",
            },
            await ParametersAsync(table, version));
    }

    /// <summary>Two DELETEs in one transaction list both conditions, as Spark's <c>predicate</c> is a list. Mixed
    /// work, or a transaction recording another operation, records none: the parameters describe one operation.</summary>
    [Fact]
    public async Task ATransaction_ListsOneKindsConditions_AndOtherwiseRecordsNone()
    {
        await using var table = await CreateAsync();

        long both;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 1L));
            await txn.DeleteAsync(Ex.Equal("id", 4L));
            both = await txn.CommitAsync();
        }
        Assert.Equal(["(id = 1L)", "(id = 4L)"], Conditions((await ParametersAsync(table, both))["predicate"]));

        long mixed;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 2L));
            await txn.WriteAsync([Rows(7)]);
            mixed = await txn.CommitAsync();
        }
        Assert.DoesNotContain("predicate", (await ParametersAsync(table, mixed)).Keys);

        long renamed;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 3L));
            txn.Operation = "PURGE";
            renamed = await txn.CommitAsync();
        }
        Assert.DoesNotContain("predicate", (await ParametersAsync(table, renamed)).Keys);
    }

    /// <summary>One DELETE by condition and one by a delegate: the list would be missing a condition, so nothing is
    /// recorded.</summary>
    [Fact]
    public async Task ATransactionWithOneDelegateCondition_RecordsNone()
    {
        await using var table = await CreateAsync();
        long version;
        await using (var txn = table.StartTransaction())
        {
            await txn.DeleteAsync(Ex.Equal("id", 1L));
            await txn.DeleteAsync(batch => (BooleanArray)new BooleanArray.Builder()
                .AppendRange(Enumerable.Range(0, batch.Length).Select(i => ((Int64Array)batch.Column(0)).GetValue(i) == 4))
                .Build());
            version = await txn.CommitAsync();
        }
        Assert.DoesNotContain("predicate", (await ParametersAsync(table, version)).Keys);
    }
}
