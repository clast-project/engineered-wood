// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> engineered-wood's key-based MERGE (#563) against Spark's own: each case builds two
/// identical tables, MERGEs one with <see cref="DeltaTable.MergeAsync"/> and the other with the equivalent
/// <c>MERGE INTO</c> run by Spark, and requires the same outcome: the same rows and the same change feed, or a
/// refusal from both. Spark reads both tables, so this is also the proof that what our MERGE commits is a table
/// Spark reads. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class MergeInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public MergeInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_merge_interop_{Guid.NewGuid():N}");
        _ours = Path.Combine(root, "ours");
        _theirs = Path.Combine(root, "theirs");
        Directory.CreateDirectory(_ours);
        Directory.CreateDirectory(_theirs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        string root = Path.GetDirectoryName(_ours)!;
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("ts", Int64Type.Default, true))
        .Build();

    private static RecordBatch Rows(params (long? Id, string Value, long Ts)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        var ts = new Int64Array.Builder();
        foreach (var (id, value, t) in rows)
        {
            if (id is { } i) ids.Append(i); else ids.AppendNull();
            values.Append(value);
            ts.Append(t);
        }
        return new RecordBatch(TableSchema, [ids.Build(), values.Build(), ts.Build()], rows.Length);
    }

    /// <summary>The same SOURCE as Spark SQL, for <c>USING (...) s</c>.</summary>
    private static string SqlSource(params (long? Id, string Value, long Ts)[] rows) =>
        "(SELECT * FROM VALUES "
        + string.Join(", ", rows.Select(r =>
            $"({(r.Id is { } i ? i + "L" : "CAST(NULL AS BIGINT)")}, '{r.Value}', {r.Ts}L)"))
        + " AS s(id, value, ts))";

    /// <summary>Two identical change-feed, row-tracking tables, written by engineered-wood in two commits.</summary>
    private async Task CreateTwinsAsync()
    {
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(dir), TableSchema, enableRowTracking: true,
                configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
            await table.WriteAsync([Rows((1, "a", 10), (2, "b", 20), (3, "c", 30))]);
            await table.WriteAsync([Rows((null, "n", 0), (7, "g", 70))]);
        }
    }

    private static List<(long? Id, string? Value, long? Ts)> SparkRows(string dir) =>
        [.. Spark.Invoke("read", new { path = dir }).GetProperty("rows").EnumerateArray()
            .Select(r => (Long(r, "id"), r.GetProperty("value").GetString(), Long(r, "ts")))
            .OrderBy(r => r.Item1 ?? long.MinValue).ThenBy(r => r.Item2, StringComparer.Ordinal)];

    private static List<(string Type, long? Id, string? Value)> SparkFeed(string dir, long version) =>
        [.. Spark.Invoke("read_changes", new { path = dir, start = version, end = version })
            .GetProperty("rows").EnumerateArray()
            .Select(r => (r.GetProperty("_change_type").GetString()!, Long(r, "id"), r.GetProperty("value").GetString()))
            .OrderBy(r => r.Item1, StringComparer.Ordinal).ThenBy(r => r.Item2 ?? long.MinValue)];

    private static long? Long(JsonElement row, string name) =>
        row.GetProperty(name).ValueKind == JsonValueKind.Null ? null : row.GetProperty(name).GetInt64();

    private async Task<List<(string Type, long? Id, string? Value)>> OurFeedAsync(long version)
    {
        var feed = new List<(string, long?, string?)>();
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        await foreach (var batch in table.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var types = (StringArray)batch.Column("_change_type");
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                feed.Add((types.GetString(i), ids.GetValue(i), values.GetString(i)));
        }
        return [.. feed.OrderBy(r => r.Item1, StringComparer.Ordinal).ThenBy(r => r.Item2 ?? long.MinValue)];
    }

    private async Task<long> OurMergeAsync(RecordBatch source, MergeOptions options)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        return (await table.MergeAsync([source], options)).Version;
    }

    /// <summary>
    /// A conditional upsert with a full-sync delete, over two files, a NULL key on each side and a matched row
    /// whose condition fails: the table and its change feed come out as Spark's MERGE leaves them.
    /// </summary>
    [SkippableFact]
    public async Task AFullSyncUpsert_LeavesWhatSparksMergeLeaves()
    {
        Spark.Require();
        await CreateTwinsAsync();
        (long?, string, long)[] source =
            [(2, "B", 21), (3, "stale", 29), (4, "d", 40), (null, "source-null", 1), (7, "G", 71)];

        long version = await OurMergeAsync(Rows(source), new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.Update.When(Ex.GreaterThan(
                UnboundReference.FromParts(["source", "ts"]), UnboundReference.FromParts(["target", "ts"])))],
            WhenNotMatched = [MergeAction.Insert],
            WhenNotMatchedBySource = [MergeAction.Delete.When(Ex.GreaterThan(
                UnboundReference.FromParts(["target", "ts"]), Ex.Lit(LiteralValue.Of(5L))))],
        });
        Spark.Invoke("sql", new
        {
            path = _theirs,
            sql = new[]
            {
                $"MERGE INTO delta.`{{path}}` t USING {SqlSource(source)} s ON t.id = s.id "
                + "WHEN MATCHED AND s.ts > t.ts THEN UPDATE SET * "
                + "WHEN NOT MATCHED THEN INSERT * "
                + "WHEN NOT MATCHED BY SOURCE AND t.ts > 5 THEN DELETE",
            },
        });

        var theirs = SparkRows(_theirs);
        Assert.Equal(
            new (long?, string?, long?)[]
            {
                (null, "n", 0), (null, "source-null", 1), (2, "B", 21), (3, "c", 30), (4, "d", 40), (7, "G", 71),
            },
            theirs);
        Assert.Equal(theirs, SparkRows(_ours));

        var theirFeed = SparkFeed(_theirs, version);
        Assert.Equal(theirFeed, SparkFeed(_ours, version));
        Assert.Equal(theirFeed, await OurFeedAsync(version));
    }

    /// <summary>
    /// Two source rows on one target key: refused by both when a matched update could apply, whatever its
    /// condition says, and accepted by both when the MERGE only inserts.
    /// </summary>
    [SkippableTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ADuplicatedSourceKey_IsRefusedOrAcceptedAsSparkDecides(bool withMatchedUpdate)
    {
        Spark.Require();
        await CreateTwinsAsync();
        (long?, string, long)[] source = [(1, "old", 5), (1, "new", 15), (9, "i", 90)];
        var options = new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = withMatchedUpdate
                ? [MergeAction.Update.When(
                    Ex.GreaterThan(UnboundReference.FromParts(["source", "ts"]), UnboundReference.FromParts(["target", "ts"])))]
                : [],
            WhenNotMatched = [MergeAction.Insert],
        };
        string sql = $"MERGE INTO delta.`{{path}}` t USING {SqlSource(source)} s ON t.id = s.id "
            + (withMatchedUpdate ? "WHEN MATCHED AND s.ts > t.ts THEN UPDATE SET * " : "")
            + "WHEN NOT MATCHED THEN INSERT *";

        var spark = Spark.InvokeRaw("sql", new { path = _theirs, sql = new[] { sql } });

        if (withMatchedUpdate)
        {
            Assert.False(spark.GetProperty("ok").GetBoolean());
            Assert.Contains(DeltaTableErrorCodes.MultipleSourceRowsMatchingTargetRow, spark.GetProperty("error").GetString());
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => OurMergeAsync(Rows(source), options));
            Assert.Equal(DeltaTableErrorCodes.MultipleSourceRowsMatchingTargetRow, ex.ErrorCode);
        }
        else
        {
            Assert.True(spark.GetProperty("ok").GetBoolean(), spark.ToString());
            await OurMergeAsync(Rows(source), options);
            Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        }
    }

    private static Expression E(string sql) => EngineeredWood.Expressions.Sql.SparkSqlParser.ParseExpression(sql);
    private static Predicate P(string sql) => EngineeredWood.Expressions.Sql.SparkSqlParser.ParsePredicate(sql);

    /// <summary>
    /// Several clauses of each kind, tried in order, with assigned values reading both rows, a NOT MATCHED BY SOURCE
    /// update, and NULL keys on each side (#575, #576): the table and its change feed come out as Spark's MERGE
    /// leaves them.
    /// </summary>
    [SkippableFact]
    public async Task OrderedClausesWithAssignedValues_LeaveWhatSparksMergeLeaves()
    {
        Spark.Require();
        await CreateTwinsAsync();
        (long?, string, long)[] source =
            [(1, "del", 11), (2, "B", 25), (3, "C", 5), (4, "d", 40), (5, "e", 60), (null, "sn", 1)];

        long version = await OurMergeAsync(Rows(source), new MergeOptions
        {
            Keys = ["id"],
            WhenMatched =
            [
                MergeAction.Delete.When(P("source.value = 'del'")),
                MergeAction.UpdateSet(("value", E("concat(target.value, source.value)")), ("ts", E("target.ts + source.ts")))
                    .When(P("source.ts > target.ts")),
                MergeAction.Update.When(P("source.ts < target.ts")),
            ],
            WhenNotMatched =
            [
                MergeAction.InsertValues(("id", E("source.id")), ("value", E("upper(source.value)")), ("ts", E("0")))
                    .When(P("source.ts > 50")),
                MergeAction.Insert,
            ],
            WhenNotMatchedBySource =
            [
                MergeAction.UpdateSet(("value", E("'orphan'")), ("ts", E("target.ts * 2"))).When(P("target.ts > 25")),
                MergeAction.Delete.When(P("target.id IS NULL")),
            ],
        });
        Spark.Invoke("sql", new
        {
            path = _theirs,
            sql = new[]
            {
                $"MERGE INTO delta.`{{path}}` t USING {SqlSource(source)} s ON t.id = s.id "
                + "WHEN MATCHED AND s.value = 'del' THEN DELETE "
                + "WHEN MATCHED AND s.ts > t.ts THEN UPDATE SET value = concat(t.value, s.value), ts = t.ts + s.ts "
                + "WHEN MATCHED AND s.ts < t.ts THEN UPDATE SET * "
                + "WHEN NOT MATCHED AND s.ts > 50 THEN INSERT (id, value, ts) VALUES (s.id, upper(s.value), 0) "
                + "WHEN NOT MATCHED THEN INSERT * "
                + "WHEN NOT MATCHED BY SOURCE AND t.ts > 25 THEN UPDATE SET value = 'orphan', ts = t.ts * 2 "
                + "WHEN NOT MATCHED BY SOURCE AND t.id IS NULL THEN DELETE",
            },
        });

        var theirs = SparkRows(_theirs);
        Assert.Equal(
            new (long?, string?, long?)[]
            {
                (null, "sn", 1), (2, "bB", 45), (3, "C", 5), (4, "d", 40), (5, "E", 0), (7, "orphan", 140),
            },
            theirs);
        Assert.Equal(theirs, SparkRows(_ours));

        var theirFeed = SparkFeed(_theirs, version);
        Assert.Equal(theirFeed, SparkFeed(_ours, version));
        Assert.Equal(theirFeed, await OurFeedAsync(version));
    }

    /// <summary>
    /// An assigned value of another type is converted as Spark's MERGE converts it -- an ANSI cast to the column's
    /// type -- or refused by both: malformed input, and a value the column cannot hold.
    /// </summary>
    [SkippableTheory]
    [InlineData("ts", "'42'", null)]
    [InlineData("ts", "2.7D", null)]
    [InlineData("ts", "1.5BD", null)]
    [InlineData("value", "s.ts * 3", null)]
    [InlineData("value", "DATE'2021-02-03'", null)]
    [InlineData("ts", "'abc'", "CAST_INVALID_INPUT")]
    [InlineData("ts", "9.3E18D", DeltaTableErrorCodes.CastOverflowInTableWrite)]
    [InlineData("ts", "t.ts + 9223372036854775807L", "ARITHMETIC_OVERFLOW")]
    public async Task AnAssignedValue_IsConvertedOrRefusedAsSparkDoes(string column, string sparkValue, string? error)
    {
        Spark.Require();
        await CreateTwinsAsync();
        (long?, string, long)[] source = [(2, "B", 25)];
        string ours = sparkValue.Replace("s.", "source.").Replace("t.", "target.");
        var options = new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, E(ours)))],
        };
        string sql = $"MERGE INTO delta.`{{path}}` t USING {SqlSource(source)} s ON t.id = s.id "
            + $"WHEN MATCHED THEN UPDATE SET {column} = {sparkValue}";

        var spark = Spark.InvokeRaw("sql", new { path = _theirs, sql = new[] { sql } });

        if (error is null)
        {
            Assert.True(spark.GetProperty("ok").GetBoolean(), spark.ToString());
            await OurMergeAsync(Rows(source), options);
            Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        }
        else
        {
            Assert.False(spark.GetProperty("ok").GetBoolean());
            Assert.Contains(error, spark.GetProperty("error").GetString());
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => OurMergeAsync(Rows(source), options));
            Assert.Contains(error, ex is DeltaFormatException d ? d.ErrorCode + " " + d.Message : ex.Message);
        }
    }
}
