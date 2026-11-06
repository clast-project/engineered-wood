// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <c>commitInfo.operationParameters</c> of a DELETE, UPDATE, MERGE and replaceWhere against Spark's (#591). Twin
/// tables get the same operation, one from this library and one from Spark SQL, and this library's parameters must
/// have Spark's keys and Spark's value shapes. The condition text is not compared: Spark's is its Catalyst tree's
/// debug text (<c>(id#494L = 2)</c>), and this library's is Spark SQL. Instead Spark runs the recorded text, which
/// must parse there and select the rows this library's operation did.
/// </summary>
[Collection("Interop")]
public class OperationParametersInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public OperationParametersInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_opparams_interop_{Guid.NewGuid():N}");
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
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("d", Date32Type.Default, true))
        .Field(new Field("amt", new Decimal128Type(10, 2), true))
        .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true))
        .Field(new Field("dbl", DoubleType.Default, true))
        .Field(new Field("bin", BinaryType.Default, true))
        .Build();

    private static readonly DateTimeOffset Epoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Rows 1 to 5; row <c>i</c> holds day <c>i</c>, <c>i.50</c>, hour <c>i</c>, <c>i * 1.5</c> and the
    /// byte <c>i</c>. Row 2's value has a quote in it, row 3's a backslash, and row 5's is null.</summary>
    private static RecordBatch Rows()
    {
        var id = new Int64Array.Builder();
        var value = new StringArray.Builder();
        var d = new Date32Array.Builder();
        var amt = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var dbl = new DoubleArray.Builder();
        var bin = new BinaryArray.Builder();
        for (int i = 1; i <= 5; i++)
        {
            id.Append(i);
            if (i == 5)
                value.AppendNull();
            else
                value.Append(i switch { 2 => "it's", 3 => @"a\b", _ => $"v{i}" });
            d.Append(Epoch.AddDays(i).DateTime);
            amt.Append(i + 0.50m);
            ts.Append(Epoch.AddHours(i));
            dbl.Append(i * 1.5);
            bin.Append(new byte[] { (byte)i });
        }
        return new RecordBatch(TableSchema,
            [id.Build(), value.Build(), d.Build(), amt.Build(), ts.Build(), dbl.Build(), bin.Build()], 5);
    }

    private async Task CreateTwinsAsync()
    {
        foreach (string dir in new[] { _ours, _theirs })
        {
            await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(dir), TableSchema);
            await table.WriteAsync([Rows()]);
        }
    }

    private async Task<Dictionary<string, string>> OurParametersAsync(long version)
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours));
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version != version)
                continue;
            using var document = JsonDocument.Parse(entry.OperationParameters ?? "{}");
            return document.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString()!);
        }
        throw new InvalidOperationException($"No version {version}.");
    }

    /// <summary>Runs Spark's statement on its twin and returns the operationParameters of its latest commit.</summary>
    private Dictionary<string, string> TheirParameters(string sql)
    {
        var result = Spark.Invoke("sql", new
        {
            path = _theirs,
            sql = new[] { sql, "DESCRIBE HISTORY delta.`{path}` LIMIT 1" },
        });
        var row = result.GetProperty("statements")[1].GetProperty("rows")[0];
        return row.GetProperty("operationParameters").EnumerateObject()
            .ToDictionary(p => p.Name, p => p.Value.GetString()!);
    }

    private static long[] Ids(string path)
    {
        var result = Spark.Invoke("sql", new { path, sql = new[] { "SELECT id FROM delta.`{path}` ORDER BY id" } });
        return result.GetProperty("statements")[0].GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("id").GetInt64()).ToArray();
    }

    private static string[] JsonArray(string json) => JsonSerializer.Deserialize<string[]>(json)!;

    /// <summary>Each clause's action and whether it carries a condition: the part of a MERGE clause list that does
    /// not depend on how the condition is spelled.</summary>
    private static string[] Clauses(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateArray()
            .Select(c => c.GetProperty("actionType").GetString() + (c.TryGetProperty("predicate", out _) ? "?" : ""))
            .ToArray();
    }

    private static readonly Dictionary<string, Predicate> ConditionsByName = new()
    {
        ["quote"] = Ex.Equal("value", "it's"),
        ["backslash"] = Ex.Equal("value", @"a\b"),
        ["in"] = Ex.In("id", LiteralValue.Of(2L), LiteralValue.Of(4L)),
        ["not"] = Ex.And(Ex.Not(Ex.GreaterThan("id", 3L)), Ex.IsNotNull("value")),
        ["is null"] = Ex.IsNull("value"),
        ["decimal"] = Ex.GreaterThan("amt", 2.50m),
        ["date literal"] = SparkSqlParser.ParsePredicate("d = DATE '2026-01-03'"),
#if NET6_0_OR_GREATER
        ["date"] = Ex.Equal("d", new DateOnly(2026, 1, 3)),
#endif
        ["timestamp"] = Ex.LessThan("ts", Epoch.AddHours(2.5)),
        ["double"] = Ex.GreaterThanOrEqual("dbl", 4.5),
        ["binary"] = Ex.Equal("bin", new byte[] { 2 }),
        ["arithmetic"] = Ex.Equal(Ex.Call("-", Ex.Ref("id"), Ex.Lit(1L)), Ex.Lit(2L)),
        ["starts with"] = Ex.StartsWith("value", "v"),
        ["not starts with"] = Ex.NotStartsWith("value", "v"),
    };

    public static TheoryData<string> Conditions() => new(ConditionsByName.Keys);

    /// <summary>The condition, given to this library's DELETE as a tree, is recorded as text Spark parses and
    /// that deletes the same rows from Spark's twin; and Spark's own DELETE records the same key, a list of one.</summary>
    [SkippableTheory]
    [MemberData(nameof(Conditions))]
    public async Task ADeletesRecordedCondition_DeletesTheSameRowsInSpark(string name)
    {
        Spark.Require();
        await CreateTwinsAsync();

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            (_, version) = await table.DeleteAsync(ConditionsByName[name]);

        var ours = await OurParametersAsync(version);
        Assert.Equal(["predicate"], ours.Keys);
        string recorded = Assert.Single(JsonArray(ours["predicate"]));

        var theirs = TheirParameters($"DELETE FROM delta.`{{path}}` WHERE {recorded}");
        Assert.Equal(["predicate"], theirs.Keys);
        Assert.Single(JsonArray(theirs["predicate"]));

        long[] remaining = Ids(_ours);
        Assert.NotEqual(5, remaining.Length);
        Assert.Equal(remaining, Ids(_theirs));
    }

    [SkippableFact]
    public async Task AnUpdate_RecordsSparksKeys()
    {
        Spark.Require();
        await CreateTwinsAsync();

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            (_, version) = await table.UpdateAsync(Ex.Equal("id", 3L), batch =>
            {
                var values = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    values.Append("x");
                var columns = Enumerable.Range(0, batch.ColumnCount).Select(batch.Column).ToArray();
                columns[1] = values.Build();
                return new RecordBatch(batch.Schema, columns, batch.Length);
            });
        }

        var ours = await OurParametersAsync(version);
        var theirs = TheirParameters("UPDATE delta.`{path}` SET value = 'x' WHERE id = 3");
        Assert.Equal(theirs.Keys.OrderBy(k => k), ours.Keys.OrderBy(k => k));
        Assert.Equal(["(id = 3L)"], JsonArray(ours["predicate"]));
        Assert.Single(JsonArray(theirs["predicate"]));
    }

    [SkippableFact]
    public async Task AMerge_RecordsSparksKeysAndClauses()
    {
        Spark.Require();
        await CreateTwinsAsync();

        var source = new RecordBatch(new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, false))
                .Field(new Field("value", StringType.Default, true))
                .Build(),
            [new Int64Array.Builder().Append(2).Append(6).Build(), new StringArray.Builder().Append("B").Append("f").Build()],
            2);

        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            (_, version) = await table.MergeAsync([source], new MergeOptions
            {
                Keys = ["id"],
                WhenMatched =
                [
                    MergeAction.UpdateColumns("value").When(
                        Ex.NotEqual(UnboundReference.FromParts(["source", "value"]), Ex.Lit("zz"))),
                    MergeAction.Delete,
                ],
                WhenNotMatched = [MergeAction.InsertColumns("id", "value")],
                WhenNotMatchedBySource = [MergeAction.Delete.When(Ex.Equal(
                    UnboundReference.FromParts(["target", "id"]), Ex.Lit(LiteralValue.Of(5L))))],
            });
        }

        var ours = await OurParametersAsync(version);
        var theirs = TheirParameters(
            "MERGE INTO delta.`{path}` t USING (SELECT * FROM VALUES (2L, 'B'), (6L, 'f') AS s(id, value)) s "
            + "ON t.id = s.id WHEN MATCHED AND s.value <> 'zz' THEN UPDATE SET value = s.value "
            + "WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value) "
            + "WHEN NOT MATCHED BY SOURCE AND t.id = 5 THEN DELETE");

        Assert.Equal(theirs.Keys.OrderBy(k => k), ours.Keys.OrderBy(k => k));
        Assert.Equal(["(target.id = source.id)"], JsonArray(ours["predicate"]));
        Assert.Single(JsonArray(theirs["predicate"]));
        foreach (string key in new[] { "matchedPredicates", "notMatchedPredicates", "notMatchedBySourcePredicates" })
            Assert.Equal(Clauses(theirs[key]), Clauses(ours[key]));
    }

    /// <summary>A replaceWhere records Spark's three keys, and its condition as Spark SQL, which Spark records for
    /// <c>INSERT … REPLACE WHERE</c> too.</summary>
    [SkippableFact]
    public async Task AReplaceWhere_RecordsSparksParameters()
    {
        Spark.Require();
        await CreateTwinsAsync();

        var row = Rows().Slice(1, 1);
        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
            version = await table.OverwriteWhereAsync([row], Ex.And(Ex.GreaterThanOrEqual("id", 2L), Ex.LessThan("id", 4L)));

        var ours = await OurParametersAsync(version);
        var theirs = TheirParameters(
            "INSERT INTO delta.`{path}` REPLACE WHERE id >= 2L AND id < 4L "
            + "SELECT * FROM delta.`{path}` VERSION AS OF 1 WHERE id = 2");

        Assert.Equal(theirs, ours);
    }
}
