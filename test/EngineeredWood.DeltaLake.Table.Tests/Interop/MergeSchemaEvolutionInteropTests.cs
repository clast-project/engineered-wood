// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using static EngineeredWood.DeltaLake.Table.Tests.Interop.MergeSchemaInteropTests;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> engineered-wood's MERGE with <see cref="MergeOptions.MergeSchema"/> (#604) against Spark's
/// MERGE under <c>spark.databricks.delta.schema.autoMerge.enabled</c>: each case has Spark create and seed two
/// identical tables, MERGEs one with <see cref="DeltaTable.MergeAsync"/> and the other with the equivalent
/// <c>MERGE INTO</c>, and requires the same logical schema (names, types, nullability, recorded type changes), the
/// same table features, the same rows and the same change feed, and that Spark can write through the table EW
/// evolved. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class MergeSchemaEvolutionInteropTests : IDisposable
{
    private const string AutoMerge = "spark.databricks.delta.schema.autoMerge.enabled";

    private readonly string _ours;
    private readonly string _theirs;

    public MergeSchemaEvolutionInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_merge_evolution_interop_{Guid.NewGuid():N}");
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

    private static Expression E(string sql) => SparkSqlParser.ParseExpression(sql);
    private static Predicate P(string sql) => SparkSqlParser.ParsePredicate(sql);

    private static void CreateAndSeed(string dir, bool columnMapping)
    {
        string properties = "'delta.enableTypeWidening'='true', 'delta.enableChangeDataFeed'='true'"
            + (columnMapping ? ", 'delta.columnMapping.mode'='name'" : "");
        Sql(dir,
            $"CREATE TABLE delta.`{{path}}` (id INT, p STRING, st STRUCT<a: INT, c: STRING>) USING delta "
            + $"TBLPROPERTIES ({properties})",
            "INSERT INTO delta.`{path}` VALUES (1, 'a', named_struct('a', 1, 'c', 'c1')), "
            + "(2, 'b', named_struct('a', 2, 'c', 'c2'))");
    }

    private static IArrowArray Ints(params int[] values) => new Int32Array.Builder().AppendRange(values).Build();
    private static IArrowArray Longs(params long[] values) => new Int64Array.Builder().AppendRange(values).Build();
    private static IArrowArray Strings(params string[] values) => new StringArray.Builder().AppendRange(values).Build();

    private static StructArray Struct(params (string Name, IArrowArray Values)[] fields) => new(
        new StructType(fields.Select(f => new Field(f.Name, f.Values.Data.DataType, true)).ToList()),
        fields[0].Values.Length, fields.Select(f => f.Values).ToList(), ArrowBuffer.Empty);

    private static RecordBatch Batch(params (string Name, IArrowArray Values)[] columns) => new(
        new Apache.Arrow.Schema(columns.Select(c => new Field(c.Name, c.Values.Data.DataType, true)).ToList(), null),
        columns.Select(c => c.Values).ToList(), columns[0].Values.Length);

    /// <summary>A case: the source as Spark SQL and as a batch, the clauses as Spark SQL and as options.</summary>
    private sealed record Case(string SourceSql, RecordBatch Source, string Clauses, MergeOptions Options, bool Evolve);

    private static Case Build(string name) => name switch
    {
        // Every column copied: a key widened int -> long, a new top-level column, a new struct field and one the
        // source's struct lacks, its fields in another order.
        "star" or "star-column-mapping" => new Case(
            "SELECT * FROM VALUES (1L, 'A', named_struct('b', 'x', 'a', 10), 'e1'), "
            + "(3L, 'C', named_struct('b', 'z', 'a', 30), 'e3') AS s(id, p, st, extra)",
            Batch(("id", Longs(1, 3)), ("p", Strings("A", "C")),
                ("st", Struct(("b", Strings("x", "z")), ("a", Ints(10, 30)))), ("extra", Strings("e1", "e3"))),
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update], WhenNotMatched = [MergeAction.Insert] },
            Evolve: true),

        // A new column added by assignments to its name, with the source column's type.
        "explicit" => new Case(
            "SELECT * FROM VALUES (1, 'A', 5L), (3, 'C', 7L) AS s(id, p, extra)",
            Batch(("id", Ints(1, 3)), ("p", Strings("A", "C")), ("extra", Longs(5, 7))),
            "WHEN MATCHED THEN UPDATE SET extra = s.extra + 1 "
            + "WHEN NOT MATCHED THEN INSERT (id, extra) VALUES (s.id, s.extra)",
            new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = [MergeAction.UpdateSet(("extra", E("source.extra + 1")))],
                WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), ("extra", E("source.extra")))],
            },
            Evolve: true),

        // A source without `p` and with a struct lacking `c`: the update keeps both, the insert writes NULL.
        "lacks" => new Case(
            "SELECT * FROM VALUES (1, named_struct('a', 10)), (3, named_struct('a', 30)) AS s(id, st)",
            Batch(("id", Ints(1, 3)), ("st", Struct(("a", Ints(10, 30))))),
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update], WhenNotMatched = [MergeAction.Insert] },
            Evolve: true),

        // A clause no row reaches: the schema evolves in a commit of its own.
        "never" => new Case(
            "SELECT * FROM VALUES (1, 'A', 'e1') AS s(id, p, extra)",
            Batch(("id", Ints(1)), ("p", Strings("A")), ("extra", Strings("e1"))),
            "WHEN MATCHED AND s.id > 100 THEN UPDATE SET *",
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update.When(P("source.id > 100"))] },
            Evolve: true),

        // No evolution: the source's own column is left out by `*`.
        "off" => new Case(
            "SELECT * FROM VALUES (1, 'A', named_struct('a', 10, 'c', 'cA'), 'e1'), "
            + "(3, 'C', named_struct('a', 30, 'c', 'cC'), 'e3') AS s(id, p, st, extra)",
            Batch(("id", Ints(1, 3)), ("p", Strings("A", "C")),
                ("st", Struct(("a", Ints(10, 30)), ("c", Strings("cA", "cC")))), ("extra", Strings("e1", "e3"))),
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update], WhenNotMatched = [MergeAction.Insert] },
            Evolve: false),

        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    public static TheoryData<string> Cases => ["star", "star-column-mapping", "explicit", "lacks", "never", "off"];

    /// <summary>The version's change feed as Spark reads it, each row's JSON without its commit timestamp.</summary>
    private static List<string> Feed(string dir, long version) =>
        [.. Spark.Invoke("read_changes", new { path = dir, start = version, end = version })
            .GetProperty("rows").EnumerateArray()
            .Select(r =>
            {
                var node = JsonNode.Parse(r.GetRawText())!.AsObject();
                node.Remove("_commit_timestamp");
                return node.ToJsonString();
            })
            .OrderBy(x => x, StringComparer.Ordinal)];

    private static long LatestVersion(string dir) =>
        Directory.GetFiles(Path.Combine(dir, "_delta_log"), "*.json").Length - 1;

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    public async Task AMergeWithSchemaEvolution_LeavesWhatSparksLeaves(string name)
    {
        Spark.Require();
        var c = Build(name);
        bool columnMapping = name == "star-column-mapping";
        CreateAndSeed(_ours, columnMapping);
        CreateAndSeed(_theirs, columnMapping);

        Sql(_theirs,
            $"SET {AutoMerge} = {(c.Evolve ? "true" : "false")}",
            $"MERGE INTO delta.`{{path}}` t USING ({c.SourceSql}) s ON t.id = s.id {c.Clauses}",
            $"SET {AutoMerge} = false");
        long version;
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            var options = new MergeOptions
            {
                Keys = c.Options.Keys,
                WhenMatched = c.Options.WhenMatched,
                WhenNotMatched = c.Options.WhenNotMatched,
                MergeSchema = c.Evolve,
            };
            (_, version) = await table.MergeAsync([c.Source], options);
        }

        Assert.Equal(LatestVersion(_theirs), version);
        Assert.Equal(LogicalSchema(_theirs), LogicalSchema(_ours));
        Assert.Equal(Features(_theirs), Features(_ours));
        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
        Assert.Equal(Feed(_theirs, version), Feed(_ours, version));

        // Spark writes through the table EW evolved.
        Sql(_ours, "INSERT INTO delta.`{path}` (id) VALUES (9)");
    }
}
