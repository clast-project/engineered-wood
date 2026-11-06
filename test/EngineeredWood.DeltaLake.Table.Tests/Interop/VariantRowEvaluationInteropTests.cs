// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Row-level <c>variant_get</c> (#681) against Spark: the rows a variant-path predicate selects, on a table Spark
/// wrote and on one EW wrote shredded; a DELETE by such a predicate; and a <c>variant_get</c> CHECK constraint
/// added by either side and enforced by the other, NULL counting as a violation.
/// </summary>
[Collection("Interop")]
public class VariantRowEvaluationInteropTests : IDisposable
{
    private readonly string _tempDir;

    public VariantRowEvaluationInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_rows_xval_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    /// <summary>Spark 4.1+ (GA variant); variant_get in a Delta CHECK was measured on delta-spark 4.3.1.</summary>
    private static void RequireGaVariant()
    {
        Spark.Require();
        string version = Spark.Version ?? "";
        int i = version.IndexOf("4.", StringComparison.Ordinal);
        var parts = i < 0 ? [] : version.Substring(i).Split('.', ' ');
        Skip.IfNot(parts.Length >= 2 && int.TryParse(parts[1], out int minor) && minor >= 1,
            $"variant_get over Delta needs Spark 4.1+; resolved {version}");
    }

    private static readonly (long Id, string? Json)[] Rows =
    [
        (1, "{\"a\":1,\"n\":1.5}"),
        (2, "{\"a\":9,\"n\":10}"),
        (3, "{\"a\":\"7\",\"n\":-2.5}"),
        (4, "{\"a\":\"x\"}"),
        (5, "{\"b\":{\"a\":3}}"),
        (6, "[1,2]"),
        (7, "7"),
        (8, null),
        (9, "{\"a\":null,\"n\":null}"),
        (10, "{\"a\":1.9,\"n\":3}"),
        (11, "{\"a\":[4,5]}"),
        (12, "{\"a\":{\"c\":2}}"),
    ];

    /// <summary>Predicates Spark answers without an error over <see cref="Rows"/>.</summary>
    private static readonly string[] Predicates =
    [
        "try_variant_get(v, '$.a', 'int') > 5",
        "try_variant_get(v, '$.a', 'int') <= 1",
        "try_cast(v:a AS int) = 7",
        "v:n::double > 2.0D",
        "v:n::int = 1",
        "variant_get(v, '$.n', 'decimal(4,1)') < 0",
        "v:n::string = '1.5'",
        "try_variant_get(v, '$.a[1]', 'int') = 5",
        "try_variant_get(v, '$.b.a', 'bigint') = 3",
        "v:b:a::int = 3",
        "try_variant_get(v, '$.a', 'boolean') = true",
        "try_variant_get(v, '$.a', 'int') IS NULL",
        "variant_get(v, '$.a') IS NULL",
        "variant_get(v, '$.a') IS NOT NULL AND try_variant_get(v, '$.a', 'int') IS NULL",
        "try_variant_get(v, '$', 'int') = 7",
        "try_variant_get(v, '$[0]', 'int') = 1",
        "NOT (try_variant_get(v, '$.a', 'int') > 5)",
    ];

    // The driver fills `{path}` in with Python's str.format, so a literal brace is doubled.
    private static string Literal(string json) =>
        "parse_json('" + json.Replace("'", "\\'").Replace("{", "{{").Replace("}", "}}") + "')";

    private static string InsertSql() =>
        "INSERT INTO delta.`{path}` VALUES "
        + string.Join(", ", Rows.Select(r => $"({r.Id}, {(r.Json is null ? "CAST(NULL AS VARIANT)" : Literal(r.Json))})"));

    // As Spark's parse_json reads the JSON: a number with a fraction is a decimal (VariantJsonReader makes it a
    // double).
    private static VariantValue? Parse(string? json)
    {
        if (json is null)
            return null;
        var (metadata, value) = VariantJsonReader.Parse(json);
        return Decimals(new VariantReader(metadata, value).ToVariantValue());

        static VariantValue Decimals(VariantValue v) =>
            v.IsObject ? VariantValue.FromObject(v.AsObject().ToDictionary(f => f.Key, f => Decimals(f.Value)))
            : v.IsArray ? VariantValue.FromArray(v.AsArray().Select(Decimals).ToList())
            : v.PrimitiveType == VariantPrimitiveType.Double ? VariantValue.FromDecimal4((decimal)v.AsDouble())
            : v;
    }

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("v", VariantType.Default, true))
        .Build();

    private static RecordBatch Batch(IEnumerable<(long Id, string? Json)> rows)
    {
        var ids = new Int64Array.Builder();
        var vb = new VariantArray.Builder();
        int n = 0;
        foreach (var (id, json) in rows)
        {
            ids.Append(id);
            if (Parse(json) is { } v) vb.Append(v); else vb.AppendNull();
            n++;
        }
        return new RecordBatch(Schema, [ids.Build(), vb.Build(allocator: null)], n);
    }

    private async Task WriteShreddedAsync()
    {
        var options = DeltaTableOptions.Default with
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                VariantShredSchemas = new Dictionary<string, ShredSchema>
                {
                    ["v"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        ["a"] = ShredSchema.Primitive(ShredType.Int8),
                        ["b"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                        {
                            ["a"] = ShredSchema.Primitive(ShredType.Int8),
                        }),
                    }),
                },
            },
        };
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema,
            configuration: new Dictionary<string, string> { ["delta.enableVariantShredding"] = "true" }, options: options);
        await table.WriteAsync([Batch(Rows)]);
    }

    private static List<long> Ids(JsonElement rows) =>
        rows.EnumerateArray().Select(r => r.GetProperty("id").GetInt64()).OrderBy(x => x).ToList();

    /// <summary>Each predicate's ids as Spark selects them, in one driver call.</summary>
    private List<List<long>> SparkIds() =>
        Spark.Invoke("sql", new
            {
                path = _tempDir,
                sql = Predicates.Select(p => $"SELECT id FROM delta.`{{path}}` WHERE {p}").ToArray(),
            })
            .GetProperty("statements").EnumerateArray().Select(s => Ids(s.GetProperty("rows"))).ToList();

    /// <summary>Each predicate's ids as EW's evaluator selects them from the rows EW reads.</summary>
    private async Task<List<List<long>>> EwIdsAsync()
    {
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());
        var answers = Predicates.Select(_ => new List<long>()).ToList();
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await foreach (var batch in table.ReadAsync())
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            for (int p = 0; p < Predicates.Length; p++)
            {
                var mask = evaluator.EvaluatePredicate(SparkSqlParser.ParsePredicate(Predicates[p]), batch);
                for (int i = 0; i < batch.Length; i++)
                {
                    if (mask.GetValue(i) == true)
                        answers[p].Add(ids.GetValue(i)!.Value);
                }
            }
        }
        return answers.Select(a => a.OrderBy(x => x).ToList()).ToList();
    }

    private async Task AssertSameRowsAsSparkAsync()
    {
        var spark = SparkIds();
        var ew = await EwIdsAsync();
        var failures = new List<string>();
        for (int p = 0; p < Predicates.Length; p++)
        {
            if (!spark[p].SequenceEqual(ew[p]))
                failures.Add($"{Predicates[p]}: Spark [{string.Join(",", spark[p])}], EW [{string.Join(",", ew[p])}]");
        }
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    [SkippableFact]
    public async Task OnASparkWrittenTable_EwSelectsTheRowsSparkSelects()
    {
        RequireGaVariant();
        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "CREATE TABLE delta.`{path}` (id BIGINT, v VARIANT) USING delta", InsertSql() },
        });

        await AssertSameRowsAsSparkAsync();
    }

    [SkippableFact]
    public async Task OnAnEwShreddedTable_EwSelectsTheRowsSparkSelects()
    {
        RequireGaVariant();
        await WriteShreddedAsync();

        await AssertSameRowsAsSparkAsync();
    }

    [SkippableFact]
    public async Task AVariantGetThatFailsInSpark_FailsInEw()
    {
        RequireGaVariant();
        await WriteShreddedAsync();

        // Spark pushes the extraction into the scan, where the same error comes back as FAILED_READ_FILE with the
        // cause past what the driver reports. Evaluated after the scan, the error is the cast's own.
        const string sql = "variant_get(v, '$.a', 'int') > 0";
        var raw = Spark.InvokeRaw("sql", new
        {
            path = _tempDir,
            sql = new[]
            {
                "SET spark.sql.variant.pushVariantIntoScan = false",
                $"SELECT id FROM delta.`{{path}}` WHERE {sql}",
            },
        });
        Assert.False(raw.GetProperty("ok").GetBoolean());
        Assert.Contains("INVALID_VARIANT_CAST", raw.GetProperty("error").GetString());

        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var ex = await Assert.ThrowsAsync<SparkEvaluationException>(async () =>
        {
            await foreach (var batch in table.ReadAsync())
                evaluator.EvaluatePredicate(SparkSqlParser.ParsePredicate(sql), batch);
        });
        Assert.Equal("INVALID_VARIANT_CAST", ex.ErrorClass);
    }

    [SkippableFact]
    public async Task AnEwDelete_LeavesTheRowsSparksDeleteLeaves()
    {
        RequireGaVariant();
        const string predicate = "try_variant_get(v, '$.a', 'int') > 5 OR v:n::double < 0";
        var create = new[] { "CREATE TABLE delta.`{path}` (id BIGINT, v VARIANT) USING delta", InsertSql() };

        string theirs = Path.Combine(_tempDir, "theirs");
        string ours = Path.Combine(_tempDir, "ours");
        Spark.Invoke("sql", new { path = theirs, sql = create.Append($"DELETE FROM delta.`{{path}}` WHERE {predicate}").ToArray() });
        Spark.Invoke("sql", new { path = ours, sql = create });

        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(ours)))
            await table.DeleteAsync(SparkSqlParser.ParsePredicate(predicate));

        var sparks = Ids(Spark.Invoke("sql", new { path = theirs, sql = System.Array.Empty<string>() }).GetProperty("rows"));
        var ewsAsSparkReadsIt = Ids(Spark.Invoke("sql", new { path = ours, sql = System.Array.Empty<string>() }).GetProperty("rows"));
        Assert.Equal(sparks, ewsAsSparkReadsIt);
        Assert.Equal(Rows.Length - 2, sparks.Count); // ids 2 and 3
    }

    [SkippableFact]
    public async Task ASparkVariantGetConstraint_IsEnforcedByEw()
    {
        RequireGaVariant();
        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[]
            {
                "CREATE TABLE delta.`{path}` (id BIGINT, v VARIANT) USING delta",
                $"INSERT INTO delta.`{{path}}` VALUES (1, {Literal("{\"a\":2}")})",
                "ALTER TABLE delta.`{path}` ADD CONSTRAINT pos CHECK (variant_get(v, '$.a', 'int') > 0)",
            },
        });

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await table.WriteAsync([Batch([(2, "{\"a\":\"5\"}")])]);

        foreach (var violating in new[] { "{\"b\":1}", "{\"a\":null}", null, "{\"a\":-1}" })
        {
            var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Batch([(3, violating)])]));
            Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
        }

        var cast = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([Batch([(3, "{\"a\":\"x\"}")])]));
        Assert.Contains("INVALID_VARIANT_CAST", cast.Message);

        Assert.Equal([1, 2], Ids(Spark.Invoke("sql", new { path = _tempDir, sql = System.Array.Empty<string>() }).GetProperty("rows")));
    }

    [SkippableFact]
    public async Task AnEwVariantGetConstraint_IsEnforcedBySpark()
    {
        RequireGaVariant();
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema))
        {
            await table.WriteAsync([Batch([(1, "{\"a\":2}")])]);
            await table.AddConstraintAsync("pos", "try_variant_get(v, '$.a', 'int') > 0");
        }

        foreach (var violating in new[] { "{\"b\":1}", "{\"a\":\"x\"}", "{\"a\":-1}" })
        {
            var raw = Spark.InvokeRaw("sql", new { path = _tempDir, sql = new[] { $"INSERT INTO delta.`{{path}}` VALUES (3, {Literal(violating)})" } });
            Assert.False(raw.GetProperty("ok").GetBoolean(), $"Spark accepted {violating}");
            Assert.Contains("DELTA_VIOLATE_CONSTRAINT", raw.GetProperty("error").GetString());
        }

        var accepted = Spark.Invoke("sql", new { path = _tempDir, sql = new[] { $"INSERT INTO delta.`{{path}}` VALUES (2, {Literal("{\"a\":\"5\"}")})" } });
        Assert.Equal([1, 2], Ids(accepted.GetProperty("rows")));
    }
}
