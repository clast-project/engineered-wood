// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Pruning on variant paths (#677) against Spark's answers. No engine prunes with variant bounds (measured:
/// delta-spark 4.3.1 and DuckDB 2.0's delta_scan read every file), so Spark is the oracle for the ANSWER only:
/// every row Spark selects must come from a file EW kept. A scan's filter prunes files and does not filter rows,
/// so the rows EW returns are exactly the kept files' rows, and "kept" is checked as "Spark's ids are a subset".
/// </summary>
[Collection("Interop")]
public class VariantPruningInteropTests : IDisposable
{
    private readonly string _tempDir;

    public VariantPruningInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_prune_xval_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static void RequireSparkVariantStats()
    {
        Spark.Require();
        string version = Spark.Version ?? "";
        int i = version.IndexOf('/');
        var parts = i < 0 ? [] : version.Substring(i + 1).Trim().Split('.');
        bool atLeast43 = parts.Length >= 2 && int.TryParse(parts[0], out int major) && int.TryParse(parts[1], out int minor)
            && (major > 4 || (major == 4 && minor >= 3));
        Skip.IfNot(atLeast43, $"variant data-skipping stats need delta-spark 4.3+; resolved {version}");
    }

    // One file per batch. `a` is bounded [1, 3], [10, 12] and not at all (mixed types); `s` and `c.d` are
    // bounded in each. A scalar variant and a NULL variant answer NULL for every path, and `b` lands in the
    // residual. Rows 13 and 14 lack `a`, `s` and `c.d`: an object missing a shredded field, which Spark refused
    // in EW's encoding until the field groups were written required (#679). (Spark bounds nothing in the third
    // file: its shredding inference gives up on a file whose top level is mixed.)
    private static readonly (long Id, string? Json)[][] Files =
    [
        [(1, "{\"a\":1,\"s\":\"p\",\"c\":{\"d\":1.5}}"), (2, "{\"a\":3,\"s\":\"q\",\"c\":{\"d\":2.5}}")],
        [(10, "{\"a\":10,\"s\":\"x\",\"c\":{\"d\":10.5}}"), (11, "{\"a\":12,\"s\":\"y\",\"c\":{\"d\":11.5}}"),
            (12, "{\"a\":11,\"s\":\"z\",\"c\":{\"d\":12.5},\"b\":1}"), (13, "{\"b\":1}"), (14, "{\"c\":{\"e\":1}}")],
        [(20, "{\"a\":5,\"s\":\"m\",\"c\":{\"d\":5.5}}"), (21, "{\"a\":\"str\",\"s\":\"n\",\"c\":{\"d\":6.5}}"), (22, "7"), (23, null)],
    ];

    /// <summary>Predicates Spark answers without an error on these rows, and whether EW must skip a file for each
    /// (one whose bounds rule it out, on both tables). The others are there for the answer alone.</summary>
    private static readonly (string Sql, bool Prunes)[] Predicates =
    [
        ("try_variant_get(v, '$.a', 'int') > 5", true),
        ("try_variant_get(v, '$.a', 'bigint') < 4", true),
        ("try_cast(v:a AS int) = 7", true),
        ("try_variant_get(v, '$.a', 'int') IN (5, 20)", true),
        ("NOT (try_variant_get(v, '$.a', 'int') <= 5)", false),
        ("try_variant_get(v, '$.a', 'int') != 3", false),
        ("v:s::string > 'w'", true),
        ("v:s::string = 'q' OR v:s::string = 'm'", true),
        ("v:s::string < 'n'", true),
        ("try_variant_get(v, '$.a', 'tinyint') > 11", true),
        ("try_variant_get(v, '$.c.d', 'double') < 2.0D", false), // a decimal bound does not answer double
        ("v:c.d::decimal(10,1) > 10.6", true),
        ("v:c.d::decimal(10,2) <= 1.5", true),
        ("try_variant_get(v, '$.missing', 'int') > 0", false),
        ("try_variant_get(v, '$.a', 'int') IS NULL", false),
        ("try_variant_get(v, '$.a', 'int') > 5 AND id < 100", true),
    ];

    // As Spark's parse_json reads the JSON: a number with a fraction is a decimal (VariantJsonReader makes it a
    // double), so both tables bound `c.d` as a decimal.
    private static VariantValue? Parse(string? json)
    {
        if (json is null)
            return null;
        var (metadata, value) = VariantJsonReader.Parse(json);
        return Decimals(new VariantReader(metadata, value).ToVariantValue());

        static VariantValue Decimals(VariantValue v) =>
            v.IsObject ? VariantValue.FromObject(v.AsObject().ToDictionary(f => f.Key, f => Decimals(f.Value)))
            : v.PrimitiveType == VariantPrimitiveType.Double ? VariantValue.FromDecimal4((decimal)v.AsDouble())
            : v;
    }

    private static async Task<List<long>> EwIdsAsync(DeltaTable table, string sql)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = SparkSqlParser.ParsePredicate(sql) }))
        {
            var column = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        return ids;
    }

    /// <summary>Each predicate's ids as Spark selects them, in one driver call.</summary>
    private List<HashSet<long>> SparkIds()
    {
        var result = Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = Predicates.Select(p => $"SELECT id FROM delta.`{{path}}` WHERE {p.Sql}").ToArray(),
        });
        return result.GetProperty("statements").EnumerateArray()
            .Select(s => s.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("id").GetInt64()).ToHashSet())
            .ToList();
    }

    private async Task AssertSparksRowsAreKeptAsync((long Id, string? Json)[][] files)
    {
        var spark = SparkIds();
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        int total = files.Sum(f => f.Length);
        var failures = new List<string>();
        for (int i = 0; i < Predicates.Length; i++)
        {
            var (sql, prunes) = Predicates[i];
            var ew = (await EwIdsAsync(table, sql)).ToHashSet();
            var lost = spark[i].Where(id => !ew.Contains(id)).OrderBy(id => id).ToList();
            if (lost.Count > 0)
                failures.Add($"{sql}: Spark selects {string.Join(",", lost)} from files EW skipped");
            if (prunes && ew.Count == total)
                failures.Add($"{sql}: EW skipped no file (Spark selects {string.Join(",", spark[i].OrderBy(x => x))})");
        }
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    /// <summary>EW's own bounds: an EW-written shredded table, which Spark reads.</summary>
    [SkippableFact]
    public async Task EwsBounds_KeepEveryRowSparkSelects()
    {
        RequireSparkVariantStats();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();
        var options = DeltaTableOptions.Default with
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                VariantShredSchemas = new Dictionary<string, ShredSchema>
                {
                    ["v"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        ["a"] = ShredSchema.Primitive(ShredType.Int8),
                        ["s"] = ShredSchema.Primitive(ShredType.String),
                        ["c"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                        {
                            ["d"] = ShredSchema.FromArrowType(new Decimal32Type(9, 1)),
                        }),
                    }),
                },
            },
        };
        await using (var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema,
            configuration: new Dictionary<string, string> { ["delta.enableVariantShredding"] = "true" }, options: options))
        {
            foreach (var file in Files)
            {
                var ids = new Int64Array.Builder();
                var vb = new VariantArray.Builder();
                foreach (var (id, json) in file)
                {
                    ids.Append(id);
                    if (Parse(json) is { } v)
                        vb.Append(v);
                    else
                        vb.AppendNull();
                }
                await table.WriteAsync([new RecordBatch(schema, [ids.Build(), vb.Build(allocator: null)], file.Length)]);
            }
        }

        await AssertSparksRowsAreKeptAsync(Files);
    }

    /// <summary>Spark's own bounds: a table delta-spark 4.3 wrote, shredded by its inference, with real bounds.</summary>
    [SkippableFact]
    public async Task SparksBounds_KeepEveryRowSparkSelects()
    {
        RequireSparkVariantStats();

        Spark.Invoke("write_shredded_variant", new
        {
            path = _tempDir,
            batches = Files.Select(f => f.Select(r => new { id = r.Id, json = r.Json }).ToArray()).ToArray(),
        });

        // The premise: Spark bounded the paths these cases prune on, in the files they prune.
        using (var stats = JsonDocument.Parse((await new DeltaLake.Log.TransactionLog(new LocalTableFileSystem(_tempDir))
                   .ReadCommitAsync(2)).OfType<DeltaLake.Actions.AddFile>().Single().Stats!))
        {
            Assert.True(VariantStatsEncoding.TryDecode(
                stats.RootElement.GetProperty("minValues").GetProperty("v").GetString()!, out var metadata, out var value));
            string bound = VariantJsonWriter.ToJson(metadata, value, indented: false);
            Assert.Contains("a", bound, StringComparison.Ordinal);
        }

        await AssertSparksRowsAreKeptAsync(Files);
    }
}
