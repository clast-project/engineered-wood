// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table.Stats;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Variant min/max statistics (#675) against the two engines that compute them independently.
/// </summary>
/// <remarks>
/// <para>delta-spark 4.3 is the oracle for the Delta form: it derives the bounds from a shredded file's footer
/// (under the confs the driver's <c>_variant_stats_confs</c> sets: none of them is the default) and decodes
/// them again when it writes a checkpoint. DuckDB is the oracle for the values: it computes min and max of
/// each path from the rows, not the footer. No engine skips files with these bounds yet (measured: Spark 4.3.1
/// and DuckDB's delta extension both read every file), so pruning has no oracle here.</para>
/// </remarks>
[Collection("Interop")]
public class VariantStatisticsInteropTests : IDisposable
{
    private readonly string _tempDir;

    public VariantStatisticsInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_stats_xval_{Guid.NewGuid():N}");
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
        Skip.IfNot(DeltaSparkAtLeast(4, 3), $"variant data-skipping stats need delta-spark 4.3+; resolved {version}");
    }

    private static bool DeltaSparkAtLeast(int major, int minor)
    {
        // Spark.Version is "<pyspark>/<delta-spark>", e.g. "4.1.2/4.3.1".
        string version = Spark.Version ?? "";
        int i = version.IndexOf('/');
        if (i < 0) return false;
        var parts = version.Substring(i + 1).Trim().Split('.');
        return parts.Length >= 2
            && int.TryParse(parts[0], out int ma) && int.TryParse(parts[1], out int mi)
            && (ma > major || (ma == major && mi >= minor));
    }

    /// <summary>A binary Variant object as path → (primitive type, JSON), so a bound compares by type too.</summary>
    private static Dictionary<string, (string Type, string Json)> Paths(byte[] metadata, byte[] value)
    {
        var storageType = (StructType)VariantType.Default.StorageType;
        var children = storageType.Fields
            .Select(f => (IArrowArray)new BinaryArray.Builder().Append(f.Name == "metadata" ? metadata : value).Build())
            .ToArray();
        var variant = (VariantArray)VariantType.Default.CreateArray(new StructArray(storageType, 1, children, ArrowBuffer.Empty, 0));
        return variant.GetLogicalVariantValue(0).AsObject().ToDictionary(
            kv => kv.Key, kv => (kv.Value.PrimitiveType.ToString(), VariantJsonWriter.ToJson(kv.Value, indented: false)));
    }

    private static Dictionary<string, (string Type, string Json)> Paths(string z85)
    {
        Assert.True(VariantStatsEncoding.TryDecode(z85, out byte[] metadata, out byte[] value), $"not a z85 variant: {z85}");
        return Paths(metadata, value);
    }

    private static async Task<Parquet.Metadata.FileMetaData> FooterAsync(string file)
    {
        await using var raf = new LocalRandomAccessFile(file);
        await using var reader = new ParquetFileReader(raf, ownsFile: false);
        return await reader.ReadMetadataAsync();
    }

    private static string Format(Dictionary<string, (string Type, string Json)> paths) =>
        string.Join(", ", paths.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => $"{p.Key}={p.Value.Type}:{p.Value.Json}"));

    // ── Spark ────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// For every file Spark wrote, the bounds EW derives from that file's footer are Spark's bounds, path for
    /// path and type for type: objects with a uniform, a nested, a mixed and an array path in one file, and every
    /// typed scalar Spark shreds in another.
    /// </summary>
    [SkippableFact]
    public async Task SparksBounds_AreWhatEwDerivesFromTheSameFooter()
    {
        RequireSparkVariantStats();

        const string scalars =
            "to_variant_object(named_struct('t', CAST({0} AS TINYINT), 's', CAST({0} AS SMALLINT), 'i', CAST({0} AS INT), "
            + "'l', CAST({0} AS BIGINT), 'f', CAST({0}.5 AS FLOAT), 'x', CAST({0}.25 AS DOUBLE), "
            + "'dec', CAST({0}.10 AS DECIMAL(9,2)), 'big', CAST('1234567890123456789{0}.5' AS DECIMAL(30,1)), "
            + "'dt', DATE_ADD(DATE'2026-11-04', {0}), 'ts', TIMESTAMP'2026-11-04 12:00:00' + MAKE_INTERVAL(0,0,0,0,{0}), "
            + "'ntz', TIMESTAMP_NTZ'2026-11-04 12:00:00' + MAKE_INTERVAL(0,0,0,0,{0}), "
            + "'str', 'a string that is longer than thirty-two characters, number {0}'))";
        Spark.Invoke("write_shredded_variant", new
        {
            path = _tempDir,
            batches = new object[]
            {
                new object[]
                {
                    new { id = 1L, json = "{\"a\":1,\"b\":\"x\",\"c\":{\"d\":1.5},\"m\":1,\"arr\":[1,2]}" },
                    new { id = 2L, json = "{\"a\":3,\"b\":\"y\",\"c\":{\"d\":2.5},\"m\":\"s\",\"arr\":[3]}" },
                    new { id = 3L, json = (string?)null },
                },
                new object[]
                {
                    new { id = 4L, sql = string.Format(CultureInfo.InvariantCulture, scalars, 1) },
                    new { id = 5L, sql = string.Format(CultureInfo.InvariantCulture, scalars, 7) },
                },
            },
        });

        var fs = new LocalTableFileSystem(_tempDir);
        var adds = new List<AddFile>();
        var log = new TransactionLog(fs);
        for (long version = 1; version <= 2; version++)
            adds.AddRange((await log.ReadCommitAsync(version)).OfType<AddFile>());
        Assert.Equal(2, adds.Count);

        var sawPaths = new HashSet<string>(StringComparer.Ordinal);
        bool sawFloating = false;
        foreach (var add in adds)
        {
            using var stats = JsonDocument.Parse(add.Stats!);
            var sparkMin = Paths(stats.RootElement.GetProperty("minValues").GetProperty("v").GetString()!);
            var sparkMax = Paths(stats.RootElement.GetProperty("maxValues").GetProperty("v").GetString()!);
            Assert.NotEmpty(sparkMin);

            var footer = await FooterAsync(Path.Combine(_tempDir, add.Path));
            var ew = VariantFooterStats.Compute(footer, ["v"], stringPrefixLength: 32);
            Assert.True(ew.ContainsKey("v"), $"EW derived no bounds for {add.Path}; Spark: {Format(sparkMin)}");

            // The one deliberate difference. parquet-mr 1.16 records no nan_count, and EW bounds a float path only
            // when the footer says it holds no NaN (#676 review). Spark bounds it anyway, which is safe for its own
            // files because parquet-mr puts a NaN INTO the min/max (measured: [1.5, NaN, 0.5] gives (0.5, NaN), and
            // Spark then leaves the path out), but would not be for a writer that skips NaNs without counting them.
            bool nanCountsUnknown = footer.RowGroups.SelectMany(g => g.Columns).All(c =>
                c.MetaData is not { Type: Parquet.PhysicalType.Float or Parquet.PhysicalType.Double } m
                || m.Statistics?.NanCount is null);
            Assert.True(nanCountsUnknown, "parquet-mr now records nan_count: compare the float paths with Spark again");
            static bool Floating((string Type, string Json) v) => v.Type is "Float" or "Double";
            var expectedMin = sparkMin.Where(p => !Floating(p.Value)).ToDictionary(p => p.Key, p => p.Value);
            var expectedMax = sparkMax.Where(p => !Floating(p.Value)).ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal(Format(expectedMin), Format(Paths(ew["v"].Min)));
            Assert.Equal(Format(expectedMax), Format(Paths(ew["v"].Max)));
            sawPaths.UnionWith(sparkMin.Keys);
            sawFloating |= sparkMin.Values.Any(Floating);
        }

        // The cases are the ones claimed: a nested path and a cut string bounded, a mixed and an array path not.
        Assert.Contains("$['c']['d']", sawPaths);
        Assert.Contains("$['str']", sawPaths);
        Assert.Contains("$['big']", sawPaths);
        Assert.True(sawFloating, "Spark bounded no float path, so the difference above was not exercised");
        Assert.DoesNotContain("$['m']", sawPaths);
        Assert.DoesNotContain(sawPaths, p => p.StartsWith("$['arr']", StringComparison.Ordinal));
    }

    /// <summary>
    /// EW's bounds decode in Spark: Spark's own checkpoint of an EW-written shredded table carries, for EW's file,
    /// the values EW encoded. delta-spark reads them from EW's z85 strings, so a framing error would surface here.
    /// </summary>
    [SkippableFact]
    public async Task EwsBounds_DecodeInSparksCheckpoint()
    {
        RequireSparkVariantStats();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();
        var options = DeltaTableOptions.Default with
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default },
        };
        var ids = new Int64Array.Builder();
        var vb = new VariantArray.Builder();
        for (int i = 0; i < 3; i++)
        {
            ids.Append(i);
            vb.Append(VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                ["a"] = VariantValue.FromInt64(10 * i - 5),
                ["b"] = VariantValue.FromString("s" + i),
                ["c"] = VariantValue.FromObject(new Dictionary<string, VariantValue> { ["d"] = VariantValue.FromDouble(i + 0.5) }),
            }));
        }
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, schema,
            configuration: new Dictionary<string, string> { ["delta.enableVariantShredding"] = "true" }, options: options))
        {
            await table.WriteAsync([new RecordBatch(schema, [ids.Build(), vb.Build(allocator: null)], 3)]);
        }
        var ewAdd = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single();
        using var ewStats = JsonDocument.Parse(ewAdd.Stats!);
        var ewMin = Paths(ewStats.RootElement.GetProperty("minValues").GetProperty("v").GetString()!);
        var ewMax = Paths(ewStats.RootElement.GetProperty("maxValues").GetProperty("v").GetString()!);
        Assert.Equal(["$['a']", "$['b']", "$['c']['d']"], ewMin.Keys.OrderBy(k => k, StringComparer.Ordinal));

        var result = Spark.Invoke("checkpoint_variant_bounds", new { path = _tempDir, col = "v" });
        var sparkFile = result.GetProperty("files").EnumerateArray()
            .Single(f => f.GetProperty("path").GetString() == ewAdd.Path);

        AssertSameJson(ewMin, sparkFile.GetProperty("min").GetString());
        AssertSameJson(ewMax, sparkFile.GetProperty("max").GetString());
    }

    private static void AssertSameJson(Dictionary<string, (string Type, string Json)> ew, string? sparkJson)
    {
        Assert.NotNull(sparkJson);
        using var spark = JsonDocument.Parse(sparkJson!);
        var sparkPaths = spark.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText());
        Assert.Equal(ew.Keys.OrderBy(k => k, StringComparer.Ordinal), sparkPaths.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var (path, (_, json)) in ew)
            Assert.True(SameJsonValue(json, sparkPaths[path]), $"{path}: EW {json}, Spark {sparkPaths[path]}");
    }

    private static bool SameJsonValue(string a, string b)
    {
        using var x = JsonDocument.Parse(a);
        using var y = JsonDocument.Parse(b);
        return x.RootElement.ValueKind == JsonValueKind.Number && y.RootElement.ValueKind == JsonValueKind.Number
            ? x.RootElement.GetDecimal() == y.RootElement.GetDecimal()
            : x.RootElement.GetRawText() == y.RootElement.GetRawText();
    }

    /// <summary>
    /// Spark's bounds survive EW: an EW append and an EW checkpoint keep them, in the JSON stats and in
    /// stats_parsed, and Spark still reads the table through that checkpoint.
    /// </summary>
    [SkippableFact]
    public async Task SparksBounds_SurviveAnEwAppendAndCheckpoint()
    {
        RequireSparkVariantStats();

        Spark.Invoke("write_shredded_variant", new
        {
            path = _tempDir,
            rows = new object[]
            {
                new { id = 1L, json = "{\"a\":1,\"b\":\"x\"}" },
                new { id = 2L, json = "{\"a\":2,\"b\":\"y\"}" },
            },
        });
        var fs = new LocalTableFileSystem(_tempDir);
        var sparkAdd = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single();
        using var sparkStats = JsonDocument.Parse(sparkAdd.Stats!);
        string sparkMin = sparkStats.RootElement.GetProperty("minValues").GetProperty("v").GetString()!;
        Assert.Contains("$['a']", Paths(sparkMin).Keys);

        await using (var table = await DeltaTable.OpenAsync(fs))
        {
            var schema = table.CurrentSnapshot.Schema;
            var arrow = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, true))
                .Field(new Field("v", VariantType.Default, true))
                .Build();
            var vb = new VariantArray.Builder();
            vb.Append(VariantValue.FromObject(new Dictionary<string, VariantValue> { ["a"] = VariantValue.FromInt64(3) }));
            await table.WriteAsync([new RecordBatch(arrow, [new Int64Array.Builder().Append(3).Build(), vb.Build(allocator: null)], 1)]);
            await table.CheckpointAsync();
        }

        var reader = new CheckpointReader(fs);
        var fromCheckpoint = (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!))
            .OfType<AddFile>().Single(a => a.Path == sparkAdd.Path);
        Assert.Equal(sparkAdd.Stats, fromCheckpoint.Stats);
        using var rebuilt = JsonDocument.Parse(fromCheckpoint.TypedStats!.View.BuildStatsJson(fromCheckpoint.TypedStats.Row)!);
        Assert.Equal(sparkMin, rebuilt.RootElement.GetProperty("minValues").GetProperty("v").GetString());

        var read = Spark.Invoke("read_variant", new { path = _tempDir, col = "v", id_col = "id" });
        Assert.Equal(["{\"a\":1,\"b\":\"x\"}", "{\"a\":2,\"b\":\"y\"}", "{\"a\":3}"],
            read.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("vjson").GetString()).ToList());
    }

    // ── DuckDB ───────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The bounds EW derives from a DuckDB-written footer (several row groups, a 16-byte decimal, a nested path)
    /// are DuckDB's own min and max of each path, computed from the rows.
    /// </summary>
    [SkippableFact]
    public async Task DuckDbWrittenFooter_GivesDuckDbsMinAndMax()
    {
        DuckDb.Require();
        Skip.IfNot(DuckDb.WritesShreddedVariantOracle,
            $"a DuckDB-written shredded file is the oracle from 2.0; resolved {DuckDb.Version}");

        string path = Path.Combine(_tempDir, "duck.parquet");
        const string query =
            "SELECT i AS id, {'i': i - 2500, 's': 's' || i, 'x': i * 0.5, 'dec': ((i - 1000) / 7.0)::DECIMAL(38,10), "
            + "'dt': DATE '2026-11-04' + (i % 400)::INTEGER, 'ts': TIMESTAMP '2026-11-04 12:00:00' + to_seconds(i), "
            + "'n': {'k': (i % 97)::INTEGER}}::VARIANT AS v FROM range(5000) t(i) ORDER BY id";
        var written = DuckDb.Invoke("write_parquet_variant",
            new { path, query, order_by = "id", copy_options = "ROW_GROUP_SIZE 2048" });
        Assert.Contains("v, typed_value, n, typed_value, k, typed_value",
            written.GetProperty("leaves").EnumerateArray().Select(l => l.GetString()));

        var footer = await FooterAsync(path);
        Assert.True(footer.RowGroups.Count > 1, $"expected several row groups, got {footer.RowGroups.Count}");
        var ew = VariantFooterStats.Compute(footer, ["v"], stringPrefixLength: 32);
        var min = Paths(ew["v"].Min);
        var max = Paths(ew["v"].Max);

        var exprs = new (string Key, string Sql)[]
        {
            ("$['i']", "v.i::BIGINT"), ("$['s']", "v.s::VARCHAR"), ("$['x']", "v.x::DOUBLE"),
            ("$['dec']", "v.dec::DECIMAL(38,10)"), ("$['dt']", "v.dt::DATE"), ("$['ts']", "v.ts::TIMESTAMP"),
            ("$['n']['k']", "v.n.k::INTEGER"),
        };
        var duck = DuckDb.Invoke("variant_path_bounds", new { path, exprs = exprs.Select(e => new { key = e.Key, sql = e.Sql }) })
            .GetProperty("bounds");

        Assert.Equal(exprs.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal), min.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var (key, _) in exprs)
        {
            var d = duck.GetProperty(key);
            AssertSameValue(key, min[key], d.GetProperty("min").GetString()!);
            AssertSameValue(key, max[key], d.GetProperty("max").GetString()!);
        }
        Assert.Equal("Decimal16", min["$['dec']"].Type);
    }

    /// <summary>EW's bound (as Arrow's JSON prints it) against DuckDB's value as text, compared as values.</summary>
    private static void AssertSameValue(string key, (string Type, string Json) ew, string duck)
    {
        using var doc = JsonDocument.Parse(ew.Json);
        var e = doc.RootElement;
        bool same = ew.Type switch
        {
            "String" => e.GetString() == duck,
            "Date" => e.GetString() == duck,
            "TimestampNtz" or "Timestamp" =>
                DateTime.Parse(e.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
                == DateTime.Parse(duck, CultureInfo.InvariantCulture),
            "Double" or "Float" => e.GetDouble() == double.Parse(duck, CultureInfo.InvariantCulture),
            _ => e.GetDecimal() == decimal.Parse(duck, CultureInfo.InvariantCulture),
        };
        Assert.True(same, $"{key}: EW {ew.Type} {ew.Json}, DuckDB {duck}");
    }
}
