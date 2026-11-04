// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// External validation of Delta VARIANT support against delta-rs (tier 1) and PySpark (tier 3) — the
/// only thing that proves EngineeredWood's variant dialect is the Delta spec's and not merely
/// self-consistent. Round-tripping through EW's own reader hid, until this suite existed, that:
/// EW's reader keyed off the parquet annotation rather than the Delta schema (so an unannotated
/// Spark-4.0 table read as a bare struct), and Spark 4.0.x's parquet reader NPEs on the annotation EW
/// emits by default (the reason for <see cref="DeltaTableOptions.EmitVariantLogicalType"/>).
///
/// <para>Validated against BOTH Spark lines: the unannotated path on pyspark 4.0.1 / delta-spark 4.0.0
/// (the pinned tier-3 base) and the GA annotated path — plus the nested-variant cases below — on an
/// isolated 4.1 venv pointed at via <c>EW_SPARK_PYTHON</c>. That venv is <b>pyspark 4.1.2 /
/// delta-spark 4.3.1</b> on Python 3.13 and <b>pyspark 4.1.1 / delta-spark 4.1.0</b> on 3.11; the
/// pairing is not free to choose, and this comment previously named 4.1.3 / 4.1.0, which fails 10 of
/// the 109 cases with <c>NoSuchMethodError</c> on every checkpointed table. See
/// <c>doc/running-tests.md</c> for why each line needs the version it does. The GA and nested cases
/// SKIP on 4.0.x via <see cref="SparkHasGaVariant"/>, so the suite stays green on whichever Spark is
/// on hand — and a <c>Skipped: 0</c> is what proves they actually ran.</para>
/// </summary>
[Collection("Interop")]
public class VariantInteropTests : IDisposable
{
    private readonly string _tempDir;

    public VariantInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_xval_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static readonly byte[] EmptyMetadata = [0x01, 0x00, 0x00];
    private static readonly byte[] True = [0x04];             // (1 << 2) | 0
    private static readonly byte[] Int8_42 = [0x0C, 0x2A];    // (3 << 2) | 0, then 42

    // Convert.ToHexString is net5+; the test project also targets net472. Match the driver's lowercase hex.
    private static string Hex(byte[] bytes) =>
        string.Concat(bytes.Select(b => b.ToString("x2")));

    private static Apache.Arrow.Schema VariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    private async Task WriteVariantTable(bool emitAnnotation)
    {
        var options = DeltaTableOptions.Default with { EmitVariantLogicalType = emitAnnotation };
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), options: options);

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var vb = new VariantArray.Builder();
        vb.Append(EmptyMetadata, True);
        vb.Append(EmptyMetadata, Int8_42);
        vb.AppendNull();
        await table.WriteAsync([new RecordBatch(VariantSchema(), [ids, vb.Build(allocator: null)], 3)]);
    }

    private static readonly StructType NestedInner = new(
    [
        new Field("v", VariantType.Default, true),
        new Field("tag", StringType.Default, true),
    ]);

    private static Apache.Arrow.Schema NestedVariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", NestedInner, true))
            .Build();

    /// <summary>Writes a table whose variant is NESTED inside a struct (<c>s: struct&lt;v: variant, tag&gt;</c>).
    /// EW annotates the nested variant group regardless of <see cref="DeltaTableOptions.EmitVariantLogicalType"/>
    /// — <c>StripAnnotation</c> is top-level only — so the reader must be GA variant (Spark >= 4.1).</summary>
    private async Task WriteNestedVariantTable()
    {
        var schema = NestedVariantSchema();
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var vb = new VariantArray.Builder();
        vb.Append(EmptyMetadata, True);
        vb.Append(EmptyMetadata, Int8_42);
        vb.AppendNull();
        var tags = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var s = new StructArray(NestedInner, 3, [vb.Build(allocator: null), tags], ArrowBuffer.Empty, 0);
        await table.WriteAsync([new RecordBatch(schema, [ids, s], 3)]);
    }

    // ── Tier 1: delta-rs reads BOTH physical layouts (it ignores the annotation, keys off the schema). ──

    [SkippableTheory]
    [InlineData(true)]   // annotated  (default)
    [InlineData(false)]  // unannotated (Spark-4.0 compatible)
    public async Task EwWritten_DeltaRsReadsVariantBytes(bool annotated)
    {
        DeltaRs.Require();

        await WriteVariantTable(emitAnnotation: annotated);

        var result = DeltaRs.Invoke("read_variant", new { path = _tempDir, col = "v", id_col = "id" });
        var rows = result.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);

        // Row 1: true; row 2: int8(42); row 3: null. Bytes asserted exactly, resolved by name driver-side.
        Assert.Equal(Hex(True), rows[0].GetProperty("value").GetString());
        Assert.Equal(Hex(EmptyMetadata), rows[0].GetProperty("metadata").GetString());
        Assert.Equal(Hex(Int8_42), rows[1].GetProperty("value").GetString());
        Assert.True(rows[2].GetProperty("null").GetBoolean());
    }

    // ── Tier 3: PySpark, version-aware. ──
    //
    // Variant is GA in Spark 4.1 and experimental in 4.0.x, and the two disagree on the parquet
    // annotation: 4.1 both writes and reads it; 4.0.x writes UNannotated and its reader NPEs on an
    // annotated group. So the mode EW must write to be Spark-readable depends on the Spark on hand —
    // exactly what EmitVariantLogicalType exists to select. The test picks the compatible mode for the
    // running Spark, which keeps the tier green whether it is pinned at 4.0.x or upgraded to 4.1.

    private static bool SparkHasGaVariant(out string version)
    {
        version = Spark.Version ?? "";
        // Version looks like "pyspark 4.1.1 / delta-spark 4.1.0"; GA variant is Spark >= 4.1.
        int i = version.IndexOf("4.", StringComparison.Ordinal);
        if (i < 0) return false;
        var parts = version.Substring(i).Split('.', ' ');
        return parts.Length >= 2 && int.TryParse(parts[1], out int minor) && minor >= 1;
    }

    [SkippableFact]
    public async Task EwWritten_SparkReadsVariant()
    {
        Spark.Require();

        // Write the layout THIS Spark can read: annotated for 4.1+ (GA), unannotated for 4.0.x.
        bool ga = SparkHasGaVariant(out _);
        await WriteVariantTable(emitAnnotation: ga);

        var result = Spark.Invoke("read_variant", new { path = _tempDir, col = "v", id_col = "id" });
        var rows = result.GetProperty("rows").EnumerateArray().ToList();

        // to_json renders the decoded variant; if the bytes were wrong Spark would raise MALFORMED_VARIANT.
        Assert.Equal("true", rows[0].GetProperty("vjson").GetString());
        Assert.Equal("42", rows[1].GetProperty("vjson").GetString());
        Assert.True(rows[2].GetProperty("vjson").ValueKind == System.Text.Json.JsonValueKind.Null
            || rows[2].GetProperty("vjson").GetString() is null);
    }

    [SkippableFact]
    public async Task SparkWritten_EwReadsVariant()
    {
        Spark.Require();

        Spark.Invoke("write_variant", new
        {
            path = _tempDir,
            rows = new object[]
            {
                new { id = 1L, json = "{\"a\":1,\"b\":\"x\"}" },
                new { id = 2L, json = "[1,2,3]" },
                new { id = 3L, json = (string?)null },
            },
        });

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        // EW must present it as a VariantArray (whether or not Spark annotated the group) with the values
        // intact — the Spark->EW direction that silently degraded to a struct before the schema-driven
        // coercion, and that also exercises Spark's (value, metadata) child order vs EW's (metadata, value).
        var col = batches.SelectMany(b =>
        {
            var v = Assert.IsType<VariantArray>(b.Column(b.Schema.GetFieldIndex("v")));
            return Enumerable.Range(0, v.Length).Select(i => v.IsNull(i) ? null : v.GetValueBytes(i).ToArray());
        }).ToList();

        Assert.Equal(3, col.Count);
        Assert.Contains(col, x => x is null);                 // the SQL-NULL row
        Assert.Equal(2, col.Count(x => x is not null));       // the two real variants round-tripped
    }

    // ── Tier 3, NESTED variant (variant inside a struct). GA-only. ──
    //
    // EW's parquet layer wraps a nested variant (VariantNestedWrapper) and annotates it on write at
    // every depth; the Delta layer round-trips it (NestedVariant_InsideStruct_RoundTrips). What only a
    // reference reader/writer can prove is that the NESTED physical group — annotation + child order —
    // matches the spec. Both directions require GA variant (Spark >= 4.1): EW always annotates the
    // nested group (StripAnnotation is top-level only) and Spark 4.0.x's reader NPEs on the annotation.
    // On a 4.0.x Spark these report as SKIPPED — a version gate, not a missing toolchain, and not the
    // silent pass this used to be.

    [SkippableFact]
    public async Task EwWrittenNestedVariant_SparkReadsVariant()
    {
        Spark.Require();
        Skip.IfNot(SparkHasGaVariant(out string sparkVersion),
            $"GA VARIANT needs Spark 4.1+; resolved {sparkVersion}");

        await WriteNestedVariantTable();

        // to_json(s.v) forces Spark to DECODE the nested variant; wrong bytes -> MALFORMED_VARIANT.
        var result = Spark.Invoke("read_variant", new { path = _tempDir, col = "s.v", id_col = "id" });
        var rows = result.GetProperty("rows").EnumerateArray().ToList();

        Assert.Equal("true", rows[0].GetProperty("vjson").GetString());
        Assert.Equal("42", rows[1].GetProperty("vjson").GetString());
        Assert.True(rows[2].GetProperty("vjson").ValueKind == System.Text.Json.JsonValueKind.Null
            || rows[2].GetProperty("vjson").GetString() is null);
    }

    [SkippableFact]
    public async Task SparkWrittenNestedVariant_EwReadsVariant()
    {
        Spark.Require();
        Skip.IfNot(SparkHasGaVariant(out string sparkVersion),
            $"GA VARIANT needs Spark 4.1+; resolved {sparkVersion}");

        Spark.Invoke("write_nested_variant", new
        {
            path = _tempDir,
            rows = new object[]
            {
                new { id = 1L, json = "{\"a\":1,\"b\":\"x\"}", tag = "a" },
                new { id = 2L, json = "[1,2,3]", tag = "b" },
                new { id = 3L, json = (string?)null, tag = "c" },
            },
        });

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);

        // The nested column must present as struct<v: VariantArray, tag> — EW must reconcile Spark's
        // annotated nested group and its (value, metadata) child order against the schema's VariantType.
        var values = batches.SelectMany(b =>
        {
            var s = Assert.IsType<StructArray>(b.Column(b.Schema.GetFieldIndex("s")));
            var v = Assert.IsType<VariantArray>(s.Fields[0]);
            return Enumerable.Range(0, v.Length).Select(i => v.IsNull(i) ? null : v.GetValueBytes(i).ToArray());
        }).ToList();

        Assert.Equal(3, values.Count);
        Assert.Contains(values, x => x is null);              // the SQL-NULL nested variant
        Assert.Equal(2, values.Count(x => x is not null));    // the two real nested variants round-tripped
    }

    // ── Tier 3, SHREDDED Spark tables (#671). ──
    //
    // delta.enableVariantShredding makes delta-spark 4.1+ declare variantShredding (or, with its force conf, the
    // preview name) and Spark 4.1 shred each file. Before #671 EW refused to open either table. GA-only: Spark
    // 4.0 shreds only under a test-only forced schema, and delta-spark 4.0.0 knows only the preview name.

    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SparkWrittenShredded_EwReadsAndAppends(bool preview)
    {
        Spark.Require();
        Skip.IfNot(SparkHasGaVariant(out string sparkVersion),
            $"Shredded Delta writes need Spark 4.1+ / delta-spark 4.1+; resolved {sparkVersion}");

        string[] json = ["{\"a\":1,\"b\":\"x\"}", "{\"a\":2,\"b\":\"y\"}", "{\"a\":3,\"b\":\"z\"}"];
        var result = Spark.Invoke("write_shredded_variant", new
        {
            path = _tempDir,
            preview,
            rows = new object[]
            {
                new { id = 1L, json = json[0] },
                new { id = 2L, json = json[1] },
                new { id = 3L, json = json[2] },
                new { id = 4L, json = (string?)null },
            },
        });

        string feature = preview ? "variantShredding-preview" : "variantShredding";
        var features = result.GetProperty("tableFeatures").EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Contains(feature, features);

        // Spark shredded at least one file, or this would not be testing the shredded read.
        bool anyShredded = false;
        foreach (string file in Directory.GetFiles(_tempDir, "*.parquet", SearchOption.TopDirectoryOnly))
        {
            await using var raf = new LocalRandomAccessFile(file);
            await using var reader = new Parquet.ParquetFileReader(raf, ownsFile: false);
            anyShredded |= (await reader.ReadMetadataAsync()).Schema.Any(s => s.Name == "typed_value");
        }
        Assert.True(anyShredded, "Spark wrote no shredded data file");

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Contains(feature, table.CurrentSnapshot.Protocol.ReaderFeatures!);

        async Task<List<string?>> ReadAsync()
        {
            var values = new List<(long Id, string? Json)>();
            await foreach (var b in table.ReadAllAsync())
            {
                var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var v = Assert.IsType<VariantArray>(b.Column(b.Schema.GetFieldIndex("v")));
                for (int i = 0; i < v.Length; i++)
                {
                    values.Add((ids.GetValue(i)!.Value,
                        v.IsNull(i) ? null : VariantJsonWriter.ToJson(v.GetLogicalVariantValue(i), indented: false)));
                }
            }
            return values.OrderBy(x => x.Id).Select(x => x.Json).ToList();
        }

        // The values come back reassembled from typed_value, not empty.
        Assert.Equal([json[0], json[1], json[2], null], await ReadAsync());

        // delta-spark 4.3+ also writes z85-encoded variant bounds, a STRING under minValues.v in the JSON stats.
        // A filtered read parses those stats for pruning, and must neither throw nor prune on the variant bound.
        var add = (await new DeltaLake.Log.TransactionLog(fs).ReadCommitAsync(1)).OfType<DeltaLake.Actions.AddFile>().First();
        using (var stats = System.Text.Json.JsonDocument.Parse(add.Stats!))
        {
            bool variantBounds = stats.RootElement.TryGetProperty("minValues", out var min)
                && min.TryGetProperty("v", out var vMin) && vMin.ValueKind == System.Text.Json.JsonValueKind.String;
            if (DeltaSparkAtLeast(4, 3))
                Assert.True(variantBounds, $"delta-spark 4.3+ wrote no variant bounds: {add.Stats}");
        }
        // The filter prunes FILES (there is no row-level post-filter), so: every id bound excludes the file
        // (0 rows), and one that admits it keeps all four.
        async Task<long> CountAsync(EngineeredWood.Expressions.Predicate filter)
        {
            long n = 0;
            await foreach (var b in table.ReadAsync(new DeltaReadOptions { Filter = filter }))
                n += b.Length;
            return n;
        }
        Assert.Equal(0, await CountAsync(EngineeredWood.Expressions.Expressions.GreaterThan("id", 10L)));
        Assert.Equal(4, await CountAsync(EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", 2L)));

        // Writable too: EW appends unshredded, which a shredding table permits, and Spark reads the mix.
        var appendIds = new Int64Array.Builder().Append(5).Build();
        var appendVariants = new VariantArray.Builder();
        appendVariants.Append(Apache.Arrow.Scalars.Variant.VariantValue.FromInt32(7));
        var appendSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("v", VariantType.Default, true))
            .Build();
        await table.WriteAsync([new RecordBatch(appendSchema, [appendIds, appendVariants.Build(allocator: null)], 1)]);
        Assert.Equal([json[0], json[1], json[2], null, "7"], await ReadAsync());

        var sparkRead = Spark.Invoke("read_variant", new { path = _tempDir, col = "v", id_col = "id" });
        Assert.Equal(5, sparkRead.GetProperty("rows").GetArrayLength());

        // An EW checkpoint over those stats: stats_parsed has no variant bounds, and both engines still read it.
        await table.CheckpointAsync();
        await using (var fromCheckpoint = await DeltaTable.OpenAsync(fs))
        {
            long rows = 0;
            await foreach (var b in fromCheckpoint.ReadAllAsync())
                rows += b.Length;
            Assert.Equal(5, rows);
        }
        sparkRead = Spark.Invoke("read_variant", new { path = _tempDir, col = "v", id_col = "id" });
        Assert.Equal(5, sparkRead.GetProperty("rows").GetArrayLength());
    }

    /// <summary>True when the resolved delta-spark is at least <paramref name="major"/>.<paramref name="minor"/>.</summary>
    private static bool DeltaSparkAtLeast(int major, int minor)
    {
        // Spark.Version is "<pyspark>/<delta-spark>", e.g. "4.1.2/4.3.1" (Spark.cs's probe).
        string version = Spark.Version ?? "";
        int i = version.IndexOf('/');
        if (i < 0) return false;
        var parts = version.Substring(i + 1).Trim().Split('.');
        return parts.Length >= 2
            && int.TryParse(parts[0], out int ma) && int.TryParse(parts[1], out int mi)
            && (ma > major || (ma == major && mi >= minor));
    }
}
