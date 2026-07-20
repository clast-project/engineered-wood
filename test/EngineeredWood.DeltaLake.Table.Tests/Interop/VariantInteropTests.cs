// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
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
/// </summary>
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

    // ── Tier 1: delta-rs reads BOTH physical layouts (it ignores the annotation, keys off the schema). ──

    [Theory]
    [InlineData(true)]   // annotated  (default)
    [InlineData(false)]  // unannotated (Spark-4.0 compatible)
    public async Task EwWritten_DeltaRsReadsVariantBytes(bool annotated)
    {
        if (!DeltaRs.EnsureAvailable()) return;

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

    [Fact]
    public async Task EwWritten_SparkReadsVariant()
    {
        if (!Spark.EnsureAvailable()) return;

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

    [Fact]
    public async Task SparkWritten_EwReadsVariant()
    {
        if (!Spark.EnsureAvailable()) return;

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
}
