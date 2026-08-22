// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// External validation of EW's SHREDDED variant output — the one claim the shredding work had not
/// measured. Round-tripping through EW's own reader cannot prove a shredded file is readable
/// elsewhere: EW would reassemble its own layout happily even if that layout were wrong, which is
/// precisely the failure the unshredded path once had (the reader keyed off the parquet annotation
/// rather than the schema, and nothing noticed until this suite's Delta sibling existed).
///
/// <para><b>Why these tests live in this project.</b> Shredding on write is a PARQUET feature and
/// nothing here touches a Delta log — the files are raw parquet. They sit beside the other tiers
/// because the driver harness does, and duplicating it into the parquet test project would buy
/// nothing.</para>
///
/// <para><b>Three readers, three different questions.</b> Spark 4.1 has the reference variant
/// implementation and answers "does a foreign engine see a VARIANT and decode it". DuckDB has a
/// native VARIANT type and its own parquet reader, answering the same question through completely
/// separate code. pyarrow (parquet-cpp, present in the delta-rs tier's environment) answers the one
/// neither can: whether what is ON DISK matches the shredding spec's layout contract, since both of
/// the others materialise the logical value and would hide a layout EW alone knew how to read.</para>
/// </summary>
[Collection("Interop")]
public class VariantShreddingInteropTests : IDisposable
{
    private readonly string _tempDir;

    public VariantShreddingInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"variant_shred_xval_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static VariantValue Obj(int a, string b) => VariantValue.FromObject(
        new Dictionary<string, VariantValue>
        {
            ["a"] = VariantValue.FromInt32(a),
            ["b"] = VariantValue.FromString(b),
        });

    /// <summary>Row 3 is SQL NULL; the rest are uniform objects, so inference hoists both fields.</summary>
    private static readonly string[] ExpectedJson =
    [
        "{\"a\":1,\"b\":\"x\"}",
        "{\"a\":2,\"b\":\"y\"}",
        null!,
    ];

    /// <summary>
    /// Writes <c>id BIGINT, v VARIANT</c> as raw parquet, shredded or not, and returns the path.
    /// </summary>
    private async Task<string> WriteAsync(bool shredded)
    {
        string path = Path.Combine(_tempDir, shredded ? "shredded.parquet" : "canonical.parquet");

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var vb = new VariantArray.Builder();
        vb.Append(Obj(1, "x"));
        vb.Append(Obj(2, "y"));
        vb.AppendNull();
        var variants = vb.Build(allocator: null);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", variants.Data.DataType, true))
            .Build();

        var options = shredded
            ? ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default }
            : ParquetWriteOptions.Default;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [ids, variants], 3));
            await writer.CloseAsync();
        }

        // Sanity, so a foreign reader's disagreement below is never ambiguous about WHICH layout it
        // disagreed with.
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        Assert.Equal(shredded, meta.Schema.Any(s => s.Name == "typed_value"));

        return path;
    }

    private static bool SparkHasGaVariant()
    {
        string version = Spark.Version ?? "";
        int i = version.IndexOf("4.", StringComparison.Ordinal);
        if (i < 0) return false;
        var parts = version.Substring(i).Split('.', ' ');
        return parts.Length >= 2 && int.TryParse(parts[1], out int minor) && minor >= 1;
    }

    [SkippableTheory]
    [InlineData(true)]   // shredded — the layout this suite exists for
    [InlineData(false)]  // canonical — the control: same values, different layout
    public async Task EwWritten_SparkReadsVariant(bool shredded)
    {
        Spark.Require();
        // 4.0.x predates the VARIANT logical type entirely.
        Skip.IfNot(SparkHasGaVariant(), $"GA VARIANT needs Spark 4.1+; resolved {Spark.Version}");

        string path = await WriteAsync(shredded);
        var result = Spark.Invoke("read_parquet_variant", new { path, col = "v", id_col = "id" });

        // A shredded group has THREE children; a reader that keyed off the two-child storage shape
        // would surface a struct here rather than a variant.
        Assert.Equal("variant", result.GetProperty("column_type").GetString());

        var rows = result.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);
        for (int i = 0; i < 3; i++)
        {
            if (ExpectedJson[i] is null)
            {
                Assert.True(rows[i].GetProperty("null").GetBoolean());
                continue;
            }
            Assert.False(rows[i].GetProperty("null").GetBoolean());
            // to_json forces a DECODE: for a shredded row the data is in typed_value, so a reader
            // that ignored shredding would report an empty value rather than throw.
            Assert.Equal(ExpectedJson[i], rows[i].GetProperty("vjson").GetString());
        }
    }

    [SkippableTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EwWritten_DuckDbReadsVariant(bool shredded)
    {
        DuckDb.Require();
        // < 1.4 has no VARIANT; it would read a bare struct.
        Skip.IfNot(DuckDb.HasVariantType, $"DuckDB VARIANT needs 1.4+; resolved {DuckDb.Version}");

        string path = await WriteAsync(shredded);
        var result = DuckDb.Invoke("read_parquet_variant", new { path, col = "v", order_by = "id" });

        Assert.Equal("VARIANT", result.GetProperty("column_type").GetString());

        var rows = result.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);
        for (int i = 0; i < 3; i++)
        {
            if (ExpectedJson[i] is null)
            {
                Assert.True(rows[i].GetProperty("null").GetBoolean());
                continue;
            }
            Assert.False(rows[i].GetProperty("null").GetBoolean());
            Assert.Equal(ExpectedJson[i], rows[i].GetProperty("vjson").GetString());
        }
    }

    /// <summary>
    /// What is actually on disk, per parquet-cpp. The spec's layout contract for a shredded column:
    /// <c>metadata</c> required, <c>value</c> optional and NULL on a row that shredded cleanly, and
    /// one <c>value</c>/<c>typed_value</c> pair per hoisted field under <c>typed_value</c>.
    /// </summary>
    [SkippableFact]
    public async Task EwWrittenShredded_MatchesTheSpecLayout()
    {
        DeltaRs.Require();

        string path = await WriteAsync(shredded: true);
        var result = DeltaRs.Invoke("parquet_variant_layout", new { path, col = "v" });

        var leaves = result.GetProperty("leaves");
        var nullable = result.GetProperty("nullable");

        Assert.False(nullable.GetProperty("v.metadata").GetBoolean());
        Assert.True(nullable.GetProperty("v.value").GetBoolean());

        // One pair per hoisted field, and the typed leaf carries the field's PHYSICAL type — the
        // whole point of shredding, and what a statistics/pruning reader keys off.
        Assert.Equal("INT32",
            leaves.GetProperty("v.typed_value.a.typed_value").GetProperty("physical_type").GetString());
        Assert.Equal("BYTE_ARRAY",
            leaves.GetProperty("v.typed_value.b.typed_value").GetProperty("physical_type").GetString());
        Assert.True(nullable.GetProperty("v.typed_value.a.value").GetBoolean());
        Assert.True(nullable.GetProperty("v.typed_value.b.value").GetBoolean());

        var rows = result.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);

        // A row that shredded cleanly leaves the residual NULL — not empty bytes, which is the
        // encoding a reader would surface as a present-but-empty variant.
        Assert.True(rows[0].GetProperty("residual_value_null").GetBoolean());
        Assert.True(rows[1].GetProperty("residual_value_null").GetBoolean());

        // The SQL-NULL row is a null GROUP, which is how the spec spells row-level missingness —
        // distinct from a present value holding a variant JSON null.
        Assert.True(rows[2].GetProperty("null").GetBoolean());
    }

    /// <summary>
    /// The control for the layout assertions: the same values written canonically have no
    /// <c>typed_value</c> at all, so the differences above are layout and not content.
    /// </summary>
    [SkippableFact]
    public async Task EwWrittenCanonical_HasNoTypedValue()
    {
        DeltaRs.Require();

        string path = await WriteAsync(shredded: false);
        var result = DeltaRs.Invoke("parquet_variant_layout", new { path, col = "v" });

        var paths = result.GetProperty("leaves").EnumerateObject()
            .Select(l => l.Name).ToList();

        Assert.Contains("v.metadata", paths);
        Assert.Contains("v.value", paths);
        Assert.DoesNotContain(paths, p => p.Contains("typed_value", StringComparison.Ordinal));
    }
}
