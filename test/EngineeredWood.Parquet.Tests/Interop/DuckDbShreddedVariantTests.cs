// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Interop;

/// <summary>
/// EW reads SHREDDED variant files that DuckDB wrote. Every other shredded file EW's reader is tested
/// on came from EW's own writer, so a reader and writer that agreed on a wrong layout would pass;
/// DuckDB (1.5+) shreds a <c>VARIANT</c> column on <c>COPY … TO parquet</c> by default, independently.
/// </summary>
/// <remarks>
/// <para>The oracle is DuckDB's reading of its own file: EW must decode each row to the value DuckDB
/// does. Each case first proves the file really is shredded the way it claims (a named
/// <c>typed_value</c> leaf), so an inference change in DuckDB cannot quietly turn a case into an
/// unshredded one that validates nothing.</para>
///
/// <para>DuckDB's choice of what to shred decides what is covered: a uniform column puts every row in
/// <c>typed_value</c>; a mixed one shreds the most common type and leaves the rest in the residual
/// <c>value</c>, so both halves of the reassembly run.</para>
/// </remarks>
public class DuckDbShreddedVariantTests : IDisposable
{
    private readonly string _tempDir;

    public DuckDbShreddedVariantTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-duckshred-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>A uniform scalar column of three rows with a SQL NULL in the middle one.</summary>
    private static string Uniform(string literal) =>
        $"SELECT i AS id, CASE WHEN i = 1 THEN NULL ELSE {literal} END::VARIANT AS v FROM range(3) t(i)";

    /// <summary>
    /// Shapes whose JSON EW and DuckDB print identically, so DuckDB's JSON is the expected value row
    /// for row. The middle string is a leaf the file must contain for the case to be the one it claims.
    /// </summary>
    public static TheoryData<string, string, string> JsonComparableShapes => new()
    {
        {
            "objects", "v, typed_value, b, typed_value",
            "SELECT i AS id, {'a': i, 'b': 's' || i}::VARIANT AS v FROM range(4) t(i)"
        },
        {
            // The most common type (INT32) is shredded; a string, an object, an array and two NULLs
            // stay in the residual. DuckDB 2.0 stores the JSON null as a SQL NULL variant.
            "mixed", "v, typed_value",
            "SELECT * FROM (VALUES (0, 1::VARIANT), (1, 'two'::VARIANT), (2, {'a': 3}::VARIANT), "
            + "(3, [1,2]::VARIANT), (4, NULL::VARIANT), (5, 'null'::JSON::VARIANT)) t(id, v)"
        },
        {
            // An object inside an object, and arrays of scalars and of objects inside both.
            "nested", "v, typed_value, c, typed_value, list, element, typed_value, k, typed_value",
            "SELECT i AS id, {'a': {'x': i, 'y': [i, i+1]}, 'c': [{'k': i}]}::VARIANT AS v FROM range(3) t(i)"
        },
        {
            "arrays", "v, typed_value, list, element, typed_value",
            "SELECT i AS id, [i, i*2, i*3]::VARIANT AS v FROM range(3) t(i)"
        },
        {
            // Several row groups under one file-level layout: the first fits it, the last is mostly
            // residual. Objects are 80% of the rows: at an even split DuckDB's inference sometimes
            // shreds the strings instead, and the leaf check fails the case rather than run it unshredded.
            "rowgroups", "v, typed_value, a, typed_value",
            "SELECT i AS id, (CASE WHEN i < 4000 THEN {'a': i}::VARIANT ELSE ('s' || i)::VARIANT END) AS v "
            + "FROM range(5000) t(i)"
        },
        { "tinyint", "v, typed_value", Uniform("(-5)::TINYINT") },
        { "smallint", "v, typed_value", Uniform("(-300)::SMALLINT") },
        { "integer", "v, typed_value", Uniform("70000::INTEGER") },
        { "bigint", "v, typed_value", Uniform("5000000000::BIGINT") },
        { "float", "v, typed_value", Uniform("1.5::FLOAT") },
        { "double", "v, typed_value", Uniform("2.25::DOUBLE") },
        { "decimal38", "v, typed_value", Uniform("12345678901234567890.0123456789::DECIMAL(38,10)") },
        { "boolean", "v, typed_value", Uniform("true") },
        { "date", "v, typed_value", Uniform("DATE '2026-11-04'") },
        { "string", "v, typed_value", Uniform("'text'") },
        { "uuid", "v, typed_value", Uniform("'0f8fad5b-d9cb-469f-a165-70867728950e'::UUID") },
    };

    [SkippableTheory]
    [MemberData(nameof(JsonComparableShapes))]
    public async Task DuckDbWritten_ReadsAsDuckDbReadsIt(string name, string typedLeaf, string query)
    {
        var (duck, ew) = await WriteWithDuckDbAndReadAsync(name, typedLeaf, query);

        Assert.Equal(duck.Json.Count, ew.Count);
        for (int i = 0; i < ew.Count; i++)
        {
            string? actual = ew[i] is { } v ? Json(v) : null;
            Assert.True(duck.Json[i] == actual,
                $"row {i}: DuckDB read {duck.Json[i] ?? "NULL"}, EW read {actual ?? "NULL"}");
        }
    }

    /// <summary>
    /// Types whose JSON the two print differently: DuckDB writes a space-separated timestamp and
    /// <c>\x..</c> binary, Arrow's writer ISO 8601, base64, and a nanosecond timestamp or a time as its
    /// raw count. These assert the decoded variant itself, its primitive type and its value.
    /// </summary>
    public static TheoryData<string, string, string, string> TypedScalars => new()
    {
        { "timestamp", "TIMESTAMP '2026-11-04 12:34:56.789'", "TimestampNtz", "\"2026-11-04T12:34:56.7890000\"" },
        { "timestamptz", "TIMESTAMPTZ '2026-11-04 12:34:56+00'", "Timestamp", "\"2026-11-04T12:34:56.0000000\\u002B00:00\"" },
        // 2026-11-04 12:34:56.123456789 in nanoseconds since the epoch.
        { "timestamp_ns", "TIMESTAMP_NS '2026-11-04 12:34:56.123456789'", "TimestampNtzNanos", "1793795696123456789" },
        // 12:34:56.5 in microseconds since midnight.
        { "time", "TIME '12:34:56.5'", "TimeNtz", "45296500000" },
        { "blob", "'\\x01\\x02'::BLOB", "Binary", "\"AQI=\"" },
    };

    [SkippableTheory]
    [MemberData(nameof(TypedScalars))]
    public async Task DuckDbWritten_TypedScalar_DecodesToItsVariantType(
        string name, string literal, string primitiveType, string json)
    {
        var (_, ew) = await WriteWithDuckDbAndReadAsync(name, "v, typed_value", Uniform(literal));

        Assert.Equal(3, ew.Count);
        Assert.Null(ew[1]);
        foreach (var value in new[] { ew[0]!.Value, ew[2]!.Value })
        {
            Assert.Equal(primitiveType, value.PrimitiveType.ToString());
            Assert.Equal(json, Json(value));
        }
    }

    /// <summary>
    /// A shredded DECIMAL that fits 4 or 8 bytes reads back with the right VALUE but not the right
    /// SCALE: <c>12.30</c> comes back as <c>12.3</c>. EW reads the <c>typed_value</c> column correctly,
    /// as a Decimal32/Decimal64 of the declared scale, but Apache.Arrow 23's
    /// <c>Decimal32Array.GetValue</c> and <c>Decimal64Array.GetValue</c>, which the reassembly calls,
    /// return a scale-normalised <c>decimal</c>: their decode divides, and <c>decimal</c> division drops
    /// trailing zeros. The same values in the residual keep their scale, as does a 16-byte decimal on
    /// .NET 7+. This pins today's behaviour, so a fixed Arrow shows up here; see doc/known-issues.md.
    /// </summary>
    [SkippableTheory]
    [InlineData("decimal9", "12.30::DECIMAL(9,2)", "12.30", "12.3")]
    [InlineData("decimal18", "123456.7890::DECIMAL(18,4)", "123456.7890", "123456.789")]
    public async Task DuckDbWritten_NarrowDecimal_KeepsItsValueButLosesTrailingZeros(
        string name, string literal, string duckJson, string ewJson)
    {
        var (duck, ew) = await WriteWithDuckDbAndReadAsync(name, "v, typed_value", Uniform(literal));

        Assert.Equal(duckJson, duck.Json[0]);
        Assert.Equal(ewJson, Json(ew[0]!.Value));
        Assert.Equal(
            decimal.Parse(duckJson, CultureInfo.InvariantCulture),
            decimal.Parse(Json(ew[0]!.Value), CultureInfo.InvariantCulture));
    }

    private static string Json(VariantValue value) => VariantJsonWriter.ToJson(value, indented: false);

    /// <summary>
    /// DuckDB writes <paramref name="query"/> and reports its own reading of the file; EW then reads the
    /// same file. Returns both, EW's as one value per row in file order (null for a NULL variant).
    /// </summary>
    private async Task<(ExternalParquetReaders.DuckDbVariantFile Duck, List<VariantValue?> Ew)> WriteWithDuckDbAndReadAsync(
        string name, string typedLeaf, string query)
    {
        ExternalParquetReaders.Require();

        string path = Path.Combine(_tempDir, name + ".parquet");
        // ORDER BY inside the COPY fixes the file's row order, so EW's file order matches DuckDB's
        // ORDER BY id row for row. ROW_GROUP_SIZE keeps the several-row-group case small.
        var (duck, installed, error) = ExternalParquetReaders.DuckDbWriteVariant(
            path, query + " ORDER BY id", orderBy: "id", copyOptions: "ROW_GROUP_SIZE 2048");
        Skip.IfNot(installed, $"DuckDB is not installed in the parquet-readers interpreter: {error}");
        Assert.True(duck is not null, $"DuckDB failed to write the {name} file: {error}");
        Assert.True(duck!.Leaves.Contains(typedLeaf),
            $"DuckDB {duck.Version} did not shred the {name} column as expected; leaves: {string.Join(" | ", duck.Leaves)}");

        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = registry });

        var meta = await reader.ReadMetadataAsync();
        if (name == "rowgroups")
            Assert.True(meta.RowGroups.Count > 1, $"expected several row groups, got {meta.RowGroups.Count}");

        var values = new List<VariantValue?>();
        for (int g = 0; g < meta.RowGroups.Count; g++)
        {
            var batch = await reader.ReadRowGroupAsync(g);
            var variant = Assert.IsType<VariantArray>(batch.Column(batch.Schema.GetFieldIndex("v")));
            for (int i = 0; i < variant.Length; i++)
                values.Add(variant.IsNull(i) ? null : variant.GetLogicalVariantValue(i));
        }

        return (duck, values);
    }
}
