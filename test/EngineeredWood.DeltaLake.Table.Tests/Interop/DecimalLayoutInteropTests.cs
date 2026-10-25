// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// EW writes a decimal column in the layout its precision calls for (#504): INT32 up to 9 digits, INT64 up to
/// 18, then the narrowest FIXED_LEN_BYTE_ARRAY. Spark and delta-rs must decode every one of them to the values
/// written. The edges (±(10^p - 1)) are where a narrowing that dropped the top bytes would show.
/// </summary>
[Collection("Interop")]
public class DecimalLayoutInteropTests : IDisposable
{
    private readonly string _tempDir;

    // Column, precision, and the layout EW must have written it in.
    private static readonly (string Name, int Precision, Parquet.PhysicalType Type, int? Length)[] Columns =
    [
        ("d9", 9, Parquet.PhysicalType.Int32, null),
        ("d18", 18, Parquet.PhysicalType.Int64, null),
        ("d20", 20, Parquet.PhysicalType.FixedLenByteArray, 9),
        ("d38", 38, Parquet.PhysicalType.FixedLenByteArray, 16),
    ];

    public DecimalLayoutInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_decimal_layout_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    [SkippableFact]
    public async Task Spark_DecodesEveryLayout()
    {
        Spark.Require();
        var expected = await WriteTableAsync();

        AssertRows(expected, Spark.Invoke("read", new { path = _tempDir }));
    }

    [SkippableFact]
    public async Task DeltaRs_DecodesEveryLayout()
    {
        DeltaRs.Require();
        var expected = await WriteTableAsync();

        AssertRows(expected, DeltaRs.Invoke("read", new { path = _tempDir }));
    }

    /// <summary>Writes the table and returns, per id, each column's value as decimal text (null for a null).</summary>
    private async Task<Dictionary<long, string?[]>> WriteTableAsync()
    {
        var fields = new List<Field> { new("id", Int64Type.Default, false) };
        foreach (var c in Columns)
            fields.Add(new Field(c.Name, new Decimal128Type(c.Precision, 2), true));
        var schema = new Apache.Arrow.Schema(fields, null);

        // Per column: 0, -0.01, the largest and smallest values the precision allows, and a null.
        var rows = new Dictionary<long, string?[]>();
        var arrays = new List<IArrowArray>();
        const int rowCount = 5;
        arrays.Add(new Int64Array.Builder().AppendRange(Enumerable.Range(0, rowCount).Select(i => (long)i)).Build());
        for (long id = 0; id < rowCount; id++)
            rows[id] = new string?[Columns.Length];

        for (int c = 0; c < Columns.Length; c++)
        {
            var max = BigInteger.Pow(10, Columns[c].Precision) - 1;
            BigInteger?[] values = [0, -1, max, -max, null];
            arrays.Add(Decimals(new Decimal128Type(Columns[c].Precision, 2), values));
            for (int id = 0; id < rowCount; id++)
                rows[id][c] = values[id] is { } v ? Text(v) : null;
        }

        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([new RecordBatch(schema, arrays, rowCount)]);

        // The layouts the readers are being asked about must be the ones in the file.
        string file = Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Single();
        await using var raf = new LocalRandomAccessFile(file);
        await using var reader = new Parquet.ParquetFileReader(raf, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        foreach (var c in Columns)
        {
            var element = metadata.Schema.First(s => s.Name == c.Name);
            Assert.Equal((c.Type, c.Length), (element.Type!.Value, element.TypeLength));
        }

        return rows;
    }

    private static void AssertRows(Dictionary<long, string?[]> expected, JsonElement result)
    {
        Assert.Equal(expected.Count, result.GetProperty("row_count").GetInt32());
        foreach (var row in result.GetProperty("rows").EnumerateArray())
        {
            long id = row.GetProperty("id").GetInt64();
            for (int c = 0; c < Columns.Length; c++)
            {
                var value = row.GetProperty(Columns[c].Name);
                string? actual = value.ValueKind == JsonValueKind.Null ? null : value.GetString();
                Assert.True(expected[id][c] == actual,
                    $"{Columns[c].Name} at id {id}: expected {expected[id][c] ?? "null"}, got {actual ?? "null"}");
            }
        }
    }

    private static IArrowArray Decimals(Decimal128Type type, BigInteger?[] unscaled)
    {
        var bytes = new byte[unscaled.Length * 16];
        var validity = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < unscaled.Length; i++)
        {
            validity.Append(unscaled[i] is not null);
            var value = unscaled[i] ?? BigInteger.Zero;
            var dest = bytes.AsSpan(i * 16, 16);
            dest.Fill(value.Sign < 0 ? (byte)0xFF : (byte)0x00);
            value.ToByteArray().CopyTo(dest);
        }
        return new Decimal128Array(new ArrayData(type, unscaled.Length, unscaled.Count(v => v is null), 0,
            [validity.Build(), new ArrowBuffer(bytes)]));
    }

    // An unscaled value at scale 2 as Python's str(Decimal) renders it: "0.00", "-0.01", "9999999.99".
    private static string Text(BigInteger unscaled)
    {
        string digits = BigInteger.Abs(unscaled).ToString().PadLeft(3, '0');
        return (unscaled.Sign < 0 ? "-" : "") + digits.Substring(0, digits.Length - 2) + "." + digits.Substring(digits.Length - 2);
    }
}
