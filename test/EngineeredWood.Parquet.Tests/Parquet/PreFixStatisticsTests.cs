// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Files written by EngineeredWood before #398, which is every release up to 0.3.0, recorded
/// chunk bounds ordered by physical type: FIXED_LEN_BYTE_ARRAY decimals as unsigned bytes,
/// unsigned integers as signed. A chunk with values on both sides of the break (zero for the
/// decimals, 2^31 or 2^63 for the integers) came out with min above max, and row-group pruning
/// skipped it for every predicate, returning none of its matching rows.
/// </summary>
/// <remarks>
/// Such a file cannot be written any more, so this writes today's and puts back the bounds the
/// old writer chose, which were measured by writing the same data with a build of edabc29~1:
/// the smallest value above the break as the minimum, the largest below it as the maximum.
/// </remarks>
public class PreFixStatisticsTests : IDisposable
{
    private const int Rows = 1000;
    private const ulong TwoTo63 = 9_223_372_036_854_775_808UL;

    private readonly string _tempDir;

    public PreFixStatisticsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-prefix-stats-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<string, string, bool> Queries() => new()
    {
        // label, column, whether row group 1 (the control, whose bounds are right) can be skipped
        { "d18 = -250", "d18", true },
        { "d18 < 0", "d18", true },
        { "d18 > 400", "d18", false },
        { "d38 < 0", "d38", true },
        { "d50 < 0", "d50", true },
        // A decimal IN list widens its members by type before comparing (#280/#323), which here
        // forces a rounding the evaluator will not guess at: Unknown even against correct bounds.
        { "d18 IN (-250, -1)", "d18", false },
        { "u32 = 100", "u32", true },
        { "u32 > 2^31", "u32", true },
        { "u64 = 100", "u64", true },
        { "u64 > 2^63", "u64", true },
    };

    [Theory]
    [MemberData(nameof(Queries))]
    public async Task InvertedBounds_DoNotLoseMatchingRows(string label, string column, bool controlSkippable)
    {
        _ = column;
        var (filter, match) = Query(label);
        string path = await WriteWithPreFixBoundsAsync();

        // The rewrite reproduces the old footers: row group 0 reads as min above max.
        await AssertInvertedAsync(path, column);

        var all = await ReadAsync(path, null);
        var filtered = await ReadAsync(path, filter);
        Assert.Equal(Count(all, match), Count(filtered, match));

        // Row group 0 must be kept; the control is still pruned wherever its (correct) bounds allow.
        int returned = filtered.Sum(b => b.Length);
        Assert.Equal(controlSkippable ? Rows : 2 * Rows, returned);
    }

    private static (Predicate Filter, Func<RecordBatch, int, bool> Match) Query(string label) => label switch
    {
        "d18 = -250" => (Ex.Equal("d18", LiteralValue.Of(-250m)), (b, r) => Dec(b, "d18", r) == -250m),
        "d18 < 0" => (Ex.LessThan("d18", LiteralValue.Of(0m)), (b, r) => Dec(b, "d18", r) < 0m),
        "d18 > 400" => (Ex.GreaterThan("d18", LiteralValue.Of(400m)), (b, r) => Dec(b, "d18", r) > 400m),
        "d38 < 0" => (Ex.LessThan("d38", LiteralValue.Of(0m)), (b, r) => Dec(b, "d38", r) < 0m),
        "d50 < 0" => (Ex.LessThan("d50", LiteralValue.Of(0m)), (b, r) => Dec(b, "d50", r) < 0m),
        "d18 IN (-250, -1)" => (Ex.In("d18", LiteralValue.Of(-250m), LiteralValue.Of(-1m)),
            (b, r) => Dec(b, "d18", r) is -250m or -1m),
        "u32 = 100" => (Ex.Equal("u32", LiteralValue.Of(100u)), (b, r) => U32(b, r) == 100u),
        "u32 > 2^31" => (Ex.GreaterThan("u32", LiteralValue.Of(2_147_483_648u)), (b, r) => U32(b, r) > 2_147_483_648u),
        "u64 = 100" => (Ex.Equal("u64", LiteralValue.Of(100UL)), (b, r) => U64(b, r) == 100UL),
        "u64 > 2^63" => (Ex.GreaterThan("u64", LiteralValue.Of(TwoTo63)), (b, r) => U64(b, r) > TwoTo63),
        _ => throw new ArgumentOutOfRangeException(nameof(label)),
    };

    /// <summary>
    /// Row group 0: decimals -500 to 499, integers 0-499 then 2^31 + 500 (u32) or 2^63 + 500 (u64)
    /// onwards. Row group 1, the control: decimals 1000 to 1999, integers 10000 onwards.
    /// </summary>
    private async Task<string> WriteWithPreFixBoundsAsync()
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions()))
        {
            await writer.WriteRowGroupAsync(Group(0));
            await writer.WriteRowGroupAsync(Group(1));
            await writer.CloseAsync();
        }

        // The old writer's bounds for row group 0: the smallest value above the break, then the
        // largest below it. The control's bounds were right then too, and are left alone.
        var oldBounds = new Dictionary<string, (byte[] Min, byte[] Max)>
        {
            ["u32"] = (LittleEndian32(2_147_483_648u + 500), LittleEndian32(499)),
            ["u64"] = (LittleEndian64(TwoTo63 + 500), LittleEndian64(499)),
        };

        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        int bodyLength = bytes.Length - 8 - footerLength;
        var metadata = MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bodyLength, footerLength));

        var rowGroup = metadata.RowGroups[0];
        var columns = rowGroup.Columns.Select(chunk =>
        {
            var meta = chunk.MetaData!;
            string name = meta.PathInSchema[^1];
            (byte[] Min, byte[] Max) bounds;
            if (name.StartsWith("d", StringComparison.Ordinal))
            {
                int width = metadata.Schema.Single(e => e.Name == name).TypeLength!.Value;
                int scale = name == "d18" ? 2 : 5;
                // 0 and -1, at the column's scale
                bounds = (BigEndian(BigInteger.Zero, width), BigEndian(-BigInteger.Pow(10, scale), width));
            }
            else
            {
                bounds = oldBounds[name];
            }

            var stats = With(With(meta.Statistics!, nameof(Statistics.MinValue), bounds.Min), nameof(Statistics.MaxValue), bounds.Max);
            return With(chunk, nameof(ColumnChunk.MetaData), With(meta, nameof(ColumnMetaData.Statistics), stats));
        }).ToList();

        var rewritten = With(metadata, nameof(FileMetaData.RowGroups),
            new[] { With(rowGroup, nameof(RowGroup.Columns), columns) }.Concat(metadata.RowGroups.Skip(1)).ToList());
        byte[] footer = MetadataEncoder.EncodeFileMetaData(rewritten);

        using var output = new MemoryStream();
        output.Write(bytes, 0, bodyLength);
        output.Write(footer, 0, footer.Length);
        var suffix = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(suffix, footer.Length);
        "PAR1"u8.CopyTo(suffix.AsSpan(4));
        output.Write(suffix, 0, suffix.Length);
        File.WriteAllBytes(path, output.ToArray());
        return path;
    }

    private static async Task AssertInvertedAsync(string path, string column)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());
        var min = accessor.GetMinValue(metadata.RowGroups[0], column)!.Value;
        var max = accessor.GetMaxValue(metadata.RowGroups[0], column)!.Value;
        Assert.True(min.CompareTo(max) > 0, $"{column}: min {min} is not above max {max}");
    }

    private static RecordBatch Group(int group)
    {
        var d18 = new Decimal128Array.Builder(new Decimal128Type(18, 2));
        var d38 = new Decimal128Array.Builder(new Decimal128Type(38, 5));
        var d50 = new Decimal256Array.Builder(new Decimal256Type(50, 5));
        var u32 = new UInt32Array.Builder();
        var u64 = new UInt64Array.Builder();
        for (int i = 0; i < Rows; i++)
        {
            decimal d = group == 0 ? i - 500 : 1000 + i;
            d18.Append(d);
            d38.Append(d);
            d50.Append(d);
            u32.Append(group == 0 ? (i < 500 ? (uint)i : 2_147_483_648u + (uint)i) : 10_000u + (uint)i);
            u64.Append(group == 0 ? (i < 500 ? (ulong)i : TwoTo63 + (ulong)i) : 10_000UL + (ulong)i);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d18", new Decimal128Type(18, 2), false))
            .Field(new Field("d38", new Decimal128Type(38, 5), false))
            .Field(new Field("d50", new Decimal256Type(50, 5), false))
            .Field(new Field("u32", UInt32Type.Default, false))
            .Field(new Field("u64", UInt64Type.Default, false))
            .Build();
        return new RecordBatch(schema, [d18.Build(), d38.Build(), d50.Build(), u32.Build(), u64.Build()], Rows);
    }

    private static async Task<List<RecordBatch>> ReadAsync(string path, Predicate? filter)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false,
            new ParquetReadOptions { Filter = filter, DecimalOutput = DecimalOutputKind.Decimal128 });
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadAllAsync())
            batches.Add(batch);
        return batches;
    }

    private static int Count(List<RecordBatch> batches, Func<RecordBatch, int, bool> match) =>
        batches.Sum(b => Enumerable.Range(0, b.Length).Count(r => match(b, r)));

    private static decimal Dec(RecordBatch b, string column, int row) => b.Column(column) switch
    {
        Decimal128Array a => a.GetValue(row)!.Value,
        Decimal256Array a => a.GetValue(row)!.Value,
        var a => throw new NotSupportedException(a.GetType().Name),
    };

    private static uint U32(RecordBatch b, int row) => ((UInt32Array)b.Column("u32")).GetValue(row)!.Value;

    private static ulong U64(RecordBatch b, int row) => ((UInt64Array)b.Column("u64")).GetValue(row)!.Value;

    private static byte[] LittleEndian32(uint value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] LittleEndian64(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return bytes;
    }

    /// <summary>Two's complement, big-endian, sign-extended to <paramref name="width"/> bytes.</summary>
    private static byte[] BigEndian(BigInteger value, int width)
    {
        byte[] little = value.ToByteArray();
        var result = new byte[width];
        byte fill = value.Sign < 0 ? (byte)0xFF : (byte)0x00;
        for (int i = 0; i < width; i++)
            result[width - 1 - i] = i < little.Length ? little[i] : fill;
        return result;
    }

    /// <summary>A copy of <paramref name="source"/> with one init-only property replaced.</summary>
    private static T With<T>(T source, string property, object? value)
        where T : class
    {
        var copy = (T)typeof(object).GetMethod("MemberwiseClone", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(source, null)!;
        typeof(T).GetProperty(property)!.SetValue(copy, value);
        return copy;
    }
}
