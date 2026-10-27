// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NET8_0_OR_GREATER // HalfFloatArray does not exist on .NET Framework, so neither does a FLOAT16 column to write.
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// FLOAT16 statistics are ordered numerically, like FLOAT and DOUBLE (#547). They used to come from an
/// unsigned byte comparison of the little-endian halves, which orders by the LOW byte first and gets
/// negatives wrong: 1.0, 1.5, 1.0009765625 came out as [1.0, 1.0009765625], and a reader pruning on that
/// skipped a row group holding 1.5. FLOAT16 now also gets a <c>nan_count</c>, a ColumnIndex, and the
/// configured floating-point column order.
/// </summary>
/// <remarks>Values are raw half-precision bits: System.Half is not on every target.</remarks>
public sealed class Float16StatisticsTests : IDisposable
{
    private const ushort One = 0x3C00, OnePointFive = 0x3E00, JustAboveOne = 0x3C01, MinusThree = 0xC200,
        MinusHalf = 0xB800, PlusZero = 0x0000, MinusZero = 0x8000, Two = 0x4000;
    private const ushort QuietNaN = 0x7E00, NegativeNaN = 0xFE01, PayloadNaN = 0x7E05;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ew-f16stats-" + Guid.NewGuid().ToString("N")[..8]);

    public Float16StatisticsTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    public static TheoryData<string, bool, bool> Writers()
    {
        var data = new TheoryData<string, bool, bool>();
        foreach (string writer in new[] { "file", "buffered" })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool pageIndex in new[] { true, false })
            data.Add(writer, dictionary, pageIndex);
        return data;
    }

    [Theory]
    [MemberData(nameof(Writers))]
    public async Task Bounds_AreNumeric(string writer, bool dictionary, bool pageIndex)
    {
        string path = await WriteAsync([One, OnePointFive, JustAboveOne, MinusThree, MinusHalf], writer, dictionary, pageIndex);

        var stats = await ReadStatisticsAsync(path);
        Assert.Equal(MinusThree, Bits(stats.MinValue!));
        Assert.Equal(OnePointFive, Bits(stats.MaxValue!));
        Assert.Equal(0, stats.NanCount);
        // The deprecated min/max promise a signed comparison of the bytes, which is not this order.
        Assert.Null(stats.Min);
        Assert.Null(stats.Max);
    }

    /// <summary>
    /// Bounds that are wrong without being inverted: the byte order made them [1.0, 1.0009765625], and
    /// this reader skipped the row group for <c>h &gt; 1.2</c> although it holds 1.5.
    /// </summary>
    [Theory]
    [MemberData(nameof(Writers))]
    public async Task Pruning_DoesNotSkipARowGroupThatMatches(string writer, bool dictionary, bool pageIndex)
    {
        string path = await WriteAsync([One, OnePointFive, JustAboveOne], writer, dictionary, pageIndex);

        Assert.True(await CandidateAsync(path, Ex.GreaterThan("h", 1.2)));
        Assert.False(await CandidateAsync(path, Ex.GreaterThan("h", 1.6)));
    }

    [Theory]
    [MemberData(nameof(Writers))]
    public async Task NaN_IsCountedAndKeptOutOfTheBounds(string writer, bool dictionary, bool pageIndex)
    {
        string path = await WriteAsync([QuietNaN, One, NegativeNaN, Two], writer, dictionary, pageIndex);

        var stats = await ReadStatisticsAsync(path);
        Assert.Equal(2, stats.NanCount);
        Assert.Equal(One, Bits(stats.MinValue!));
        Assert.Equal(Two, Bits(stats.MaxValue!));
    }

    /// <summary>TYPE_ORDER's signed-zero rule applies to FLOAT16 as to FLOAT and DOUBLE (#546).</summary>
    [Theory]
    [MemberData(nameof(Writers))]
    public async Task TypeOrder_ZeroBoundsAreSigned(string writer, bool dictionary, bool pageIndex)
    {
        string path = await WriteAsync([PlusZero, PlusZero], writer, dictionary, pageIndex);

        var stats = await ReadStatisticsAsync(path);
        Assert.Equal(MinusZero, Bits(stats.MinValue!));
        Assert.Equal(PlusZero, Bits(stats.MaxValue!));
    }

    /// <summary>
    /// Under IEEE 754 total order an all-NaN chunk's bounds are its smallest and largest NaN (#553);
    /// under TYPE_ORDER it has none.
    /// </summary>
    [Theory]
    [MemberData(nameof(Writers))]
    public async Task AllNaN_BoundsFollowTheOrder(string writer, bool dictionary, bool pageIndex)
    {
        ushort[] values = [QuietNaN, NegativeNaN, PayloadNaN];

        var total = await ReadStatisticsAsync(await WriteAsync(values, writer, dictionary, pageIndex,
            FloatingPointColumnOrder.Ieee754TotalOrder));
        Assert.Equal(NegativeNaN, Bits(total.MinValue!));
        Assert.Equal(PayloadNaN, Bits(total.MaxValue!));
        Assert.Equal(3, total.NanCount);

        var typeOrder = await ReadStatisticsAsync(await WriteAsync(values, writer, dictionary, pageIndex));
        Assert.Null(typeOrder.MinValue);
        Assert.Null(typeOrder.MaxValue);
        Assert.Equal(3, typeOrder.NanCount);
    }

    /// <summary>
    /// FLOAT16 now gets a ColumnIndex, with per-page numeric bounds and NaN counts. Pages of ascending
    /// runs: each page's bounds are its own, so the ColumnIndex reports the order as ascending.
    /// </summary>
    [Theory]
    [InlineData("file", true)]
    [InlineData("file", false)]
    [InlineData("buffered", true)]
    [InlineData("buffered", false)]
    public async Task ColumnIndex_IsWritten(string writer, bool dictionary)
    {
        ushort[] runs = [MinusThree, MinusHalf, One, JustAboveOne, OnePointFive, Two];
        var values = runs.SelectMany(v => Enumerable.Repeat(v, 500)).Append(QuietNaN).ToArray();
        string path = await WriteAsync(values, writer, dictionary, pageIndex: true, dataPageSize: 256);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var index = (await reader.ReadPageIndexAsync(0))[0];
        var columnIndex = Assert.IsType<ColumnIndex>(index.ColumnIndex);
        Assert.True(columnIndex.MinValues.Count > 1, "expected several pages");

        Assert.Equal(MinusThree, Bits(columnIndex.MinValues[0]));
        Assert.Equal(Two, Bits(columnIndex.MaxValues[^1]));
        Assert.NotNull(columnIndex.NanCounts);
        Assert.Equal(1, columnIndex.NanCounts!.Sum());
        Assert.Equal(BoundaryOrder.Ascending, columnIndex.BoundaryOrder);
        for (int p = 1; p < columnIndex.MinValues.Count; p++)
            Assert.True(Key(Bits(columnIndex.MinValues[p - 1])) <= Key(Bits(columnIndex.MinValues[p])));
    }

    /// <summary>
    /// The configured floating-point column order applies to FLOAT16 too — parquet-format recommends
    /// IEEE 754 total order for it — and the reader honours it on a FLOAT16 column.
    /// </summary>
    [Theory]
    [InlineData(FloatingPointColumnOrder.TypeDefined, ColumnOrder.TypeDefined)]
    [InlineData(FloatingPointColumnOrder.Ieee754TotalOrder, ColumnOrder.Ieee754TotalOrder)]
    public async Task ColumnOrder_FollowsTheFloatingPointOption(FloatingPointColumnOrder option, ColumnOrder expected)
    {
        string path = await WriteAsync([One, Two], "file", dictionary: false, pageIndex: true, option);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        Assert.Equal([expected], (await reader.ReadMetadataAsync()).ColumnOrders);
        Assert.Equal(expected, (await reader.GetSchemaAsync()).Columns[0].ColumnOrder);
    }

    // ── Helpers ──

    private async Task<string> WriteAsync(ushort[] halves, string writer, bool dictionary, bool pageIndex,
        FloatingPointColumnOrder order = FloatingPointColumnOrder.TypeDefined, int? dataPageSize = null)
    {
        var bytes = new byte[halves.Length * 2];
        for (int i = 0; i < halves.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(i * 2), halves[i]);
        var column = new HalfFloatArray(new ArrayData(HalfFloatType.Default, halves.Length, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("h", HalfFloatType.Default, nullable: false)).Build(),
            [column], halves.Length);

        var options = new ParquetWriteOptions
        {
            DictionaryEnabled = dictionary,
            WritePageIndex = pageIndex,
            FloatingPointOrder = order,
        };
        if (dataPageSize is { } size)
            options = options with { DataPageSize = size };

        string path = Path.Combine(_dir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        if (writer == "file")
        {
            await using var w = new ParquetFileWriter(file, ownsFile: false, options);
            await w.WriteRowGroupAsync(batch);
            await w.CloseAsync();
        }
        else
        {
            await using var w = new BufferedParquetWriter(file, ownsFile: false, options);
            await w.AppendAsync(batch);
            await w.CloseAsync();
        }
        return path;
    }

    private static async Task<Statistics> ReadStatisticsAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!.Statistics!;
    }

    private static async Task<bool> CandidateAsync(string path, EngineeredWood.Expressions.Predicate filter)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return (await reader.GetCandidateRowGroupsAsync(filter))[0];
    }

    private static ushort Bits(byte[] bound)
    {
        Assert.Equal(2, bound.Length);
        return BinaryPrimitives.ReadUInt16LittleEndian(bound);
    }

    /// <summary>Total-order key, written independently of the code under test.</summary>
    private static int Key(ushort bits) => (bits & 0x8000) != 0 ? -(bits & 0x7FFF) - 1 : bits;
}
#endif
