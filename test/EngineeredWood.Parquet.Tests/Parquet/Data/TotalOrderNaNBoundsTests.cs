// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Under IEEE 754 total order, a chunk or page whose every value is NaN records the smallest and
/// largest NaN in that order as its min and max (parquet.thrift, ColumnOrder and ColumnIndex). The
/// writer used to record the first NaN in row order as both (#553). Total order sorts NaNs by sign and
/// payload: a negative NaN below everything, more negative the larger its payload; a positive NaN
/// above everything, higher the larger its payload.
/// </summary>
public sealed class TotalOrderNaNBoundsTests : IDisposable
{
    private const int RunLength = 600;

    // Row order: the first NaN is +NaN(2), which the old writer recorded as both bounds.
    private static readonly ulong[] DoubleNaNs =
        [0x7FF8_0000_0000_0002, 0xFFF8_0000_0000_0001, 0x7FF8_0000_0000_0009, 0xFFF8_0000_0000_0005];
    private const ulong DoubleMin = 0xFFF8_0000_0000_0005;   // the negative NaN with the larger payload
    private const ulong DoubleMax = 0x7FF8_0000_0000_0009;   // the positive NaN with the larger payload

    private static readonly uint[] FloatNaNs = [0x7FC0_0002, 0xFFC0_0001, 0x7FC0_0009, 0xFFC0_0005];
    private const uint FloatMin = 0xFFC0_0005;
    private const uint FloatMax = 0x7FC0_0009;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ew-nanbounds-" + Guid.NewGuid().ToString("N")[..8]);

    public TotalOrderNaNBoundsTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    public static TheoryData<bool, string, bool, bool> Cases()
    {
        var data = new TheoryData<bool, string, bool, bool>();
        foreach (bool isFloat in new[] { true, false })
        foreach (string writer in new[] { "file", "buffered" })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool pageIndex in new[] { true, false })
            data.Add(isFloat, writer, dictionary, pageIndex);
        return data;
    }

    /// <summary>The page-index cases: the same, less the dimension without a page index.</summary>
    public static TheoryData<bool, string, bool> PageIndexCases()
    {
        var data = new TheoryData<bool, string, bool>();
        foreach (bool isFloat in new[] { true, false })
        foreach (string writer in new[] { "file", "buffered" })
        foreach (bool dictionary in new[] { true, false })
            data.Add(isFloat, writer, dictionary);
        return data;
    }

    /// <summary>
    /// Every NaN in its own run of rows across several pages: the chunk's bounds are the smallest and
    /// largest NaN overall, whether found by a full scan, from the dictionary, or folded from the pages.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task AllNaNChunk_BoundsAreTheSmallestAndLargestNaN(bool isFloat, string writer, bool dictionary, bool pageIndex)
    {
        string path = await WriteAsync(isFloat, writer, dictionary, pageIndex);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var stats = metadata.RowGroups[0].Columns[0].MetaData!.Statistics!;

        Assert.Equal(RunLength * 4L, stats.NanCount);
        Assert.Equal(Min(isFloat), Bits(stats.MinValue!));
        Assert.Equal(Max(isFloat), Bits(stats.MaxValue!));
    }

    /// <summary>
    /// Each page's ColumnIndex bounds are that page's smallest and largest NaN. The first page holds
    /// only the first run, so both are +NaN(2); a page spanning two runs holds both of theirs.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCases))]
    public async Task AllNaNPages_ColumnIndexBoundsAreEachPagesSmallestAndLargestNaN(
        bool isFloat, string writer, bool dictionary)
    {
        string path = await WriteAsync(isFloat, writer, dictionary, pageIndex: true);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var index = (await reader.ReadPageIndexAsync(0))[0];
        var columnIndex = index.ColumnIndex!;
        var pages = index.OffsetIndex!.PageLocations;
        Assert.True(pages.Count > 1, "expected several pages");

        ulong[] nans = isFloat ? FloatNaNs.Select(v => (ulong)v).ToArray() : DoubleNaNs;
        for (int p = 0; p < pages.Count; p++)
        {
            long first = pages[p].FirstRowIndex;
            long last = (p + 1 < pages.Count ? pages[p + 1].FirstRowIndex : RunLength * 4L) - 1;
            var inPage = Enumerable.Range((int)(first / RunLength), (int)(last / RunLength - first / RunLength) + 1)
                .Select(run => nans[run]).ToArray();

            Assert.Equal(inPage.OrderBy(v => Key(v, isFloat)).First(), Bits(columnIndex.MinValues[p]));
            Assert.Equal(inPage.OrderBy(v => Key(v, isFloat)).Last(), Bits(columnIndex.MaxValues[p]));
        }
    }

    /// <summary>Under TYPE_ORDER an all-NaN chunk has no min/max at all.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TypeOrder_AllNaNChunk_HasNoBounds(bool dictionary)
    {
        string path = await WriteAsync(isFloat: false, "file", dictionary, pageIndex: true, FloatingPointColumnOrder.TypeDefined);

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var stats = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!.Statistics!;

        Assert.Null(stats.MinValue);
        Assert.Null(stats.MaxValue);
        Assert.Equal(RunLength * 4L, stats.NanCount);
    }

    private async Task<string> WriteAsync(bool isFloat, string writer, bool dictionary, bool pageIndex,
        FloatingPointColumnOrder order = FloatingPointColumnOrder.Ieee754TotalOrder)
    {
        IArrowArray column;
        if (isFloat)
        {
            var builder = new FloatArray.Builder();
            foreach (uint bits in FloatNaNs)
                for (int i = 0; i < RunLength; i++)
                    builder.Append(BitConverter.ToSingle(BitConverter.GetBytes(bits), 0));
            column = builder.Build();
        }
        else
        {
            var builder = new DoubleArray.Builder();
            foreach (ulong bits in DoubleNaNs)
                for (int i = 0; i < RunLength; i++)
                    builder.Append(BitConverter.Int64BitsToDouble((long)bits));
            column = builder.Build();
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("x", column.Data.DataType, nullable: false)).Build(),
            [column], column.Length);
        var options = new ParquetWriteOptions
        {
            FloatingPointOrder = order,
            DictionaryEnabled = dictionary,
            WritePageIndex = pageIndex,
            DataPageSize = 2048,   // several pages, some spanning two runs
        };

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

    private static ulong Min(bool isFloat) => isFloat ? FloatMin : DoubleMin;

    private static ulong Max(bool isFloat) => isFloat ? FloatMax : DoubleMax;

    private static ulong Bits(byte[] bound) => bound.Length == 4
        ? BinaryPrimitives.ReadUInt32LittleEndian(bound)
        : BinaryPrimitives.ReadUInt64LittleEndian(bound);

    /// <summary>IEEE 754 total-order key, written independently of the code under test.</summary>
    private static long Key(ulong bits, bool isFloat)
    {
        if (isFloat)
        {
            int b = unchecked((int)(uint)bits);
            return b < 0 ? -(long)(b & 0x7FFF_FFFF) - 1 : b;
        }
        long b64 = unchecked((long)bits);
        return b64 < 0 ? -(b64 & 0x7FFF_FFFF_FFFF_FFFF) - 1 : b64;
    }
}
