// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #470: a list's values array may hold elements that no row's offsets reference (offsets need not start at 0 or
/// end at the values' length). Arrays from another library's filter or take, or from FFI, look like this. The
/// writer threw IndexOutOfRangeException on them; it must write only the referenced elements.
/// </summary>
public class UnreferencedListValuesTests : IDisposable
{
    private readonly string _tempDir;

    public UnreferencedListValuesTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-unref-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    private static ArrowBuffer Offsets(params int[] offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets)
        {
            b.Append(o);
        }
        return b.Build();
    }

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid)
        {
            b.Append(v);
        }
        return b.Build();
    }

    private static Int64Array Longs(params long[] values) => new Int64Array.Builder().AppendRange(values).Build();

    private static readonly ListType LongList = new(new Field("element", Int64Type.Default, true));

    private async Task<List<long[]?>> RoundTripAsync(IArrowArray column)
    {
        var schema = new ArrowSchema.Builder().Field(new Field("l", column.Data.DataType, true)).Build();
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [column], column.Length));
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var read = (ListArray)(await reader.ReadRowGroupAsync(0)).Column(0);
        return Enumerable.Range(0, read.Length)
            .Select(i => read.IsNull(i) ? null : ((Int64Array)read.GetSlicedValues(i)).Values.ToArray())
            .ToList();
    }

    // The issue's probes: one row, values [1, 2, 3].
    [Theory]
    [InlineData(new[] { 0, 1 }, new long[] { 1 })] // trailing elements unreferenced
    [InlineData(new[] { 2, 3 }, new long[] { 3 })] // leading elements unreferenced
    [InlineData(new[] { 1, 2 }, new long[] { 2 })] // both
    [InlineData(new[] { 0, 3 }, new long[] { 1, 2, 3 })] // control: all referenced
    public async Task AListWithUnreferencedValues_WritesOnlyTheReferencedOnes(int[] offsets, long[] expected)
    {
        var list = new ListArray(LongList, 1, Offsets(offsets), Longs(1, 2, 3), ArrowBuffer.Empty, nullCount: 0);

        var rows = await RoundTripAsync(list);

        Assert.Equal(expected, Assert.Single(rows));
    }

    [Fact]
    public async Task ValuesUnderANullRow_AndTrailingOnes_AreNotWritten()
    {
        // Row 0 = [10]; row 1 is null, and its offsets span 11 and 12, which must not be written; row 2 = [];
        // row 3 = [14]; the trailing 15 is referenced by nothing. (Offsets are one monotonic run, so the values a
        // row cannot reach are the leading ones, the trailing ones, and those under a null row.)
        var list = new ListArray(LongList, 4, Offsets(0, 1, 3, 3, 4), Longs(10, 11, 12, 14, 15),
            Validity(true, false, true, true), nullCount: 1);

        var rows = await RoundTripAsync(list);

        Assert.Equal(4, rows.Count);
        Assert.Equal([10L], rows[0]);
        Assert.Null(rows[1]);
        Assert.Empty(rows[2]!);
        Assert.Equal([14L], rows[3]);
    }

    // The same walk serves a list's element that is itself nested.
    [Fact]
    public async Task AListOfStructsWithUnreferencedValues_WritesOnlyTheReferencedOnes()
    {
        var elementType = new StructType([new Field("x", Int64Type.Default, true)]);
        var listType = new ListType(new Field("element", elementType, true));
        var elements = new StructArray(elementType, 4, [Longs(1, 2, 3, 4)], ArrowBuffer.Empty, nullCount: 0);
        var list = new ListArray(listType, 2, Offsets(1, 2, 3), elements, ArrowBuffer.Empty, nullCount: 0);

        var read = await ReadBackAsync(list);

        var readList = (ListArray)read;
        var x = (Int64Array)((StructArray)readList.Values).Fields[0];
        Assert.Equal([2L, 3L], Enumerable.Range(0, 2).Select(i => x.GetValue(readList.ValueOffsets[i])!.Value));
        Assert.Equal(2, readList.Values.Length);
    }

    [Fact]
    public async Task AListOfListsWithUnreferencedValues_WritesOnlyTheReferencedOnes()
    {
        // Inner lists [1, 2] and [0, 3], with a trailing 9 that neither references; the one outer row holds inner
        // list 1 only, so inner list 0 is unreferenced too.
        var inner = new ListArray(LongList, 2, Offsets(0, 2, 4), Longs(1, 2, 0, 3, 9), ArrowBuffer.Empty, nullCount: 0);
        var outerType = new ListType(new Field("element", LongList, true));
        var outer = new ListArray(outerType, 1, Offsets(1, 2), inner, ArrowBuffer.Empty, nullCount: 0);

        var read = (ListArray)await ReadBackAsync(outer);

        var innerRead = (ListArray)read.Values;
        Assert.Equal(1, innerRead.Length);
        Assert.Equal([0L, 3L], ((Int64Array)innerRead.GetSlicedValues(0)).Values.ToArray());
    }

    [Fact]
    public async Task AMapWithUnreferencedEntries_WritesOnlyTheReferencedOnes()
    {
        var mapType = new MapType(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true));
        var entries = new StructArray(new StructType([mapType.KeyField, mapType.ValueField]), 4,
            [new StringArray.Builder().Append("a").Append("b").Append("c").Append("d").Build(), Longs(1, 2, 3, 4)],
            ArrowBuffer.Empty, nullCount: 0);
        // Row 0 = {b: 2}, row 1 = {c: 3}; the leading a and the trailing d are referenced by nothing.
        var map = new MapArray(mapType, 2, Offsets(1, 2, 3), entries, ArrowBuffer.Empty, nullCount: 0);

        var read = (MapArray)await ReadBackAsync(map);

        Assert.Equal(["b", "c"], Enumerable.Range(0, read.Keys.Length).Select(i => ((StringArray)read.Keys).GetString(i)));
        Assert.Equal([2L, 3L], Enumerable.Range(0, 2).Select(i => ((Int64Array)read.Values).GetValue(i)!.Value));
    }

    // A map whose entries struct is itself sliced: keys and values are not sliced with it.
    [Fact]
    public async Task AMapOverSlicedEntries_ReadsTheEntriesRows()
    {
        var mapType = new MapType(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true));
        var entryType = new StructType([mapType.KeyField, mapType.ValueField]);
        var allEntries = new StructArray(entryType, 3,
            [new StringArray.Builder().Append("a").Append("b").Append("c").Build(), Longs(1, 2, 3)],
            ArrowBuffer.Empty, nullCount: 0);
        var entries = (StructArray)allEntries.Slice(1, 2);
        var map = new MapArray(mapType, 1, Offsets(0, 2), entries, ArrowBuffer.Empty, nullCount: 0);

        var read = (MapArray)await ReadBackAsync(map);

        Assert.Equal(["b", "c"], Enumerable.Range(0, 2).Select(i => ((StringArray)read.Keys).GetString(i)));
        Assert.Equal([2L, 3L], Enumerable.Range(0, 2).Select(i => ((Int64Array)read.Values).GetValue(i)!.Value));
    }

    private async Task<IArrowArray> ReadBackAsync(IArrowArray column)
    {
        var schema = new ArrowSchema.Builder().Field(new Field("c", column.Data.DataType, true)).Build();
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [column], column.Length));
            await writer.CloseAsync();
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        return (await reader.ReadRowGroupAsync(0)).Column(0);
    }
}
