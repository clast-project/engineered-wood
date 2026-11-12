// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="ZeroOffsetView"/> re-bases a slice without copying its values (#667). The oracle is the gather it
/// replaced: a slice written through its view must produce the same file, byte for byte, as the same slice
/// gathered by <see cref="ArrowCompute.Take(IArrowArray, ReadOnlySpan{int})"/>.
/// </summary>
public class ZeroOffsetViewTests : IDisposable
{
    private const int Rows = 61;
    private readonly string _tempDir;

    public ZeroOffsetViewTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-zero-offset-" + Guid.NewGuid().ToString("N").Substring(0, 8));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Dictionary<string, Func<IArrowArray>> Columns = new()
    {
        ["int32"] = () => Ints(seed: 1),
        ["int8"] = Int8s,
        ["bool"] = Bools,
        ["decimal128"] = Decimals,
        ["timestamp"] = Timestamps,
        ["fixed_size_binary"] = FixedBinaries,
        ["string"] = () => Strings(seed: 2),
        ["large_string"] = LargeStrings,
        ["binary"] = Binaries,
        ["struct"] = Struct,
        ["list"] = List,
        ["map"] = Map,
        ["fixed_size_list"] = FixedSizeList,
        ["guid"] = Guids,
    };

    public static TheoryData<string, int, int> Cases()
    {
        var data = new TheoryData<string, int, int>();
        foreach (var name in Columns.Keys)
        {
            // Byte-aligned and unaligned starts, a single row, and a slice reaching the last row.
            foreach (var (offset, length) in new[] { (8, 30), (3, 41), (13, 1), (1, Rows - 1) })
                data.Add(name, offset, length);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ASlice_WritesTheSameFileAsItsGather(string name, int offset, int length)
    {
        var array = Columns[name]();
        var view = ZeroOffsetView.TrySlice(array, offset, length);
        Assert.NotNull(view);
        AssertZeroOffsetThroughout(view!.Data, length);

        var gathered = ArrowCompute.Take(array, Enumerable.Range(offset, length).ToArray());
        Assert.Equal(await WriteAsync(name, gathered), await WriteAsync(name, view));
        // The view does not keep its source alive; unrooted, Arrow's finalizer frees the source's buffers
        // under the write (seen on net472). The writer roots its caller's batch the same way.
        GC.KeepAlive(array);
    }

    /// <summary>
    /// The writer's own two entry points for a slice: a caller's sliced batch, and a batch it splits itself.
    /// Each must write what the gathered rows write.
    /// </summary>
    [Theory]
    [InlineData(7)]
    [InlineData(16)]
    public async Task TheWriter_WritesSlicedAndSplitBatchesAsGathered(int rowGroupRows)
    {
        var names = Columns.Keys.ToArray();
        var whole = Batch(names, names.Select(n => Columns[n]()).ToArray());
        var options = ParquetWriteOptions.Default with { RowGroupMaxRows = rowGroupRows };

        var sliced = whole.Slice(5, 50);
        var gathered = Batch(names, names.Select(n =>
            ArrowCompute.Take(whole.Column(n), Enumerable.Range(5, 50).ToArray())).ToArray());

        Assert.Equal(await WriteBatchAsync("gathered", gathered, options), await WriteBatchAsync("sliced", sliced, options));
    }

    /// <summary>
    /// A caller need not hold the batch it writes. The writer hands its encoders views onto that batch's
    /// buffers, and a view does not keep them alive, so the writer must root the batch itself: unrooted,
    /// Arrow 23's finalizer frees the buffers mid-write. Collections are forced throughout to make that likely.
    /// </summary>
    [Theory]
    [InlineData(1000)] // compacted in place
    [InlineData(16)]   // split into row groups
    public async Task AnUnheldBatch_StaysAliveForTheWrite(int rowGroupRows)
    {
        var names = Columns.Keys.ToArray();
        var options = ParquetWriteOptions.Default with { RowGroupMaxRows = rowGroupRows };
        var expected = await WriteBatchAsync("gathered", GatheredBatch(names), options);

        using var stop = new CancellationTokenSource();
        var collector = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        });
        try
        {
            for (int i = 0; i < 5; i++)
            {
                // Built inside the call, so nothing here (not even a parameter on a state machine) holds it.
                string path = Path.Combine(_tempDir, "unheld-" + Guid.NewGuid().ToString("N") + ".parquet");
                await using (var file = new LocalSequentialFile(path))
                await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
                {
                    await writer.WriteRowGroupAsync(SlicedBatch(names));
                    await writer.CloseAsync();
                }
                Assert.Equal(expected, File.ReadAllBytes(path));
            }
        }
        finally
        {
            stop.Cancel();
            await collector;
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static RecordBatch SlicedBatch(string[] names) =>
        Batch(names, names.Select(n => Columns[n]()).ToArray()).Slice(5, 50);

    private static RecordBatch GatheredBatch(string[] names) =>
        Batch(names, names.Select(n => ArrowCompute.Take(Columns[n](), Enumerable.Range(5, 50).ToArray())).ToArray());

    [Fact]
    public void FixedWidthValues_AreViewedNotCopied()
    {
        var array = Ints(seed: 3);
        var view = ZeroOffsetView.TrySlice(array, 9, 20)!;

        var source = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span);
        var viewed = MemoryMarshal.Cast<byte, int>(view.Data.Buffers[1].Span);
        Assert.True(Same(source.Slice(9), viewed));
        GC.KeepAlive(array);
        GC.KeepAlive(view);
    }

    [Fact]
    public void StringValues_AreViewedNotCopied()
    {
        var array = Strings(seed: 4);
        var view = (StringArray)ZeroOffsetView.TrySlice(array, 11, 20)!;

        int first = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span)[11];
        Assert.True(Same(array.Data.Buffers[2].Span.Slice(first), view.Data.Buffers[2].Span));
        Assert.Equal(Enumerable.Range(11, 20).Select(i => array.GetString(i)), Enumerable.Range(0, 20).Select(i => view.GetString(i)));
    }

    [Fact]
    public void ANullFreeSlice_KeepsNoBitmap()
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < 20; i++)
        {
            if (i < 4)
                builder.AppendNull();
            else
                builder.Append(i);
        }

        var view = ZeroOffsetView.TrySlice(builder.Build(), 5, 10)!;

        Assert.Equal(0, view.NullCount);
        Assert.Equal(0, view.Data.Buffers[0].Length);
    }

    [Fact]
    public void AStringView_IsDeclined()
    {
        var builder = new StringViewArray.Builder();
        builder.Append("a").Append("b").Append("c");
        Assert.Null(ZeroOffsetView.TrySlice(builder.Build(), 1, 2));
    }

    private static bool Same<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b) =>
        System.Runtime.CompilerServices.Unsafe.AreSame(
            ref MemoryMarshal.GetReference(a), ref MemoryMarshal.GetReference(b));

    private static void AssertZeroOffsetThroughout(ArrayData data, int length)
    {
        Assert.Equal(0, data.Offset);
        Assert.Equal(length, data.Length);
        var layout = data.DataType;
        while (layout is ExtensionType extension)
            layout = extension.StorageType;

        switch (layout)
        {
            case StructType:
                foreach (var child in data.Children)
                    AssertZeroOffsetThroughout(child, length);
                break;
            case ListType or MapType:
                var offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span);
                Assert.Equal(0, offsets[0]);
                AssertZeroOffsetThroughout(data.Children[0], offsets[length]);
                break;
            case FixedSizeListType fixedSize:
                AssertZeroOffsetThroughout(data.Children[0], length * fixedSize.ListSize);
                break;
        }
    }

    private async Task<byte[]> WriteAsync(string name, IArrowArray column) =>
        await WriteBatchAsync(name, Batch([name], [column]), ParquetWriteOptions.Default);

    private async Task<byte[]> WriteBatchAsync(string label, RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, label + "-" + Guid.NewGuid().ToString("N") + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return File.ReadAllBytes(path);
    }

    private static RecordBatch Batch(string[] names, IArrowArray[] columns) =>
        new(new ArrowSchema(names.Select((n, i) => new Field(n, columns[i].Data.DataType, true)).ToList(), null),
            columns, columns[0].Length);

    // ───── Columns: Rows rows each, with nulls spread so that slices start on and off byte boundaries ─────

    private static bool IsNull(int row) => row % 5 == 2 || row % 7 == 0;

    private static Int32Array Ints(int seed)
    {
        var b = new Int32Array.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(r * 31 + seed);
        }
        return b.Build();
    }

    private static IArrowArray Int8s()
    {
        var b = new Int8Array.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append((sbyte)(r - 30));
        }
        return b.Build();
    }

    private static IArrowArray Bools()
    {
        var b = new BooleanArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(r % 3 == 0);
        }
        return b.Build();
    }

    private static IArrowArray Decimals()
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(20, 2));
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(r * 1.25m - 7);
        }
        return b.Build();
    }

    private static IArrowArray Timestamps()
    {
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var t0 = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(t0.AddMinutes(r * 17));
        }
        return b.Build();
    }

    private static IArrowArray FixedBinaries()
    {
        var values = new byte[Rows * 5];
        var validity = new ArrowBuffer.BitmapBuilder();
        int nulls = 0;
        for (int r = 0; r < Rows; r++)
        {
            validity.Append(!IsNull(r));
            if (IsNull(r))
                nulls++;
            else
                new byte[] { (byte)r, (byte)(r * 3), 1, 2, (byte)(255 - r) }.CopyTo(values, r * 5);
        }
        return new FixedSizeBinaryArray(new ArrayData(
            new FixedSizeBinaryType(5), Rows, nulls, 0, new[] { validity.Build(), new ArrowBuffer(values) }));
    }

    private static StringArray Strings(int seed)
    {
        var b = new StringArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(new string((char)('a' + (r + seed) % 26), r % 9));
        }
        return b.Build();
    }

    private static IArrowArray LargeStrings()
    {
        var b = new LargeStringArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append("v" + (r * 7 % 13));
        }
        return b.Build();
    }

    private static IArrowArray Binaries()
    {
        var b = new BinaryArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(Enumerable.Range(0, r % 6).Select(i => (byte)(r + i)).ToArray());
        }
        return b.Build();
    }

    private static IArrowArray Struct()
    {
        var validity = new ArrowBuffer.BitmapBuilder();
        int nulls = 0;
        for (int r = 0; r < Rows; r++)
        {
            bool valid = r % 6 != 4;
            validity.Append(valid);
            if (!valid)
                nulls++;
        }
        var type = new StructType(
        [
            new Field("s", StringType.Default, true),
            new Field("n", Int32Type.Default, true),
        ]);
        return new StructArray(type, Rows, new IArrowArray[] { Strings(seed: 5), Ints(seed: 6) }, validity.Build(), nulls);
    }

    private static IArrowArray List()
    {
        var b = new ListArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)b.ValueBuilder;
        for (int r = 0; r < Rows; r++)
        {
            if (r % 8 == 3)
            {
                b.AppendNull();
                continue;
            }
            b.Append();
            for (int i = 0; i < r % 4; i++)
            {
                if ((r + i) % 5 == 0)
                    values.AppendNull();
                else
                    values.Append(r * 10 + i);
            }
        }
        return b.Build();
    }

    private static IArrowArray Map()
    {
        var offsets = new List<int> { 0 };
        var keys = new StringArray.Builder();
        var values = new Int32Array.Builder();
        var validity = new ArrowBuffer.BitmapBuilder();
        int nulls = 0;
        for (int r = 0; r < Rows; r++)
        {
            bool valid = r % 9 != 5;
            validity.Append(valid);
            if (!valid)
            {
                nulls++;
            }
            else
            {
                for (int i = 0; i < r % 3; i++)
                {
                    keys.Append("k" + i);
                    values.Append(r + i);
                }
            }
            offsets.Add(keys.Length);
        }

        var type = new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));
        var entries = new StructArray(
            (StructType)type.Fields[0].DataType, keys.Length,
            new IArrowArray[] { keys.Build(), values.Build() }, ArrowBuffer.Empty, 0);
        var offsetBuffer = new ArrowBuffer(MemoryMarshal.AsBytes(offsets.ToArray().AsSpan()).ToArray());
        return new MapArray(type, Rows, offsetBuffer, entries, validity.Build(), nulls);
    }

    private static IArrowArray FixedSizeList()
    {
        var values = new Int32Array.Builder();
        var validity = new ArrowBuffer.BitmapBuilder();
        int nulls = 0;
        for (int r = 0; r < Rows; r++)
        {
            bool valid = r % 7 != 3;
            validity.Append(valid);
            if (!valid)
                nulls++;
            for (int i = 0; i < 3; i++)
            {
                if ((r + i) % 4 == 0)
                    values.AppendNull();
                else
                    values.Append(r * 3 + i);
            }
        }
        var type = new FixedSizeListType(new Field("element", Int32Type.Default, true), 3);
        return new FixedSizeListArray(type, Rows, values.Build(), validity.Build(), nulls);
    }

    private static IArrowArray Guids()
    {
        var b = new GuidArray.Builder();
        for (int r = 0; r < Rows; r++)
        {
            if (IsNull(r))
                b.AppendNull();
            else
                b.Append(new Guid(r, (short)r, (short)(r * 2), 1, 2, 3, 4, 5, 6, 7, (byte)r));
        }
        return b.Build();
    }
}
