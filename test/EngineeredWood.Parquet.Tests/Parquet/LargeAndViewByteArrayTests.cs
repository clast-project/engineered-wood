// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Utf8 = System.Text.Encoding;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #481: the writers read a BYTE_ARRAY column's values as 32-bit offsets into one data buffer, which is the layout
/// of string and binary only. A list of large_string or large_binary (64-bit offsets) was written with every value
/// empty, and a string_view or binary_view column (no offsets at all) could not be written.
/// </summary>
public class LargeAndViewByteArrayTests : IDisposable
{
    private readonly string _tempDir;

    public LargeAndViewByteArrayTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-bytearray-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    public enum Writer { File, Buffered }

    private const string Long = "a longer value, past the 12-byte view inline limit";

    private static readonly string?[] Values = ["a", null, "", Long, "b"];

    public static TheoryData<string, Writer> Layouts()
    {
        var data = new TheoryData<string, Writer>();
        foreach (var layout in new[] { "large_string", "large_binary", "string_view", "binary_view" })
        {
            data.Add(layout, Writer.File);
            data.Add(layout, Writer.Buffered);
        }
        return data;
    }

    public static TheoryData<string> NestedLayouts() =>
        new() { "large_string", "large_binary", "string_view", "binary_view" };

    private static IArrowArray Build(string layout, string?[] values)
    {
        switch (layout)
        {
            case "large_string":
            {
                var b = new LargeStringArray.Builder();
                foreach (var v in values)
                {
                    if (v is null) b.AppendNull(); else b.Append(v);
                }
                return b.Build();
            }
            case "large_binary":
            {
                var b = new LargeBinaryArray.Builder();
                foreach (var v in values)
                {
                    if (v is null) b.AppendNull(); else b.Append(Utf8.UTF8.GetBytes(v).AsSpan());
                }
                return b.Build();
            }
            case "string_view":
            {
                var b = new StringViewArray.Builder();
                foreach (var v in values)
                {
                    if (v is null) b.AppendNull(); else b.Append(v);
                }
                return b.Build();
            }
            case "binary_view":
            {
                var b = new BinaryViewArray.Builder();
                foreach (var v in values)
                {
                    if (v is null) b.AppendNull(); else b.Append(Utf8.UTF8.GetBytes(v).AsSpan());
                }
                return b.Build();
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(layout));
        }
    }

    // Any byte-array layout the reader hands back, as text.
    private static string? Text(IArrowArray array, int i)
    {
        if (array is ExtensionArray extension)
            return Text(extension.Storage, i);
        if (array.IsNull(i))
            return null;
        return array switch
        {
            StringArray a => a.GetString(i),
            LargeStringArray a => a.GetString(i),
            StringViewArray a => a.GetString(i),
            BinaryArray a => Utf8.UTF8.GetString(a.GetBytes(i).ToArray()),
            LargeBinaryArray a => Utf8.UTF8.GetString(a.GetBytes(i).ToArray()),
            BinaryViewArray a => Utf8.UTF8.GetString(a.GetBytes(i).ToArray()),
            _ => throw new NotSupportedException(array.GetType().Name),
        };
    }

    private static string?[] Texts(IArrowArray array) =>
        Enumerable.Range(0, array.Length).Select(i => Text(array, i)).ToArray();

    private async Task<IArrowArray> RoundTripAsync(IArrowArray column, Writer kind = Writer.File)
    {
        var schema = new ArrowSchema.Builder().Field(new Field("c", column.Data.DataType, true)).Build();
        var batch = new RecordBatch(schema, [column], column.Length);
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        {
            if (kind == Writer.File)
            {
                await using var w = new ParquetFileWriter(file, ownsFile: false);
                await w.WriteRowGroupAsync(batch);
                await w.CloseAsync();
            }
            else
            {
                await using var w = new BufferedParquetWriter(file, ownsFile: false);
                await w.AppendAsync(batch);
                await w.CloseAsync();
            }
        }

        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        return (await reader.ReadRowGroupAsync(0)).Column(0);
    }

    private static ArrowBuffer Offsets(params int[] offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets)
            b.Append(o);
        return b.Build();
    }

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid)
            b.Append(v);
        return b.Build();
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task AFlatColumn_RoundTrips(string layout, Writer kind)
    {
        var read = await RoundTripAsync(Build(layout, Values), kind);

        Assert.Equal(Values, Texts(read));
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task ASlicedFlatColumn_RoundTrips(string layout, Writer kind)
    {
        var read = await RoundTripAsync(ArrowArrayFactory.Slice(Build(layout, Values), 1, 3), kind);

        Assert.Equal(Values.Skip(1).Take(3).ToArray(), Texts(read));
    }

    [Theory]
    [MemberData(nameof(NestedLayouts))]
    public async Task AList_RoundTrips(string layout)
    {
        // Row 0 = [a, null], row 1 = null, row 2 = [], row 3 = ["", long, b].
        var values = Build(layout, Values);
        var type = new ListType(new Field("element", values.Data.DataType, true));
        var list = new ListArray(type, 4, Offsets(0, 2, 2, 2, 5), values, Validity(true, false, true, true), 1);

        var read = (ListArray)await RoundTripAsync(list);

        Assert.True(read.IsNull(1));
        Assert.Equal([0, 2, 2, 2, 5], read.ValueOffsets.ToArray());
        Assert.Equal(Values, Texts(read.Values));
    }

    [Theory]
    [MemberData(nameof(NestedLayouts))]
    public async Task AStructField_RoundTrips(string layout)
    {
        var values = Build(layout, Values);
        var type = new StructType([new Field("x", values.Data.DataType, true)]);
        var column = new StructArray(type, values.Length, [values], ArrowBuffer.Empty, nullCount: 0);

        var read = (StructArray)await RoundTripAsync(column);

        Assert.Equal(Values, Texts(read.Fields[0]));
    }

    [Theory]
    [MemberData(nameof(NestedLayouts))]
    public async Task AMapKeyAndValue_RoundTrip(string layout)
    {
        string?[] keyTexts = ["k1", "k2", Long];
        string?[] valueTexts = ["v1", null, "v3"];
        var keys = Build(layout, keyTexts);
        var values = Build(layout, valueTexts);
        var mapType = new MapType(
            new Field("key", keys.Data.DataType, false), new Field("value", values.Data.DataType, true));
        var entries = new StructArray(new StructType([mapType.KeyField, mapType.ValueField]), 3, [keys, values],
            ArrowBuffer.Empty, nullCount: 0);
        var map = new MapArray(mapType, 2, Offsets(0, 2, 3), entries, ArrowBuffer.Empty, nullCount: 0);

        var read = (MapArray)await RoundTripAsync(map);

        Assert.Equal(keyTexts, Texts(read.Keys));
        Assert.Equal(valueTexts, Texts(read.Values));
    }

    [Theory]
    [MemberData(nameof(NestedLayouts))]
    public async Task AListOverASlicedValuesArray_RoundTrips(string layout)
    {
        // The values array starts at element 1 of its buffers: row 0 = [null, ""], row 1 = [long].
        var values = ArrowArrayFactory.Slice(Build(layout, Values), 1, 3);
        var type = new ListType(new Field("element", values.Data.DataType, true));
        var list = new ListArray(type, 2, Offsets(0, 2, 3), values, ArrowBuffer.Empty, nullCount: 0);

        var read = (ListArray)await RoundTripAsync(list);

        Assert.Equal(Values.Skip(1).Take(3).ToArray(), Texts(read.Values));
    }

    [Theory]
    [InlineData("large_string")]
    [InlineData("string_view")]
    public async Task ARunEndEncodedColumn_RoundTrips(string layout)
    {
        // Runs: "a" x2, null x1, long x2.
        var runValues = Build(layout, ["a", null, Long]);
        var ree = new RunEndEncodedArray(new Int32Array.Builder().Append(2).Append(3).Append(5).Build(), runValues);

        var read = await RoundTripAsync(ree);

        var plain = read is RunEndEncodedArray r ? EngineeredWood.Arrow.RunEndEncoding.Expand(r) : read;
        Assert.Equal(["a", "a", null, Long, Long], Texts(plain));
    }

    // Review of #482: an extension type is written as its storage, so one over a large or view layout reached the
    // encoders unconverted.
    private sealed class TagType(IArrowType storage) : ExtensionType(storage)
    {
        public override string Name => "ew.test.tag";

        public override string ExtensionMetadata => "";

        public override ExtensionArray CreateArray(IArrowArray storageArray) => new TagArray(this, storageArray);
    }

    private sealed class TagArray(ExtensionType type, IArrowArray storage) : ExtensionArray(type, storage);

    [Theory]
    [MemberData(nameof(NestedLayouts))]
    public async Task AnExtensionColumn_RoundTrips(string layout)
    {
        var storage = Build(layout, Values);
        var column = new TagType(storage.Data.DataType).CreateArray(storage);

        var read = await RoundTripAsync(column);

        Assert.Equal(Values, Texts(read));
    }

    [Theory]
    [MemberData(nameof(NestedLayouts))]
    public async Task AListOfAnExtension_RoundTrips(string layout)
    {
        var storage = Build(layout, Values);
        var values = new TagType(storage.Data.DataType).CreateArray(storage);
        var type = new ListType(new Field("element", values.Data.DataType, true));
        var list = new ListArray(type, 2, Offsets(0, 2, 5), values, ArrowBuffer.Empty, nullCount: 0);

        var read = (ListArray)await RoundTripAsync(list);

        Assert.Equal(Values, Texts(read.Values));
    }
}
