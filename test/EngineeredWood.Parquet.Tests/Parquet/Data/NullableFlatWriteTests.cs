// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Parquet.Tests.Parquet.Data;

/// <summary>
/// A nullable flat column's definition levels come from its validity bitmap a byte at a time, and its
/// fixed-width values are gathered without a branch on the level. These round-trip what that has to get
/// right: random nulls at shares from none to all, row counts that end mid-byte, pages that start
/// mid-column, and an array that declares no nulls over a bitmap with clear bits, which <c>IsNull</c>
/// reads as all valid.
/// <para>The sliced cases guard the whole write path, not the writer's own offset handling: the file
/// writer rebases every sliced column, bitmap included, before an encoder sees it
/// (<c>ParquetFileWriter.CompactSlicedColumns</c>), so the array reaching <c>ColumnChunkWriter</c> always
/// has offset 0, and passing it 0 instead of <c>array.Offset</c> leaves these green. The expansion at a
/// non-zero bit offset is tested in <c>BitmapHelperTests</c>.</para>
/// </summary>
public class NullableFlatWriteTests : IDisposable
{
    private readonly string _tempDir;

    public NullableFlatWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nullflat-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    public static TheoryData<string, double, bool> Cases()
    {
        var data = new TheoryData<string, double, bool>();
        foreach (string kind in new[] { "int32", "int64", "float", "double", "string" })
        foreach (double share in new[] { 0.0, 1.0 / 7, 0.5, 1.0 })
        foreach (bool dictionary in new[] { false, true })
            data.Add(kind, share, dictionary);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task RandomNulls_RoundTrip(string kind, double nullShare, bool dictionary)
    {
        var array = Build(kind, 5_003, nullShare, seed: 7);
        var read = await RoundTripAsync(array, Options(dictionary));
        AssertSame(array, read);
    }

    [Theory]
    [InlineData("int64", 0)]
    [InlineData("int64", 13)]
    [InlineData("double", 13)]
    [InlineData("string", 5)]
    public async Task SlicedArray_RoundTrips(string kind, int start)
    {
        var whole = Build(kind, 3_000, 1.0 / 3, seed: 11);
        var slice = ArrowArrayFactory.Slice(whole, start, 2_001);
        var read = await RoundTripAsync(slice, Options(dictionary: false));
        AssertSame(slice, read);
    }

    [Fact]
    public async Task DeclaredNoNulls_OverClearBits_ReadsAllValid()
    {
        // Arrow's IsNull ignores the bitmap of an array that declares no nulls; the levels must too.
        const int rows = 21;
        var values = new long[rows];
        for (int i = 0; i < rows; i++)
            values[i] = i * 3;
        var bitmap = new byte[] { 0b1010_1010, 0x00, 0b0001_0101 };
        var data = new ArrayData(Int64Type.Default, rows, nullCount: 0, offset: 0,
            [new ArrowBuffer(bitmap), new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()).ToArray())]);
        var array = new Int64Array(data);
        Assert.False(array.IsNull(0));

        var read = (Int64Array)await RoundTripAsync(array, Options(dictionary: false));
        Assert.Equal(0, read.NullCount);
        for (int i = 0; i < rows; i++)
            Assert.Equal(values[i], read.GetValue(i));
    }

    private static ParquetWriteOptions Options(bool dictionary) => ParquetWriteOptions.Default with
    {
        DictionaryEnabled = dictionary,
        // Several pages per column, so the gathers start part way into the levels.
        DataPageSize = 4_096,
    };

    private async Task<IArrowArray> RoundTripAsync(IArrowArray array, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        var schema = new Apache.Arrow.Schema([new Field("c", array.Data.DataType, nullable: true)], null);
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], array.Length));
            await writer.CloseAsync();
        }

        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        var parts = new List<IArrowArray>();
        await foreach (var batch in reader.ReadAllAsync())
            parts.Add(batch.Column(0));
        return parts.Count == 1 ? parts[0] : ArrowArrayConcatenator.Concatenate(parts);
    }

    private static void AssertSame(IArrowArray expected, IArrowArray actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.Equal(expected.NullCount, actual.NullCount);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.True(expected.IsNull(i) == actual.IsNull(i), $"null at row {i}");
            if (expected.IsNull(i))
                continue;
            object? e = Value(expected, i), a = Value(actual, i);
            Assert.True(Equals(e, a), $"row {i}: expected {e}, read {a}");
        }
    }

    private static object? Value(IArrowArray array, int i) => array switch
    {
        Int32Array a => a.GetValue(i),
        Int64Array a => a.GetValue(i),
        FloatArray a => a.GetValue(i),
        DoubleArray a => a.GetValue(i),
        StringArray a => a.GetString(i),
        _ => throw new NotSupportedException(array.GetType().Name),
    };

    private static IArrowArray Build(string kind, int rows, double nullShare, int seed)
    {
        var random = new Random(seed);
        bool IsNull() => random.NextDouble() < nullShare;
        switch (kind)
        {
            case "int32":
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < rows; i++) { if (IsNull()) b.AppendNull(); else b.Append(random.Next(100)); }
                return b.Build();
            }
            case "int64":
            {
                var b = new Int64Array.Builder();
                for (int i = 0; i < rows; i++) { if (IsNull()) b.AppendNull(); else b.Append((long)random.Next(-1_000, 1_000) << 20); }
                return b.Build();
            }
            case "float":
            {
                var b = new FloatArray.Builder();
                for (int i = 0; i < rows; i++) { if (IsNull()) b.AppendNull(); else b.Append(random.Next(500) / 4f); }
                return b.Build();
            }
            case "double":
            {
                var b = new DoubleArray.Builder();
                for (int i = 0; i < rows; i++) { if (IsNull()) b.AppendNull(); else b.Append(random.Next(500) / 8.0); }
                return b.Build();
            }
            case "string":
            {
                var b = new StringArray.Builder();
                for (int i = 0; i < rows; i++) { if (IsNull()) b.AppendNull(); else b.Append("s" + random.Next(300)); }
                return b.Build();
            }
            default:
                throw new ArgumentException(kind);
        }
    }
}
