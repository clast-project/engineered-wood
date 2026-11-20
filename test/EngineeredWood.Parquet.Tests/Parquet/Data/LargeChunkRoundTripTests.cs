// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Chunks that encode larger than their raw values: random BYTE_ARRAY values under DELTA_LENGTH_BYTE_ARRAY and
/// DELTA_BYTE_ARRAY, and fixed-width values whose shared prefixes widen DELTA_BYTE_ARRAY's length streams. Their
/// pages were once written into a buffer pre-sized from an estimate, which these cases outgrew (#508); they are now
/// held in a segmented page store (#936 decision 2), and span many of its segments.
/// </summary>
public class LargeChunkRoundTripTests : IDisposable
{
    private const int RowCount = 1_000_000;

    private readonly string _tempDir;

    public LargeChunkRoundTripTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-large-chunk-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // BYTE_ARRAY values are random bytes of random length, so nothing compresses and nothing shares a
    // prefix. ParquetFileWriter compacts a sliced column before it is encoded, so the sliced cases
    // check the values that come back; PlainValueBytes_SlicedBinary covers a slice's offsets.
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, true, true)]
    public async Task ByteArrayChunk_Incompressible_RoundTrips(bool nullable, bool deltaByteArray, bool sliced)
    {
        var builder = new BinaryArray.Builder();
        var rng = new Random(508);
        int built = sliced ? RowCount + 1000 : RowCount;
        for (int i = 0; i < built; i++)
        {
            if (nullable && rng.NextDouble() < 0.1)
            {
                builder.AppendNull();
                continue;
            }

            var value = new byte[rng.Next(0, 40)];
            rng.NextBytes(value);
            builder.Append((ReadOnlySpan<byte>)value);
        }
        IArrowArray array = builder.Build();
        if (sliced)
            array = Apache.Arrow.ArrowArrayFactory.BuildArray(array.Data.Slice(1000, RowCount));

        var options = Options(CompressionCodec.Uncompressed, dictionary: false) with
        {
            ByteArrayEncoding = deltaByteArray ? ByteArrayEncoding.DeltaByteArray : ByteArrayEncoding.DeltaLengthByteArray,
        };
        var (_, read) = await WriteMeasureAndRead(array, options);

        // A slice keeps the parent's buffers and an offset into them; the values written must be the
        // slice's, not the parent's first rows.
        var expected = (BinaryArray)array;
        var actual = (BinaryArray)read;
        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected.IsNull(i), actual.IsNull(i));
            Assert.True(expected.GetBytes(i).SequenceEqual(actual.GetBytes(i)), $"row {i} differs");
            GC.KeepAlive(expected);
            GC.KeepAlive(actual);
        }
    }

    [Fact]
    public void PlainValueBytes_SlicedBinary_CountsTheSliceOnly()
    {
        var builder = new BinaryArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append((ReadOnlySpan<byte>)new byte[i]);
        var slice = Apache.Arrow.ArrowArrayFactory.BuildArray(builder.Build().Data.Slice(10, 5));

        // Rows 10..14 hold 10 + 11 + 12 + 13 + 14 bytes, each behind a 4-byte length.
        Assert.Equal(60 + 5 * 4, ColumnChunkWriter.PlainValueBytes(slice, 5, PhysicalType.ByteArray, 0));
    }

    // DELTA_BYTE_ARRAY lengths: one value per 32 shares a 1-byte prefix with the one before, which
    // widens both length streams to 2 bits while saving one byte. The chunk is larger than its raw
    // values: when the chunk's buffer was pre-sized from an estimate, this one fell short and the write threw.
    [Theory]
    [InlineData(16)]
    [InlineData(2)]
    public async Task FlbaDeltaByteArray_SharedPrefixes_RoundTrips(int typeLength)
    {
        var options = Options(CompressionCodec.Uncompressed, dictionary: false)
            with { ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray };
        var array = SharedPrefixFlba(RowCount, typeLength);

        var (_, read) = await WriteMeasureAndRead(array, options);

        Assert.Equal(
            array.Data.Buffers[1].Span.ToArray(),
            read.Data.Buffers[1].Span.Slice(0, RowCount * typeLength).ToArray());
        GC.KeepAlive(array);
        GC.KeepAlive(read);
    }

    [Fact]
    public async Task ByteArrayDeltaByteArray_SharedPrefixes_RoundTrips()
    {
        const int rows = 100_000;
        var flba = SharedPrefixFlba(rows, 16);
        var builder = new BinaryArray.Builder();
        for (int i = 0; i < rows; i++)
            builder.Append(flba.Data.Buffers[1].Span.Slice(i * 16, 16));
        GC.KeepAlive(flba);
        var array = builder.Build();
        var options = Options(CompressionCodec.Uncompressed, dictionary: false)
            with { ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray };

        var (_, read) = await WriteMeasureAndRead(array, options);

        var binary = (BinaryArray)read;
        for (int i = 0; i < rows; i++)
            Assert.True(array.GetBytes(i).SequenceEqual(binary.GetBytes(i)), $"row {i} differs");
        GC.KeepAlive(array);
        GC.KeepAlive(binary);
    }

    // Consecutive values differ in their first byte, except that every 32nd shares it with the one
    // before and differs in the second.
    private static FixedSizeBinaryArray SharedPrefixFlba(int rows, int typeLength)
    {
        var bytes = new byte[rows * typeLength];
        for (int i = 0; i < rows; i++)
        {
            var value = bytes.AsSpan(i * typeLength, typeLength);
            value[0] = (byte)i;
            value[1] = 0xAA;
            if (i % 32 == 1)
            {
                value[0] = bytes[(i - 1) * typeLength];
                value[1] = 0x55;
            }
        }

        var type = new FixedSizeBinaryType(typeLength);
        return new FixedSizeBinaryArray(
            new ArrayData(type, rows, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
    }

    private static ParquetWriteOptions Options(CompressionCodec codec, bool dictionary) =>
        ParquetWriteOptions.Default with
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = dictionary,
            Compression = codec,
        };

    private async Task<(long Size, IArrowArray Read)> WriteMeasureAndRead(
        IArrowArray array, ParquetWriteOptions options)
    {
        var path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", array.Data.DataType, nullable: array.NullCount > 0))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], array.Length));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        var column = meta.RowGroups[0].Columns[0].MetaData!;
        Assert.Contains(ExpectedEncoding(array.Data.DataType, options), column.Encodings);

        var batch = await reader.ReadRowGroupAsync(0);
        return (column.TotalCompressedSize, batch.Column(0));
    }

    // Each case asserts the encoding it means to measure, so a change of default cannot quietly
    // turn one into a different case.
    private static EngineeredWood.Parquet.Encoding ExpectedEncoding(IArrowType type, ParquetWriteOptions options) =>
        type is ListType list ? ExpectedEncoding(list.ValueDataType, options)
        : options.DictionaryEnabled ? EngineeredWood.Parquet.Encoding.RleDictionary
        : type is Int32Type or Int64Type ? EngineeredWood.Parquet.Encoding.DeltaBinaryPacked
        : options.ByteArrayEncoding == ByteArrayEncoding.DeltaByteArray ? EngineeredWood.Parquet.Encoding.DeltaByteArray
        : type is BinaryType ? EngineeredWood.Parquet.Encoding.DeltaLengthByteArray
        : EngineeredWood.Parquet.Encoding.Plain;
}
