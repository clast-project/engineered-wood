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
/// The chunk stream is pre-sized by <c>ColumnChunkWriter.EstimateColumnSize</c> and
/// <c>EstimateDictionaryColumnSize</c>, and a chunk that outgrows its estimate doubles the stream and
/// copies it (#508). The cases here are the ones that encode larger than their raw values:
/// incompressible integers, whose DELTA_BINARY_PACKED form at full width is larger; wide dictionary
/// indices; and small pages, whose headers and padding outweigh their values.
/// </summary>
public class ColumnChunkSizeEstimateTests : IDisposable
{
    private const int RowCount = 1_000_000;

    // Every case's pages are cut by the row limit: the size-based page length is longer for each.
    private const int DefaultPageLength = 20_000;

    private readonly string _tempDir;

    public ColumnChunkSizeEstimateTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-chunk-estimate-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // "extremes": every delta alternates between the type's minimum and maximum, the worst case.
    // "random": uniform over the full range, as hashes or ids are; its deltas need the full width too.
    [Theory]
    [InlineData("extremes", CompressionCodec.Uncompressed)]
    [InlineData("extremes", CompressionCodec.Snappy)]
    [InlineData("random", CompressionCodec.Uncompressed)]
    [InlineData("random", CompressionCodec.Snappy)]
    public async Task Int32Chunk_FitsItsEstimate(string shape, CompressionCodec codec)
    {
        var options = Options(codec, dictionary: false);
        long size = await WriteAndMeasure(Int32Values(shape, RowCount, nullable: false), options);

        AssertFits(size, ColumnChunkWriter.EstimateColumnSize(
            RowCount, RowCount, RowCount * 4L, Layout(RowCount, DefaultPageLength), PhysicalType.Int32, 0, 0, 0, deltaByteArray: false, options.Compression));
    }

    [Theory]
    [InlineData("extremes", CompressionCodec.Uncompressed)]
    [InlineData("extremes", CompressionCodec.Snappy)]
    [InlineData("random", CompressionCodec.Uncompressed)]
    [InlineData("random", CompressionCodec.Snappy)]
    public async Task Int64Chunk_FitsItsEstimate(string shape, CompressionCodec codec)
    {
        var options = Options(codec, dictionary: false);
        long size = await WriteAndMeasure(Int64Values(shape, RowCount), options);

        AssertFits(size, ColumnChunkWriter.EstimateColumnSize(
            RowCount, RowCount, RowCount * 8L, Layout(RowCount, DefaultPageLength), PhysicalType.Int64, 0, 0, 0, deltaByteArray: false, options.Compression));
    }

    // "wide": 100,000 entries, so 17-bit indices, where the estimate this replaced allowed 8.
    // "narrow": four entries, in runs of 8 alternating with 8 values that do not repeat, where run
    // headers cost more than the 2-bit indices. "single": one entry, so the indices are almost free
    // and page headers dominate.
    [Theory]
    [InlineData("wide", false, CompressionCodec.Uncompressed)]
    [InlineData("wide", false, CompressionCodec.Snappy)]
    [InlineData("wide", true, CompressionCodec.Uncompressed)]
    [InlineData("wide", true, CompressionCodec.Snappy)]
    [InlineData("narrow", false, CompressionCodec.Uncompressed)]
    [InlineData("narrow", true, CompressionCodec.Uncompressed)]
    [InlineData("single", false, CompressionCodec.Uncompressed)]
    [InlineData("single", true, CompressionCodec.Uncompressed)]
    public async Task DictionaryChunk_FitsItsEstimate(string shape, bool nullable, CompressionCodec codec)
    {
        var options = Options(codec, dictionary: true);
        var (array, distinct) = DictionaryValues(shape, RowCount, nullable);
        long size = await WriteAndMeasure(array, options);

        AssertFits(size, ColumnChunkWriter.EstimateDictionaryColumnSize(
            distinct * sizeof(int), RowCount, Layout(RowCount, DefaultPageLength), DictionaryEncoder.GetIndexBitWidth(distinct),
            maxDefLevel: nullable ? 1 : 0, maxRepLevel: 0, options.Compression));
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
    public async Task ByteArrayChunk_FitsItsEstimate(bool nullable, bool deltaByteArray, bool sliced)
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
        var (size, read) = await WriteMeasureAndRead(array, options);

        // A slice keeps the parent's buffers and an offset into them; the values written must be the
        // slice's, not the parent's first rows.
        var expected = (BinaryArray)array;
        var actual = (BinaryArray)read;
        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected.IsNull(i), actual.IsNull(i));
            Assert.True(expected.GetBytes(i).SequenceEqual(actual.GetBytes(i)), $"row {i} differs");
        }

        int nonNull = RowCount - array.NullCount;
        AssertFits(size, ColumnChunkWriter.EstimateColumnSize(
            RowCount, nonNull,
            ColumnChunkWriter.PlainValueBytes(array, nonNull, PhysicalType.ByteArray, 0),
            Layout(RowCount, DefaultPageLength), PhysicalType.ByteArray, 0, nullable ? 1 : 0, 0, deltaByteArray, options.Compression));
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

    // A repeated column's page cut backs up to the start of the record it falls in. Records of 3 and
    // 6 values against 8-value pages cut each pair into two pages, where equal pages of 8 would be
    // 1.125. Every page then has random deltas to pack, so each pads a full-width
    // DELTA_BINARY_PACKED miniblock to 32 deltas: an estimate counting equal pages falls short.
    [Fact]
    public async Task RepeatedChunk_ShortenedPages_FitsItsEstimate()
    {
        const int pageLength = 8;
        const int levelTarget = 200_000;
        var options = Options(CompressionCodec.Uncompressed, dictionary: false)
            with { DataPageSize = pageLength * sizeof(int) };

        var rng = new Random(508);
        var type = new ListType(new Field("item", Int32Type.Default, nullable: false));
        var values = new Int32Array.Builder();
        var offsets = new List<int> { 0 };
        var repLevels = new List<int>();
        for (int record = 0; repLevels.Count < levelTarget; record++)
        {
            int length = record % 2 == 0 ? 3 : pageLength - 2;
            for (int i = 0; i < length; i++)
            {
                values.Append(rng.Next(int.MinValue, int.MaxValue));
                repLevels.Add(i == 0 ? 0 : 1);
            }
            offsets.Add(repLevels.Count);
        }
        var offsetBytes = new byte[offsets.Count * sizeof(int)];
        Buffer.BlockCopy(offsets.ToArray(), 0, offsetBytes, 0, offsetBytes.Length);
        var array = new ListArray(
            type, offsets.Count - 1, new ArrowBuffer(offsetBytes), values.Build(), ArrowBuffer.Empty);

        long size = await WriteAndMeasure(array, options);

        int levels = repLevels.Count;
        var layout = ColumnChunkWriter.LayOutPages(repLevels.ToArray(), levels, pageLength, options.DataPageRowCountLimit);
        Assert.True(
            layout.Count > Layout(levels, pageLength).Count * 3 / 2,
            $"{layout.Count} pages: the cut was meant to shorten them");
        AssertFits(size, ColumnChunkWriter.EstimateColumnSize(
            levels, levels, levels * 4L, layout, PhysicalType.Int32, 0, maxDefLevel: 1, maxRepLevel: 1,
            deltaByteArray: false, options.Compression));
    }

    // A small DataPageRowCountLimit makes per-page costs outweigh the values: a 1-row INT32 page is
    // about 27 bytes of header around 4 of value, and at 7 rows the last DELTA_BINARY_PACKED
    // miniblock is padded from 6 deltas to 32.
    [Theory]
    [InlineData("int32", 1)]
    [InlineData("int32", 2)]
    [InlineData("int32", 7)]
    [InlineData("int32", 33)]
    [InlineData("int32-nullable", 1)]
    [InlineData("int32-nullable", 7)]
    [InlineData("int64", 1)]
    [InlineData("int64", 7)]
    [InlineData("flba", 1)]
    [InlineData("flba", 7)]
    [InlineData("flba-dba", 1)]
    [InlineData("flba-dba", 7)]
    [InlineData("dict", 1)]
    [InlineData("dict", 7)]
    [InlineData("dict-nullable", 1)]
    public async Task RowLimitedChunk_FitsItsEstimate(string kind, int limit)
    {
        const int rows = 20_000;
        bool nullable = kind.EndsWith("-nullable", StringComparison.Ordinal);
        var options = Options(CompressionCodec.Uncompressed, dictionary: kind.StartsWith("dict", StringComparison.Ordinal))
            with
        {
            DataPageRowCountLimit = limit,
            ByteArrayEncoding = kind == "flba-dba" ? ByteArrayEncoding.DeltaByteArray : ParquetWriteOptions.Default.ByteArrayEncoding,
        };

        IArrowArray array;
        int estimate;
        switch (kind)
        {
            case "int32":
            case "int32-nullable":
                array = Int32Values("random", rows, nullable);
                estimate = ColumnChunkWriter.EstimateColumnSize(
                    rows, rows - array.NullCount, (rows - array.NullCount) * 4L, Layout(rows, limit), PhysicalType.Int32, 0, nullable ? 1 : 0, 0, false, options.Compression);
                break;
            case "int64":
                array = Int64Values("random", rows);
                estimate = ColumnChunkWriter.EstimateColumnSize(
                    rows, rows, rows * 8L, Layout(rows, limit), PhysicalType.Int64, 0, 0, 0, false, options.Compression);
                break;
            case "flba":
            case "flba-dba":
                array = SharedPrefixFlba(rows, 16);
                estimate = ColumnChunkWriter.EstimateColumnSize(
                    rows, rows, rows * 16L, Layout(rows, limit), PhysicalType.FixedLenByteArray, 16, 0, 0, kind == "flba-dba", options.Compression);
                break;
            default:
                int distinct;
                (array, distinct) = DictionaryValues("wide", rows, nullable);
                estimate = ColumnChunkWriter.EstimateDictionaryColumnSize(
                    distinct * sizeof(int), rows, Layout(rows, limit), DictionaryEncoder.GetIndexBitWidth(distinct),
                    maxDefLevel: nullable ? 1 : 0, maxRepLevel: 0, options.Compression);
                break;
        }

        AssertFits(await WriteAndMeasure(array, options), estimate);
    }

    // A codec frames every page, and a page of a few bytes cannot cover that in proportion: a 2-byte
    // page compresses to 22 bytes of Gzip. Checksums add a CRC to every page header.
    [Theory]
    [InlineData(CompressionCodec.Snappy, false)]
    [InlineData(CompressionCodec.Gzip, false)]
    [InlineData(CompressionCodec.Gzip, true)]
    [InlineData(CompressionCodec.Brotli, false)]
    [InlineData(CompressionCodec.Lz4, false)]
    [InlineData(CompressionCodec.Zstd, false)]
    [InlineData(CompressionCodec.Zstd, true)]
    [InlineData(CompressionCodec.Uncompressed, true)]
    public async Task SmallPages_EachCodec_FitTheirEstimate(CompressionCodec codec, bool checksums)
    {
        const int rows = 20_000;
        var rng = new Random(508);
        var flbaBytes = new byte[rows * 2];
        rng.NextBytes(flbaBytes);
        var flba = new FixedSizeBinaryArray(new ArrayData(
            new FixedSizeBinaryType(2), rows, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(flbaBytes)]));
        var ints = Int32Values("random", rows, nullable: false);
        var (dictionary, distinct) = DictionaryValues("wide", rows, nullable: false);

        foreach (int limit in new[] { 1, 7 })
        {
            var layout = Layout(rows, limit);
            var plain = Options(codec, dictionary: false) with
            {
                DataPageRowCountLimit = limit,
                PageChecksumEnabled = checksums,
            };
            var dictionaryOptions = plain with { DictionaryEnabled = true };

            AssertFits(await WriteAndMeasure(flba, plain), ColumnChunkWriter.EstimateColumnSize(
                rows, rows, rows * 2L, layout, PhysicalType.FixedLenByteArray, 2, 0, 0, false, codec));
            AssertFits(await WriteAndMeasure(ints, plain), ColumnChunkWriter.EstimateColumnSize(
                rows, rows, rows * 4L, layout, PhysicalType.Int32, 0, 0, 0, false, codec));
            AssertFits(await WriteAndMeasure(dictionary, dictionaryOptions), ColumnChunkWriter.EstimateDictionaryColumnSize(
                distinct * sizeof(int), rows, layout, DictionaryEncoder.GetIndexBitWidth(distinct), 0, 0, codec));
        }
    }

    // DELTA_BYTE_ARRAY lengths: one value per 32 shares a 1-byte prefix with the one before, which
    // widens both length streams to 2 bits while saving one byte. The chunk is larger than its raw
    // values, and its page buffer was sized below the encoding, so the write threw.
    [Theory]
    [InlineData(16)]
    [InlineData(2)]
    public async Task FlbaDeltaByteArray_SharedPrefixes_FitsItsEstimateAndRoundTrips(int typeLength)
    {
        var options = Options(CompressionCodec.Uncompressed, dictionary: false)
            with { ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray };
        var array = SharedPrefixFlba(RowCount, typeLength);

        var (size, read) = await WriteMeasureAndRead(array, options);

        AssertFits(size, ColumnChunkWriter.EstimateColumnSize(
            RowCount, RowCount, (long)RowCount * typeLength, Layout(RowCount, DefaultPageLength), PhysicalType.FixedLenByteArray, typeLength, 0, 0,
            deltaByteArray: true, options.Compression));
        Assert.Equal(
            array.Data.Buffers[1].Span.ToArray(),
            read.Data.Buffers[1].Span.Slice(0, RowCount * typeLength).ToArray());
    }

    [Fact]
    public async Task ByteArrayDeltaByteArray_SharedPrefixes_RoundTrips()
    {
        const int rows = 100_000;
        var flba = SharedPrefixFlba(rows, 16);
        var builder = new BinaryArray.Builder();
        for (int i = 0; i < rows; i++)
            builder.Append(flba.Data.Buffers[1].Span.Slice(i * 16, 16));
        var array = builder.Build();
        var options = Options(CompressionCodec.Uncompressed, dictionary: false)
            with { ByteArrayEncoding = ByteArrayEncoding.DeltaByteArray };

        var (_, read) = await WriteMeasureAndRead(array, options);

        var binary = (BinaryArray)read;
        for (int i = 0; i < rows; i++)
            Assert.True(array.GetBytes(i).SequenceEqual(binary.GetBytes(i)), $"row {i} differs");
    }

    private static Int32Array Int32Values(string shape, int rows, bool nullable)
    {
        var builder = new Int32Array.Builder();
        var rng = new Random(508);
        int value = int.MinValue;
        for (int i = 0; i < rows; i++)
        {
            if (nullable && rng.NextDouble() < 0.1)
            {
                builder.AppendNull();
                continue;
            }

            if (shape == "random")
                value = rng.Next(int.MinValue, int.MaxValue);
            else if (i > 0)
                value = unchecked(value + (i % 2 == 1 ? int.MinValue : int.MaxValue));
            builder.Append(value);
        }
        return builder.Build();
    }

    private static Int64Array Int64Values(string shape, int rows)
    {
        var builder = new Int64Array.Builder();
        var rng = new Random(508);
        var draw = new byte[8];
        long value = long.MinValue;
        for (int i = 0; i < rows; i++)
        {
            if (shape == "random")
            {
                rng.NextBytes(draw);
                value = BitConverter.ToInt64(draw, 0);
            }
            else if (i > 0)
            {
                value = unchecked(value + (i % 2 == 1 ? long.MinValue : long.MaxValue));
            }
            builder.Append(value);
        }
        return builder.Build();
    }

    private static (Int32Array Array, int Distinct) DictionaryValues(string shape, int rows, bool nullable)
    {
        int distinct = shape switch { "wide" => Math.Min(100_000, rows / 10), "narrow" => 4, _ => 1 };
        var builder = new Int32Array.Builder();
        var rng = new Random(508);
        for (int i = 0; i < rows; i++)
        {
            if (nullable && rng.NextDouble() < 0.1)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append(shape switch
            {
                "wide" => rng.Next(distinct),
                "narrow" => (i / 8) % 2 == 0 ? 0 : i % 4,
                _ => 0,
            });
        }
        return (builder.Build(), distinct);
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

    // A column that is not repeated is cut into equal pages of this length, or fewer at the end.
    private static ColumnChunkWriter.PageLayout Layout(int rows, int pageLength) =>
        ColumnChunkWriter.LayOutPages(repLevels: null, rows, int.MaxValue, pageLength);

    private static ParquetWriteOptions Options(CompressionCodec codec, bool dictionary) =>
        ParquetWriteOptions.Default with
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = dictionary,
            Compression = codec,
        };

    private static void AssertFits(long size, int estimate) =>
        Assert.True(size <= estimate, $"the chunk is {size:N0} bytes but the estimate is {estimate:N0}");

    private async Task<long> WriteAndMeasure(IArrowArray array, ParquetWriteOptions options) =>
        (await WriteMeasureAndRead(array, options)).Size;

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
