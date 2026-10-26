// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The chunk stream is pre-sized by <c>ColumnChunkWriter.EstimateColumnSize</c>, and a chunk that
/// outgrows it doubles the stream and copies it (#508). Incompressible integers are the case it has
/// to cover: at full width DELTA_BINARY_PACKED is larger than the raw values.
/// </summary>
public class ColumnChunkSizeEstimateTests : IDisposable
{
    private const int RowCount = 1_000_000;

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
        var builder = new Int32Array.Builder();
        var rng = new Random(508);
        int value = int.MinValue;
        for (int i = 0; i < RowCount; i++)
        {
            if (shape == "random")
                value = rng.Next(int.MinValue, int.MaxValue);
            else if (i > 0)
                value = unchecked(value + (i % 2 == 1 ? int.MinValue : int.MaxValue));
            builder.Append(value);
        }

        long size = await WriteAndMeasure(Int32Type.Default, builder.Build(), codec);

        int estimate = ColumnChunkWriter.EstimateColumnSize(RowCount, PhysicalType.Int32, 0);
        Assert.True(size <= estimate, $"the chunk is {size:N0} bytes but the estimate is {estimate:N0}");
    }

    [Theory]
    [InlineData("extremes", CompressionCodec.Uncompressed)]
    [InlineData("extremes", CompressionCodec.Snappy)]
    [InlineData("random", CompressionCodec.Uncompressed)]
    [InlineData("random", CompressionCodec.Snappy)]
    public async Task Int64Chunk_FitsItsEstimate(string shape, CompressionCodec codec)
    {
        var builder = new Int64Array.Builder();
        var rng = new Random(508);
        var draw = new byte[8];
        long value = long.MinValue;
        for (int i = 0; i < RowCount; i++)
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

        long size = await WriteAndMeasure(Int64Type.Default, builder.Build(), codec);

        int estimate = ColumnChunkWriter.EstimateColumnSize(RowCount, PhysicalType.Int64, 0);
        Assert.True(size <= estimate, $"the chunk is {size:N0} bytes but the estimate is {estimate:N0}");
    }

    // A dictionary chunk is sized by EstimateDictionaryColumnSize instead. "wide": 100,000 entries,
    // so 17-bit indices, where the estimate it replaced allowed 8. "narrow": four entries, in runs of
    // 8 alternating with 8 values that do not repeat, where run headers cost more than the 2-bit
    // indices. "single": one entry, so the indices are almost free and page headers dominate.
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
        int distinct = shape switch { "wide" => 100_000, "narrow" => 4, _ => 1 };
        var builder = new Int32Array.Builder();
        var rng = new Random(508);
        for (int i = 0; i < RowCount; i++)
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

        long size = await WriteAndMeasure(
            Int32Type.Default, builder.Build(), codec, nullable, dictionary: true);

        int estimate = ColumnChunkWriter.EstimateDictionaryColumnSize(
            distinct * sizeof(int), RowCount, DictionaryEncoder.GetIndexBitWidth(distinct),
            maxDefLevel: nullable ? 1 : 0, maxRepLevel: 0);
        Assert.True(size <= estimate, $"the chunk is {size:N0} bytes but the estimate is {estimate:N0}");
    }

    private async Task<long> WriteAndMeasure(
        IArrowType type, IArrowArray array, CompressionCodec codec,
        bool nullable = false, bool dictionary = false)
    {
        var path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", type, nullable))
            .Build();
        var options = ParquetWriteOptions.Default with
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = dictionary,
            Compression = codec,
        };

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
        Assert.Contains(
            dictionary ? EngineeredWood.Parquet.Encoding.RleDictionary : EngineeredWood.Parquet.Encoding.DeltaBinaryPacked,
            column.Encodings);
        return column.TotalCompressedSize;
    }
}
