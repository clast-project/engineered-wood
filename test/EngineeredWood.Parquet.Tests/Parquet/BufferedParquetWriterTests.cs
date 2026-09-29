// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

public class BufferedParquetWriterTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedParquetWriterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-buffered-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    [Fact]
    public async Task SingleBatch_RoundTrip()
    {
        string path = TempPath("single_batch.parquet");
        var batch = MakeMixedBatch(1000);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        await VerifyRoundTrip(path, batch.Length, batch.ColumnCount);
    }

    [Fact]
    public async Task MultipleBatches_ConsolidateIntoOneRowGroup()
    {
        string path = TempPath("multi_batch.parquet");
        int batchSize = 200;
        int batchCount = 5;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            for (int i = 0; i < batchCount; i++)
                await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 42 + i));
            await writer.CloseAsync();
        }

        // Should produce ONE row group with 1000 rows (not 5 row groups of 200)
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Equal(1000, metadata.NumRows);
        Assert.Single(metadata.RowGroups);
        Assert.Equal(1000, metadata.RowGroups[0].NumRows);
    }

    [Fact]
    public async Task AutoFlush_WhenRowGroupMaxReached()
    {
        string path = TempPath("auto_flush.parquet");
        int batchSize = 300;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions
            {
                Compression = CompressionCodec.Uncompressed,
                RowGroupMaxRows = 500,
            }))
        {
            // Append 3 batches of 300 → 900 rows. With maxRows=500:
            // After batch 1: 300 buffered
            // After batch 2: 600 → auto-flush at 500, then 100 remaining + batch 3
            await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 1));
            await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 2));
            await writer.AppendAsync(MakeMixedBatch(batchSize, seed: 3));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        Assert.Equal(900, metadata.NumRows);
        // Should have produced 2 row groups (500 + 400)
        Assert.Equal(2, metadata.RowGroups.Count);
    }

    [Fact]
    public async Task MultipleBatches_AllDataPreserved()
    {
        string path = TempPath("data_preserved.parquet");
        var rng = new Random(42);
        string[] categories = ["alpha", "beta", "gamma"];

        int totalRows = 0;
        var allCategories = new List<string>();
        var allValues = new List<int>();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Snappy }))
        {
            for (int batch = 0; batch < 5; batch++)
            {
                int batchSize = 100 + batch * 50;
                var schema = new Apache.Arrow.Schema.Builder()
                    .Field(new Field("cat", StringType.Default, nullable: false))
                    .Field(new Field("val", Int32Type.Default, nullable: false))
                    .Build();

                var catBuilder = new StringArray.Builder();
                var valBuilder = new Int32Array.Builder();
                for (int i = 0; i < batchSize; i++)
                {
                    string cat = categories[rng.Next(categories.Length)];
                    int val = rng.Next(1000);
                    catBuilder.Append(cat);
                    valBuilder.Append(val);
                    allCategories.Add(cat);
                    allValues.Add(val);
                }

                var b = new RecordBatch(schema, [catBuilder.Build(), valBuilder.Build()], batchSize);
                await writer.AppendAsync(b);
                totalRows += batchSize;
            }
            await writer.CloseAsync();
        }

        // Read back and verify all values present
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(totalRows, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var readCats = (StringArray)readBatch.Column(0);
        var readVals = (Int32Array)readBatch.Column(1);

        var readCatList = new List<string>();
        var readValList = new List<int>();
        for (int i = 0; i < readBatch.Length; i++)
        {
            readCatList.Add(readCats.GetString(i)!);
            readValList.Add(readVals.GetValue(i)!.Value);
        }

        allCategories.Sort();
        readCatList.Sort();
        Assert.Equal(allCategories, readCatList);

        allValues.Sort();
        readValList.Sort();
        Assert.Equal(allValues, readValList);
    }

    [Fact]
    public async Task NullableColumns_HandledCorrectly()
    {
        string path = TempPath("nullable.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, nullable: true))
            .Field(new Field("score", Int32Type.Default, nullable: true))
            .Build();

        int expectedNulls = 0;

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            var rng = new Random(99);
            for (int b = 0; b < 3; b++)
            {
                var nameBuilder = new StringArray.Builder();
                var scoreBuilder = new Int32Array.Builder();
                for (int i = 0; i < 100; i++)
                {
                    if (rng.Next(5) == 0)
                    {
                        nameBuilder.AppendNull();
                        expectedNulls++;
                    }
                    else
                        nameBuilder.Append("item");

                    if (rng.Next(5) == 0)
                        scoreBuilder.AppendNull();
                    else
                        scoreBuilder.Append(rng.Next(100));
                }
                await writer.AppendAsync(new RecordBatch(schema,
                    [nameBuilder.Build(), scoreBuilder.Build()], 100));
            }
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var readBatch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(300, readBatch.Length);
        var names = (StringArray)readBatch.Column(0);
        int actualNulls = 0;
        for (int i = 0; i < names.Length; i++)
            if (names.IsNull(i)) actualNulls++;
        Assert.Equal(expectedNulls, actualNulls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BloomFilter_DictionaryColumns_MatchTheFileWriter(bool writeStatistics)
    {
        // The buffered writer's dictionary path wrote no Bloom filter at all, whatever
        // BloomFilterColumns said. Statistics off is its own case: that path used to return early.
        var batch = MakeMixedBatch(1000);
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = ["id", "name"],
            WriteStatistics = writeStatistics,
        };

        string buffered = TempPath("bloom_buffered.parquet");
        await using (var file = new LocalSequentialFile(buffered))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        string direct = TempPath("bloom_direct.parquet");
        await using (var file = new LocalSequentialFile(direct))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var bufferedFile = new LocalRandomAccessFile(buffered);
        await using var reader = new ParquetFileReader(bufferedFile, ownsFile: false);
        var bufferedMeta = await reader.ReadMetadataAsync();

        await using var directFile = new LocalRandomAccessFile(direct);
        await using var directReader = new ParquetFileReader(directFile, ownsFile: false);
        var directMeta = await directReader.ReadMetadataAsync();

        byte[] bufferedBytes = File.ReadAllBytes(buffered);
        byte[] directBytes = File.ReadAllBytes(direct);
        for (int c = 0; c < 2; c++)
        {
            var b = bufferedMeta.RowGroups[0].Columns[c].MetaData!;
            var d = directMeta.RowGroups[0].Columns[c].MetaData!;
            Assert.Contains(Encoding.RleDictionary, b.Encodings);
            Assert.NotNull(b.BloomFilterOffset);

            // A filter is a set, so the two writers' different dictionary orders build the same bytes.
            Assert.Equal(
                directBytes.AsSpan((int)d.BloomFilterOffset!.Value, d.BloomFilterLength!.Value).ToArray(),
                bufferedBytes.AsSpan((int)b.BloomFilterOffset.Value, b.BloomFilterLength!.Value).ToArray());
        }

        Assert.True((await reader.GetCandidateRowGroupsAsync("name", "gamma"))[0]);
        Assert.False((await reader.GetCandidateRowGroupsAsync("name", "zzz_absent"))[0]);
        Assert.False((await reader.GetCandidateRowGroupsAsync("id", 12345))[0]);
    }

    public static TheoryData<string, bool, bool> NarrowIntegerCases()
    {
        var data = new TheoryData<string, bool, bool>();
        foreach (var type in new[] { "int8", "uint8", "int16", "uint16" })
        foreach (bool highCardinality in new[] { false, true })
        foreach (bool writeStatistics in new[] { true, false })
            data.Add(type, highCardinality, writeStatistics);
        return data;
    }

    [Theory]
    [MemberData(nameof(NarrowIntegerCases))]
    public async Task NarrowIntegers_RoundTripAndMatchTheFileWriter(string type, bool highCardinality, bool writeStatistics)
    {
        // These types are written as INT32, whose PLAIN dictionary entries are four bytes. The buffered writer
        // kept each entry at its Arrow width, so the dictionary page was one or two bytes per entry: with
        // statistics on the write threw, and with them off the reader refused the file (#441). Low cardinality
        // keeps the dictionary; high cardinality falls back to rebuilding the column from it.
        const int rows = 600;
        long[] extremes = type switch
        {
            "int8" => [sbyte.MinValue, -1, 0, 1, sbyte.MaxValue],
            "uint8" => [0, 1, 128, 200, byte.MaxValue],
            "int16" => [short.MinValue, -1, 0, 1, short.MaxValue],
            _ => [0, 1, 32768, 60000, ushort.MaxValue],
        };
        long min = extremes[0], span = extremes[extremes.Length - 1] - min + 1;
        long? Value(int i) => i % 11 == 4 ? null
            : highCardinality ? min + (i * 7919L % span) : extremes[i % extremes.Length];

        IArrowArray array;
        IArrowType arrowType;
        switch (type)
        {
            case "int8":
                var i8 = new Int8Array.Builder();
                for (int i = 0; i < rows; i++) { if (Value(i) is long v) i8.Append((sbyte)v); else i8.AppendNull(); }
                (array, arrowType) = (i8.Build(), Int8Type.Default);
                break;
            case "uint8":
                var u8 = new UInt8Array.Builder();
                for (int i = 0; i < rows; i++) { if (Value(i) is long v) u8.Append((byte)v); else u8.AppendNull(); }
                (array, arrowType) = (u8.Build(), UInt8Type.Default);
                break;
            case "int16":
                var i16 = new Int16Array.Builder();
                for (int i = 0; i < rows; i++) { if (Value(i) is long v) i16.Append((short)v); else i16.AppendNull(); }
                (array, arrowType) = (i16.Build(), Int16Type.Default);
                break;
            default:
                var u16 = new UInt16Array.Builder();
                for (int i = 0; i < rows; i++) { if (Value(i) is long v) u16.Append((ushort)v); else u16.AppendNull(); }
                (array, arrowType) = (u16.Build(), UInt16Type.Default);
                break;
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Field("n", arrowType, nullable: true)).Build(), [array], rows);
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            BloomFilterColumns = ["n"],
            WriteStatistics = writeStatistics,
        };

        string buffered = TempPath("narrow_buffered.parquet");
        await using (var file = new LocalSequentialFile(buffered))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false, options))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        string direct = TempPath("narrow_direct.parquet");
        await using (var file = new LocalSequentialFile(direct))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
        }

        await using var bufferedFile = new LocalRandomAccessFile(buffered);
        await using var reader = new ParquetFileReader(bufferedFile, ownsFile: false);
        var read = (await reader.ReadRowGroupAsync(0)).Column(0);
        for (int i = 0; i < rows; i++)
        {
            long? actual = read.IsNull(i) ? null : read switch
            {
                Int8Array a => a.GetValue(i),
                UInt8Array a => a.GetValue(i),
                Int16Array a => a.GetValue(i),
                UInt16Array a => (long?)a.GetValue(i),
                _ => throw new InvalidOperationException(read.GetType().Name),
            };
            Assert.Equal(Value(i), actual);
        }

        await using var directFile = new LocalRandomAccessFile(direct);
        await using var directReader = new ParquetFileReader(directFile, ownsFile: false);
        var b = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!;
        var d = (await directReader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!;

        Assert.Equal(!highCardinality, b.Encodings.Contains(Encoding.RleDictionary));
        Assert.Equal(d.Statistics?.MinValue, b.Statistics?.MinValue);
        Assert.Equal(d.Statistics?.MaxValue, b.Statistics?.MaxValue);
        Assert.Equal(d.Statistics?.NullCount, b.Statistics?.NullCount);
        Assert.Equal(
            File.ReadAllBytes(direct).AsSpan((int)d.BloomFilterOffset!.Value, d.BloomFilterLength!.Value).ToArray(),
            File.ReadAllBytes(buffered).AsSpan((int)b.BloomFilterOffset!.Value, b.BloomFilterLength!.Value).ToArray());
    }

    private static RecordBatch MakeMixedBatch(int rowCount, int seed = 42)
    {
        var rng = new Random(seed);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: true))
            .Field(new Field("value", DoubleType.Default, nullable: false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var valBuilder = new DoubleArray.Builder();
        string[] names = ["alpha", "beta", "gamma", "delta"];

        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(rng.Next(100));
            if (rng.Next(10) == 0) nameBuilder.AppendNull();
            else nameBuilder.Append(names[rng.Next(names.Length)]);
            valBuilder.Append(rng.NextDouble() * 100);
        }

        return new RecordBatch(schema, [idBuilder.Build(), nameBuilder.Build(), valBuilder.Build()], rowCount);
    }

    private static async Task VerifyRoundTrip(string path, int expectedRows, int expectedCols)
    {
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(expectedRows, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        Assert.Equal(expectedRows, readBatch.Length);
        Assert.Equal(expectedCols, readBatch.ColumnCount);
    }
}
