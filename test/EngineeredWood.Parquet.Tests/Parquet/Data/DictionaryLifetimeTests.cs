// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A dictionary's arrays are rented from the shared pool and returned once its column chunk is done
/// (#644). A batched read decodes a row group's pages over many batches from one dictionary, so the
/// dictionary must outlive every batch. If it were returned early, a batch decoded after that would
/// read arrays the pool had handed to someone else. Between batches, these tests read a second file
/// with other dictionaries, so any array returned early is rented again and overwritten.
/// </summary>
public class DictionaryLifetimeTests : IDisposable
{
    // 20% distinct is the most EW's writer dictionary-encodes. At this size the INT64 and DOUBLE
    // dictionaries (~150 KB) are over the large-object threshold, as they are in the files that
    // prompted #644.
    private const int Rows = 96_000;
    private const int Distinct = Rows / 5 - 1;

    private readonly string _tempDir;

    public DictionaryLifetimeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-dict-lifetime-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<DataPageVersion, CompressionCodec, int?, long?> Reads()
    {
        var data = new TheoryData<DataPageVersion, CompressionCodec, int?, long?>();
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (var codec in new[] { CompressionCodec.Uncompressed, CompressionCodec.Snappy })
        {
            // A row-count batch builds its page maps from the OffsetIndex, whose dictionary arrives
            // with the first data read; a byte budget scans the page headers instead.
            data.Add(version, codec, 7_777, null);
            data.Add(version, codec, null, 64 * 1024);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Reads))]
    public async Task BatchedRead_DecodesEveryBatch_FromALiveDictionary(
        DataPageVersion version, CompressionCodec codec, int? batchSize, long? maxBytes)
    {
        string path = await WriteAsync("main.parquet", version, codec, seed: 1);
        string other = await WriteAsync("other.parquet", version, codec, seed: 2);
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { BatchSize = batchSize, MaxBatchByteSize = maxBytes });
        AssertDictionaryEncoded(await reader.ReadMetadataAsync());

        int row = 0;
        int batches = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
        {
            using (batch)
            {
                AssertRows(batch, row, seed: 1);
                row += batch.Length;
                batches++;
            }

            await ReadWholeAsync(other, seed: 2);
        }

        Assert.Equal(Rows, row);
        Assert.True(batches > 2, $"Expected several batches, got {batches}.");
    }

    /// <summary>
    /// A caller that stops after the first batch abandons the enumeration. That returns the
    /// dictionaries too, and returns them only once: an array returned twice would be rented by two
    /// readers at the same time, which the concurrent reads afterwards would show.
    /// </summary>
    [Fact]
    public async Task AbandonedBatchedRead_ReturnsItsDictionariesOnce()
    {
        string path = await WriteAsync("main.parquet", DataPageVersion.V2, CompressionCodec.Snappy, seed: 1);
        string other = await WriteAsync("other.parquet", DataPageVersion.V2, CompressionCodec.Snappy, seed: 2);

        for (int i = 0; i < 3; i++)
        {
            await using var file = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(
                file, ownsFile: false, new ParquetReadOptions { BatchSize = 5_000 });
            await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
            {
                using (batch)
                    AssertRows(batch, 0, seed: 1);
                break;
            }
        }

        await Task.WhenAll(Enumerable.Range(0, 8).Select(i =>
            Task.Run(() => ReadWholeAsync(i % 2 == 0 ? path : other, seed: i % 2 == 0 ? 1 : 2))));
    }

    [Fact]
    public void DisposedDictionary_RefusesLookups()
    {
        var decoder = new DictionaryDecoder(PhysicalType.Int64);
        var bytes = new byte[8 * 3];
        for (int i = 0; i < 3; i++)
            BitConverter.GetBytes(10L * (i + 1)).CopyTo(bytes, 8 * i);
        decoder.Load(bytes, 3, 0);
        Assert.Equal(30L, decoder.GetInt64(2));

        decoder.Dispose();
        decoder.Dispose(); // idempotent

        Assert.Equal(0, decoder.Count);
        Assert.Throws<NullReferenceException>(() => decoder.GetInt64(0));

        // A page decoded after the release is a reader bug, and says so rather than blame the file.
        var element = new SchemaElement { Name = "id", Type = PhysicalType.Int64, RepetitionType = FieldRepetitionType.Required };
        var column = new ColumnDescriptor
        {
            Path = ["id"],
            PhysicalType = PhysicalType.Int64,
            MaxDefinitionLevel = 0,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = new SchemaNode { Element = element, Children = [] },
        };
        Assert.Throws<ObjectDisposedException>(() => decoder.CheckIndices([0], bitWidth: 2, column));
    }

    private static long IdAt(int row, int seed) => (row * 7919L % Distinct) * 1_000_003L + seed;

    private static double AmountAt(int row, int seed) => (row * 104_729L % Distinct) / 8.0 + seed;

    private static string NameAt(int row, int seed) => $"name-{row * 31L % Distinct:D6}-{seed}";

    private async Task<string> WriteAsync(string name, DataPageVersion version, CompressionCodec codec, int seed)
    {
        var ids = new Int64Array.Builder();
        var amounts = new DoubleArray.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(IdAt(i, seed));
            amounts.Append(AmountAt(i, seed));
            names.Append(NameAt(i, seed));
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, nullable: false))
            .Field(new Field("amount", DoubleType.Default, nullable: false))
            .Field(new Field("name", StringType.Default, nullable: false))
            .Build();
        using var batch = new RecordBatch(schema, [ids.Build(), amounts.Build(), names.Build()], Rows);

        string path = Path.Combine(_tempDir, name);
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
        {
            Compression = codec,
            DataPageVersion = version,
            DictionaryEnabled = true,
            DataPageSize = 8 * 1024, // many pages, so a batch rarely ends where a page does
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task ReadWholeAsync(string path, int seed)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        using var batch = await reader.ReadRowGroupAsync(0);
        AssertRows(batch, 0, seed);
    }

    private static void AssertDictionaryEncoded(FileMetaData metadata)
    {
        foreach (var chunk in metadata.RowGroups[0].Columns)
            Assert.NotNull(chunk.MetaData!.DictionaryPageOffset);
    }

    private static void AssertRows(RecordBatch batch, int firstRow, int seed)
    {
        var ids = (Int64Array)batch.Column(0);
        var amounts = (DoubleArray)batch.Column(1);
        var names = (StringArray)batch.Column(2);
        for (int i = 0; i < batch.Length; i++)
        {
            int row = firstRow + i;
            if (ids.GetValue(i) != IdAt(row, seed)
                || amounts.GetValue(i) != AmountAt(row, seed)
                || names.GetString(i) != NameAt(row, seed))
            {
                Assert.Fail(
                    $"Row {row}: ({ids.GetValue(i)}, {amounts.GetValue(i)}, {names.GetString(i)}), expected " +
                    $"({IdAt(row, seed)}, {AmountAt(row, seed)}, {NameAt(row, seed)}).");
            }
        }
    }
}
