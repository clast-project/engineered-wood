// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

public class PageChecksumTests
{
    [Fact]
    public async Task RoundTrip_WriteWithCrc_ReadWithValidation()
    {
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, false))
                .Field(new Apache.Arrow.Field("name", Apache.Arrow.Types.StringType.Default, false))
                .Build();

            var ids = new Apache.Arrow.Int32Array.Builder();
            var names = new Apache.Arrow.StringArray.Builder();
            for (int i = 0; i < 200; i++)
            {
                ids.Append(i);
                names.Append($"name_{i}");
            }

            var batch = new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[]
            {
                ids.Build(), names.Build()
            }, 200);

            // Write with CRC enabled.
            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
            {
                PageChecksumEnabled = true,
            }))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // Read with CRC validation enabled — should succeed.
            await using var readFile = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(readFile, ownsFile: false, new ParquetReadOptions
            {
                PageChecksumValidation = true,
            });

            var result = await reader.ReadRowGroupAsync(0);
            Assert.Equal(200, result.Length);

            // Verify data is correct.
            var idCol = (Apache.Arrow.Int32Array)result.Column(0);
            Assert.Equal(0, idCol.GetValue(0));
            Assert.Equal(199, idCol.GetValue(199));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TamperDetection_FlippedBit_ThrowsOnValidation()
    {
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("x", Apache.Arrow.Types.Int32Type.Default, false))
                .Build();

            var values = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { values }, 100);

            // Write with CRC.
            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
            {
                PageChecksumEnabled = true,
            }))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // Tamper with a byte that's definitely in compressed page data: 5 bytes before the end of the
            // last column chunk, located through the footer. (Not "just before the footer": page indexes
            // sit there, and they carry no CRC.)
            var fileBytes = File.ReadAllBytes(path);
            int footerLen = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
            var metadata = EngineeredWood.Parquet.Metadata.MetadataDecoder.DecodeFileMetaData(
                fileBytes.AsSpan(fileBytes.Length - 8 - footerLen, footerLen));
            var lastChunk = metadata.RowGroups[^1].Columns[^1].MetaData!;
            long chunkStart = lastChunk.DictionaryPageOffset ?? lastChunk.DataPageOffset;
            int tamperOffset = checked((int)(chunkStart + lastChunk.TotalCompressedSize - 5));
            fileBytes[tamperOffset] ^= 0xFF; // flip all bits for maximum disruption
            File.WriteAllBytes(path, fileBytes);

            // Read with CRC validation — should throw (possibly wrapped in AggregateException
            // from Parallel.For).
            await using var readFile = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(readFile, ownsFile: false, new ParquetReadOptions
            {
                PageChecksumValidation = true,
            });

            var ex = await Assert.ThrowsAnyAsync<Exception>(
                () => reader.ReadRowGroupAsync(0).AsTask());

            // Unwrap AggregateException if thrown from Parallel.For.
            var inner = ex is AggregateException agg ? agg.InnerException! : ex;
            Assert.IsType<ParquetFormatException>(inner);
            Assert.Contains("CRC", inner.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task BackwardCompat_NoCrc_ValidationEnabled_Succeeds()
    {
        // Existing test files don't have CRC. With validation enabled,
        // reading should still succeed (CRC is optional — no CRC = no validation).
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("alltypes_plain.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions
        {
            PageChecksumValidation = true,
        });

        var result = await reader.ReadRowGroupAsync(0);
        Assert.Equal(8, result.Length);
    }

    [Fact]
    public async Task WriteWithoutCrc_ReadWithValidation_Succeeds()
    {
        // Files written without CRC should read fine even with validation enabled.
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("x", Apache.Arrow.Types.Int32Type.Default, false))
                .Build();

            var values = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 50)).Build();
            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { values }, 50);

            // Write WITHOUT CRC (default).
            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // Read WITH validation — should succeed (no CRC to validate).
            await using var readFile = new LocalRandomAccessFile(path);
            await using var reader = new ParquetFileReader(readFile, ownsFile: false, new ParquetReadOptions
            {
                PageChecksumValidation = true,
            });

            var result = await reader.ReadRowGroupAsync(0);
            Assert.Equal(50, result.Length);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task CrossValidation_ParquetSharpReadsFileWithCrc()
    {
        // Verify that files with CRC don't break third-party readers.
        var path = Path.GetTempFileName();
        try
        {
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, false))
                .Build();

            var values = new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
            var batch = new Apache.Arrow.RecordBatch(schema,
                new Apache.Arrow.IArrowArray[] { values }, 100);

            await using (var file = new LocalSequentialFile(path))
            await using (var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
            {
                PageChecksumEnabled = true,
            }))
            {
                await writer.WriteRowGroupAsync(batch);
            }

            // ParquetSharp should read the file without errors.
            using var reader = new ParquetSharp.ParquetFileReader(path);
            Assert.Equal(100, reader.FileMetaData.NumRows);

            using var rg = reader.RowGroup(0);
            using var col = rg.Column(0).LogicalReader<int>();
            var buffer = new int[100];
            col.ReadBatch(buffer);
            Assert.Equal(0, buffer[0]);
            Assert.Equal(99, buffer[99]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ───── Batched reads (#407) ─────
    //
    // A row group read in several batches goes through the page map, not ColumnChunkReader.ReadColumn:
    // side pages are decoded while the map is built, data pages one batch at a time. Both must be
    // checked, whether the map comes from the page headers or from the OffsetIndex.

    private const int Rows = 5000;

    public static TheoryData<bool, int?, long?> BatchedLayouts() => new()
    {
        { false, 10, null },
        { true, 10, null },
        { false, null, 2000 },
        { true, null, 2000 },
    };

    [Theory]
    [MemberData(nameof(BatchedLayouts))]
    public async Task Batched_TamperedDataPage_ThrowsOnValidation(bool writePageIndex, int? batchSize, long? maxBatchBytes)
    {
        string path = await WriteDoublesAsync(i => i * 1.0, dictionary: false, writePageIndex);
        var (bytes, chunk) = ReadFirstChunk(path);

        // Inside the last data page's values: PLAIN doubles, so without the check this reads as data.
        bytes[checked((int)(chunk.DataPageOffset + chunk.TotalCompressedSize - 5))] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        await AssertBatchedReadRefusesAsync(path, batchSize, maxBatchBytes);
        await AssertBatchedReadSucceedsAsync(path, batchSize, maxBatchBytes, validate: false);
    }

    [Theory]
    [MemberData(nameof(BatchedLayouts))]
    public async Task Batched_TamperedDictionaryPage_ThrowsOnValidation(bool writePageIndex, int? batchSize, long? maxBatchBytes)
    {
        string path = await WriteDoublesAsync(i => i % 250, dictionary: true, writePageIndex);
        var (bytes, chunk) = ReadFirstChunk(path);
        long dictionaryOffset = Assert.IsType<long>(chunk.DictionaryPageOffset);
        EngineeredWood.Parquet.Data.PageHeaderDecoder.Decode(bytes.AsSpan(checked((int)dictionaryOffset)), out int headerSize);

        // Inside the dictionary's PLAIN doubles.
        bytes[checked((int)dictionaryOffset + headerSize + 2)] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        await AssertBatchedReadRefusesAsync(path, batchSize, maxBatchBytes);
        await AssertBatchedReadSucceedsAsync(path, batchSize, maxBatchBytes, validate: false);
    }

    [Theory]
    [MemberData(nameof(BatchedLayouts))]
    public async Task Batched_Untampered_ReadsWithValidation(bool writePageIndex, int? batchSize, long? maxBatchBytes)
    {
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { false, true })
        {
            string path = await WriteDoublesAsync(i => i % 250, dictionary, writePageIndex, version);
            await AssertBatchedReadSucceedsAsync(path, batchSize, maxBatchBytes, validate: true);
        }
    }

    private static async Task<string> WriteDoublesAsync(
        Func<int, double> value, bool dictionary, bool writePageIndex, DataPageVersion version = DataPageVersion.V2)
    {
        string path = Path.Combine(Path.GetTempPath(), "ew-crc-" + Guid.NewGuid().ToString("N")[..8] + ".parquet");
        var values = new Apache.Arrow.DoubleArray.Builder().AppendRange(Enumerable.Range(0, Rows).Select(value)).Build();
        var batch = new Apache.Arrow.RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("x", Apache.Arrow.Types.DoubleType.Default, false)).Build(),
            [values], Rows);

        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, new ParquetWriteOptions
        {
            PageChecksumEnabled = true,
            DictionaryEnabled = dictionary,
            Compression = EngineeredWood.Compression.CompressionCodec.Uncompressed,
            DataPageVersion = version,
            DataPageSize = 1024,
            WritePageIndex = writePageIndex,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static (byte[] Bytes, EngineeredWood.Parquet.Metadata.ColumnMetaData Chunk) ReadFirstChunk(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BitConverter.ToInt32(bytes, bytes.Length - 8);
        var metadata = EngineeredWood.Parquet.Metadata.MetadataDecoder.DecodeFileMetaData(
            bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength));
        return (bytes, metadata.RowGroups[0].Columns[0].MetaData!);
    }

    private static async Task<int> ReadBatchedAsync(string path, int? batchSize, long? maxBatchBytes, bool validate)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, new ParquetReadOptions
        {
            PageChecksumValidation = validate,
            BatchSize = batchSize,
            MaxBatchByteSize = maxBatchBytes,
        });

        int batches = 0, rows = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
        {
            batches++;
            rows += batch.Length;
        }

        Assert.Equal(Rows, rows);
        Assert.True(batches > 1, "the read must take the batched path");
        return batches;
    }

    private static async Task AssertBatchedReadRefusesAsync(string path, int? batchSize, long? maxBatchBytes)
    {
        var ex = await Assert.ThrowsAsync<ParquetFormatException>(() => ReadBatchedAsync(path, batchSize, maxBatchBytes, validate: true));
        Assert.Contains("CRC", ex.Message);
    }

    private static Task AssertBatchedReadSucceedsAsync(string path, int? batchSize, long? maxBatchBytes, bool validate) =>
        ReadBatchedAsync(path, batchSize, maxBatchBytes, validate);
}
