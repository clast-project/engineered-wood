// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Compression;

/// <summary>
/// Gzip pages as .NET's GZipStream writes them. From .NET 9 on it is zlib-ng, which grows
/// incompressible input by up to 1/8 at Fastest, the writer's default Gzip level, and writes nothing
/// at all for an empty input.
/// </summary>
public class GzipPageTests : IDisposable
{
    private readonly string _tempDir;

    public GzipPageTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-gzip-pages-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // Default page sizes: 20,000 random INT32s are an 80 KB page, and 20,000 FLBA(16)s a 320 KB one.
    // Both outgrew the compression buffer, sized at the input plus 64 bytes, and the write threw.
    [Theory]
    [InlineData("int32")]
    [InlineData("flba16")]
    public async Task IncompressibleChunk_WritesAndRoundTrips(string kind)
    {
        const int rows = 100_000;
        var rng = new Random(508);
        IArrowArray array;
        if (kind == "int32")
        {
            var builder = new Int32Array.Builder();
            for (int i = 0; i < rows; i++)
                builder.Append(rng.Next(int.MinValue, int.MaxValue));
            array = builder.Build();
        }
        else
        {
            var bytes = new byte[rows * 16];
            rng.NextBytes(bytes);
            array = new FixedSizeBinaryArray(new ArrayData(
                new FixedSizeBinaryType(16), rows, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        }

        var (read, _) = await WriteAndRead(array, nullable: false);

        Assert.Equal(
            array.Data.Buffers[1].Span.ToArray(),
            read.Data.Buffers[1].Span.Slice(0, array.Data.Buffers[1].Length).ToArray());
    }

    // An all-null V2 page has an empty values section, which is still compressed: a 0-byte payload
    // declared compressed is not a stream. zlib-ng wrote nothing for it.
    [Fact]
    public async Task AllNullChunk_HoldsAnEmptyGzipStream_AndRoundTrips()
    {
        const int rows = 1_000;
        var builder = new Int32Array.Builder();
        for (int i = 0; i < rows; i++)
            builder.AppendNull();

        var (read, chunk) = await WriteAndRead(builder.Build(), nullable: true);

        Assert.Equal(rows, read.NullCount);
        byte[] emptyGzip =
        [
            0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF,
            0x03, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        Assert.True(
            chunk.AsSpan().IndexOf(emptyGzip) >= 0,
            "the page's empty values section is not an empty Gzip stream");
    }

    private async Task<(IArrowArray Read, byte[] Chunk)> WriteAndRead(IArrowArray array, bool nullable)
    {
        var path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", array.Data.DataType, nullable))
            .Build();
        var options = ParquetWriteOptions.Default with
        {
            DataPageVersion = DataPageVersion.V2,
            DictionaryEnabled = false,
            Compression = CompressionCodec.Gzip,
        };

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], array.Length));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var column = (await reader.ReadMetadataAsync()).RowGroups[0].Columns[0].MetaData!;
        var batch = await reader.ReadRowGroupAsync(0);

        long start = column.DictionaryPageOffset ?? column.DataPageOffset;
        var chunk = File.ReadAllBytes(path).AsSpan((int)start, (int)column.TotalCompressedSize).ToArray();
        return (batch.Column(0), chunk);
    }
}
