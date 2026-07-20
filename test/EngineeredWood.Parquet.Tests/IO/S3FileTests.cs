// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// Integration tests for <see cref="S3SequentialFile"/> and <see cref="S3RandomAccessFile"/>.
/// Requires an S3-compatible endpoint (e.g. MinIO) reachable at the URL below.
/// Tests are skipped automatically when no endpoint is available.
/// </summary>
/// <remarks>
/// To run locally with MinIO:
/// <c>docker run -p 9000:9000 minio/minio server /data</c> (default creds minioadmin/minioadmin).
/// </remarks>
public class S3FileTests : IAsyncLifetime
{
    private const string ServiceUrl = "http://localhost:9000";

    private IAmazonS3? _client;
    private string _bucket = "";
    private bool _s3Available;

    private IAmazonS3 Client => _client ?? throw new InvalidOperationException("Client not initialized");

    public async Task InitializeAsync()
    {
        try
        {
            // Short timeout + no retries so the skip path is fast when no endpoint is up.
            var config = new AmazonS3Config
            {
                ServiceURL = ServiceUrl,
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(2),
                MaxErrorRetry = 0,
            };
            _client = new AmazonS3Client(new BasicAWSCredentials("minioadmin", "minioadmin"), config);

            _bucket = "ew-test-" + Guid.NewGuid().ToString("N")[..8];
            await _client.PutBucketAsync(new PutBucketRequest { BucketName = _bucket });
            _s3Available = true;
        }
        catch
        {
            _s3Available = false;
        }
    }

    public async Task DisposeAsync()
    {
        if (_client != null && _s3Available)
        {
            try
            {
                var listed = await _client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = _bucket });
                if (listed.S3Objects.Count > 0)
                {
                    await _client.DeleteObjectsAsync(new DeleteObjectsRequest
                    {
                        BucketName = _bucket,
                        Objects = listed.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList(),
                    });
                }
                await _client.DeleteBucketAsync(_bucket);
            }
            catch
            {
                // Best-effort cleanup.
            }
            _client.Dispose();
        }
    }

    [Fact]
    public async Task WriteAndRead_SimpleParquetFile()
    {
        if (!_s3Available) return;

        const string key = "test-simple.parquet";

        var values = new int[] { 10, 20, 30, 40, 50 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await using (var file = new S3SequentialFile(Client, _bucket, key))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new S3RandomAccessFile(Client, _bucket, key);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(5, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], col.GetValue(i));
    }

    [Fact]
    public async Task WriteAndRead_WithCompression()
    {
        if (!_s3Available) return;

        const string key = "test-compressed.parquet";

        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append($"value-{i}");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        var options = new ParquetWriteOptions { Compression = CompressionCodec.Snappy };

        await using (var file = new S3SequentialFile(Client, _bucket, key))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new S3RandomAccessFile(Client, _bucket, key);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (StringArray)readBatch.Column(0);
        Assert.Equal(100, col.Length);
        Assert.Equal("value-0", col.GetString(0));
        Assert.Equal("value-99", col.GetString(99));
    }

    [Fact]
    public async Task WriteAndRead_MultipartUpload_LargePayload()
    {
        if (!_s3Available) return;

        const string key = "test-multipart.bin";

        // Exceed the 8 MiB part size so the writer takes the multipart path
        // (>= one full part + a final part + CompleteMultipartUpload).
        var payload = new byte[10 * 1024 * 1024 + 12345];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i * 31 + 7);

        await using (var file = new S3SequentialFile(Client, _bucket, key))
        {
            // Write in odd-sized chunks to exercise buffer/part boundary handling.
            int offset = 0;
            while (offset < payload.Length)
            {
                int n = Math.Min(700_003, payload.Length - offset);
                await file.WriteAsync(payload.AsMemory(offset, n));
                offset += n;
            }
            await file.FlushAsync();
        }

        await using var readFile = new S3RandomAccessFile(Client, _bucket, key);
        Assert.Equal(payload.Length, await readFile.GetLengthAsync());

        // Spot-check ranges across part boundaries.
        foreach (var (off, len) in new[] { (0, 64), (8 * 1024 * 1024 - 16, 64), (payload.Length - 100, 100) })
        {
            using var owner = await readFile.ReadAsync(new FileRange(off, len));
            Assert.True(payload.AsSpan(off, len).SequenceEqual(owner.Memory.Span));
        }
    }

    [Fact]
    public async Task Position_TracksWrittenBytes()
    {
        if (!_s3Available) return;

        await using var file = new S3SequentialFile(Client, _bucket, "test-position.bin");

        Assert.Equal(0, file.Position);

        await file.WriteAsync(new byte[100]);
        Assert.Equal(100, file.Position);

        await file.WriteAsync(new byte[200]);
        Assert.Equal(300, file.Position);

        await file.FlushAsync();
    }

    [Fact]
    public async Task RandomAccess_ReadRanges_ReturnsCorrectBytes()
    {
        if (!_s3Available) return;

        const string key = "test-ranges.bin";

        var payload = new byte[4096];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 251);

        await using (var file = new S3SequentialFile(Client, _bucket, key))
        {
            await file.WriteAsync(payload);
            await file.FlushAsync();
        }

        await using var readFile = new S3RandomAccessFile(Client, _bucket, key);
        Assert.Equal(payload.Length, await readFile.GetLengthAsync());

        var ranges = new[]
        {
            new FileRange(0, 16),
            new FileRange(1000, 256),
            new FileRange(4096 - 8, 8),
        };

        var results = await readFile.ReadRangesAsync(ranges);
        Assert.Equal(ranges.Length, results.Count);

        for (int r = 0; r < ranges.Length; r++)
        {
            using var owner = results[r];
            var expected = payload.AsSpan((int)ranges[r].Offset, (int)ranges[r].Length);
            Assert.True(expected.SequenceEqual(owner.Memory.Span));
        }
    }
}
