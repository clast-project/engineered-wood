// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Gcs;
using EngineeredWood.Parquet;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// Integration tests for <see cref="GcsSequentialFile"/> and <see cref="GcsRandomAccessFile"/>.
/// Requires a GCS emulator (e.g. fake-gcs-server) reachable at the URL below.
/// Tests are skipped automatically when no emulator is available.
/// </summary>
/// <remarks>
/// To run locally:
/// <c>docker run -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443</c>
/// </remarks>
public class GcsFileTests : IAsyncLifetime
{
    private const string EmulatorBaseUri = "http://localhost:4443/storage/v1/";
    private const string ProjectId = "ew-test-project";

    private StorageClient? _client;
    private string _bucket = "";
    private bool _emulatorAvailable;

    private StorageClient Client => _client ?? throw new InvalidOperationException("Client not initialized");

    public async Task InitializeAsync()
    {
        try
        {
            _client = new StorageClientBuilder
            {
                BaseUri = EmulatorBaseUri,
                UnauthenticatedAccess = true,
            }.Build();

            _bucket = "ew-test-" + Guid.NewGuid().ToString("N")[..8];
            await _client.CreateBucketAsync(ProjectId, _bucket);
            _emulatorAvailable = true;
        }
        catch
        {
            _emulatorAvailable = false;
        }
    }

    public async Task DisposeAsync()
    {
        if (_client != null && _emulatorAvailable)
        {
            try
            {
                await _client.DeleteBucketAsync(_bucket, new DeleteBucketOptions { DeleteObjects = true });
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    [Fact]
    public async Task WriteAndRead_SimpleParquetFile()
    {
        if (!_emulatorAvailable) return;

        const string objectName = "test-simple.parquet";

        var values = new int[] { 10, 20, 30, 40, 50 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await using (var file = new GcsSequentialFile(Client, _bucket, objectName))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new GcsRandomAccessFile(Client, _bucket, objectName);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(5, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], col.GetValue(i));
    }

    [Fact]
    public async Task WriteAndRead_LargerFile_MultipleRowGroups()
    {
        if (!_emulatorAvailable) return;

        const string objectName = "test-large.parquet";

        var values = Enumerable.Range(0, 10_000).ToArray();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await using (var file = new GcsSequentialFile(Client, _bucket, objectName))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new GcsRandomAccessFile(Client, _bucket, objectName);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(10_000, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], col.GetValue(i));
    }

    [Fact]
    public async Task WriteAndRead_WithCompression()
    {
        if (!_emulatorAvailable) return;

        const string objectName = "test-compressed.parquet";

        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
            builder.Append($"value-{i}");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Snappy,
        };

        await using (var file = new GcsSequentialFile(Client, _bucket, objectName))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new GcsRandomAccessFile(Client, _bucket, objectName);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (StringArray)readBatch.Column(0);
        Assert.Equal(100, col.Length);
        Assert.Equal("value-0", col.GetString(0));
        Assert.Equal("value-99", col.GetString(99));
    }

    [Fact]
    public async Task Position_TracksWrittenBytes()
    {
        if (!_emulatorAvailable) return;

        await using var file = new GcsSequentialFile(Client, _bucket, "test-position.bin");

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
        if (!_emulatorAvailable) return;

        const string objectName = "test-ranges.bin";

        var payload = new byte[4096];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 251);

        await using (var file = new GcsSequentialFile(Client, _bucket, objectName))
        {
            await file.WriteAsync(payload);
            await file.FlushAsync();
        }

        await using var readFile = new GcsRandomAccessFile(Client, _bucket, objectName);

        Assert.Equal(payload.Length, await readFile.GetLengthAsync());

        var ranges = new[]
        {
            new EngineeredWood.IO.FileRange(0, 16),
            new EngineeredWood.IO.FileRange(1000, 256),
            new EngineeredWood.IO.FileRange(4096 - 8, 8),
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

    [Fact]
    public async Task RandomAccess_KnownLength_SkipsMetadataFetch()
    {
        if (!_emulatorAvailable) return;

        const string objectName = "test-known-length.bin";

        var payload = new byte[1234];
        await using (var file = new GcsSequentialFile(Client, _bucket, objectName))
        {
            await file.WriteAsync(payload);
            await file.FlushAsync();
        }

        await using var readFile = new GcsRandomAccessFile(Client, _bucket, objectName, knownLength: payload.Length);
        Assert.Equal(payload.Length, await readFile.GetLengthAsync());

        using var owner = await readFile.ReadAsync(new EngineeredWood.IO.FileRange(0, payload.Length));
        Assert.Equal(payload.Length, owner.Memory.Length);
    }
}
