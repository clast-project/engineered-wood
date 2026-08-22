// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using EngineeredWood.Compression;
using EngineeredWood.IO.Azure;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// Integration tests for <see cref="AzureBlobSequentialFile"/>.
/// Requires Azurite running on localhost:10000.
/// Tests are skipped automatically when Azurite is not available.
/// </summary>
public class AzureBlobSequentialFileTests : IAsyncLifetime
{
    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private BlobContainerClient? _container;
    private bool _azuriteAvailable;
    private string? _unavailableReason;

    private BlobContainerClient Container => _container ?? throw new InvalidOperationException("Container not initialized");

    public async Task InitializeAsync()
    {
        try
        {
            // Pin the service version and fail fast — see AzureTableFileSystemTests.InitializeAsync
            // for why an unpinned BlobClientOptions cannot talk to Azurite at all.
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2024_11_04);
            options.Retry.MaxRetries = 0;
            options.Retry.NetworkTimeout = TimeSpan.FromSeconds(2);

            var service = new BlobServiceClient(AzuriteConnectionString, options);
            _container = service.GetBlobContainerClient("ew-test-" + Guid.NewGuid().ToString("N")[..8]);
            await _container.CreateIfNotExistsAsync();
            _azuriteAvailable = true;
        }
        catch (Exception ex)
        {
            _azuriteAvailable = false;
            _unavailableReason = ex.Message;
        }
    }

    public async Task DisposeAsync()
    {
        if (_container != null && _azuriteAvailable)
            await _container.DeleteIfExistsAsync();
    }

    [SkippableFact]
    public async Task WriteAndRead_SimpleParquetFile()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);

        var blobName = "test-simple.parquet";
        var blockBlob = _container!.GetBlockBlobClient(blobName);

        // Write
        var values = new int[] { 10, 20, 30, 40, 50 };
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await using (var file = new AzureBlobSequentialFile(blockBlob))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Read back
        await using var readFile = new AzureBlobRandomAccessFile(
            Container.GetBlobClient(blobName));
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(5, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], col.GetValue(i));
    }

    [SkippableFact]
    public async Task WriteAndRead_SmallBlockSize_MultipleBlocks()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);

        var blockBlob = _container!.GetBlockBlobClient("test-small-blocks.parquet");

        // Use a tiny block size to force multiple block staging
        var values = Enumerable.Range(0, 1000).ToArray();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", Int32Type.Default, nullable: false))
            .Build();
        var batch = new RecordBatch(schema,
            [new Int32Array.Builder().AppendRange(values).Build()], values.Length);

        await using (var file = new AzureBlobSequentialFile(blockBlob, blockSize: 256))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Read back
        await using var readFile = new AzureBlobRandomAccessFile(
            Container.GetBlobClient("test-small-blocks.parquet"));
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.Equal(1000, metadata.NumRows);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (Int32Array)readBatch.Column(0);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], col.GetValue(i));
    }

    [SkippableFact]
    public async Task WriteAndRead_WithCompression()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);

        var blockBlob = _container!.GetBlockBlobClient("test-compressed.parquet");

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

        await using (var file = new AzureBlobSequentialFile(blockBlob))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // Read back
        await using var readFile = new AzureBlobRandomAccessFile(
            Container.GetBlobClient("test-compressed.parquet"));
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);

        var readBatch = await reader.ReadRowGroupAsync(0);
        var col = (StringArray)readBatch.Column(0);
        Assert.Equal(100, col.Length);
        Assert.Equal("value-0", col.GetString(0));
        Assert.Equal("value-99", col.GetString(99));
    }

    [SkippableFact]
    public async Task Position_TracksWrittenBytes()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);

        var blockBlob = _container!.GetBlockBlobClient("test-position.parquet");
        await using var file = new AzureBlobSequentialFile(blockBlob);

        Assert.Equal(0, file.Position);

        await file.WriteAsync(new byte[100]);
        Assert.Equal(100, file.Position);

        await file.WriteAsync(new byte[200]);
        Assert.Equal(300, file.Position);

        await file.FlushAsync();
    }

    /// <summary>
    /// Writing again after an explicit <c>FlushAsync</c> must not lose the second write.
    ///
    /// <para>Nothing forbids it — <c>WriteAsync</c> guards on disposed, not on committed — but the
    /// commit used to be conditional on <c>!_committed</c>, so the trailing block was staged and then
    /// never referenced by any block list. The blob read back as just the first write, with no error
    /// anywhere. Both halves matter: the length proves the second write landed, and reading it proves
    /// the re-commit kept the first block rather than replacing the list with only the new one.</para>
    /// </summary>
    [SkippableFact]
    public async Task WriteAfterFlush_IsStillCommitted()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);

        const string blobName = "test-write-after-flush.bin";
        var blockBlob = _container!.GetBlockBlobClient(blobName);

        await using (var file = new AzureBlobSequentialFile(blockBlob))
        {
            await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes("first"));
            await file.FlushAsync();
            await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes("second"));
        }

        var downloaded = await Container.GetBlobClient(blobName).DownloadContentAsync();
        Assert.Equal("firstsecond", downloaded.Value.Content.ToString());
    }
}
