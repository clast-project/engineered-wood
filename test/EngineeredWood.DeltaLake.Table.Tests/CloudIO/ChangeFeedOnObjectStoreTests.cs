// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;
using EngineeredWood.IO.Azure;
using EngineeredWood.IO.Gcs;
using EngineeredWood.Tests.IO;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.DeltaLake.Table.Tests.CloudIO;

/// <summary>
/// The change feed on an object store, against the emulators the cloud-io CI job starts (#725). The feed opens
/// each file with the size its action records and asks nothing of the store first, so a missing object first
/// shows up as the footer read failing with the backend's own error (a 404 in Azure's, S3's or GCS's SDK
/// exception). Only then does the feed check whether the file exists: one VACUUM removed is skipped, one still
/// there is an error. Local tests cover the logic; these cover each backend's errors and existence checks.
/// </summary>
public abstract class ChangeFeedOnObjectStoreTests : IAsyncLifetime
{
    private const int Appends = 3;
    private const int RowsPerAppend = 50;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    /// <summary>What to start, for the skip or failure message.</summary>
    protected abstract string Emulator { get; }

    protected bool Available { get; set; }

    protected string? UnavailableReason { get; set; }

    /// <summary>A file system rooted at a fresh, empty prefix of the emulator's store.</summary>
    protected abstract ITableFileSystem NewFileSystem();

    public abstract Task InitializeAsync();

    public abstract Task DisposeAsync();

    /// <summary>Three appends to a change-feed table; returns the first append's data file path.</summary>
    private static async Task<string> CreateTableAsync(ITableFileSystem fs)
    {
        await using var table = await DeltaTable.CreateAsync(
            fs, Schema, configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        for (int a = 0; a < Appends; a++)
        {
            var ids = new Int64Array.Builder();
            var values = new StringArray.Builder();
            for (int i = 0; i < RowsPerAppend; i++)
            {
                ids.Append((long)a * RowsPerAppend + i);
                values.Append($"v{a}-{i}");
            }
            await table.WriteAsync([new RecordBatch(table.ArrowSchema, [ids.Build(), values.Build()], RowsPerAppend)]);
        }
        var add = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single();
        return DeltaPath.Decode(add.Path);
    }

    private static async Task<List<long>> InsertedIdsAsync(ITableFileSystem fs, int window)
    {
        await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { MaxConcurrentReads = window });
        var ids = new List<long>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
                       {
                           StartVersion = 0,
                           EndVersion = table.CurrentSnapshot.Version,
                       }))
        {
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var changeType = (StringArray)batch.Column(batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(CdfConfig.Insert, changeType.GetString(i));
                ids.Add(id.GetValue(i)!.Value);
            }
        }
        return ids;
    }

    [SkippableTheory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AVacuumedDataFile_IsSkipped(int window)
    {
        CloudEmulator.Require(Emulator, Available, UnavailableReason);
        var fs = NewFileSystem();
        string first = await CreateTableAsync(fs);
        Assert.Equal(Appends * RowsPerAppend, (await InsertedIdsAsync(fs, window)).Count);

        await fs.DeleteAsync(first);
        Assert.False(await fs.ExistsAsync(first));

        var ids = await InsertedIdsAsync(fs, window);
        Assert.Equal(Enumerable.Range(RowsPerAppend, (Appends - 1) * RowsPerAppend).Select(i => (long)i), ids);
    }

    [SkippableTheory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AnUnreadableDataFile_IsAnError(int window)
    {
        CloudEmulator.Require(Emulator, Available, UnavailableReason);
        var fs = NewFileSystem();
        string first = await CreateTableAsync(fs);

        // Still there, at its recorded size, but not Parquet: the footer read fails on a file that exists.
        long size;
        await using (var file = await fs.OpenReadAsync(first))
            size = await file.GetLengthAsync();
        await fs.WriteAllBytesAsync(first, new byte[size]);

        await Assert.ThrowsAnyAsync<Exception>(() => InsertedIdsAsync(fs, window));
    }
}

public sealed class ChangeFeedOnAzureTests : ChangeFeedOnObjectStoreTests
{
    private const string ConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private BlobContainerClient? _container;

    protected override string Emulator => "Azurite on 127.0.0.1:10000";

    protected override ITableFileSystem NewFileSystem() =>
        new AzureTableFileSystem(_container!, "table-" + Guid.NewGuid().ToString("N")[..8]);

    public override async Task InitializeAsync()
    {
        try
        {
            // Pinned: Azurite refuses the SDK's newest REST version (see AzureTableFileSystemTests).
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2024_11_04);
            options.Retry.MaxRetries = 0;
            options.Retry.NetworkTimeout = TimeSpan.FromSeconds(2);
            _container = new BlobServiceClient(ConnectionString, options)
                .GetBlobContainerClient("ew-cdf-" + Guid.NewGuid().ToString("N")[..8]);
            await _container.CreateIfNotExistsAsync();
            Available = true;
        }
        catch (Exception ex)
        {
            UnavailableReason = ex.Message;
        }
    }

    public override async Task DisposeAsync()
    {
        if (_container is not null && Available)
            await _container.DeleteIfExistsAsync();
    }
}

public sealed class ChangeFeedOnS3Tests : ChangeFeedOnObjectStoreTests
{
    private AmazonS3Client? _client;
    private string _bucket = "";

    protected override string Emulator => "MinIO on localhost:9000 (minioadmin/minioadmin)";

    protected override ITableFileSystem NewFileSystem() =>
        new S3TableFileSystem(_client!, _bucket, "table-" + Guid.NewGuid().ToString("N")[..8]);

    public override async Task InitializeAsync()
    {
        try
        {
            var config = new AmazonS3Config
            {
                ServiceURL = "http://localhost:9000",
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(2),
                MaxErrorRetry = 0,
            };
            _client = new AmazonS3Client(new BasicAWSCredentials("minioadmin", "minioadmin"), config);
            _bucket = "ew-cdf-" + Guid.NewGuid().ToString("N")[..8];
            await _client.PutBucketAsync(new PutBucketRequest { BucketName = _bucket });
            Available = true;
        }
        catch (Exception ex)
        {
            UnavailableReason = ex.Message;
        }
    }

    public override async Task DisposeAsync()
    {
        if (_client is not null && Available)
        {
            try
            {
                var listed = await _client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = _bucket });
                if (listed.S3Objects is { Count: > 0 })
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
        }
        _client?.Dispose();
    }
}

public sealed class ChangeFeedOnGcsTests : ChangeFeedOnObjectStoreTests
{
    private StorageClient? _client;
    private string _bucket = "";

    protected override string Emulator => "storage-testbench on localhost:4443";

    protected override ITableFileSystem NewFileSystem() =>
        new GcsTableFileSystem(_client!, _bucket, "table-" + Guid.NewGuid().ToString("N")[..8]);

    public override async Task InitializeAsync()
    {
        try
        {
            _client = new StorageClientBuilder
            {
                BaseUri = "http://localhost:4443/storage/v1/",
                UnauthenticatedAccess = true,
            }.Build();
            _bucket = "ew-cdf-" + Guid.NewGuid().ToString("N")[..8];
            await _client.CreateBucketAsync("ew-test-project", _bucket);
            Available = true;
        }
        catch (Exception ex)
        {
            UnavailableReason = ex.Message;
        }
    }

    public override async Task DisposeAsync()
    {
        if (_client is not null && Available)
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
        _client?.Dispose();
    }
}
