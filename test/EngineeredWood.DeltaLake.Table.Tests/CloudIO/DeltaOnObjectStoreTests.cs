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
/// Delta behaviour that depends on each backend's own semantics, against the emulators the cloud-io CI job starts:
/// its errors and existence checks, and its ETags. Local tests cover the logic.
/// </summary>
/// <remarks>
/// <para><b>Replaced log (#750).</b> A handle held across a drop and re-create is refused by the identity of its base
/// commit as each backend lists it: Azure's ETag, S3's (the content's MD5 on MinIO), GCS's generation. The local
/// file system has none of these, so only here is the whole check run against real tags, and an unchanged log shown
/// to commit with them, which a tag reported differently by two listings would break.</para>
///
/// <para><b>The change feed (#725).</b> The feed opens
/// each file with the size its action records and asks nothing of the store first, so a missing object first
/// shows up as the footer read failing with the backend's own error (a 404 in Azure's, S3's or GCS's SDK
/// exception). Only then does the feed check whether the file exists: one VACUUM removed is skipped, one still
/// there is an error.</para>
/// </remarks>
public abstract class DeltaOnObjectStoreTests : IAsyncLifetime
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

    // ── #750: a handle held across a drop and re-create ────────────────────────────────────────────────

    private static RecordBatch Rows(long start, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (long i = start; i < start + count; i++)
        {
            ids.Append(i);
            values.Append($"v{i}");
        }
        return new RecordBatch(Schema, [ids.Build(), values.Build()], count);
    }

    /// <summary>Creates a table and appends <paramref name="appends"/> one-row commits from <paramref name="start"/>.</summary>
    private static async Task CreateWithAppendsAsync(ITableFileSystem fs, int appends, long start)
    {
        await using var table = await DeltaTable.CreateAsync(fs, Schema);
        for (int a = 0; a < appends; a++)
            await table.WriteAsync([Rows(start + a, 1)]);
    }

    /// <summary>Deletes every object under the table's prefix, as a DROP of an external location leaves it.</summary>
    private static async Task DropAsync(ITableFileSystem fs)
    {
        var paths = new List<string>();
        await foreach (var file in fs.ListAsync(""))
            paths.Add(file.Path);
        foreach (string path in paths)
            await fs.DeleteAsync(path);
    }

    private static async Task<List<long>> VersionsAsync(ITableFileSystem fs)
    {
        var versions = new List<long>();
        await foreach (long v in new TransactionLog(fs).ListVersionsAsync())
            versions.Add(v);
        return versions;
    }

    /// <summary>The issue's three shapes, against the handle's base at v3: shorter, the same, and longer.</summary>
    [SkippableTheory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task AStaleHandle_CannotCommitIntoARecreatedTable(int newAppends)
    {
        CloudEmulator.Require(Emulator, Available, UnavailableReason);
        var fs = NewFileSystem();
        await CreateWithAppendsAsync(fs, 3, start: 1);                      // v3
        await using var stale = await DeltaTable.OpenAsync(fs);

        await DropAsync(fs);
        await CreateWithAppendsAsync(fs, newAppends, start: 100);           // v<newAppends>

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await stale.WriteAsync([Rows(9, 1)]));
        Assert.Equal(DeltaErrorCodes.TransactionLogReplaced, ex.ErrorCode);
        Assert.Equal(Enumerable.Range(0, newAppends + 1).Select(v => (long)v), await VersionsAsync(fs));
    }

    /// <summary>
    /// The other half: with real tags, a log nobody replaced still commits, from a handle that is current, from
    /// one another writer has moved past (a rebase), and from one whose base is still the latest.
    /// </summary>
    [SkippableFact]
    public async Task AnUnchangedLog_StillCommits()
    {
        CloudEmulator.Require(Emulator, Available, UnavailableReason);
        var fs = NewFileSystem();
        await CreateWithAppendsAsync(fs, 1, start: 1);                      // v1
        await using var a = await DeltaTable.OpenAsync(fs);
        await using var b = await DeltaTable.OpenAsync(fs);

        Assert.Equal(2, await a.WriteAsync([Rows(2, 1)]));
        Assert.Equal(3, await b.WriteAsync([Rows(3, 1)]));                  // rebased past a's commit
        Assert.Equal(4, await a.WriteAsync([Rows(4, 1)]));                  // a's base: its own v2, then refreshed
        Assert.Equal(5, await b.WriteAsync([Rows(5, 1)]));
        Assert.Equal([0L, 1, 2, 3, 4, 5], await VersionsAsync(fs));
    }
}

public sealed class DeltaOnAzureTests : DeltaOnObjectStoreTests
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

public sealed class DeltaOnS3Tests : DeltaOnObjectStoreTests
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

public sealed class DeltaOnGcsTests : DeltaOnObjectStoreTests
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
