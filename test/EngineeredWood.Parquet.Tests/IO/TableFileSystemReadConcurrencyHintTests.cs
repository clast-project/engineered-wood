// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Amazon.Runtime;
using Amazon.S3;
using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;
using EngineeredWood.IO.Azure;
using EngineeredWood.IO.Gcs;
using EngineeredWood.IO.Local;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.Parquet.Tests.IO;

/// <summary>
/// The object-store table file systems suggest a scan window (#632): their <c>maxConcurrency</c>, the same the
/// files they open are given and report. Nothing here touches the network: the clients connect lazily, and
/// opening a file sends no request.
/// </summary>
public class TableFileSystemReadConcurrencyHintTests
{
    private static S3TableFileSystem S3(int? maxConcurrency, CoalescingOptions? coalescing = null)
    {
        var client = new AmazonS3Client(
            new BasicAWSCredentials("key", "secret"),
            new AmazonS3Config { ServiceURL = "http://127.0.0.1:1", ForcePathStyle = true });
        return maxConcurrency is int m
            ? new S3TableFileSystem(client, "bucket", maxConcurrency: m, coalescingOptions: coalescing)
            : new S3TableFileSystem(client, "bucket", coalescingOptions: coalescing);
    }

    private static AzureTableFileSystem Azure(int? maxConcurrency, CoalescingOptions? coalescing = null)
    {
        var container = new BlobContainerClient(new Uri("http://127.0.0.1:1/devstoreaccount1/container"));
        return maxConcurrency is int m
            ? new AzureTableFileSystem(container, maxConcurrency: m, coalescingOptions: coalescing)
            : new AzureTableFileSystem(container, coalescingOptions: coalescing);
    }

    private static GcsTableFileSystem Gcs(int? maxConcurrency, CoalescingOptions? coalescing = null)
    {
        var client = StorageClient.CreateUnauthenticated();
        return maxConcurrency is int m
            ? new GcsTableFileSystem(client, "bucket", maxConcurrency: m, coalescingOptions: coalescing)
            : new GcsTableFileSystem(client, "bucket", coalescingOptions: coalescing);
    }

    public static TheoryData<string> Backends() => new() { "s3", "azure", "gcs" };

    private static ITableFileSystem Create(string backend, int? maxConcurrency, CoalescingOptions? coalescing = null) => backend switch
    {
        "s3" => S3(maxConcurrency, coalescing),
        "azure" => Azure(maxConcurrency, coalescing),
        "gcs" => Gcs(maxConcurrency, coalescing),
        _ => throw new ArgumentOutOfRangeException(nameof(backend)),
    };

    [Theory]
    [MemberData(nameof(Backends))]
    public async Task TheHint_IsTheMaxConcurrency_AndTheFilesItOpensShareIt(string backend)
    {
        foreach (var (configured, expected) in new (int?, int)[] { (null, 16), (5, 5) })
        {
            var fs = Create(backend, configured);
            Assert.Equal(expected, ((IReadConcurrencyHint)fs).PreferredConcurrentReads);

            using var file = await fs.OpenReadAsync("dir/file.parquet");
            Assert.Equal(expected, ((IReadConcurrencyHint)file).PreferredConcurrentReads);
        }
    }

    private static CoalescingOptions CoalescingOf(IRandomAccessFile file) => file switch
    {
        S3RandomAccessFile s3 => s3.CoalescingOptions,
        AzureBlobRandomAccessFile azure => azure.CoalescingOptions,
        GcsRandomAccessFile gcs => gcs.CoalescingOptions,
        _ => throw new InvalidOperationException(file.GetType().Name),
    };

    /// <summary>
    /// A table file system hands the files it opens the coalescing options it was given (#876), so a table scan's
    /// request size can be tuned; given none, a file keeps the defaults it has on its own.
    /// </summary>
    [Theory]
    [MemberData(nameof(Backends))]
    public async Task TheFilesItOpens_GetItsCoalescingOptions(string backend)
    {
        var options = new CoalescingOptions { MaxRequestBytes = 8 * 1024 * 1024, MaxGapBytes = 4096 };
        using (var file = await Create(backend, null, options).OpenReadAsync("dir/file.parquet"))
            Assert.Same(options, CoalescingOf(file));

        using (var file = await Create(backend, null).OpenReadAsync("dir/file.parquet"))
        {
            var defaults = new CoalescingOptions();
            Assert.Equal(defaults.MaxRequestBytes, CoalescingOf(file).MaxRequestBytes);
            Assert.Equal(defaults.MaxGapBytes, CoalescingOf(file).MaxGapBytes);
        }
    }

    [Theory]
    [MemberData(nameof(Backends))]
    public void ANonPositiveMaxConcurrency_IsRefused(string backend)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(backend, 0));
    }

    [Fact]
    public void TheLocalFileSystem_SuggestsNothing()
    {
        Assert.IsNotAssignableFrom<IReadConcurrencyHint>(new LocalTableFileSystem(Path.GetTempPath()));
    }
}
