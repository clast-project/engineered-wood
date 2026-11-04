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
    private static S3TableFileSystem S3(int? maxConcurrency)
    {
        var client = new AmazonS3Client(
            new BasicAWSCredentials("key", "secret"),
            new AmazonS3Config { ServiceURL = "http://127.0.0.1:1", ForcePathStyle = true });
        return maxConcurrency is int m
            ? new S3TableFileSystem(client, "bucket", maxConcurrency: m)
            : new S3TableFileSystem(client, "bucket");
    }

    private static AzureTableFileSystem Azure(int? maxConcurrency)
    {
        var container = new BlobContainerClient(new Uri("http://127.0.0.1:1/devstoreaccount1/container"));
        return maxConcurrency is int m
            ? new AzureTableFileSystem(container, maxConcurrency: m)
            : new AzureTableFileSystem(container);
    }

    private static GcsTableFileSystem Gcs(int? maxConcurrency)
    {
        var client = StorageClient.CreateUnauthenticated();
        return maxConcurrency is int m
            ? new GcsTableFileSystem(client, "bucket", maxConcurrency: m)
            : new GcsTableFileSystem(client, "bucket");
    }

    public static TheoryData<string> Backends() => new() { "s3", "azure", "gcs" };

    private static ITableFileSystem Create(string backend, int? maxConcurrency) => backend switch
    {
        "s3" => S3(maxConcurrency),
        "azure" => Azure(maxConcurrency),
        "gcs" => Gcs(maxConcurrency),
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
