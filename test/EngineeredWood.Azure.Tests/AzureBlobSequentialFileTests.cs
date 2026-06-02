// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;
using EngineeredWood.IO.Tests;

namespace EngineeredWood.Azure.Tests;

public class AzureBlobSequentialFileTests : SequentialFileTests
{
    private const string ConnectionStringEnv = "AZURE_STORAGE_CONNECTION_STRING";
    private const string DefaultConnectionString = "UseDevelopmentStorage=true";

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable(ConnectionStringEnv) ?? DefaultConnectionString;

    protected override async Task<(ISequentialFile File, Func<Task<byte[]>> ReadBack, Func<Task> Cleanup)>
        CreateFileAsync(string testId)
    {
        BlobContainerClient containerClient = new(ConnectionString, "sequential-file-tests");
        await containerClient.CreateIfNotExistsAsync();

        var blobName = $"{testId}_{Guid.NewGuid():N}";
        BlockBlobClient blockBlobClient = new(ConnectionString, containerClient.Name, blobName);

        AzureBlobSequentialFile file = new(blockBlobClient);

        async Task<byte[]> ReadBack()
        {
            using MemoryStream stream = new();
            await blockBlobClient.DownloadToAsync(stream);
            return stream.ToArray();
        }

        async Task Cleanup()
        {
            await blockBlobClient.DeleteIfExistsAsync();
        }

        return (file, ReadBack, Cleanup);
    }
}
