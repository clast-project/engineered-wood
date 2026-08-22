// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// Integration tests for <see cref="AzureTableFileSystem"/>.
/// Requires Azurite running on localhost:10000.
/// Tests are skipped automatically when Azurite is not available.
/// </summary>
public class AzureTableFileSystemTests : IAsyncLifetime
{
    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private BlobContainerClient? _container;
    private bool _azuriteAvailable;
    private string? _unavailableReason;

    private AzureTableFileSystem NewFs(string? root = "table-root") =>
        new(_container!, root);

    public async Task InitializeAsync()
    {
        try
        {
            // Fail fast when Azurite is absent so the skip path doesn't spend minutes
            // in the SDK's default connection retry/backoff.
            //
            // The service version is PINNED, and that is load-bearing rather than tidy: a default
            // BlobClientOptions negotiates the SDK's newest REST version, which Azurite answers with
            // 400 "The API version is not supported by Azurite". The probe below caught that, set
            // available=false, and every test returned early reporting PASSED — so these tests could
            // not have run honestly even on a machine with Azurite up. Measured 2026-08-07; it is the
            // second cause behind issue #79, independent of CI starting no emulator at all.
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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [SkippableFact]
    public async Task WriteAllBytes_ThenReadAllBytes_Roundtrips()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        await fs.WriteAllBytesAsync("_delta_log/00000000000000000000.json", Bytes("hello world"));

        byte[] read = await fs.ReadAllBytesAsync("_delta_log/00000000000000000000.json");
        Assert.Equal("hello world", Encoding.UTF8.GetString(read));
    }

    [SkippableFact]
    public async Task TryWriteAllBytes_CreatesOnce_AndPreservesExistingContent()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        Assert.True(await fs.TryWriteAllBytesAsync("commit.json", Bytes("winner")));
        Assert.False(await fs.TryWriteAllBytesAsync("commit.json", Bytes("loser")));

        Assert.Equal(
            "winner",
            Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("commit.json")));
    }

    [SkippableFact]
    public async Task Exists_ReflectsPresence()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        Assert.False(await fs.ExistsAsync("missing.json"));

        await fs.WriteAllBytesAsync("present.json", Bytes("x"));
        Assert.True(await fs.ExistsAsync("present.json"));
    }

    [SkippableFact]
    public async Task Delete_RemovesFile_AndMissingIsNoOp()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        await fs.WriteAllBytesAsync("doomed.json", Bytes("x"));
        Assert.True(await fs.ExistsAsync("doomed.json"));

        await fs.DeleteAsync("doomed.json");
        Assert.False(await fs.ExistsAsync("doomed.json"));

        // Deleting a missing blob must not throw.
        await fs.DeleteAsync("doomed.json");
        await fs.DeleteAsync("never-existed.json");
    }

    [SkippableFact]
    public async Task List_ReturnsRelativePaths_InLexicographicOrder()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        await fs.WriteAllBytesAsync("_delta_log/00000000000000000002.json", Bytes("2"));
        await fs.WriteAllBytesAsync("_delta_log/00000000000000000000.json", Bytes("0"));
        await fs.WriteAllBytesAsync("_delta_log/00000000000000000001.json", Bytes("1"));
        await fs.WriteAllBytesAsync("data/part-000.parquet", Bytes("data"));

        var logFiles = new List<TableFileInfo>();
        await foreach (var info in fs.ListAsync("_delta_log/"))
            logFiles.Add(info);

        Assert.Equal(3, logFiles.Count);
        Assert.Equal("_delta_log/00000000000000000000.json", logFiles[0].Path);
        Assert.Equal("_delta_log/00000000000000000001.json", logFiles[1].Path);
        Assert.Equal("_delta_log/00000000000000000002.json", logFiles[2].Path);
        Assert.Equal(1, logFiles[0].Size);
    }

    [SkippableFact]
    public async Task Create_NoOverwrite_ThrowsWhenExists_OverwriteSucceeds()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        await fs.WriteAllBytesAsync("file.bin", Bytes("original"));

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await using var _ = await fs.CreateAsync("file.bin", overwrite: false);
        });

        await using (var file = await fs.CreateAsync("file.bin", overwrite: true))
        {
            await file.WriteAsync(Bytes("replaced"));
        }

        Assert.Equal("replaced", Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("file.bin")));
    }

    [SkippableFact]
    public async Task OpenRead_ReadsBackWrittenContent()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        var payload = new byte[1000];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 256);
        await fs.WriteAllBytesAsync("blob.bin", payload);

        await using var file = await fs.OpenReadAsync("blob.bin");
        Assert.Equal(payload.Length, await file.GetLengthAsync());

        using var owner = await file.ReadAsync(new FileRange(100, 50));
        Assert.True(payload.AsSpan(100, 50).SequenceEqual(owner.Memory.Span));
    }
}
