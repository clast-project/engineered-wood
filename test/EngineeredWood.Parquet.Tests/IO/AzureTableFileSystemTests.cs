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
            await file.CompleteAsync();
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

    /// <summary>Bytes no two blocks share at the same offset, so a block out of place is caught.</summary>
    private static byte[] Payload(int length, int seed)
    {
        var payload = new byte[length];
        new Random(seed).NextBytes(payload);
        return payload;
    }

    private static void AssertSameBytes(byte[] expected, byte[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        Assert.True(expected.AsSpan().SequenceEqual(actual), "the bytes read back differ");
    }

    /// <summary>Writes <paramref name="payload"/> in irregular pieces, straddling block boundaries.</summary>
    private static async Task WriteInPiecesAsync(ISequentialFile file, byte[] payload)
    {
        int[] pieces = [1, 4093, 1 << 20, 3 * (1 << 20) + 7, 65536];
        int offset = 0;
        for (int i = 0; offset < payload.Length; i++)
        {
            int length = Math.Min(pieces[i % pieces.Length], payload.Length - offset);
            await file.WriteAsync(payload.AsMemory(offset, length));
            offset += length;
        }
    }

    /// <summary>
    /// A file of five 4 MiB blocks and a partial one comes back byte for byte whether its blocks staged in turn
    /// (no spares), mostly in turn (one spare), or several at once (the default): blocks may finish in any order,
    /// and the committed list puts them back in theirs (#713).
    /// </summary>
    [SkippableTheory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(AzureTableFileSystem.DefaultUploadBuffers)]
    public async Task Create_ManyBlocks_RoundTrips_HoweverManyStageAtOnce(int uploadBuffers)
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = new AzureTableFileSystem(_container!, "table-root", uploadBuffers: uploadBuffers);
        var payload = Payload(5 * AzureBlobSequentialFile.DefaultBlockSize + 12345, seed: uploadBuffers);

        await using (var file = await fs.CreateAsync("data/part-0.parquet"))
        {
            await WriteInPiecesAsync(file, payload);
            Assert.Equal(payload.Length, file.Position);
            await file.CompleteAsync();
        }

        AssertSameBytes(payload, await fs.ReadAllBytesAsync("data/part-0.parquet"));
    }

    /// <summary>Two writers open at once share one spare block, and both files come back whole.</summary>
    [SkippableFact]
    public async Task Create_TwoWritersSharingOneSpare_BothRoundTrip()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = new AzureTableFileSystem(_container!, "table-root", uploadBuffers: 1);
        var first = Payload(3 * AzureBlobSequentialFile.DefaultBlockSize + 1, seed: 1);
        var second = Payload(3 * AzureBlobSequentialFile.DefaultBlockSize + 2, seed: 2);

        await using (var a = await fs.CreateAsync("a.bin"))
        await using (var b = await fs.CreateAsync("b.bin"))
        {
            await Task.WhenAll(WriteInPiecesAsync(a, first), WriteInPiecesAsync(b, second));
            await Task.WhenAll(a.CompleteAsync().AsTask(), b.CompleteAsync().AsTask());
        }

        AssertSameBytes(first, await fs.ReadAllBytesAsync("a.bin"));
        AssertSameBytes(second, await fs.ReadAllBytesAsync("b.bin"));
    }

    /// <summary>A writer disposed with blocks staging in the background publishes nothing.</summary>
    [SkippableFact]
    public async Task Create_DisposedWithoutCompleting_PublishesNothing()
    {
        CloudEmulator.Require("Azurite on 127.0.0.1:10000", _azuriteAvailable, _unavailableReason);
        var fs = NewFs();

        await using (var file = await fs.CreateAsync("abandoned.bin"))
            await WriteInPiecesAsync(file, Payload(3 * AzureBlobSequentialFile.DefaultBlockSize, seed: 3));

        Assert.False(await fs.ExistsAsync("abandoned.bin"));
    }
}
