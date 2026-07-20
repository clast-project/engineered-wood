// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.IO;
using EngineeredWood.IO.Gcs;
using Google.Cloud.Storage.V1;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// Integration tests for <see cref="GcsTableFileSystem"/>.
/// Requires a GCS emulator (e.g. fake-gcs-server) reachable at the URL below.
/// Tests are skipped automatically when no emulator is available.
/// </summary>
/// <remarks>
/// To run locally:
/// <c>docker run -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443</c>
/// </remarks>
public class GcsTableFileSystemTests : IAsyncLifetime
{
    private const string EmulatorBaseUri = "http://localhost:4443/storage/v1/";
    private const string ProjectId = "ew-test-project";

    private StorageClient? _client;
    private string _bucket = "";
    private bool _emulatorAvailable;

    private GcsTableFileSystem NewFs(string? root = "table-root") =>
        new(_client!, _bucket, root);

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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task WriteAllBytes_ThenReadAllBytes_Roundtrips()
    {
        if (!_emulatorAvailable) return;
        var fs = NewFs();

        await fs.WriteAllBytesAsync("_delta_log/00000000000000000000.json", Bytes("hello world"));

        byte[] read = await fs.ReadAllBytesAsync("_delta_log/00000000000000000000.json");
        Assert.Equal("hello world", Encoding.UTF8.GetString(read));
    }

    [Fact]
    public async Task Exists_ReflectsPresence()
    {
        if (!_emulatorAvailable) return;
        var fs = NewFs();

        Assert.False(await fs.ExistsAsync("missing.json"));

        await fs.WriteAllBytesAsync("present.json", Bytes("x"));
        Assert.True(await fs.ExistsAsync("present.json"));
    }

    [Fact]
    public async Task Delete_RemovesFile_AndMissingIsNoOp()
    {
        if (!_emulatorAvailable) return;
        var fs = NewFs();

        await fs.WriteAllBytesAsync("doomed.json", Bytes("x"));
        Assert.True(await fs.ExistsAsync("doomed.json"));

        await fs.DeleteAsync("doomed.json");
        Assert.False(await fs.ExistsAsync("doomed.json"));

        // Deleting a missing object must not throw.
        await fs.DeleteAsync("doomed.json");
        await fs.DeleteAsync("never-existed.json");
    }

    [Fact]
    public async Task List_ReturnsRelativePaths_InLexicographicOrder()
    {
        if (!_emulatorAvailable) return;
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

    [Fact]
    public async Task Rename_ToFreshTarget_MovesData_AndReturnsTrue()
    {
        if (!_emulatorAvailable) return;
        var fs = NewFs();

        await fs.WriteAllBytesAsync("_delta_log/_commit_abc.tmp", Bytes("commit payload"));

        bool ok = await fs.RenameAsync("_delta_log/_commit_abc.tmp", "_delta_log/00000000000000000005.json");

        Assert.True(ok);
        Assert.False(await fs.ExistsAsync("_delta_log/_commit_abc.tmp"));
        Assert.True(await fs.ExistsAsync("_delta_log/00000000000000000005.json"));
        Assert.Equal("commit payload",
            Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("_delta_log/00000000000000000005.json")));
    }

    [Fact]
    public async Task Rename_TargetExists_ReturnsFalse_AndLeavesBothObjectsIntact()
    {
        if (!_emulatorAvailable) return;
        var fs = NewFs();

        // Simulates two writers racing to commit version 5: the target already exists.
        await fs.WriteAllBytesAsync("_delta_log/00000000000000000005.json", Bytes("winner"));
        await fs.WriteAllBytesAsync("_delta_log/_commit_loser.tmp", Bytes("loser"));

        bool ok = await fs.RenameAsync("_delta_log/_commit_loser.tmp", "_delta_log/00000000000000000005.json");

        Assert.False(ok);
        // The committed target is untouched, and the loser's temp file is left for cleanup.
        Assert.Equal("winner",
            Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("_delta_log/00000000000000000005.json")));
        Assert.True(await fs.ExistsAsync("_delta_log/_commit_loser.tmp"));
    }

    [Fact]
    public async Task Create_NoOverwrite_ThrowsWhenExists_OverwriteSucceeds()
    {
        if (!_emulatorAvailable) return;
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

    [Fact]
    public async Task OpenRead_ReadsBackWrittenContent()
    {
        if (!_emulatorAvailable) return;
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
