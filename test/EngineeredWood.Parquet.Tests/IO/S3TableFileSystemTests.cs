// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EngineeredWood.IO;
using EngineeredWood.IO.Aws;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// Integration tests for <see cref="S3TableFileSystem"/>.
/// Requires an S3-compatible endpoint (e.g. MinIO) reachable at the URL below.
/// Tests are skipped automatically when no endpoint is available.
/// </summary>
/// <remarks>
/// The rename-race test relies on conditional writes (<c>If-None-Match: *</c>), which need
/// an endpoint that supports them (AWS S3, or MinIO/LocalStack from 2024 onward).
/// </remarks>
public class S3TableFileSystemTests : IAsyncLifetime
{
    private const string ServiceUrl = "http://localhost:9000";

    private IAmazonS3? _client;
    private string _bucket = "";
    private bool _s3Available;

    private S3TableFileSystem NewFs(string? root = "table-root") =>
        new(_client!, _bucket, root);

    public async Task InitializeAsync()
    {
        try
        {
            var config = new AmazonS3Config
            {
                ServiceURL = ServiceUrl,
                ForcePathStyle = true,
                Timeout = TimeSpan.FromSeconds(2),
                MaxErrorRetry = 0,
            };
            _client = new AmazonS3Client(new BasicAWSCredentials("minioadmin", "minioadmin"), config);

            _bucket = "ew-test-" + Guid.NewGuid().ToString("N")[..8];
            await _client.PutBucketAsync(new PutBucketRequest { BucketName = _bucket });
            _s3Available = true;
        }
        catch
        {
            _s3Available = false;
        }
    }

    public async Task DisposeAsync()
    {
        if (_client != null && _s3Available)
        {
            try
            {
                var listed = await _client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = _bucket });
                if (listed.S3Objects.Count > 0)
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
            _client.Dispose();
        }
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task WriteAllBytes_ThenReadAllBytes_Roundtrips()
    {
        if (!_s3Available) return;
        var fs = NewFs();

        await fs.WriteAllBytesAsync("_delta_log/00000000000000000000.json", Bytes("hello world"));

        byte[] read = await fs.ReadAllBytesAsync("_delta_log/00000000000000000000.json");
        Assert.Equal("hello world", Encoding.UTF8.GetString(read));
    }

    [Fact]
    public async Task Exists_ReflectsPresence()
    {
        if (!_s3Available) return;
        var fs = NewFs();

        Assert.False(await fs.ExistsAsync("missing.json"));

        await fs.WriteAllBytesAsync("present.json", Bytes("x"));
        Assert.True(await fs.ExistsAsync("present.json"));
    }

    [Fact]
    public async Task Delete_RemovesFile_AndMissingIsNoOp()
    {
        if (!_s3Available) return;
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
        if (!_s3Available) return;
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
        if (!_s3Available) return;
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
        if (!_s3Available) return;
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
        if (!_s3Available) return;
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
        if (!_s3Available) return;
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
