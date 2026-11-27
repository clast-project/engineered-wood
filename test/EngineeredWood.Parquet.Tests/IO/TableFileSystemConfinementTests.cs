// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
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
/// Every root-relative <see cref="ITableFileSystem"/> refuses a path that names something outside its root, for every
/// operation (#962). The object stores are pointed at an unreachable endpoint: the refusal comes before any request,
/// so an <see cref="ArgumentException"/> here is the check and not a network failure.
/// </summary>
public sealed class TableFileSystemConfinementTests : IDisposable
{
    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    private readonly string _base = Path.Combine(Path.GetTempPath(), $"ew-confine-{Guid.NewGuid():N}");
    private readonly string _root;
    private readonly string _outside;

    public TableFileSystemConfinementTests()
    {
        _root = Path.Combine(_base, "root");
        Directory.CreateDirectory(Path.Combine(_root, "sub"));
        _outside = Path.Combine(_base, "outside.txt");
        File.WriteAllText(_outside, "OUT");
        File.WriteAllText(Path.Combine(_root, "inside.txt"), "IN");
    }

    public void Dispose()
    {
        if (Directory.Exists(_base))
            Directory.Delete(_base, recursive: true);
    }

    /// <summary>Paths every backend refuses: they climb out, or are rooted.</summary>
    public static TheoryData<string> EscapingPaths => new()
    {
        "../outside.txt",
        "sub/../../outside.txt",
        @"..\outside.txt",
        "../root/inside.txt", // climbs above the root, even though it comes back down
        "../root-other/x",    // a sibling whose name starts with the root's
        "/outside.txt",
        @"\outside.txt",
        "s3://bucket/outside.txt", // an absolute URI: a key under the root on the object stores before #962
        "file:///outside.txt",
    };

    [Theory]
    [MemberData(nameof(EscapingPaths))]
    public async Task EscapingPath_IsRefused_ByEveryBackend(string path)
    {
        foreach (string backend in new[] { "local", "s3", "azure", "gcs" })
        {
            // Outside Windows a local file name may hold a backslash, so there it is a name, not a separator.
            if (backend == "local" && !IsWindows && path.IndexOf('\\') >= 0)
                continue;
            await AssertRefusedAsync(Create(backend), path);
        }
        AssertOutsideUntouched();
    }

    /// <summary>In an object store a dot segment is part of the key, which the Azure SDK collapses before sending
    /// (MEASURED: <c>t/a/../b</c> requests <c>t/b</c>) and engines disagree on, so it is refused even when it would
    /// stay inside (#963).</summary>
    [Theory]
    [InlineData("s3", "sub/../inside.txt")]
    [InlineData("s3", "./inside.txt")]
    [InlineData("azure", "sub/../inside.txt")]
    [InlineData("azure", "./inside.txt")]
    [InlineData("gcs", "sub/./inside.txt")]
    [InlineData("gcs", "sub/..")]
    public async Task DotSegment_IsRefused_ByTheObjectStores(string backend, string path) =>
        await AssertRefusedAsync(Create(backend), path);

    [Fact]
    public async Task Local_AbsolutePathOutsideTheRoot_IsRefused()
    {
        var fs = Create("local");
        await AssertRefusedAsync(fs, _outside);
        await AssertRefusedAsync(fs, _outside.Replace('\\', '/'));
        AssertOutsideUntouched();
    }

    /// <summary>The Windows spellings that reached outside the root before #962, MEASURED on Windows 11: a
    /// drive-relative path (resolved against that drive's working directory), UNC and device paths, and <c>NUL</c>,
    /// which <c>Path.GetFullPath</c> maps to <c>\\.\NUL</c>.</summary>
    [SkippableFact]
    public async Task Local_WindowsRootedSpellings_AreRefused()
    {
        Skip.IfNot(IsWindows, "drive letters, UNC and device paths are Windows path syntax");
        var fs = Create("local");
        string drive = _base.Substring(0, 2);
        string noDrive = _base.Substring(2);
        string[] paths =
        [
            drive + "outside.txt",
            drive + noDrive + @"\outside.txt",
            noDrive + @"\outside.txt",
            @"\\?\" + _outside,
            @"\\.\" + _outside,
            @"\\localhost\" + drive[0] + "$" + noDrive + @"\outside.txt",
            "//localhost/" + drive[0] + "$" + noDrive.Replace('\\', '/') + "/outside.txt",
            "NUL",
        ];
        foreach (string path in paths)
            await AssertRefusedAsync(fs, path);
        AssertOutsideUntouched();
    }

    /// <summary>A local directory collapses dot segments that stay inside, as <c>file</c> locations do (#963).</summary>
    [Theory]
    [InlineData("./inside.txt")]
    [InlineData("sub/../inside.txt")]
    [InlineData("sub/./../inside.txt")]
    [InlineData("%2E%2E/inside.txt")] // a literal directory name, never decoded here
    public async Task Local_DotSegmentsInside_Collapse(string path)
    {
        var fs = Create("local");
        if (path.StartsWith("%", StringComparison.Ordinal))
        {
            Assert.False(await fs.ExistsAsync(path));
            return;
        }

        Assert.Equal("IN", System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(path)));
    }

    /// <summary>A listing of the root spelled with dot segments lists the root, not the root's parent, which is where
    /// the directory of <c>"sub/.."</c> taken as a file name would be.</summary>
    [Theory]
    [InlineData(".")]
    [InlineData("sub/..")]
    public async Task Local_ListingTheRootByDotSegments_StaysInside(string prefix)
    {
        Directory.CreateDirectory(_root + "2");
        File.WriteAllText(Path.Combine(_root + "2", "sibling.txt"), "S");
        var paths = new List<string>();
        await foreach (var file in Create("local").ListAsync(prefix))
            paths.Add(file.Path);
        Assert.Equal(new[] { "inside.txt" }, paths);
    }

    /// <summary>A listing whose last segment is a dot segment lists that directory, not its siblings whose names start
    /// with its name.</summary>
    [Theory]
    [InlineData("sub/.")]
    [InlineData("sub/x/..")]
    [InlineData("sub/./")]
    [InlineData("sub/.//")]
    [InlineData("sub/x/../")]
    [InlineData("sub/")]
    public async Task Local_ListingADirectoryByDotSegments_ListsThatDirectory(string prefix)
    {
        File.WriteAllText(Path.Combine(_root, "sub", "a.txt"), "A");
        Directory.CreateDirectory(Path.Combine(_root, "sub2"));
        File.WriteAllText(Path.Combine(_root, "sub2", "b.txt"), "B");
        var paths = new List<string>();
        await foreach (var file in Create("local").ListAsync(prefix))
            paths.Add(file.Path);
        Assert.Equal(new[] { "sub/a.txt" }, paths);
    }

    private ITableFileSystem Create(string backend) => backend switch
    {
        "local" => new LocalTableFileSystem(_root),
        "s3" => new S3TableFileSystem(
            new AmazonS3Client(new BasicAWSCredentials("k", "s"),
                new AmazonS3Config { ServiceURL = "http://127.0.0.1:1", ForcePathStyle = true, MaxErrorRetry = 0 }),
            "bucket", "tables/t"),
        "azure" => new AzureTableFileSystem(
            new BlobContainerClient(new Uri("http://127.0.0.1:1/acct/container"),
                new BlobClientOptions { Retry = { MaxRetries = 0 } }),
            "tables/t"),
        "gcs" => new GcsTableFileSystem(
            new StorageClientBuilder { BaseUri = "http://127.0.0.1:1/storage/v1/", UnauthenticatedAccess = true }.Build(),
            "bucket", "tables/t"),
        _ => throw new ArgumentOutOfRangeException(nameof(backend)),
    };

    private static async Task AssertRefusedAsync(ITableFileSystem fs, string path)
    {
        string because = $"{fs.GetType().Name} with '{path}'";
        await AssertArgumentAsync(because, async () => (await fs.OpenReadAsync(path)).Dispose());
        await AssertArgumentAsync(because, async () => await (await fs.CreateAsync(path, overwrite: true)).DisposeAsync());
        await AssertArgumentAsync(because, async () => await fs.DeleteAsync(path));
        await AssertArgumentAsync(because, async () => await fs.ExistsAsync(path));
        await AssertArgumentAsync(because, async () => await fs.ReadAllBytesAsync(path));
        await AssertArgumentAsync(because, async () => await fs.TryWriteAllBytesAsync(path, new byte[] { 1 }));
        await AssertArgumentAsync(because, async () => await fs.WriteAllBytesAsync(path, new byte[] { 1 }));
        await AssertArgumentAsync(because, async () =>
        {
            await foreach (var _ in fs.ListAsync(path)) { }
        });
    }

    private static async Task AssertArgumentAsync(string because, Func<Task> action)
    {
        var e = await Record.ExceptionAsync(action);
        Assert.True(e is ArgumentException, $"{because}: expected ArgumentException, got {e?.GetType().Name ?? "none"}: {e?.Message}");
    }

    private void AssertOutsideUntouched()
    {
        Assert.Equal("OUT", File.ReadAllText(_outside));
        Assert.Equal(new[] { "outside.txt", "root" },
            Directory.EnumerateFileSystemEntries(_base).Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal));
    }
}
