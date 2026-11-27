// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>How an <c>add</c>/<c>remove</c>/<c>cdc</c> path is placed: relative to the table, or an absolute URI
/// inside or outside it (#963).</summary>
public sealed class DeltaFileLocatorTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"ew_locator_{Guid.NewGuid():N}");
    private readonly LocalTableFileSystem _table;
    private readonly LocalTableFileSystem _other;

    public DeltaFileLocatorTests()
    {
        Directory.CreateDirectory(Path.Combine(_root, "table"));
        Directory.CreateDirectory(Path.Combine(_root, "other"));
        _table = new LocalTableFileSystem(Path.Combine(_root, "table"));
        _other = new LocalTableFileSystem(Path.Combine(_root, "other"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    private static string Uri(LocalTableFileSystem fs) =>
        "file:///" + fs.Location.LocalPath!.Replace('\\', '/').TrimStart('/');

    /// <summary>Hadoop's qualified spelling (<c>file:/x</c>), as delta-spark's SHALLOW CLONE writes a local path.</summary>
    private static string HadoopUri(LocalTableFileSystem fs) =>
        "file:/" + fs.Location.LocalPath!.Replace('\\', '/').TrimStart('/');

    [Theory]
    [InlineData("part-0.parquet", false)]
    [InlineData("p=a%3Ab/part-0.parquet", false)] // an encoded colon in a partition value is not a scheme
    [InlineData("s3://bucket/t/part-0.parquet", true)]
    [InlineData("abfss://c@a.dfs.core.windows.net/t/x.parquet", true)]
    [InlineData("file:/tmp/t/x.parquet", true)]
    [InlineData("C:/t/x.parquet", false)] // a drive, not a scheme (and refused as rooted when read)
    public void IsAbsolute_MeansAUriScheme(string path, bool absolute) =>
        Assert.Equal(absolute, DeltaFileLocator.IsAbsolute(path));

    [Fact]
    public void Relative_IsDecodedUnderTheTable()
    {
        var locator = new DeltaFileLocator(_table, resolver: null);
        var file = locator.Resolve("p=a%2520b/x%20y.parquet");
        Assert.Same(_table, file.FileSystem);
        Assert.Equal("p=a%20b/x y.parquet", file.Path);
        Assert.True(file.InTable);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Absolute_InsideTheTable_IsTheTables(bool hadoopSpelling)
    {
        var locator = new DeltaFileLocator(_table, resolver: null);
        string prefix = hadoopSpelling ? HadoopUri(_table) : Uri(_table);
        var file = locator.Resolve(prefix + "/p=a%2520b/x%20y.parquet");
        Assert.Same(_table, file.FileSystem);
        Assert.Equal("p=a%20b/x y.parquet", file.Path);
        Assert.True(file.InTable);
        Assert.Equal("p=a%20b/x y.parquet", locator.TableRelativePath(prefix + "/p=a%2520b/x%20y.parquet"));
    }

    [Fact]
    public void Absolute_OutsideTheTable_GoesThroughTheResolver()
    {
        string path = Uri(_other) + "/x.parquet";
        Assert.Throws<UnregisteredStorageLocationException>(() => new DeltaFileLocator(_table, null).Resolve(path));

        var locator = new DeltaFileLocator(_table, new StorageResolver().Register(_other));
        var file = locator.Resolve(path);
        Assert.Same(_other, file.FileSystem);
        Assert.Equal("x.parquet", file.Path);
        Assert.False(file.InTable);
        Assert.Null(locator.TableRelativePath(path));
    }

    [Fact]
    public void Absolute_SiblingWithACommonPrefix_IsOutside()
    {
        // ".../table-other" is not under ".../table": segments compare whole.
        var sibling = new LocalTableFileSystem(Path.Combine(_root, "table-other"));
        var locator = new DeltaFileLocator(_table, resolver: null);
        Assert.Null(locator.TableRelativePath(Uri(sibling) + "/x.parquet"));
    }

    [Theory]
    [InlineData("/tmp/t/x.parquet")]
    [InlineData("\\tmp\\t\\x.parquet")]
    [InlineData("C:/t/x.parquet")]
    [InlineData("%2Ftmp/t/x.parquet")]
    [InlineData("C%3A/t/x.parquet")]
    public void Rooted_WithoutAScheme_IsRefused(string path)
    {
        var locator = new DeltaFileLocator(_table, resolver: null);
        var ex = Assert.Throws<DeltaFormatException>(() => locator.Resolve(path));
        Assert.Equal(DeltaErrorCodes.InvalidFilePath, ex.ErrorCode);
        Assert.Throws<DeltaFormatException>(() => locator.TableRelativePath(path));
    }

    [Theory]
    [InlineData("s3://bucket/t/../x.parquet")]
    [InlineData("s3://bucket/t/%2E%2E/x.parquet")]
    [InlineData("s3://bucket/t/x%zz.parquet")]
    public void Absolute_ThatNoLocationNames_IsRefused(string path)
    {
        var ex = Assert.Throws<DeltaFormatException>(() => new DeltaFileLocator(_table, null).Resolve(path));
        Assert.Equal(DeltaErrorCodes.InvalidFilePath, ex.ErrorCode);
    }

    [Fact]
    public void TableRelativePath_UnknownTableLocation_CannotPlaceAnAbsolutePath()
    {
        var locator = new DeltaFileLocator(new UnlocatedFileSystem(), resolver: null);
        Assert.Null(locator.TableLocation);
        Assert.Equal("x.parquet", locator.TableRelativePath("x.parquet"));
        Assert.Throws<NotSupportedException>(() => locator.TableRelativePath(Uri(_table) + "/x.parquet"));
    }

    [Fact]
    public void EncodedDirectoryForRewrite()
    {
        var locator = new DeltaFileLocator(_table, resolver: null);
        Assert.Equal("", locator.EncodedDirectoryForRewrite("x.parquet"));
        Assert.Equal("p=a%2520b/", locator.EncodedDirectoryForRewrite("p=a%2520b/x.parquet"));
        Assert.Equal("p=a%2520b/", locator.EncodedDirectoryForRewrite(Uri(_table) + "/p=a%2520b/x.parquet"));
        Assert.Equal("", locator.EncodedDirectoryForRewrite(Uri(_table) + "/x.parquet"));
        Assert.Null(locator.EncodedDirectoryForRewrite(Uri(_other) + "/p=1/x.parquet"));
    }

    [Fact]
    public void ResolveInTable_RefusesAFileOutsideTheTable()
    {
        var locator = new DeltaFileLocator(_table, new StorageResolver().Register(_other));
        Assert.Equal("x.parquet", locator.ResolveInTable(Uri(_table) + "/x.parquet", "the seam"));
        Assert.Throws<NotSupportedException>(() => locator.ResolveInTable(Uri(_other) + "/x.parquet", "the seam"));
    }

    private sealed class UnlocatedFileSystem : ITableFileSystem
    {
        public PathNameConstraints PathConstraints => PathNameConstraints.None;

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
