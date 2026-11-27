// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Core.Tests.IO;

public class StorageLocationTests
{
    private static readonly bool IsWindows = Path.DirectorySeparatorChar == '\\';

    [Theory]
    [InlineData("s3://bucket/a/b", "s3a://bucket/a/b")]
    [InlineData("s3://bucket/a/b", "s3n://bucket/a/b/")]
    [InlineData("abfss://c@acct.dfs.core.windows.net/a/b", "https://acct.blob.core.windows.net/c/a/b")]
    [InlineData("wasbs://c@acct.blob.core.windows.net/a/b", "https://ACCT.blob.core.windows.net/c/a/b")]
    [InlineData("abfs://c@acct.dfs.core.windows.net/a", "http://acct.blob.core.windows.net/c/a")]
    [InlineData("abfss://c@acct.dfs.core.chinacloudapi.cn/a", "https://acct.blob.core.chinacloudapi.cn/c/a")]
    [InlineData("abfss://c@acct.dfs.core.usgovcloudapi.net/a", "https://ACCT.dfs.core.usgovcloudapi.net/c/a")]
    [InlineData("file:/tmp/wh/t", "file:///tmp/wh/t")]
    [InlineData("file:/tmp/wh/t", "file://localhost/tmp/wh/t")]
    [InlineData("file:/tmp/wh/t", "/tmp/wh/t")]
    [InlineData("file:/tmp/wh/t", "/tmp/wh/x/../t/.")]
    [InlineData("file:/C:/wh/t", "C:\\wh\\t")]
    [InlineData("file:/C:/wh/t", "file:///C:/wh/t/")]
    [InlineData("file://server/share/t", "\\\\server\\share\\t")]
    public void SpellingsOfOneLocationAreEqual(string a, string b)
    {
        var left = StorageLocation.Parse(a);
        var right = StorageLocation.Parse(b);
        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
    }

    [Theory]
    [InlineData("s3://bucket/a/b", "s3://Bucket/a/b")]
    [InlineData("s3://bucket/a/b", "s3://bucket/a/B")]
    [InlineData("s3://bucket/a/b", "gs://bucket/a/b")]
    [InlineData("s3://bucket/a%2Fb", "s3://bucket/a/b")]
    [InlineData("s3://bucket/a%20b", "s3://bucket/a b")]
    [InlineData("abfss://c@acct.dfs.core.windows.net/a", "abfss://c@other.dfs.core.windows.net/a")]
    [InlineData("abfss://c@acct.dfs.core.windows.net/a", "abfss://d@acct.dfs.core.windows.net/a")]
    [InlineData("file:/tmp/a%20b", "file:/tmp/a b")]
    [InlineData("http://example.com/t", "https://example.com/t")]
    [InlineData("https://service.blob.example.com/c/t", "abfss://c@service.dfs.example.com/t")]
    [InlineData("https://service.blob.example.com/c/t", "https://service.dfs.example.com/c/t")]
    [InlineData("https://acct.blob.core.windows.net.example.com/c/t", "abfss://c@acct.dfs.core.windows.net/t")]
    public void DifferentObjectsAreNotEqual(string a, string b) =>
        Assert.NotEqual(StorageLocation.Parse(a), StorageLocation.Parse(b));

    [Fact]
    public void FileLocationsCompareByCaseOnlyWhereTheFileSystemDoes()
    {
        var lower = StorageLocation.Parse("file:/C:/wh/table");
        var upper = StorageLocation.Parse("file:/c:/WH/Table");
        Assert.Equal(IsWindows, lower == upper);
    }

    [Theory]
    [InlineData("s3://bucket/a/../b")]
    [InlineData("s3://bucket/a/./b")]
    [InlineData("s3://bucket/..")]
    [InlineData("gs://bucket/t/../x")]
    [InlineData("abfss://c@acct.dfs.core.windows.net/t/../x")]
    [InlineData("https://acct.blob.core.windows.net/c/t/../x")]
    [InlineData("hdfs://nn/t/../x")]
    public void ObjectStoreDotSegmentsAreRefused(string location)
    {
        Assert.Throws<NotSupportedException>(() => StorageLocation.Parse(location));
        Assert.False(StorageLocation.TryParse(location, out _));
    }

    /// <summary>The object stores' file systems strip a leading <c>/</c> and turn <c>\</c> into <c>/</c>, so these
    /// would reach a different key than they name.</summary>
    [Theory]
    [InlineData("s3://bucket/a//b")]
    [InlineData("s3://bucket//x")]
    [InlineData("s3://bucket/t/a\\b")]
    [InlineData("gs://bucket/t//x")]
    [InlineData("abfss://c@acct.dfs.core.windows.net/t/a\\b")]
    public void ObjectStoreKeysThatDoNotRoundTripAreRefused(string location)
    {
        Assert.Throws<NotSupportedException>(() => StorageLocation.Parse(location));
        Assert.False(StorageLocation.TryParse(location, out _));
    }

    [Theory]
    [InlineData("a//b")]
    [InlineData("a\\b")]
    public void CombineRefusesObjectStoreKeysThatDoNotRoundTrip(string relative) =>
        Assert.Throws<NotSupportedException>(() => StorageLocation.Parse("s3://bucket/t").Combine(relative));

    /// <summary>On Windows a <c>\</c> in a file URI separates segments, as the operating system takes it, so a
    /// <c>..</c> written with it navigates here rather than in the backend, after the prefix check.</summary>
    [Fact]
    public void FileUriBackslashesSeparateSegmentsOnWindows()
    {
        var table = StorageLocation.Parse("file:///C:/safe/table");
        var location = StorageLocation.Parse("file:///C:/safe/table/sub\\..\\..\\victim.dv");
        if (IsWindows)
        {
            Assert.Equal(StorageLocation.Parse("C:/safe/victim.dv"), location);
            Assert.False(location.IsUnder(table));
        }
        else
        {
            // Elsewhere \ is an ordinary file-name character.
            Assert.Equal("sub\\..\\..\\victim.dv", location.RelativeTo(table));
        }
    }

    /// <summary>Below a root, Windows takes a drive as a new root (<c>Path.Combine("C:/t", "D:/x")</c> is
    /// <c>D:/x</c>), so a drive anywhere but first would let a path under one location reach another.</summary>
    [Theory]
    [InlineData("file:///C:/safe/table/C:/outside/dv.bin")]
    [InlineData("C:/a/D:/x")]
    [InlineData("/tmp/C:/x")]
    [InlineData("file://server/C:/x")]
    public void DrivesBelowTheRootAreRefused(string location)
    {
        Assert.Throws<FormatException>(() => StorageLocation.Parse(location));
        Assert.Throws<ArgumentException>(() => StorageLocation.Parse("file:///C:/safe/table").Combine("sub/D:/x"));
    }

    [Fact]
    public void PercentEncodedUrisAreDecodedSegmentBySegment()
    {
        // The structure is read first, so an escaped # or ? is part of a name rather than a delimiter.
        var dv = StorageLocation.ParsePercentEncoded("file:///tmp/table/a%23b%3Fc/dv%20x.bin");
        Assert.Equal(StorageLocation.Parse("/tmp/table").Combine("a#b?c/dv x.bin"), dv);
        Assert.Equal("file:///tmp/table/a%23b%3Fc/dv%20x.bin", dv.ToString());
        Assert.Equal(
            StorageLocation.Parse("s3://bucket/t").Combine("a#b"),
            StorageLocation.ParsePercentEncoded("s3://bucket/t/a%23b"));

        // An unescaped # or ? is still a fragment or query, which no location has, whatever the scheme.
        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded("file:///tmp/table/a#b"));
        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded("s3://bucket/dv?version=1"));
        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded("abfss://c@acct.dfs.core.windows.net/dv#f"));

        // Parse, which reads no URI syntax in an object-store key, keeps them as key characters.
        Assert.Equal("dv?version=1", StorageLocation.Parse("s3://bucket/dv?version=1").RelativeTo(StorageLocation.Parse("s3://bucket")));
    }

    /// <summary>java.net.URI refuses a malformed escape; <c>Uri.UnescapeDataString</c> would keep it as text.</summary>
    [Theory]
    [InlineData("file:///tmp/dv%ZZ.bin")]
    [InlineData("file:///tmp/dv%2")]
    [InlineData("s3://bucket/t/dv%")]
    [InlineData("s3://bucket/t/%G0/dv.bin")]
    public void MalformedEscapesAreRefused(string location) =>
        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded(location));

    /// <summary>MEASURED on Java 17: <c>java.net.URI</c> refuses each of these raw ("Illegal character in path"),
    /// so delta-spark could not read such a vector. A raw <c>\</c> would also mean one file on Windows and another
    /// elsewhere. Raw non-ASCII is legal there, and here.</summary>
    [Theory]
    [InlineData("file:///tmp/a b/dv.bin")]
    [InlineData("s3://bucket/t/a b")]
    [InlineData("file:///tmp/a\\b/dv.bin")]
    [InlineData("s3://bucket/t/a\\b")]
    [InlineData("file:///tmp/a[1].bin")]
    [InlineData("file:///tmp/a|b")]
    [InlineData("file:///tmp/a^b")]
    [InlineData("file:///tmp/a\"b")]
    [InlineData("file:///tmp/a<b>")]
    [InlineData("file:///tmp/a{b}")]
    [InlineData("file:///tmp/a`b")]
    public void CharactersAUriCannotHoldAreRefused(string location)
    {
        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded(location));
        Assert.Equal(
            StorageLocation.Parse("file:///tmp/é.bin"), StorageLocation.ParsePercentEncoded("file:///tmp/%C3%A9.bin"));
        Assert.Equal(
            StorageLocation.Parse("file:///tmp/é.bin"), StorageLocation.ParsePercentEncoded("file:///tmp/é.bin"));
    }

    /// <summary>MEASURED on Java 17: <c>java.net.URI</c> accepts brackets around an IPv6 host
    /// (<c>http://[::1]:10000/...</c>, an Azurite-style endpoint) and refuses them in the path.</summary>
    [Fact]
    public void BracketsAreLegalAroundAnIpv6HostOnly()
    {
        var dv = StorageLocation.ParsePercentEncoded("http://[::1]:10000/devstoreaccount1/container/dv.bin");
        Assert.Equal(StorageLocation.Parse("http://[::1]:10000/devstoreaccount1/container/dv.bin"), dv);
        Assert.True(dv.IsUnder(StorageLocation.Parse("http://[::1]:10000/devstoreaccount1/container")));
        Assert.Equal(StorageLocation.Parse("s3://[::1]/b/dv.bin"), StorageLocation.ParsePercentEncoded("s3://[::1]/b/dv.bin"));

        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded("http://[::1]:10000/c/a[1].bin"));
        Assert.Throws<FormatException>(() => StorageLocation.ParsePercentEncoded("s3://bucket/t/a]b"));
    }

    [Theory]
    [InlineData("s3://bucket/t/%2E%2E/x")]
    [InlineData("s3://bucket/t/a%2F..%2Fx")]
    [InlineData("file:///tmp/t/%2E%2E/x")]
    [InlineData("file:///tmp/t/a%2Fb")]
    [InlineData("file:///tmp/t/a%5Cb")]
    [InlineData("file:///tmp/t/a%00b")]
    public void SegmentsThatDecodeToMoreThanOneSegmentAreRefused(string location) =>
        Assert.Throws<NotSupportedException>(() => StorageLocation.ParsePercentEncoded(location));

    [Fact]
    public void PercentEncodedDotsAreLiteralSegments()
    {
        // Nothing is decoded, so %2E%2E is an ordinary segment rather than a way around the refusal.
        var location = StorageLocation.Parse("s3://bucket/t/%2E%2E/x");
        Assert.Equal("t/%2E%2E/x", location.RelativeTo(StorageLocation.Parse("s3://bucket")));
        Assert.True(location.IsUnder(StorageLocation.Parse("s3://bucket/t")));
    }

    [Theory]
    [InlineData("")]
    [InlineData("relative/path")]
    [InlineData("C:relative")]
    [InlineData("s3:/bucket/x")]
    [InlineData("s3:///x")]
    [InlineData("file:relative")]
    [InlineData("/tmp/../../x")]
    [InlineData("C:/..")]
    [InlineData("\\\\?\\C:\\x")]
    [InlineData("\\\\.\\C:\\x")]
    [InlineData("file://?/C:/x")]
    [InlineData("file://./C:/x")]
    [InlineData("s3://bucket/a\nb")]
    public void NonLocationsAreRefused(string text)
    {
        Assert.False(StorageLocation.TryParse(text, out _));
        Assert.Throws<FormatException>(() => StorageLocation.Parse(text));
    }

    [Theory]
    [InlineData("s3://bucket/a/b", "s3://bucket/a/b")]
    [InlineData("s3a://bucket/t/", "s3a://bucket/t")]
    [InlineData("file:/C:/wh/t", "file:/C:/wh/t")]
    [InlineData("file:///tmp/wh/t", "file:///tmp/wh/t")]
    [InlineData("/tmp/wh/t", "file:///tmp/wh/t")]
    [InlineData("C:\\wh\\t", "file:///C:/wh/t")]
    [InlineData("\\\\server\\share\\t", "file://server/share/t")]
    [InlineData("/", "file:///")]
    public void ToStringKeepsTheSpellingOfAUri(string text, string expected) =>
        Assert.Equal(expected, StorageLocation.Parse(text).ToString());

    [Theory]
    [InlineData("file:/C:/wh/t", "C:\\wh\\t", "C:/wh/t")]
    [InlineData("file:///tmp/wh/t", "\\tmp\\wh\\t", "/tmp/wh/t")]
    [InlineData("file://server/share/t", "\\\\server\\share\\t", "//server/share/t")]
    [InlineData("C:\\", "C:\\", "C:/")]
    public void LocalPathIsWhatTheOperatingSystemOpens(string text, string windows, string posix) =>
        Assert.Equal(IsWindows ? windows : posix, StorageLocation.Parse(text).LocalPath);

    [Fact]
    public void ObjectStoreLocationsHaveNoLocalPath() =>
        Assert.Null(StorageLocation.Parse("s3://bucket/t").LocalPath);

    [Theory]
    [InlineData("s3://bucket/table", "s3://bucket/table", true)]
    [InlineData("s3://bucket/table/x", "s3://bucket/table", true)]
    [InlineData("s3://bucket/table-other/x", "s3://bucket/table", false)]
    [InlineData("s3://bucket/tab", "s3://bucket/table", false)]
    [InlineData("s3://bucket/table/x", "s3://bucket", true)]
    [InlineData("s3://other/table/x", "s3://bucket", false)]
    [InlineData("s3a://bucket/table/x", "s3://bucket/table", true)]
    [InlineData("abfss://c@acct.dfs.core.windows.net/t/x", "https://acct.blob.core.windows.net/c", true)]
    [InlineData("https://acct.blob.core.windows.net/c2/t", "https://acct.blob.core.windows.net/c", false)]
    [InlineData("file:/tmp/wh/t/x", "/tmp/wh", true)]
    [InlineData("file:/tmp/wh-other/t", "/tmp/wh", false)]
    public void IsUnderComparesWholeSegments(string location, string prefix, bool expected) =>
        Assert.Equal(expected, StorageLocation.Parse(location).IsUnder(StorageLocation.Parse(prefix)));

    [Fact]
    public void RelativeToGivesThePathUnderThePrefix()
    {
        var table = StorageLocation.Parse("abfss://c@acct.dfs.core.windows.net/wh/t");
        var container = StorageLocation.Parse("https://acct.blob.core.windows.net/c");
        Assert.Equal("wh/t", table.RelativeTo(container));
        Assert.Equal("", table.RelativeTo(table));
        Assert.Throws<ArgumentException>(() => container.RelativeTo(table));
    }

    [Fact]
    public void CombineAppendsSegmentsInTheBasesSpelling()
    {
        var table = StorageLocation.Parse("s3a://bucket/wh/t/");
        var file = table.Combine("data/a=1/x.parquet");
        Assert.Equal("s3a://bucket/wh/t/data/a=1/x.parquet", file.ToString());
        Assert.Equal(StorageLocation.Parse("s3://bucket/wh/t/data/a=1/x.parquet"), file);
        Assert.True(file.IsUnder(table));
        Assert.Same(table, table.Combine(""));
    }

    [Fact]
    public void CombineKeepsObjectStoreSegmentsVerbatim()
    {
        var file = StorageLocation.Parse("s3://bucket/t").Combine("a/b%2E/c#d");
        Assert.Equal("s3://bucket/t/a/b%2E/c#d", file.ToString());
        Assert.Equal("a/b%2E/c#d", file.RelativeTo(StorageLocation.Parse("s3://bucket/t")));
    }

    [Theory]
    [InlineData("s3://bucket/t", "../x")]
    [InlineData("s3://bucket/t", "a/../b")]
    [InlineData("s3://bucket/t", "./b")]
    [InlineData("gs://bucket/t", "a/..")]
    public void CombineRefusesObjectStoreDotSegments(string baseLocation, string relative) =>
        Assert.Throws<NotSupportedException>(() => StorageLocation.Parse(baseLocation).Combine(relative));

    [Fact]
    public void CombineNavigatesFileDotSegmentsButNotAboveTheBase()
    {
        var table = StorageLocation.Parse("file:///tmp/wh/t");
        Assert.Equal(StorageLocation.Parse("/tmp/wh/t/b"), table.Combine("a/../b"));
        Assert.Equal(StorageLocation.Parse("/tmp/wh/t/b"), table.Combine(".\\b"));
        Assert.Throws<ArgumentException>(() => table.Combine("../x"));
        Assert.Throws<ArgumentException>(() => table.Combine("a/../../x"));
    }

    [Theory]
    [InlineData("/etc/passwd")]
    [InlineData("\\x")]
    [InlineData("C:\\x")]
    [InlineData("s3://other/x")]
    [InlineData("file:/x")]
    public void CombineRefusesAbsolutePaths(string path) =>
        Assert.Throws<ArgumentException>(() => StorageLocation.Parse("s3://bucket/t").Combine(path));

    [Theory]
    [InlineData("s3://b/x", true)]
    [InlineData("file:/x", true)]
    [InlineData("/x", true)]
    [InlineData("\\\\s\\x", true)]
    [InlineData("C:\\x", true)]
    [InlineData("C:/x", true)]
    [InlineData("x/y", false)]
    [InlineData("a=1:2/x", false)]
    [InlineData("", false)]
    public void LooksAbsoluteSpotsSchemesAndRootedPaths(string text, bool expected) =>
        Assert.Equal(expected, StorageLocation.LooksAbsolute(text));
}

public class StorageResolverTests
{
    private sealed class NamedFileSystem(string name) : ITableFileSystem
    {
        public string Name { get; } = name;
        public PathNameConstraints PathConstraints => PathNameConstraints.None;
        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    [Fact]
    public void TheLongestRegisteredPrefixWins()
    {
        var bucket = new NamedFileSystem("bucket");
        var table = new NamedFileSystem("table");
        var resolver = new StorageResolver()
            .Register("s3://bucket", bucket)
            .Register("s3://bucket/wh/t", table);

        var inTable = resolver.Resolve("s3a://bucket/wh/t/data/x.parquet");
        Assert.Same(table, inTable.FileSystem);
        Assert.Equal("data/x.parquet", inTable.Path);

        var sibling = resolver.Resolve("s3://bucket/wh/t-other/x");
        Assert.Same(bucket, sibling.FileSystem);
        Assert.Equal("wh/t-other/x", sibling.Path);

        Assert.Equal("", resolver.Resolve("s3://bucket/wh/t").Path);
    }

    [Theory]
    [InlineData("s3://other-bucket/x")]
    [InlineData("gs://bucket/x")]
    [InlineData("hdfs://nn/x")]
    [InlineData("file:/tmp/x")]
    public void AnUnregisteredLocationIsRefused(string location)
    {
        var resolver = new StorageResolver().Register("s3://bucket", new NamedFileSystem("bucket"));
        var ex = Assert.Throws<UnregisteredStorageLocationException>(() => resolver.Resolve(location));
        Assert.Equal(StorageLocation.Parse(location), ex.Location);
        Assert.False(resolver.TryResolve(StorageLocation.Parse(location), out _));
    }

    [Fact]
    public void APrefixRegistersOnce()
    {
        var resolver = new StorageResolver().Register("s3://bucket/t", new NamedFileSystem("a"));
        Assert.Throws<ArgumentException>(() => resolver.Register("s3a://bucket/t/", new NamedFileSystem("b")));
    }

    [Fact]
    public async Task ALocalFileSystemRegistersAtItsRoot()
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-resolver-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(root, "t", "data"));
        try
        {
            File.WriteAllText(Path.Combine(root, "t", "data", "x.txt"), "hello");
            var fs = new LocalTableFileSystem(root);
            var resolver = new StorageResolver().Register(fs);

            string uri = "file:///" + root.Replace('\\', '/').TrimStart('/') + "/t/data/x.txt";
            var resolved = resolver.Resolve(uri);
            Assert.Same(fs, resolved.FileSystem);
            Assert.Equal("t/data/x.txt", resolved.Path);
            Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(
                await resolved.FileSystem.ReadAllBytesAsync(resolved.Path)));

            Assert.Equal(fs.Location, resolver.Resolve(root).Prefix);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
