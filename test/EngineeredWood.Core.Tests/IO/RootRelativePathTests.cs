// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Core.Tests.IO;

/// <summary>The path checks every root-relative file system applies (#962).</summary>
public class RootRelativePathTests
{
    [Theory]
    [InlineData("")]
    [InlineData("x")]
    [InlineData("a/b/c.parquet")]
    [InlineData("./x")]
    [InlineData("a/../x")]
    [InlineData("a/b/../../x")]
    [InlineData("a/..")]
    [InlineData("a//b")]
    [InlineData("dir/")]
    [InlineData("...")]          // a name, not a dot segment
    [InlineData(".. /x")]         // likewise
    [InlineData("%2E%2E/x")]      // literal: nothing here decodes
    [InlineData("db.t:v1/x")]     // a colon is not a scheme without ":/"
    [InlineData("region=a:b/x")]
    public void StaysInside_IsAccepted(string path) =>
        RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: true);

    [Theory]
    [InlineData("..")]
    [InlineData("../x")]
    [InlineData("a/../../x")]
    [InlineData("./../x")]
    [InlineData("../t/x")] // climbs out and comes back: refused, a path may not climb above its base
    [InlineData("a/../../a/x")]
    [InlineData("/x")]
    [InlineData("//server/share/x")]
    [InlineData("s3://other/x")]
    [InlineData("file:///C:/x")]
    [InlineData("file:/tmp/x")]
    [InlineData("abfss://c@a.dfs.core.windows.net/x")]
    public void Escaping_IsRefused(string path) =>
        Assert.Throws<ArgumentException>(() => RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: false));

    [Theory]
    [InlineData(@"..\x")]
    [InlineData(@"a\..\..\x")]
    [InlineData(@"\x")]
    [InlineData(@"\\server\share\x")]
    [InlineData(@"\\?\C:\x")]
    public void Backslash_SeparatesOnlyWhenAsked(string path)
    {
        Assert.Throws<ArgumentException>(() => RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: true));
        RootRelativePath.ThrowIfEscapesRoot(path.TrimStart('\\'), backslashSeparates: false);
    }

    /// <summary>A mixed-separator path can stay inside under one reading of <c>\</c> and climb out under the other,
    /// which is why a wrapper that does not know its backend checks both.</summary>
    [Fact]
    public void MixedSeparators_DependOnTheReading()
    {
        const string path = @"a\b/../../x";
        RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: true);
        Assert.Throws<ArgumentException>(() => RootRelativePath.ThrowIfEscapesRoot(path, backslashSeparates: false));
    }

    [Fact]
    public void Null_IsRefused()
    {
        Assert.Throws<ArgumentNullException>(() => RootRelativePath.ThrowIfEscapesRoot(null!, true));
        Assert.Throws<ArgumentNullException>(() => RootRelativePath.ToObjectKey(null!));
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("x", "x")]
    [InlineData("a/b.parquet", "a/b.parquet")]
    [InlineData(@"a\b.parquet", "a/b.parquet")]
    [InlineData("dir/", "dir/")]
    [InlineData("a//b", "a//b")]
    [InlineData("...", "...")]
    [InlineData("%2E%2E/x", "%2E%2E/x")]
    [InlineData("region=a b%23c/x", "region=a b%23c/x")]
    [InlineData("db.t:v1/x", "db.t:v1/x")]
    public void ObjectKey_KeepsTheKey(string path, string key) =>
        Assert.Equal(key, RootRelativePath.ToObjectKey(path));

    [Theory]
    [InlineData("/x")]
    [InlineData(@"\x")]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData("./x")]
    [InlineData("a/./x")]
    [InlineData("a/../x")] // stays inside if collapsed, but an object store does not collapse it
    [InlineData("../x")]
    [InlineData(@"a\..\x")]
    [InlineData("a/..")]
    [InlineData("s3://other/x")]  // read as the key "root/s3://other/x" before #962
    [InlineData("gs://b/x")]
    [InlineData("C:/x")]
    [InlineData(@"C:\x")]
    public void ObjectKey_RefusesRootedAndDotSegments(string path) =>
        Assert.Throws<ArgumentException>(() => RootRelativePath.ToObjectKey(path));
}
