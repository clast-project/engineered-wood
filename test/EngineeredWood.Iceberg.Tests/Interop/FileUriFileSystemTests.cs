// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg.Tests.Interop;

public sealed class FileUriFileSystemTests
{
    private static readonly string Root = Path.Combine(Path.GetTempPath(), "ew_fileuri_root");

    private static readonly FileUriFileSystem Fs = new(Root);

    [Fact]
    public void Map_TurnsAUriUnderTheRootIntoARelativePath()
    {
        Assert.Equal("ew.db/t/metadata/v1.json", Fs.Map(Fs.RootUri + "/ew.db/t/metadata/v1.json"));
        Assert.Equal("ew.db/t", Fs.Map("ew.db/t"));
        Assert.Equal("", Fs.Map(Fs.RootUri));
        Assert.Equal("", Fs.Map(""));
    }

    [Fact]
    public void Map_AppliesDotDotBeforeCheckingTheRoot()
    {
        // Under the root after normalization: allowed.
        Assert.Equal("b", Fs.Map(Fs.RootUri + "/a/../b"));

        // A string prefix check passes these; they resolve outside the root.
        Assert.Throws<ArgumentException>(() => Fs.Map(Fs.RootUri + "/../outside"));
        Assert.Throws<ArgumentException>(() => Fs.Map(Fs.RootUri + "/a/../../outside"));
        Assert.Throws<ArgumentException>(() => Fs.Map("../outside"));
    }

    [Fact]
    public void Map_RejectsASiblingDirectorySharingTheRootsPrefix()
    {
        Assert.Throws<ArgumentException>(() => Fs.Map(Fs.RootUri + "-sibling/x"));
    }
}
