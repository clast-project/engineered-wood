// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage.Blobs;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;

namespace EngineeredWood.Parquet.Tests.IO;

/// <summary>The location an Azure file system registers at. No requests are made.</summary>
public class AzureTableFileSystemLocationTests
{
    private static BlobContainerClient Container(string uri) => new(new Uri(uri));

    [Fact]
    public void Location_IsTheContainerAndRoot_WithoutTheSas()
    {
        var fs = new AzureTableFileSystem(Container("https://acct.blob.core.windows.net/c?sv=2024&sig=secret"), "wh/t");
        Assert.Equal(StorageLocation.Parse("abfss://c@acct.dfs.core.windows.net/wh/t"), fs.Location);
        Assert.DoesNotContain("sig", fs.Location.ToString());
    }

    /// <summary>A blob name may hold a <c>#</c> or <c>?</c>; the root's segments are appended rather than parsed as
    /// URI syntax, so such a root registers like any other.</summary>
    [Theory]
    [InlineData("region=a#b")]
    [InlineData("region=a?b")]
    public void Location_KeepsDelimiterCharactersInTheRoot(string root)
    {
        var fs = new AzureTableFileSystem(Container("https://acct.blob.core.windows.net/c"), root);
        Assert.Equal(StorageLocation.Parse("https://acct.blob.core.windows.net/c").Combine(root), fs.Location);

        var resolver = new StorageResolver().Register(fs.Location, fs);
        Assert.Equal("x.parquet", resolver.Resolve(fs.Location.Combine("x.parquet")).Path);
    }

    [Fact]
    public void Location_OfAContainerWithNoRoot_IsTheContainer()
    {
        var fs = new AzureTableFileSystem(Container("http://127.0.0.1:1/devstoreaccount1/container"));
        Assert.Equal(StorageLocation.Parse("http://127.0.0.1:1/devstoreaccount1/container"), fs.Location);
    }
}
