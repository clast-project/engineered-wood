// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

public class RoaringBitmapWriterTests
{
    [Fact]
    public void RoundTrip_SingleValue()
    {
        byte[] blob = RoaringBitmapWriter.Serialize([42L]);
        var result = RoaringBitmapReader.Deserialize(blob);
        Assert.Single(result);
        Assert.Contains(42L, result);
    }

    [Fact]
    public void RoundTrip_MultipleValues()
    {
        byte[] blob = RoaringBitmapWriter.Serialize([1L, 5L, 10L, 100L]);
        var result = RoaringBitmapReader.Deserialize(blob);
        Assert.Equal(4, result.Count);
        Assert.Contains(1L, result);
        Assert.Contains(5L, result);
        Assert.Contains(10L, result);
        Assert.Contains(100L, result);
    }

    [Fact]
    public void RoundTrip_ConsecutiveValues()
    {
        var indices = Enumerable.Range(0, 50).Select(i => (long)i).ToArray();
        byte[] blob = RoaringBitmapWriter.Serialize(indices);
        var result = RoaringBitmapReader.Deserialize(blob);
        Assert.Equal(50, result.Count);
        for (int i = 0; i < 50; i++)
            Assert.Contains((long)i, result);
    }

    [Fact]
    public void RoundTrip_LargeSet()
    {
        // Enough values to potentially trigger bitmap container (>4096)
        var indices = Enumerable.Range(0, 100).Select(i => (long)(i * 3)).ToArray();
        byte[] blob = RoaringBitmapWriter.Serialize(indices);
        var result = RoaringBitmapReader.Deserialize(blob);
        Assert.Equal(100, result.Count);
        foreach (long idx in indices)
            Assert.Contains(idx, result);
    }

    [Fact]
    public void RoundTrip_DuplicatesIgnored()
    {
        byte[] blob = RoaringBitmapWriter.Serialize([5L, 5L, 5L, 10L, 10L]);
        var result = RoaringBitmapReader.Deserialize(blob);
        Assert.Equal(2, result.Count);
        Assert.Contains(5L, result);
        Assert.Contains(10L, result);
    }
}
