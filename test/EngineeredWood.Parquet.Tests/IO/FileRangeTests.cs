// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.Tests.IO;

public class FileRangeTests
{
    [Fact]
    public void End_ReturnsOffsetPlusLength()
    {
        var range = new FileRange(10, 20);
        Assert.Equal(30, range.End);
    }

    [Fact]
    public void OverlapsOrAdjacent_OverlappingRanges_ReturnsTrue()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(5, 10);
        Assert.True(a.OverlapsOrAdjacent(b));
        Assert.True(b.OverlapsOrAdjacent(a));
    }

    [Fact]
    public void OverlapsOrAdjacent_AdjacentRanges_ReturnsTrue()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(10, 5);
        Assert.True(a.OverlapsOrAdjacent(b));
        Assert.True(b.OverlapsOrAdjacent(a));
    }

    [Fact]
    public void OverlapsOrAdjacent_DisjointRanges_ReturnsFalse()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(20, 5);
        Assert.False(a.OverlapsOrAdjacent(b));
        Assert.False(b.OverlapsOrAdjacent(a));
    }

    [Fact]
    public void IsWithinGap_RangesWithinThreshold_ReturnsTrue()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(15, 5); // gap of 5
        Assert.True(a.IsWithinGap(b, maxGapBytes: 5));
        Assert.True(b.IsWithinGap(a, maxGapBytes: 5));
    }

    [Fact]
    public void IsWithinGap_RangesBeyondThreshold_ReturnsFalse()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(15, 5); // gap of 5
        Assert.False(a.IsWithinGap(b, maxGapBytes: 4));
    }

    [Fact]
    public void IsWithinGap_OverlappingRanges_ReturnsTrue()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(5, 10);
        Assert.True(a.IsWithinGap(b, maxGapBytes: 0));
    }

    [Fact]
    public void Merge_OverlappingRanges_ReturnsSmallestCoveringRange()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(5, 10);
        var merged = a.Merge(b);
        Assert.Equal(new FileRange(0, 15), merged);
    }

    [Fact]
    public void Merge_DisjointRanges_CoversGap()
    {
        var a = new FileRange(0, 10);
        var b = new FileRange(20, 5);
        var merged = a.Merge(b);
        Assert.Equal(new FileRange(0, 25), merged);
    }

    [Fact]
    public void Merge_IsCommutative()
    {
        var a = new FileRange(10, 20);
        var b = new FileRange(50, 10);
        Assert.Equal(a.Merge(b), b.Merge(a));
    }

    [Fact]
    public void RecordStruct_StructuralEquality()
    {
        var a = new FileRange(10, 20);
        var b = new FileRange(10, 20);
        Assert.Equal(a, b);
        Assert.True(a == b);
    }
}
