// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Vortex.Encodings;
using EngineeredWood.Vortex.Tests.TestData;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Reads <c>vortex.zigzag</c> integer arrays. The fixtures come from <c>write_zigzag_widths</c>
/// and <c>write_zigzag_default</c> in <c>Rust/src/main.rs</c>.
/// </summary>
public class VortexZigZagTests
{
    private const string Widths = "zigzag_widths_64rows.vortex";
    private const string Sliced = "zigzag_sliced_59rows.vortex";
    private const string Default = "zigzag_default_20000rows.vortex";

    [Theory]
    [InlineData(Widths, 0)]
    [InlineData(Sliced, 5)]
    public async Task ReadsEverySignedWidth(string fixture, int firstRow)
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(fixture));
        var rows = Enumerable.Range(firstRow, 64 - firstRow).ToArray();

        var i8 = Assert.IsType<Int8Array>(await reader.ReadColumnAsync(0));
        Assert.Equal(rows.Select(i => (sbyte)Row(i, sbyte.MinValue, sbyte.MaxValue)), i8.Values.ToArray());
        var i16 = Assert.IsType<Int16Array>(await reader.ReadColumnAsync(1));
        Assert.Equal(rows.Select(i => (short)Row(i, short.MinValue, short.MaxValue)), i16.Values.ToArray());
        var i32 = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(2));
        Assert.Equal(rows.Select(i => (int)Row(i, int.MinValue, int.MaxValue)), i32.Values.ToArray());
        var i64 = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(3));
        Assert.Equal(rows.Select(i => Row(i, long.MinValue, long.MaxValue)), i64.Values.ToArray());

        var n32 = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(4));
        Assert.Equal(rows.Count(i => i % 6 == 0), n32.NullCount);
        Assert.Equal(
            rows.Select(i => i % 6 == 0 ? (int?)null : (int)Row(i, int.MinValue, int.MaxValue)),
            Enumerable.Range(0, n32.Length).Select(i => n32.GetValue(i)));
    }

    [Theory]
    [InlineData(Widths)]
    [InlineData(Sliced)]
    public async Task EveryHandBuiltColumnIsZigZag(string fixture)
    {
        var nodes = await FixtureArrayNodes.ReadAsync(TestDataPath.Resolve(fixture));
        Assert.Equal(5, nodes.Count(n => n.Encoding == VortexArrayEncodings.ZigZag));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task DefaultWriterChoosesZigZagWithPackedChildren(int column)
    {
        var roots = await FixtureArrayNodes.ReadColumnRootsAsync(TestDataPath.Resolve(Default), column);

        Assert.NotEmpty(roots);
        Assert.All(roots, r => Assert.Equal(VortexArrayEncodings.ZigZag, r.Encoding));
        Assert.Contains(roots, r => r.Children[0] != VortexArrayEncodings.Primitive);
    }

    [Fact]
    public async Task ReadsZigZagChosenByTheDefaultWriter()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Default));

        var a = Assert.IsType<Int32Array>(await reader.ReadColumnAsync(0));
        var b = Assert.IsType<Int64Array>(await reader.ReadColumnAsync(1));
        Assert.Equal(20_000, a.Length);
        Assert.Equal(Enumerable.Range(0, 20_000).Count(i => i % 11 == 0), a.NullCount);
        for (int i = 0; i < 20_000; i++)
        {
            Assert.Equal(DefaultA(i), a.GetValue(i));
            Assert.Equal(DefaultB(i), b.GetValue(i));
        }
    }

    /// <summary>Port of <c>zigzag_row</c> in the fixture generator.</summary>
    private static long Row(int i, long min, long max) => i switch
    {
        0 => min,
        1 => max,
        2 => 0,
        3 => -1,
        _ => i % 2 == 0 ? i * 37L % 100 : -(i * 37L % 100),
    };

    private static int? DefaultA(int i) =>
        i % 11 == 0 ? null
        : i % 997 == 0 ? (i % 2 == 0 ? 1_000_000_000 : -1_000_000_000)
        : (int)(i * 7919L % 7) - 3;

    private static long DefaultB(int i) =>
        i % 1500 == 0 ? -(1L << 50)
        : i % 1500 == 750 ? 1L << 50
        : i * 31L % 5 - 2;
}
