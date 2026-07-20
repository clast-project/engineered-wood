// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Tests.TestData;
using Xunit.Abstractions;

namespace EngineeredWood.Vortex.Tests;

public class VortexLayoutTests
{
    private readonly ITestOutputHelper _output;

    public VortexLayoutTests(ITestOutputHelper output) { _output = output; }

    /// <summary>
    /// Records the layout tree the Rust impl emits for our reference fixture,
    /// so when we evolve chunk 6 / 7 we can see exactly what shapes the
    /// decoder needs to handle.
    /// </summary>
    [Theory]
    [InlineData("dict_int_64rows.vortex")]
    [InlineData("bitpacked_int_64rows.vortex")]
    [InlineData("chunked_int_3chunks.vortex")]
    public async Task DumpsLayoutTreeForFixture(string fixture)
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve(fixture));

        var sb = new StringBuilder();
        sb.AppendLine($"{fixture}:");
        DumpNode(sb, reader.RootLayout, depth: 0);
        _output.WriteLine(sb.ToString());
    }

    [Fact]
    public async Task DumpsLayoutTreeForReference()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));

        var sb = new StringBuilder();
        DumpNode(sb, reader.RootLayout, depth: 0);
        _output.WriteLine(sb.ToString());
    }

    [Fact]
    public async Task RootRowCountMatchesFixture()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));

        Assert.Equal(3L, reader.NumberOfRows);
        Assert.Equal(3UL, reader.RootLayout.RowCount);
    }

    [Fact]
    public async Task RootEncodingIsRegistered()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));

        // Whatever the root happens to be, its EncodingId must be a string from
        // the file's layout_specs registry.
        Assert.Contains(reader.RootLayout.EncodingId, reader.LayoutSpecs);
        WalkAndAssert(reader.RootLayout, reader.LayoutSpecs);
    }

    /// <summary>
    /// Locks in the tree shape vortex 0.70 emits for our reference fixture:
    ///   vortex.struct rows=3
    ///     vortex.stats rows=3 meta=6B
    ///       vortex.flat rows=3 segs=[0]   (data array for field "a")
    ///       vortex.flat rows=1 segs=[1]   (per-field stats table)
    /// If a future Rust upgrade changes this we'll see the test fail and
    /// update the chunk-6 decoder dispatch to match.
    /// </summary>
    [Fact]
    public async Task FixtureLayoutMatchesExpectedShape()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));

        var root = reader.RootLayout;
        Assert.Equal(VortexLayoutEncodings.Struct, root.EncodingId);
        Assert.Equal(3UL, root.RowCount);
        Assert.Empty(root.SegmentRefs);
        Assert.Single(root.Children);

        var statsWrap = root.Children[0];
        Assert.Equal(VortexLayoutEncodings.Stats, statsWrap.EncodingId);
        Assert.Equal(3UL, statsWrap.RowCount);
        Assert.Equal(2, statsWrap.Children.Count);
        Assert.Empty(statsWrap.SegmentRefs);
        Assert.NotEmpty(statsWrap.Metadata);

        var dataLeaf = statsWrap.Children[0];
        Assert.Equal(VortexLayoutEncodings.Flat, dataLeaf.EncodingId);
        Assert.Equal(3UL, dataLeaf.RowCount);
        Assert.Empty(dataLeaf.Children);
        Assert.Single(dataLeaf.SegmentRefs);
        Assert.Equal(0u, dataLeaf.SegmentRefs[0]);

        var statsLeaf = statsWrap.Children[1];
        Assert.Equal(VortexLayoutEncodings.Flat, statsLeaf.EncodingId);
        Assert.Equal(1UL, statsLeaf.RowCount);
        Assert.Empty(statsLeaf.Children);
        Assert.Single(statsLeaf.SegmentRefs);
        Assert.Equal(1u, statsLeaf.SegmentRefs[0]);
    }

    private static void WalkAndAssert(VortexLayout node, IReadOnlyList<string> registry)
    {
        Assert.Contains(node.EncodingId, registry);
        // Every segment ref must point inside the file's segment_specs vector.
        // (Range check happens in VortexFileReaderTests; here we just spot-check
        // that segment refs are non-negative — they are uint, so this is free.)
        foreach (var child in node.Children)
            WalkAndAssert(child, registry);
    }

    private static void DumpNode(StringBuilder sb, VortexLayout node, int depth)
    {
        for (int i = 0; i < depth; i++) sb.Append("  ");
        sb.Append(node.EncodingId);
        sb.Append(" rows=").Append(node.RowCount);
        if (node.SegmentRefs.Count > 0)
            sb.Append(" segs=[").Append(string.Join(",", node.SegmentRefs)).Append(']');
        if (node.Metadata.Length > 0)
            sb.Append(" meta=").Append(node.Metadata.Length).Append("B");
        sb.AppendLine();
        foreach (var child in node.Children)
            DumpNode(sb, child, depth + 1);
    }
}
