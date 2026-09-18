// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Vortex.Tests.TestData;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// A <c>vortex.listview</c> whose views are not contiguous — out of order, overlapping, repeated,
/// empty and null rows, with elements no row covers — has to be re-packed into Arrow's contiguous
/// list layout (#382). The fixtures keep the list as a view (<c>write_listview_noncontiguous</c> and
/// <c>write_map_noncontiguous</c> in <c>Rust/src/main.rs</c>); the map's entries are always a
/// list-view, so a map lands on the same path. Each fixture sits beside the Arrow IPC file
/// <c>vortex-oracle</c> decoded it to, so these run everywhere without Rust.
/// </summary>
public class VortexListViewTests
{
    [Theory]
    [InlineData("listview_noncontiguous_7rows")]
    [InlineData("map_noncontiguous_7rows")]
    public async Task NonContiguousListView_ReadsLikeVortex(string fixture) =>
        await VortexUpstreamCompatTests.CompareAsync(
            TestDataPath.Resolve(fixture + ".arrow"), TestDataPath.Resolve(fixture + ".vortex"));

    [Fact]
    public async Task NonContiguousListView_KeepsPrimitiveElementNulls()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("listview_noncontiguous_7rows.vortex"));

        // Elements 2 and 7 are null. The re-pack once copied value bytes alone, so both read as 0.
        var ints = Assert.IsType<ListArray>(await reader.ReadColumnAsync(0));
        Assert.Equal(
            new[] { "[60, null, 80]", "[0, 10]", "[10, null, 30]", "null", "[]", "[null]", "[60, null, 80]" },
            Enumerable.Range(0, ints.Length).Select(i => ArrowValues.Render(ints, i)));
    }

    [Fact]
    public async Task NonContiguousMap_ReadsEntries()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("map_noncontiguous_7rows.vortex"));

        var m = Assert.IsType<MapArray>(await reader.ReadColumnAsync(0));
        Assert.Equal(7, m.Length);
        Assert.Equal(1, m.NullCount);
        Assert.True(m.IsNull(3));
        // The entries are {key, value} structs; value 2 is null, and rows 0 and 6 share one view.
        Assert.Equal(
            new[]
            {
                "{\"k6\": 600, \"k7\": 700, \"k8\": 800}", "{\"k0\": 0, \"k1\": 100}",
                "{\"k1\": 100, \"k2\": null, \"k3\": 300}", "null", "{}", "{\"k2\": null}",
                "{\"k6\": 600, \"k7\": 700, \"k8\": 800}",
            },
            Enumerable.Range(0, m.Length).Select(i => ArrowValues.Render(m, i)));
    }
}
