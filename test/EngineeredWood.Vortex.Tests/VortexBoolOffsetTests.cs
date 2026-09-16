// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// A sliced <c>vortex.bool</c> starts partway into its first byte, recorded as
/// <c>BoolMetadata.offset</c>, for its values and for any validity child
/// (<c>write_bool_sliced</c> in <c>Rust/src/main.rs</c>).
/// </summary>
public class VortexBoolOffsetTests
{
    [Fact]
    public async Task ReadsSlicedBoolValuesAndValidity()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("bool_sliced_61rows.vortex"));

        var b = Assert.IsType<BooleanArray>(await reader.ReadColumnAsync(0));
        var expected = Enumerable.Range(3, 61)
            .Select(i => i % 5 == 0 ? (bool?)null : i % 3 == 0)
            .ToArray();
        Assert.Equal(expected.Length, b.Length);
        Assert.Equal(expected.Count(e => e is null), b.NullCount);
        Assert.Equal(expected, Enumerable.Range(0, b.Length).Select(i => b.GetValue(i)));
    }
}
