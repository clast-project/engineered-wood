// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// The <c>Map</c> dtype and the <c>vortex.map</c> encoding (vortex 0.86+), over upstream's own
/// <c>map.vortex</c> compatibility fixture (committed under <c>TestData/upstream-compat</c>, so
/// this runs everywhere). <see cref="VortexUpstreamCompatTests"/> checks the same file against
/// vortex's reader value by value; these tests spell out what that agreement means — the Arrow
/// type a map dtype becomes, and that entry order, duplicate keys, null values and null maps all
/// survive the round trip.
/// </summary>
public class VortexMapTests
{
    private static string FixturePath =>
        TestDataPath.Resolve(Path.Combine("upstream-compat", "map.vortex"));

    [Fact]
    public async Task MapDType_BecomesArrowMapType()
    {
        await using var reader = await VortexFileReader.OpenAsync(FixturePath);

        // attrs: map(i32, utf8?, keys_sorted=false), non-nullable.
        var attrs = reader.Schema.FieldsList[0];
        var attrsType = Assert.IsType<MapType>(attrs.DataType);
        Assert.False(attrs.IsNullable);
        Assert.False(attrsType.KeySorted);
        Assert.IsType<Int32Type>(attrsType.KeyField.DataType);
        Assert.False(attrsType.KeyField.IsNullable);
        Assert.IsType<StringType>(attrsType.ValueField.DataType);
        Assert.True(attrsType.ValueField.IsNullable);
        // Arrow models a map as List<Struct{key, value}>; the entries struct is never null.
        Assert.False(attrsType.Fields[0].IsNullable);

        // sorted_attrs: map(utf8, i64, keys_sorted=true), non-nullable, non-nullable values.
        var sorted = reader.Schema.FieldsList[1];
        var sortedType = Assert.IsType<MapType>(sorted.DataType);
        Assert.False(sorted.IsNullable);
        Assert.True(sortedType.KeySorted);
        Assert.IsType<StringType>(sortedType.KeyField.DataType);
        Assert.IsType<Int64Type>(sortedType.ValueField.DataType);
        Assert.False(sortedType.ValueField.IsNullable);

        // nullable_attrs: the same map type as attrs, but the map itself may be null.
        var nullable = reader.Schema.FieldsList[2];
        Assert.IsType<MapType>(nullable.DataType);
        Assert.True(nullable.IsNullable);
    }

    [Fact]
    public async Task ReadsMapValues_KeepingEntryOrderDuplicatesAndNulls()
    {
        await using var reader = await VortexFileReader.OpenAsync(FixturePath);

        var attrs = Assert.IsType<MapArray>(await reader.ReadColumnAsync(0));
        Assert.Equal(4, attrs.Length);
        Assert.Equal(0, attrs.NullCount);
        // Duplicate keys keep both entries, in file order; a value may be null.
        Assert.Equal("1=one, 2=null", Render(attrs));
        Assert.Equal("", Render(attrs, 1));
        Assert.Equal("1=dup-old, 1=dup-new", Render(attrs, 2));
        Assert.Equal("5=five", Render(attrs, 3));

        var sorted = Assert.IsType<MapArray>(await reader.ReadColumnAsync(1));
        Assert.Equal("a=1, b=2", Render(sorted));
        Assert.Equal("z=26", Render(sorted, 1));
        Assert.Equal("", Render(sorted, 2));
        Assert.Equal("k=11, m=13, n=14", Render(sorted, 3));

        var nullable = Assert.IsType<MapArray>(await reader.ReadColumnAsync(2));
        Assert.Equal(1, nullable.NullCount);
        // A null map and an empty map are different values, not two spellings of one.
        Assert.True(nullable.IsNull(0));
        Assert.True(nullable.IsValid(1));
        Assert.Equal("", Render(nullable, 1));
        Assert.Equal("7=seven", Render(nullable, 2));
        Assert.Equal("8=null", Render(nullable, 3));
    }

    /// <summary>One map row as <c>key=value</c> entries in order, or <c>null</c> for a null map.</summary>
    private static string Render(MapArray map, int row = 0)
    {
        if (map.IsNull(row)) return "null";
        var entries = map.KeyValues;
        var offsets = map.ValueOffsets;
        var items = new List<string>();
        for (int i = offsets[row]; i < offsets[row + 1]; i++)
            items.Add($"{Cell(entries.Fields[0], i)}={Cell(entries.Fields[1], i)}");
        return string.Join(", ", items);
    }

    private static string Cell(IArrowArray array, int index) => array switch
    {
        Int32Array a => a.IsNull(index) ? "null" : a.GetValue(index)!.Value.ToString(),
        Int64Array a => a.IsNull(index) ? "null" : a.GetValue(index)!.Value.ToString(),
        StringArray a => a.IsNull(index) ? "null" : a.GetString(index),
        _ => throw new NotSupportedException($"No rendering for {array.GetType().Name}."),
    };
}
