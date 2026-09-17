// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// <see cref="ArrowValues"/> decides whether EW and upstream vortex read a file alike, so it must
/// render the logical row of a sliced array, and must tell apart types that differ only in a
/// nested field's nullability or a map's key sortedness.
/// </summary>
public class ArrowValuesTests
{
    // Apache.Arrow applies a slice's offset inside ValueOffsets, Sizes and StructArray.Fields, so
    // the renderer indexes them by logical row. These pin that: a renderer that added the offset
    // again would read rows 4 and 5, past the two-row slice.

    [Fact]
    public void RendersTheLogicalRowOfASlicedList()
    {
        var b = new ListArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)b.ValueBuilder;
        for (int i = 0; i < 4; i++) { b.Append(); values.Append(i * 10); values.Append(i * 10 + 1); }

        var sliced = ArrowArrayFactory.Slice(b.Build(), 2, 2);

        Assert.Equal("[20, 21]", ArrowValues.Render(sliced, 0));
        Assert.Equal("[30, 31]", ArrowValues.Render(sliced, 1));
    }

    [Fact]
    public void RendersTheLogicalRowOfASlicedListView()
    {
        var b = new ListViewArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)b.ValueBuilder;
        for (int i = 0; i < 4; i++) { b.Append(); values.Append(i * 10); values.Append(i * 10 + 1); }

        Assert.Equal("[20, 21]", ArrowValues.Render(ArrowArrayFactory.Slice(b.Build(), 2, 2), 0));
    }

    [Fact]
    public void RendersTheLogicalRowOfASlicedFixedSizeList()
    {
        var b = new FixedSizeListArray.Builder(Int32Type.Default, 2);
        var values = (Int32Array.Builder)b.ValueBuilder;
        for (int i = 0; i < 4; i++) { b.Append(); values.Append(i * 10); values.Append(i * 10 + 1); }

        Assert.Equal("[20, 21]", ArrowValues.Render(ArrowArrayFactory.Slice(b.Build(), 2, 2), 0));
    }

    [Fact]
    public void RendersTheLogicalRowOfASlicedMap()
    {
        var b = new MapArray.Builder(new MapType(Int32Type.Default, Int32Type.Default));
        for (int i = 0; i < 4; i++)
        {
            b.Append();
            ((Int32Array.Builder)b.KeyBuilder).Append(i);
            ((Int32Array.Builder)b.ValueBuilder).Append(i * 100);
        }

        Assert.Equal("{2: 200}", ArrowValues.Render(ArrowArrayFactory.Slice(b.Build(), 2, 2), 0));
    }

    [Fact]
    public void RendersTheLogicalRowOfASlicedStruct()
    {
        var x = new Int32Array.Builder().AppendRange(new[] { 0, 1, 2, 3 }).Build();
        var type = new StructType(new[] { new Field("x", Int32Type.Default, false) });
        var sliced = (StructArray)ArrowArrayFactory.Slice(
            new StructArray(type, 4, new IArrowArray[] { x }, ArrowBuffer.Empty, 0), 2, 2);

        Assert.Equal("{x: 2}", ArrowValues.Render(sliced, 0));
        // The reader's whole-row projection (StructFieldColumnPlan) relies on this too.
        Assert.Equal(2, sliced.Fields[0].Length);
        Assert.Equal(2, ((Int32Array)sliced.Fields[0]).GetValue(0));
    }

    [Fact]
    public void TypeNameCarriesNestedNullability()
    {
        Assert.NotEqual(
            ArrowValues.TypeName(new ListType(new Field("item", StringType.Default, nullable: true))),
            ArrowValues.TypeName(new ListType(new Field("item", StringType.Default, nullable: false))));
        Assert.Equal(
            ArrowValues.TypeName(new ListType(new Field("item", StringType.Default, nullable: true))),
            ArrowValues.TypeName(new ListViewType(new Field("element", StringViewType.Default, nullable: true))));
    }

    [Fact]
    public void TypeNameCarriesMapSortednessAndValueNullability()
    {
        string Map(bool sorted, bool valuesNullable) => ArrowValues.TypeName(new MapType(
            new Field("key", Int32Type.Default, false),
            new Field("value", StringType.Default, valuesNullable),
            keySorted: sorted));

        Assert.NotEqual(Map(sorted: true, valuesNullable: true), Map(sorted: false, valuesNullable: true));
        Assert.NotEqual(Map(sorted: false, valuesNullable: true), Map(sorted: false, valuesNullable: false));
    }
}
