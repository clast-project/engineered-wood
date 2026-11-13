// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Encodings;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// The decoders read their children through spans and keep no root, which is safe only while every buffer they read
/// is managed (#837). A chunked child was joined by Arrow's concatenator, into native memory; these pin that the join
/// is now managed, and that it still joins sliced, nullable chunks correctly.
/// </summary>
public class ManagedArraysTests
{
    private static bool IsManaged(ArrowBuffer buffer) =>
        buffer.IsEmpty || MemoryMarshal.TryGetArray(buffer.Memory, out _);

    private static void AssertManaged(IArrowArray array)
    {
        foreach (var buffer in array.Data.Buffers)
            Assert.True(IsManaged(buffer));
    }

    [Fact]
    public void Concatenate_FixedWidth_JoinsSlicedNullableChunksIntoManagedBuffers()
    {
        // Arrow's builders allocate native memory: the inputs are what Arrow's concatenator used to be handed.
        var first = new Int64Array.Builder().Append(1).AppendNull().Append(3).Append(4).Build();
        var second = new Int64Array.Builder().Append(5).Append(6).AppendNull().Build();
        var sliced = (Int64Array)first.Slice(1, 3); // [null, 3, 4], offset 1

        var joined = (Int64Array)ManagedArrays.Concatenate(new IArrowArray[] { sliced, second });

        Assert.Equal(new long?[] { null, 3, 4, 5, 6, null }, Enumerable.Range(0, joined.Length).Select(joined.GetValue));
        Assert.Equal(2, joined.NullCount);
        AssertManaged(joined);
        GC.KeepAlive(first);
        GC.KeepAlive(second);
    }

    [Fact]
    public void Concatenate_WithoutNulls_HasNoBitmap()
    {
        var joined = ManagedArrays.Concatenate(new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Append(2).Build(),
            new Int32Array.Builder().Append(3).Build(),
        });

        Assert.Equal(0, joined.NullCount);
        Assert.True(joined.Data.Buffers[0].IsEmpty);
        Assert.Equal(new int?[] { 1, 2, 3 }, Enumerable.Range(0, 3).Select(((Int32Array)joined).GetValue));
    }

    [Fact]
    public void Concatenate_Boolean_JoinsBitsAtAnyOffset()
    {
        var first = new BooleanArray.Builder().Append(true).Append(false).Append(true).AppendNull().Append(false).Build();
        var second = new BooleanArray.Builder().Append(false).Append(true).Build();
        var sliced = (BooleanArray)first.Slice(2, 3); // [true, null, false], offset 2

        var joined = (BooleanArray)ManagedArrays.Concatenate(new IArrowArray[] { sliced, second });

        Assert.Equal(new bool?[] { true, null, false, false, true }, Enumerable.Range(0, joined.Length).Select(joined.GetValue));
        AssertManaged(joined);
        GC.KeepAlive(first);
        GC.KeepAlive(second);
    }

    [Fact]
    public void Concatenate_Decimal_KeepsTypeAndValues()
    {
        var type = new Decimal128Type(10, 2);
        var first = new Decimal128Array.Builder(type).Append(1.25m).AppendNull().Build();
        var second = new Decimal128Array.Builder(type).Append(-3.50m).Build();

        var joined = (Decimal128Array)ManagedArrays.Concatenate(new IArrowArray[] { first, second });

        Assert.Same(type, joined.Data.DataType);
        Assert.Equal(new decimal?[] { 1.25m, null, -3.50m }, Enumerable.Range(0, 3).Select(joined.GetValue));
        AssertManaged(joined);
    }

    [Fact]
    public void Concatenate_String_GoesThroughArrowCompute()
    {
        var joined = (StringArray)ManagedArrays.Concatenate(new IArrowArray[]
        {
            new StringArray.Builder().Append("a").AppendNull().Build(),
            new StringArray.Builder().Append("bc").Build(),
        });

        Assert.Equal(new[] { "a", null, "bc" }, Enumerable.Range(0, 3).Select(i => joined.GetString(i)));
        AssertManaged(joined);
    }

    [Fact]
    public void Concatenate_SkipsEmptyChunks_AndReturnsALoneChunkAsItIs()
    {
        var only = new Int32Array.Builder().Append(7).Build();
        var empty = new Int32Array.Builder().Build();

        Assert.Same(only, ManagedArrays.Concatenate(new IArrowArray[] { empty, only, empty }));
    }

    [Fact]
    public void Bitmap_IsManagedAndSetsValidRows()
    {
        var bitmap = ManagedArrays.Bitmap(10, i => i % 3 != 0);

        Assert.True(IsManaged(bitmap));
        for (int i = 0; i < 10; i++)
            Assert.Equal(i % 3 != 0, BitUtility.GetBit(bitmap.Span, i));
    }
}
