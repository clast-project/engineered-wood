// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <see cref="ArrowCompute.Concatenate"/>: the types Apache.Arrow's concatenator refuses
/// (apache/arrow-dotnet#443), which EngineeredWood reads every day.
/// </summary>
public class ConcatenateTests
{
    [Fact]
    public void Guid_KeepsItsTypeAndValues()
    {
        Guid[] first = [Guid.NewGuid(), Guid.NewGuid()], second = [Guid.NewGuid()];
        var result = ArrowCompute.Concatenate([Guids(first), Guids(second)]);

        var guids = Assert.IsType<GuidArray>(result);
        Assert.Equal(first.Concat(second), Enumerable.Range(0, guids.Length).Select(i => guids.GetGuid(i)!.Value));
    }

    [Fact]
    public void Variant_KeepsItsTypeAndValues()
    {
        byte[] metadata = [0x01, 0x00, 0x00];
        byte[][] values = [[0x04], [0x08], [0x0C, 0x2A]];
        var a = new VariantArray.Builder();
        a.Append(metadata, values[0]);
        a.AppendNull();
        var b = new VariantArray.Builder();
        b.Append(metadata, values[1]);
        b.Append(metadata, values[2]);

        var result = Assert.IsType<VariantArray>(
            ArrowCompute.Concatenate([a.Build(allocator: null), b.Build(allocator: null)]));

        Assert.Equal(4, result.Length);
        Assert.Equal(values[0], result.GetValueBytes(0).ToArray());
        Assert.True(result.IsNull(1));
        Assert.Equal(values[1], result.GetValueBytes(2).ToArray());
        Assert.Equal(values[2], result.GetValueBytes(3).ToArray());
    }

    [Fact]
    public void UnregisteredExtension_OverSlicedInputs()
    {
        var type = new MoneyType();
        var first = type.CreateArray(RawArrays.Fixed(Int64Type.Default, new[] { 1L, 2L, 3L }, offset: 1));
        var second = type.CreateArray(RawArrays.Fixed(Int64Type.Default, new[] { 7L, 8L, 9L }, [true, false, true], offset: 0));

        var money = Assert.IsType<MoneyArray>(ArrowCompute.Concatenate([first, second]));

        Assert.Same(type, money.Data.DataType);
        Assert.Equal(new long?[] { 2, 3, 7, null, 9 }, Enumerable.Range(0, money.Length).Select(money.GetAmount));
    }

    [Fact]
    public void ExtensionOverStructStorage()
    {
        var type = new PairType();
        var result = Assert.IsType<PairArray>(ArrowCompute.Concatenate([Pairs(type, (1, "a")), Pairs(type, (2, "b"), (3, null))]));

        var a = (Int32Array)result.Inner.Fields[0];
        var b = (StringArray)result.Inner.Fields[1];
        Assert.Equal(new int?[] { 1, 2, 3 }, Enumerable.Range(0, 3).Select(i => a.GetValue(i)));
        Assert.Equal(new[] { "a", "b", null }, Enumerable.Range(0, 3).Select(i => b.GetString(i)));
    }

    /// <summary>
    /// A VARIANT inside a struct is the case a top-level workaround misses: Arrow's concatenator
    /// recurses into the child and refuses it there.
    /// </summary>
    [Fact]
    public void ExtensionInsideAStruct()
    {
        var money = new MoneyType();
        var structType = new StructType([new Field("id", Int32Type.Default, false), new Field("m", money, true)]);
        StructArray Build(int[] ids, long[] amounts) => new(
            structType, ids.Length,
            [new Int32Array.Builder().AppendRange(ids).Build(), money.CreateArray(RawArrays.Fixed(Int64Type.Default, amounts))],
            ArrowBuffer.Empty, nullCount: 0);

        var result = Assert.IsType<StructArray>(ArrowCompute.Concatenate([Build([1, 2], [10, 20]), Build([3], [30])]));

        Assert.Same(structType, result.Data.DataType);
        var m = Assert.IsType<MoneyArray>(result.Fields[1]);
        Assert.Equal(new long?[] { 10, 20, 30 }, Enumerable.Range(0, 3).Select(m.GetAmount));
    }

    [Fact]
    public void ExtensionInsideAList()
    {
        var listType = new ListType(new Field("item", GuidType.Default, true));
        Guid[] all = [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()];
        ListArray Build(Guid[] items, int[] offsets) => new(
            listType, offsets.Length - 1,
            new ArrowBuffer.Builder<int>().AppendRange(offsets).Build(),
            Guids(items), ArrowBuffer.Empty, nullCount: 0);

        // [[g0, g1]] + [[], [g2]]
        var result = Assert.IsType<ListArray>(ArrowCompute.Concatenate([Build(all.Take(2).ToArray(), [0, 2]), Build(all.Skip(2).ToArray(), [0, 0, 1])]));

        Assert.Equal(3, result.Length);
        Assert.Equal(0, result.GetValueLength(1));
        var items = Assert.IsType<GuidArray>(result.Values);
        Assert.Equal(all, Enumerable.Range(0, 3).Select(i => items.GetGuid(i)!.Value));
    }

    /// <summary>An extension over another extension: both layers must come off before Arrow sees it.</summary>
    [Fact]
    public void StackedExtension_KeepsBothLayers()
    {
        var money = new MoneyType();
        var outer = new TakeExtensionTests.WrappedType(money);
        IArrowArray Build(long[] values) => outer.CreateArray(money.CreateArray(RawArrays.Fixed(Int64Type.Default, values)));

        var result = Assert.IsType<TakeExtensionTests.WrappedArray>(ArrowCompute.Concatenate([Build([1, 2]), Build([3])]));

        Assert.Same(outer, result.Data.DataType);
        var inner = Assert.IsType<MoneyArray>(result.Storage);
        Assert.Equal(new long?[] { 1, 2, 3 }, Enumerable.Range(0, 3).Select(inner.GetAmount));
    }

    /// <summary>
    /// Two extension types over the same storage are different types. The storage would concatenate,
    /// and relabelling would silently give the second input the first one's type.
    /// </summary>
    [Fact]
    public void DifferentExtensionTypes_AreRefused()
    {
        var money = new MoneyType().CreateArray(RawArrays.Fixed(Int64Type.Default, new[] { 1L }));
        var wrapped = new TakeExtensionTests.WrappedType(Int64Type.Default).CreateArray(RawArrays.Fixed(Int64Type.Default, new[] { 2L }));

        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([money, wrapped]));
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([wrapped, money]));
    }

    /// <summary>Separate instances of one extension type are the same type.</summary>
    [Fact]
    public void SameExtensionType_FromSeparateInstances_Concatenates()
    {
        var a = new MoneyType().CreateArray(RawArrays.Fixed(Int64Type.Default, new[] { 1L }));
        var b = new MoneyType().CreateArray(RawArrays.Fixed(Int64Type.Default, new[] { 2L }));

        var result = Assert.IsType<MoneyArray>(ArrowCompute.Concatenate([a, b]));
        Assert.Equal(new long?[] { 1, 2 }, Enumerable.Range(0, 2).Select(result.GetAmount));
    }

    [Fact]
    public void NullArrays_AreCounted()
    {
        var result = Assert.IsType<NullArray>(ArrowCompute.Concatenate([new NullArray(3), new NullArray(0), new NullArray(2)]));
        Assert.Equal(5, result.Length);
    }

    /// <summary>
    /// An empty slice of a view array keeps its parent's data buffers, which makes Arrow's
    /// concatenator overrun its buffer list.
    /// </summary>
    [Fact]
    public void EmptyViewInput_IsSkipped()
    {
        var parent = Views("a value longer than twelve bytes");
        var empty = new StringViewArray(parent.Data.Slice(0, 0));
        var b = Views("another value longer than twelve");

        var result = Assert.IsType<StringViewArray>(ArrowCompute.Concatenate([empty, b, empty]));

        Assert.Equal(1, result.Length);
        Assert.Equal("another value longer than twelve", result.GetString(0));
    }

    [Fact]
    public void AllEmpty_ReturnsAnEmptyArrayOfTheType()
    {
        var empty = new Int32Array.Builder().Build();
        var result = ArrowCompute.Concatenate([empty, empty]);
        Assert.IsType<Int32Array>(result);
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public void OrdinaryTypes_StillConcatenate()
    {
        var result = Assert.IsType<StringArray>(ArrowCompute.Concatenate(
            [new StringArray.Builder().Append("a").AppendNull().Build(), new StringArray.Builder().Append("c").Build()]));
        Assert.Equal(new[] { "a", null, "c" }, Enumerable.Range(0, 3).Select(i => result.GetString(i)));
    }

    [Fact]
    public void NoInput_Throws()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([]));
    }

    private static GuidArray Guids(Guid[] values)
    {
        var builder = new GuidArray.Builder();
        foreach (var value in values)
            builder.Append(value);
        return builder.Build(allocator: null);
    }

    private static PairArray Pairs(PairType type, params (int A, string? B)[] rows)
    {
        var a = new Int32Array.Builder();
        var b = new StringArray.Builder();
        foreach (var (x, y) in rows)
        {
            a.Append(x);
            if (y is null) b.AppendNull(); else b.Append(y);
        }

        var storage = new StructArray((StructType)type.StorageType, rows.Length, [a.Build(), b.Build()], ArrowBuffer.Empty, nullCount: 0);
        return (PairArray)type.CreateArray(storage);
    }

    private static StringViewArray Views(params string[] values)
    {
        var builder = new StringViewArray.Builder();
        foreach (var value in values)
            builder.Append(value);
        return builder.Build();
    }
}
