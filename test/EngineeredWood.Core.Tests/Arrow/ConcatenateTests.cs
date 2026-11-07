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

    /// <summary>
    /// Structs are joined child by child, by position, under the first input's type. Inputs whose fields differ in
    /// name, order or nullability must be refused, or values land under the wrong field (#733).
    /// </summary>
    [Theory]
    [InlineData("a", "b")]          // a different name
    [InlineData("a!", "a")]         // NOT NULL onto nullable
    [InlineData("a", "a!")]         // nullable onto NOT NULL
    [InlineData("a,b", "b,a")]      // the same fields in another order: the values would swap
    [InlineData("a", "a,b")]        // a different field count
    [InlineData("a,b", "a")]
    public void StructsOfDifferentFields_AreRefused(string first, string second)
    {
        var ex = Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([Struct(first), Struct(second)]));
        Assert.Contains("their types differ", ex.Message);
    }

    [Fact]
    public void StructsOfDifferentFields_InsideAList_AreRefused()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([ListOf(Struct("a,b")), ListOf(Struct("b,a"))]));
    }

    [Fact]
    public void StructsOfDifferentFields_InsideAStruct_AreRefused()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([Wrap(Struct("a,b")), Wrap(Struct("b,a"))]));
    }

    [Fact]
    public void StructsOfTheSameFields_FromSeparateTypeInstances_Concatenate()
    {
        var result = Assert.IsType<StructArray>(ArrowCompute.Concatenate([Struct("a!,b"), Struct("a!,b")]));
        Assert.Equal(new int?[] { 1, 1 }, Enumerable.Range(0, 2).Select(i => ((Int32Array)result.Fields[0]).GetValue(i)));
        Assert.Equal(new int?[] { 2, 2 }, Enumerable.Range(0, 2).Select(i => ((Int32Array)result.Fields[1]).GetValue(i)));
    }

    /// <summary>The message spells out the fields, so the difference can be read off it.</summary>
    [Fact]
    public void Refusal_NamesTheFields()
    {
        var ex = Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([Struct("a!,b"), Struct("b,a")]));
        Assert.Contains("struct<b: int32, a: int32> onto struct<a: int32 not null, b: int32>", ex.Message);
    }

    /// <summary>
    /// A union row names its branch by a type id, which the type maps onto a field. Unions with the same fields
    /// but a different mapping would be joined under the first input's mapping, so a row of the second input
    /// that picks <c>a</c> would read as <c>b</c>.
    /// </summary>
    [Fact]
    public void UnionsOfDifferentTypeIdMappings_AreRefused()
    {
        var first = OneRowUnion(UnionMode.Sparse, typeIds: [0, 1], pick: 0);
        var second = OneRowUnion(UnionMode.Sparse, typeIds: [1, 0], pick: 1);

        var ex = Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([first, second]));
        Assert.Contains("ids [1, 0]", ex.Message);
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([Wrap(first), Wrap(second)]));
    }

    [Fact]
    public void UnionsOfDifferentModes_AreRefused()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate(
            [OneRowUnion(UnionMode.Sparse, [0, 1], pick: 0), OneRowUnion(UnionMode.Dense, [0, 1], pick: 0)]));
    }

    [Fact]
    public void UnionsOfTheSameType_FromSeparateTypeInstances_Concatenate()
    {
        var result = Assert.IsType<SparseUnionArray>(ArrowCompute.Concatenate(
            [OneRowUnion(UnionMode.Sparse, [3, 5], pick: 3), OneRowUnion(UnionMode.Sparse, [3, 5], pick: 5)]));

        Assert.Equal(new byte[] { 3, 5 }, result.TypeIds.ToArray());
    }

    [Fact]
    public void DifferentLeafTypes_AreRefused()
    {
        var ints = new Int32Array.Builder().Append(1).Build();
        var longs = new Int64Array.Builder().Append(2).Build();
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([ints, longs]));
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([new NullArray(1), ints]));
    }

    /// <summary>
    /// Arrow's concatenator widens top-level decimals that differ only in precision, which loses nothing; that is
    /// kept. A different scale is refused.
    /// </summary>
    [Fact]
    public void TopLevelDecimals_OfDifferentPrecision_Widen()
    {
        var result = ArrowCompute.Concatenate([Decimals(10, 2, 1.25m), Decimals(18, 2, 3.5m)]);

        var type = Assert.IsType<Decimal128Type>(result.Data.DataType);
        Assert.Equal((18, 2), (type.Precision, type.Scale));
        var values = Assert.IsType<Decimal128Array>(result);
        Assert.Equal(new decimal?[] { 1.25m, 3.5m }, Enumerable.Range(0, 2).Select(i => values.GetValue(i)));
    }

    [Fact]
    public void Decimals_OfDifferentScale_AreRefused()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([Decimals(10, 2, 1.25m), Decimals(10, 3, 1.25m)]));
    }

    /// <summary>
    /// Inside a struct the joined parent keeps the first input's type, so a widened child would be mislabelled:
    /// there the precision must agree too.
    /// </summary>
    [Fact]
    public void NestedDecimals_OfDifferentPrecision_AreRefused()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate(
            [Wrap(Decimals(10, 2, 1.25m)), Wrap(Decimals(18, 2, 3.5m))]));
    }

    [Fact]
    public void NoInput_Throws()
    {
        Assert.Throws<ArgumentException>(() => ArrowCompute.Concatenate([]));
    }

    /// <summary>
    /// A one-row struct of int32 children, one per comma-separated name (a trailing <c>!</c> makes it NOT NULL),
    /// each holding its position: 1, 2, and so on.
    /// </summary>
    private static StructArray Struct(string spec)
    {
        var fields = new List<Field>();
        var children = new List<IArrowArray>();
        foreach (var (name, i) in spec.Split(',').Select((n, i) => (n, i)))
        {
            fields.Add(new Field(name.TrimEnd('!'), Int32Type.Default, nullable: !name.EndsWith("!", StringComparison.Ordinal)));
            children.Add(new Int32Array.Builder().Append(i + 1).Build());
        }
        return new StructArray(new StructType(fields), 1, children, ArrowBuffer.Empty, nullCount: 0);
    }

    private static StructArray Wrap(IArrowArray child) => new(
        new StructType([new Field("inner", child.Data.DataType, nullable: true)]),
        child.Length, [child], ArrowBuffer.Empty, nullCount: 0);

    private static ListArray ListOf(IArrowArray values)
    {
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(values.Length).Build();
        return new ListArray(new ListType(new Field("item", values.Data.DataType, nullable: true)),
            1, offsets, values, ArrowBuffer.Empty, nullCount: 0);
    }

    /// <summary>
    /// A one-row union of two int32 branches, <c>a</c> (holding 1) and <c>b</c> (holding 2), whose type ids are
    /// <paramref name="typeIds"/>; the row picks the branch whose id is <paramref name="pick"/>.
    /// </summary>
    private static UnionArray OneRowUnion(UnionMode mode, int[] typeIds, byte pick)
    {
        var type = new UnionType(
            [new Field("a", Int32Type.Default, nullable: true), new Field("b", Int32Type.Default, nullable: true)],
            typeIds, mode);
        var ids = new ArrowBuffer.Builder<byte>().Append(pick).Build();
        IArrowArray[] children = [new Int32Array.Builder().Append(1).Build(), new Int32Array.Builder().Append(2).Build()];
        if (mode == UnionMode.Sparse)
            return new SparseUnionArray(type, 1, children, ids, nullCount: 0);

        // Dense: only the picked branch holds a row, at offset 0.
        int picked = System.Array.IndexOf(typeIds, (int)pick);
        children = [
            picked == 0 ? children[0] : new Int32Array.Builder().Build(),
            picked == 1 ? children[1] : new Int32Array.Builder().Build(),
        ];
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Build();
        return new DenseUnionArray(type, 1, children, ids, offsets, nullCount: 0);
    }

    private static Decimal128Array Decimals(int precision, int scale, decimal value) =>
        new Decimal128Array.Builder(new Decimal128Type(precision, scale)).Append(value).Build();

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
