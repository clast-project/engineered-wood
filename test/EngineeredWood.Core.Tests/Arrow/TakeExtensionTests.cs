// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// An extension type Apache.Arrow has never heard of, over Int64 storage. Nothing registers it, so a
/// gather that dispatched on the extension NAME would fall through on it.
/// </summary>
internal sealed class MoneyType : ExtensionType
{
    public MoneyType() : base(Int64Type.Default) { }

    public override string Name => "test.money";

    public override string ExtensionMetadata => "currency=USD";

    public override ExtensionArray CreateArray(IArrowArray storage) => new MoneyArray(this, storage);
}

internal sealed class MoneyArray : ExtensionArray
{
    public MoneyArray(MoneyType type, IArrowArray storage) : base(type, storage) { }

    public long? GetAmount(int index) =>
        ((Int64Array)Storage).GetValue(index);
}

/// <summary>
/// An extension type over STRUCT storage, the shape VARIANT uses. Extensions over nested storage exercise
/// a different recursion path than extensions over a primitive.
/// </summary>
internal sealed class PairType : ExtensionType
{
    public PairType() : base(new StructType(
    [
        new Field("a", Int32Type.Default, true),
        new Field("b", StringType.Default, true),
    ]))
    { }

    public override string Name => "test.pair";

    public override string ExtensionMetadata => string.Empty;

    public override ExtensionArray CreateArray(IArrowArray storage) => new PairArray(this, storage);
}

internal sealed class PairArray : ExtensionArray
{
    public PairArray(PairType type, IArrowArray storage) : base(type, storage) { }

    public StructArray Inner => (StructArray)Storage;
}

public class TakeExtensionTests
{
    private static MoneyArray Money(long[] values, bool[]? valid = null, int offset = 0)
    {
        var storage = RawArrays.Fixed(Int64Type.Default, values, valid, offset);
        var type = new MoneyType();
        return (MoneyArray)type.CreateArray(storage);
    }

    [Fact]
    public void UnknownExtensionType_GathersAndKeepsItsOwnTypeInstance()
    {
        var type = new MoneyType();
        var storage = RawArrays.Fixed(Int64Type.Default, new[] { 100L, 200L, 300L, 400L });
        var array = type.CreateArray(storage);

        var result = ArrowCompute.Take(array, (int[])[3, 0, 2]);

        // The gather must not need to recognise "test.money" — it reuses the type instance already on the
        // array, so an unregistered extension survives exactly like a built-in one.
        var money = Assert.IsType<MoneyArray>(result);
        Assert.Same(type, money.Data.DataType);
        Assert.Equal("test.money", ((ExtensionType)money.Data.DataType).Name);

        Assert.Equal(400L, money.GetAmount(0));
        Assert.Equal(100L, money.GetAmount(1));
        Assert.Equal(300L, money.GetAmount(2));
    }

    [Fact]
    public void UnknownExtensionType_GathersNulls()
    {
        var array = Money([1L, 2L, 3L, 4L], [true, false, true, false]);

        var money = Assert.IsType<MoneyArray>(ArrowCompute.Take(array, (int[])[1, 2, 3, 0]));

        Assert.Null(money.GetAmount(0));
        Assert.Equal(3L, money.GetAmount(1));
        Assert.Null(money.GetAmount(2));
        Assert.Equal(1L, money.GetAmount(3));
    }

    [Fact]
    public void SlicedExtensionArray_ReadsFromLogicalRow()
    {
        // The storage is built with a logical offset, so the extension array's view starts partway in.
        // If Storage were handed back without the offset, this would silently read the skipped rows.
        var array = Money([10L, 20L, 30L, 40L, 50L], offset: 2);
        Assert.Equal(3, array.Length);

        var money = Assert.IsType<MoneyArray>(ArrowCompute.Take(array, (int[])[0, 2]));

        Assert.Equal(30L, money.GetAmount(0));
        Assert.Equal(50L, money.GetAmount(1));
    }

    [Fact]
    public void ExtensionSlicedAfterConstruction_ReadsFromLogicalRow()
    {
        // Sharper than slicing the storage before wrapping: here the offset sits on the EXTENSION array's
        // own ArrayData while its buffers still cover all five rows. If ExtensionArray.Storage ignored that
        // offset, the gather would silently read the skipped rows.
        var type = new MoneyType();
        var full = type.CreateArray(
            RawArrays.Fixed(Int64Type.Default, new[] { 10L, 20L, 30L, 40L, 50L }));

        var sliced = (ExtensionArray)ArrowArrayFactory.BuildArray(new ArrayData(
            type, length: 3, nullCount: 0, offset: 2,
            full.Data.Buffers, full.Data.Children));
        Assert.Equal(3, sliced.Length);

        var result = ArrowCompute.Take(sliced, (int[])[0, 2]);

        var money = Assert.IsAssignableFrom<ExtensionArray>(result);
        Assert.Equal(30L, ((Int64Array)money.Storage).GetValue(0));
        Assert.Equal(50L, ((Int64Array)money.Storage).GetValue(1));
    }

    [Fact]
    public void ExtensionOverExtensionStorage_GathersThroughBothLayers()
    {
        // The gather recurses on storage, so a stacked extension should unwrap and rewrap at every layer
        // without either annotation being lost.
        var inner = new MoneyType();
        var outer = new WrappedType(inner);

        var innerArray = inner.CreateArray(
            RawArrays.Fixed(Int64Type.Default, new[] { 7L, 8L, 9L }));
        var outerArray = outer.CreateArray(innerArray);

        var result = ArrowCompute.Take(outerArray, (int[])[2, 0]);

        var outerExt = Assert.IsAssignableFrom<ExtensionArray>(result);
        Assert.Equal("test.wrapped", ((ExtensionType)outerExt.Data.DataType).Name);

        var innerExt = Assert.IsAssignableFrom<ExtensionArray>(outerExt.Storage);
        Assert.Equal("test.money", ((ExtensionType)innerExt.Data.DataType).Name);

        Assert.Equal(9L, ((Int64Array)innerExt.Storage).GetValue(0));
        Assert.Equal(7L, ((Int64Array)innerExt.Storage).GetValue(1));
    }

    internal sealed class WrappedType : ExtensionType
    {
        public WrappedType(IArrowType storage) : base(storage) { }

        public override string Name => "test.wrapped";

        public override string ExtensionMetadata => string.Empty;

        public override ExtensionArray CreateArray(IArrowArray storage) =>
            new WrappedArray(this, storage);
    }

    internal sealed class WrappedArray : ExtensionArray
    {
        public WrappedArray(WrappedType type, IArrowArray storage) : base(type, storage) { }
    }

    [Fact]
    public void ExtensionOverStructStorage_GathersThroughNestedStorage()
    {
        var type = new PairType();
        var storageType = (StructType)type.StorageType;

        var a = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 });
        var b = RawArrays.VarBinary(StringType.Default, ["x", "y", "z"], large: false);
        var storage = RawArrays.Struct(storageType, 3, [a, b]);
        var array = type.CreateArray(storage);

        var result = ArrowCompute.Take(array, (int[])[2, 0]);

        var pair = Assert.IsType<PairArray>(result);
        Assert.Same(type, pair.Data.DataType);
        Assert.Equal(2, pair.Length);

        Assert.Equal(3, ((Int32Array)pair.Inner.Fields[0]).GetValue(0));
        Assert.Equal(1, ((Int32Array)pair.Inner.Fields[0]).GetValue(1));
        Assert.Equal("z", ((StringArray)pair.Inner.Fields[1]).GetString(0));
        Assert.Equal("x", ((StringArray)pair.Inner.Fields[1]).GetString(1));
    }

    [Fact]
    public void ExtensionNestedAsStructChild_KeepsItsAnnotation()
    {
        // TakeStruct rebuilds each child from raw ArrayData. If that rebuild goes through a factory that
        // does not know the extension, the child comes back as bare storage and the batch's schema then
        // disagrees with its own column.
        var moneyType = new MoneyType();
        var money = moneyType.CreateArray(
            RawArrays.Fixed(Int64Type.Default, new[] { 100L, 200L, 300L }));

        var outerType = new StructType(
        [
            new Field("amount", moneyType, true),
            new Field("n", Int32Type.Default, true),
        ]);
        var outer = RawArrays.Struct(
            outerType, 3, [money, RawArrays.Fixed(Int32Type.Default, new[] { 7, 8, 9 })]);

        var result = Assert.IsType<StructArray>(ArrowCompute.Take(outer, (int[])[2, 0]));

        var child = result.Fields[0];
        Assert.Equal(ArrowTypeId.Extension, child.Data.DataType.TypeId);
        var childExt = Assert.IsAssignableFrom<ExtensionType>(child.Data.DataType);
        Assert.Equal("test.money", childExt.Name);

        var storage = Assert.IsAssignableFrom<ExtensionArray>(child).Storage;
        Assert.Equal(300L, ((Int64Array)storage).GetValue(0));
        Assert.Equal(100L, ((Int64Array)storage).GetValue(1));
    }

    [Fact]
    public void ExtensionOverListStorage_GathersThroughStorageAndKeepsAnnotation()
    {
        // A list is nested storage of a third shape — its child is reached through its offsets rather than
        // being parallel to its rows, so it exercises a recursion path neither the primitive nor the struct
        // extension case does.
        var values = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4, 5 });
        var listType = new ListType(new Field("item", Int32Type.Default, true));
        var list = RawArrays.List(listType, [0, 2, 2, 5], values);

        var wrapperType = new WrapperType(listType);
        var array = wrapperType.CreateArray(list);

        var result = ArrowCompute.Take(array, (int[])[2, 0, 1]);

        var ext = Assert.IsType<WrapperArray>(result);
        Assert.Equal("test.wrapper", ((ExtensionType)ext.Data.DataType).Name);

        var storage = Assert.IsType<ListArray>(ext.Storage);
        Assert.Equal(3, storage.Length);
        Assert.Equal(3, ((Int32Array)storage.GetSlicedValues(0)!).Length);   // source row 2 → [3,4,5]
        Assert.Equal(2, ((Int32Array)storage.GetSlicedValues(1)!).Length);   // source row 0 → [1,2]
        Assert.Equal(0, ((Int32Array)storage.GetSlicedValues(2)!).Length);   // source row 1 → []
    }

    [Fact]
    public void ExtensionOverUnsupportedStorage_Throws()
    {
        // An extension does not make an ungatherable storage type gatherable; it must still refuse rather
        // than hand back a wrong-length column.
        var indices = new Int32Array.Builder().Append(0).Append(1).Build();
        var dictionary = new StringArray.Builder().Append("alpha").Append("bravo").Build();
        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, ordered: false);
        var dict = new DictionaryArray(dictType, indices, dictionary);

        var array = new WrapperType(dictType).CreateArray(dict);

        Assert.Throws<NotSupportedException>(() => ArrowCompute.Take(array, (int[])[1, 0]));
    }

    /// <summary>An extension over whatever storage a test hands it, so one type covers both cases above.</summary>
    internal sealed class WrapperType : ExtensionType
    {
        public WrapperType(IArrowType storage) : base(storage) { }

        public override string Name => "test.wrapper";

        public override string ExtensionMetadata => string.Empty;

        public override ExtensionArray CreateArray(IArrowArray storage) =>
            new WrapperArray(this, storage);
    }

    internal sealed class WrapperArray : ExtensionArray
    {
        public WrapperArray(WrapperType type, IArrowArray storage) : base(type, storage) { }
    }
}
