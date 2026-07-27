// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// <c>ArrowCompute.MakeNullArray</c> backfills a column that a data file does not contain but the schema it
/// is read under declares. Two properties matter, and the per-type builder loops this replaced got both
/// wrong in places: every row must actually read as NULL, and the column must report the type that was
/// asked for — including the parameters a builder for a narrower type silently drops (timestamp unit and
/// timezone, decimal precision and scale, Large-vs-32-bit offsets, child types).
/// </summary>
public class MakeNullArrayTests
{
    /// <summary>
    /// Every fixed-width type, from the shared width table. Unlike the gather, half-float belongs here on
    /// every target: this method is reached from a SCHEMA rather than from an existing array, so a caller can
    /// ask for one on netstandard2.0 — where it must fail with an explanation. See
    /// <see cref="HalfFloat_OnNetstandard_ThrowsWithAnExplanation"/>.
    /// </summary>
    public static TheoryData<string> FixedWidthTypes
    {
        get
        {
            var cases = new TheoryData<string>();
            foreach (string name in FixedWidthCases.All)
            {
#if !NET6_0_OR_GREATER
                if (name == "halffloat") continue;
#endif
                cases.Add(name);
            }
            return cases;
        }
    }

    /// <summary>Asserts the shape every all-null column must have, whatever its type.</summary>
    private static void AssertAllNull(IArrowArray array, int length)
    {
        Assert.Equal(length, array.Length);
        Assert.Equal(length, array.Data.NullCount);
        Assert.Equal(0, array.Data.Offset);

        // The validity bitmap has to be ALLOCATED here. An absent one means all-valid, so the inverse of the
        // no-null shape (ArrowBuffer.Empty + nullCount 0) is not optional in this direction.
        if (length > 0)
        {
            Assert.True(
                array.Data.Buffers[0].Length > 0,
                "an all-null column needs a real validity bitmap; an absent one reads as all-valid");
        }

        var asArray = Assert.IsAssignableFrom<Apache.Arrow.Array>(array);
        for (int i = 0; i < length; i++)
            Assert.True(asArray.IsNull(i), $"row {i} did not read as null");
    }

    [Theory]
    [MemberData(nameof(FixedWidthTypes))]
    public void FixedWidth_AllRowsNullAndTypePreserved(string name)
    {
        var (type, width) = FixedWidthCases.Resolve(name);

        var result = ArrowCompute.MakeNullArray(type, 5);

        AssertAllNull(result, 5);

        // Reference equality is the strongest form of "nothing was re-tagged": a builder for a narrower type
        // would hand back a fresh instance with default unit/timezone/precision/scale.
        Assert.Same(type, result.Data.DataType);

        // The value buffer is sized and zeroed, so a consumer that reads slots without consulting validity
        // gets zeros rather than reading off the end of a short buffer.
        Assert.Equal(5 * width, result.Data.Buffers[1].Length);
        foreach (byte b in result.Data.Buffers[1].Span)
            Assert.Equal(0, b);
    }

    [Fact]
    public void Boolean_AllRowsNullWithZeroedValueBitmap()
    {
        // Longer than one bitmap byte so a single-byte allocation cannot pass.
        var result = Assert.IsType<BooleanArray>(ArrowCompute.MakeNullArray(BooleanType.Default, 10));

        AssertAllNull(result, 10);
        for (int i = 0; i < 10; i++)
            Assert.Null(result.GetValue(i));

        // Values are bit-packed, so the buffer is bitmap-sized rather than one byte per row.
        Assert.Equal(2, result.Data.Buffers[1].Length);
        foreach (byte b in result.Data.Buffers[1].Span)
            Assert.Equal(0, b);
    }

    [Fact]
    public void String_AllRowsNullWithMonotonicOffsets()
    {
        var result = Assert.IsType<StringArray>(ArrowCompute.MakeNullArray(StringType.Default, 4));

        AssertAllNull(result, 4);
        for (int i = 0; i < 4; i++)
            Assert.Null(result.GetString(i));

        // length + 1 offsets, all zero — every row is a zero-length slot and the run never goes backwards.
        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, int>(result.Data.Buffers[1].Span);
        Assert.Equal(5, offsets.Length);
        for (int i = 0; i < 5; i++)
            Assert.Equal(0, offsets[i]);
    }

    [Fact]
    public void Binary_AllRowsNull()
    {
        var result = Assert.IsType<BinaryArray>(ArrowCompute.MakeNullArray(BinaryType.Default, 3));

        AssertAllNull(result, 3);
        Assert.Equal(4 * sizeof(int), result.Data.Buffers[1].Length);
    }

    [Fact]
    public void LargeString_IsNotNarrowedToString()
    {
        var result = ArrowCompute.MakeNullArray(LargeStringType.Default, 3);

        // Narrowing to StringType would contradict the schema this column is being backfilled into.
        Assert.IsType<LargeStringArray>(result);
        Assert.Same(LargeStringType.Default, result.Data.DataType);
        AssertAllNull(result, 3);

        // 64-bit offsets, so the buffer is twice the width of the 32-bit case.
        Assert.Equal(4 * sizeof(long), result.Data.Buffers[1].Length);
    }

    [Fact]
    public void LargeBinary_IsNotNarrowedToBinary()
    {
        var result = ArrowCompute.MakeNullArray(LargeBinaryType.Default, 3);

        Assert.IsType<LargeBinaryArray>(result);
        Assert.Same(LargeBinaryType.Default, result.Data.DataType);
        AssertAllNull(result, 3);
        Assert.Equal(4 * sizeof(long), result.Data.Buffers[1].Length);
    }

    [Fact]
    public void Struct_ChildrenAreAllNullAtTheSameLength()
    {
        // A parameterised child type, so a child built by a defaulting builder would be visibly wrong.
        var tsType = new TimestampType(TimeUnit.Millisecond, "UTC");
        var structType = new StructType(
        [
            new Field("n", Int32Type.Default, true),
            new Field("s", StringType.Default, true),
            new Field("t", tsType, true),
        ]);

        var result = Assert.IsType<StructArray>(ArrowCompute.MakeNullArray(structType, 4));

        AssertAllNull(result, 4);
        Assert.Same(structType, result.Data.DataType);
        Assert.Equal(3, result.Fields.Count);

        // Children are parallel to the parent's rows: same length, and all-null themselves rather than
        // merely unreachable through the parent's validity bitmap.
        Assert.IsType<Int32Array>(result.Fields[0]);
        Assert.IsType<StringArray>(result.Fields[1]);
        var t = Assert.IsType<TimestampArray>(result.Fields[2]);
        Assert.Same(tsType, t.Data.DataType);

        foreach (var child in result.Fields)
            AssertAllNull(child, 4);
    }

    [Fact]
    public void Struct_RecursesIntoNestedStructs()
    {
        var inner = new StructType([new Field("deep", new Decimal128Type(38, 10), true)]);
        var outer = new StructType(
        [
            new Field("inner", inner, true),
            new Field("flat", Int64Type.Default, true),
        ]);

        var result = Assert.IsType<StructArray>(ArrowCompute.MakeNullArray(outer, 3));

        AssertAllNull(result, 3);
        var innerArray = Assert.IsType<StructArray>(result.Fields[0]);
        AssertAllNull(innerArray, 3);

        var deep = Assert.IsType<Decimal128Array>(innerArray.Fields[0]);
        AssertAllNull(deep, 3);
        var deepType = Assert.IsType<Decimal128Type>(deep.Data.DataType);
        Assert.Equal(38, deepType.Precision);
        Assert.Equal(10, deepType.Scale);
    }

    [Fact]
    public void List_RowsReadAsNullNotAsEmptyList()
    {
        var listType = new ListType(new Field("item", Int32Type.Default, true));

        var result = Assert.IsType<ListArray>(ArrowCompute.MakeNullArray(listType, 4));

        // The distinction that matters: a NULL list is not the same value as an empty list, and all-zero
        // offsets alone would produce the latter if the validity bitmap were missing.
        AssertAllNull(result, 4);
        Assert.Same(listType, result.Data.DataType);

        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, int>(result.Data.Buffers[1].Span);
        Assert.Equal(5, offsets.Length);
        for (int i = 0; i < 5; i++)
            Assert.Equal(0, offsets[i]);

        // The values child is empty — no row references any of its slots.
        Assert.Equal(0, result.Values.Length);
        Assert.IsType<Int32Array>(result.Values);
    }

    [Fact]
    public void List_RecursesIntoItsItemType()
    {
        var itemStruct = new StructType([new Field("x", new Time64Type(TimeUnit.Nanosecond), true)]);
        var listType = new ListType(new Field("item", itemStruct, true));

        var result = Assert.IsType<ListArray>(ArrowCompute.MakeNullArray(listType, 2));

        AssertAllNull(result, 2);
        var values = Assert.IsType<StructArray>(result.Values);
        Assert.Equal(0, values.Length);
        Assert.Same(itemStruct, values.Data.DataType);
    }

    [Fact]
    public void FixedSizeList_SizesItsChildByListSize()
    {
        var listType = new FixedSizeListType(new Field("item", Int32Type.Default, true), listSize: 3);

        var result = Assert.IsType<FixedSizeListArray>(ArrowCompute.MakeNullArray(listType, 4));

        AssertAllNull(result, 4);
        Assert.Same(listType, result.Data.DataType);

        // No offsets buffer at all: the child occupies length * listSize slots whether or not the parent
        // rows are null, so an empty child here would leave every row pointing past the end of it.
        Assert.Equal(4 * 3, result.Values.Length);
        AssertAllNull(result.Values, 12);
    }

    [Fact]
    public void Map_TakesTheListLayoutWithAnEmptyEntriesChild()
    {
        var mapType = new MapType(
            new Field("key", StringType.Default, false),
            new Field("value", Int32Type.Default, true));

        var result = Assert.IsType<MapArray>(ArrowCompute.MakeNullArray(mapType, 3));

        AssertAllNull(result, 3);
        Assert.Same(mapType, result.Data.DataType);

        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, int>(result.Data.Buffers[1].Span);
        Assert.Equal(4, offsets.Length);
        for (int i = 0; i < 4; i++)
            Assert.Equal(0, offsets[i]);

        // A map's child is the key/value entry struct, and no row references any entry.
        Assert.Equal(0, result.Values.Length);
    }

    [Fact]
    public void LargeList_UsesSixtyFourBitOffsets()
    {
        var listType = new LargeListType(new Field("item", Int64Type.Default, true));

        var result = ArrowCompute.MakeNullArray(listType, 3);

        AssertAllNull(result, 3);
        Assert.Same(listType, result.Data.DataType);
        Assert.Equal(4 * sizeof(long), result.Data.Buffers[1].Length);
    }

    [Fact]
    public void Extension_KeepsItsAnnotationAndBuildsThroughStorage()
    {
        // GuidType is a real Arrow extension over FixedSizeBinary(16) — no test-only subclass needed.
        var guidType = new GuidType();

        var result = ArrowCompute.MakeNullArray(guidType, 4);

        // Backfilling as bare storage would put a FixedSizeBinary column into a batch whose schema declares
        // the extension type — a silent mismatch rather than a failure.
        var ext = Assert.IsAssignableFrom<ExtensionArray>(result);
        var extType = Assert.IsAssignableFrom<ExtensionType>(ext.Data.DataType);
        Assert.Equal(guidType.Name, extType.Name);

        Assert.Equal(4, result.Length);
        AssertAllNull(ext.Storage, 4);
    }

    [Fact]
    public void Extension_OverStructStorageRecursesThroughIt()
    {
        // PairType is an extension over STRUCT storage — the VARIANT shape, and the case whose arm order
        // matters: a StructType-first switch would build bare storage and strip the annotation.
        var pairType = new PairType();

        var result = ArrowCompute.MakeNullArray(pairType, 3);

        var ext = Assert.IsAssignableFrom<ExtensionArray>(result);
        Assert.Equal("test.pair", Assert.IsAssignableFrom<ExtensionType>(ext.Data.DataType).Name);

        var inner = Assert.IsType<StructArray>(ext.Storage);
        AssertAllNull(inner, 3);
        foreach (var child in inner.Fields)
            AssertAllNull(child, 3);
    }

    [Fact]
    public void Extension_UnknownToArrowStillBuilds()
    {
        // Nothing registers MoneyType, so a factory that dispatched on the extension NAME would miss it.
        var moneyType = new MoneyType();

        var result = ArrowCompute.MakeNullArray(moneyType, 4);

        var ext = Assert.IsAssignableFrom<ExtensionArray>(result);
        Assert.Same(moneyType, ext.Data.DataType);
        AssertAllNull(ext.Storage, 4);
        Assert.IsType<Int64Array>(ext.Storage);
    }

    [Fact]
    public void NullType_NeedsNoBuffers()
    {
        var result = ArrowCompute.MakeNullArray(NullType.Default, 5);

        // A null array is all-null by construction and carries no buffers, so AssertAllNull's
        // validity-bitmap requirement does not apply to it.
        Assert.IsType<NullArray>(result);
        Assert.Equal(5, result.Length);
        Assert.Equal(5, result.Data.NullCount);
    }

    [Theory]
    [InlineData("int32")]
    [InlineData("timestamp_us_utc")]
    [InlineData("decimal128")]
    public void ZeroLength_IsWellFormed(string name)
    {
        var (type, _) = FixedWidthCases.Resolve(name);

        // A file with no rows still needs its absent columns backfilled at length 0, and the empty batch
        // must not carry a mis-sized buffer into whatever consumes it.
        var result = ArrowCompute.MakeNullArray(type, 0);

        Assert.Equal(0, result.Length);
        Assert.Equal(0, result.Data.NullCount);
        Assert.Same(type, result.Data.DataType);
    }

    [Fact]
    public void ZeroLength_VarWidthStillCarriesItsTerminatingOffset()
    {
        var result = ArrowCompute.MakeNullArray(StringType.Default, 0);

        Assert.Equal(0, result.Length);

        // Even with no rows there is one offset — dropping it leaves an offsets buffer Arrow cannot read.
        var offsets = System.Runtime.InteropServices.MemoryMarshal
            .Cast<byte, int>(result.Data.Buffers[1].Span);
        Assert.Equal(1, offsets.Length);
        Assert.Equal(0, offsets[0]);
    }

    // ── Unions ──
    //
    // A union is the one type here with no validity bitmap: the Arrow spec puts its nulls in the child that
    // each row's type id selects. So "all null" cannot be asserted by reading a parent bitmap — it has to be
    // read the way a consumer would, by following each row's type id into its branch.

    private static UnionType MakeUnionType(UnionMode mode, params int[] typeIds) =>
        new(
            [
                new Field("i", Int32Type.Default, nullable: true),
                new Field("s", StringType.Default, nullable: true),
            ],
            typeIds.Length > 0 ? typeIds : [0, 1],
            mode);

    [Fact]
    public void DenseUnion_EveryRowReadsAsNullThroughItsSelectedBranch()
    {
        var type = MakeUnionType(UnionMode.Dense);

        var result = Assert.IsType<DenseUnionArray>(ArrowCompute.MakeNullArray(type, 4));

        Assert.Equal(4, result.Length);

        // A dense union addresses its children through the offsets buffer, so only the selected branch
        // carries rows; the other stays empty rather than being padded.
        var selected = Assert.IsType<Int32Array>(result.Fields[0]);
        Assert.Equal(4, selected.Length);
        Assert.Equal(0, Assert.IsType<StringArray>(result.Fields[1]).Length);

        for (int i = 0; i < result.Length; i++)
        {
            Assert.Equal(0, result.TypeIds[i]);
            Assert.Equal(i, result.ValueOffsets[i]);
            Assert.True(selected.IsNull(result.ValueOffsets[i]));
        }
    }

    [Fact]
    public void SparseUnion_EveryBranchIsParallelToTheParent()
    {
        var type = MakeUnionType(UnionMode.Sparse);

        var result = Assert.IsType<SparseUnionArray>(ArrowCompute.MakeNullArray(type, 3));

        Assert.Equal(3, result.Length);

        // Sparse children are addressed by row position, so a short child would be read past its end.
        var ints = Assert.IsType<Int32Array>(result.Fields[0]);
        var strings = Assert.IsType<StringArray>(result.Fields[1]);
        Assert.Equal(3, ints.Length);
        Assert.Equal(3, strings.Length);

        for (int i = 0; i < result.Length; i++)
        {
            Assert.Equal(0, result.TypeIds[i]);
            Assert.True(ints.IsNull(i));
            Assert.True(strings.IsNull(i));
        }
    }

    [Fact]
    public void Union_TypeIdWrittenIsTheOneTheTypeMapAssigns_NotTheBranchIndex()
    {
        // A union numbers its branches however it likes, and the type_ids buffer holds those numbers, not
        // positions. Writing a bare 0 here would point every row at whichever branch happens to be numbered
        // 0 — a different branch than the one actually carrying the nulls.
        var type = MakeUnionType(UnionMode.Dense, 7, 9);

        var result = Assert.IsType<DenseUnionArray>(ArrowCompute.MakeNullArray(type, 2));

        for (int i = 0; i < result.Length; i++)
            Assert.Equal(7, result.TypeIds[i]);
    }

    [Fact]
    public void Union_ZeroLength_IsWellFormed()
    {
        // The case the ORC reader actually hits: a list<union> whose lists are all empty needs a
        // zero-length child of the declared union type.
        var type = MakeUnionType(UnionMode.Dense);

        var result = Assert.IsType<DenseUnionArray>(ArrowCompute.MakeNullArray(type, 0));

        Assert.Equal(0, result.Length);
        Assert.Same(type, result.Data.DataType);
        Assert.Equal(2, result.Fields.Count);
        Assert.Equal(0, result.Fields[0].Length);
        Assert.Equal(0, result.Fields[1].Length);
    }

    [Fact]
    public void Union_NestedInsideAList_Recurses()
    {
        // The path that made this arm necessary: MakeNullArray recurses into a list's element type, so a
        // union anywhere in a nested subtree is reached even when the caller never names one.
        var elementType = MakeUnionType(UnionMode.Dense);
        var listType = new ListType(new Field("item", elementType, nullable: true));

        var result = Assert.IsType<ListArray>(ArrowCompute.MakeNullArray(listType, 3));

        Assert.Equal(3, result.Length);
        Assert.Equal(3, result.Data.NullCount);
        Assert.IsType<DenseUnionArray>(result.Values);

        for (int i = 0; i < result.Length; i++)
            Assert.True(result.IsNull(i));
    }

    [Fact]
    public void ApacheArrow_RefusesABranchlessUnionType_WhichIsWhyMakeNullUnionNeedsNoGuard()
    {
        // A union with no branches would have nowhere to put a null — no child means no validity anywhere
        // in the array — so MakeNullUnion indexing branch 0 unconditionally rests on that type being
        // impossible to construct. It is: Apache.Arrow rejects an empty field list outright (reporting it,
        // oddly, as a null 'fields' argument). If a future Arrow version starts allowing it, this test
        // fails and MakeNullUnion needs the guard that is deliberately absent today.
        Assert.Throws<ArgumentNullException>(
            () => new UnionType(new Field[0], new int[0], UnionMode.Dense));
    }

    [Fact]
    public void UnsupportedType_ThrowsRatherThanSubstitutingAnotherType()
    {
        // Interval is deliberately absent from the width table, matching Take. The point of the throw is
        // that the alternative — falling back to some other type, as the code this replaced fell back to
        // String — produces a column contradicting the schema rather than an error.
        var ex = Assert.Throws<NotSupportedException>(
            () => ArrowCompute.MakeNullArray(IntervalType.MonthDayNanosecond, 3));

        Assert.Contains("ArrowCompute", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Interval", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

#if !NET6_0_OR_GREATER
    [Fact]
    public void HalfFloat_OnNetstandard_ThrowsWithAnExplanation()
    {
        // Apache.Arrow cannot construct a HalfFloatArray on this target at all. Since the type is reached
        // from a schema, a caller can genuinely ask for it, so the failure explains itself rather than
        // surfacing Arrow's message from inside a backfill.
        var ex = Assert.Throws<NotSupportedException>(
            () => ArrowCompute.MakeNullArray(HalfFloatType.Default, 3));

        Assert.Contains("HalfFloat", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("target framework", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
#endif
}
