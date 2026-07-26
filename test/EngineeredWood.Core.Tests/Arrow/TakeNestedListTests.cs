// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// The list-shaped gathers — LIST, LARGE LIST, MAP and FIXED SIZE LIST.
///
/// <para>These differ from every other arm in where the child rows live: a struct's children are parallel to
/// its parent's rows, but a list's are reached through its offsets buffer, and a fixed-size list's are
/// derived arithmetically. That is the whole hazard surface here, so the cases below are built around it —
/// a sliced parent, a child carrying its own offset, duplicated rows, and nulls that must stay distinct
/// from empty lists.</para>
/// </summary>
public class TakeNestedListTests
{
    private static ListType Int32List => new(new Field("item", Int32Type.Default, true));

    private static LargeListType Int32LargeList => new(new Field("item", Int32Type.Default, true));

    /// <summary>Reads one logical row of a list-shaped result back as plain ints, or null for a null row.</summary>
    private static int?[]? Row(IArrowArray array, int index)
    {
        var list = (Apache.Arrow.Array)array;
        if (list.IsNull(index))
            return null;

        var values = (Int32Array)(list switch
        {
            ListArray l => l.GetSlicedValues(index)!,
            LargeListArray l => l.GetSlicedValues(index)!,
            FixedSizeListArray l => l.GetSlicedValues(index)!,
            _ => throw new ArgumentOutOfRangeException(nameof(array)),
        });

        var row = new int?[values.Length];
        for (int i = 0; i < values.Length; i++)
            row[i] = values.GetValue(i);
        return row;
    }

    private static void AssertRow(int?[]? expected, IArrowArray array, int index)
    {
        int?[]? actual = Row(array, index);
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);
        Assert.Equal(expected, actual);
    }

    // ---------------------------------------------------------------- LIST

    [Fact]
    public void List_GathersRowsAndTheirChildValues()
    {
        // Rows: [1,2] [] [3,4,5] [6]
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4, 5, 6 });
        var array = RawArrays.List(Int32List, [0, 2, 2, 5, 6], child);
        Assert.Equal(4, array.Length);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[2, 0, 1, 3]));

        Assert.Equal(4, result.Length);
        AssertRow([3, 4, 5], result, 0);
        AssertRow([1, 2], result, 1);
        AssertRow([], result, 2);
        AssertRow([6], result, 3);

        // The gathered child holds exactly the selected rows' values, in order — not the source child.
        var gatheredChild = Assert.IsType<Int32Array>(result.Values);
        Assert.Equal(6, gatheredChild.Length);
        Assert.Equal(new int?[] { 3, 4, 5, 1, 2, 6 }, Enumerable.Range(0, 6).Select(gatheredChild.GetValue));
    }

    [Fact]
    public void List_NullRowStaysDistinctFromEmptyRowAndOffsetsStayMonotonic()
    {
        // Row 1 is NULL, row 3 is an EMPTY list. Collapsing one into the other is the classic list bug.
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 });
        var array = RawArrays.List(
            Int32List, [0, 2, 2, 3, 3], child, physicalValid: [true, false, true, true]);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[3, 1, 0, 2]));

        AssertRow([], result, 0);
        Assert.Null(Row(result, 1));
        AssertRow([1, 2], result, 2);
        AssertRow([3], result, 3);
        Assert.Equal(1, result.NullCount);

        var offsets = MemoryMarshal.Cast<byte, int>(result.Data.Buffers[1].Span);
        for (int i = 0; i < result.Length; i++)
            Assert.True(offsets[i + 1] >= offsets[i], $"offset {i + 1} went backwards");
    }

    [Fact]
    public void List_Offset_ReadsFromLogicalRowNotPhysicalSlot()
    {
        // This is the shape ParquetFileWriter.CompactSlicedColumns hands over: a column that is a VIEW onto a
        // larger array. An offset-blind gather returns physical slots 0 and 1 — [10,11] and [12] — instead.
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 10, 11, 12, 13, 14, 15 });
        var array = RawArrays.List(Int32List, [0, 2, 3, 5, 6], child, offset: 2);
        Assert.Equal(2, array.Length);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[0, 1]));

        AssertRow([13, 14], result, 0);   // logical 0 → physical slot 2
        AssertRow([15], result, 1);       // logical 1 → physical slot 3
    }

    [Fact]
    public void List_Offset_ReadsCorrectValidityBits()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 10, 11, 12, 13 });
        // Physical slot 1 and 3 are null; with offset 1 those are logical rows 0 and 2.
        var array = RawArrays.List(
            Int32List, [0, 1, 2, 3, 4], child,
            physicalValid: [true, false, true, false], offset: 1);
        Assert.Equal(3, array.Length);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[0, 1, 2]));

        Assert.Null(Row(result, 0));
        AssertRow([12], result, 1);
        Assert.Null(Row(result, 2));
        Assert.Equal(2, result.NullCount);
    }

    [Fact]
    public void List_ChildOffsetIsAppliedOnTopOfTheListOffsets()
    {
        // MEASURED against Apache.Arrow's own GetSlicedValues: a list's offsets hold LOGICAL positions in the
        // child, so the child's own offset applies on top. A gather that treated them as physical child slots
        // would read 99 and 10 here instead of 10 and 20 — silently, and only when the child is itself a view.
        var childValues = RawArrays.Fixed(Int32Type.Default, new[] { 99, 10, 20, 30 }, offset: 1);
        var array = RawArrays.List(Int32List, [0, 2, 3], childValues);

        // Arrow's own reader agrees with the expectation, so this pins a convention rather than a preference.
        AssertRow([10, 20], array, 0);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[1, 0]));

        AssertRow([30], result, 0);
        AssertRow([10, 20], result, 1);
    }

    [Fact]
    public void List_DuplicateIndices_CopyChildValuesEachTime()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 });
        var array = RawArrays.List(Int32List, [0, 2, 3], child);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[0, 0, 1]));

        AssertRow([1, 2], result, 0);
        AssertRow([1, 2], result, 1);
        AssertRow([3], result, 2);

        // The duplicate must be materialised, not aliased back onto one child slice.
        Assert.Equal(5, ((Int32Array)result.Values).Length);
    }

    [Fact]
    public void List_MatchesArrowsOwnBuilderGather()
    {
        // Cross-check against Apache.Arrow's ListArray.Builder, which is lossless for Int32 children.
        var lb = new ListArray.Builder(Int32Type.Default);
        var vb = (Int32Array.Builder)lb.ValueBuilder;
        lb.Append(); vb.Append(1).Append(2);
        lb.AppendNull();
        lb.Append(); vb.Append(3);
        lb.Append();
        lb.Append(); vb.Append(4).Append(5).Append(6);
        var source = lb.Build();

        int[] indices = [4, 1, 3, 0, 2, 4];

        var expected = new ListArray.Builder(Int32Type.Default);
        var evb = (Int32Array.Builder)expected.ValueBuilder;
        foreach (int r in indices)
        {
            if (source.IsNull(r)) { expected.AppendNull(); continue; }
            expected.Append();
            var slice = (Int32Array)source.GetSlicedValues(r)!;
            for (int i = 0; i < slice.Length; i++) evb.Append(slice.GetValue(i)!.Value);
        }

        var viaBuilder = expected.Build();
        var viaTake = Assert.IsType<ListArray>(ArrowCompute.Take(source, indices));

        Assert.Equal(viaBuilder.Length, viaTake.Length);
        for (int i = 0; i < viaBuilder.Length; i++)
            AssertRow(Row(viaBuilder, i), viaTake, i);
    }

    [Fact]
    public void ListOfStruct_RecursesIntoTheChildsFields()
    {
        var structType = new StructType(
        [
            new Field("n", Int32Type.Default, true),
            new Field("s", StringType.Default, true),
        ]);

        var n = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4 });
        var s = RawArrays.VarBinary(StringType.Default, ["a", "b", "c", "d"], large: false);
        var entries = RawArrays.Struct(structType, 4, [n, s]);
        var array = RawArrays.List(
            new ListType(new Field("item", structType, true)), [0, 2, 4], entries);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[1, 0]));

        var row0 = Assert.IsType<StructArray>(result.GetSlicedValues(0));
        Assert.Equal(2, row0.Length);
        Assert.Equal(3, ((Int32Array)row0.Fields[0]).GetValue(0));
        Assert.Equal("c", ((StringArray)row0.Fields[1]).GetString(0));

        var row1 = Assert.IsType<StructArray>(result.GetSlicedValues(1));
        Assert.Equal("a", ((StringArray)row1.Fields[1]).GetString(0));
    }

    [Fact]
    public void ListOfList_RecursesThroughBothLevels()
    {
        // Rows: [[1,2],[3]] [[4]]  — the inner list's offsets must be re-derived, not carried over.
        var leaf = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4 });
        var inner = RawArrays.List(Int32List, [0, 2, 3, 4], leaf);
        var outer = RawArrays.List(
            new ListType(new Field("item", Int32List, true)), [0, 2, 3], inner);

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(outer, (int[])[1, 0]));

        var row0 = Assert.IsType<ListArray>(result.GetSlicedValues(0));
        Assert.Equal(1, row0.Length);
        AssertRow([4], row0, 0);

        var row1 = Assert.IsType<ListArray>(result.GetSlicedValues(1));
        Assert.Equal(2, row1.Length);
        AssertRow([1, 2], row1, 0);
        AssertRow([3], row1, 1);
    }

    // ---------------------------------------------------------------- MAP

    private static MapArray BuildMap()
    {
        // Rows: {a:1, b:2} NULL {c:3} {}
        var mb = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default, keySorted: false));
        var kb = (StringArray.Builder)mb.KeyBuilder;
        var vb = (Int32Array.Builder)mb.ValueBuilder;
        mb.Append(); kb.Append("a"); vb.Append(1); kb.Append("b"); vb.Append(2);
        mb.AppendNull();
        mb.Append(); kb.Append("c"); vb.Append(3);
        mb.Append();
        return mb.Build();
    }

    private static (string Key, int? Value)[] MapRow(MapArray map, int index)
    {
        var entries = (StructArray)map.GetSlicedValues(index)!;
        var keys = (StringArray)entries.Fields[0];
        var values = (Int32Array)entries.Fields[1];
        var row = new (string, int?)[entries.Length];
        for (int i = 0; i < entries.Length; i++)
            row[i] = (keys.GetString(i), values.GetValue(i));
        return row;
    }

    [Fact]
    public void Map_IsGatheredAsAMapNotAsAPlainList()
    {
        // MapArray DERIVES FROM ListArray, so a gather whose list arm precedes its map arm swallows every map
        // and hands back something shaped as a list — key/value structure gone, and the batch's schema still
        // claiming a map. This is the regression test for that arm ordering.
        var map = BuildMap();

        var result = ArrowCompute.Take(map, (int[])[2, 0, 3, 1]);

        var gathered = Assert.IsType<MapArray>(result);
        Assert.Equal(ArrowTypeId.Map, gathered.Data.DataType.TypeId);
        Assert.Same(map.Data.DataType, gathered.Data.DataType);

        Assert.Equal([("c", 3)], MapRow(gathered, 0));
        Assert.Equal([("a", 1), ("b", 2)], MapRow(gathered, 1));
        Assert.Empty(MapRow(gathered, 2));
        Assert.True(gathered.IsNull(3));
        Assert.Equal(1, gathered.NullCount);

        // The entries child must stay a two-field key/value struct rather than being flattened.
        var entries = Assert.IsType<StructArray>(gathered.KeyValues);
        Assert.Equal(2, entries.Fields.Count);
        Assert.Equal(3, entries.Length);
    }

    [Fact]
    public void Map_KeySortedFlagAndFieldNamesSurviveTheGather()
    {
        // The type is reused verbatim rather than rebuilt, so a keysSorted map stays keysSorted — a rebuilt
        // default would quietly drop the flag and let a reader skip a sort it still needs.
        var type = new MapType(StringType.Default, Int32Type.Default, keySorted: true);
        var mb = new MapArray.Builder(type);
        var kb = (StringArray.Builder)mb.KeyBuilder;
        var vb = (Int32Array.Builder)mb.ValueBuilder;
        mb.Append(); kb.Append("a"); vb.Append(1);
        mb.Append(); kb.Append("b"); vb.Append(2);
        var map = mb.Build();

        var result = Assert.IsType<MapArray>(ArrowCompute.Take(map, (int[])[1, 0]));

        var gatheredType = Assert.IsType<MapType>(result.Data.DataType);
        Assert.True(gatheredType.KeySorted);
        Assert.Equal(type.KeyField.Name, gatheredType.KeyField.Name);
        Assert.Equal(type.ValueField.Name, gatheredType.ValueField.Name);
    }

    [Fact]
    public void Map_Offset_ReadsFromLogicalRow()
    {
        var map = BuildMap();
        var sliced = Assert.IsType<MapArray>(map.Slice(2, 2));
        Assert.Equal(2, sliced.Data.Offset);

        var result = Assert.IsType<MapArray>(ArrowCompute.Take(sliced, (int[])[0, 1]));

        Assert.Equal([("c", 3)], MapRow(result, 0));
        Assert.Empty(MapRow(result, 1));
    }

    // ---------------------------------------------------------------- LARGE LIST

    [Fact]
    public void LargeList_GathersAndIsNotNarrowedToList()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4, 5 });
        var type = Int32LargeList;
        var array = RawArrays.LargeList(type, [0L, 2L, 2L, 5L], child);

        var result = ArrowCompute.Take(array, (int[])[2, 1, 0]);

        // Narrowing to ListType would contradict the schema the column lands in and overflow past 2^31 children.
        var large = Assert.IsType<LargeListArray>(result);
        Assert.Equal(ArrowTypeId.LargeList, large.Data.DataType.TypeId);
        Assert.Same(type, large.Data.DataType);

        AssertRow([3, 4, 5], large, 0);
        AssertRow([], large, 1);
        AssertRow([1, 2], large, 2);

        // The offsets must be rebuilt as 64-bit, which is 8 bytes per row plus the terminator.
        Assert.Equal((3 + 1) * sizeof(long), large.Data.Buffers[1].Length);
    }

    [Fact]
    public void LargeList_Offset_ReadsFromLogicalRowAndGathersNulls()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 7, 8, 9, 10 });
        var array = RawArrays.LargeList(
            Int32LargeList, [0L, 1L, 2L, 3L, 4L], child,
            physicalValid: [true, true, false, true], offset: 1);
        Assert.Equal(3, array.Length);

        var result = Assert.IsType<LargeListArray>(ArrowCompute.Take(array, (int[])[2, 1, 0]));

        AssertRow([10], result, 0);
        Assert.Null(Row(result, 1));
        AssertRow([8], result, 2);
        Assert.Equal(1, result.NullCount);
    }

    // ---------------------------------------------------------------- FIXED SIZE LIST

    private static FixedSizeListType Pairs => new(new Field("item", Int32Type.Default, true), 2);

    [Fact]
    public void FixedSizeList_GathersWholeWindows()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4, 5, 6 });
        var array = RawArrays.FixedSizeList(Pairs, physicalCount: 3, child);

        var result = Assert.IsType<FixedSizeListArray>(ArrowCompute.Take(array, (int[])[2, 0]));

        Assert.Equal(2, result.Length);
        AssertRow([5, 6], result, 0);
        AssertRow([1, 2], result, 1);

        // Arrow requires the child to be exactly length * ListSize; a gather that dropped or duplicated a
        // slot would leave a well-formed-looking array whose rows are shifted.
        Assert.Equal(4, ((Apache.Arrow.Array)result.Values).Length);
    }

    [Fact]
    public void FixedSizeList_NullRowStillOwnsItsChildSlots()
    {
        // A null row's values are undefined, but Arrow still requires the child to be length * ListSize —
        // skipping a null row's slots would shorten the child and shift every later row's window.
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4, 5, 6 });
        var array = RawArrays.FixedSizeList(
            Pairs, physicalCount: 3, child, physicalValid: [true, false, true]);

        var result = Assert.IsType<FixedSizeListArray>(ArrowCompute.Take(array, (int[])[1, 2, 0]));

        Assert.Equal(3, result.Length);
        Assert.Equal(1, result.NullCount);
        Assert.True(result.IsNull(0));
        AssertRow([5, 6], result, 1);
        AssertRow([1, 2], result, 2);
        Assert.Equal(6, ((Apache.Arrow.Array)result.Values).Length);
    }

    [Fact]
    public void FixedSizeList_Offset_ReadsFromLogicalRow()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4, 5, 6 });
        var array = RawArrays.FixedSizeList(Pairs, physicalCount: 3, child, offset: 1);
        Assert.Equal(2, array.Length);

        var result = Assert.IsType<FixedSizeListArray>(ArrowCompute.Take(array, (int[])[1, 0]));

        // Logical rows 1 and 0 are physical slots 2 and 1 — child windows [5,6] and [3,4].
        AssertRow([5, 6], result, 0);
        AssertRow([3, 4], result, 1);
    }

    // ---------------------------------------------------------------- SHAPE

    [Theory]
    [InlineData("list")]
    [InlineData("largelist")]
    [InlineData("map")]
    [InlineData("fixedsizelist")]
    public void EmptySelection_YieldsTypedZeroLengthColumn(string kind)
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4 });
        IArrowArray array = kind switch
        {
            "list" => RawArrays.List(Int32List, [0, 2, 4], child),
            "largelist" => RawArrays.LargeList(Int32LargeList, [0L, 2L, 4L], child),
            "map" => BuildMap(),
            "fixedsizelist" => RawArrays.FixedSizeList(Pairs, physicalCount: 2, child),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        var result = ArrowCompute.Take(array, System.Array.Empty<int>());

        Assert.Equal(0, result.Length);
        Assert.Equal(array.Data.DataType.TypeId, result.Data.DataType.TypeId);

        // The child must be emptied too — carrying the source's over would leave offsets that no longer
        // describe it, and a fixed-size list would break its length * ListSize invariant outright.
        Assert.Equal(0, result.Data.Children[0].Length);
    }

    [Fact]
    public void NoNullSource_ProducesNoValidityBuffer()
    {
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3, 4 });
        var array = RawArrays.List(Int32List, [0, 2, 4], child);

        var result = ArrowCompute.Take(array, (int[])[1, 1, 0]);

        Assert.Equal(0, result.Data.NullCount);
        Assert.Equal(0, result.Data.Buffers[0].Length);
    }

    [Fact]
    public void UnknownNullCount_IsTreatedAsMayContainNulls()
    {
        // Arrow uses -1 for "not computed"; the gather must probe each row rather than skip validity.
        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 });
        var offsets = new ArrowBuffer.Builder<int>(4);
        offsets.Append(0).Append(2).Append(2).Append(3);
        var bitmap = new ArrowBuffer.BitmapBuilder(3);
        bitmap.Append(true).Append(false).Append(true);

        var array = ArrowArrayFactory.BuildArray(new ArrayData(
            Int32List, 3, nullCount: -1, offset: 0,
            new[] { bitmap.Build(), offsets.Build() },
            new[] { child.Data }));

        var result = Assert.IsType<ListArray>(ArrowCompute.Take(array, (int[])[2, 1, 0]));

        AssertRow([3], result, 0);
        Assert.Null(Row(result, 1));
        AssertRow([1, 2], result, 2);
        Assert.Equal(1, result.NullCount);
    }

    [Fact]
    public void RecordBatchOverload_GathersAListColumnAlongsideAFlatOne()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("l", Int32List, true))
            .Build();

        var child = RawArrays.Fixed(Int32Type.Default, new[] { 1, 2, 3 });
        var batch = new RecordBatch(
            schema,
            [
                RawArrays.Fixed(Int32Type.Default, new[] { 10, 20, 30 }),
                RawArrays.List(Int32List, [0, 2, 3, 3], child),
            ],
            3);

        var result = ArrowCompute.Take(batch, schema, (int[])[2, 0]);

        Assert.Equal(2, result.Length);
        Assert.Equal(30, ((Int32Array)result.Column(0)).GetValue(0));
        AssertRow([], result.Column(1), 0);
        AssertRow([1, 2], result.Column(1), 1);
    }
}
