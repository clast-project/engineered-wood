// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Compaction;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="TableNullability"/> takes each field's flag from the table schema, keeps a NOT NULL field optional
/// only for a null the Parquet writer would reach, and does not trust the incoming flag for that.
/// </summary>
public class TableNullabilityTests
{
    private static readonly Field RequiredChild = new("x", Int64Type.Default, nullable: false);

    private static IReadOnlyList<Field> TableWithStruct() =>
        [new Field("s", new StructType([RequiredChild]), nullable: true)];

    // A struct of `valid` slots over a child of `childValues`, the child labelled `childNullable`.
    private static RecordBatch StructBatch(bool[] valid, long?[] childValues, bool childNullable)
    {
        var child = new Int64Array.Builder();
        foreach (var v in childValues)
        {
            if (v is null) child.AppendNull(); else child.Append(v.Value);
        }
        var bitmap = new ArrowBuffer.BitmapBuilder();
        foreach (bool b in valid)
            bitmap.Append(b);
        var type = new StructType([new Field("x", Int64Type.Default, childNullable)]);
        var s = new StructArray(type, valid.Length, [child.Build()], bitmap.Build(), valid.Count(b => !b));
        return new RecordBatch(new Apache.Arrow.Schema([new Field("s", type, true)], null), [s], valid.Length);
    }

    private static bool ChildNullable(RecordBatch batch) =>
        ((StructType)batch.Schema.FieldsList[0].DataType).Fields[0].IsNullable;

    /// <summary>
    /// A nested schema reconcile labels a passed-through child with the table's NOT NULL whatever it holds. A legacy
    /// null in it must still make the child optional, or the Parquet writer refuses the batch.
    /// </summary>
    [Fact]
    public void AReachableNull_UnderARequiredLabel_KeepsTheChildOptional()
    {
        var batch = StructBatch([true, true], [1, null], childNullable: false);
        Assert.True(ChildNullable(TableNullability.Apply(batch, TableWithStruct())));
    }

    /// <summary>
    /// A placeholder null under a null struct slot is never written, so it does not count: the source comes out
    /// labelled exactly as one without the placeholder, and the two can share a file.
    /// </summary>
    [Fact]
    public void ANullUnderANullParent_DoesNotCount()
    {
        var withPlaceholder = TableNullability.Apply(
            StructBatch([true, false], [1, null], childNullable: true), TableWithStruct());
        var without = TableNullability.Apply(
            StructBatch([true, true], [1, 2], childNullable: false), TableWithStruct());

        Assert.False(ChildNullable(withPlaceholder));
        Assert.False(ChildNullable(without));
        Assert.True(RollingDataFileWriter.SameLayout(withPlaceholder.Schema.FieldsList, without.Schema.FieldsList));
    }

    /// <summary>A sliced struct's written slots are found past its offset, in the unsliced child.</summary>
    [Fact]
    public void ASlicedStruct_ChecksTheChildAtItsOffset()
    {
        var batch = StructBatch([true, true, true], [null, 1, 2], childNullable: true);
        Assert.False(ChildNullable(TableNullability.Apply(batch.Slice(1, 2), TableWithStruct())));
        Assert.True(ChildNullable(TableNullability.Apply(batch.Slice(0, 2), TableWithStruct())));
    }

    /// <summary>A list's values outside its offsets are never written either.</summary>
    [Fact]
    public void AListValueOutsideTheOffsets_DoesNotCount()
    {
        var element = new Field("element", Int64Type.Default, nullable: false);
        IReadOnlyList<Field> table = [new Field("l", new ListType(element), nullable: true)];

        // Values [null, 1, 2]; the one list slot covers values 1..2 only.
        var values = new Int64Array.Builder().AppendNull().Append(1).Append(2).Build();
        var offsets = new ArrowBuffer.Builder<int>().Append(1).Append(3).Build();
        var type = new ListType(new Field("element", Int64Type.Default, true));
        var list = new ListArray(type, 1, offsets, values, ArrowBuffer.Empty, nullCount: 0);
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("l", type, true)], null), [list], 1);

        var result = TableNullability.Apply(batch, table);
        Assert.False(((ListType)result.Schema.FieldsList[0].DataType).ValueField.IsNullable);
    }

    // A one-slot list over a run-end encoded element whose rows are [null, 1, 2] (runs ending at 1, 2, 3), the slot
    // covering rows from..3.
    private static RecordBatch RunEndListBatch(int from)
    {
        var runEnds = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new Int64Array.Builder().AppendNull().Append(1).Append(2).Build();
        var ree = new RunEndEncodedArray(runEnds, values);
        var offsets = new ArrowBuffer.Builder<int>().Append(from).Append(3).Build();
        var type = new ListType(new Field("element", ree.Data.DataType, true));
        var list = new ListArray(type, 1, offsets, ree, ArrowBuffer.Empty, nullCount: 0);
        return new RecordBatch(new Apache.Arrow.Schema([new Field("l", type, true)], null), [list], 1);
    }

    /// <summary>
    /// A run-end encoded column's nulls are its null runs, read per row: one outside the written slots does not
    /// count, one inside does.
    /// </summary>
    [Theory]
    [InlineData(1, false)]
    [InlineData(0, true)]
    public void ARunEndEncodedNull_CountsOnlyInsideTheWrittenSlots(int from, bool expectNullable)
    {
        var batch = RunEndListBatch(from);
        var elementType = ((ListType)batch.Schema.FieldsList[0].DataType).ValueField.DataType;
        IReadOnlyList<Field> table =
            [new Field("l", new ListType(new Field("element", elementType, nullable: false)), nullable: true)];

        var result = TableNullability.Apply(batch, table);
        Assert.Equal(expectNullable, ((ListType)result.Schema.FieldsList[0].DataType).ValueField.IsNullable);
    }

    /// <summary>
    /// A map key takes the table's (required) flag even when a source labelled it optional, so two sources that
    /// differ only in the key's label share a file.
    /// </summary>
    [Fact]
    public void AMapKeyLabelledOptional_TakesTheTablesRequiredFlag()
    {
        static RecordBatch MapBatch(bool keyNullable)
        {
            var type = new MapType(
                new Field("key", StringType.Default, keyNullable), new Field("value", Int64Type.Default, true));
            var keys = new StringArray.Builder().Append("a").Build();
            var values = new Int64Array.Builder().Append(1).Build();
            var entries = new StructArray(type.KeyValueType, 1, [keys, values], ArrowBuffer.Empty, nullCount: 0);
            var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();
            var map = new MapArray(type, 1, offsets, entries, ArrowBuffer.Empty, nullCount: 0);
            return new RecordBatch(new Apache.Arrow.Schema([new Field("m", type, true)], null), [map], 1);
        }

        IReadOnlyList<Field> table = [MapBatch(keyNullable: false).Schema.FieldsList[0]];
        var optional = TableNullability.Apply(MapBatch(keyNullable: true), table);
        var required = TableNullability.Apply(MapBatch(keyNullable: false), table);

        Assert.False(((MapType)optional.Schema.FieldsList[0].DataType).KeyField.IsNullable);
        Assert.True(RollingDataFileWriter.SameLayout(optional.Schema.FieldsList, required.Schema.FieldsList));
    }
}
