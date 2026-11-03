// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table.Compaction;

/// <summary>
/// Relabels a batch's field nullability, at every depth, to the table schema's. Only the labels change: the buffers,
/// offsets and children are the batch's own.
///
/// <para>Compaction joins rows from files of different vintages, and their Arrow nullability need not agree: an EW
/// append keeps the caller's flags (an <c>id</c> declared non-nullable), while Spark writes every column optional.
/// Values and types match, so the rows belong in one file and one row group, but
/// <see cref="RollingDataFileWriter"/> starts a new file at any layout difference, nullability included (#572).
/// Taking the flags from the table schema makes every source agree.</para>
///
/// <para>A column the table declares nullable is written nullable. One it declares NOT NULL is written required,
/// unless the source has nulls in it that the Parquet writer would reach: a file can predate the constraint, or come
/// from a writer that did not enforce it, and a required Parquet column cannot hold them. The incoming flag is not
/// trusted for this, since a nested schema reconcile labels a passed-through child with the table's flag whatever
/// it holds. A null the writer never reaches (a struct child's placeholder under a null parent, a value outside a
/// list's offsets) does not count, as in the writer's own check: counting it would label two sources of the same
/// data differently, and split them into separate files. A field the table schema does not describe keeps its
/// flags.</para>
/// </summary>
internal static class TableNullability
{
    /// <param name="tableFields">The table's columns, named as the batch is (physical under column mapping).</param>
    public static RecordBatch Apply(RecordBatch batch, IReadOnlyList<Field> tableFields)
    {
        var byName = new Dictionary<string, Field>(tableFields.Count, StringComparer.Ordinal);
        foreach (var f in tableFields)
            byName[f.Name] = f;

        var fields = batch.Schema.FieldsList;
        Field[]? newFields = null;
        IArrowArray[]? newColumns = null;
        for (int i = 0; i < fields.Count; i++)
        {
            if (!byName.TryGetValue(fields[i].Name, out var target))
                continue;
            var column = batch.Column(i);
            var data = Align(column.Data, target.DataType, AllSlots);
            bool nullable = Nullable(target, column, AllSlots);
            if (ReferenceEquals(data, column.Data) && nullable == fields[i].IsNullable)
                continue;

            newFields ??= [.. fields];
            newColumns ??= [.. Enumerable.Range(0, batch.ColumnCount).Select(batch.Column)];
            newFields[i] = Relabel(fields[i], data.DataType, nullable);
            if (!ReferenceEquals(data, column.Data))
                newColumns[i] = ArrowArrayFactory.BuildArray(data);
        }

        return newFields is null
            ? batch
            : new RecordBatch(new Apache.Arrow.Schema(newFields, batch.Schema.Metadata), newColumns!, batch.Length);
    }

    // The positions of an array the Parquet writer encodes, in the array's own logical index space (what IsNull
    // takes); null means every position. Computed only when a null check needs it. The child mappings follow
    // NestedLevelWriter, as RowGroupSchemaCheck's do.
    private delegate IReadOnlyList<int>? WrittenSlots();

    private static readonly WrittenSlots AllSlots = () => null;

    // The data with every nested field's nullability taken from the target type, or the same instance when nothing
    // changes. Works on ArrayData, whose children are unsliced: the parent keeps its offset, so the relabelled array
    // addresses exactly the rows it did. `slots` are the data's written positions.
    private static ArrayData Align(ArrayData data, IArrowType target, WrittenSlots slots)
    {
        switch (data.DataType)
        {
            // MapType derives from ListType, so it is matched first. Map keys are always required; only the value's
            // flag follows the table.
            case MapType am when target is MapType tm:
            {
                var map = (MapArray)ArrowArrayFactory.BuildArray(data);
                var entries = data.Children[0];
                var entrySlots = StructChildSlots(entries, ListChildSlots(map, slots));
                var key = Align(entries.Children[0], tm.KeyField.DataType, entrySlots);
                var value = Align(entries.Children[1], tm.ValueField.DataType, entrySlots);
                bool valueNullable = Nullable(
                    tm.ValueField, ArrowArrayFactory.BuildArray(entries.Children[1]), entrySlots);
                if (ReferenceEquals(key, entries.Children[0]) && ReferenceEquals(value, entries.Children[1])
                    && valueNullable == am.ValueField.IsNullable)
                {
                    return data;
                }
                var mapType = new MapType(
                    Relabel(am.KeyField, key.DataType, am.KeyField.IsNullable),
                    Relabel(am.ValueField, value.DataType, valueNullable),
                    am.KeySorted);
                var newEntries = new ArrayData(mapType.KeyValueType, entries.Length, entries.NullCount,
                    entries.Offset, entries.Buffers, [key, value]);
                return new ArrayData(mapType, data.Length, data.NullCount, data.Offset, data.Buffers, [newEntries]);
            }
            case MapType:
                return data;

            case ListType al when Element(target) is { } te:
            {
                var list = (ListArray)ArrowArrayFactory.BuildArray(data);
                var field = AlignElement(al.ValueField, te, data.Children[0], ListChildSlots(list, slots), out var values);
                return field is null ? data : Rebuilt(data, new ListType(field), values);
            }
            case LargeListType al when Element(target) is { } te:
            {
                var list = (LargeListArray)ArrowArrayFactory.BuildArray(data);
                var field = AlignElement(al.ValueField, te, data.Children[0], LargeListChildSlots(list, slots),
                    out var values);
                return field is null ? data : Rebuilt(data, new LargeListType(field), values);
            }
            case FixedSizeListType af when Element(target) is { } te:
            {
                var list = (FixedSizeListArray)ArrowArrayFactory.BuildArray(data);
                var field = AlignElement(af.ValueField, te, data.Children[0],
                    FixedSizeListChildSlots(list, af.ListSize, slots), out var values);
                return field is null ? data : Rebuilt(data, new FixedSizeListType(field, af.ListSize), values);
            }

            case StructType ast when target is StructType tst:
            {
                var byName = new Dictionary<string, Field>(tst.Fields.Count, StringComparer.Ordinal);
                foreach (var f in tst.Fields)
                    byName[f.Name] = f;

                var childSlots = StructChildSlots(data, slots);
                Field[]? fields = null;
                ArrayData[]? children = null;
                for (int i = 0; i < ast.Fields.Count; i++)
                {
                    var actual = ast.Fields[i];
                    if (!byName.TryGetValue(actual.Name, out var t))
                        continue;
                    var child = Align(data.Children[i], t.DataType, childSlots);
                    bool nullable = Nullable(t, ArrowArrayFactory.BuildArray(data.Children[i]), childSlots);
                    if (ReferenceEquals(child, data.Children[i]) && nullable == actual.IsNullable)
                        continue;
                    fields ??= [.. ast.Fields];
                    children ??= [.. data.Children];
                    fields[i] = Relabel(actual, child.DataType, nullable);
                    children[i] = child;
                }
                return fields is null
                    ? data
                    : new ArrayData(new StructType(fields), data.Length, data.NullCount, data.Offset, data.Buffers,
                        children!);
            }

            default:
                return data;
        }
    }

    // The element field relabelled, or null when neither it nor its values change.
    private static Field? AlignElement(
        Field actual, Field target, ArrayData child, WrittenSlots childSlots, out ArrayData values)
    {
        values = Align(child, target.DataType, childSlots);
        bool nullable = Nullable(target, ArrowArrayFactory.BuildArray(child), childSlots);
        return ReferenceEquals(values, child) && nullable == actual.IsNullable
            ? null
            : Relabel(actual, values.DataType, nullable);
    }

    private static Field? Element(IArrowType type) => type switch
    {
        MapType => null,
        ListType l => l.ValueField,
        LargeListType l => l.ValueField,
        FixedSizeListType f => f.ValueField,
        _ => null,
    };

    private static ArrayData Rebuilt(ArrayData data, IArrowType type, ArrayData child) =>
        new(type, data.Length, data.NullCount, data.Offset, data.Buffers, [child]);

    // Nullable when the table says so, or when the writer would reach a null in it. The incoming flag plays no
    // part: see the class remarks.
    private static bool Nullable(Field target, IArrowArray array, WrittenSlots slots) =>
        target.IsNullable || HasWrittenNull(array, slots);

    private static bool HasWrittenNull(IArrowArray array, WrittenSlots slots)
    {
        // A run-end encoded array has no validity of its own; its nulls are null runs in Values. Counted whole,
        // which can only keep a field nullable.
        if (array is RunEndEncodedArray ree)
            return ree.Values.NullCount > 0;
        if (array.NullCount == 0)
            return false;
        var written = slots();
        if (written is null)
            return true;
        foreach (int slot in written)
        {
            if (array.IsNull(slot))
                return true;
        }
        return false;
    }

    private static IEnumerable<int> Slots(int length, WrittenSlots slots) =>
        slots() ?? Enumerable.Range(0, length);

    // A struct's children are not sliced with it: written slot i of the struct is child slot offset + i, and a null
    // struct slot writes no child.
    private static WrittenSlots StructChildSlots(ArrayData parent, WrittenSlots slots) => () =>
    {
        var array = ArrowArrayFactory.BuildArray(parent);
        var result = new List<int>();
        foreach (int slot in Slots(array.Length, slots))
        {
            if (!array.IsNull(slot))
                result.Add(parent.Offset + slot);
        }
        return result;
    };

    // ValueOffsets already applies the list's own offset.
    private static WrittenSlots ListChildSlots(ListArray parent, WrittenSlots slots) => () =>
    {
        var result = new List<int>();
        var offsets = parent.ValueOffsets;
        foreach (int slot in Slots(parent.Length, slots))
        {
            if (parent.IsNull(slot))
                continue;
            for (int j = offsets[slot]; j < offsets[slot + 1]; j++)
                result.Add(j);
        }
        return result;
    };

    private static WrittenSlots LargeListChildSlots(LargeListArray parent, WrittenSlots slots) => () =>
    {
        var result = new List<int>();
        var offsets = parent.ValueOffsets;
        foreach (int slot in Slots(parent.Length, slots))
        {
            if (parent.IsNull(slot))
                continue;
            for (long j = offsets[slot]; j < offsets[slot + 1]; j++)
                result.Add(checked((int)j));
        }
        return result;
    };

    // A fixed-size list's child is not sliced with it either: slot i's elements start at (offset + i) * width.
    private static WrittenSlots FixedSizeListChildSlots(FixedSizeListArray parent, int width, WrittenSlots slots) =>
        () =>
        {
            var result = new List<int>();
            int offset = parent.Data.Offset;
            foreach (int slot in Slots(parent.Length, slots))
            {
                if (parent.IsNull(slot))
                    continue;
                int start = checked((offset + slot) * width);
                for (int j = 0; j < width; j++)
                    result.Add(start + j);
            }
            return result;
        };

    private static Field Relabel(Field field, IArrowType type, bool nullable) =>
        new(field.Name, type, nullable, field.Metadata);
}
