// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Relabels a batch's field nullability, at every depth, to the table schema's. Only the labels change: the buffers,
/// offsets and children are the batch's own.
///
/// <para>Rows that belong in one file can arrive with Arrow nullability that disagrees. Compaction joins files of
/// different vintages: an EW append keeps the caller's flags (an <c>id</c> declared non-nullable), while Spark writes
/// every column optional. A write can join batches from different producers. Values and types match, so the rows
/// belong in one file and one row group, but <see cref="RollingDataFileWriter"/> starts a new file at any layout
/// difference, nullability included (#572, #651). Taking the flags from the table schema makes every source
/// agree.</para>
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
            // The relabel builds the field from the array's type, so the array first takes the nested names the
            // schema declares, where the two disagree: an array built with ListArray.Builder names its element
            // "item" under a schema field that says "element", and the writer, which names columns from the schema,
            // would have switched to "item" (and lost a bloom filter configured on the declared path).
            var declared = Declared(column.Data, fields[i].DataType);
            var data = Align(declared, target.DataType, AllSlots);
            bool nullable = Nullable(target, column, AllSlots);
            if (ReferenceEquals(data, declared) && nullable == fields[i].IsNullable)
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

    /// <summary>
    /// <see cref="Apply"/> for batches written as the row groups of ONE file, whose schema the first row group fixes.
    /// A NOT NULL column is written required only when no batch reaches a null in it. Batch by batch, an updater's
    /// rows (no nulls) came out required and the copied rows after them (an old null) optional, and the file refused
    /// the second row group.
    /// </summary>
    public static IReadOnlyList<RecordBatch> ApplyToOneFile(
        IReadOnlyList<RecordBatch> batches, IReadOnlyList<Field> tableFields)
    {
        var relabeled = new RecordBatch[batches.Count];
        for (int i = 0; i < relabeled.Length; i++)
            relabeled[i] = Apply(batches[i], tableFields);
        if (relabeled.Length < 2)
            return relabeled;

        // Every batch's flags, merged: optional wherever any batch is. Applying the merged fields then gives each batch
        // the same flags, since a field merged as required has no written null in any batch.
        var merged = new Dictionary<string, Field>(StringComparer.Ordinal);
        foreach (var batch in relabeled)
        {
            foreach (var field in batch.Schema.FieldsList)
                merged[field.Name] = merged.TryGetValue(field.Name, out var seen) ? Union(seen, field) : field;
        }
        var mergedFields = merged.Values.ToList();
        for (int i = 0; i < relabeled.Length; i++)
            relabeled[i] = Apply(relabeled[i], mergedFields);
        return relabeled;
    }

    // One field optional where either is, at every depth the two have the same shape; `a`'s otherwise.
    private static Field Union(Field a, Field b)
    {
        var type = UnionType(a.DataType, b.DataType);
        bool nullable = a.IsNullable || b.IsNullable;
        return ReferenceEquals(type, a.DataType) && nullable == a.IsNullable
            ? a
            : new Field(a.Name, type, nullable, a.Metadata);
    }

    private static IArrowType UnionType(IArrowType a, IArrowType b)
    {
        switch (a, b)
        {
            // MapType derives from ListType, so it is matched first.
            case (MapType am, MapType bm):
            {
                var key = Union(am.KeyField, bm.KeyField);
                var value = Union(am.ValueField, bm.ValueField);
                return ReferenceEquals(key, am.KeyField) && ReferenceEquals(value, am.ValueField)
                    ? a
                    : new MapType(key, value, am.KeySorted);
            }
            case (MapType, _):
            case (_, MapType):
                return a;
            case (ListType al, ListType bl):
            {
                var element = Union(al.ValueField, bl.ValueField);
                return ReferenceEquals(element, al.ValueField) ? a : new ListType(element);
            }
            case (LargeListType al, LargeListType bl):
            {
                var element = Union(al.ValueField, bl.ValueField);
                return ReferenceEquals(element, al.ValueField) ? a : new LargeListType(element);
            }
            case (FixedSizeListType al, FixedSizeListType bl) when al.ListSize == bl.ListSize:
            {
                var element = Union(al.ValueField, bl.ValueField);
                return ReferenceEquals(element, al.ValueField) ? a : new FixedSizeListType(element, al.ListSize);
            }
            case (StructType ast, StructType bst) when ast.Fields.Count == bst.Fields.Count:
            {
                Field[]? fields = null;
                for (int i = 0; i < ast.Fields.Count; i++)
                {
                    if (ast.Fields[i].Name != bst.Fields[i].Name)
                        return a;
                    var child = Union(ast.Fields[i], bst.Fields[i]);
                    if (ReferenceEquals(child, ast.Fields[i]))
                        continue;
                    fields ??= [.. ast.Fields];
                    fields[i] = child;
                }
                return fields is null ? a : new StructType(fields);
            }
            default:
                return a;
        }
    }

    // The data with each nested field's name, nullability and metadata taken from `declared`, where the two are the
    // same kind of container with the same number of children; leaf types are always the array's own, so the result
    // describes its buffers. The same instance when nothing differs.
    private static ArrayData Declared(ArrayData data, IArrowType declared)
    {
        if (ReferenceEquals(data.DataType, declared))
            return data;

        switch (declared, data.DataType)
        {
            // MapType derives from ListType, so it is matched first.
            case (MapType dm, MapType am):
            {
                var entries = data.Children[0];
                var key = Declared(entries.Children[0], dm.KeyField.DataType);
                var value = Declared(entries.Children[1], dm.ValueField.DataType);
                var keyField = Retyped(dm.KeyField, key.DataType);
                var valueField = Retyped(dm.ValueField, value.DataType);
                if (SameField(keyField, am.KeyField) && SameField(valueField, am.ValueField))
                    return data;
                var mapType = new MapType(keyField, valueField, am.KeySorted);
                var newEntries = new ArrayData(mapType.KeyValueType, entries.Length, entries.NullCount,
                    entries.Offset, entries.Buffers, [key, value]);
                return new ArrayData(mapType, data.Length, data.NullCount, data.Offset, data.Buffers, [newEntries]);
            }
            case (MapType, _):
            case (_, MapType):
                return data;
            case (ListType dl, ListType al):
            {
                var child = Declared(data.Children[0], dl.ValueDataType);
                var field = Retyped(dl.ValueField, child.DataType);
                return SameField(field, al.ValueField) ? data : Rebuilt(data, new ListType(field), child);
            }
            case (LargeListType dl, LargeListType al):
            {
                var child = Declared(data.Children[0], dl.ValueDataType);
                var field = Retyped(dl.ValueField, child.DataType);
                return SameField(field, al.ValueField) ? data : Rebuilt(data, new LargeListType(field), child);
            }
            case (FixedSizeListType dl, FixedSizeListType al) when dl.ListSize == al.ListSize:
            {
                var child = Declared(data.Children[0], dl.ValueDataType);
                var field = Retyped(dl.ValueField, child.DataType);
                return SameField(field, al.ValueField)
                    ? data
                    : Rebuilt(data, new FixedSizeListType(field, al.ListSize), child);
            }
            case (StructType ds, StructType ast) when ds.Fields.Count == ast.Fields.Count:
            {
                var children = new ArrayData[ast.Fields.Count];
                var fields = new Field[ast.Fields.Count];
                bool changed = false;
                for (int i = 0; i < children.Length; i++)
                {
                    children[i] = Declared(data.Children[i], ds.Fields[i].DataType);
                    fields[i] = Retyped(ds.Fields[i], children[i].DataType);
                    changed |= !SameField(fields[i], ast.Fields[i]);
                }
                return changed
                    ? new ArrayData(new StructType(fields), data.Length, data.NullCount, data.Offset, data.Buffers,
                        children)
                    : data;
            }
            default:
                return data;
        }
    }

    // `declared`'s name, nullability and metadata over `type`.
    private static Field Retyped(Field declared, IArrowType type) =>
        ReferenceEquals(declared.DataType, type) ? declared : new Field(declared.Name, type, declared.IsNullable, declared.Metadata);

    // Whether two fields have one name, nullability, metadata and type instance: enough to keep the array's own type.
    private static bool SameField(Field a, Field b) =>
        ReferenceEquals(a, b)
        || (a.Name == b.Name && a.IsNullable == b.IsNullable && ReferenceEquals(a.DataType, b.DataType)
            && ReferenceEquals(a.Metadata, b.Metadata));

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
            // MapType derives from ListType, so it is matched first. The key takes the table's flag as given (keys
            // are required, and a key null is refused by the writer whatever the label says); the value follows the
            // usual rule.
            case MapType am when target is MapType tm:
            {
                var map = (MapArray)ArrowArrayFactory.BuildArray(data);
                var entries = data.Children[0];
                var entrySlots = StructChildSlots(entries, ListChildSlots(map, slots));
                var key = Align(entries.Children[0], tm.KeyField.DataType, entrySlots);
                var value = Align(entries.Children[1], tm.ValueField.DataType, entrySlots);
                bool valueNullable = Nullable(
                    tm.ValueField, ArrowArrayFactory.BuildArray(entries.Children[1]), entrySlots);
                bool keyNullable = tm.KeyField.IsNullable;
                if (ReferenceEquals(key, entries.Children[0]) && ReferenceEquals(value, entries.Children[1])
                    && valueNullable == am.ValueField.IsNullable && keyNullable == am.KeyField.IsNullable)
                {
                    return data;
                }
                var mapType = new MapType(
                    Relabel(am.KeyField, key.DataType, keyNullable),
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
        // A run-end encoded array has no validity of its own: its nulls are null runs in Values, expanded per row
        // exactly as the writers and RowGroupSchemaCheck read them, so a null run outside the written slots does
        // not count.
        if (array is RunEndEncodedArray ree)
        {
            if (EngineeredWood.Parquet.Data.NestedLevelWriter.RunNulls(ree) is not { } runNulls)
                return false;
            var writtenRuns = slots();
            if (writtenRuns is null)
                return System.Array.IndexOf(runNulls, true) >= 0;
            foreach (int slot in writtenRuns)
            {
                if (runNulls[slot])
                    return true;
            }
            return false;
        }
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
