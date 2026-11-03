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
/// unless the source has nulls in it: a file can predate the constraint, or come from a writer that did not enforce
/// it, and a required Parquet column cannot hold them. A field the table schema does not describe keeps its
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
            var data = Align(column.Data, target.DataType);
            bool nullable = Nullable(fields[i], target, data);
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

    // The data with every nested field's nullability taken from the target type, or the same instance when nothing
    // changes. Works on ArrayData throughout, whose children are unsliced: the parent keeps its offset, so the
    // relabelled array addresses exactly the rows it did.
    private static ArrayData Align(ArrayData data, IArrowType target)
    {
        switch (data.DataType)
        {
            // MapType derives from ListType, so it is matched first. Map keys are always required; only the value's
            // flag follows the table.
            case MapType am when target is MapType tm:
            {
                var entries = data.Children[0];
                var key = Align(entries.Children[0], tm.KeyField.DataType);
                var value = Align(entries.Children[1], tm.ValueField.DataType);
                bool valueNullable = Nullable(am.ValueField, tm.ValueField, value);
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
                var field = AlignElement(al.ValueField, te, data.Children[0], out var values);
                return field is null ? data : Rebuilt(data, new ListType(field), values);
            }
            case LargeListType al when Element(target) is { } te:
            {
                var field = AlignElement(al.ValueField, te, data.Children[0], out var values);
                return field is null ? data : Rebuilt(data, new LargeListType(field), values);
            }
            case FixedSizeListType af when Element(target) is { } te:
            {
                var field = AlignElement(af.ValueField, te, data.Children[0], out var values);
                return field is null ? data : Rebuilt(data, new FixedSizeListType(field, af.ListSize), values);
            }

            case StructType ast when target is StructType tst:
            {
                var byName = new Dictionary<string, Field>(tst.Fields.Count, StringComparer.Ordinal);
                foreach (var f in tst.Fields)
                    byName[f.Name] = f;

                Field[]? fields = null;
                ArrayData[]? children = null;
                for (int i = 0; i < ast.Fields.Count; i++)
                {
                    var actual = ast.Fields[i];
                    if (!byName.TryGetValue(actual.Name, out var t))
                        continue;
                    var child = Align(data.Children[i], t.DataType);
                    bool nullable = Nullable(actual, t, child);
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
    private static Field? AlignElement(Field actual, Field target, ArrayData child, out ArrayData values)
    {
        values = Align(child, target.DataType);
        bool nullable = Nullable(actual, target, values);
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

    private static bool Nullable(Field actual, Field target, ArrayData data) =>
        target.IsNullable || (actual.IsNullable && HasNulls(data));

    // Counted over the whole of an unsliced child, so a null the writers would never reach (under a null parent)
    // still keeps the field nullable. That only costs a definition level.
    private static bool HasNulls(ArrayData data) =>
        data.NullCount > 0 || (data.NullCount < 0 && ArrowArrayFactory.BuildArray(data).NullCount > 0);

    private static Field Relabel(Field field, IArrowType type, bool nullable) =>
        new(field.Name, type, nullable, field.Metadata);
}
