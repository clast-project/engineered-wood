// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Read-path reconcile for metadata-only schema changes. ADD/DROP COLUMN on a column-mapping table commit a
/// new <c>metaData</c> action without rewriting any data file, so files of different vintages disagree with
/// the current schema: a file written before an ADD lacks the column, and one written before a DROP still
/// carries it. These helpers reconcile a batch to the current schema — backfilling absent columns as typed
/// all-NULL arrays and dropping removed ones — at every nesting depth.
/// </summary>
internal static class SchemaEvolution
{
    /// <summary>
    /// Schema evolution reconcile: a column ADDed (via DeltaTable.AddColumnAsync) after a data file was
    /// written is absent from that file's parquet — backfill it as an all-NULL array of the field's type; a
    /// column DROPped (via DeltaTable.DropColumnAsync) still exists in old files — drop it from the batch.
    /// Reconciles the batch to exactly <paramref name="expectedFields"/> (the current schema's expected output
    /// columns), taking present columns by name. No-op (returns the batch unchanged) when the batch already
    /// matches the expected column set.
    /// </summary>
    public static RecordBatch BackfillMissingColumns(RecordBatch batch, IReadOnlyList<Field> expectedFields)
    {
        var present = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
            present[batch.Schema.FieldsList[i].Name] = i;

        // Reconcile every expected column (recursing into STRUCT children — a field ADDed/DROPped inside a
        // nested struct after this file was written must be backfilled/removed at its nesting level too).
        bool changed = batch.Schema.FieldsList.Count != expectedFields.Count;

        // ORDER is part of the reconciliation, not just membership. The loop below emits in expectedFields
        // order, but the !changed fast path returns the source batch VERBATIM — so without this check the
        // emitted column order depended on whether anything else happened to need rebuilding: a projection
        // came back in the caller's order from a file that matched the schema, and in the table's order from
        // one that needed a backfill. Two files of the SAME table could disagree, which is not a shape any
        // caller can bind to and not a schema GetReadSchema could promise.
        if (!changed)
        {
            for (int i = 0; i < expectedFields.Count; i++)
            {
                if (!string.Equals(
                        batch.Schema.FieldsList[i].Name, expectedFields[i].Name, StringComparison.Ordinal))
                {
                    changed = true;
                    break;
                }
            }
        }
        var arrays = new List<IArrowArray>(expectedFields.Count);
        var schemaBuilder = new Apache.Arrow.Schema.Builder();
        foreach (var f in expectedFields)
        {
            IArrowArray reconciled;
            if (present.TryGetValue(f.Name, out int idx))
            {
                var column = batch.Column(idx);
                reconciled = ReconcileColumn(column, f.DataType, batch.Length);
                if (ReferenceEquals(reconciled, column))
                {
                    // Pass-through column: keep the SOURCE field. Stamping the expected field onto an array
                    // that was not touched can describe it wrongly — the names match by construction, but the
                    // expected TYPE may not be the one the array actually carries (a host-transport form, an
                    // unconverted widening pair). A batch whose schema contradicts its arrays is the same
                    // class of silent lie the positional pairing in ValueWidener told.
                    schemaBuilder.Field(batch.Schema.FieldsList[idx]);
                }
                else
                {
                    changed = true;
                    // Rebuilt to the expected STRUCTURE, but a child the rebuild passed through keeps its own
                    // type, so the label is the rebuilt array's type under the expected field's name.
                    schemaBuilder.Field(WithType(f, reconciled.Data.DataType));
                }
            }
            else
            {
                reconciled = ArrowCompute.MakeNullArray(f.DataType, batch.Length);
                changed = true;
                schemaBuilder.Field(f);
            }
            arrays.Add(reconciled);
        }
        if (!changed)
            return batch; // common path — file matches the current schema, no rebuild.
        return new RecordBatch(schemaBuilder.Build(), arrays, batch.Length);
    }

    // Reconciles ONE column against its expected type: a STRUCT whose child set differs from the expected
    // struct (nested ADD/DROP after the file was written) is rebuilt — missing children backfilled as typed
    // all-NULL arrays, extra children dropped, children recursed. Non-structs (and matching structs) pass
    // through unchanged (reference-equal). Struct children are NOT sliced with the parent, so backfilled
    // child arrays are sized to the PHYSICAL child length (parent offset + length; see the TakeRows
    // convention) and the parent's offset/validity are preserved on the rebuilt array.
    //
    // Lists and maps are recursed through too: a struct INSIDE a list element or a map key/value evolves the same
    // way (Spark: ALTER TABLE ADD COLUMNS (l.element.b ...)), and an old file's element structs must come back in
    // the table's shape. Only the child is rebuilt; the container keeps its offsets, validity and offset.
    private static IArrowArray ReconcileColumn(IArrowArray column, IArrowType expectedType, int logicalLength)
    {
        switch (expectedType, column.Data.DataType)
        {
            // MapType derives from ListType, so it has to be matched first.
            case (MapType em, MapType am):
            {
                var entries = column.Data.Children[0];
                var key = ReconcileChild(entries.Children[0], em.KeyField.DataType);
                var value = ReconcileChild(entries.Children[1], em.ValueField.DataType);
                if (ReferenceEquals(key, entries.Children[0]) && ReferenceEquals(value, entries.Children[1]))
                    return column;
                var mapType = new MapType(
                    WithType(em.KeyField, key.DataType), WithType(em.ValueField, value.DataType), am.KeySorted);
                var newEntries = new ArrayData(mapType.KeyValueType, entries.Length, entries.NullCount,
                    entries.Offset, entries.Buffers, [key, value]);
                return ArrowArrayFactory.BuildArray(new ArrayData(mapType, column.Data.Length,
                    column.Data.NullCount, column.Data.Offset, column.Data.Buffers, [newEntries]));
            }
            case (MapType, _):
            case (_, MapType):
                return column;
            case (ListType el, ListType):
            {
                var values = ReconcileChild(column.Data.Children[0], el.ValueDataType);
                return ReferenceEquals(values, column.Data.Children[0])
                    ? column
                    : ArrowArrayFactory.BuildArray(new ArrayData(
                        new ListType(WithType(el.ValueField, values.DataType)), column.Data.Length,
                        column.Data.NullCount, column.Data.Offset, column.Data.Buffers, [values]));
            }
            case (LargeListType el, LargeListType):
            {
                var values = ReconcileChild(column.Data.Children[0], el.ValueDataType);
                return ReferenceEquals(values, column.Data.Children[0])
                    ? column
                    : ArrowArrayFactory.BuildArray(new ArrayData(
                        new LargeListType(WithType(el.ValueField, values.DataType)), column.Data.Length,
                        column.Data.NullCount, column.Data.Offset, column.Data.Buffers, [values]));
            }
        }

        if (expectedType is not Apache.Arrow.Types.StructType expectedStruct || column is not StructArray sa)
            return column;

        var actualStruct = (Apache.Arrow.Types.StructType)sa.Data.DataType;
        var childIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int i = 0; i < actualStruct.Fields.Count; i++)
            childIndex[actualStruct.Fields[i].Name] = i;

        int physicalLength = sa.Data.Offset + sa.Length;
        foreach (var child in sa.Fields)
            physicalLength = System.Math.Max(physicalLength, child.Length);

        bool changed = actualStruct.Fields.Count != expectedStruct.Fields.Count;
        var children = new List<IArrowArray>(expectedStruct.Fields.Count);
        for (int i = 0; i < expectedStruct.Fields.Count; i++)
        {
            var expectedChild = expectedStruct.Fields[i];
            IArrowArray reconciled;
            if (childIndex.TryGetValue(expectedChild.Name, out int idx))
            {
                if (idx != i)
                    changed = true; // reordered relative to the expected layout
                var child = sa.Fields[idx];
                reconciled = ReconcileColumn(child, expectedChild.DataType, child.Length);
                if (!ReferenceEquals(reconciled, child))
                    changed = true;
            }
            else
            {
                reconciled = ArrowCompute.MakeNullArray(expectedChild.DataType, physicalLength);
                changed = true;
            }
            children.Add(reconciled);
        }
        if (!changed)
            return column;

        // The expected children's names and order, each with the type its array actually has: a child passed
        // through unchanged may carry a type the reconcile does not convert (an old file's millisecond timestamp
        // under a microsecond schema), and declaring the expected one would contradict the buffers.
        var fields = new List<Field>(children.Count);
        for (int i = 0; i < children.Count; i++)
            fields.Add(WithType(expectedStruct.Fields[i], children[i].Data.DataType));
        return new StructArray(
            new Apache.Arrow.Types.StructType(fields), sa.Length, children, sa.NullBitmapBuffer, sa.NullCount,
            sa.Data.Offset);
    }

    // A container's child (list values, map keys or values) reconciled against its expected type; `data` itself
    // when nothing changed.
    private static ArrayData ReconcileChild(ArrayData data, IArrowType expectedType)
    {
        var child = ArrowArrayFactory.BuildArray(data);
        var reconciled = ReconcileColumn(child, expectedType, child.Length);
        return ReferenceEquals(reconciled, child) ? data : reconciled.Data;
    }

    // The expected field's name, nullability and metadata, with the type the reconciled child actually has.
    private static Field WithType(Field expected, IArrowType type) =>
        ReferenceEquals(expected.DataType, type)
            ? expected
            : new Field(expected.Name, type, expected.IsNullable, expected.Metadata);

}
