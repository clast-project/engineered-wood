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
                    schemaBuilder.Field(f); // rebuilt to the expected structure, so the expected label is right
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
    private static IArrowArray ReconcileColumn(IArrowArray column, IArrowType expectedType, int logicalLength)
    {
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

        return new StructArray(
            expectedStruct, sa.Length, children, sa.NullBitmapBuffer, sa.NullCount, sa.Data.Offset);
    }

}
