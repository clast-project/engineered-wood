// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

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
            schemaBuilder.Field(f);
            IArrowArray reconciled;
            if (present.TryGetValue(f.Name, out int idx))
            {
                var column = batch.Column(idx);
                reconciled = ReconcileColumn(column, f.DataType, batch.Length);
                if (!ReferenceEquals(reconciled, column))
                    changed = true;
            }
            else
            {
                reconciled = MakeNullArray(f.DataType, batch.Length);
                changed = true;
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
                reconciled = MakeNullArray(expectedChild.DataType, physicalLength);
                changed = true;
            }
            children.Add(reconciled);
        }
        if (!changed)
            return column;

        return new StructArray(
            expectedStruct, sa.Length, children, sa.NullBitmapBuffer, sa.NullCount, sa.Data.Offset);
    }

    /// <summary>Builds an all-NULL array of the given Arrow type and length (for schema-evolution backfill).</summary>
    /// <summary>Builds an all-null array of <paramref name="type"/>. Shared with
    /// <see cref="TypeWidening.ValueWidener"/> so both backfill paths agree on the typed-NULL shape
    /// (and neither silently substitutes a string column).</summary>
    internal static IArrowArray MakeNullArrayPublic(IArrowType type, int length) =>
        MakeNullArray(type, length);

    private static IArrowArray MakeNullArray(IArrowType type, int length)
    {
        switch (type)
        {
            // MUST precede the StructType case: an extension over a struct storage type (VARIANT)
            // would otherwise backfill as a bare storage struct, dropping the annotation and
            // producing a batch whose column type contradicts the schema it is backfilled into.
            case ExtensionType ext:
                return ext.CreateArray(MakeNullArray(ext.StorageType, length));
            case BooleanType:
            { var b = new BooleanArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int8Type:
            { var b = new Int8Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int16Type:
            { var b = new Int16Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int32Type:
            { var b = new Int32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Int64Type:
            { var b = new Int64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt8Type:
            { var b = new UInt8Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt16Type:
            { var b = new UInt16Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt32Type:
            { var b = new UInt32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case UInt64Type:
            { var b = new UInt64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case FloatType:
            { var b = new FloatArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case DoubleType:
            { var b = new DoubleArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Decimal128Type dec:
            { var b = new Decimal128Array.Builder(dec); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Date32Type:
            { var b = new Date32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case TimestampType ts:
            { var b = new TimestampArray.Builder(ts); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case BinaryType:
            { var b = new BinaryArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Decimal256Type dec:
            { var b = new Decimal256Array.Builder(dec); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Date64Type:
            { var b = new Date64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Time32Type t32:
            { var b = new Time32Array.Builder(t32); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Time64Type t64:
            { var b = new Time64Array.Builder(t64); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            case Apache.Arrow.Types.StructType st:
            {
                // An all-null struct: zeroed validity + typed all-null children (children length == the
                // struct's own length; the caller passes the PHYSICAL length when backfilling a child).
                var children = new List<IArrowArray>(st.Fields.Count);
                foreach (var f in st.Fields)
                    children.Add(MakeNullArray(f.DataType, length));
                return new StructArray(st, length, children, AllNullBitmap(length), nullCount: length);
            }
            case ListType lt:
            {
                // An all-null list: zeroed validity + all-zero offsets over an empty values child.
                var offsets = new ArrowBuffer.Builder<int>(length + 1);
                for (int i = 0; i <= length; i++) offsets.Append(0);
                return new ListArray(lt, length, offsets.Build(), MakeNullArray(lt.ValueDataType, 0),
                                     AllNullBitmap(length), nullCount: length);
            }
            case StringType:
            { var b = new StringArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
            default:
                throw new NotSupportedException(
                    $"Schema-evolution backfill has no NULL-array builder for Arrow type '{type.Name}'.");
        }
    }

    private static ArrowBuffer AllNullBitmap(int length)
    {
        var bitmap = new ArrowBuffer.Builder<byte>((length + 7) / 8);
        for (int i = 0; i < (length + 7) / 8; i++) bitmap.Append(0);
        return bitmap.Build();
    }
}
