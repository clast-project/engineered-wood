// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Schema;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Refuses rows that put a NULL where the table's schema says none can be: a non-nullable column or struct
/// field, an array element of an array that does not contain null, a map key, or a map value of a map whose
/// values do not contain null (#514).
/// </summary>
/// <remarks>
/// <para>
/// The schema checked against is the TABLE's, never the batch's own fields. The Parquet writer refuses a null
/// in a field the batch declares non-nullable, but a batch that declares the field nullable went through, and
/// the table's schema then misdescribed its data. A reader that trusts nullability, as Spark's optimizer does
/// when it folds <c>x IS NULL</c> to false, gives wrong answers over such a file. delta-spark refuses the write
/// with <c>DELTA_NOT_NULL_CONSTRAINT_VIOLATED</c>, and so does this.
/// </para>
/// <para>
/// A nested field applies only where every parent holds a value: a non-nullable child of a null struct is not
/// a violation, nor is anything under a null list or map. A declared column or struct field the batch omits
/// reads as null, so omitting a non-nullable one is refused too (wherever its parent is non-null).
/// </para>
/// <para>
/// Delta enforces nullability at write time only, as it does CHECK constraints, so this runs beside
/// <see cref="DeltaConstraintEnforcer"/>. It is not a "write-time expression" though: it needs no expression
/// evaluator and every table may carry it, so it does not gate the external-file commit seam.
/// </para>
/// </remarks>
internal static class DeltaNotNullEnforcer
{
    /// <summary>Refuses <paramref name="batch"/> if any row holds a NULL that <paramref name="schema"/> forbids.</summary>
    /// <param name="batch">The rows being written, by logical name (a physical name is also matched).</param>
    /// <param name="schema">The table schema the rows are written under.</param>
    /// <param name="writerSupplied">Columns the writer fills in after this check (identity columns), keyed by
    /// name: <c>true</c> when the writer replaces any value the batch carries, <c>false</c> when it fills the
    /// column only if the batch omits it. A column the writer supplies is never refused for being absent.</param>
    /// <exception cref="DeltaFormatException">A row holds a forbidden NULL.</exception>
    public static void Validate(
        RecordBatch batch, StructType schema, IReadOnlyDictionary<string, bool>? writerSupplied = null)
    {
        if (batch.Length == 0)
            return;

        var arrowFields = batch.Schema.FieldsList;
        foreach (var field in schema.Fields)
        {
            bool replaced = false;
            bool supplied = writerSupplied is not null && writerSupplied.TryGetValue(field.Name, out replaced);
            if (supplied && replaced)
                continue;

            int index = IndexOf(arrowFields, field);
            if (index < 0)
            {
                if (!field.Nullable && !supplied)
                    throw Absent(field.Name);
                continue;
            }

            Check(field.Type, field.Nullable, batch.Column(index), slots: null, field.Name);
        }
    }

    // Slots: the positions of the array that hold a row's value, in the array's own index space; null is every
    // position. A position under a null parent or outside a list's offsets is no row's value.
    private static void Check(
        DeltaDataType type, bool nullable, IArrowArray array, IReadOnlyList<int>? slots, string path)
    {
        array = Logical(array);
        if (!nullable && HasNull(array, slots))
            throw Violation(path);

        switch (type, array)
        {
            case (StructType declared, StructArray structs):
            {
                var present = PresentSlots(structs, slots);
                if (present.Count == 0)
                    return;
                var children = ((ArrowStructType)structs.Data.DataType).Fields;
                foreach (var child in declared.Fields)
                {
                    int index = IndexOf(children, child);
                    if (index < 0)
                    {
                        if (!child.Nullable)
                            throw Absent(path + "." + child.Name);
                        continue;
                    }
                    Check(child.Type, child.Nullable, structs.Fields[index], present, path + "." + child.Name);
                }
                return;
            }

            case (ArrayType declared, ListArray list):
                Check(declared.ElementType, declared.ContainsNull, list.Values,
                    ListSlots(list, slots, list.ValueOffsets), path + ".element");
                return;

            case (ArrayType declared, LargeListArray list):
            {
                var offsets = list.ValueOffsets;
                var elements = new List<int>();
                foreach (int slot in PresentSlots(list, slots))
                {
                    for (long j = offsets[slot]; j < offsets[slot + 1]; j++)
                        elements.Add(checked((int)j));
                }
                Check(declared.ElementType, declared.ContainsNull, list.Values, elements, path + ".element");
                return;
            }

            case (ArrayType declared, FixedSizeListArray list):
            {
                // A fixed-size list's child is not sliced with it: slot i's elements start at (offset + i) * width.
                int width = ((Apache.Arrow.Types.FixedSizeListType)list.Data.DataType).ListSize;
                var elements = new List<int>();
                foreach (int slot in PresentSlots(list, slots))
                {
                    int start = checked((list.Data.Offset + slot) * width);
                    for (int j = 0; j < width; j++)
                        elements.Add(start + j);
                }
                Check(declared.ElementType, declared.ContainsNull, list.Values, elements, path + ".element");
                return;
            }

            // A Delta map key is never null; the protocol has no flag for it.
            case (MapType declared, MapArray map):
            {
                var entries = ListSlots(map, slots, map.ValueOffsets);
                Check(declared.KeyType, nullable: false, map.Keys, entries, path + ".key");
                Check(declared.ValueType, declared.ValueContainsNull, map.Values, entries, path + ".value");
                return;
            }
        }

        // A primitive has nothing below it. Any other pairing is a host's own representation of the declared
        // type (a variant as a struct of binaries, under a value-blind writer), whose inner shape is not ours
        // to judge.
    }

    // The array whose validity a row's value is read from: an extension's storage, a run-end encoded array
    // expanded (its nulls are null runs in its values, and it has no validity bitmap of its own).
    private static IArrowArray Logical(IArrowArray array) => array switch
    {
        ExtensionArray extension => Logical(extension.Storage),
        RunEndEncodedArray runs => Logical(RunEndEncoding.Expand(runs)),
        _ => array,
    };

    private static bool HasNull(IArrowArray array, IReadOnlyList<int>? slots)
    {
        // A valid index that points at a null dictionary value is a null too.
        var dictionaryValues = (array as DictionaryArray)?.Dictionary;
        bool valuesHaveNulls = dictionaryValues is not null && dictionaryValues.NullCount > 0;
        if (array.NullCount == 0 && !valuesHaveNulls)
            return false;

        if (slots is null)
        {
            for (int i = 0; i < array.Length; i++)
                if (IsNull(array, i, valuesHaveNulls))
                    return true;
            return false;
        }
        foreach (int slot in slots)
            if (IsNull(array, slot, valuesHaveNulls))
                return true;
        return false;
    }

    private static bool IsNull(IArrowArray array, int index, bool dictionaryValuesHaveNulls)
    {
        if (array.IsNull(index))
            return true;
        if (!dictionaryValuesHaveNulls)
            return false;
        var dictionary = (DictionaryArray)array;
        return dictionary.Dictionary.IsNull(checked((int)IndexAt(dictionary.Indices, index)));
    }

    private static long IndexAt(IArrowArray indices, int i) => indices switch
    {
        Int8Array a => a.GetValue(i)!.Value,
        Int16Array a => a.GetValue(i)!.Value,
        Int32Array a => a.GetValue(i)!.Value,
        Int64Array a => a.GetValue(i)!.Value,
        UInt8Array a => a.GetValue(i)!.Value,
        UInt16Array a => a.GetValue(i)!.Value,
        UInt32Array a => a.GetValue(i)!.Value,
        UInt64Array a => checked((long)a.GetValue(i)!.Value),
        _ => throw new NotSupportedException($"Dictionary index type {indices.Data.DataType.Name}."),
    };

    private static List<int> PresentSlots(IArrowArray parent, IReadOnlyList<int>? slots)
    {
        var result = new List<int>();
        if (slots is null)
        {
            for (int i = 0; i < parent.Length; i++)
                if (!parent.IsNull(i))
                    result.Add(i);
        }
        else
        {
            foreach (int slot in slots)
                if (!parent.IsNull(slot))
                    result.Add(slot);
        }
        return result;
    }

    // ValueOffsets already applies the list's own offset.
    private static List<int> ListSlots(IArrowArray list, IReadOnlyList<int>? slots, ReadOnlySpan<int> offsets)
    {
        var elements = new List<int>();
        foreach (int slot in PresentSlots(list, slots))
        {
            for (int j = offsets[slot]; j < offsets[slot + 1]; j++)
                elements.Add(j);
        }
        return elements;
    }

    // By logical name, or by physical name under column mapping, as the write paths' rename accepts either.
    private static int IndexOf(IReadOnlyList<Field> arrowFields, StructField declared)
    {
        string? physical = null;
        declared.Metadata?.TryGetValue(ColumnMapping.PhysicalNameKey, out physical);
        for (int i = 0; i < arrowFields.Count; i++)
        {
            string name = arrowFields[i].Name;
            if (string.Equals(name, declared.Name, StringComparison.Ordinal)
                || (physical is not null && string.Equals(name, physical, StringComparison.Ordinal)))
            {
                return i;
            }
        }
        return -1;
    }

    private static DeltaFormatException Violation(string path) => new(
        DeltaTableErrorCodes.NotNullConstraintViolated,
        $"NOT NULL constraint violated for column: {path}. A row being written holds a null there, which the "
        + "table's schema declares impossible; the batch's own nullability does not matter. Remove the nulls, "
        + "or make the column nullable.");

    private static DeltaFormatException Absent(string path) => new(
        DeltaTableErrorCodes.NotNullConstraintViolated,
        $"NOT NULL constraint violated for column: {path}. The batch omits it, so every row being written "
        + "would read it as null, which the table's schema declares impossible. Supply the column.");
}
