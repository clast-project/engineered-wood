// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
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
    /// <param name="batch">The rows being written. Its fields bind to the schema's as the write's physical rename
    /// binds them: by logical name across the whole level first, then by physical name.</param>
    /// <param name="schema">The table schema the rows are written under.</param>
    /// <param name="writerSupplied">Columns the writer fills in after this check (identity columns), keyed by
    /// name: <c>true</c> when the writer replaces any value the batch carries, <c>false</c> when it fills the
    /// column only if the batch omits it. A column the writer supplies is never refused for being absent.</param>
    /// <param name="rows">The rows of <paramref name="batch"/> to check; null checks every row.</param>
    /// <exception cref="DeltaFormatException">A row holds a forbidden NULL.</exception>
    public static void Validate(
        RecordBatch batch, StructType schema, IReadOnlyDictionary<string, bool>? writerSupplied = null,
        IReadOnlyList<int>? rows = null)
    {
        if (batch.Length == 0 || rows is { Count: 0 })
            return;

        var bound = Bind(batch.Schema.FieldsList, schema.Fields);
        for (int f = 0; f < schema.Fields.Count; f++)
        {
            var field = schema.Fields[f];
            bool replaced = false;
            bool supplied = writerSupplied is not null && writerSupplied.TryGetValue(field.Name, out replaced);
            if (supplied && replaced)
                continue;

            if (bound[f] < 0)
            {
                if (!field.Nullable && !supplied)
                    throw Absent(field.Name);
                continue;
            }

            Check(field.Type, field.Nullable, batch.Column(bound[f]), rows, field.Name);
        }
    }

    // Slots: the positions of the array that hold a row's value, in the array's own index space; null is every
    // position. A position under a null parent or outside a list's offsets is no row's value.
    private static void Check(
        DeltaDataType type, bool nullable, IArrowArray array, IReadOnlyList<int>? slots, string path)
    {
        // Nothing at or below this level can be violated, so nothing is decoded or gathered.
        if (nullable && !Constrains(type))
            return;

        switch (array)
        {
            case ExtensionArray extension:
                Check(type, nullable, extension.Storage, slots, path);
                return;

            // No validity bitmap of its own: its nulls are null runs in its values.
            case RunEndEncodedArray runs:
                Check(type, nullable, RunEndEncoding.Expand(runs), slots, path);
                return;

            // A row's value is the dictionary entry its index names, so a null index is a null, and the entries
            // the present indices name are checked as the values themselves (an entry no row uses is not).
            case DictionaryArray dictionary:
            {
                var indices = dictionary.Indices;
                if (!nullable && HasNull(indices, slots))
                    throw Violation(path);
                var entries = new List<int>();
                foreach (int slot in slots ?? Enumerable.Range(0, indices.Length))
                {
                    if (!indices.IsNull(slot))
                        entries.Add(checked((int)IndexAt(indices, slot)));
                }
                Check(type, nullable, dictionary.Dictionary, entries, path);
                return;
            }
        }

        if (!nullable && HasNull(array, slots))
            throw Violation(path);

        switch (type, array)
        {
            case (StructType declared, StructArray structs):
            {
                var present = PresentSlots(structs, slots);
                if (present.Count == 0)
                    return;
                var bound = Bind(((ArrowStructType)structs.Data.DataType).Fields, declared.Fields);
                for (int f = 0; f < declared.Fields.Count; f++)
                {
                    var child = declared.Fields[f];
                    if (bound[f] < 0)
                    {
                        if (!child.Nullable)
                            throw Absent(path + "." + child.Name);
                        continue;
                    }
                    Check(child.Type, child.Nullable, structs.Fields[bound[f]], present, path + "." + child.Name);
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

    // Whether anything strictly below a value of this type is non-nullable. A map always is: its keys.
    private static bool Constrains(DeltaDataType type) =>
        type is PrimitiveType ? false : ConstrainsCache.GetValue(type, static t => new StrongBox<bool>(t switch
        {
            StructType s => s.Fields.Any(f => !f.Nullable || Constrains(f.Type)),
            ArrayType a => !a.ContainsNull || Constrains(a.ElementType),
            MapType => true,
            _ => false,
        })).Value;

    // Keyed weakly on the schema's type objects, which a snapshot reuses for every batch it writes.
    private static readonly ConditionalWeakTable<DeltaDataType, StrongBox<bool>> ConstrainsCache = new();

    private static bool HasNull(IArrowArray array, IReadOnlyList<int>? slots)
    {
        if (array.NullCount == 0)
            return false;
        if (slots is null)
            return true;
        foreach (int slot in slots)
            if (array.IsNull(slot))
                return true;
        return false;
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
        foreach (int slot in slots ?? Enumerable.Range(0, parent.Length))
        {
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

    // For each declared field, the index of the Arrow field the write's physical rename binds to it, or -1. That
    // rename (ColumnMappingRecursive.ToPhysical) resolves an Arrow name by LOGICAL name across the whole level
    // first and by physical name only when no field has that logical name. After a rename one column's physical
    // name can be another's logical name, so matching either spelling field by field could credit a column
    // with another column's values.
    private static int[] Bind(IReadOnlyList<Field> arrowFields, IReadOnlyList<StructField> declared)
    {
        var byLogical = new Dictionary<string, int>(StringComparer.Ordinal);
        var byPhysical = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int f = 0; f < declared.Count; f++)
        {
            if (!byLogical.ContainsKey(declared[f].Name))
                byLogical[declared[f].Name] = f;
            if (declared[f].Metadata is { } md
                && md.TryGetValue(ColumnMapping.PhysicalNameKey, out var physical)
                && !byPhysical.ContainsKey(physical))
            {
                byPhysical[physical] = f;
            }
        }

        var bound = new int[declared.Count];
        for (int f = 0; f < bound.Length; f++)
            bound[f] = -1;
        for (int i = 0; i < arrowFields.Count; i++)
        {
            string name = arrowFields[i].Name;
            if ((byLogical.TryGetValue(name, out int f) || byPhysical.TryGetValue(name, out f)) && bound[f] < 0)
                bound[f] = i;
        }
        return bound;
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
