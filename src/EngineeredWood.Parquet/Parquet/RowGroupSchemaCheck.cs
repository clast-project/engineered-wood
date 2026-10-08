// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet;

/// <summary>
/// Refuses a row group whose Arrow shape differs from the one the file's schema was built from. A Parquet file has
/// one schema, fixed by its first batch, and the writers encode every later batch against it; a mismatch is not
/// an error there, it is silently wrong data (an Int64 batch written into an Int32 column comes back truncated).
/// </summary>
/// <remarks>
/// Compared at every depth: column count, names and order; struct child names and order; types, including their
/// parameters (decimal precision and scale, time units, byte widths, list sizes). Not compared: field metadata, the
/// names Arrow gives list and map children (Parquet writes its own), a run-end encoded column's run-end type, and a
/// timestamp's time zone beyond whether it is null (Parquet records only that, as isAdjustedToUTC). A null that
/// would be written into a column the file made required is refused, since the file cannot represent it and the
/// writers would encode it as a value; map keys are always required. Nullable flags themselves are not compared,
/// and a null the writers never reach (under a null parent, outside a list's offsets) is accepted.
/// </remarks>
internal static class RowGroupSchemaCheck
{
    public static void EnsureMatches(Apache.Arrow.Schema fileSchema, RecordBatch batch)
    {
        var expected = fileSchema.FieldsList;
        var actual = batch.Schema.FieldsList;
        CheckChildren(expected, actual, i => batch.Column(i), prefix: null, AllSlots, firstBatch: false);
    }

    /// <summary>
    /// Refuses a first batch with a null in a column its own schema makes required (a non-nullable field, or a map
    /// key). Arrow does not enforce the nullable flag, and the writers encode a required column without definition
    /// levels, so each such null would be written as a value (an Int32 null reads back as 0).
    /// </summary>
    public static void EnsureWritable(RecordBatch batch)
    {
        var fields = batch.Schema.FieldsList;
        CheckChildren(fields, fields, i => batch.Column(i), prefix: null, AllSlots, firstBatch: true);
    }

    // The positions of an array that the writers actually encode, in the array's own index space (what IsNull
    // takes); null means every position. A slot under a null parent, or outside a list's offsets, is never
    // written, so a null there is harmless even in a required column. Computed only when a null check needs it.
    private delegate IReadOnlyList<int>? WrittenSlots();

    private static readonly WrittenSlots AllSlots = () => null;

    private static void CheckChildren(
        IReadOnlyList<Field> expected, IReadOnlyList<Field> actual, Func<int, IArrowArray?> arrayAt, string? prefix,
        WrittenSlots slots, bool firstBatch)
    {
        int common = Math.Min(expected.Count, actual.Count);
        for (int i = 0; i < common; i++)
        {
            string path = Join(prefix, actual[i].Name);
            if (!string.Equals(expected[i].Name, actual[i].Name, StringComparison.Ordinal))
            {
                throw Mismatch(path,
                    $"the file has column '{Join(prefix, expected[i].Name)}' in this position");
            }
            CheckField(expected[i], actual[i], arrayAt(i), path, slots, firstBatch);
        }

        if (actual.Count > common)
            throw Mismatch(Join(prefix, actual[common].Name), "the file has no such column");
        if (expected.Count > common)
            throw Mismatch(Join(prefix, expected[common].Name), "the row group is missing this column");
    }

    // alwaysRequired: map keys, which ArrowToSchemaConverter and NestedLevelWriter write as required whatever the
    // Arrow key field says.
    private static void CheckField(
        Field expected, Field actual, IArrowArray? array, string path, WrittenSlots slots, bool firstBatch,
        bool alwaysRequired = false)
    {
        if ((alwaysRequired || !expected.IsNullable) && HasWrittenNull(array, slots))
        {
            if (firstBatch)
                throw NullInRequired(path, alwaysRequired);
            throw Mismatch(path, "the file's column is required, and the row group has nulls in it");
        }

        CheckType(expected.DataType, actual.DataType, array, path, slots, firstBatch);
    }

    private static void CheckType(
        IArrowType expected, IArrowType actual, IArrowArray? array, string path, WrittenSlots slots,
        bool firstBatch)
    {
        if (expected.TypeId != actual.TypeId)
            throw TypeMismatch(path, expected, actual);

        switch (expected)
        {
            case ExtensionType ee:
                var ae = (ExtensionType)actual;
                if (!string.Equals(ee.Name, ae.Name, StringComparison.Ordinal))
                    throw TypeMismatch(path, expected, actual);
                CheckType(ee.StorageType, ae.StorageType, (array as ExtensionArray)?.Storage, path, slots, firstBatch);
                return;

            case StructType es:
                var sa = array as StructArray;
                CheckChildren(es.Fields, ((StructType)actual).Fields, i => sa?.Fields[i], path,
                    sa is null ? AllSlots : StructChildSlots(sa, slots), firstBatch);
                return;

            case MapType em:
                var am = (MapType)actual;
                var ma = array as MapArray;
                var entries = ma is null ? AllSlots : ListChildSlots(ma, slots);
                CheckField(em.KeyField, am.KeyField, ma?.Keys, path + ".key", entries, firstBatch,
                    alwaysRequired: true);
                CheckField(em.ValueField, am.ValueField, ma?.Values, path + ".value", entries, firstBatch);
                return;

            case ListType el:
                var la = array as ListArray;
                CheckField(el.ValueField, ((ListType)actual).ValueField, la?.Values, path + ".element",
                    la is null ? AllSlots : ListChildSlots(la, slots), firstBatch);
                return;

            case LargeListType ell:
                var lla = array as LargeListArray;
                CheckField(ell.ValueField, ((LargeListType)actual).ValueField, lla?.Values, path + ".element",
                    lla is null ? AllSlots : LargeListChildSlots(lla, slots), firstBatch);
                return;

            case FixedSizeListType ef:
                var af = (FixedSizeListType)actual;
                if (ef.ListSize != af.ListSize)
                    throw TypeMismatch(path, expected, actual);
                var fa = array as FixedSizeListArray;
                CheckField(ef.ValueField, af.ValueField, fa?.Values, path + ".element",
                    fa is null ? AllSlots : FixedSizeListChildSlots(fa, ef.ListSize, slots), firstBatch);
                return;

            // The run ends only say where each run stops, and Parquet never sees them; the values are what is
            // encoded, so they are what must match.
            case RunEndEncodedType er:
                CheckType(er.ValuesDataType, ((RunEndEncodedType)actual).ValuesDataType,
                    (array as RunEndEncodedArray)?.Values, path, AllSlots, firstBatch);
                return;

            case DictionaryType ed:
                var ad = (DictionaryType)actual;
                CheckType(ed.IndexType, ad.IndexType, null, path, AllSlots, firstBatch);
                CheckType(ed.ValueType, ad.ValueType, null, path, AllSlots, firstBatch);
                return;
        }

        if (!SameParameters(expected, actual))
            throw TypeMismatch(path, expected, actual);
    }

    private static bool HasWrittenNull(IArrowArray? array, WrittenSlots slots)
    {
        switch (array)
        {
            case null:
                return false;
            // A run-end encoded array has no validity bitmap of its own: its nulls are null runs in Values, which
            // is where ColumnChunkWriter's def levels come from too. Counted whole, since REE is written flat.
            case RunEndEncodedArray ree:
                return ree.Values.NullCount > 0;
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

    private static IEnumerable<int> Slots(IArrowArray parent, WrittenSlots slots) =>
        slots() ?? Enumerable.Range(0, parent.Length);

    // The child mappings follow NestedLevelWriter. A struct's children are NOT sliced with it, so slot i's child
    // is at the struct's offset + i; a null struct slot writes no child value.
    private static WrittenSlots StructChildSlots(StructArray parent, WrittenSlots slots) => () =>
    {
        var result = new List<int>();
        int offset = parent.Data.Offset;
        foreach (int slot in Slots(parent, slots))
        {
            if (!parent.IsNull(slot))
                result.Add(offset + slot);
        }
        return result;
    };

    // ValueOffsets already applies the list's own offset.
    private static WrittenSlots ListChildSlots(ListArray parent, WrittenSlots slots) => () =>
    {
        var result = new List<int>();
        var offsets = parent.ValueOffsets;
        foreach (int slot in Slots(parent, slots))
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
        foreach (int slot in Slots(parent, slots))
        {
            if (parent.IsNull(slot))
                continue;
            for (long j = offsets[slot]; j < offsets[slot + 1]; j++)
                result.Add(checked((int)j));
        }
        return result;
    };

    // A fixed-size list's child is NOT sliced with it either: slot i's elements start at (offset + i) * width.
    private static WrittenSlots FixedSizeListChildSlots(FixedSizeListArray parent, int width, WrittenSlots slots) =>
        () =>
        {
            var result = new List<int>();
            int offset = parent.Data.Offset;
            foreach (int slot in Slots(parent, slots))
            {
                if (parent.IsNull(slot))
                    continue;
                int start = checked((offset + slot) * width);
                for (int j = 0; j < width; j++)
                    result.Add(start + j);
            }
            return result;
        };

    // Leaf types whose TypeId leaves parameters open.
    private static bool SameParameters(IArrowType expected, IArrowType actual) => (expected, actual) switch
    {
        _ when Decimal(expected) is { } ed => ed == Decimal(actual),
        (TimestampType et, TimestampType at) =>
            // Null, not empty, is "no zone": ArrowToSchemaConverter sets isAdjustedToUTC from Timezone != null.
            et.Unit == at.Unit && (et.Timezone is null) == (at.Timezone is null),
        (Time32Type et, Time32Type at) => et.Unit == at.Unit,
        (Time64Type et, Time64Type at) => et.Unit == at.Unit,
        (DurationType ed, DurationType ad) => ed.Unit == ad.Unit,
        (IntervalType ei, IntervalType ai) => ei.Unit == ai.Unit,
        (FixedSizeBinaryType ef, FixedSizeBinaryType af) => ef.ByteWidth == af.ByteWidth,
        _ => true,
    };

    private static (int Precision, int Scale)? Decimal(IArrowType type) => type switch
    {
        Decimal32Type d => (d.Precision, d.Scale),
        Decimal64Type d => (d.Precision, d.Scale),
        Decimal128Type d => (d.Precision, d.Scale),
        Decimal256Type d => (d.Precision, d.Scale),
        _ => null,
    };

    private static string Join(string? prefix, string name) => prefix is null ? name : prefix + "." + name;

    private static ArgumentException TypeMismatch(string path, IArrowType expected, IArrowType actual) =>
        Mismatch(path, $"the file has {Describe(expected)}, the row group has {Describe(actual)}");

    private static ArgumentException Mismatch(string path, string detail) =>
        new($"The row group does not match the file's schema at column '{path}': {detail}. A Parquet file has "
            + "one schema, fixed by the first batch written; write a batch of a different shape to a new file.",
            "batch");

    private static ArgumentException NullInRequired(string path, bool mapKey) =>
        new($"The batch has nulls in column '{path}', which "
            + (mapKey ? "is a map key, and Parquet map keys are always required"
                : "its schema declares non-nullable, so Parquet writes it as required")
            + "; a required column cannot hold a null, and the writers would encode each one as a value. "
            + (mapKey ? "Remove the null keys." : "Declare the field nullable, or remove the nulls."),
            "batch");

    private static string Describe(IArrowType type) => type switch
    {
        _ when Decimal(type) is var (p, sc) => $"{type.Name}({p}, {sc})",
        TimestampType t => $"timestamp[{t.Unit}{(t.Timezone is null ? "" : $", \"{t.Timezone}\"")}]",
        Time32Type t => $"time32[{t.Unit}]",
        Time64Type t => $"time64[{t.Unit}]",
        DurationType d => $"duration[{d.Unit}]",
        FixedSizeBinaryType f => $"fixed_size_binary[{f.ByteWidth}]",
        FixedSizeListType f => $"fixed_size_list[{f.ListSize}]",
        ExtensionType e => $"extension<{e.Name}>",
        _ => type.Name,
    };
}
