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
/// names Arrow gives list and map children (Parquet writes its own), and a timestamp's time zone beyond whether it
/// has one (Parquet records only that). A nullable field where the file's is required is refused only when it
/// holds nulls, since the file cannot represent them and the writers would encode each one as a value.
/// </remarks>
internal static class RowGroupSchemaCheck
{
    public static void EnsureMatches(Apache.Arrow.Schema fileSchema, RecordBatch batch)
    {
        var expected = fileSchema.FieldsList;
        var actual = batch.Schema.FieldsList;
        CheckChildren(expected, actual, i => batch.Column(i), prefix: null);
    }

    private static void CheckChildren(
        IReadOnlyList<Field> expected, IReadOnlyList<Field> actual, Func<int, IArrowArray?> arrayAt, string? prefix)
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
            CheckField(expected[i], actual[i], arrayAt(i), path);
        }

        if (actual.Count > common)
            throw Mismatch(Join(prefix, actual[common].Name), "the file has no such column");
        if (expected.Count > common)
            throw Mismatch(Join(prefix, expected[common].Name), "the row group is missing this column");
    }

    private static void CheckField(Field expected, Field actual, IArrowArray? array, string path)
    {
        if (!expected.IsNullable && actual.IsNullable && array is { NullCount: > 0 })
            throw Mismatch(path, "the file's column is required, and the row group has nulls in it");

        CheckType(expected.DataType, actual.DataType, array, path);
    }

    private static void CheckType(IArrowType expected, IArrowType actual, IArrowArray? array, string path)
    {
        if (expected.TypeId != actual.TypeId)
            throw TypeMismatch(path, expected, actual);

        switch (expected)
        {
            case ExtensionType ee:
                var ae = (ExtensionType)actual;
                if (!string.Equals(ee.Name, ae.Name, StringComparison.Ordinal))
                    throw TypeMismatch(path, expected, actual);
                CheckType(ee.StorageType, ae.StorageType, (array as ExtensionArray)?.Storage, path);
                return;

            case StructType es:
                var sa = array as StructArray;
                CheckChildren(es.Fields, ((StructType)actual).Fields, i => sa?.Fields[i], path);
                return;

            case MapType em:
                var am = (MapType)actual;
                var ma = array as MapArray;
                CheckField(em.KeyField, am.KeyField, ma?.Keys, path + ".key");
                CheckField(em.ValueField, am.ValueField, ma?.Values, path + ".value");
                return;

            case ListType el:
                CheckField(el.ValueField, ((ListType)actual).ValueField, (array as ListArray)?.Values,
                    path + ".element");
                return;

            case LargeListType ell:
                CheckField(ell.ValueField, ((LargeListType)actual).ValueField, (array as LargeListArray)?.Values,
                    path + ".element");
                return;

            case FixedSizeListType ef:
                var af = (FixedSizeListType)actual;
                if (ef.ListSize != af.ListSize)
                    throw TypeMismatch(path, expected, actual);
                CheckField(ef.ValueField, af.ValueField, (array as FixedSizeListArray)?.Values, path + ".element");
                return;

            case DictionaryType ed:
                var ad = (DictionaryType)actual;
                CheckType(ed.IndexType, ad.IndexType, null, path);
                CheckType(ed.ValueType, ad.ValueType, null, path);
                return;
        }

        if (!SameParameters(expected, actual))
            throw TypeMismatch(path, expected, actual);
    }

    // Leaf types whose TypeId leaves parameters open.
    private static bool SameParameters(IArrowType expected, IArrowType actual) => (expected, actual) switch
    {
        _ when Decimal(expected) is { } ed => ed == Decimal(actual),
        (TimestampType et, TimestampType at) =>
            et.Unit == at.Unit && string.IsNullOrEmpty(et.Timezone) == string.IsNullOrEmpty(at.Timezone),
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

    private static string Describe(IArrowType type) => type switch
    {
        _ when Decimal(type) is var (p, sc) => $"{type.Name}({p}, {sc})",
        TimestampType t => $"timestamp[{t.Unit}{(string.IsNullOrEmpty(t.Timezone) ? "" : ", " + t.Timezone)}]",
        Time32Type t => $"time32[{t.Unit}]",
        Time64Type t => $"time64[{t.Unit}]",
        DurationType d => $"duration[{d.Unit}]",
        FixedSizeBinaryType f => $"fixed_size_binary[{f.ByteWidth}]",
        FixedSizeListType f => $"fixed_size_list[{f.ListSize}]",
        ExtensionType e => $"extension<{e.Name}>",
        _ => type.Name,
    };
}
