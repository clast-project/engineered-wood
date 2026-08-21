// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Reconciles temporal columns between Arrow's four time units and the three Parquet has, and
/// restores the zone names Parquet cannot store.
/// </summary>
/// <remarks>
/// <para>Parquet's TIMESTAMP and TIME annotations carry only MILLIS, MICROS and NANOS. A
/// second-precision Arrow column therefore has no unit to write as. Relabelling it was the original
/// silent bug — <c>Timestamp(Second)</c> holding 1700000000 read back a million times too small —
/// and refusing it outright made a whole Arrow type unwritable. Rescaling is the third option:
/// seconds become milliseconds by multiplying the values, so the column is writable and every
/// instant is preserved exactly.</para>
///
/// <para>The read direction restores the ZONE from <c>ARROW:schema</c> and deliberately leaves the
/// unit alone. That asymmetry is PyArrow's, measured rather than assumed: it reads its own
/// <c>timestamp[s]</c> column back as <c>timestamp[ms]</c> while restoring
/// <c>tz=America/New_York</c> faithfully. Handing seconds back here would make EngineeredWood the
/// outlier in the opposite direction.</para>
///
/// <para>Both directions recurse into structs, lists, fixed-size lists and maps, because such a
/// column can sit at any depth — a map KEY of <c>timestamp[s]</c> is one of the shapes that found
/// this.</para>
/// </remarks>
internal static class TimeUnitRescaler
{
    private const long MillisPerSecond = 1000;

    /// <summary>The Arrow type Parquet can express for <paramref name="type"/>, recursively.</summary>
    /// <param name="type">The declared Arrow type.</param>
    internal static IArrowType ToParquetUnits(IArrowType type) => RewriteType(type, SecondsToMillis);

    private static IArrowType? SecondsToMillis(IArrowType type) => type switch
    {
        TimestampType { Unit: TimeUnit.Second } ts => new TimestampType(TimeUnit.Millisecond, ts.Timezone),
        Time32Type { Unit: TimeUnit.Second } => new Time32Type(TimeUnit.Millisecond),
        _ => null,
    };

    /// <summary>
    /// Applies <paramref name="leaf"/> to every type in the tree, rebuilding only the containers
    /// whose contents actually changed. Returns the same instance when nothing did.
    /// </summary>
    /// <param name="type">The type to rewrite.</param>
    /// <param name="leaf">Returns a replacement type, or null to leave this one alone.</param>
    internal static IArrowType RewriteType(IArrowType type, Func<IArrowType, IArrowType?> leaf)
    {
        var replacement = leaf(type);
        if (replacement is not null)
            return replacement;

        switch (type)
        {
            case StructType structType:
            {
                var fields = RewriteFields(structType.Fields, leaf);
                return fields is null ? type : new StructType(fields);
            }

            case MapType mapType:
            {
                var key = RewriteField(mapType.KeyField, leaf);
                var value = RewriteField(mapType.ValueField, leaf);
                if (key is null && value is null)
                    return type;
                return new MapType(
                    key ?? mapType.KeyField, value ?? mapType.ValueField, mapType.KeySorted);
            }

            // MapType derives from ListType, so it has to be matched first.
            case ListType listType:
            {
                var value = RewriteField(listType.ValueField, leaf);
                return value is null ? type : new ListType(value);
            }

            case FixedSizeListType fixedType:
            {
                var value = RewriteField(fixedType.ValueField, leaf);
                return value is null ? type : new FixedSizeListType(value, fixedType.ListSize);
            }

            default:
                return type;
        }
    }

    /// <summary>
    /// The schema Parquet can express for <paramref name="schema"/>. Needed for a file that never
    /// receives a row group, whose declared schema reaches the footer without passing any batch.
    /// </summary>
    /// <param name="schema">The declared Arrow schema.</param>
    internal static Apache.Arrow.Schema ToParquetUnits(Apache.Arrow.Schema schema)
    {
        Field[]? fields = null;
        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            var field = schema.FieldsList[i];
            var type = ToParquetUnits(field.DataType);
            if (!ReferenceEquals(type, field.DataType))
            {
                fields ??= [.. schema.FieldsList];
                fields[i] = new Field(field.Name, type, field.IsNullable, field.Metadata);
            }
        }

        return fields is null ? schema : new Apache.Arrow.Schema(fields, schema.Metadata);
    }

    /// <summary>
    /// Rescales <paramref name="array"/> into the units Parquet can express. Returns the same
    /// instance when nothing needed rescaling.
    /// </summary>
    /// <param name="array">The array to rescale.</param>
    internal static IArrowArray ToParquetUnits(IArrowArray array)
    {
        switch (array.Data.DataType)
        {
            case TimestampType { Unit: TimeUnit.Second } ts:
                return new TimestampArray(Scaled(
                    array, new TimestampType(TimeUnit.Millisecond, ts.Timezone)));

            case Time32Type { Unit: TimeUnit.Second }:
                return new Time32Array(Scaled32(
                    array, new Time32Type(TimeUnit.Millisecond)));

            default:
                return Containers(array, target: null, (child, _) => ToParquetUnits(child));
        }
    }

    /// <summary>
    /// The type <see cref="ToDeclaredUnits(IArrowArray, IArrowType)"/> would produce, without the
    /// array. Used for a file with no row groups, whose schema is all there is to restore.
    /// </summary>
    /// <param name="derived">The type derived from the Parquet schema.</param>
    /// <param name="declared">The originally declared Arrow type.</param>
    internal static IArrowType ToDeclaredUnits(IArrowType derived, IArrowType declared)
    {
        switch (derived)
        {
            case TimestampType actual when declared is TimestampType target:
                return target.Timezone == actual.Timezone
                    ? derived
                    : new TimestampType(actual.Unit, target.Timezone);

            case StructType actual when declared is StructType target
                && target.Fields.Count == actual.Fields.Count:
            {
                Field[]? fields = null;
                for (int i = 0; i < actual.Fields.Count; i++)
                {
                    var type = ToDeclaredUnits(actual.Fields[i].DataType, target.Fields[i].DataType);
                    if (!ReferenceEquals(type, actual.Fields[i].DataType))
                    {
                        fields ??= [.. actual.Fields];
                        fields[i] = new Field(
                            actual.Fields[i].Name, type, actual.Fields[i].IsNullable,
                            actual.Fields[i].Metadata);
                    }
                }

                return fields is null ? derived : new StructType(fields);
            }

            // MapType derives from ListType, so it has to be matched first or a map would be
            // rebuilt as a plain list.
            case MapType actual when declared is MapType target:
            {
                var key = ToDeclaredUnits(actual.KeyField.DataType, target.KeyField.DataType);
                var value = ToDeclaredUnits(actual.ValueField.DataType, target.ValueField.DataType);
                if (ReferenceEquals(key, actual.KeyField.DataType)
                    && ReferenceEquals(value, actual.ValueField.DataType))
                {
                    return derived;
                }

                return new MapType(
                    new Field(
                        actual.KeyField.Name, key, actual.KeyField.IsNullable,
                        actual.KeyField.Metadata),
                    new Field(
                        actual.ValueField.Name, value, actual.ValueField.IsNullable,
                        actual.ValueField.Metadata),
                    actual.KeySorted);
            }

            // A fixed-size list is written as an ordinary LIST, so the declared side is the only
            // place the fixed shape appears. Match it here or a zone nested inside one is not
            // restored. The width itself is deliberately not taken back — see ArrowSchemaMetadata.
            case ListType actual when declared is ListType or FixedSizeListType:
            {
                var declaredValue = declared is FixedSizeListType fixedTarget
                    ? fixedTarget.ValueField.DataType
                    : ((ListType)declared).ValueField.DataType;
                var value = ToDeclaredUnits(actual.ValueField.DataType, declaredValue);
                return ReferenceEquals(value, actual.ValueField.DataType)
                    ? derived
                    : new ListType(new Field(
                        actual.ValueField.Name, value, actual.ValueField.IsNullable,
                        actual.ValueField.Metadata));
            }

            default:
                return derived;
        }
    }

    /// <summary>
    /// Restores what <paramref name="declared"/> — the schema the writer recorded in
    /// <c>ARROW:schema</c> — carries that Parquet could not: timestamp zone names. Returns the same
    /// instance when there is nothing to restore.
    /// </summary>
    /// <param name="array">The assembled array, typed from the Parquet schema.</param>
    /// <param name="declared">The originally declared Arrow type.</param>
    internal static IArrowArray ToDeclaredUnits(IArrowArray array, IArrowType declared)
    {
        switch (declared)
        {
            // The ZONE is restored and the unit deliberately is not. Restoring the zone costs
            // nothing — Parquet stores an instant, and the Arrow zone is presentation — whereas the
            // unit was physically rescaled on the way in. PyArrow, which defines this convention,
            // reads its OWN second-precision column back as milliseconds; measured, not assumed.
            // Handing back seconds here would make EngineeredWood the outlier in the other
            // direction, and would need a cast Parquet gives no warrant for.
            case TimestampType target when array.Data.DataType is TimestampType actual
                && target.Timezone != actual.Timezone:
                return new TimestampArray(
                    Retyped(array, new TimestampType(actual.Unit, target.Timezone)));

            default:
                return Containers(
                    array,
                    declared,
                    (child, childTarget) =>
                        childTarget is null ? child : ToDeclaredUnits(child, childTarget));
        }
    }

    private static IArrowArray Containers(
        IArrowArray array, IArrowType? target, Func<IArrowArray, IArrowType?, IArrowArray> rewrite)
    {
        switch (array)
        {
            case StructArray sa when Structural(target, sa.Fields.Count) is StructType or null:
            {
                var declared = target as StructType;
                IArrowArray[]? children = null;
                for (int i = 0; i < sa.Fields.Count; i++)
                {
                    var original = sa.Fields[i];
                    var rewritten = rewrite(original, declared?.Fields[i].DataType);
                    if (!ReferenceEquals(rewritten, original))
                    {
                        children ??= [.. sa.Fields];
                        children[i] = rewritten;
                    }
                }

                if (children is null)
                    return array;

                var fields = new Field[children.Length];
                var source = (StructType)sa.Data.DataType;
                for (int i = 0; i < fields.Length; i++)
                {
                    fields[i] = new Field(
                        source.Fields[i].Name, children[i].Data.DataType,
                        source.Fields[i].IsNullable, source.Fields[i].Metadata);
                }

                return new StructArray(new ArrayData(
                    new StructType(fields), sa.Length, sa.NullCount, sa.Offset,
                    sa.Data.Buffers, [.. children.Select(child => child.Data)]));
            }

            // MapArray derives from ListArray, so it has to be excluded here or a map would be
            // rebuilt as a plain list of its entries struct and lose its type.
            case ListArray la when array is not MapArray:
            {
                // A fixed-size list was written as an ordinary LIST, so the declared side may still
                // describe it as fixed. Take its element either way.
                var declared = target switch
                {
                    MapType => null,
                    FixedSizeListType fixedTarget => fixedTarget.ValueField.DataType,
                    ListType listTarget => listTarget.ValueField.DataType,
                    _ => null,
                };
                var values = rewrite(la.Values, declared);
                if (ReferenceEquals(values, la.Values))
                    return array;
                var source = (ListType)la.Data.DataType;
                var valueField = new Field(
                    source.ValueField.Name, values.Data.DataType, source.ValueField.IsNullable,
                    source.ValueField.Metadata);
                return new ListArray(new ArrayData(
                    new ListType(valueField), la.Length, la.NullCount, la.Offset,
                    la.Data.Buffers, [values.Data]));
            }

            case FixedSizeListArray fa:
            {
                var declared = (target as FixedSizeListType)?.ValueField.DataType;
                var values = rewrite(fa.Values, declared);
                if (ReferenceEquals(values, fa.Values))
                    return array;
                var source = (FixedSizeListType)fa.Data.DataType;
                var valueField = new Field(
                    source.ValueField.Name, values.Data.DataType, source.ValueField.IsNullable,
                    source.ValueField.Metadata);
                return new FixedSizeListArray(new ArrayData(
                    new FixedSizeListType(valueField, source.ListSize), fa.Length, fa.NullCount,
                    fa.Offset, fa.Data.Buffers, [values.Data]));
            }

            case MapArray ma:
            {
                var declared = target as MapType;
                var entries = ma.KeyValues;
                var key = rewrite(entries.Fields[0], declared?.KeyField.DataType);
                var value = rewrite(entries.Fields[1], declared?.ValueField.DataType);
                if (ReferenceEquals(key, entries.Fields[0]) && ReferenceEquals(value, entries.Fields[1]))
                    return array;

                var source = (MapType)ma.Data.DataType;
                var keyField = new Field(
                    source.KeyField.Name, key.Data.DataType, source.KeyField.IsNullable);
                var valueField = new Field(
                    source.ValueField.Name, value.Data.DataType, source.ValueField.IsNullable);
                var entriesData = new ArrayData(
                    new StructType([keyField, valueField]), entries.Length, entries.NullCount,
                    entries.Offset, entries.Data.Buffers, [key.Data, value.Data]);
                return new MapArray(new ArrayData(
                    new MapType(keyField, valueField, source.KeySorted), ma.Length, ma.NullCount,
                    ma.Offset, ma.Data.Buffers, [entriesData]));
            }

            default:
                return array;
        }
    }

    /// <summary>
    /// A declared type only guides the rewrite when it still describes the same shape. Metadata that
    /// disagrees with the assembled array is ignored rather than trusted into a type mismatch.
    /// </summary>
    private static IArrowType? Structural(IArrowType? target, int childCount) =>
        target is StructType structType && structType.Fields.Count == childCount ? structType : null;

    private static ArrayData Retyped(IArrowArray array, IArrowType type) =>
        new(type, array.Length, array.Data.NullCount, array.Data.Offset,
            array.Data.Buffers, array.Data.Children);

    private static ArrayData Scaled(IArrowArray array, IArrowType type)
    {
        var source = array.Data.Buffers[1].Span.CastTo<long>();
        int limit = array.Data.Offset + array.Length;
        var values = new long[Math.Max(source.Length, limit)];
        source.CopyTo(values);

        var typed = (Apache.Arrow.Array)array;
        for (int i = array.Data.Offset; i < limit; i++)
        {
            if (typed.IsNull(i - array.Data.Offset))
                continue;
            values[i] = Multiply(values[i]);
        }

        return new ArrayData(
            type, array.Length, array.Data.NullCount, array.Data.Offset,
            [array.Data.Buffers[0], new ArrowBuffer.Builder<long>(values.Length).Append(values).Build()]);
    }

    private static ArrayData Scaled32(IArrowArray array, IArrowType type)
    {
        var source = array.Data.Buffers[1].Span.CastTo<int>();
        int limit = array.Data.Offset + array.Length;
        var values = new int[Math.Max(source.Length, limit)];
        source.CopyTo(values);

        var typed = (Apache.Arrow.Array)array;
        for (int i = array.Data.Offset; i < limit; i++)
        {
            if (typed.IsNull(i - array.Data.Offset))
                continue;
            values[i] = checked((int)Multiply(values[i]));
        }

        return new ArrayData(
            type, array.Length, array.Data.NullCount, array.Data.Offset,
            [array.Data.Buffers[0], new ArrowBuffer.Builder<int>(values.Length).Append(values).Build()]);
    }

    private static long Multiply(long seconds)
    {
        try
        {
            return checked(seconds * MillisPerSecond);
        }
        catch (OverflowException error)
        {
            throw new NotSupportedException(
                $"Second-precision value {seconds} does not fit a millisecond column, which is the "
                + "finest unit Parquet has for it. Rescale or clamp the column before writing.",
                error);
        }
    }

    private static Field[]? RewriteFields(
        IReadOnlyList<Field> fields, Func<IArrowType, IArrowType?> leaf)
    {
        Field[]? rewritten = null;
        for (int i = 0; i < fields.Count; i++)
        {
            var field = RewriteField(fields[i], leaf);
            if (field is not null)
            {
                rewritten ??= [.. fields];
                rewritten[i] = field;
            }
        }

        return rewritten;
    }

    private static Field? RewriteField(Field field, Func<IArrowType, IArrowType?> leaf)
    {
        var type = RewriteType(field.DataType, leaf);
        return ReferenceEquals(type, field.DataType)
            ? null
            : new Field(field.Name, type, field.IsNullable);
    }
}
