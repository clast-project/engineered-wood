// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.Schema;
using MapType = Apache.Arrow.Types.MapType;
using StructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.TypeWidening;

/// <summary>
/// Widens Arrow array values from a narrower type to the current schema type.
/// Used when reading data files written before a type widening change.
/// </summary>
internal static class ValueWidener
{
    /// <summary>
    /// Widens a RecordBatch's values to the target schema's types, matching columns BY NAME. Columns whose
    /// types differ are converted; every other column passes through untouched, keeping its own field.
    ///
    /// <para>This only WIDENS. Reconciling the batch's column SET to the current schema — backfilling a column
    /// ADDed after the file was written, dropping one DROPped since — belongs to
    /// <see cref="SchemaEvolution.BackfillMissingColumns"/>, which every caller runs afterwards.</para>
    /// </summary>
    public static RecordBatch WidenBatch(
        RecordBatch batch, Apache.Arrow.Schema targetSchema)
    {
        // BY NAME, never by position: a schema-EVOLVED file's column set differs from the current schema (a
        // column ADDed after the file was written is absent from it, a DROPped one is still present), so
        // pairing the two positionally lines one column's data up against another column's type — which
        // either throws deep in an encoder or, when the types happen to be compatible, silently writes the
        // values under the wrong name.
        var targetByName = new Dictionary<string, IArrowType>(StringComparer.Ordinal);
        foreach (var f in targetSchema.FieldsList)
            targetByName[f.Name] = f.DataType;

        bool needsWidening = false;
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (targetByName.TryGetValue(batch.Schema.FieldsList[i].Name, out var t)
                && !TypesMatch(batch.Schema.FieldsList[i].DataType, t))
            {
                needsWidening = true;
                break;
            }
        }

        if (!needsWidening)
            return batch;

        var fields = new List<Field>(batch.ColumnCount);
        var columns = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var f = batch.Schema.FieldsList[i];
            var source = batch.Column(i);

            if (targetByName.TryGetValue(f.Name, out var targetType)
                && !TypesMatch(f.DataType, targetType))
            {
                var widened = WidenArray(source, targetType);
                columns[i] = widened;
                // Label the field with the type the array now HAS, never the target's: WidenArray returns the
                // source unchanged for a pair it does not support, and a nested array keeps its own child set
                // (reconciling that is BackfillMissingColumns' job), so the target type can describe it wrongly.
                // Relabeling data with a type it does not have is the same lie the positional pairing told.
                fields.Add(ReferenceEquals(widened, source)
                    ? f
                    : new Field(f.Name, widened.Data.DataType, f.IsNullable, f.Metadata));
            }
            else
            {
                columns[i] = source;
                fields.Add(f);
            }
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, batch.Length);
    }

    /// <summary>
    /// Widens an individual array to the target type.
    /// </summary>
    public static IArrowArray WidenArray(IArrowArray source, IArrowType targetType)
    {
        // The arms below are the POLICY — which widenings Delta permits — while ArrowCompute.Widen is the
        // mechanism. They stay enumerated pair by pair rather than collapsed into coarser patterns, because a
        // coarser pattern would also match narrowings (Int32Array to Int16Type) and claim they were legal.
        return (source, targetType) switch
        {
            // Integer widening: byte → short → int → long
            (Int8Array, Int16Type) => ArrowCompute.Widen(source, targetType),
            (Int8Array, Int32Type) => ArrowCompute.Widen(source, targetType),
            (Int8Array, Int64Type) => ArrowCompute.Widen(source, targetType),
            (Int16Array, Int32Type) => ArrowCompute.Widen(source, targetType),
            (Int16Array, Int64Type) => ArrowCompute.Widen(source, targetType),
            (Int32Array, Int64Type) => ArrowCompute.Widen(source, targetType),

            // Float → Double
            (FloatArray, DoubleType) => ArrowCompute.Widen(source, targetType),

            // Integer → Double
            (Int8Array, DoubleType) => ArrowCompute.Widen(source, targetType),
            (Int16Array, DoubleType) => ArrowCompute.Widen(source, targetType),
            (Int32Array, DoubleType) => ArrowCompute.Widen(source, targetType),

            // Date → Timestamp_ntz
            (Date32Array, TimestampType ts) when ts.Timezone is null =>
                ArrowCompute.Widen(source, targetType),

            // Decimal widening (precision and/or scale change)
            (Decimal128Array a, Decimal128Type dt) => WidenDecimal128(a, dt),

            // Integer → Decimal widening
            (Int8Array, Decimal128Type dt) => WidenIntegerToDecimal<sbyte>(source, dt),
            (Int16Array, Decimal128Type dt) => WidenIntegerToDecimal<short>(source, dt),
            (Int32Array, Decimal128Type dt) => WidenIntegerToDecimal<int>(source, dt),
            (Int64Array, Decimal128Type dt) => WidenIntegerToDecimal<long>(source, dt),

            // A field widened INSIDE a struct, list element or map key/value (PROTOCOL.md records those on the
            // struct child, or with fieldPath element/key/value). Without these arms an old file's nested
            // values came back narrow under a wide schema, and compaction wrote them into the wide column.
            (StructArray or ListArray or LargeListArray, NestedType) => WidenNested(source, targetType),

            _ => source, // No widening needed or unsupported
        };
    }

    #region Nested Widening

    private static IArrowArray WidenNested(IArrowArray source, IArrowType targetType)
    {
        var widened = WidenData(source.Data, targetType);
        return ReferenceEquals(widened, source.Data) ? source : ArrowArrayFactory.BuildArray(widened);
    }

    // Widens every leaf under `data` whose type the target puts at the same place, returning `data` itself when
    // nothing changes. Struct children are matched BY NAME, as the top level is: a file's struct may lack a child
    // added since, or still carry one dropped, and that set is reconciled afterwards. Only the CHILDREN are
    // rebuilt; each container keeps its own length, offset, validity and offsets buffers. A widened child comes
    // back with offset 0 and the same length as before, so element i is unchanged and the parent's indexing into
    // it still holds.
    private static ArrayData WidenData(ArrayData data, IArrowType target)
    {
        switch (data.DataType, target)
        {
            // MapType derives from ListType, so it has to be matched first.
            case (MapType sm, MapType tm):
            {
                var entries = data.Children[0];
                var key = WidenData(entries.Children[0], tm.KeyField.DataType);
                var value = WidenData(entries.Children[1], tm.ValueField.DataType);
                if (ReferenceEquals(key, entries.Children[0]) && ReferenceEquals(value, entries.Children[1]))
                    return data;
                var mapType = new MapType(
                    WithType(sm.KeyField, key.DataType), WithType(sm.ValueField, value.DataType), sm.KeySorted);
                var newEntries = new ArrayData(mapType.KeyValueType, entries.Length, entries.NullCount,
                    entries.Offset, entries.Buffers, [key, value]);
                return new ArrayData(mapType, data.Length, data.NullCount, data.Offset, data.Buffers, [newEntries]);
            }
            case (MapType, _):
            case (_, MapType):
                return data;
            case (ListType sl, ListType tl):
            {
                var values = WidenData(data.Children[0], tl.ValueDataType);
                return ReferenceEquals(values, data.Children[0])
                    ? data
                    : new ArrayData(new ListType(WithType(sl.ValueField, values.DataType)), data.Length,
                        data.NullCount, data.Offset, data.Buffers, [values]);
            }
            case (LargeListType sl, LargeListType tl):
            {
                var values = WidenData(data.Children[0], tl.ValueDataType);
                return ReferenceEquals(values, data.Children[0])
                    ? data
                    : new ArrayData(new LargeListType(WithType(sl.ValueField, values.DataType)), data.Length,
                        data.NullCount, data.Offset, data.Buffers, [values]);
            }
            case (StructType ss, StructType ts):
            {
                ArrayData[]? children = null;
                for (int i = 0; i < ss.Fields.Count; i++)
                {
                    int t = ts.GetFieldIndex(ss.Fields[i].Name, StringComparer.Ordinal);
                    if (t < 0)
                        continue;
                    var child = WidenData(data.Children[i], ts.Fields[t].DataType);
                    if (ReferenceEquals(child, data.Children[i]))
                        continue;
                    children ??= (ArrayData[])data.Children.Clone();
                    children[i] = child;
                }
                if (children is null)
                    return data;
                var fields = new List<Field>(ss.Fields.Count);
                for (int i = 0; i < ss.Fields.Count; i++)
                    fields.Add(WithType(ss.Fields[i], children[i].DataType));
                return new ArrayData(new StructType(fields), data.Length, data.NullCount, data.Offset,
                    data.Buffers, children);
            }
            case (NestedType, _):
            case (_, NestedType):
                return data;
            default:
            {
                if (TypesMatch(data.DataType, target))
                    return data;
                var leaf = ArrowArrayFactory.BuildArray(data);
                var widened = WidenArray(leaf, target);
                return ReferenceEquals(widened, leaf) ? data : widened.Data;
            }
        }
    }

    private static Field WithType(Field field, IArrowType type) =>
        ReferenceEquals(field.DataType, type)
            ? field
            : new Field(field.Name, type, field.IsNullable, field.Metadata);

    #endregion

    #region Decimal Widening

    private const int Decimal128Width = 16;

    private static Decimal128Array WidenDecimal128(Decimal128Array source, Decimal128Type targetType)
    {
        var sourceType = (Decimal128Type)source.Data.DataType;

        if (sourceType.Precision == targetType.Precision &&
            sourceType.Scale == targetType.Scale)
            return source;

        int scaleDelta = targetType.Scale - sourceType.Scale;
        if (scaleDelta == 0)
        {
            // Only precision changed — binary representation is the same,
            // just re-tag with the new type
            return RetagDecimal128(source, targetType);
        }

        if (scaleDelta < 0)
        {
            throw new NotSupportedException(
                $"Cannot widen {sourceType} to {targetType}: lowering the scale is not a widening.");
        }

        // Scale increased: multiply each value by 10^scaleDelta, in Int128 rather than BigInteger — a
        // BigInteger per row (plus ToByteArray's allocation on netstandard2.0) cost an order of magnitude more.
        //
        // Unchecked is safe for a legal widening: a value of precision p is below 10^p, so the product is
        // below 10^(p + scaleDelta) <= 10^p' <= 10^38 < 2^127. The slots under nulls hold arbitrary bytes and
        // may well wrap, which is harmless (validity masks them) — and is why the multiply can't be checked.
        var multiplier = PowerOfTen(scaleDelta);

        int length = source.Length;
        ReadOnlySpan<byte> src = source.Data.Buffers[1].Span
            .Slice(source.Data.Offset * Decimal128Width, length * Decimal128Width);

        // Every slot is written below, nulls included, so the buffer needs no zero-fill first.
        var values = ArrowCompute.Uninitialized<byte>(length * Decimal128Width);
        Span<byte> dst = values.AsSpan();

        for (int i = 0, o = 0; i < length; i++, o += Decimal128Width)
            WriteInt128LE(dst.Slice(o, Decimal128Width), ReadInt128LE(src.Slice(o, Decimal128Width)) * multiplier);

        return BuildDecimal128(source, targetType, values);
    }

    /// <summary>
    /// Re-tags a Decimal128Array with a new type (precision change only, same binary data).
    /// </summary>
    private static Decimal128Array RetagDecimal128(Decimal128Array source, Decimal128Type newType)
    {
        var oldData = source.Data;
        var newData = new ArrayData(newType, oldData.Length,
            oldData.NullCount, oldData.Offset, oldData.Buffers, oldData.Children);
        return new Decimal128Array(newData);
    }

    /// <summary>
    /// Widens an integer array to Decimal128: each value times 10^scale. As with the decimal rescale, a legal
    /// target has room for the source's whole range at that scale, so the multiply cannot overflow on a valid
    /// slot, and every slot (nulls included) is written.
    /// </summary>
    private static Decimal128Array WidenIntegerToDecimal<T>(IArrowArray source, Decimal128Type targetType)
        where T : struct
    {
        var multiplier = PowerOfTen(targetType.Scale);

        int length = source.Length;
        ReadOnlySpan<T> src = MemoryMarshal
            .Cast<byte, T>(source.Data.Buffers[1].Span)
            .Slice(source.Data.Offset, length);

        var values = ArrowCompute.Uninitialized<byte>(length * Decimal128Width);
        Span<byte> dst = values.AsSpan();

        for (int i = 0, o = 0; i < length; i++, o += Decimal128Width)
            WriteInt128LE(dst.Slice(o, Decimal128Width), (Int128)ToInt64(src[i]) * multiplier);

        return BuildDecimal128(source, targetType, values);
    }

    // The typeof tests are JIT-time constants for a value-type T, so each instantiation keeps only its own arm.
    private static long ToInt64<T>(T value) where T : struct
    {
        if (typeof(T) == typeof(sbyte)) return Unsafe.As<T, sbyte>(ref value);
        if (typeof(T) == typeof(short)) return Unsafe.As<T, short>(ref value);
        if (typeof(T) == typeof(int)) return Unsafe.As<T, int>(ref value);
        if (typeof(T) == typeof(long)) return Unsafe.As<T, long>(ref value);
        throw new NotSupportedException($"Cannot widen {typeof(T)} to a decimal.");
    }

    private static Decimal128Array BuildDecimal128(IArrowArray source, Decimal128Type targetType, byte[] values)
    {
        // Widening never changes which rows are null, so the source bitmap is shared when it can be.
        var (validity, nullCount) = ArrowCompute.AlignedValidity(source);
        return new Decimal128Array(new ArrayData(
            targetType, source.Length, nullCount, offset: 0,
            [validity, new ArrowBuffer(values)]));
    }

    private static Int128 PowerOfTen(int exponent)
    {
        Int128 result = Int128.One;
        for (int i = 0; i < exponent; i++)
            result *= 10;
        return result;
    }

    // Decimal128 values are 16-byte little-endian two's complement: the low 64 bits first. Composed from two
    // 64-bit halves rather than reinterpreting the bytes, since the netstandard2.0 polyfill's field layout
    // is not part of its contract.
    private static Int128 ReadInt128LE(ReadOnlySpan<byte> bytes) =>
        new(BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes));

    private static void WriteInt128LE(Span<byte> dest, Int128 value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(dest, (ulong)value);
        BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), (ulong)(value >> 64));
    }

    #endregion

    #region Helpers

    private static bool TypesMatch(IArrowType a, IArrowType b)
    {
        if (a.TypeId != b.TypeId)
            return false;

        return (a, b) switch
        {
            // Every extension type reports TypeId.Extension, so the `_ => true` fallback below would
            // call two DIFFERENT extensions equal. Compare by extension name and storage instead.
            (ExtensionType ea, ExtensionType eb) =>
                string.Equals(ea.Name, eb.Name, StringComparison.Ordinal)
                && TypesMatch(ea.StorageType, eb.StorageType),
            (Decimal128Type da, Decimal128Type db) =>
                da.Precision == db.Precision && da.Scale == db.Scale,
            (TimestampType ta, TimestampType tb) =>
                ta.Unit == tb.Unit && ta.Timezone == tb.Timezone,
            // Containers match when everything they share matches; a struct child present on one side only is
            // a column-set difference, which is BackfillMissingColumns' to reconcile, not a widening.
            (MapType ma, MapType mb) =>
                TypesMatch(ma.KeyField.DataType, mb.KeyField.DataType)
                && TypesMatch(ma.ValueField.DataType, mb.ValueField.DataType),
            (ListType la, ListType lb) => TypesMatch(la.ValueDataType, lb.ValueDataType),
            (LargeListType la, LargeListType lb) => TypesMatch(la.ValueDataType, lb.ValueDataType),
            (StructType sa, StructType sb) => StructChildrenMatch(sa, sb),
            _ => true,
        };
    }

    private static bool StructChildrenMatch(StructType a, StructType b)
    {
        foreach (var child in a.Fields)
        {
            int i = b.GetFieldIndex(child.Name, StringComparer.Ordinal);
            if (i >= 0 && !TypesMatch(child.DataType, b.Fields[i].DataType))
                return false;
        }
        return true;
    }

    #endregion
}
