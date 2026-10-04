// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
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
            (Int8Array a, Decimal128Type dt) => WidenIntToDecimal(a, dt, v => (long)v),
            (Int16Array a, Decimal128Type dt) => WidenIntToDecimal(a, dt, v => (long)v),
            (Int32Array a, Decimal128Type dt) => WidenIntToDecimal(a, dt, v => (long)v),
            (Int64Array a, Decimal128Type dt) => WidenLongToDecimal(a, dt),

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

        // Scale increased: multiply each value by 10^scaleDelta
        // Decimal128 values are stored as 16-byte little-endian two's complement
        var multiplier = BigInteger.Pow(10, scaleDelta);

        int byteWidth = 16;
        var resultBytes = new byte[source.Length * byteWidth];
        var nullBitmap = new ArrowBuffer.BitmapBuilder();

        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i))
            {
                nullBitmap.Append(false);
                // Leave bytes as zero
            }
            else
            {
                nullBitmap.Append(true);
                var valueBytes = source.GetBytes(i);
                var value = ReadInt128LE(valueBytes);
                var scaled = value * multiplier;
                WriteInt128LE(scaled, resultBytes.AsSpan(i * byteWidth, byteWidth));
            }
        }

        var valueBuffer = new ArrowBuffer(resultBytes);
        var nullBuffer = nullBitmap.Build();

        var data = new ArrayData(targetType, source.Length,
            source.NullCount, 0,
            [nullBuffer, valueBuffer]);

        return new Decimal128Array(data);
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
    /// Widens an integer array to Decimal128.
    /// </summary>
    private static Decimal128Array WidenIntToDecimal<T>(
        PrimitiveArray<T> source, Decimal128Type targetType, Func<T, long> toLong)
        where T : struct, IEquatable<T>
    {
        var scaleFactor = BigInteger.Pow(10, targetType.Scale);
        int byteWidth = 16;
        var resultBytes = new byte[source.Length * byteWidth];
        var nullBitmap = new ArrowBuffer.BitmapBuilder();

        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i))
            {
                nullBitmap.Append(false);
            }
            else
            {
                nullBitmap.Append(true);
                long intVal = toLong(source.GetValue(i)!.Value);
                var scaled = new BigInteger(intVal) * scaleFactor;
                WriteInt128LE(scaled, resultBytes.AsSpan(i * byteWidth, byteWidth));
            }
        }

        var data = new ArrayData(targetType, source.Length,
            source.NullCount, 0,
            [nullBitmap.Build(), new ArrowBuffer(resultBytes)]);
        return new Decimal128Array(data);
    }

    private static Decimal128Array WidenLongToDecimal(Int64Array source, Decimal128Type targetType)
    {
        var scaleFactor = BigInteger.Pow(10, targetType.Scale);
        int byteWidth = 16;
        var resultBytes = new byte[source.Length * byteWidth];
        var nullBitmap = new ArrowBuffer.BitmapBuilder();

        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i))
            {
                nullBitmap.Append(false);
            }
            else
            {
                nullBitmap.Append(true);
                var scaled = new BigInteger(source.GetValue(i)!.Value) * scaleFactor;
                WriteInt128LE(scaled, resultBytes.AsSpan(i * byteWidth, byteWidth));
            }
        }

        var data = new ArrayData(targetType, source.Length,
            source.NullCount, 0,
            [nullBitmap.Build(), new ArrowBuffer(resultBytes)]);
        return new Decimal128Array(data);
    }

    /// <summary>
    /// Reads a 128-bit signed integer from a 16-byte little-endian buffer.
    /// </summary>
    private static BigInteger ReadInt128LE(ReadOnlySpan<byte> bytes)
    {
        // BigInteger constructor expects little-endian, unsigned=false (signed)
#if NET6_0_OR_GREATER
        return new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#else
        var arr = bytes.ToArray();
        return new BigInteger(arr);
#endif
    }

    /// <summary>
    /// Writes a BigInteger as a 128-bit little-endian two's complement value.
    /// </summary>
    private static void WriteInt128LE(BigInteger value, Span<byte> dest)
    {
        // Fill with sign extension byte first
        byte fill = value < 0 ? (byte)0xFF : (byte)0x00;
        dest.Fill(fill);

#if NET6_0_OR_GREATER
        value.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
        var bytes = value.ToByteArray(); // Little-endian, signed, minimal representation
        bytes.AsSpan(0, Math.Min(bytes.Length, dest.Length)).CopyTo(dest);
#endif
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
