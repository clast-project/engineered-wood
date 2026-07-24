// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table.TypeWidening;

/// <summary>
/// Widens Arrow array values from a narrower type to the current schema type.
/// Used when reading data files written before a type widening change.
/// </summary>
internal static class ValueWidener
{
    /// <summary>
    /// Widens a RecordBatch to match the target schema.
    /// Columns whose types differ are converted; matching columns are passed through.
    /// </summary>
    public static RecordBatch WidenBatch(
        RecordBatch batch, Apache.Arrow.Schema targetSchema)
    {
        // Match target fields BY NAME, never by position: a schema-EVOLVED file's column set differs from
        // the current schema (a column ADDed after the file was written is absent; a DROPped one is still
        // present), so positional pairing relabels one column's data as another's — silent corruption.
        // Columns without a same-named target field pass through unchanged (schema-evolution reconcile
        // downstream adds/drops columns); this method only WIDENS values.
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
            if (targetByName.TryGetValue(f.Name, out var targetType)
                && !TypesMatch(f.DataType, targetType))
            {
                var widened = WidenArray(batch.Column(i), targetType);
                columns[i] = widened;
                // Relabel with the target type ONLY when the array was actually converted: WidenArray
                // passes an unsupported pair through unchanged (e.g. a host-transport variant blob vs
                // the canonical extension type), and labeling untouched data with the target type lies
                // about its representation — a downstream schema export then mis-describes the column.
                fields.Add(ReferenceEquals(widened, batch.Column(i))
                    ? f
                    : new Field(f.Name, targetType, f.IsNullable, f.Metadata));
            }
            else
            {
                columns[i] = batch.Column(i);
                fields.Add(f);
            }
        }

        return new RecordBatch(new Apache.Arrow.Schema(fields, batch.Schema.Metadata), columns, batch.Length);
    }

    /// <summary>
    /// Widens an individual array to the target type.
    /// </summary>
    public static IArrowArray WidenArray(IArrowArray source, IArrowType targetType)
    {
        return (source, targetType) switch
        {
            // Integer widening: byte → short → int → long
            (Int8Array a, Int16Type) => WidenInt8ToInt16(a),
            (Int8Array a, Int32Type) => WidenInt8ToInt32(a),
            (Int8Array a, Int64Type) => WidenInt8ToInt64(a),
            (Int16Array a, Int32Type) => WidenInt16ToInt32(a),
            (Int16Array a, Int64Type) => WidenInt16ToInt64(a),
            (Int32Array a, Int64Type) => WidenInt32ToInt64(a),

            // Float → Double
            (FloatArray a, DoubleType) => WidenFloatToDouble(a),

            // Integer → Double
            (Int8Array a, DoubleType) => WidenInt8ToDouble(a),
            (Int16Array a, DoubleType) => WidenInt16ToDouble(a),
            (Int32Array a, DoubleType) => WidenInt32ToDouble(a),

            // Date → Timestamp_ntz
            (Date32Array a, TimestampType ts) when ts.Timezone is null =>
                WidenDate32ToTimestamp(a, ts),

            // Decimal widening (precision and/or scale change)
            (Decimal128Array a, Decimal128Type dt) => WidenDecimal128(a, dt),

            // Integer → Decimal widening
            (Int8Array a, Decimal128Type dt) => WidenIntToDecimal(a, dt, v => (long)v),
            (Int16Array a, Decimal128Type dt) => WidenIntToDecimal(a, dt, v => (long)v),
            (Int32Array a, Decimal128Type dt) => WidenIntToDecimal(a, dt, v => (long)v),
            (Int64Array a, Decimal128Type dt) => WidenLongToDecimal(a, dt),

            _ => source, // No widening needed or unsupported
        };
    }

    #region Integer Widening

    private static Int16Array WidenInt8ToInt16(Int8Array source)
    {
        var b = new Int16Array.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static Int32Array WidenInt8ToInt32(Int8Array source)
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static Int64Array WidenInt8ToInt64(Int8Array source)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static Int32Array WidenInt16ToInt32(Int16Array source)
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static Int64Array WidenInt16ToInt64(Int16Array source)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static Int64Array WidenInt32ToInt64(Int32Array source)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    #endregion

    #region Float Widening

    private static DoubleArray WidenFloatToDouble(FloatArray source)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static DoubleArray WidenInt8ToDouble(Int8Array source)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static DoubleArray WidenInt16ToDouble(Int16Array source)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    private static DoubleArray WidenInt32ToDouble(Int32Array source)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i)) b.AppendNull();
            else b.Append(source.GetValue(i)!.Value);
        }
        return b.Build();
    }

    #endregion

    #region Date → Timestamp Widening

    private static TimestampArray WidenDate32ToTimestamp(Date32Array source, TimestampType tsType)
    {
        var b = new TimestampArray.Builder(tsType);
        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i))
                b.AppendNull();
            else
            {
                int days = source.GetValue(i)!.Value;
                b.Append(new DateTimeOffset(epoch.AddDays(days), TimeSpan.Zero));
            }
        }
        return b.Build();
    }

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
            _ => true,
        };
    }

    private static IArrowArray BuildNullArray(IArrowType type, int length)
    {
        return type switch
        {
            Int64Type => BuildNullInt64(length),
            Int32Type => BuildNullInt32(length),
            Int16Type => BuildNullInt16(length),
            Int8Type => BuildNullInt8(length),
            DoubleType => BuildNullDouble(length),
            FloatType => BuildNullFloat(length),
            BooleanType => BuildNullBoolean(length),
            // An extension column (VARIANT) must keep its type: the string fallback below would put a
            // StringArray into a batch declaring the extension type — a silent schema/array mismatch
            // rather than a failure. Delegate to the schema-evolution builder, which handles the
            // storage type and re-wraps.
            ExtensionType ext => ext.CreateArray(
                SchemaEvolution.MakeNullArrayPublic(ext.StorageType, length)),
            _ => BuildNullString(length),
        };
    }

    private static Int64Array BuildNullInt64(int length)
    { var b = new Int64Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static Int32Array BuildNullInt32(int length)
    { var b = new Int32Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static Int16Array BuildNullInt16(int length)
    { var b = new Int16Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static Int8Array BuildNullInt8(int length)
    { var b = new Int8Array.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static DoubleArray BuildNullDouble(int length)
    { var b = new DoubleArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static FloatArray BuildNullFloat(int length)
    { var b = new FloatArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static BooleanArray BuildNullBoolean(int length)
    { var b = new BooleanArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }
    private static StringArray BuildNullString(int length)
    { var b = new StringArray.Builder(); for (int i = 0; i < length; i++) b.AppendNull(); return b.Build(); }

    #endregion
}
