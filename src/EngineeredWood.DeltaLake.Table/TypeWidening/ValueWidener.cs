// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
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
        bool needsWidening = false;

        for (int i = 0; i < batch.ColumnCount && i < targetSchema.FieldsList.Count; i++)
        {
            if (!TypesMatch(batch.Schema.FieldsList[i].DataType,
                            targetSchema.FieldsList[i].DataType))
            {
                needsWidening = true;
                break;
            }
        }

        if (!needsWidening)
            return batch;

        var columns = new IArrowArray[targetSchema.FieldsList.Count];

        for (int i = 0; i < targetSchema.FieldsList.Count; i++)
        {
            if (i < batch.ColumnCount)
            {
                var sourceType = batch.Schema.FieldsList[i].DataType;
                var targetType = targetSchema.FieldsList[i].DataType;

                columns[i] = TypesMatch(sourceType, targetType)
                    ? batch.Column(i)
                    : WidenArray(batch.Column(i), targetType);
            }
            else
            {
                // Missing column — fill with nulls of the TARGET type. This used to fall back to a
                // StringArray for anything it had no per-type builder for, so a null Timestamp or Decimal
                // column came back typed String: an array contradicting the schema it was placed into,
                // silently, rather than a failure.
                columns[i] = ArrowCompute.MakeNullArray(
                    targetSchema.FieldsList[i].DataType, batch.Length);
            }
        }

        return new RecordBatch(targetSchema, columns, batch.Length);
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

            _ => source, // No widening needed or unsupported
        };
    }

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

    #endregion
}
