// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using EngineeredWood.Expressions;

namespace EngineeredWood.Iceberg.Values;

/// <summary>
/// The Iceberg spec's binary single-value serialization (Appendix D): the bytes of a manifest's
/// <c>lower_bounds</c> and <c>upper_bounds</c>, of a manifest list's partition-summary bounds, and of the
/// values a Puffin theta sketch is fed.
/// </summary>
/// <remarks>
/// <para>Three entry points, for three jobs:</para>
/// <list type="bullet">
/// <item><description><see cref="Decode"/> reads a value written for exactly the given type, and throws on
/// anything else. Use it where the bytes must be well formed.</description></item>
/// <item><description><see cref="TryDecodeBound"/> reads a column bound for scan planning. It accepts what a
/// writer may have produced before the column's type was promoted, deciding by the length of the bytes, and
/// declines (returns false) wherever using the bound could prune a file that holds a match. A declined bound
/// is an unknown one, never an error: the file is read.</description></item>
/// <item><description><see cref="Encode"/> writes a value, in its canonical form.</description></item>
/// </list>
/// <para>
/// Values are <see cref="LiteralValue"/>s of the kinds <see cref="IcebergLiterals"/> lists. The decoded
/// bounds go into <see cref="Manifest.DataFile.ColumnLowerBounds"/> and
/// <see cref="Manifest.DataFile.ColumnUpperBounds"/>, which the scan planner hands to
/// <see cref="StatisticsEvaluator"/>. Keep the raw bytes as well when a manifest must be written back:
/// decoding is not lossless for every input (a bound for a promoted column comes back widened, a truncated
/// fixed bound comes back short).
/// </para>
/// </remarks>
public static class SingleValueBinary
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Decodes a value serialized for exactly <paramref name="type"/>.</summary>
    /// <exception cref="FormatException">The bytes are not a value of the type: the wrong length, a decimal with
    /// more digits than its precision, a time outside a day, a string that is not UTF-8.</exception>
    /// <exception cref="NotSupportedException">The type has no binary single value (<c>unknown</c>,
    /// <c>variant</c>, nested types), or the value is outside what .NET's date types hold (a date or a
    /// microsecond timestamp outside the years 1 to 9999).</exception>
    public static LiteralValue Decode(IcebergType type, ReadOnlySpan<byte> bytes)
    {
        switch (type)
        {
            case BooleanType:
                RequireLength(type, bytes, 1);
                return LiteralValue.Of(bytes[0] != 0);
            case IntegerType:
                RequireLength(type, bytes, 4);
                return LiteralValue.Of(BinaryPrimitives.ReadInt32LittleEndian(bytes));
            case LongType:
                RequireLength(type, bytes, 8);
                return LiteralValue.Of(BinaryPrimitives.ReadInt64LittleEndian(bytes));
            case FloatType:
                RequireLength(type, bytes, 4);
                return LiteralValue.Of(ReadSingle(bytes));
            case DoubleType:
                RequireLength(type, bytes, 8);
                return LiteralValue.Of(ReadDouble(bytes));
            case DecimalType d:
                return DecodeDecimal(d, bytes) is { } dec
                    ? dec
                    : throw Malformed(type, bytes, $"it is not an unscaled value of at most {d.Precision} digits");
            case DateType:
                RequireLength(type, bytes, 4);
                return IcebergLiterals.DateLiteral(BinaryPrimitives.ReadInt32LittleEndian(bytes));
            case TimeType:
                RequireLength(type, bytes, 8);
                return DecodeTime(bytes) is { } time
                    ? time
                    : throw Malformed(type, bytes, "it is not a time of day");
            case TimestampType or TimestamptzType:
                RequireLength(type, bytes, 8);
                return IcebergLiterals.MicrosLiteral(BinaryPrimitives.ReadInt64LittleEndian(bytes));
            case TimestampNsType or TimestamptzNsType:
                RequireLength(type, bytes, 8);
                return LiteralValue.TimestampNanosOf(BinaryPrimitives.ReadInt64LittleEndian(bytes));
            case StringType:
                return DecodeString(bytes) is { } s
                    ? LiteralValue.Of(s)
                    : throw Malformed(type, bytes, "it is not UTF-8");
            case UuidType:
                RequireLength(type, bytes, 16);
                return LiteralValue.Of(ReadUuid(bytes));
            case FixedType f:
                RequireLength(type, bytes, f.Length);
                return LiteralValue.Of(bytes.ToArray());
            case BinaryType or GeometryType or GeographyType:
                return LiteralValue.Of(bytes.ToArray());
        }

        throw IcebergLiterals.Unsupported(type);
    }

    /// <summary>
    /// Decodes a column bound from a manifest, for <see cref="StatisticsEvaluator"/>.
    /// </summary>
    /// <param name="type">The column's type in the schema being read, which may be a promotion of the type the
    /// bound was written for.</param>
    /// <param name="bytes">The serialized bound.</param>
    /// <param name="bound">The bound, in the kind <paramref name="type"/> decodes to.</param>
    /// <returns>False when the bound cannot be used, so the column's bound is unknown.</returns>
    /// <remarks>
    /// <para>
    /// Bounds are not rewritten when a column's type is promoted, so the spec has readers tell the written
    /// type from the length: a 4-byte <c>long</c> bound is an <c>int</c>, a 4-byte <c>double</c> bound a
    /// <c>float</c>, and a 4-byte <c>timestamp</c> or <c>timestamp_ns</c> bound a <c>date</c>, which is that
    /// day's midnight. A decimal's unscaled bytes are read at any length. The bound comes back widened to the
    /// type being read, so it compares with that column's literals.
    /// </para>
    /// <para>Declines, rather than return something that could prune a matching file:</para>
    /// <list type="bullet">
    /// <item><description>a NaN, which the spec forbids as a bound and which, as a lower bound, would claim
    /// every value is NaN;</description></item>
    /// <item><description>a value outside what the kind holds (a date or a microsecond timestamp outside the
    /// years 1 to 9999, a promoted date outside a nanosecond timestamp's range);</description></item>
    /// <item><description>a string bound that is not UTF-8, a decimal with more digits than its precision, any
    /// other length the type and its promotions do not produce;</description></item>
    /// <item><description><c>variant</c>, <c>geometry</c> and <c>geography</c>, whose bounds have encodings of
    /// their own, and <c>unknown</c>, which has none.</description></item>
    /// </list>
    /// <para>
    /// A string, binary or fixed bound may be truncated, and a <c>fixed</c> bound is accepted shorter than the
    /// type's length for that reason. A truncated lower bound is a prefix, still at most every value; a
    /// truncated upper bound is a prefix with its last character or byte incremented, still at least every
    /// value. Both stay sound for the evaluator.
    /// </para>
    /// </remarks>
    public static bool TryDecodeBound(IcebergType type, ReadOnlySpan<byte> bytes, out LiteralValue bound)
    {
        bound = default;
        switch (type)
        {
            case BooleanType when bytes.Length == 1:
                bound = LiteralValue.Of(bytes[0] != 0);
                return true;
            case IntegerType when bytes.Length == 4:
                bound = LiteralValue.Of(BinaryPrimitives.ReadInt32LittleEndian(bytes));
                return true;
            case LongType when bytes.Length == 8:
                bound = LiteralValue.Of(BinaryPrimitives.ReadInt64LittleEndian(bytes));
                return true;
            case LongType when bytes.Length == 4: // promoted from int
                bound = LiteralValue.Of((long)BinaryPrimitives.ReadInt32LittleEndian(bytes));
                return true;
            case FloatType when bytes.Length == 4:
                float f = ReadSingle(bytes);
                if (float.IsNaN(f))
                    return false;
                bound = LiteralValue.Of(f);
                return true;
            case DoubleType when bytes.Length is 8 or 4: // 4 = promoted from float
                double d = bytes.Length == 8 ? ReadDouble(bytes) : ReadSingle(bytes);
                if (double.IsNaN(d))
                    return false;
                bound = LiteralValue.Of(d);
                return true;
            case DecimalType dt:
                if (DecodeDecimal(dt, bytes) is not { } dec)
                    return false;
                bound = dec;
                return true;
            case DateType when bytes.Length == 4:
                return IcebergLiterals.TryDateLiteral(BinaryPrimitives.ReadInt32LittleEndian(bytes), out bound);
            case TimeType when bytes.Length == 8:
                if (DecodeTime(bytes) is not { } time)
                    return false;
                bound = time;
                return true;
            case TimestampType or TimestamptzType when bytes.Length == 8:
                return IcebergLiterals.TryMicrosLiteral(BinaryPrimitives.ReadInt64LittleEndian(bytes), out bound);
            case TimestampType when bytes.Length == 4: // promoted from date
                // A date out of .NET's range would overflow the multiplication; declining it first costs nothing.
                return IcebergLiterals.TryDateLiteral(BinaryPrimitives.ReadInt32LittleEndian(bytes), out _)
                    && IcebergLiterals.TryMicrosLiteral(
                        BinaryPrimitives.ReadInt32LittleEndian(bytes) * IcebergLiterals.MicrosPerDay, out bound);
            case TimestampNsType or TimestamptzNsType when bytes.Length == 8:
                bound = LiteralValue.TimestampNanosOf(BinaryPrimitives.ReadInt64LittleEndian(bytes));
                return true;
            case TimestampNsType when bytes.Length == 4: // promoted from date
                long days = BinaryPrimitives.ReadInt32LittleEndian(bytes);
                // A signed 64-bit nanosecond count spans about 106,751 days either side of the epoch.
                if (days is < -106_751 or > 106_751)
                    return false;
                bound = LiteralValue.TimestampNanosOf(days * IcebergLiterals.NanosPerDay);
                return true;
            case StringType:
                if (DecodeString(bytes) is not { } s)
                    return false;
                bound = LiteralValue.Of(s);
                return true;
            case UuidType when bytes.Length == 16:
                bound = LiteralValue.Of(ReadUuid(bytes));
                return true;
            case FixedType fx when bytes.Length <= fx.Length:
                bound = LiteralValue.Of(bytes.ToArray());
                return true;
            case BinaryType:
                bound = LiteralValue.Of(bytes.ToArray());
                return true;
        }

        return false;
    }

    /// <summary>Serializes a value of <paramref name="type"/>.</summary>
    /// <param name="type">The value's Iceberg type.</param>
    /// <param name="value">A non-null value in one of the kinds <see cref="IcebergLiterals.Coerce"/> accepts
    /// for the type.</param>
    /// <exception cref="ArgumentException">The value is null, or not a value of the type.</exception>
    /// <exception cref="NotSupportedException">The type has no binary single value.</exception>
    public static byte[] Encode(IcebergType type, LiteralValue value)
    {
        if (value.IsNull)
            throw new ArgumentException("A null has no binary single-value serialization.", nameof(value));

        var v = IcebergLiterals.Coerce(type, value);
        switch (type)
        {
            case BooleanType:
                return [v.AsBoolean ? (byte)1 : (byte)0];
            case IntegerType:
                return Int32Bytes(v.AsInt32);
            case DateType:
                return Int32Bytes(IcebergLiterals.ToEpochDays(type, v));
            case LongType:
                return Int64Bytes(v.AsInt64);
            case TimeType:
                return Int64Bytes(IcebergLiterals.ToTimeMicros(type, v));
            case TimestampType or TimestamptzType:
                return Int64Bytes(IcebergLiterals.ToEpochMicros(type, v));
            case TimestampNsType or TimestamptzNsType:
                return Int64Bytes(v.AsTimestampNanos);
            case FloatType:
                return Int32Bytes(SingleToBits(v.AsFloat));
            case DoubleType:
                return Int64Bytes(BitConverter.DoubleToInt64Bits(v.AsDouble));
            case DecimalType:
                return UnscaledBigEndian(v.AsHighPrecisionDecimal.UnscaledValue);
            case StringType:
                return Encoding.UTF8.GetBytes(v.AsString);
            case UuidType:
                return UuidBytes(v.AsGuid);
            case FixedType or BinaryType or GeometryType or GeographyType:
                return (byte[])v.AsBinary.Clone();
        }

        throw IcebergLiterals.Unsupported(type);
    }

    // ── Pieces ──

    private static LiteralValue? DecodeDecimal(DecimalType type, ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length == 0)
            return (LiteralValue?)null;

        var unscaled = ReadBigEndianTwosComplement(bytes);
        return IcebergLiterals.FitsPrecision(unscaled, type.Precision)
            ? LiteralValue.HighPrecisionDecimalOf(unscaled, type.Scale)
            : (LiteralValue?)null;
    }

    private static LiteralValue? DecodeTime(ReadOnlySpan<byte> bytes)
    {
        long micros = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        return micros is >= 0 and < IcebergLiterals.MicrosPerDay ? IcebergLiterals.TimeLiteral(micros) : (LiteralValue?)null;
    }

    private static string? DecodeString(ReadOnlySpan<byte> bytes)
    {
        try
        {
#if NETSTANDARD2_0
            return StrictUtf8.GetString(bytes.ToArray());
#else
            return StrictUtf8.GetString(bytes);
#endif
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    private static BigInteger ReadBigEndianTwosComplement(ReadOnlySpan<byte> bytes)
    {
#if NETSTANDARD2_0
        var little = bytes.ToArray();
        Array.Reverse(little);
        return new BigInteger(little);
#else
        return new BigInteger(bytes, isUnsigned: false, isBigEndian: true);
#endif
    }

    /// <summary>The minimal two's-complement big-endian bytes, as Java's <c>BigInteger.toByteArray</c>.</summary>
    internal static byte[] UnscaledBigEndian(BigInteger unscaled)
    {
#if NETSTANDARD2_0
        var bytes = unscaled.ToByteArray();
        Array.Reverse(bytes);
        return bytes;
#else
        return unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
#endif
    }

    /// <summary>A UUID from its 16 RFC 4122 bytes, which are big-endian.</summary>
    internal static Guid ReadUuid(ReadOnlySpan<byte> bytes) => new(
        BinaryPrimitives.ReadInt32BigEndian(bytes),
        BinaryPrimitives.ReadInt16BigEndian(bytes.Slice(4)),
        BinaryPrimitives.ReadInt16BigEndian(bytes.Slice(6)),
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);

    /// <summary>A UUID's 16 RFC 4122 bytes. <see cref="Guid.ToByteArray()"/> stores its first three fields little-endian.</summary>
    internal static byte[] UuidBytes(Guid value)
    {
        var bytes = value.ToByteArray();
        Array.Reverse(bytes, 0, 4);
        Array.Reverse(bytes, 4, 2);
        Array.Reverse(bytes, 6, 2);
        return bytes;
    }

    private static unsafe float ReadSingle(ReadOnlySpan<byte> bytes)
    {
        int bits = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        return *(float*)&bits;
    }

    private static double ReadDouble(ReadOnlySpan<byte> bytes) =>
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(bytes));

    private static unsafe int SingleToBits(float value) => *(int*)&value;

    private static byte[] Int32Bytes(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] Int64Bytes(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static void RequireLength(IcebergType type, ReadOnlySpan<byte> bytes, int length)
    {
        if (bytes.Length != length)
            throw Malformed(type, bytes, $"it is {bytes.Length} bytes, not {length}");
    }

    private static FormatException Malformed(IcebergType type, ReadOnlySpan<byte> bytes, string why) =>
        new($"Bytes {Hex(bytes)} are not a binary single value of Iceberg type {IcebergLiterals.TypeName(type)}: {why}.");

    private static string Hex(ReadOnlySpan<byte> bytes)
    {
        const int Shown = 32;
        var sb = new StringBuilder(Math.Min(bytes.Length, Shown) * 2 + 3);
        for (int i = 0; i < bytes.Length && i < Shown; i++)
            sb.Append(bytes[i].ToString("x2", System.Globalization.CultureInfo.InvariantCulture));
        if (bytes.Length > Shown)
            sb.Append("...");
        return sb.Length == 0 ? "(empty)" : sb.ToString();
    }
}
