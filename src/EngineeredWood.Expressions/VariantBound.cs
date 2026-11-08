// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;

namespace EngineeredWood.Expressions;

/// <summary>The Variant primitive types a per-path bound can hold (see <see cref="VariantBound"/>).</summary>
public enum VariantBoundKind : byte
{
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    Decimal,
    Date,
    Timestamp,
    TimestampNtz,
    String,
}

/// <summary>
/// One end of the range a path of a variant column holds, as a Variant primitive value, and the one rule set
/// that turns it into a bound on <c>variant_get</c>'s result (<see cref="ToLiteral"/>).
/// </summary>
/// <remarks>
/// Two sources produce these and share the rules: a Delta file's variant <c>minValues</c> / <c>maxValues</c>,
/// which hold Variant values, and the statistics of a shredded <c>typed_value</c> column in a Parquet footer or
/// page index, whose Parquet type maps to a Variant primitive type as the shredding spec maps it.
/// </remarks>
public readonly struct VariantBound
{
    private static readonly DateTime s_epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly long _integer;    // Int8..Int64; Date: days since the epoch; Timestamp(Ntz): micros
    private readonly double _real;     // Float, Double (a Float is held exactly)
    private readonly object? _ref;     // String: string; Decimal: BigInteger (the unscaled value)
    private readonly int _scale;       // Decimal

    private VariantBound(VariantBoundKind kind, long integer = 0, double real = 0, object? reference = null, int scale = 0)
    {
        Kind = kind;
        _integer = integer;
        _real = real;
        _ref = reference;
        _scale = scale;
    }

    public VariantBoundKind Kind { get; }

    public static VariantBound Boolean(bool value) => new(VariantBoundKind.Boolean, integer: value ? 1 : 0);

    /// <summary>An integer of the Variant type <paramref name="kind"/> (<c>Int8</c> to <c>Int64</c>).</summary>
    public static VariantBound Integer(VariantBoundKind kind, long value) =>
        kind is VariantBoundKind.Int8 or VariantBoundKind.Int16 or VariantBoundKind.Int32 or VariantBoundKind.Int64
            ? new(kind, integer: value)
            : throw new ArgumentOutOfRangeException(nameof(kind), kind, "Not an integer kind.");

    public static VariantBound Float(float value) => new(VariantBoundKind.Float, real: value);

    public static VariantBound Double(double value) => new(VariantBoundKind.Double, real: value);

    /// <summary>A decimal, by its unscaled digits and scale.</summary>
    public static VariantBound Decimal(BigInteger unscaled, int scale) =>
        new(VariantBoundKind.Decimal, reference: unscaled, scale: scale);

    /// <summary>A date, in days since 1970-01-01.</summary>
    public static VariantBound Date(int days) => new(VariantBoundKind.Date, integer: days);

    /// <summary>A timestamp with time zone, in microseconds since the epoch (UTC).</summary>
    public static VariantBound Timestamp(long micros) => new(VariantBoundKind.Timestamp, integer: micros);

    /// <summary>A timestamp without time zone, in microseconds since the epoch.</summary>
    public static VariantBound TimestampNtz(long micros) => new(VariantBoundKind.TimestampNtz, integer: micros);

    public static VariantBound String(string value) =>
        new(VariantBoundKind.String, reference: value ?? throw new ArgumentNullException(nameof(value)));

    private bool IsInteger =>
        Kind is VariantBoundKind.Int8 or VariantBoundKind.Int16 or VariantBoundKind.Int32 or VariantBoundKind.Int64;

    /// <summary>
    /// This value as a bound on <c>variant_get(v, path, <paramref name="targetType"/>)</c>, or null when it does
    /// not convert to that type EXACTLY. <paramref name="targetType"/> is a canonical Spark type name
    /// (<see cref="VariantGetCall.TargetType"/>).
    /// </summary>
    /// <remarks>
    /// <para>Exactly means that every value between two bounds converts to the type without rounding, overflow or
    /// a change of order, so that a unit the bounds prune holds no row whose conversion fails: skipping it would
    /// answer a query Spark answers with an error. An integer converts to any integer or decimal type it fits; a
    /// decimal to a decimal of at least its scale that it fits; a float to <c>float</c> or <c>double</c>, a double
    /// to <c>double</c>; a string, boolean or date to its own type; a timestamp to <c>timestamp</c>, a timestamp
    /// without zone to <c>timestamp_ntz</c>. Nothing else, so a string never answers <c>int</c>, though Spark
    /// converts <c>"7"</c> to 7.</para>
    /// <para>The literal's kind is the one a Delta column of the type bounds with: <see cref="LiteralValue.Kind.Int32"/>
    /// for the integer types up to <c>int</c>, a <see cref="DateTimeOffset"/> at UTC midnight for a date, and a
    /// <see cref="decimal"/> where one holds the value exactly.</para>
    /// </remarks>
    public LiteralValue? ToLiteral(string targetType)
    {
        try
        {
            switch (targetType)
            {
                case "tinyint":
                    return IsInteger && _integer is >= sbyte.MinValue and <= sbyte.MaxValue
                        ? (LiteralValue?)LiteralValue.Of((int)_integer) : null;
                case "smallint":
                    return IsInteger && _integer is >= short.MinValue and <= short.MaxValue
                        ? (LiteralValue?)LiteralValue.Of((int)_integer) : null;
                case "int":
                    return IsInteger && _integer is >= int.MinValue and <= int.MaxValue
                        ? (LiteralValue?)LiteralValue.Of((int)_integer) : null;
                case "bigint":
                    return IsInteger ? (LiteralValue?)LiteralValue.Of(_integer) : null;
                case "float":
                    return Kind == VariantBoundKind.Float ? (LiteralValue?)LiteralValue.Of((float)_real) : null;
                case "double":
                    return Kind is VariantBoundKind.Float or VariantBoundKind.Double
                        ? (LiteralValue?)LiteralValue.Of(_real) : null;
                case "boolean":
                    return Kind == VariantBoundKind.Boolean ? (LiteralValue?)LiteralValue.Of(_integer != 0) : null;
                case "string":
                    return Kind == VariantBoundKind.String ? (LiteralValue?)LiteralValue.Of((string)_ref!) : null;
                case "date":
                    return Kind == VariantBoundKind.Date
                        ? (LiteralValue?)LiteralValue.Of(new DateTimeOffset(s_epoch.AddDays(_integer), TimeSpan.Zero))
                        : null;
                case "timestamp":
                    return Kind == VariantBoundKind.Timestamp ? (LiteralValue?)Micros(_integer) : null;
                case "timestamp_ntz":
                    return Kind == VariantBoundKind.TimestampNtz ? (LiteralValue?)Micros(_integer) : null;
                default:
                    return TryParseDecimalType(targetType, out int precision, out int scale)
                        ? ToDecimal(precision, scale)
                        : null;
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            return null; // a date or timestamp outside DateTime's range
        }
        catch (OverflowException)
        {
            return null;
        }

        static LiteralValue Micros(long micros) =>
            LiteralValue.Of(new DateTimeOffset(s_epoch.AddTicks(checked(micros * 10)), TimeSpan.Zero));
    }

    private LiteralValue? ToDecimal(int precision, int declared)
    {
        BigInteger unscaled;
        int scale;
        if (Kind == VariantBoundKind.Decimal)
        {
            unscaled = (BigInteger)_ref!;
            scale = _scale;
        }
        else if (IsInteger)
        {
            unscaled = _integer;
            scale = 0;
        }
        else
        {
            return null;
        }

        // A scale above the declared one would round; one below it pads, which is exact.
        if (scale < 0 || scale > declared)
            return null;
        if (declared > scale)
            unscaled *= BigInteger.Pow(10, declared - scale);
        if (BigInteger.Abs(unscaled) >= BigInteger.Pow(10, precision))
            return null;
        return DecimalLiteral(unscaled, declared);
    }

    // The canonical decimal(p,s) of VariantGetCall.TryNormalizeTypeName, with its own bounds checked again.
    private static bool TryParseDecimalType(string typeName, out int precision, out int scale)
    {
        precision = scale = -1;
        if (!typeName.StartsWith("decimal(", StringComparison.Ordinal) || !typeName.EndsWith(")", StringComparison.Ordinal))
            return false;
        string[] parts = typeName.Substring(8, typeName.Length - 9).Split(',');
        return parts.Length == 2
            && int.TryParse(parts[0].Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out precision)
            && int.TryParse(parts[1].Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out scale)
            && precision is >= 1 and <= 38 && scale <= precision;
    }

    // The largest magnitude a System.Decimal can hold (a 96-bit unscaled integer).
    private static readonly BigInteger Decimal96Max = (BigInteger.One << 96) - 1;

    /// <summary>
    /// A <see cref="decimal"/> when one holds the value exactly (scale 0-28, magnitude within 96 bits), else a
    /// high-precision decimal: the forms a Delta decimal column's bounds take, and both compare exactly.
    /// </summary>
    private static LiteralValue DecimalLiteral(BigInteger unscaled, int scale)
    {
        BigInteger magnitude = BigInteger.Abs(unscaled);
        if (scale <= 28 && magnitude <= Decimal96Max)
        {
            uint lo = (uint)(magnitude & uint.MaxValue);
            uint mid = (uint)((magnitude >> 32) & uint.MaxValue);
            uint hi = (uint)((magnitude >> 64) & uint.MaxValue);
            return LiteralValue.Of(new decimal((int)lo, (int)mid, (int)hi, unscaled.Sign < 0, (byte)scale));
        }
        return LiteralValue.HighPrecisionDecimalOf(unscaled, scale);
    }
}
