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

    private bool IsInteger => IsIntegerKind(Kind);

    private static bool IsIntegerKind(VariantBoundKind kind) =>
        kind is VariantBoundKind.Int8 or VariantBoundKind.Int16 or VariantBoundKind.Int32 or VariantBoundKind.Int64;

    /// <summary>
    /// The value of an integer, a boolean (0 or 1), a date (days since 1970-01-01), or a timestamp of either
    /// kind (microseconds since the epoch).
    /// </summary>
    public long AsInteger => IsInteger || Kind is VariantBoundKind.Boolean or VariantBoundKind.Date
        or VariantBoundKind.Timestamp or VariantBoundKind.TimestampNtz
        ? _integer
        : throw new InvalidOperationException($"A {Kind} bound has no integer value.");

    /// <summary>The value of a float (held exactly) or a double.</summary>
    public double AsReal => Kind is VariantBoundKind.Float or VariantBoundKind.Double
        ? _real
        : throw new InvalidOperationException($"A {Kind} bound has no floating-point value.");

    /// <summary>The value of a string.</summary>
    public string AsString => Kind == VariantBoundKind.String
        ? (string)_ref!
        : throw new InvalidOperationException($"A {Kind} bound has no string value.");

    /// <summary>The value of a decimal, by its unscaled digits and scale.</summary>
    public (BigInteger Unscaled, int Scale) AsDecimal => Kind == VariantBoundKind.Decimal
        ? ((BigInteger)_ref!, _scale)
        : throw new InvalidOperationException($"A {Kind} bound has no decimal value.");

    /// <summary>
    /// The inverse of <see cref="ToLiteral"/>: the value a path of Variant type <paramref name="kind"/> must hold
    /// for <c>variant_get(v, path, <paramref name="targetType"/>)</c> to equal <paramref name="literal"/>.
    /// </summary>
    /// <param name="kind">The Variant type the path holds, as a shredded leaf's Parquet type maps to it.</param>
    /// <param name="scale">For a <see cref="VariantBoundKind.Decimal"/> kind, its scale; otherwise ignored.</param>
    /// <param name="targetType">A canonical Spark type name (<see cref="VariantGetCall.TargetType"/>).</param>
    /// <param name="literal">The value the result is compared with for equality.</param>
    /// <param name="value">
    /// The one value of the kind that converts to <paramref name="literal"/>, or null when no value of the kind
    /// does.
    /// </param>
    /// <returns>
    /// False when this cannot say, so the caller must not conclude anything: the pair of kind and type is one
    /// Spark converts lossily, or <paramref name="literal"/> is not exactly a value of <paramref name="targetType"/>.
    /// </returns>
    /// <remarks>
    /// <para>Three outcomes, not two, because a null <paramref name="value"/> is a claim that no row's result
    /// equals the literal. That claim holds only for a pair whose every value converts exactly or not at all,
    /// which are the pairs <see cref="ToLiteral"/> converts: an integer to an integer type or a decimal, a decimal
    /// to a decimal of at least its scale, a float to <c>float</c> or <c>double</c>, a double to <c>double</c>,
    /// and the others to their own type. A value that does not convert (an <c>Int64</c> past <c>int</c>) is an
    /// error in <c>variant_get</c> and NULL in <c>try_variant_get</c>, never equal. Spark converts every other
    /// pair loosely, measured against Spark 4.0: a string <c>"7"</c> to <c>int</c> 7, a double 1.5 to <c>int</c>
    /// 1, a decimal 1.25 to <c>decimal(5,1)</c> 1.3, a date to a timestamp. Those decline.</para>
    /// <para>The literal is first read as a value of <paramref name="targetType"/>, exactly, so the answer does
    /// not depend on how a comparison of mixed types widens. A literal that is not one (7.5 against
    /// <c>int</c>, or 1000 against <c>tinyint</c>) declines.</para>
    /// <para>A NaN against <c>float</c> or <c>double</c> maps to a NaN of the kind; whether a source can be asked
    /// about it is the caller's concern.</para>
    /// <para>Spark compares a decimal at the least common type of the two sides, which can give up scale: for a
    /// <c>decimal(38,38)</c> result and the literal <c>0</c> it is <c>decimal(38,37)</c>, so a result of 1e-38
    /// rounds to 0 and matches. Where that type would round either side, this declines. The result's type is
    /// known exactly here, so only the literal's typing follows <see cref="SparkDecimalRounding"/>.</para>
    /// </remarks>
    /// <param name="setMembership">
    /// Whether <paramref name="literal"/> is a member of an <c>IN</c> list rather than an equality's operand. A set
    /// types an integral member by its type's width, not its digits, which can force the common type to round.
    /// </param>
    public static bool TryInvert(
        VariantBoundKind kind, int scale, string targetType, LiteralValue literal, out VariantBound? value,
        bool setMembership = false)
    {
        value = null;
        if (literal.IsNull || !Bounded(literal) || ComparisonRounds(targetType, literal, setMembership))
            return false;

        switch (targetType)
        {
            case "tinyint":
                return InvertInteger(kind, literal, sbyte.MinValue, sbyte.MaxValue, out value);
            case "smallint":
                return InvertInteger(kind, literal, short.MinValue, short.MaxValue, out value);
            case "int":
                return InvertInteger(kind, literal, int.MinValue, int.MaxValue, out value);
            case "bigint":
                return InvertInteger(kind, literal, long.MinValue, long.MaxValue, out value);

            case "float":
            {
                if (kind != VariantBoundKind.Float || !TryAsFloat(literal, out float f))
                    return false;
                value = Float(f);
                return true;
            }

            case "double":
            {
                if (kind is not (VariantBoundKind.Float or VariantBoundKind.Double) || !TryAsDouble(literal, out double d))
                    return false;
                if (kind == VariantBoundKind.Double)
                {
                    value = Double(d);
                    return true;
                }
                // A float widens to double exactly (Spark 4.0: a float 0.1 reads as 0.10000000149011612), so only a
                // double that is a float has one. A NaN is a NaN.
                float narrowed = (float)d;
                if (double.IsNaN(d) || (double)narrowed == d)
                    value = Float(narrowed);
                return true;
            }

            case "boolean":
                if (kind != VariantBoundKind.Boolean || literal.Type != LiteralValue.Kind.Boolean)
                    return false;
                value = Boolean(literal.AsBoolean);
                return true;

            case "string":
                if (kind != VariantBoundKind.String || literal.Type != LiteralValue.Kind.String)
                    return false;
                value = String(literal.AsString);
                return true;

            case "date":
            {
                if (kind != VariantBoundKind.Date || !TryAsDays(literal, out int days))
                    return false;
                value = Date(days);
                return true;
            }

            case "timestamp" or "timestamp_ntz":
            {
                var own = targetType == "timestamp" ? VariantBoundKind.Timestamp : VariantBoundKind.TimestampNtz;
                if (kind != own || !TryAsMicros(literal, out long micros))
                    return false;
                value = new VariantBound(own, integer: micros);
                return true;
            }

            default:
                return TryParseDecimalType(targetType, out int precision, out int declared)
                    && InvertDecimal(kind, scale, precision, declared, literal, out value);
        }
    }

    // Any literal exactly a value of a Spark type has at most 38 significant digits and scale 38. One far past that,
    // trailing zeros or not, is declined before anything is computed from its digits: a public
    // HighPrecisionDecimalOf(1, 50_000_000) would otherwise build a 50-million-digit power of ten.
    private static bool Bounded(LiteralValue literal)
    {
        if (literal.Type != LiteralValue.Kind.HighPrecisionDecimal)
            return true;
        var (unscaled, scale) = literal.AsHighPrecisionDecimal;
        return scale is >= 0 and <= 2 * SparkDecimalRounding.MaxPrecision
            && BigInteger.Abs(unscaled).ToByteArray().Length <= 32; // 2^256 > 10^76
    }

    /// <summary>
    /// Whether Spark's comparison of a <paramref name="targetType"/> result with <paramref name="literal"/> unifies the
    /// two as decimals at a type that rounds either one. Two integrals, or a float or double, are not unified as
    /// decimals.
    /// </summary>
    private static bool ComparisonRounds(string targetType, LiteralValue literal, bool setMembership)
    {
        int resultIntegral, resultScale;
        switch (targetType)
        {
            // DecimalType.forType's widths.
            case "tinyint": resultIntegral = 3; resultScale = 0; break;
            case "smallint": resultIntegral = 5; resultScale = 0; break;
            case "int": resultIntegral = 10; resultScale = 0; break;
            case "bigint": resultIntegral = 20; resultScale = 0; break;
            default:
                if (!TryParseDecimalType(targetType, out int precision, out resultScale))
                    return false;
                resultIntegral = precision - resultScale;
                break;
        }

        bool literalIsDecimal = literal.Type is LiteralValue.Kind.Decimal or LiteralValue.Kind.HighPrecisionDecimal;
        if (resultScale == 0 && !literalIsDecimal && !targetType.StartsWith("decimal", StringComparison.Ordinal))
            return false;
        if (!SparkDecimalRounding.TryDescribe(literal, literalTyping: !setMembership, out int literalIntegral,
                out int literalScale))
        {
            return false; // a float or double: compared as a double, which the inverse's own typing settles
        }
        // A set member's decimal width is the whole set's, unknown from one member: the widest it could be.
        if (literalIntegral < 0)
            literalIntegral = SparkDecimalRounding.MaxPrecision - literalScale;

        int commonScale = Math.Max(resultScale, literalScale);
        int commonIntegral = Math.Max(resultIntegral, literalIntegral);
        return commonIntegral + commonScale > SparkDecimalRounding.MaxPrecision;
    }

    // An integer result of the range [min, max]: the literal must be exactly one, and the kind must be an integer,
    // whose value is then the literal itself, if the kind can hold it.
    private static bool InvertInteger(VariantBoundKind kind, LiteralValue literal, long min, long max, out VariantBound? value)
    {
        value = null;
        if (!IsIntegerKind(kind) || !TryAsScaled(literal, out BigInteger unscaled, out int literalScale)
            || !TryRescale(unscaled, literalScale, 0, out BigInteger integer)
            || integer < min || integer > max)
        {
            return false;
        }

        if (FitsKind(kind, integer))
            value = Integer(kind, (long)integer);
        return true;
    }

    private static bool InvertDecimal(
        VariantBoundKind kind, int scale, int precision, int declared, LiteralValue literal, out VariantBound? value)
    {
        value = null;
        // The pairs ToDecimal converts: an integer (scale 0), or a decimal of at most the declared scale. A decimal
        // of more scale rounds, which Spark does rather than fail.
        int kindScale;
        if (IsIntegerKind(kind))
            kindScale = 0;
        else if (kind == VariantBoundKind.Decimal && scale >= 0 && scale <= declared)
            kindScale = scale;
        else
            return false;

        if (!TryAsScaled(literal, out BigInteger unscaled, out int literalScale)
            || !TryRescale(unscaled, literalScale, declared, out BigInteger result)
            || BigInteger.Abs(result) >= BigInteger.Pow(10, precision))
        {
            return false;
        }

        // Back to the kind's scale: a result with digits below it is no value's conversion.
        if (!TryRescale(result, declared, kindScale, out BigInteger own))
            return true;
        if (kind == VariantBoundKind.Decimal)
            value = Decimal(own, kindScale);
        else if (FitsKind(kind, own))
            value = Integer(kind, (long)own);
        return true;
    }

    private static bool FitsKind(VariantBoundKind kind, BigInteger value) => kind switch
    {
        VariantBoundKind.Int8 => value >= sbyte.MinValue && value <= sbyte.MaxValue,
        VariantBoundKind.Int16 => value >= short.MinValue && value <= short.MaxValue,
        VariantBoundKind.Int32 => value >= int.MinValue && value <= int.MaxValue,
        VariantBoundKind.Int64 => value >= long.MinValue && value <= long.MaxValue,
        _ => false,
    };

    // unscaled × 10^-from as an unscaled value at scale `to`, or false when that would drop a nonzero digit.
    private static bool TryRescale(BigInteger unscaled, int from, int to, out BigInteger result)
    {
        if (to >= from)
        {
            result = unscaled * BigInteger.Pow(10, to - from);
            return true;
        }
        result = BigInteger.DivRem(unscaled, BigInteger.Pow(10, from - to), out BigInteger remainder);
        return remainder.IsZero;
    }

    // An exact number as unscaled × 10^-scale: an integer, or a decimal. Not a float or double, which are inexact.
    private static bool TryAsScaled(LiteralValue literal, out BigInteger unscaled, out int scale)
    {
        scale = 0;
        switch (literal.Type)
        {
            case LiteralValue.Kind.Int32: unscaled = literal.AsInt32; return true;
            case LiteralValue.Kind.Int64: unscaled = literal.AsInt64; return true;
            case LiteralValue.Kind.Decimal:
            {
                int[] bits = decimal.GetBits(literal.AsDecimal);
                scale = (bits[3] >> 16) & 0x7F;
                var magnitude = (new BigInteger((uint)bits[2]) << 64)
                    | (new BigInteger((uint)bits[1]) << 32)
                    | new BigInteger((uint)bits[0]);
                unscaled = (bits[3] & unchecked((int)0x80000000)) != 0 ? -magnitude : magnitude;
                return true;
            }
            case LiteralValue.Kind.HighPrecisionDecimal:
                (unscaled, scale) = literal.AsHighPrecisionDecimal;
                return scale >= 0;
            default:
                unscaled = default;
                return false;
        }
    }

    private static bool TryAsDouble(LiteralValue literal, out double value)
    {
        switch (literal.Type)
        {
            case LiteralValue.Kind.Double: value = literal.AsDouble; return true;
            case LiteralValue.Kind.Float: value = literal.AsFloat; return true;
            case LiteralValue.Kind.Int32: value = literal.AsInt32; return true;
            case LiteralValue.Kind.Int64:
                value = literal.AsInt64;
                // Exact only if it converts back. long.MaxValue rounds up to 2^63, which no long holds.
                return value < 9.2233720368547758E18 && (long)value == literal.AsInt64;
            default:
                value = 0;
                return false;
        }
    }

    private static bool TryAsFloat(LiteralValue literal, out float value)
    {
        value = 0;
        if (literal.Type == LiteralValue.Kind.Float)
        {
            value = literal.AsFloat;
            return true;
        }
        if (!TryAsDouble(literal, out double d))
            return false;
        value = (float)d;
        return double.IsNaN(d) || (double)value == d;
    }

    private static bool TryAsDays(LiteralValue literal, out int days)
    {
        days = 0;
        switch (literal.Type)
        {
#if NET6_0_OR_GREATER
            case LiteralValue.Kind.DateOnly:
                days = literal.AsDateOnly.DayNumber - 719_162; // days from 0001-01-01 to 1970-01-01
                return true;
#endif
            case LiteralValue.Kind.DateTimeOffset:
            {
                // ToLiteral's form for a date: UTC midnight. Any other instant is no date.
                var utc = literal.AsDateTimeOffset.UtcDateTime;
                if (utc.TimeOfDay != TimeSpan.Zero)
                    return false;
                days = (int)(utc - s_epoch).TotalDays;
                return true;
            }
            default:
                return false;
        }
    }

    private static bool TryAsMicros(LiteralValue literal, out long micros)
    {
        micros = 0;
        if (literal.Type != LiteralValue.Kind.DateTimeOffset)
            return false;
        long ticks = literal.AsDateTimeOffset.UtcTicks - s_epoch.Ticks;
        if (ticks % 10 != 0)
            return false;
        micros = ticks / 10;
        return true;
    }

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
