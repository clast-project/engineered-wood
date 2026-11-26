// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using EngineeredWood.Expressions;

namespace EngineeredWood.Iceberg.Values;

/// <summary>
/// How a value of each Iceberg primitive type is held as a <see cref="LiteralValue"/>: the one kind
/// <see cref="SingleValueBinary"/> and <see cref="SingleValueJson"/> decode to, and the kinds they accept
/// when encoding.
/// </summary>
/// <remarks>
/// <list type="table">
/// <listheader><term>Iceberg type</term><description>Decoded kind (and other kinds accepted when encoding)</description></listheader>
/// <item><term>boolean</term><description><see cref="LiteralValue.Kind.Boolean"/></description></item>
/// <item><term>int</term><description><see cref="LiteralValue.Kind.Int32"/></description></item>
/// <item><term>long</term><description><see cref="LiteralValue.Kind.Int64"/> (Int32)</description></item>
/// <item><term>float</term><description><see cref="LiteralValue.Kind.Float"/></description></item>
/// <item><term>double</term><description><see cref="LiteralValue.Kind.Double"/> (Float)</description></item>
/// <item><term>decimal(P, S)</term><description><see cref="LiteralValue.Kind.HighPrecisionDecimal"/> at scale S
/// (Decimal, and HighPrecisionDecimal at another scale, when the value rescales exactly; integers)</description></item>
/// <item><term>date</term><description><see cref="LiteralValue.Kind.DateOnly"/>; on netstandard2.0, which has no
/// <c>DateOnly</c>, a <see cref="LiteralValue.Kind.DateTimeOffset"/> at UTC midnight (DateTimeOffset at a UTC
/// midnight)</description></item>
/// <item><term>time</term><description><see cref="LiteralValue.Kind.TimeOnly"/>; on netstandard2.0, an
/// <see cref="LiteralValue.Kind.Int64"/> of microseconds since midnight</description></item>
/// <item><term>timestamp, timestamptz</term><description><see cref="LiteralValue.Kind.DateTimeOffset"/> at offset
/// zero; a timestamp without a zone is its wall-clock reading as UTC (TimestampNanos on a whole microsecond;
/// DateOnly)</description></item>
/// <item><term>timestamp_ns, timestamptz_ns</term><description><see cref="LiteralValue.Kind.TimestampNanos"/>
/// (DateTimeOffset; DateOnly)</description></item>
/// <item><term>string</term><description><see cref="LiteralValue.Kind.String"/></description></item>
/// <item><term>uuid</term><description><see cref="LiteralValue.Kind.Guid"/></description></item>
/// <item><term>fixed(L), binary</term><description><see cref="LiteralValue.Kind.Binary"/></description></item>
/// <item><term>geometry, geography</term><description><see cref="LiteralValue.Kind.Binary"/> holding WKB, in the
/// binary serialization only</description></item>
/// </list>
/// <para>
/// A microsecond timestamp is held exactly: <see cref="DateTimeOffset"/>'s 100 ns tick divides a
/// microsecond. Its range is years 1 to 9999, narrower than Iceberg's, so a value outside it is refused
/// rather than clamped. A nanosecond timestamp needs <see cref="LiteralValue.Kind.TimestampNanos"/>, because
/// a tick cannot hold it.
/// </para>
/// <para>
/// <c>unknown</c>, <c>variant</c> and the nested types have no single value: their defaults are null, and
/// <c>variant</c>'s bounds have an encoding of their own.
/// </para>
/// </remarks>
public static class IcebergLiterals
{
    internal static readonly long UnixEpochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;
    internal const long MicrosPerDay = 86_400_000_000L;
    internal const long NanosPerDay = 86_400_000_000_000L;
    private static readonly long MinMicros = (DateTimeOffset.MinValue.UtcTicks - UnixEpochTicks) / 10;
    private static readonly long MaxMicros = (DateTimeOffset.MaxValue.UtcTicks - UnixEpochTicks) / 10;

    /// <summary>The kind a value of <paramref name="type"/> decodes to.</summary>
    /// <exception cref="NotSupportedException">The type has no single value.</exception>
    public static LiteralValue.Kind KindOf(IcebergType type) => type switch
    {
        BooleanType => LiteralValue.Kind.Boolean,
        IntegerType => LiteralValue.Kind.Int32,
        LongType => LiteralValue.Kind.Int64,
        FloatType => LiteralValue.Kind.Float,
        DoubleType => LiteralValue.Kind.Double,
        DecimalType => LiteralValue.Kind.HighPrecisionDecimal,
#if NET6_0_OR_GREATER
        DateType => LiteralValue.Kind.DateOnly,
        TimeType => LiteralValue.Kind.TimeOnly,
#else
        DateType => LiteralValue.Kind.DateTimeOffset,
        TimeType => LiteralValue.Kind.Int64,
#endif
        TimestampType or TimestamptzType => LiteralValue.Kind.DateTimeOffset,
        TimestampNsType or TimestamptzNsType => LiteralValue.Kind.TimestampNanos,
        StringType => LiteralValue.Kind.String,
        UuidType => LiteralValue.Kind.Guid,
        FixedType or BinaryType or GeometryType or GeographyType => LiteralValue.Kind.Binary,
        _ => throw Unsupported(type),
    };

    /// <summary>
    /// Converts <paramref name="value"/> to the kind <paramref name="type"/> decodes to, when it holds a value
    /// of that type exactly.
    /// </summary>
    /// <returns>The value in its decoded kind. A null literal stays null.</returns>
    /// <exception cref="ArgumentException">The value is not one of <paramref name="type"/>'s, or does not fit it:
    /// a decimal with more digits than its precision or a scale it cannot reach exactly, a fixed value of
    /// another length, a timestamp finer than its unit.</exception>
    /// <exception cref="NotSupportedException">The type has no single value.</exception>
    public static LiteralValue Coerce(IcebergType type, LiteralValue value)
    {
        if (value.IsNull)
            return value;

        switch (type)
        {
            case BooleanType when value.Type == LiteralValue.Kind.Boolean:
            case IntegerType when value.Type == LiteralValue.Kind.Int32:
            case LongType when value.Type == LiteralValue.Kind.Int64:
            case FloatType when value.Type == LiteralValue.Kind.Float:
            case DoubleType when value.Type == LiteralValue.Kind.Double:
            case UuidType when value.Type == LiteralValue.Kind.Guid:
            case BinaryType or GeometryType or GeographyType when value.Type == LiteralValue.Kind.Binary:
                return value;
            case StringType when value.Type == LiteralValue.Kind.String:
                if (!IsWellFormedUtf16(value.AsString))
                    throw Mismatch(type, value, "it holds an unpaired UTF-16 surrogate, which UTF-8 cannot encode");
                return value;
            case LongType when value.Type == LiteralValue.Kind.Int32:
                return LiteralValue.Of((long)value.AsInt32);
            case DoubleType when value.Type == LiteralValue.Kind.Float:
                return LiteralValue.Of((double)value.AsFloat);
            case FixedType f when value.Type == LiteralValue.Kind.Binary:
                if (value.AsBinary.Length != f.Length)
                    throw Mismatch(type, value, $"it holds {value.AsBinary.Length} bytes");
                return value;
            case DecimalType d:
                return CoerceDecimal(d, value);
            case DateType:
                return DateLiteral(ToEpochDays(type, value));
            case TimeType:
                return TimeLiteral(ToTimeMicros(type, value));
            case TimestampType or TimestamptzType:
                return MicrosLiteral(ToEpochMicros(type, value));
            case TimestampNsType or TimestamptzNsType:
                return LiteralValue.TimestampNanosOf(ToEpochNanos(type, value));
        }

        _ = KindOf(type); // throws for a type with no single value
        throw Mismatch(type, value, null);
    }

    /// <summary>Whether every surrogate in <paramref name="s"/> is half of a pair.</summary>
    private static bool IsWellFormedUtf16(string s)
    {
        for (int i = 0; i < s.Length; i++)
        {
            char c = s[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 >= s.Length || !char.IsLowSurrogate(s[i + 1]))
                    return false;
                i++;
            }
            else if (char.IsLowSurrogate(c))
            {
                return false;
            }
        }

        return true;
    }

    // ── The Iceberg-internal representation of each temporal type ──

    private static readonly long MinDays = FloorDiv(DateTimeOffset.MinValue.UtcTicks - UnixEpochTicks, TimeSpan.TicksPerDay);
    private static readonly long MaxDays = FloorDiv(DateTimeOffset.MaxValue.UtcTicks - UnixEpochTicks, TimeSpan.TicksPerDay);

    /// <summary>A date as a literal, or false outside the years 1 to 9999 that .NET's date types hold.</summary>
    internal static bool TryDateLiteral(long days, out LiteralValue literal)
    {
        if (days < MinDays || days > MaxDays)
        {
            literal = default;
            return false;
        }

#if NET6_0_OR_GREATER
        literal = LiteralValue.Of(DateOnly.FromDayNumber((int)(UnixEpochDayNumber + days)));
#else
        literal = LiteralValue.Of(new DateTimeOffset(UnixEpochTicks + (days * TimeSpan.TicksPerDay), TimeSpan.Zero));
#endif
        return true;
    }

    internal static LiteralValue DateLiteral(long days) =>
        TryDateLiteral(days, out var literal)
            ? literal
            : throw new NotSupportedException(
                $"The date {days} days from the epoch is outside the years 1 to 9999 that .NET's date types hold.");

    internal static LiteralValue TimeLiteral(long micros)
    {
#if NET6_0_OR_GREATER
        return LiteralValue.Of(new TimeOnly(micros * 10));
#else
        return LiteralValue.Of(micros);
#endif
    }

    /// <summary>A microsecond timestamp as a <see cref="DateTimeOffset"/>, or false outside its years 1 to 9999.</summary>
    internal static bool TryMicrosLiteral(long micros, out LiteralValue literal)
    {
        if (micros < MinMicros || micros > MaxMicros)
        {
            literal = default;
            return false;
        }

        literal = LiteralValue.Of(new DateTimeOffset(UnixEpochTicks + (micros * 10), TimeSpan.Zero));
        return true;
    }

    internal static LiteralValue MicrosLiteral(long micros) =>
        TryMicrosLiteral(micros, out var literal)
            ? literal
            : throw new NotSupportedException(
                $"The timestamp {micros} microseconds from the epoch is outside the years 1 to 9999 that DateTimeOffset holds.");

#if NET6_0_OR_GREATER
    internal static readonly int UnixEpochDayNumber = new DateOnly(1970, 1, 1).DayNumber;
#endif

    /// <summary>Days since the epoch of a date value.</summary>
    internal static int ToEpochDays(IcebergType type, LiteralValue value)
    {
        switch (value.Type)
        {
#if NET6_0_OR_GREATER
            case LiteralValue.Kind.DateOnly:
                return value.AsDateOnly.DayNumber - UnixEpochDayNumber;
#endif
            case LiteralValue.Kind.DateTimeOffset:
                long ticks = value.AsDateTimeOffset.UtcTicks - UnixEpochTicks;
                if (FloorMod(ticks, TimeSpan.TicksPerDay) != 0)
                    throw Mismatch(type, value, "it is not a UTC midnight");
                return checked((int)FloorDiv(ticks, TimeSpan.TicksPerDay));
        }

        throw Mismatch(type, value, null);
    }

    /// <summary>Microseconds since midnight of a time value.</summary>
    internal static long ToTimeMicros(IcebergType type, LiteralValue value)
    {
        switch (value.Type)
        {
#if NET6_0_OR_GREATER
            case LiteralValue.Kind.TimeOnly:
                long ticks = value.AsTimeOnly.Ticks;
                if (ticks % 10 != 0)
                    throw Mismatch(type, value, "it is finer than a microsecond");
                return ticks / 10;
#endif
            case LiteralValue.Kind.Int64:
                long micros = value.AsInt64;
                if (micros < 0 || micros >= MicrosPerDay)
                    throw Mismatch(type, value, "it is not a time of day in microseconds");
                return micros;
        }

        throw Mismatch(type, value, null);
    }

    /// <summary>Microseconds since the epoch of a timestamp value.</summary>
    internal static long ToEpochMicros(IcebergType type, LiteralValue value)
    {
        switch (value.Type)
        {
            case LiteralValue.Kind.DateTimeOffset:
                long ticks = value.AsDateTimeOffset.UtcTicks - UnixEpochTicks;
                if (FloorMod(ticks, 10) != 0)
                    throw Mismatch(type, value, "it is finer than a microsecond");
                return FloorDiv(ticks, 10);
            case LiteralValue.Kind.TimestampNanos:
                long nanos = value.AsTimestampNanos;
                if (FloorMod(nanos, 1000) != 0)
                    throw Mismatch(type, value, "it is finer than a microsecond");
                return FloorDiv(nanos, 1000);
#if NET6_0_OR_GREATER
            case LiteralValue.Kind.DateOnly:
                return checked((long)(value.AsDateOnly.DayNumber - UnixEpochDayNumber) * MicrosPerDay);
#endif
        }

        throw Mismatch(type, value, null);
    }

    /// <summary>Nanoseconds since the epoch of a nanosecond timestamp value.</summary>
    internal static long ToEpochNanos(IcebergType type, LiteralValue value)
    {
        try
        {
            switch (value.Type)
            {
                case LiteralValue.Kind.TimestampNanos:
                    return value.AsTimestampNanos;
                case LiteralValue.Kind.DateTimeOffset:
                    return checked((value.AsDateTimeOffset.UtcTicks - UnixEpochTicks) * 100);
#if NET6_0_OR_GREATER
                case LiteralValue.Kind.DateOnly:
                    return checked((long)(value.AsDateOnly.DayNumber - UnixEpochDayNumber) * NanosPerDay);
#endif
            }
        }
        catch (OverflowException)
        {
            throw Mismatch(type, value, "it is outside the years 1677 to 2262 a nanosecond timestamp holds");
        }

        throw Mismatch(type, value, null);
    }

    // ── Decimals ──

    private static LiteralValue CoerceDecimal(DecimalType type, LiteralValue value)
    {
        BigInteger unscaled;
        int scale;
        switch (value.Type)
        {
            case LiteralValue.Kind.HighPrecisionDecimal:
                (unscaled, scale) = value.AsHighPrecisionDecimal;
                break;
            case LiteralValue.Kind.Decimal:
                (unscaled, scale) = FromDecimal(value.AsDecimal);
                break;
            case LiteralValue.Kind.Int32:
                (unscaled, scale) = (value.AsInt32, 0);
                break;
            case LiteralValue.Kind.Int64:
                (unscaled, scale) = (value.AsInt64, 0);
                break;
            default:
                throw Mismatch(type, value, null);
        }

        // Zero is zero at every scale, however far apart the scales are.
        if (unscaled.IsZero)
            return LiteralValue.HighPrecisionDecimalOf(BigInteger.Zero, type.Scale);

        // Every power below is bounded by the value itself, so a scale from a file's metadata cannot ask for a
        // number millions of digits long (#760): scaling up must stay within the precision, and scaling down
        // is exact only if the value has at least as many digits as the scales are apart.
        long digits = DigitCount(unscaled);
        if (scale < type.Scale)
        {
            if (digits + ((long)type.Scale - scale) > type.Precision)
                throw Mismatch(type, value, $"it has more digits than precision {type.Precision}");
            unscaled *= BigInteger.Pow(10, type.Scale - scale);
        }
        else if (scale > type.Scale)
        {
            if ((long)scale - type.Scale > digits)
                throw Mismatch(type, value, $"it does not reach scale {type.Scale} exactly");
            var divisor = BigInteger.Pow(10, scale - type.Scale);
            var quotient = BigInteger.DivRem(unscaled, divisor, out var remainder);
            if (!remainder.IsZero)
                throw Mismatch(type, value, $"it does not reach scale {type.Scale} exactly");
            unscaled = quotient;
        }

        if (!FitsPrecision(unscaled, type.Precision))
            throw Mismatch(type, value, $"it has more digits than precision {type.Precision}");

        return LiteralValue.HighPrecisionDecimalOf(unscaled, type.Scale);
    }

    private const int MaxDecimalDigits = 38;

    /// <summary>The number of decimal digits in a non-zero value's magnitude, without formatting it.</summary>
    private static long DigitCount(BigInteger value)
    {
        var magnitude = BigInteger.Abs(value);
        // log10 estimates the count to within one; settle it against the power it implies.
        long estimate = (long)Math.Floor(BigInteger.Log10(magnitude)) + 1;
        if (estimate > 1 && magnitude < BigInteger.Pow(10, (int)estimate - 1))
            return estimate - 1;
        if (magnitude >= BigInteger.Pow(10, (int)estimate))
            return estimate + 1;
        return estimate;
    }

    private static readonly BigInteger[] s_powersOfTen = BuildPowersOfTen();

    private static BigInteger[] BuildPowersOfTen()
    {
        var powers = new BigInteger[MaxDecimalDigits + 2];
        powers[0] = BigInteger.One;
        for (int i = 1; i < powers.Length; i++)
            powers[i] = powers[i - 1] * 10;
        return powers;
    }

    /// <summary>Whether an unscaled value has at most <paramref name="precision"/> digits.</summary>
    internal static bool FitsPrecision(BigInteger unscaled, int precision)
    {
        if (precision <= 0)
            return false;
        var magnitude = BigInteger.Abs(unscaled);
        return precision < s_powersOfTen.Length
            ? magnitude < s_powersOfTen[precision]
            : magnitude < BigInteger.Pow(10, precision);
    }

    private static (BigInteger Unscaled, int Scale) FromDecimal(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int scale = (bits[3] >> 16) & 0x7F;
        bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;
        var magnitude = (new BigInteger((uint)bits[2]) << 64)
            | (new BigInteger((uint)bits[1]) << 32)
            | new BigInteger((uint)bits[0]);
        return (negative ? -magnitude : magnitude, scale);
    }

    // ── Arithmetic ──

    internal static long FloorDiv(long value, long divisor)
    {
        long quotient = value / divisor;
        return value % divisor < 0 ? quotient - 1 : quotient;
    }

    internal static long FloorMod(long value, long divisor)
    {
        long remainder = value % divisor;
        return remainder < 0 ? remainder + divisor : remainder;
    }

    internal static NotSupportedException Unsupported(IcebergType type) =>
        new($"Iceberg type {TypeName(type)} has no single-value serialization.");

    private static ArgumentException Mismatch(IcebergType type, LiteralValue value, string? why) =>
        new(why is null
            ? $"A {value.Type} literal is not a value of Iceberg type {TypeName(type)}."
            : $"The {value.Type} literal {value} is not a value of Iceberg type {TypeName(type)}: {why}.",
            nameof(value));

    internal static string TypeName(IcebergType type) => type switch
    {
        BooleanType => "boolean",
        IntegerType => "int",
        LongType => "long",
        FloatType => "float",
        DoubleType => "double",
        DecimalType d => $"decimal({d.Precision}, {d.Scale})",
        DateType => "date",
        TimeType => "time",
        TimestampType => "timestamp",
        TimestamptzType => "timestamptz",
        TimestampNsType => "timestamp_ns",
        TimestamptzNsType => "timestamptz_ns",
        StringType => "string",
        UuidType => "uuid",
        FixedType f => $"fixed[{f.Length}]",
        BinaryType => "binary",
        UnknownType => "unknown",
        VariantType => "variant",
        GeometryType => "geometry",
        GeographyType => "geography",
        StructType => "struct",
        ListType => "list",
        MapType => "map",
        _ => type.GetType().Name,
    };
}
