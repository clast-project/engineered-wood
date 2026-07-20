// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Runtime.InteropServices;

namespace EngineeredWood.Expressions;

/// <summary>
/// A typed scalar value used in expressions. Supports cross-type numeric
/// promotion for comparisons (e.g. <c>int</c> vs <c>long</c>, <c>float</c> vs
/// <c>double</c>) and equality.
/// </summary>
/// <remarks>
/// Implemented as a value type to avoid boxing common scalars. Reference-typed
/// values (string, byte[], BigInteger) and types larger than 8 bytes are stored
/// in an object slot; primitive scalars are stored in an inline 16-byte union.
/// </remarks>
public readonly struct LiteralValue : IEquatable<LiteralValue>, IComparable<LiteralValue>
{
    /// <summary>The underlying logical type carried by this literal.</summary>
    public enum Kind : byte
    {
        Null,
        Boolean,
        Int32,
        Int64,
        UInt32,
        UInt64,
        Float,
        Double,
        Half,
        Decimal,
        HighPrecisionDecimal,
        String,
        Binary,
        DateOnly,
        TimeOnly,
        DateTimeOffset,
        Guid,
    }

    private readonly Kind _kind;
    private readonly InlineStorage _inline;
    private readonly object? _ref;

    [StructLayout(LayoutKind.Explicit)]
    private struct InlineStorage
    {
        [FieldOffset(0)] public bool Boolean;
        [FieldOffset(0)] public int Int32;
        [FieldOffset(0)] public long Int64;
        [FieldOffset(0)] public uint UInt32;
        [FieldOffset(0)] public ulong UInt64;
        [FieldOffset(0)] public float Float;
        [FieldOffset(0)] public double Double;
#if NET6_0_OR_GREATER
        [FieldOffset(0)] public Half Half;
        [FieldOffset(0)] public DateOnly DateOnly;
        [FieldOffset(0)] public TimeOnly TimeOnly;
#else
        [FieldOffset(0)] public ushort HalfBits;
        [FieldOffset(0)] public int DateOnlyDayNumber;
        [FieldOffset(0)] public long TimeOnlyTicks;
#endif
        [FieldOffset(0)] public long DateTimeOffsetTicks;
        [FieldOffset(8)] public short DateTimeOffsetMinutes;
    }

    private LiteralValue(Kind kind, InlineStorage inline, object? reference)
    {
        _kind = kind;
        _inline = inline;
        _ref = reference;
    }

    /// <summary>The logical type of this value.</summary>
    public Kind Type => _kind;

    /// <summary>Returns true if this value is null.</summary>
    public bool IsNull => _kind == Kind.Null;

    /// <summary>The null literal.</summary>
    public static LiteralValue Null { get; } = default;

    // ── Factory methods ──

    public static LiteralValue Of(bool value) => new(Kind.Boolean, new InlineStorage { Boolean = value }, null);
    public static LiteralValue Of(int value) => new(Kind.Int32, new InlineStorage { Int32 = value }, null);
    public static LiteralValue Of(long value) => new(Kind.Int64, new InlineStorage { Int64 = value }, null);
    public static LiteralValue Of(uint value) => new(Kind.UInt32, new InlineStorage { UInt32 = value }, null);
    public static LiteralValue Of(ulong value) => new(Kind.UInt64, new InlineStorage { UInt64 = value }, null);
    public static LiteralValue Of(float value) => new(Kind.Float, new InlineStorage { Float = value }, null);
    public static LiteralValue Of(double value) => new(Kind.Double, new InlineStorage { Double = value }, null);
    public static LiteralValue Of(decimal value) => new(Kind.Decimal, default, value);
    public static LiteralValue Of(string value) => new(Kind.String, default, value);
    public static LiteralValue Of(byte[] value) => new(Kind.Binary, default, value);
    public static LiteralValue Of(Guid value) => new(Kind.Guid, default, value);

#if NET6_0_OR_GREATER
    public static LiteralValue Of(Half value) => new(Kind.Half, new InlineStorage { Half = value }, null);
    public static LiteralValue Of(DateOnly value) => new(Kind.DateOnly, new InlineStorage { DateOnly = value }, null);
    public static LiteralValue Of(TimeOnly value) => new(Kind.TimeOnly, new InlineStorage { TimeOnly = value }, null);
#endif

    public static LiteralValue Of(DateTimeOffset value) => new(
        Kind.DateTimeOffset,
        new InlineStorage
        {
            DateTimeOffsetTicks = value.Ticks,
            DateTimeOffsetMinutes = (short)(value.Offset.Ticks / TimeSpan.TicksPerMinute),
        },
        null);

    /// <summary>
    /// Creates a high-precision decimal value for Decimal128/256 columns whose
    /// precision exceeds System.decimal's 28-29 digit limit.
    /// </summary>
    public static LiteralValue HighPrecisionDecimalOf(BigInteger unscaledValue, int scale) =>
        new(Kind.HighPrecisionDecimal, new InlineStorage { Int32 = scale }, unscaledValue);

    // ── Implicit conversions ──

    public static implicit operator LiteralValue(bool value) => Of(value);
    public static implicit operator LiteralValue(int value) => Of(value);
    public static implicit operator LiteralValue(long value) => Of(value);
    public static implicit operator LiteralValue(uint value) => Of(value);
    public static implicit operator LiteralValue(ulong value) => Of(value);
    public static implicit operator LiteralValue(float value) => Of(value);
    public static implicit operator LiteralValue(double value) => Of(value);
    public static implicit operator LiteralValue(decimal value) => Of(value);
    public static implicit operator LiteralValue(string value) => Of(value);
    public static implicit operator LiteralValue(byte[] value) => Of(value);
    public static implicit operator LiteralValue(Guid value) => Of(value);
    public static implicit operator LiteralValue(DateTimeOffset value) => Of(value);
#if NET6_0_OR_GREATER
    public static implicit operator LiteralValue(Half value) => Of(value);
    public static implicit operator LiteralValue(DateOnly value) => Of(value);
    public static implicit operator LiteralValue(TimeOnly value) => Of(value);
#endif

    // ── Accessors ──

    public bool AsBoolean => _kind == Kind.Boolean
        ? _inline.Boolean
        : throw InvalidAccess(Kind.Boolean);

    public int AsInt32 => _kind == Kind.Int32
        ? _inline.Int32
        : throw InvalidAccess(Kind.Int32);

    public long AsInt64 => _kind == Kind.Int64
        ? _inline.Int64
        : throw InvalidAccess(Kind.Int64);

    public uint AsUInt32 => _kind == Kind.UInt32
        ? _inline.UInt32
        : throw InvalidAccess(Kind.UInt32);

    public ulong AsUInt64 => _kind == Kind.UInt64
        ? _inline.UInt64
        : throw InvalidAccess(Kind.UInt64);

    public float AsFloat => _kind == Kind.Float
        ? _inline.Float
        : throw InvalidAccess(Kind.Float);

    public double AsDouble => _kind == Kind.Double
        ? _inline.Double
        : throw InvalidAccess(Kind.Double);

    public decimal AsDecimal => _kind == Kind.Decimal
        ? (decimal)_ref!
        : throw InvalidAccess(Kind.Decimal);

    public string AsString => _kind == Kind.String
        ? (string)_ref!
        : throw InvalidAccess(Kind.String);

    public byte[] AsBinary => _kind == Kind.Binary
        ? (byte[])_ref!
        : throw InvalidAccess(Kind.Binary);

    public Guid AsGuid => _kind == Kind.Guid
        ? (Guid)_ref!
        : throw InvalidAccess(Kind.Guid);

    public DateTimeOffset AsDateTimeOffset => _kind == Kind.DateTimeOffset
        ? new DateTimeOffset(_inline.DateTimeOffsetTicks,
            TimeSpan.FromMinutes(_inline.DateTimeOffsetMinutes))
        : throw InvalidAccess(Kind.DateTimeOffset);

#if NET6_0_OR_GREATER
    public Half AsHalf => _kind == Kind.Half
        ? _inline.Half
        : throw InvalidAccess(Kind.Half);

    public DateOnly AsDateOnly => _kind == Kind.DateOnly
        ? _inline.DateOnly
        : throw InvalidAccess(Kind.DateOnly);

    public TimeOnly AsTimeOnly => _kind == Kind.TimeOnly
        ? _inline.TimeOnly
        : throw InvalidAccess(Kind.TimeOnly);
#endif

    public (BigInteger UnscaledValue, int Scale) AsHighPrecisionDecimal => _kind == Kind.HighPrecisionDecimal
        ? ((BigInteger)_ref!, _inline.Int32)
        : throw InvalidAccess(Kind.HighPrecisionDecimal);

    /// <summary>
    /// Returns the underlying value boxed as <c>object</c>. Avoid in hot paths;
    /// prefer the typed accessors.
    /// </summary>
    public object? ToObject() => _kind switch
    {
        Kind.Null => null,
        Kind.Boolean => _inline.Boolean,
        Kind.Int32 => _inline.Int32,
        Kind.Int64 => _inline.Int64,
        Kind.UInt32 => _inline.UInt32,
        Kind.UInt64 => _inline.UInt64,
        Kind.Float => _inline.Float,
        Kind.Double => _inline.Double,
        Kind.Decimal => _ref,
        Kind.String => _ref,
        Kind.Binary => _ref,
        Kind.Guid => _ref,
        Kind.DateTimeOffset => AsDateTimeOffset,
#if NET6_0_OR_GREATER
        Kind.Half => _inline.Half,
        Kind.DateOnly => _inline.DateOnly,
        Kind.TimeOnly => _inline.TimeOnly,
#endif
        Kind.HighPrecisionDecimal => AsHighPrecisionDecimal,
        _ => throw new InvalidOperationException($"Unknown LiteralValue kind: {_kind}"),
    };

    private InvalidOperationException InvalidAccess(Kind expected) =>
        new($"Cannot read LiteralValue of kind {_kind} as {expected}.");

    // ── Equality ──

    public bool Equals(LiteralValue other)
    {
        if (_kind != other._kind)
            return CompareCrossType(this, other) == 0;

        return _kind switch
        {
            Kind.Null => true,
            Kind.Boolean => _inline.Boolean == other._inline.Boolean,
            Kind.Int32 => _inline.Int32 == other._inline.Int32,
            Kind.Int64 => _inline.Int64 == other._inline.Int64,
            Kind.UInt32 => _inline.UInt32 == other._inline.UInt32,
            Kind.UInt64 => _inline.UInt64 == other._inline.UInt64,
            Kind.Float => _inline.Float.Equals(other._inline.Float),
            Kind.Double => _inline.Double.Equals(other._inline.Double),
            Kind.Decimal => Equals(_ref, other._ref),
            Kind.String => string.Equals((string?)_ref, (string?)other._ref, StringComparison.Ordinal),
            Kind.Binary => BinaryEquals((byte[]?)_ref, (byte[]?)other._ref),
            Kind.Guid => Equals(_ref, other._ref),
            Kind.DateTimeOffset =>
                _inline.DateTimeOffsetTicks == other._inline.DateTimeOffsetTicks &&
                _inline.DateTimeOffsetMinutes == other._inline.DateTimeOffsetMinutes,
#if NET6_0_OR_GREATER
            Kind.Half => _inline.Half.Equals(other._inline.Half),
            Kind.DateOnly => _inline.DateOnly.Equals(other._inline.DateOnly),
            Kind.TimeOnly => _inline.TimeOnly.Equals(other._inline.TimeOnly),
#endif
            Kind.HighPrecisionDecimal =>
                _inline.Int32 == other._inline.Int32 &&
                Equals(_ref, other._ref),
            _ => false,
        };
    }

    public override bool Equals(object? obj) => obj is LiteralValue other && Equals(other);

    public override int GetHashCode() => _kind switch
    {
        Kind.Null => 0,
        Kind.Boolean => _inline.Boolean.GetHashCode(),
        Kind.Int32 => _inline.Int32.GetHashCode(),
        Kind.Int64 => _inline.Int64.GetHashCode(),
        Kind.UInt32 => _inline.UInt32.GetHashCode(),
        Kind.UInt64 => _inline.UInt64.GetHashCode(),
        Kind.Float => _inline.Float.GetHashCode(),
        Kind.Double => _inline.Double.GetHashCode(),
        Kind.DateTimeOffset => CombineHash(_inline.DateTimeOffsetTicks.GetHashCode(), _inline.DateTimeOffsetMinutes.GetHashCode()),
#if NET6_0_OR_GREATER
        Kind.Half => _inline.Half.GetHashCode(),
        Kind.DateOnly => _inline.DateOnly.GetHashCode(),
        Kind.TimeOnly => _inline.TimeOnly.GetHashCode(),
#endif
        Kind.Binary => BinaryHashCode((byte[]?)_ref),
        Kind.HighPrecisionDecimal => CombineHash(_inline.Int32.GetHashCode(), _ref?.GetHashCode() ?? 0),
        _ => _ref?.GetHashCode() ?? 0,
    };

    public static bool operator ==(LiteralValue left, LiteralValue right) => left.Equals(right);
    public static bool operator !=(LiteralValue left, LiteralValue right) => !left.Equals(right);

    // ── Comparison ──

    /// <summary>
    /// Compares two literal values, supporting cross-type numeric promotion
    /// (int vs long, float vs double, etc.). Null sorts before any non-null
    /// value. Throws <see cref="InvalidOperationException"/> if the kinds
    /// cannot be meaningfully compared.
    /// </summary>
    public int CompareTo(LiteralValue other)
    {
        if (_kind == Kind.Null)
            return other._kind == Kind.Null ? 0 : -1;
        if (other._kind == Kind.Null)
            return 1;

        if (_kind == other._kind)
        {
            return _kind switch
            {
                Kind.Boolean => _inline.Boolean.CompareTo(other._inline.Boolean),
                Kind.Int32 => _inline.Int32.CompareTo(other._inline.Int32),
                Kind.Int64 => _inline.Int64.CompareTo(other._inline.Int64),
                Kind.UInt32 => _inline.UInt32.CompareTo(other._inline.UInt32),
                Kind.UInt64 => _inline.UInt64.CompareTo(other._inline.UInt64),
                Kind.Float => _inline.Float.CompareTo(other._inline.Float),
                Kind.Double => _inline.Double.CompareTo(other._inline.Double),
                Kind.Decimal => ((decimal)_ref!).CompareTo((decimal)other._ref!),
                Kind.String => string.CompareOrdinal((string?)_ref, (string?)other._ref),
                Kind.Binary => BinaryCompare((byte[]?)_ref, (byte[]?)other._ref),
                Kind.Guid => ((Guid)_ref!).CompareTo((Guid)other._ref!),
                Kind.DateTimeOffset => AsDateTimeOffset.CompareTo(other.AsDateTimeOffset),
#if NET6_0_OR_GREATER
                Kind.Half => _inline.Half.CompareTo(other._inline.Half),
                Kind.DateOnly => _inline.DateOnly.CompareTo(other._inline.DateOnly),
                Kind.TimeOnly => _inline.TimeOnly.CompareTo(other._inline.TimeOnly),
#endif
                Kind.HighPrecisionDecimal => CompareHighPrecisionDecimal(this, other),
                _ => throw new InvalidOperationException($"Cannot compare {_kind} values."),
            };
        }

        return CompareCrossType(this, other);
    }

    private static int CompareCrossType(LiteralValue a, LiteralValue b)
    {
        // Integer ↔ integer widening
        if (TryAsInt64(a, out long ai) && TryAsInt64(b, out long bi))
            return ai.CompareTo(bi);

        // Float widening (any numeric → double)
        if (TryAsDouble(a, out double ad) && TryAsDouble(b, out double bd))
            return ad.CompareTo(bd);

        // Exact decimal comparison across Decimal / HighPrecisionDecimal (and integers), so a plain
        // decimal literal compares against a high-precision decimal column value, and vice versa, without
        // going through lossy double. Two same-kind values never reach here (handled above); this path is
        // for the mixed pairs. Float/double are deliberately excluded — that would be a lossy compare.
        if (TryAsScaledInteger(a, out var au, out int asc) && TryAsScaledInteger(b, out var bu, out int bsc))
            return CompareScaledIntegers(au, asc, bu, bsc);

#if NET6_0_OR_GREATER
        // A calendar date (DateOnly) compares against an instant (DateTimeOffset) as UTC midnight —
        // consistent with how date columns are surfaced as UTC-midnight DateTimeOffset values.
        if (TryAsInstant(a, out var ax) && TryAsInstant(b, out var bx))
            return ax.CompareTo(bx);
#endif

        throw new InvalidOperationException(
            $"Cannot compare LiteralValue of kind {a._kind} with kind {b._kind}.");
    }

    // Views a value as an exact (unscaledValue × 10^-scale) integer: integers (scale 0), System.Decimal,
    // or a high-precision decimal. Excludes float/double (inexact) and everything non-numeric.
    private static bool TryAsScaledInteger(LiteralValue v, out BigInteger unscaled, out int scale)
    {
        switch (v._kind)
        {
            case Kind.Int32: unscaled = v._inline.Int32; scale = 0; return true;
            case Kind.Int64: unscaled = v._inline.Int64; scale = 0; return true;
            case Kind.UInt32: unscaled = v._inline.UInt32; scale = 0; return true;
            case Kind.UInt64: unscaled = v._inline.UInt64; scale = 0; return true;
            case Kind.Decimal: (unscaled, scale) = DecimalToUnscaled((decimal)v._ref!); return true;
            case Kind.HighPrecisionDecimal: unscaled = (BigInteger)v._ref!; scale = v._inline.Int32; return true;
        }
        unscaled = default;
        scale = 0;
        return false;
    }

    private static (BigInteger Unscaled, int Scale) DecimalToUnscaled(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int scale = (bits[3] >> 16) & 0x7F;
        bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;
        var magnitude = (new BigInteger((uint)bits[2]) << 64)
            | (new BigInteger((uint)bits[1]) << 32)
            | new BigInteger((uint)bits[0]);
        return (negative ? -magnitude : magnitude, scale);
    }

    private static int CompareScaledIntegers(BigInteger au, int ascale, BigInteger bu, int bscale)
    {
        if (ascale == bscale)
            return au.CompareTo(bu);

        int diff = ascale - bscale;
        if (diff > 0)
            bu *= BigInteger.Pow(10, diff);
        else
            au *= BigInteger.Pow(10, -diff);

        return au.CompareTo(bu);
    }

#if NET6_0_OR_GREATER
    private static bool TryAsInstant(LiteralValue v, out DateTimeOffset instant)
    {
        switch (v._kind)
        {
            case Kind.DateTimeOffset:
                instant = v.AsDateTimeOffset;
                return true;
            case Kind.DateOnly:
                var d = v._inline.DateOnly;
                instant = new DateTimeOffset(d.Year, d.Month, d.Day, 0, 0, 0, TimeSpan.Zero);
                return true;
        }
        instant = default;
        return false;
    }
#endif

    private static bool TryAsInt64(LiteralValue v, out long result)
    {
        switch (v._kind)
        {
            case Kind.Int32: result = v._inline.Int32; return true;
            case Kind.Int64: result = v._inline.Int64; return true;
            case Kind.UInt32: result = v._inline.UInt32; return true;
            case Kind.UInt64:
                if (v._inline.UInt64 <= long.MaxValue)
                {
                    result = (long)v._inline.UInt64;
                    return true;
                }
                break;
        }
        result = 0;
        return false;
    }

    private static bool TryAsDouble(LiteralValue v, out double result)
    {
        switch (v._kind)
        {
            case Kind.Int32: result = v._inline.Int32; return true;
            case Kind.Int64: result = v._inline.Int64; return true;
            case Kind.UInt32: result = v._inline.UInt32; return true;
            case Kind.UInt64: result = v._inline.UInt64; return true;
            case Kind.Float: result = v._inline.Float; return true;
            case Kind.Double: result = v._inline.Double; return true;
#if NET6_0_OR_GREATER
            case Kind.Half: result = (double)v._inline.Half; return true;
#endif
        }
        result = 0;
        return false;
    }

    private static int CompareHighPrecisionDecimal(LiteralValue a, LiteralValue b)
    {
        var (au, ascale) = a.AsHighPrecisionDecimal;
        var (bu, bscale) = b.AsHighPrecisionDecimal;
        return CompareScaledIntegers(au, ascale, bu, bscale);
    }

    public static bool operator <(LiteralValue left, LiteralValue right) => left.CompareTo(right) < 0;
    public static bool operator >(LiteralValue left, LiteralValue right) => left.CompareTo(right) > 0;
    public static bool operator <=(LiteralValue left, LiteralValue right) => left.CompareTo(right) <= 0;
    public static bool operator >=(LiteralValue left, LiteralValue right) => left.CompareTo(right) >= 0;

    private static bool BinaryEquals(byte[]? a, byte[]? b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null || b is null) return false;
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
            if (a[i] != b[i]) return false;
        return true;
    }

    private static int BinaryCompare(byte[]? a, byte[]? b)
    {
        if (ReferenceEquals(a, b)) return 0;
        if (a is null) return -1;
        if (b is null) return 1;
        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            int c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }
        return a.Length.CompareTo(b.Length);
    }

    private static int CombineHash(int a, int b)
    {
        unchecked
        {
            return ((a << 5) + a) ^ b;
        }
    }

    private static int BinaryHashCode(byte[]? a)
    {
        if (a is null) return 0;
        // Lightweight hash: FNV-1a 32-bit
        unchecked
        {
            uint hash = 2166136261u;
            for (int i = 0; i < a.Length; i++)
            {
                hash ^= a[i];
                hash *= 16777619u;
            }
            return (int)hash;
        }
    }

    public override string ToString() => _kind == Kind.Null ? "null" : ToObject()?.ToString() ?? "";
}
