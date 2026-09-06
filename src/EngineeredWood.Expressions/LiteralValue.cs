// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Runtime.InteropServices;

namespace EngineeredWood.Expressions;

/// <summary>
/// A typed scalar value used in expressions.
/// </summary>
/// <remarks>
/// Implemented as a value type to avoid boxing common scalars. Reference-typed
/// values (string, byte[], BigInteger) and types larger than 8 bytes are stored
/// in an object slot; primitive scalars are stored in an inline 16-byte union.
/// <para>
/// Carries TWO relations, and they are not the same one.
/// <see cref="CompareTo(LiteralValue)"/> is SQL's, with cross-type numeric promotion
/// (<c>int</c> against <c>long</c>, <c>decimal</c> against <c>double</c>) measured against Spark.
/// <see cref="Equals(LiteralValue)"/> is .NET's, comparing representation, so it is an
/// equivalence relation with a consistent <see cref="GetHashCode"/>. Ask the first when
/// evaluating a predicate; ask the second when using a literal as a key or comparing two
/// expression trees.
/// </para>
/// <para>
/// WARNING: <see cref="CompareTo(LiteralValue)"/> is not a total order, so this type must not go
/// into a sorted collection whose elements span kinds -- <see cref="SortedSet{T}"/>,
/// <see cref="SortedDictionary{TKey, TValue}"/>, <c>List.Sort</c>, <c>OrderBy</c>. It implements
/// <see cref="IComparable{T}"/> for the evaluators, which compare two values at a time and can
/// take an answer of "these kinds do not compare"; a sort cannot. Measured, both ways it breaks:
/// a <see cref="SortedSet{T}"/> holding an int THROWS when a string is added, and sorting a list
/// of the two raises "Failed to compare two elements in the array"; and where the comparison does
/// answer, the answers are pairwise, so a set built from the decimal 9007199254740993, the double
/// 9007199254740992 and the long 9007199254740992 -- three values, all distinct under
/// <see cref="Equals(LiteralValue)"/> -- silently DROPS one, and reports Contains for a value it
/// does not hold. How many it drops is not worth stating: a sorted container given an
/// intransitive comparison has no defined behaviour at all, which is the point. Group by
/// <see cref="Type"/> first, or sort with a comparer of your own.
/// </para>
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

    /// <summary>
    /// Whether two literals hold the same value in the same representation.
    /// </summary>
    /// <remarks>
    /// A .NET equivalence relation, deliberately NOT SQL's <c>=</c>. Two values of different
    /// <see cref="Kind"/>s are never equal here, so <c>Of(1)</c> does not equal <c>Of(1.0d)</c>
    /// even though the SQL comparison says they match. For the SQL answer ask
    /// <see cref="CompareTo(LiteralValue)"/> and test for zero, which is what the evaluators do.
    /// <para>
    /// The split exists because the two relations cannot be the same method. SQL's cross-type
    /// comparison is pairwise: which pairs compare equal depends on the types involved, so the
    /// relation is neither transitive nor consistent with any hash. Measured, all three of these
    /// answers match Spark and all three cannot be an <c>Equals</c>: a <c>decimal(20,0)</c>
    /// holding 9007199254740993 compares equal to the double 9007199254740992 (both widen to
    /// double, where 2^53+1 does not exist), that double compares equal to the long
    /// 9007199254740992, and the decimal does not compare equal to that long, because
    /// decimal-against-integer stays exact. Routing that through <c>Equals</c> also made
    /// <c>Of(1)</c> and <c>Of(1.0d)</c> equal while hashing differently, so a hash lookup missed
    /// them, and made <c>Equals</c> THROW for any pair SQL cannot compare at all --
    /// <c>Of(1).Equals(Of("x"))</c> and <c>Null.Equals(Of(1))</c> both raised
    /// <see cref="InvalidOperationException"/>. This method never throws.
    /// </para>
    /// </remarks>
    public bool Equals(LiteralValue other)
    {
        if (_kind != other._kind)
            return false;

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

    /// <summary>A hash consistent with <see cref="Equals(LiteralValue)"/>.</summary>
    /// <remarks>
    /// Representation-based, matching the equality above: no kind is folded into another, because
    /// values of two kinds are never equal. The kind is mixed in for the same reason -- once two
    /// kinds can never be equal, separating them costs nothing and every value they shared a
    /// bucket with was a pure collision.
    /// <para>
    /// Worth doing rather than nominal. Measured over 64 small values in each of the eight numeric
    /// kinds plus the string, binary, date and time kinds -- 596 values, all distinct under
    /// <see cref="Equals(LiteralValue)"/> -- the unmixed hash gave 268 distinct codes with a worst
    /// bucket of ELEVEN: every numeric zero landed on 0, and so did the null literal, <c>false</c>
    /// and midnight. Mixing gives 596 distinct codes and a worst bucket of one.
    /// </para>
    /// <para>
    /// Deliberately NOT <see cref="CombineHash"/>, the file's own helper, which measured WORST of
    /// the four mixes tried: 468 distinct and 256 values still sharing a bucket with another kind.
    /// It is <c>33 * kind ^ hash</c>, and a kind is 0-16, so the XOR barely moves a small hash
    /// and the two operands alias. Spreading the hash first and adding the small discriminator
    /// after does not, which is why the multiply comes first here.
    /// </para>
    /// </remarks>
    public override int GetHashCode()
    {
        int value = _kind switch
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

        return unchecked((value * 31) + (int)_kind);
    }

    public static bool operator ==(LiteralValue left, LiteralValue right) => left.Equals(right);
    public static bool operator !=(LiteralValue left, LiteralValue right) => !left.Equals(right);

    // ── Comparison ──

    /// <summary>
    /// Compares two literal values, supporting cross-type numeric promotion
    /// (int vs long, float vs double, etc.). Null sorts before any non-null
    /// value. Throws <see cref="InvalidOperationException"/> if the kinds
    /// cannot be meaningfully compared.
    /// </summary>
    public int CompareTo(LiteralValue other) => CompareTo(other, out _);

    /// <summary>
    /// Compares, reporting whether the answer required a lossy widening.
    /// </summary>
    /// <param name="exact">
    /// False when the two values had to meet in a type that cannot hold one of them exactly, so
    /// the ordering is the ordering of the ROUNDED values and may not be the ordering of the real
    /// ones. True otherwise, including for every same-type comparison.
    /// </param>
    /// <remarks>
    /// The distinction exists because two different questions share this comparison. Evaluating a
    /// row under a SQL dialect wants the lossy answer — it is what the dialect specifies. Deciding
    /// whether a row group can be SKIPPED may only act on an exact one: a lossy comparison is not
    /// an error, it is a confident wrong answer, and acting on it drops rows that match. See
    /// StatisticsEvaluator, whose contract is that callers must not skip data on Unknown.
    /// </remarks>
    public int CompareTo(LiteralValue other, out bool exact)
    {
        exact = true;
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
                Kind.Float => CompareFloating(_inline.Float, other._inline.Float),
                Kind.Double => CompareFloating(_inline.Double, other._inline.Double),
                Kind.Decimal => ((decimal)_ref!).CompareTo((decimal)other._ref!),
                // Code point (== UTF-8 byte) order, NOT UTF-16 code-unit order — every format's
                // string stats are ordered over UTF-8 bytes. See StringOrdering.
                Kind.String => StringOrdering.Compare((string?)_ref, (string?)other._ref),
                Kind.Binary => BinaryCompare((byte[]?)_ref, (byte[]?)other._ref),
                Kind.Guid => ((Guid)_ref!).CompareTo((Guid)other._ref!),
                Kind.DateTimeOffset => AsDateTimeOffset.CompareTo(other.AsDateTimeOffset),
#if NET6_0_OR_GREATER
                Kind.Half => CompareFloating((double)_inline.Half, (double)other._inline.Half),
                Kind.DateOnly => _inline.DateOnly.CompareTo(other._inline.DateOnly),
                Kind.TimeOnly => _inline.TimeOnly.CompareTo(other._inline.TimeOnly),
#endif
                Kind.HighPrecisionDecimal => CompareHighPrecisionDecimal(this, other),
                _ => throw new InvalidOperationException($"Cannot compare {_kind} values."),
            };
        }

        return CompareCrossType(this, other, out exact);
    }

    private static int CompareCrossType(LiteralValue a, LiteralValue b, out bool exact)
    {
        exact = true;
        // Integer ↔ integer widening
        if (TryAsInt64(a, out long ai) && TryAsInt64(b, out long bi))
            return ai.CompareTo(bi);

        // Exact decimal comparison across Decimal / HighPrecisionDecimal (and integers), so a plain
        // decimal literal compares against a high-precision decimal column value, and vice versa, without
        // going through lossy double. Two same-kind values never reach here (handled above); this path is
        // for the mixed pairs. Float/double are deliberately excluded — that would be a lossy compare.
        //
        // ORDERED BEFORE the double widening below, and the order is load-bearing. Both branches
        // can accept a decimal-against-integer pair, and only this one is exact; measured, Spark
        // agrees -- `d1 = 0.1` over decimal(10,2) 0.10 is TRUE, which a double compare also gives,
        // but decimal-against-integer stays exact where a double could not.
        // The floating check comes first so a decimal-against-double row does not pay for
        // TryAsScaledInteger before it declines: that call reaches DecimalToUnscaled, which runs
        // decimal.GetBits and allocates a BigInteger, on the evaluator's per-row path.
        if (!IsFloating(a._kind) && !IsFloating(b._kind)
            && TryAsScaledInteger(a, out var au, out int asc)
            && TryAsScaledInteger(b, out var bu, out int bsc))
            return CompareScaledIntegers(au, asc, bu, bsc);

        // Float widening (any numeric → double), which a decimal reaches only when the other side
        // is floating point and nothing exact is possible.
        //
        // LOSSY ON PURPOSE, because Spark is. Measured: a decimal(20,0) holding 9007199254740993
        // compares EQUAL to the double 9007199254740992, because the decimal goes to double and
        // 2^53+1 is not representable there. Comparing exactly would answer false and disagree.
        if (TryAsDouble(a, out double ad) && TryAsDouble(b, out double bd))
        {
            // The one branch that can lose information, so the one that reports it.
            exact = ExactAsDouble(a) && ExactAsDouble(b);
            return CompareFloating(ad, bd);
        }

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

    /// <summary>Orders two floating values the way SQL does, which is not the way .NET does.</summary>
    /// <remarks>
    /// NaN sits at the TOP of the order: measured against Spark 4.0,
    /// <c>-Infinity &lt; finite &lt; +Infinity &lt; NaN</c>, confirmed by <c>sort_array</c>
    /// returning <c>[-inf, 1.0, inf, nan]</c>. .NET's <see cref="double.CompareTo(double)"/> puts
    /// NaN at the BOTTOM instead, so every relational operator involving one came out inverted —
    /// <c>1.0 &gt; NaN</c> answered true where Spark answers false.
    /// <para>
    /// Two things .NET already agrees on, and this does not disturb either. NaN equals itself:
    /// both return 0 for that pair, and Spark's <c>NaN = NaN</c> is true. And -0.0 equals 0.0,
    /// because <see cref="double.CompareTo(double)"/> compares by value rather than by total
    /// order — verified rather than assumed, since the two differ on exactly this point.
    /// </para>
    /// </remarks>
    private static int CompareFloating(double a, double b)
    {
        bool aNaN = double.IsNaN(a);
        bool bNaN = double.IsNaN(b);
        if (aNaN || bNaN)
            return aNaN && bNaN ? 0 : aNaN ? 1 : -1;

        return a.CompareTo(b);
    }

    /// <summary>Whether this value survives the trip to <see cref="double"/> unchanged.</summary>
    /// <remarks>
    /// Per value, not per type, so ordinary integer pruning keeps working: an <c>int</c> always
    /// fits, and a <c>long</c> fits up to 2^53, which covers almost every real predicate. Only the
    /// values that genuinely cannot round-trip give up their pruning.
    /// <para>
    /// Decimals are reported inexact without inspection. Deciding precisely means asking whether a
    /// value with up to 38 digits lands on a binary fraction, which costs more than the pruning it
    /// would buy — and Unknown here is exactly what this comparison did before #171, when the pair
    /// threw instead, so nothing regresses by saying so.
    /// </para>
    /// </remarks>
    private static bool ExactAsDouble(LiteralValue v) => v._kind switch
    {
        // 2^53 is where consecutive integers stop being representable.
        Kind.Int64 => v._inline.Int64 is >= -9007199254740992L and <= 9007199254740992L,
        Kind.UInt64 => v._inline.UInt64 <= 9007199254740992UL,
        Kind.Decimal or Kind.HighPrecisionDecimal => false,

        // Int32, UInt32, Float, Double and Half all convert without loss.
        _ => true,
    };

    /// <summary>Whether a kind is floating point, and so has no exact form to compare through.</summary>
    private static bool IsFloating(Kind kind) =>
#if NET6_0_OR_GREATER
        kind is Kind.Float or Kind.Double or Kind.Half;
#else
        kind is Kind.Float or Kind.Double;
#endif

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

            // Decimals convert too, so a decimal compares against a float or a double instead of
            // being declared incomparable. Reachable only after the exact branch above has
            // declined, so this never costs precision that was available.
            case Kind.Decimal: result = ScaledDecimal.ToDouble((decimal)v._ref!); return true;
            case Kind.HighPrecisionDecimal:
                // Not through System.Decimal, which the value may exceed: precision 38 runs to
                // about 1e38 against decimal's ~7.9e28 ceiling.
                result = ScaledDecimal.ToDouble((BigInteger)v._ref!, v._inline.Int32);
                return true;
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
