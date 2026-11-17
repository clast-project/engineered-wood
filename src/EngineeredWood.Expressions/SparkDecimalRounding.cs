// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions;

/// <summary>
/// Whether comparing a predicate's literal against a column's stored bound is a comparison Spark
/// would make on rounded values.
/// </summary>
/// <remarks>
/// <para>
/// Spark compares two exact numerics by casting both to their least common type and comparing
/// there. When that type gives up scale, the comparison is of the rounded values, and it can be
/// the opposite of the comparison of the real ones — in the direction that drops rows: pruning
/// says a file cannot match while Spark says it does. For a <c>decimal(38,38)</c> column holding
/// 38 nines, the common type with <c>1</c> is <c>decimal(38,37)</c> and the value rounds up to
/// exactly 1, so <c>d = 1</c>, <c>d &gt;= 1</c> and <c>d &lt; 1</c> are all inverted.
/// </para>
/// <para>
/// This does not reproduce the unification; it answers the narrower question of whether
/// unification would round. That needs much less than the common type does — in particular not
/// the column's declared precision, which no statistics carrier reports
/// (<see cref="IStatisticsAccessor{T}"/> has no way to supply it). The derivation is in
/// <see cref="Rounds"/>, and <c>SparkDecimalRoundingTests</c> checks it against the real
/// unification.
/// </para>
/// </remarks>
internal static class SparkDecimalRounding
{
    /// <summary>Spark's widest decimal, which bounds every declared column type.</summary>
    internal const int MaxPrecision = 38;

    /// <summary>
    /// Whether Spark's least common type for these two would round either side.
    /// </summary>
    /// <param name="literal">The predicate's value, whose digits type it exactly.</param>
    /// <param name="bound">A column's stored minimum or maximum, whose declared width is unknown.</param>
    /// <remarks>
    /// <para>
    /// Unification takes <c>scale = max(ls, cs)</c> and
    /// <c>precision = max(li, ci) + scale</c>, then, when the precision overflows,
    /// keeps the integral digits and lets the scale absorb the whole of it:
    /// <c>scale' = max(scale - (precision - 38), 0)</c>, which reduces to
    /// <c>scale' = max(38 - max(li, ci), 0)</c>. So each side is rounded exactly when that falls
    /// below its own scale.
    /// </para>
    /// <para>
    /// The bound is rounded iff <c>li + cs &gt; 38</c>, and the column's own width does not
    /// enter into it. Rounding the bound needs <c>max(li, ci) &gt; 38 - cs</c>, and
    /// <c>ci</c> can never reach that: a declared <c>decimal(cp, cs)</c> has
    /// <c>ci = cp - cs &lt;= 38 - cs</c>. Only the literal's integral digits can push it over,
    /// and those are known exactly.
    /// </para>
    /// <para>
    /// The literal is rounded iff <c>ci + ls &gt; 38</c>, which does need the column's width, so
    /// it is bounded instead. <c>ci &lt;= 38 - cs</c> makes it possible only when
    /// <c>ls &gt; cs</c>, and that is the one case answered conservatively (see the final check in
    /// the body). An integral column's width is known from its kind, so it is never affected.
    /// </para>
    /// </remarks>
    /// <param name="setMembership">
    /// Whether the literal is a member of an <c>IN</c> set rather than a binary comparison's
    /// right-hand side. It changes only how the literal is widened: a set resolves one type over
    /// all its members before the comparison rule runs, so an integral member counts for its
    /// type's width - <c>ns IN (1, 2)</c> resolves through bigint, which is <c>decimal(20,0)</c> -
    /// where the same literal in <c>ns = 1</c> counts for its digits, <c>decimal(1,0)</c>. Twenty
    /// digits against a high-scale column can force the clamp where one digit does not.
    /// </param>
    internal static bool Rounds(LiteralValue literal, LiteralValue bound, bool setMembership = false)
    {
        // Two integrals share a scale of zero and can never round. Checked first because this
        // runs for every statistics comparison and most of them involve no decimal at all.
        if (!IsExactDecimal(literal) && !IsExactDecimal(bound))
            return false;

        if (!TryDescribeWide(literal, literalTyping: !setMembership, out var literalDigits, out var literalScale)
            || !TryDescribeWide(bound, literalTyping: false, out var boundDigits, out var boundScale))
        {
            // Not a pair Spark unifies as decimals - a float compares as a double, and anything
            // else is not this rule's business. CompareTo's own exactness flag covers those.
            return false;
        }

        // A width of -1 is "declared, and not knowable from the value", which is every decimal
        // that is not a binary comparison's literal. Bounded by the widest a declared
        // decimal(p, scale) can be.
        //
        // All of this is in long: HighPrecisionDecimalOf takes any int scale, and digits minus a
        // scale near int.MinValue, or a width plus one near int.MaxValue, would wrap (#902).
        var literalWidest = literalDigits >= 0 ? literalDigits : MaxPrecision - (long)literalScale;
        var boundWidest = boundDigits >= 0 ? boundDigits : MaxPrecision - (long)boundScale;

        // The bound. Only the literal's integral digits can force its scale down, and for a binary
        // comparison those are known exactly.
        if (literalWidest + boundScale > MaxPrecision)
            return true;

        // The literal cannot be rounded to a coarser scale than the bound's own, so a literal with
        // no more scale than the bound is never touched.
        if (literalScale <= boundScale)
            return false;

        // An integral bound reports its width, so this is exact for it: a bigint is
        // decimal(20,0), which leaves eighteen digits of room before the clamp can bite.
        if (boundWidest + literalScale <= MaxPrecision)
            return false;

        // A decimal bound's declared width is unknown. Refusing here would be sound but would
        // refuse every predicate carrying more decimal places than the column, so ask the
        // narrower question instead: could the rounding change this comparison?
        //
        // Whatever scale the literal is rounded to, it is never coarser than the bound's own (the
        // case where it would be is the one already refused above), so the literal moves by less
        // than half a unit at `boundScale`. If the two values are farther apart than that, every
        // scale the clamp could choose leaves the comparison pointing the same way.
        return !FartherApartThanHalfAUnit(literal, bound);
    }

    /// <summary>Whether this is one of the two kinds Spark unifies as a decimal.</summary>
    private static bool IsExactDecimal(LiteralValue value) =>
        value.Type is LiteralValue.Kind.Decimal or LiteralValue.Kind.HighPrecisionDecimal;

    /// <summary>
    /// Whether the literal and the bound differ by more than half a unit at the bound's scale,
    /// exactly. The literal's scale must be greater than the bound's, which <see cref="Rounds"/>
    /// has established by the time it asks.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Lifting both to the literal's scale would build a power of ten as long as that scale,
    /// which any caller of <c>HighPrecisionDecimalOf</c> chooses (#902). Instead both go to one
    /// place past the bound's scale, where half a unit is exactly 5: the bound by a single
    /// multiplication by ten, and the literal by truncating its extra places toward zero.
    /// </para>
    /// <para>
    /// The truncation cannot change the answer. Its remainder is less than one at the new scale
    /// and has the literal's sign, and the truncated difference <c>D</c> is an integer there, so
    /// <c>|D| &gt;= 6</c> is farther apart and <c>|D| &lt;= 4</c> is not, whatever was dropped.
    /// Only <c>|D| == 5</c> turns on the remainder: farther apart exactly when one was dropped
    /// and it points away from the bound, the way <c>D</c> already does.
    /// </para>
    /// <para>
    /// The one power left divides away the literal's extra places, and is built only when it is
    /// shorter than the literal's own unscaled value - otherwise the truncation is zero and needs
    /// no division - so nothing here is larger than the values themselves.
    /// </para>
    /// </remarks>
    private static bool FartherApartThanHalfAUnit(LiteralValue literal, LiteralValue bound)
    {
        if (!TryUnscaled(literal, out var literalUnscaled, out var literalScale)
            || !TryUnscaled(bound, out var boundUnscaled, out var boundScale))
        {
            return false;
        }

        // Places to drop from the literal to reach one past the bound's scale; at least zero
        // because the literal's scale is past the bound's. In long: two int scales can be
        // further apart than an int holds.
        var drop = (long)literalScale - boundScale - 1;

        BigInteger truncated, remainder;
        if (drop == 0)
        {
            truncated = literalUnscaled;
            remainder = BigInteger.Zero;
        }
        else if (Digits(literalUnscaled) <= drop)
        {
            // The literal is smaller than one unit at the new scale, so all of it is dropped.
            truncated = BigInteger.Zero;
            remainder = literalUnscaled;
        }
        else
        {
            // drop < Digits, so the power is shorter than the literal and drop fits an int.
            // DivRem truncates toward zero and gives the remainder the dividend's sign.
            truncated = BigInteger.DivRem(literalUnscaled, BigInteger.Pow(10, (int)drop), out remainder);
        }

        var difference = truncated - (boundUnscaled * 10);
        var distance = BigInteger.Abs(difference);

        if (distance != 5)
            return distance > 5;

        return !remainder.IsZero && remainder.Sign == difference.Sign;
    }

    /// <summary>The value as an unscaled integer and a scale, for whichever exact kind it is.</summary>
    private static bool TryUnscaled(LiteralValue value, out BigInteger unscaled, out int scale)
    {
        scale = 0;

        switch (value.Type)
        {
            case LiteralValue.Kind.Int32:
                unscaled = new BigInteger(value.AsInt32);
                return true;
            case LiteralValue.Kind.Int64:
                unscaled = new BigInteger(value.AsInt64);
                return true;
            case LiteralValue.Kind.UInt32:
                unscaled = new BigInteger(value.AsUInt32);
                return true;
            case LiteralValue.Kind.UInt64:
                unscaled = new BigInteger(value.AsUInt64);
                return true;
            case LiteralValue.Kind.Decimal:
            {
                var d = value.AsDecimal;
                scale = DecimalScale(d);
                unscaled = new BigInteger(d * Pow10Decimal(scale));
                return true;
            }

            case LiteralValue.Kind.HighPrecisionDecimal:
            {
                var (u, s) = value.AsHighPrecisionDecimal;
                scale = s;
                unscaled = u;
                return true;
            }

            default:
                unscaled = BigInteger.Zero;
                return false;
        }
    }

    /// <summary>Ten to the scale, as a <c>decimal</c>, which holds every scale a decimal can have.</summary>
    private static decimal Pow10Decimal(int scale)
    {
        var power = 1m;
        for (var i = 0; i < scale; i++) power *= 10m;

        return power;
    }

    /// <summary>
    /// The integral digits and scale Spark would type this value at, or false if it is not exact.
    /// </summary>
    /// <param name="literalTyping">
    /// How to widen an integral. A literal takes the digits it is written with — <c>1</c> is
    /// <c>decimal(1,0)</c> — while a column takes its type's full width, so a <c>bigint</c> is
    /// <c>decimal(20,0)</c> however small the value in it.
    /// </param>
    /// <param name="digits">
    /// Integral digits, or -1 when the value is a decimal whose declared width is not knowable
    /// from the value alone.
    /// </param>
    /// <remarks>
    /// An integral digit count past what an int holds - only reachable from a scale near
    /// <c>int.MinValue</c> - saturates at <c>int.MaxValue</c>. <see cref="Rounds"/> reads the
    /// exact count instead.
    /// </remarks>
    internal static bool TryDescribe(LiteralValue value, bool literalTyping, out int digits, out int scale)
    {
        var described = TryDescribeWide(value, literalTyping, out var wide, out scale);
        digits = (int)Math.Min(wide, int.MaxValue);
        return described;
    }

    /// <summary><see cref="TryDescribe"/>, with the integral digits in a long so they cannot wrap.</summary>
    private static bool TryDescribeWide(LiteralValue value, bool literalTyping, out long digits, out int scale)
    {
        digits = 0;
        scale = 0;

        switch (value.Type)
        {
            case LiteralValue.Kind.Int32:
            case LiteralValue.Kind.Int64:
            case LiteralValue.Kind.UInt32:
            case LiteralValue.Kind.UInt64:
                digits = literalTyping ? IntegralDigits(value) : IntegralWidth(value.Type);
                return true;

            case LiteralValue.Kind.Decimal:
            {
                var d = value.AsDecimal;
                scale = DecimalScale(d);
                // From the unscaled mantissa, as the high-precision branch does. Counting the
                // truncated integer part instead reports one digit for 0.1m where it has none,
                // which turns a safe comparison against a decimal(38,38) bound into Unknown.
                digits = literalTyping ? Math.Max(Digits(UnscaledMagnitude(d)) - scale, 0) : -1;
                return true;
            }

            case LiteralValue.Kind.HighPrecisionDecimal:
            {
                var (unscaled, s) = value.AsHighPrecisionDecimal;
                scale = s;
                digits = literalTyping ? Math.Max(Digits(unscaled) - (long)s, 0) : -1;
                return true;
            }

            default:
                return false;
        }
    }

    /// <summary>The width Spark gives an integral column, from <c>DecimalType.forType</c>.</summary>
    // A uint is typed as a bigint, as the row evaluator materializes one (#589): Spark has no unsigned
    // types, and int cannot hold every uint. So only an int is 10 digits wide.
    private static int IntegralWidth(LiteralValue.Kind kind) => kind switch
    {
        LiteralValue.Kind.Int32 => 10,
        _ => 20,
    };

    private static int IntegralDigits(LiteralValue value) => value.Type switch
    {
        LiteralValue.Kind.Int32 => Digits(new BigInteger(value.AsInt32)),
        LiteralValue.Kind.Int64 => Digits(new BigInteger(value.AsInt64)),
        LiteralValue.Kind.UInt32 => Digits(new BigInteger(value.AsUInt32)),
        _ => Digits(new BigInteger(value.AsUInt64)),
    };

    /// <summary>How many digits an integer is written with, which is one for zero.</summary>
    private static int Digits(BigInteger value) => ScaledDecimal.DigitCount(value);

    /// <summary>A <c>decimal</c>'s declared scale, which its representation carries.</summary>
    private static int DecimalScale(decimal value) => (decimal.GetBits(value)[3] >> 16) & 0xFF;

    /// <summary>A <c>decimal</c>'s unscaled magnitude, which is the 96 bits below its flags.</summary>
    private static BigInteger UnscaledMagnitude(decimal value)
    {
        var bits = decimal.GetBits(value);

        return (new BigInteger((uint)bits[2]) << 64)
            | (new BigInteger((uint)bits[1]) << 32)
            | new BigInteger((uint)bits[0]);
    }
}
