// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// The shortest decimal that reads back as a binary value, in machine words rather than
/// <see cref="BigInteger"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="SparkFloatText"/> answers this exactly by expanding the value in full: a value is
/// <c>mantissa x scale x 10^shift</c>, and the digits of <c>mantissa x scale</c> ARE the value's.
/// That is correct for every input and it is why it is still there, but the expansion reaches 767
/// digits for a subnormal double, and even an ordinary magnitude costs a handful of big-integer
/// divisions per row — 0.86us against the 0.22us of the round-and-probe ladder it replaced
/// (#337, #338). This runs for every row of every cast to a string or a wide decimal.
/// </para>
/// <para>
/// <b>The whole question fits in machine words once both sides are scaled into the same
/// domain.</b> A candidate <c>s x 10^k</c> reads back as <c>c x 2^q</c> exactly when it lands
/// inside the value's rounding interval, and dividing that interval's ends by <c>10^k</c> turns
/// the test into <c>low &lt;= s &lt;= high</c> — a comparison between an integer and two
/// fixed-point numbers. Both ends come from one multiplication by a stored power of five, so the
/// cost no longer depends on how far the exponent is from zero.
/// </para>
/// <para>
/// <b>It answers only when the answer is unambiguous, and says so when it is not.</b> The stored
/// power is an approximation, so every comparison carries an error bar; whenever an operand falls
/// inside it — which is also exactly where a candidate lands ON a boundary and Java's
/// round-half-even tie rule has to decide — this declines, and the exact path answers instead.
/// Correctness therefore rests on the error bar being conservative rather than on the
/// approximation being good enough, which is a far smaller thing to have to get right, and it is
/// checked by differential sweep rather than argued: every one of the 2,139,095,040 finite
/// non-negative floats agrees with the exact path — exhaustively, not sampled — and so does every
/// double tried.
/// </para>
/// <para>
/// One routine serves both widths. A float's nine digits would fit in narrower arithmetic, but
/// a double needs a 128-bit power and a 128-bit fixed point to hold seventeen digits beside 64
/// fractional bits, and carrying two implementations of one algorithm is how they drift apart.
/// </para>
/// </remarks>
internal static class SparkFloatScaling
{
    /// <summary>
    /// How far apart two scaled quantities must be before their order is believed, in units of
    /// <c>2^-64</c>.
    /// </summary>
    /// <remarks>
    /// The stored power carries 128 significant bits, so its relative error is under 2^-127;
    /// against a scaled end of at most 2^57 — seventeen digits — that is 2^-70, well inside a
    /// single unit here, and the shift that places the result can drop one more. Four units is
    /// comfortably above both. Being generous costs nothing but a slightly more frequent hand-off,
    /// and the hand-off is correct by construction.
    /// </remarks>
    private const ulong Uncertainty = 4;

    /// <summary>
    /// The shortest decimal for <c>mantissa x 2^exponent</c>, or false when it is too close to call.
    /// </summary>
    /// <param name="narrowBelow">
    /// Whether the gap below the value is half the gap above it, which only a power of two has —
    /// see <c>SparkFloatText.NarrowBelow</c>.
    /// </param>
    internal static bool TryShortestDigits(
        long mantissa, int exponent, bool narrowBelow, int maxDigits, out string digits, out int pointAt)
    {
        digits = string.Empty;
        pointAt = 0;

        // The interval in quarter-steps: the value is `centre`, and it reads back anything from
        // `low` to `high`. A power of two is the one value whose predecessor is half a step away
        // rather than a whole one, so its lower end is a quarter rather than a half.
        var centre = mantissa << 2;
        var low = centre - (narrowBelow ? 1 : 2);
        var high = centre + 2;

        // Less two, because the three above were multiplied by four to hold that quarter.
        var scale = exponent - 2;

        // Where the last of `maxDigits` digits falls. The logarithm only has to be close: the
        // scaled result is checked against the width it was chosen for, and a miss costs one
        // retry rather than a wrong answer. Round numbers land exactly on a power of ten, where
        // the double logarithm falls the wrong side, so the retry is not a rare path.
        var estimate = Place(mantissa, exponent, maxDigits);

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var place = estimate + attempt switch { 0 => 0, 1 => 1, _ => -1 };

            if (!TryScale(centre, scale, place, out var scaledCentre)
                || !TryScale(low, scale, place, out var scaledLow)
                || !TryScale(high, scale, place, out var scaledHigh))
            {
                return false;
            }

            var whole = scaledCentre.High;
            if (whole < Pow10[maxDigits - 1] || whole >= Pow10[maxDigits])
                continue;

            if (!TryShorten(scaledCentre, scaledLow, scaledHigh, maxDigits, out var chosen))
                return false;

            // Rounding up can carry past the width — 999999999 becomes 1000000000 — which is one
            // digit more than the place was chosen for, and the point moves with it.
            var carried = chosen >= Pow10[maxDigits] ? 1 : 0;

            digits = Digits(chosen);
            pointAt = place + maxDigits + carried;

            return true;
        }

        return false;
    }

    /// <summary>
    /// Finds the shortest length whose best candidate still reads back, and that candidate.
    /// </summary>
    /// <remarks>
    /// Reading back is monotone in length — a decimal that fits at k digits is still there at k+1
    /// with a zero after it — so the shortest is bisected rather than walked up to, as in the
    /// exact path. A length of one is never asked for: Java widens the field to the one- and
    /// two-digit decimals together whenever a single digit would do, and starting at two and
    /// dropping the trailing zero afterwards is that rule.
    /// </remarks>
    private static bool TryShorten(
        in Scaled centre, in Scaled low, in Scaled high, int maxDigits, out long chosen)
    {
        chosen = 0;

        var shortest = 2;
        var longest = maxDigits;

        while (shortest < longest)
        {
            var middle = (shortest + longest) / 2;

            if (!TryReads(centre, low, high, maxDigits - middle, out var reads))
                return false;

            if (reads)
                longest = middle;
            else
                shortest = middle + 1;
        }

        return TryChoose(centre, low, high, maxDigits - shortest, out chosen);
    }

    /// <summary>Whether either neighbouring multiple of <c>10^drop</c> lands inside the interval.</summary>
    private static bool TryReads(in Scaled centre, in Scaled low, in Scaled high, int drop, out bool reads)
    {
        reads = false;

        if (!TryNeighbours(centre, drop, out var below, out var above)
            || !TryInside(below, low, high, out var belowReads)
            || !TryInside(above, low, high, out var aboveReads))
        {
            return false;
        }

        reads = belowReads || aboveReads;
        return true;
    }

    /// <summary>The candidate Java would pick at this length, which is the closer of the two.</summary>
    /// <remarks>
    /// Where both read back the closer wins, and where they are equally close the even significand
    /// does. An exact tie cannot be told from a near-tie at this precision, so it goes to the
    /// exact path along with everything else inside the error bar.
    /// </remarks>
    private static bool TryChoose(in Scaled centre, in Scaled low, in Scaled high, int drop, out long chosen)
    {
        chosen = 0;

        if (!TryNeighbours(centre, drop, out var below, out var above)
            || !TryInside(below, low, high, out var belowReads)
            || !TryInside(above, low, high, out var aboveReads))
        {
            return false;
        }

        if (belowReads != aboveReads)
        {
            chosen = belowReads ? below : above;
            return true;
        }

        // Halfway between the two candidates. The half goes in the FRACTION rather than into an
        // integer division of the sum, which would throw it away whenever the two are adjacent —
        // that is every candidate at full length, and it would tip all of them upward.
        var middle = new Scaled((below + above) >> 1, ((below + above) & 1) != 0 ? 1UL << 63 : 0);

        if (Scaled.Near(centre, middle))
            return false;

        chosen = Scaled.Less(centre, middle) ? below : above;
        return true;
    }

    /// <summary>The two multiples of <c>10^drop</c> the value sits between.</summary>
    private static bool TryNeighbours(in Scaled centre, int drop, out long below, out long above)
    {
        var step = Pow10[drop];

        below = centre.High - (centre.High % step);
        above = below + step;

        // Only the TOP end is ambiguous. The scaling never lands above the true value and is
        // never a whole unit below it, so a fraction near zero means the integer part is right —
        // whereas a fraction near one means the true value may have carried into the next
        // integer and this one is short by one.
        return centre.Low < ulong.MaxValue - Uncertainty;
    }

    /// <summary>Whether a candidate is inside the interval, when that can be told apart.</summary>
    private static bool TryInside(long candidate, in Scaled low, in Scaled high, out bool inside)
    {
        inside = false;

        var scaled = new Scaled(candidate, 0);

        // Landing within the error bar of either end is also where round-half-even would have to
        // decide whether the boundary belongs to this value. Both go to the exact path.
        if (Scaled.Near(scaled, low) || Scaled.Near(scaled, high))
            return false;

        inside = !Scaled.Less(scaled, low) && !Scaled.Less(high, scaled);
        return true;
    }

    /// <summary>
    /// <c>units x 2^scale / 10^place</c>, in fixed point, when the stored power reaches that far.
    /// </summary>
    /// <remarks>
    /// <c>units x 5^-place x 2^(scale - place)</c>, with the stored power's own exponent folded
    /// into the one shift that places the 192-bit product.
    /// </remarks>
    private static bool TryScale(long units, int scale, int place, out Scaled scaled)
    {
        scaled = default;

        if (!Powers.TryFive(-place, out var high, out var low, out var fiveExponent))
            return false;

        // The 64 fractional bits of the result are what the shift leaves behind.
        var shift = -(fiveExponent + scale - place) - 64;
        if (shift is < 0 or > 190)
            return false;

        Multiply((ulong)units, high, low, out var w2, out var w1, out var w0);

        return Scaled.TryShift(w2, w1, w0, shift, out scaled);
    }

    /// <summary>
    /// The base-ten exponent of the value's last digit, were it written with
    /// <paramref name="maxDigits"/> of them.
    /// </summary>
    private static int Place(long mantissa, int exponent, int maxDigits)
    {
        const double Log10Two = 0.30102999566398120;

        return (int)Math.Floor(Math.Log10(mantissa) + (exponent * Log10Two)) - maxDigits + 1;
    }

    /// <summary>The chosen candidate's significant digits, with the trailing zeros dropped.</summary>
    /// <remarks>
    /// The candidate is a multiple of <c>10^(maxDigits - length)</c> by construction, so dividing
    /// those zeros away is exact. Java counts a decimal's length without trailing zeros, which is
    /// how a two-digit candidate ending in one comes back as the one-digit answer.
    /// </remarks>
    private static string Digits(long chosen) =>
        chosen.ToString(CultureInfo.InvariantCulture).TrimEnd('0') is { Length: > 0 } trimmed
            ? trimmed
            : "0";

    private static readonly long[] Pow10 =
    {
        1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L,
        1_000_000_000L, 10_000_000_000L, 100_000_000_000L, 1_000_000_000_000L,
        10_000_000_000_000L, 100_000_000_000_000L, 1_000_000_000_000_000L,
        10_000_000_000_000_000L, 100_000_000_000_000_000L,
    };

    /// <summary>A 64 by 128 bit product, as three words from most significant down.</summary>
    private static void Multiply(
        ulong units, ulong high, ulong low, out ulong w2, out ulong w1, out ulong w0)
    {
        var upper = BigMul(units, high, out var upperLow);
        var lower = BigMul(units, low, out w0);

        w1 = upperLow + lower;
        w2 = upper + (w1 < lower ? 1UL : 0UL);
    }

    /// <summary>A 64x64 to 128 bit product, which netstandard2.0 has no operator for.</summary>
    private static ulong BigMul(ulong left, ulong right, out ulong low)
    {
#if NETSTANDARD2_0
        var leftLow = (uint)left;
        var leftHigh = left >> 32;
        var rightLow = (uint)right;
        var rightHigh = right >> 32;

        var lowLow = (ulong)leftLow * rightLow;
        var cross = ((ulong)leftLow * rightHigh) + (lowLow >> 32);
        var crossLow = ((ulong)leftHigh * rightLow) + (uint)cross;

        low = (crossLow << 32) | (uint)lowLow;

        return (leftHigh * rightHigh) + (cross >> 32) + (crossLow >> 32);
#else
        return Math.BigMul(left, right, out low);
#endif
    }

    /// <summary>
    /// A number as an integer part and 64 fractional bits.
    /// </summary>
    /// <remarks>
    /// Seventeen digits reach 2^57 and the fraction wants 64 bits, so neither a <c>long</c> nor a
    /// <c>double</c> can hold one; this is the smallest thing that can, and it needs nothing but
    /// comparison and subtraction. <c>UInt128</c> would do it on the modern targets and not on
    /// netstandard2.0, and one shape for every target is worth more here than the operators.
    /// </remarks>
    private readonly struct Scaled(long high, ulong low)
    {
        internal long High { get; } = high;

        internal ulong Low { get; } = low;

        /// <summary>The low 128 bits of a 192-bit value shifted right, which is all the caller wants.</summary>
        internal static bool TryShift(ulong w2, ulong w1, ulong w0, int shift, out Scaled scaled)
        {
            scaled = default;

            ulong high, low;

            if (shift < 64)
            {
                if (shift == 0)
                {
                    high = w1;
                    low = w0;
                }
                else
                {
                    high = (w1 >> shift) | (w2 << (64 - shift));
                    low = (w0 >> shift) | (w1 << (64 - shift));
                }

                // Anything left above the 128th bit means the place was mis-estimated by more than
                // the retry allows for.
                if ((w2 >> shift) != 0)
                    return false;
            }
            else if (shift < 128)
            {
                var by = shift - 64;
                high = by == 0 ? w2 : w2 >> by;
                low = by == 0 ? w1 : (w1 >> by) | (w2 << (64 - by));
            }
            else
            {
                var by = shift - 128;
                high = 0;
                low = by == 0 ? w2 : w2 >> by;
            }

            if (high > long.MaxValue)
                return false;

            scaled = new Scaled((long)high, low);
            return true;
        }

        internal static bool Less(in Scaled left, in Scaled right) =>
            left.High != right.High ? left.High < right.High : left.Low < right.Low;

        /// <summary>Whether two values are within the error bar of each other.</summary>
        internal static bool Near(in Scaled left, in Scaled right)
        {
            var high = left.High - right.High;
            var low = left.Low - right.Low;

            // One borrow out of the fraction when the subtraction wrapped.
            if (left.Low < right.Low)
                high--;

            return high switch
            {
                0 => low <= Uncertainty,
                -1 => low >= ulong.MaxValue - Uncertainty + 1,
                _ => false,
            };
        }
    }

    /// <summary>
    /// Powers of five held as a 128-bit significand and a binary exponent.
    /// </summary>
    /// <remarks>
    /// <c>5^n ~ (high:low) x 2^exponent</c> with the significand normalised to its top bit and
    /// truncated, so every entry carries the same 128 significant bits whatever its magnitude and
    /// every one of them is at or below the true power. Built from
    /// <see cref="BigInteger"/> on first touch rather than written out: a transcribed table of 691
    /// pairs is a transcription bug waiting to happen, and the arithmetic that derives it is the
    /// arithmetic the exact path already trusts.
    /// <para>
    /// The range covers every place a value can ask for. A double's decimal exponent runs from
    /// -324 to 308 and the place sits sixteen digits below it, so <c>-place</c> stays inside
    /// [-292, 340]; a float asks for far less.
    /// </para>
    /// </remarks>
    private static class Powers
    {
        private const int Lowest = -345;
        private const int Highest = 345;

        private static readonly ulong[] Highs = new ulong[Highest - Lowest + 1];
        private static readonly ulong[] Lows = new ulong[Highest - Lowest + 1];
        private static readonly int[] Exponents = new int[Highest - Lowest + 1];
        private static readonly bool[] Built = new bool[Highest - Lowest + 1];

        internal static bool TryFive(int power, out ulong high, out ulong low, out int exponent)
        {
            high = 0;
            low = 0;
            exponent = 0;

            if (power < Lowest || power > Highest)
                return false;

            var index = power - Lowest;

            if (!Built[index])
                Fill(power, index);

            high = Highs[index];
            low = Lows[index];
            exponent = Exponents[index];

            return true;
        }

        private static void Fill(int power, int index)
        {
            var numerator = power >= 0 ? BigInteger.Pow(5, power) : BigInteger.One;
            var denominator = power >= 0 ? BigInteger.One : BigInteger.Pow(5, -power);

            var exponent = 0;
            var floor = BigInteger.One << 127;
            var ceiling = BigInteger.One << 128;

            // Normalise so the significand occupies exactly 128 bits.
            while (numerator / denominator < floor)
            {
                numerator <<= 1;
                exponent--;
            }

            while (numerator / denominator >= ceiling)
            {
                denominator <<= 1;
                exponent++;
            }

            // TRUNCATED, not rounded to nearest, and the direction is the point. The shift that
            // places the product truncates too, so leaving this one downward as well makes the
            // whole computation err in ONE direction and never above the true value. That is what
            // lets the integer part be trusted when the fraction comes out at zero — which is not
            // a marginal case but the commonest one there is, since every value that IS a short
            // decimal scales to an exact integer. Rounding up instead cost a 21% hand-off rate on
            // ordinary rounded magnitudes.
            var quotient = numerator / denominator;

            Highs[index] = (ulong)(quotient >> 64);
            Lows[index] = (ulong)(quotient & ulong.MaxValue);
            Exponents[index] = exponent;

            // Written last: a reader that sees this set has seen the three above, and two threads
            // racing on a cold row compute the same numbers.
            System.Threading.Volatile.Write(ref Built[index], true);
        }
    }
}
