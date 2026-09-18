// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Spark's result type for an arithmetic operation, given its operand types.
/// </summary>
/// <remarks>
/// <para>
/// Every rule here was measured out of Spark rather than reasoned about, and is pinned by the
/// <c>coercion</c> group of <c>Fixtures/spark-expression-corpus.json</c>. Several are not what
/// an implementer would guess:
/// </para>
/// <list type="bullet">
///   <item><c>smallint * smallint</c> stays <c>smallint</c> — integral arithmetic does not widen,
///     so it can overflow at its own width.</item>
///   <item><c>int + float</c> is <c>double</c> under ANSI, not <c>float</c> -- and <c>float</c>
///     under the legacy dialect, which rounds the integral onto it first. The same pair splits
///     the same way everywhere it is unified, comparison included; <c>/</c> is double in both.</item>
///   <item><c>/</c> on two integers is <c>double</c>. It is never integer division.</item>
///   <item><c>%</c> takes the <em>narrower</em> operand's integer digits, so
///     <c>decimal(10,2) % decimal(6,4)</c> is <c>decimal(6,4)</c>.</item>
///   <item><c>decimal(38,10)</c> squared clamps to <c>decimal(38,6)</c>, sacrificing scale to
///     stay within the maximum precision.</item>
///   <item>Floating point wins over decimal. Every mix of a decimal with a <c>float</c> or a
///     <c>double</c> is a <c>double</c> — <c>decimal + float</c> included — and the arithmetic
///     is done in double: <c>decimal(38,38) 0.1 + double 0.2</c> is
///     <c>0.30000000000000004</c>. This holds in both dialects and in every context:
///     arithmetic, <c>greatest</c>/<c>least</c>, <c>coalesce</c>/<c>if</c>/<c>CASE</c> and
///     comparison.</item>
/// </list>
/// <para>
/// The corpus carries a legacy section harvested with <c>spark.sql.ansi.enabled=false</c>, and
/// the only rule here it found to differ is the integral against a float, which is why
/// <see cref="CommonType"/> and <see cref="ArithmeticResult"/> take <c>legacy</c>. A rule added
/// here is not known to hold in the legacy dialect until that section covers it.
/// </para>
/// </remarks>
internal static class SparkNumericTypes
{
    /// <summary>Spark's maximum decimal precision.</summary>
    public const int MaxPrecision = 38;

    /// <summary>
    /// The floor Spark will reduce a decimal's scale to when precision would otherwise overflow.
    /// </summary>
    public const int MinimumAdjustedScale = 6;

    /// <summary>The result type of <paramref name="op"/> over the two operand types.</summary>
    /// <remarks>
    /// A <c>void</c> operand (a bare <c>NULL</c>) takes the other one's type, and the operation is
    /// then typed as that type against itself: <c>a + NULL</c> is an <c>int</c> and
    /// <c>d1 + NULL</c> over a decimal(10,2) is a <c>decimal(11,2)</c>, the extra digit being
    /// addition's carry.
    /// <para>
    /// Two voids are a <c>double</c>, not a void: <c>NULL + NULL</c> is a double and so is
    /// <c>-NULL</c>. Spark has no arithmetic over <c>void</c>, so it defaults the operands rather
    /// than propagating the type — unlike <see cref="CommonType"/>, where an untyped null simply
    /// steps aside.
    /// </para>
    /// </remarks>
    /// <param name="op">The operator: <c>+</c>, <c>-</c>, <c>*</c>, <c>/</c> or <c>%</c>.</param>
    /// <param name="left">The left operand's type.</param>
    /// <param name="right">The right operand's type.</param>
    /// <param name="legacy">
    /// Whether the legacy dialect's rule applies, which differs only for an integral against a
    /// <c>float</c>: <c>float</c> there, <c>double</c> under ANSI.
    /// </param>
    /// <exception cref="NotSupportedException">Either operand is not a supported numeric type.</exception>
    public static IArrowType ArithmeticResult(
        string op, IArrowType left, IArrowType right, bool legacy = false)
    {
        if (left is NullType || right is NullType)
        {
            if (left is NullType && right is NullType)
                return DoubleType.Default;

            var typed = left is NullType ? right : left;
            return ArithmeticResult(op, typed, typed, legacy);
        }

        // Decimal is contagious over the integral types only: an int is read as the decimal that
        // holds it exactly and the decimal rules below apply. Against a float or a double,
        // floating point wins (see the class remarks).
        if ((IsDecimal(left) || IsDecimal(right)) && !IsFloatingPoint(left) && !IsFloatingPoint(right))
            return DecimalResult(op, AsDecimal(left), AsDecimal(right));

        // `/` is never integer division, and it is double even for two floats: `f / f` is a
        // double where `f % f` is a float. This sits below the decimal branch because
        // decimal / decimal is a decimal -- `decimal(10,2) / decimal(6,4)` is decimal(21,9).
        if (op == "/")
            return DoubleType.Default;

        if (IsFloatingPoint(left) || IsFloatingPoint(right))
            return DoubleOrFloat(left, right, legacy);

        return WiderIntegral(left, right);
    }

    /// <summary>
    /// The result type of a unary <c>+</c> or <c>-</c>, which never changes the operand's type.
    /// </summary>
    /// <remarks>
    /// The one exception is <c>void</c>, which has no arithmetic of its own: <c>+NULL</c> and
    /// <c>-NULL</c> are both <c>double</c>, the same default two void operands take in
    /// <see cref="ArithmeticResult"/>. Anything non-numeric is refused.
    /// </remarks>
    /// <param name="operand">The type the operator was applied to.</param>
    /// <param name="operatorName">
    /// <c>plus</c> or <c>minus</c>, naming the operator in the refusal a non-numeric operand gets.
    /// </param>
    public static IArrowType UnaryResult(IArrowType operand, string operatorName) =>
        operand is NullType
            ? DoubleType.Default
            : IsNumeric(operand)
                ? operand
                : throw new NotSupportedException(
                    $"unary {operatorName} is not defined for {operand.Name}");

    /// <summary>
    /// The type two branches of a conditional unify to — <c>coalesce</c>, <c>if</c>, <c>CASE</c>.
    /// </summary>
    /// <remarks>
    /// Close to <see cref="ArithmeticResult"/> but not the same function: unification widens to
    /// hold either operand, where arithmetic widens to hold a <em>result</em>.
    /// <c>coalesce(decimal(10,2), int)</c> is <c>decimal(12,2)</c> while
    /// <c>decimal(10,2) + int</c> is <c>decimal(13,2)</c> — no digit is reserved for a carry.
    /// They also sacrifice scale to different floors once the natural precision overflows; see
    /// <see cref="ClampPreferringIntegralDigits"/>.
    /// <para>
    /// <c>greatest</c> and <c>least</c> unify here too. They cast every argument to this type
    /// before comparing, so a lossy common type shows up as a wrong value:
    /// <c>greatest(0.5BD, CAST(0 AS DECIMAL(38,0)))</c> is 1, because unifying rounded 0.5 up at
    /// scale 0 before the comparison ran.
    /// </para>
    /// <para>
    /// A DATE and a TIMESTAMP unify here too; see <see cref="TemporalCommonType"/>.
    /// </para>
    /// </remarks>
    /// <param name="left">One type to unify.</param>
    /// <param name="right">The other type to unify.</param>
    /// <param name="legacy">
    /// Whether the legacy dialect's rule applies, which differs only for an integral against a
    /// <c>float</c>: <c>float</c> there, <c>double</c> under ANSI.
    /// </param>
    public static IArrowType CommonType(IArrowType left, IArrowType right, bool legacy = false)
    {
        // `void` constrains nothing, so the other side is the common type -- and two voids stay
        // void, which is what Spark answers for `greatest(NULL, NULL)`. Unlike arithmetic, which
        // defaults two voids to double, unification simply steps aside.
        if (left is NullType)
            return right;

        if (right is NullType)
            return left;

        // Before the identity check below, so that two timestamps resolve to the type this
        // evaluator builds (see SparkArrays.Timestamp) rather than whichever unit and zone the
        // left-hand column carried. TemporalCommonType decides the zoned/naive direction.
        if (SparkArrays.IsTemporal(left) && SparkArrays.IsTemporal(right))
            return TemporalCommonType(left, right);

        if (left.GetType() == right.GetType() && !IsDecimal(left))
            return left;

        if (left is StringType || right is StringType)
        {
            return left is StringType && right is StringType
                ? StringType.Default
                : throw new NotSupportedException(
                    $"no common type for {left.Name} and {right.Name}");
        }

        // Floating point is checked before decimal: a decimal unified with a double is a double.
        if (IsFloatingPoint(left) || IsFloatingPoint(right))
            return DoubleOrFloat(left, right, legacy);

        if (IsDecimal(left) || IsDecimal(right))
        {
            var (lp, ls) = AsDecimal(left);
            var (rp, rs) = AsDecimal(right);
            var scale = Math.Max(ls, rs);

            // ClampPreferringIntegralDigits, not `Clamp`: unification and arithmetic sacrifice
            // scale to different floors, and arithmetic's would leave the wider operand no room
            // for its integer digits.
            return ClampPreferringIntegralDigits(Math.Max(lp - ls, rp - rs) + scale, scale);
        }

        if (IsIntegral(left) && IsIntegral(right))
            return WiderIntegral(left, right);

        throw new NotSupportedException($"no common type for {left.Name} and {right.Name}");
    }

    /// <summary>
    /// The type two temporals unify to: a DATE, a zoned TIMESTAMP or a TIMESTAMP_NTZ.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two dates stay a date; a pair with any zoned timestamp in it is zoned; a pair with a naive
    /// timestamp and no zoned one is naive. The same in both dialects, and for <c>coalesce</c>,
    /// <c>if</c>, <c>CASE</c>, <c>nvl2</c>, <c>greatest</c> and <c>least</c> alike.
    /// </para>
    /// <para>
    /// The DATE moves up, to midnight of that day in the session zone, rather than the timestamp
    /// being truncated down — visible in the instant that comes back, not only in the type: with
    /// <c>ts</c> at 12:30:00Z and <c>dt</c> on the same day, <c>coalesce(ts, dt)</c> is 12:30:00Z
    /// while <c>coalesce(dt, ts)</c> is 00:00:00Z. The promotion itself lives in
    /// <see cref="SparkFunctions.Unify"/>, which reads every temporal branch through
    /// <see cref="SparkArrays.ReadInstant"/> — the same instant <c>CAST(dt AS TIMESTAMP)</c>
    /// takes — so the fold and the cast share one timezone policy.
    /// </para>
    /// <para>
    /// A temporal against anything else has no rule and falls through to the refusal, which is
    /// Spark's answer too: <c>coalesce(a, dt)</c> is <c>DATATYPE_MISMATCH.DATA_DIFF_TYPES</c>.
    /// </para>
    /// </remarks>
    private static IArrowType TemporalCommonType(IArrowType left, IArrowType right)
    {
        // Two dates stay a date. Everything else becomes a timestamp of one kind or the other.
        if (SparkArrays.IsDateType(left) && SparkArrays.IsDateType(right))
            return Date32Type.Default;

        // A zoned operand wins, and the asymmetry is the rule rather than a tie-break:
        // `coalesce(ntz, ts)` and `coalesce(ts, ntz)` are both a zoned `timestamp`, while
        // `coalesce(ntz, dt)` and `coalesce(dt, ntz)` are both `timestamp_ntz`. A DATE does not
        // force a zone; it takes whichever timestamp it is folded with.
        //
        // Delta's widening agrees: `date -> timestamp_ntz` is permitted and `date ->` a zoned
        // timestamp is refused, because that reads a calendar date as an absolute instant
        // (`ValueWidener`). Answering a zoned `Timestamp` for `coalesce(ntz, dt)` would be exactly
        // that reinterpretation.
        return SparkArrays.IsZonedTimestamp(left) || SparkArrays.IsZonedTimestamp(right)
            ? SparkArrays.Timestamp
            : SparkArrays.NaiveTimestamp;
    }

    /// <summary>
    /// The decimal type Spark reads an integral literal as when it meets a decimal: the narrowest
    /// one that holds the value, rather than the one that holds its type.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark's <c>DecimalType.fromLiteral</c>, reached from
    /// <c>DecimalPrecisionTypeCoercion.nondecimalAndDecimal</c> under
    /// <c>spark.sql.decimalOperations.literalPickMinimumPrecision</c> (default true). A literal
    /// <c>2</c> is a <c>decimal(1,0)</c> where an <c>int</c> column is
    /// <see cref="AsDecimal"/>'s <c>decimal(10,0)</c>, and the integer digits that saves are kept
    /// as scale: <c>1.5BD / 2</c> is <c>decimal(7,6)</c> and <c>d1 + 2</c> over a decimal(10,2)
    /// is <c>decimal(11,2)</c>, against the decimal(13,12) and decimal(13,2) the column rule
    /// gives.
    /// </para>
    /// <para>
    /// It applies only at a binary operator — arithmetic and comparison. Unification does not
    /// take it (<c>greatest(d1, 2)</c> and <c>coalesce(d1, 2)</c> are both decimal(12,2), the
    /// int-width answer), and neither does an <c>IN</c> list:
    /// <c>CAST(4E-32 AS DECIMAL(38,38)) = 0</c> is false while the same value <c>IN (0)</c> is
    /// true. So this is deliberately not reached from <see cref="CommonType"/>.
    /// </para>
    /// <para>
    /// The sign never changes the answer — -100 and 100 both have three digits — which lets the
    /// caller treat Spark's folded negative literal and our <c>negative(2)</c> call alike.
    /// </para>
    /// <para>
    /// A tinyint literal would be the exception: <c>fromLiteral</c> has cases for Short, Int and
    /// Long only, so a Byte keeps the full decimal(3,0) (<c>d5 + 2Y</c> is decimal(38,34) where
    /// <c>d5 + 2S</c> is decimal(38,36)). It cannot reach here, because <c>SparkLiteral</c>
    /// refuses the <c>Y</c> and <c>S</c> suffixes outright.
    /// </para>
    /// </remarks>
    public static Decimal128Type LiteralDecimal(long value) =>
        new(DigitCount(value), 0);

    /// <summary>The decimal digits in a value, which is its precision as Spark counts one.</summary>
    /// <remarks>
    /// Zero has one digit, not none: <c>BigDecimal(0).precision()</c> is 1, and a decimal(0,0)
    /// is not a type. Counted by division rather than through <see cref="Math.Abs(long)"/>, which
    /// throws on <see cref="long.MinValue"/> — a literal that can reach here, since
    /// <c>-9223372036854775808</c> parses as one.
    /// </remarks>
    private static int DigitCount(long value)
    {
        var digits = 0;
        do
        {
            digits++;
            value /= 10;
        }
        while (value != 0);

        return digits;
    }

    public static bool IsDecimal(IArrowType type) => type is Decimal128Type or Decimal256Type;

    public static bool IsFloatingPoint(IArrowType type) => type is FloatType or DoubleType;

    public static bool IsIntegral(IArrowType type) =>
        type is Int8Type or Int16Type or Int32Type or Int64Type;

    public static bool IsNumeric(IArrowType type) =>
        IsIntegral(type) || IsFloatingPoint(type) || IsDecimal(type);

    /// <summary>Precision and scale of a type read as a decimal.</summary>
    /// <remarks>
    /// Integers convert to the decimal that holds them exactly, which is how Spark mixes them
    /// with decimals: an <c>int</c> is <c>decimal(10,0)</c>, so <c>int + decimal(10,2)</c> works
    /// out to <c>decimal(13,2)</c>.
    /// </remarks>
    public static (int Precision, int Scale) AsDecimal(IArrowType type) => type switch
    {
        Decimal128Type d => (d.Precision, d.Scale),
        Decimal256Type d => (d.Precision, d.Scale),
        Int8Type => (3, 0),
        Int16Type => (5, 0),
        Int32Type => (10, 0),
        Int64Type => (20, 0),
        _ => throw new NotSupportedException(
            $"{type.Name} has no exact decimal representation; " +
            "mixing it with a decimal would need a lossy conversion"),
    };

    /// <summary>The floating-point type a pair with at least one float or double resolves to.</summary>
    /// <remarks>
    /// <para>
    /// The dialects disagree only about a <c>float</c> against an integral, at every integral
    /// width: ANSI widens the pair to <c>double</c>, because int to float loses bits; the legacy
    /// dialect takes the tightest common type, <c>float</c>, and rounds the integral onto it
    /// first. So <c>16777217 = CAST(16777216 AS FLOAT)</c> is false under ANSI and true under
    /// legacy, and <c>i + f</c>, <c>coalesce(i, f)</c> and <c>greatest(i, f)</c> are all floats
    /// there.
    /// </para>
    /// <para>
    /// Everything else agrees across dialects: two floats stay float, anything against a double
    /// is double, and a decimal against a float is double in both. <c>/</c> never reaches here
    /// from <see cref="ArithmeticResult"/>; it is double before the operands are asked.
    /// </para>
    /// </remarks>
    private static IArrowType DoubleOrFloat(IArrowType left, IArrowType right, bool legacy)
    {
        if (left is FloatType && right is FloatType)
            return FloatType.Default;

        if (legacy
            && (left is FloatType || right is FloatType)
            && (IsIntegral(left) || IsIntegral(right)))
        {
            return FloatType.Default;
        }

        return DoubleType.Default;
    }

    private static IArrowType WiderIntegral(IArrowType left, IArrowType right)
    {
        var rank = Math.Max(IntegralRank(left), IntegralRank(right));
        return rank switch
        {
            0 => Int8Type.Default,
            1 => Int16Type.Default,
            2 => Int32Type.Default,
            _ => Int64Type.Default,
        };
    }

    private static int IntegralRank(IArrowType type) => type switch
    {
        Int8Type => 0,
        Int16Type => 1,
        Int32Type => 2,
        Int64Type => 3,
        _ => throw new NotSupportedException($"arithmetic is not defined for {type.Name}"),
    };

    /// <summary>
    /// Spark's decimal arithmetic rules, from <c>DecimalPrecision</c>.
    /// </summary>
    private static IArrowType DecimalResult(string op, (int P, int S) l, (int P, int S) r)
    {
        var (precision, scale) = op switch
        {
            "+" or "-" => (
                Math.Max(l.S, r.S) + Math.Max(l.P - l.S, r.P - r.S) + 1,
                Math.Max(l.S, r.S)),

            "*" => (l.P + r.P + 1, l.S + r.S),

            "/" => (
                l.P - l.S + r.S + Math.Max(6, l.S + r.P + 1),
                Math.Max(6, l.S + r.P + 1)),

            // The narrower operand bounds the remainder, which is why this is the one rule that
            // can produce a *smaller* type than either input.
            "%" => (
                Math.Min(l.P - l.S, r.P - r.S) + Math.Max(l.S, r.S),
                Math.Max(l.S, r.S)),

            _ => throw new NotSupportedException($"'{op}' is not a decimal arithmetic operator"),
        };

        return Clamp(precision, scale);
    }

    /// <summary>
    /// Brings a computed precision and scale within Spark's maximum, sacrificing scale.
    /// </summary>
    /// <remarks>
    /// Spark's <c>adjustPrecisionScale</c> with precision loss allowed. Integer digits are never
    /// given up — losing them would change the magnitude of a value rather than its exactness —
    /// so the scale absorbs the whole overflow, down to a floor of
    /// <see cref="MinimumAdjustedScale"/>. That floor is why <c>decimal(38,10)</c> squared, whose
    /// natural result is <c>decimal(77,20)</c>, lands on <c>decimal(38,6)</c> rather than
    /// something with no fractional part at all.
    /// </remarks>
    public static Decimal128Type Clamp(int precision, int scale)
    {
        if (precision <= MaxPrecision)
            return new Decimal128Type(Math.Max(precision, 1), scale);

        var integerDigits = precision - scale;
        var minimumScale = Math.Min(scale, MinimumAdjustedScale);
        var adjustedScale = Math.Max(MaxPrecision - integerDigits, minimumScale);

        return new Decimal128Type(MaxPrecision, adjustedScale);
    }

    /// <summary>
    /// Brings a computed precision and scale within Spark's maximum for a least common type,
    /// which gives up scale further than <see cref="Clamp"/> will.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark's <c>DecimalType.boundedPreferIntegralDigits</c>, reached from
    /// <c>DecimalPrecisionTypeCoercion.widerDecimalType</c>. The same shape as
    /// <see cref="Clamp"/> — integer digits are kept and the scale absorbs the overflow — but the
    /// floor is zero, not <see cref="MinimumAdjustedScale"/>. So decimal(4,3) unified with
    /// decimal(38,0), whose natural common type is decimal(41,3), is decimal(38,0).
    /// </para>
    /// <para>
    /// The zero floor guarantees every operand fits: the adjusted scale leaves
    /// <c>max(p1 - s1, p2 - s2)</c> integer digits, which is what the wider operand needs. A floor
    /// of six would leave only 32 integer digits, too few for a decimal(38,0) operand, which would
    /// then unify to null — <c>greatest</c> would skip it and <c>coalesce</c> would answer NULL.
    /// </para>
    /// <para>
    /// Arithmetic keeps <see cref="Clamp"/>: decimal(6,4) unified with decimal(38,0) is
    /// decimal(38,0) while decimal(6,4) <c>+</c> decimal(38,0) is decimal(38,4). Spark 4.0
    /// changed the unification half only;
    /// <c>spark.sql.legacy.decimal.retainFractionDigitsOnTruncate</c> (default false) restores
    /// the pre-4.0 answer, and EngineeredWood targets the default.
    /// </para>
    /// </remarks>
    public static Decimal128Type ClampPreferringIntegralDigits(int precision, int scale)
    {
        if (precision <= MaxPrecision)
            return new Decimal128Type(Math.Max(precision, 1), scale);

        return new Decimal128Type(MaxPrecision, Math.Max(scale - (precision - MaxPrecision), 0));
    }
}
