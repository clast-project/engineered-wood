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
///   <item><c>int + float</c> is <c>double</c>, not <c>float</c>.</item>
///   <item><c>/</c> on two integers is <c>double</c>. It is never integer division.</item>
///   <item><c>%</c> takes the <em>narrower</em> operand's integer digits, so
///     <c>decimal(10,2) % decimal(6,4)</c> is <c>decimal(6,4)</c>.</item>
///   <item><c>decimal(38,10)</c> squared clamps to <c>decimal(38,6)</c>, sacrificing scale to
///     stay within the maximum precision.</item>
///   <item><b>Floating point wins over decimal, not the other way round.</b> Every mix of a
///     decimal with a <c>float</c> or a <c>double</c> is a <c>double</c> — including
///     <c>decimal + float</c>, which is a double rather than a float — and the arithmetic is
///     then done in double: measured, <c>decimal(38,38) 0.1 + double 0.2</c> is
///     <c>0.30000000000000004</c>, the double answer rather than the exact one. This holds in
///     both dialects and in every context: arithmetic, <c>greatest</c>/<c>least</c>,
///     <c>coalesce</c>/<c>if</c>/<c>CASE</c> and comparison. #277.</item>
/// </list>
/// <para>
/// These are ANSI-mode answers. The corpus was harvested with
/// <c>spark.sql.ansi.enabled=true</c>, and ANSI type coercion differs from legacy in exactly the
/// <c>int + float</c> sort of case, so the rules are not portable to
/// <see cref="SparkDialectOptions.Ansi"/> being false without measuring that configuration too.
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
    /// <exception cref="NotSupportedException">Either operand is not a supported numeric type.</exception>
    public static IArrowType ArithmeticResult(string op, IArrowType left, IArrowType right)
    {
        // Decimal is contagious over the INTEGRAL types only: an int is read as the decimal that
        // holds it exactly and the decimal rules below apply. Against a float or a double it is
        // the other way round -- see the remarks on FLOATING POINT WINS -- so this asks for a
        // decimal on one side and no floating point on either.
        if ((IsDecimal(left) || IsDecimal(right)) && !IsFloatingPoint(left) && !IsFloatingPoint(right))
            return DecimalResult(op, AsDecimal(left), AsDecimal(right));

        // `/` is never integer division, and it is double even for two floats. Measured:
        // `f / f` is a double where `f % f` is a float. This sits below the decimal branch
        // because decimal / decimal IS a decimal -- `decimal(10,2) / decimal(6,4)` is
        // decimal(21,9) -- and above the floating-point one because nothing else divides.
        if (op == "/")
            return DoubleType.Default;

        if (IsFloatingPoint(left) || IsFloatingPoint(right))
            return DoubleOrFloat(left, right);

        return WiderIntegral(left, right);
    }

    /// <summary>The result type of unary minus, which never changes the operand's type.</summary>
    public static IArrowType NegateResult(IArrowType operand) =>
        IsNumeric(operand)
            ? operand
            : throw new NotSupportedException($"unary minus is not defined for {operand.Name}");

    /// <summary>
    /// The type two branches of a conditional unify to — <c>coalesce</c>, <c>if</c>, <c>CASE</c>.
    /// </summary>
    /// <remarks>
    /// Close to <see cref="ArithmeticResult"/> but not the same function, and worth keeping
    /// separate: unification widens to hold either operand, where arithmetic widens to hold a
    /// <em>result</em>. Measured, <c>coalesce(decimal(10,2), int)</c> is <c>decimal(12,2)</c>
    /// while <c>decimal(10,2) + int</c> is <c>decimal(13,2)</c> — the extra digit addition needs
    /// for a carry is not needed here.
    /// <para>
    /// They also differ once the natural precision overflows, which is what #280 was: the two
    /// sacrifice scale to different floors. See <see cref="ClampPreferringIntegralDigits"/>,
    /// which is the clamp this half takes.
    /// </para>
    /// <para>
    /// <b>Greatest and least unify here too</b>, not only the conditionals in the summary. They
    /// are the callers where a lossy common type is visible as a wrong VALUE rather than a wrong
    /// type: both cast every argument to this type first and then compare, so measured,
    /// <c>greatest(0.5BD, CAST(0 AS DECIMAL(38,0)))</c> is 1 — a value neither argument held,
    /// because unifying rounded 0.5 up at scale 0 before the comparison ever ran.
    /// </para>
    /// </remarks>
    public static IArrowType CommonType(IArrowType left, IArrowType right)
    {
        if (left.GetType() == right.GetType() && !IsDecimal(left))
            return left;

        if (left is StringType || right is StringType)
        {
            return left is StringType && right is StringType
                ? StringType.Default
                : throw new NotSupportedException(
                    $"no common type for {left.Name} and {right.Name}");
        }

        // Floating point is checked BEFORE decimal for the same reason it is in
        // `ArithmeticResult`: a decimal unified with a double is a double, not a decimal.
        if (IsFloatingPoint(left) || IsFloatingPoint(right))
            return DoubleOrFloat(left, right);

        if (IsDecimal(left) || IsDecimal(right))
        {
            var (lp, ls) = AsDecimal(left);
            var (rp, rs) = AsDecimal(right);
            var scale = Math.Max(ls, rs);

            // ClampPreferringIntegralDigits, NOT `Clamp`. Unification and arithmetic sacrifice
            // scale to different floors, and using arithmetic's here is #280.
            return ClampPreferringIntegralDigits(Math.Max(lp - ls, rp - rs) + scale, scale);
        }

        if (IsIntegral(left) && IsIntegral(right))
            return WiderIntegral(left, right);

        throw new NotSupportedException($"no common type for {left.Name} and {right.Name}");
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

    private static IArrowType DoubleOrFloat(IArrowType left, IArrowType right)
    {
        // Only float combined with float stays float. Anything wider on either side — including
        // an integer, which float cannot hold exactly — goes to double.
        if (left is FloatType && right is FloatType)
            return FloatType.Default;

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
    /// Brings a computed precision and scale within Spark's maximum for a LEAST COMMON TYPE,
    /// which gives up scale further than <see cref="Clamp"/> will.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark's <c>DecimalType.boundedPreferIntegralDigits</c>, reached from
    /// <c>DecimalPrecisionTypeCoercion.widerDecimalType</c>. It is the same shape as
    /// <see cref="Clamp"/> — integer digits are kept and the scale absorbs the overflow — with
    /// one difference that is the whole of #280: <b>the floor is zero, not
    /// <see cref="MinimumAdjustedScale"/></b>. So decimal(4,3) unified with decimal(38,0), whose
    /// natural common type is decimal(41,3), is decimal(38,<b>0</b>) and not decimal(38,3).
    /// </para>
    /// <para>
    /// The floor is what makes the difference load-bearing rather than cosmetic. Reserving six
    /// fractional digits leaves only 32 integer digits, and the decimal(38,0) operand does not
    /// fit in 32 — so unifying it yields no value at all, and every caller then reads that as a
    /// null. Measured, <c>greatest(1.005, 99999999999999999999999999999999999999)</c> answered
    /// 1.005, because the larger operand had become a null that <c>greatest</c> skips, and
    /// <c>coalesce</c> over the same pair answered NULL outright.
    /// </para>
    /// <para>
    /// With the floor at zero the scale gives up exactly as much as it must and no operand can
    /// ever fail to fit: the adjusted scale leaves <c>max(p1 - s1, p2 - s2)</c> integer digits,
    /// which is by construction what the wider operand needs. That invariant is the reason the
    /// method Spark names is the one to copy rather than to approximate.
    /// </para>
    /// <para>
    /// Arithmetic keeps <see cref="Clamp"/>, and the two really are different functions rather
    /// than one that drifted: measured under the same session, decimal(6,4) unified with
    /// decimal(38,0) is decimal(38,0) while decimal(6,4) <c>+</c> decimal(38,0) is
    /// decimal(38,4). Spark 4.0 changed the unification half and left arithmetic alone —
    /// <c>spark.sql.legacy.decimal.retainFractionDigitsOnTruncate</c>, default false, restores
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
