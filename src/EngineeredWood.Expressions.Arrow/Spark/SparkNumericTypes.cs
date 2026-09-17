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
    /// <remarks>
    /// <b>A <c>void</c> operand takes the other one's type</b>, which is Spark's rule for a bare
    /// <c>NULL</c> and is why the result is not itself void: measured on 4.0.3, <c>a + NULL</c>
    /// is an <c>int</c>, <c>b + NULL</c> a <c>bigint</c> and <c>d1 + NULL</c> a
    /// <c>decimal(11,2)</c> — which is decimal(10,2) against ITSELF, the extra digit addition
    /// reserves for a carry, rather than against anything the NULL contributed.
    /// <para>
    /// <b>Two voids are a DOUBLE, not a void.</b> Measured, <c>NULL + NULL</c> is a double and so
    /// is <c>-NULL</c>. Spark has no arithmetic over <c>void</c> at all, so it defaults the
    /// operands rather than propagating the type — the one place in this file where an untyped
    /// null does NOT simply step aside.
    /// </para>
    /// <para>
    /// Until #293 a bare NULL arrived here as a STRING and every one of these threw
    /// "arithmetic is not defined for utf8". That is the defect this rule closes; it is not a
    /// refinement of an answer that was nearly right.
    /// </para>
    /// </remarks>
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
            return DoubleOrFloat(left, right, legacy);

        return WiderIntegral(left, right);
    }

    /// <summary>
    /// The result type of a unary <c>+</c> or <c>-</c>, which never changes the operand's type.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The one exception is <c>void</c>, which has no arithmetic of its own: measured,
    /// <c>-NULL</c> is a <c>double</c>, the same default two void operands take in
    /// <see cref="ArithmeticResult"/>. #293.
    /// </para>
    /// <para>
    /// <b>Shared by both unary operators</b>, which is why it takes the operator's name: the rule
    /// is the same one — a numeric keeps its type, a <c>void</c> resolves <c>double</c>, and
    /// nothing else has a rule at all — and only the message differs. Measured for #313/#340,
    /// <c>+NULL</c> and <c>-NULL</c> are both <c>double</c> and <c>+a</c> and <c>-a</c> are both
    /// <c>int</c>; the operators diverge in what they COMPUTE, not in what they resolve.
    /// </para>
    /// </remarks>
    /// <param name="operand">The type the operator was applied to.</param>
    /// <param name="operatorName">
    /// <c>plus</c> or <c>minus</c>, for the refusal a non-numeric operand earns — so that a
    /// reader of the failure is told which operator they wrote.
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
    /// <b>Not only numbers, despite the class name.</b> A DATE and a TIMESTAMP unify here too,
    /// and refusing them was #311 — see <see cref="TemporalCommonType"/>. The class is where the
    /// fold lives, not a claim about what it folds.
    /// </para>
    /// <para>
    /// <b>Greatest and least unify here too</b>, not only the conditionals in the summary. They
    /// are the callers where a lossy common type is visible as a wrong VALUE rather than a wrong
    /// type: both cast every argument to this type first and then compare, so measured,
    /// <c>greatest(0.5BD, CAST(0 AS DECIMAL(38,0)))</c> is 1 — a value neither argument held,
    /// because unifying rounded 0.5 up at scale 0 before the comparison ever ran.
    /// </para>
    /// </remarks>
    public static IArrowType CommonType(IArrowType left, IArrowType right, bool legacy = false)
    {
        // `void` constrains nothing, so the other side IS the common type -- and two voids stay
        // void, which is what Spark answers for `greatest(NULL, NULL)`. Unlike arithmetic, which
        // defaults a lone void to double, unification simply steps aside. #293.
        if (left is NullType)
            return right;

        if (right is NullType)
            return left;

        // BEFORE the identity check below, so that two timestamps resolve the type this
        // evaluator actually BUILDS rather than echoing whichever unit and zone the left-hand
        // column happened to carry. See SparkArrays.Timestamp.
        //
        // A NAIVE timestamp is excluded and falls through, which is the whole of the
        // TIMESTAMP_NTZ boundary -- see IsZonedOrDate.
        if (IsZonedOrDate(left) && IsZonedOrDate(right))
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

        // Floating point is checked BEFORE decimal for the same reason it is in
        // `ArithmeticResult`: a decimal unified with a double is a double, not a decimal.
        if (IsFloatingPoint(left) || IsFloatingPoint(right))
            return DoubleOrFloat(left, right, legacy);

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

    /// <summary>The type a DATE and a TIMESTAMP unify to, which is TIMESTAMP.</summary>
    /// <remarks>
    /// <para>
    /// Measured on 4.0.3 and identical under the legacy dialect: <c>coalesce(ts, dt)</c> is a
    /// <c>timestamp</c>, and so are <c>if</c>, <c>CASE</c>, <c>nvl2</c>, <c>greatest</c> and
    /// <c>least</c> over the same pair. <b>The DATE moves UP</b>, to midnight of that day in the
    /// session zone, rather than the timestamp being truncated down — visible in which instant
    /// comes back rather than only in the type: with <c>ts</c> at 12:30:00Z and <c>dt</c> on the
    /// same day, <c>coalesce(ts, dt)</c> is 12:30:00Z while <c>coalesce(dt, ts)</c> is
    /// 00:00:00Z.
    /// </para>
    /// <para>
    /// <b>This rule is the whole of #311, and its absence was a FALSE REJECTION</b> — the
    /// inverse of #286. Every one of those seven sites threw
    /// <c>no common type for timestamp and date32</c>, so a CHECK constraint Spark accepts made
    /// the table unwritable through us. Nothing either side of the fold was missing:
    /// <c>CAST(dt AS TIMESTAMP)</c>, <c>ts = dt</c>, <c>ts &gt; dt</c>, <c>ts IN (dt)</c> and
    /// <c>nullif(ts, dt)</c> all already agreed with Spark exactly. Only the fold lacked the
    /// rule, and <c>nullif</c> answered only because it types from <c>args[0]</c> instead of
    /// folding — it SIDESTEPS the rule rather than exercising it, which is why the gap stayed
    /// invisible.
    /// </para>
    /// <para>
    /// The promotion itself is not restated here. <c>SparkFunctions.Unify</c> reads every
    /// temporal branch through <c>SparkArrays.ReadInstant</c> and rebuilds it at the unified
    /// type, which is the same instant <c>CastToTimestamp</c> takes for a date source — so the
    /// fold adopts whatever answer the cast already gives, timezone policy included, rather than
    /// deciding that policy a second time.
    /// </para>
    /// <para>
    /// A temporal against anything else has NO rule and falls through to the refusal, which is
    /// Spark's answer too: <c>coalesce(a, dt)</c> is
    /// <c>DATATYPE_MISMATCH.DATA_DIFF_TYPES</c>. So does a TIMESTAMP_NTZ, which is not this
    /// rule's pair at all — <see cref="IsZonedOrDate"/> is where that boundary is drawn and why.
    /// </para>
    /// </remarks>
    private static IArrowType TemporalCommonType(IArrowType left, IArrowType right) =>
        SparkArrays.IsDateType(left) && SparkArrays.IsDateType(right)
            ? Date32Type.Default
            : SparkArrays.Timestamp;

    /// <summary>
    /// A date, or a timestamp that carries a zone — the operands the rule above was measured on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>TIMESTAMP_NTZ IS A DIFFERENT SPARK TYPE AND IS DELIBERATELY NOT FOLDED HERE.</b> Delta
    /// maps it to an Arrow <c>TimestampType</c> with a null zone
    /// (<c>SchemaConverter.FromDeltaPrimitive</c>), so it reaches this method looking exactly like
    /// a timestamp — and #311's corpus group contains no NTZ column at all, because the harvest
    /// schema has none. Folding it here would be inventing a rule rather than reproducing a
    /// measured one, and the answer it would invent is wrong twice over: Spark resolves
    /// <c>coalesce(ntz, dt)</c> to <c>timestamp_ntz</c>, not to a zoned timestamp, and Delta's own
    /// widening permits <c>date -&gt; timestamp_ntz</c> while REFUSING <c>date -&gt;</c> a zoned
    /// timestamp, since that reinterprets a naive calendar date as an absolute instant
    /// (<c>ValueWidener</c>, and <c>TypeWideningPolicyTests.Date32ToZonedTimestamp_IsNotWidened</c>).
    /// <c>SparkArrays.SparkTypeFromName</c> draws the same line for the CAST target and for the
    /// same reason.
    /// </para>
    /// <para>
    /// So an NTZ operand falls through to exactly what this method did before #311: two of them
    /// take the identity arm and keep the left type, and an NTZ against a DATE is refused. That is
    /// not the right long-term answer — both are gaps, and the second is #311's own false
    /// rejection one type over — but closing them needs an NTZ column in the harvest schema first.
    /// Filed as #349.
    /// </para>
    /// <para>
    /// <b>The test is "clearly zoned", not "not null"</b>, so a timestamp carrying an empty zone
    /// string — which the Delta converter never produces but nothing here can rule out — takes the
    /// old path too. The conservative direction is the one that cannot silently relabel a
    /// wall-clock value as an instant.
    /// </para>
    /// </remarks>
    private static bool IsZonedOrDate(IArrowType type) =>
        SparkArrays.IsDateType(type)
        || (type is TimestampType timestamp && !string.IsNullOrEmpty(timestamp.Timezone));

    /// <summary>
    /// The decimal type Spark reads an integral LITERAL as when it meets a decimal: the narrowest
    /// one that holds the value, rather than the one that holds its type.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark's <c>DecimalType.fromLiteral</c>, reached from
    /// <c>DecimalPrecisionTypeCoercion.nondecimalAndDecimal</c> under
    /// <c>spark.sql.decimalOperations.literalPickMinimumPrecision</c> (default true). A literal
    /// <c>2</c> is a <c>decimal(1,0)</c> where an <c>int</c> COLUMN is
    /// <see cref="AsDecimal"/>'s <c>decimal(10,0)</c>, and the nine integer digits that saves are
    /// nine the result's scale keeps: measured, <c>1.5BD / 2</c> is <c>decimal(7,6)</c> and
    /// <c>d1 + 2</c> over a decimal(10,2) is <c>decimal(11,2)</c>, against the decimal(13,12) and
    /// decimal(13,2) the column rule gives. #281.
    /// </para>
    /// <para>
    /// <b>Where it applies is narrow and was measured rather than assumed.</b> Spark inserts the
    /// cast at a BINARY OPERATOR — arithmetic and comparison — and nowhere else. Unification does
    /// not take it (<c>greatest(d1, 2)</c> and <c>coalesce(d1, 2)</c> are both decimal(12,2), the
    /// int-width answer), and neither does an <c>IN</c> list: measured,
    /// <c>CAST(4E-32 AS DECIMAL(38,38)) = 0</c> is FALSE while the same value
    /// <c>IN (0)</c> is TRUE, one expression apart. So this is deliberately not reached from
    /// <see cref="CommonType"/>.
    /// </para>
    /// <para>
    /// The sign never changes the answer — <c>fromBigDecimal</c> reads a precision, and -100 and
    /// 100 both have three digits — which is what lets the caller treat Spark's folded negative
    /// literal and our <c>negative(2)</c> call as the same thing.
    /// </para>
    /// <para>
    /// <b>A tinyint literal is the exception, and it is unreachable here.</b>
    /// <c>fromLiteral</c> has cases for Short, Int and Long only, so a Byte falls through to
    /// <c>forType</c> and keeps the full decimal(3,0): measured, <c>d5 + 2Y</c> is
    /// decimal(38,34) where <c>d5 + 2S</c> is decimal(38,36). <c>SparkLiteral</c> refuses the
    /// <c>Y</c> and <c>S</c> suffixes outright — <c>LiteralValue</c> has no 8- or 16-bit kind —
    /// so no literal that reaches this can be either.
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
    /// <b>The dialects disagree about a FLOAT against an INTEGRAL</b>, and nowhere else here. #299,
    /// measured on 4.0.3 at every integral width: ANSI widens the pair to <c>double</c>, because
    /// int to float loses bits and ANSI's coercion will not; the legacy dialect takes the tightest
    /// common type, which is <c>float</c>, and rounds the integral onto it first. So
    /// <c>16777217 = CAST(16777216 AS FLOAT)</c> is false under ANSI and TRUE under legacy, and
    /// <c>i + f</c>, <c>coalesce(i, f)</c> and <c>greatest(i, f)</c> are all floats there.
    /// </para>
    /// <para>
    /// Everything else agrees across dialects: two floats stay float, anything against a double
    /// is double, and a DECIMAL against a float is double in both -- the legacy exception is for
    /// integrals only. <c>/</c> never reaches here; it is double before the operands are asked.
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
