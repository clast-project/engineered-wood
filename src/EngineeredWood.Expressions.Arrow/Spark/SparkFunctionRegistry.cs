// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Arrow-backed implementations of the Spark functions Delta expressions use.
/// </summary>
/// <remarks>
/// <para>
/// Supplied to <see cref="ArrowRowEvaluator"/>, which routes every <see cref="FunctionCall"/>
/// here. Since arithmetic is a function call in this tree rather than a node of its own, that
/// includes <c>+ - * / %</c> and unary minus alongside <c>cast</c> and <c>try_cast</c>.
/// </para>
/// <para>
/// Semantics are bound at construction through <see cref="SparkDialectOptions"/> and are not
/// otherwise configurable — see "Where the dialect configuration lives" in
/// <c>doc/predicate-pushdown-design.md</c> for why that is a constructor argument rather than a
/// parser decision or a per-call parameter.
/// </para>
/// <para>
/// <b>Not yet implemented:</b> temporal casts (<c>CAST(ts AS DATE)</c> and friends), which need
/// the timezone policy settled first, and the named functions — <c>substring</c>,
/// <c>date_format</c>, <c>year</c>, <c>concat</c>, <c>coalesce</c>, <c>case</c>, <c>like</c>.
/// Each refuses by name rather than silently producing nothing.
/// </para>
/// </remarks>
public sealed class SparkFunctionRegistry
    : IFunctionRegistry, IComparisonCoercion, IShortCircuitingFunctions
{
    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    /// <summary>
    /// The epoch seconds <see cref="DateTimeOffset"/> can represent — years 1 through 9999.
    /// </summary>
    /// <remarks>
    /// Outside this, a cast is refused rather than approximated. Spark does not refuse: measured,
    /// it accepts an arbitrarily large epoch second and its microsecond field silently overflows,
    /// landing near year 294247 — a value PySpark itself cannot then convert back. Reproducing
    /// that would put a meaningless instant into a generated column, so this deliberately differs
    /// and fails closed instead.
    /// </remarks>
    private const decimal MinEpochSecond = -62135596800m;

    private const decimal MaxEpochSecond = 253402300799m;

    private readonly SparkDialectOptions _options;

    public SparkFunctionRegistry(SparkDialectOptions? options = null)
    {
        _options = options ?? SparkDialectOptions.Default;
    }

    /// <summary>The semantics this registry implements.</summary>
    public SparkDialectOptions Options => _options;

    public bool IsRegistered(string name) => name switch
    {
        "+" or "-" or "*" or "/" or "%" or "negative" or "cast" or "try_cast" => true,
        "length" or "upper" or "lower" or "trim" or "ltrim" or "rtrim" => true,
        "substring" or "substr" or "concat" or "||" => true,
        "like" or "ilike" or "rlike" => true,
        "year" or "month" or "day" or "dayofmonth" or "hour" or "minute" or "second" => true,
        "date_format" => true,
        "coalesce" or "nvl" or "ifnull" or "nullif" or "nvl2" or "if" or "case" => true,
        "round" or "greatest" or "least" => true,
        _ => false,
    };

    public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount)
    {
        if (args is null)
            throw new ArgumentNullException(nameof(args));

        switch (name)
        {
            case "+" or "-" or "*" or "/" or "%":
                Expect(name, args, 2);
                return Arithmetic(name, args[0], args[1], rowCount);

            case "negative":
                Expect(name, args, 1);
                return Negate(args[0], rowCount);

            // try_cast is a cast that never raises. It is NOT the legacy dialect, though one flag
            // covered both for as long as every non-raising answer was null: the legacy dialect
            // ANSWERS an overflowing integral cast — 300 as a TINYINT is 44 — where try_cast
            // yields null under either dialect. Measured; see SparkIntegralCasts and #243.
            case "cast":
                Expect(name, args, 2);
                return Cast(
                    args[0], TargetTypeOf(args[1]), rowCount,
                    raising: _options.Ansi, legacy: !_options.Ansi);

            case "try_cast":
                Expect(name, args, 2);
                return Cast(args[0], TargetTypeOf(args[1]), rowCount, raising: false, legacy: false);

            case "length":
                Expect(name, args, 1);
                return SparkFunctions.Length(args[0], rowCount);

            case "upper":
                Expect(name, args, 1);
                return SparkFunctions.MapString(args[0], rowCount, t => t.ToUpperInvariant());

            case "lower":
                Expect(name, args, 1);
                return SparkFunctions.MapString(args[0], rowCount, t => t.ToLowerInvariant());

            case "trim":
                Expect(name, args, 1);
                return SparkFunctions.MapString(args[0], rowCount, t => t.Trim());

            case "ltrim":
                Expect(name, args, 1);
                return SparkFunctions.MapString(args[0], rowCount, t => t.TrimStart());

            case "rtrim":
                Expect(name, args, 1);
                return SparkFunctions.MapString(args[0], rowCount, t => t.TrimEnd());

            case "substring" or "substr":
                if (args.Count is not (2 or 3))
                    throw new ArgumentException($"'{name}' takes 2 or 3 arguments", nameof(args));
                return SparkFunctions.Substring(args, rowCount);

            case "concat" or "||":
                return SparkFunctions.Concat(args, rowCount);

            case "like" or "ilike" or "rlike":
                Expect(name, args, 2);
                return SparkFunctions.Match(name, args, rowCount);

            case "year" or "month" or "day" or "dayofmonth" or "hour" or "minute" or "second":
                Expect(name, args, 1);
                return SparkFunctions.DatePart(name, args[0], rowCount);

            case "date_format":
                Expect(name, args, 2);
                return SparkFunctions.DateFormat(args, rowCount);

            case "coalesce" or "nvl" or "ifnull":
                return Coalesce(new EagerArguments(args), rowCount);

            case "greatest" or "least":
                return Extreme(name, args, rowCount);

            case "round":
                Expect(name, args, args.Count == 1 ? 1 : 2);
                return Round(args, rowCount);

            case "nullif":
                Expect(name, args, 2);
                return NullIf(args, rowCount);

            case "if":
                Expect(name, args, 3);
                return If(new EagerArguments(args), rowCount);

            case "nvl2":
                Expect(name, args, 3);
                return Nvl2(new EagerArguments(args), rowCount);

            case "case":
                return Case(new EagerArguments(args), rowCount);

            default:
                throw new NotSupportedException(
                    $"'{name}' is not implemented by SparkFunctionRegistry.");
        }
    }

    /// <summary>
    /// The conditional family, and only it.
    /// </summary>
    /// <remarks>
    /// <c>nullif</c>, <c>greatest</c> and <c>least</c> are measured raising over the very batches
    /// where <c>coalesce</c> and <c>if</c> answer -- and they have no branch to skip in the first
    /// place, since each needs every argument before it can decide anything. The
    /// <c>short-circuit</c> corpus group carries all three, so the boundary of the family is
    /// pinned rather than assumed.
    /// </remarks>
    public bool ShortCircuits(string name) => name switch
    {
        "coalesce" or "nvl" or "ifnull" or "nvl2" or "if" or "case" => true,
        _ => false,
    };

    public IArrowArray Invoke(string name, IConditionalArguments arguments, int rowCount)
    {
        if (arguments is null)
            throw new ArgumentNullException(nameof(arguments));

        switch (name)
        {
            case "coalesce" or "nvl" or "ifnull":
                return Coalesce(arguments, rowCount);

            case "if":
                Expect(name, arguments.Count, 3);
                return If(arguments, rowCount);

            case "nvl2":
                Expect(name, arguments.Count, 3);
                return Nvl2(arguments, rowCount);

            case "case":
                return Case(arguments, rowCount);

            default:
                throw new NotSupportedException($"'{name}' does not evaluate its own arguments.");
        }
    }

    /// <summary>
    /// The arguments of a conditional invoked through <see cref="IFunctionRegistry"/>, where they
    /// arrive already evaluated.
    /// </summary>
    /// <remarks>
    /// One implementation of the conditional family serves both entry points, rather than two that
    /// have to be kept saying the same thing. There is no expression left to look at here, only a
    /// column -- and that is enough now that a bare <c>NULL</c> arrives as a <c>void</c> column
    /// rather than as an all-null string one, because the answer travels WITH the array. #293.
    /// Ignoring the row selection is safe for the same reason the arrays are usable at all: every
    /// one of these algorithms reads a branch only at the rows that selected it.
    /// </remarks>
    private sealed class EagerArguments : IConditionalArguments
    {
        private readonly IReadOnlyList<IArrowArray> _args;

        public EagerArguments(IReadOnlyList<IArrowArray> args) => _args = args;

        public int Count => _args.Count;

        public bool IsNullLiteral(int index) => _args[index].Data.DataType is NullType;

        public IArrowArray Evaluate(int index, ReadOnlySpan<bool> rows) => _args[index];
    }

    private static void Expect(string name, int count, int arity)
    {
        if (count != arity)
            throw new ArgumentException($"'{name}' takes {arity} argument(s), got {count}");
    }

    private static void Expect(string name, IReadOnlyList<IArrowArray> args, int arity)
    {
        if (args.Count != arity)
            throw new ArgumentException(
                $"'{name}' takes {arity} argument(s), got {args.Count}", nameof(args));
    }

    // ── Arithmetic ─────────────────────────────────────────────────────────────────────────

    private IArrowArray Arithmetic(string op, IArrowArray left, IArrowArray right, int rowCount)
    {
        var result = SparkNumericTypes.ArithmeticResult(op, left.Data.DataType, right.Data.DataType);

        return result switch
        {
            Decimal128Type decimalType => DecimalArithmetic(op, left, right, decimalType, rowCount),
            DoubleType => DoubleArithmetic(op, left, right, rowCount),
            FloatType => FloatArithmetic(op, left, right, rowCount),
            _ => IntegralArithmetic(op, left, right, result, rowCount),
        };
    }

    /// <summary>
    /// Integral arithmetic, computed at 64 bits and then required to fit the result width.
    /// </summary>
    /// <remarks>
    /// The width matters: Spark does not widen integral arithmetic, so <c>smallint * smallint</c>
    /// is a <c>smallint</c> and can overflow at 16 bits even though the multiplication itself was
    /// nowhere near a 64-bit limit. Computing wide and then range-checking is what makes that
    /// overflow observable rather than silently absorbed.
    /// </remarks>
    private IArrowArray IntegralArithmetic(
        string op, IArrowArray left, IArrowArray right, IArrowType resultType, int rowCount)
    {
        var values = new long?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            var a = SparkArrays.ReadInt64(left, i);
            var b = SparkArrays.ReadInt64(right, i);

            if (a is null || b is null)
                continue;

            long computed;
            try
            {
                checked
                {
                    switch (op)
                    {
                        case "+": computed = a.Value + b.Value; break;
                        case "-": computed = a.Value - b.Value; break;
                        case "*": computed = a.Value * b.Value; break;
                        case "%":
                            if (b.Value == 0)
                            {
                                if (!_options.Ansi) continue;
                                throw SparkEvaluationException.DivideByZero();
                            }

                            computed = a.Value % b.Value;
                            break;
                        default:
                            throw new NotSupportedException($"'{op}' over integers");
                    }
                }
            }
            catch (OverflowException)
            {
                if (!_options.Ansi)
                {
                    values[i] = unchecked(Wrap(op, a.Value, b.Value));
                    continue;
                }

                throw SparkEvaluationException.Overflow(
                    SparkArrays.NarrowerThanInt(resultType),
                    $"{a.Value} {op} {b.Value} overflows {SparkArrays.Describe(resultType)}");
            }

            if (!SparkArrays.FitsIn(computed, resultType))
            {
                if (!_options.Ansi)
                {
                    values[i] = SparkArrays.Truncate(computed, resultType);
                    continue;
                }

                throw SparkEvaluationException.Overflow(
                    SparkArrays.NarrowerThanInt(resultType),
                    $"{a.Value} {op} {b.Value} overflows {SparkArrays.Describe(resultType)}");
            }

            values[i] = computed;
        }

        return SparkArrays.BuildIntegral(values, resultType, rowCount);
    }

    private static long Wrap(string op, long a, long b) => op switch
    {
        "+" => unchecked(a + b),
        "-" => unchecked(a - b),
        "*" => unchecked(a * b),
        _ => 0,
    };

    private IArrowArray DoubleArithmetic(string op, IArrowArray left, IArrowArray right, int rowCount)
    {
        var builder = new DoubleArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var a = SparkArrays.ReadDouble(left, i);
            var b = SparkArrays.ReadDouble(right, i);

            if (a is null || b is null)
            {
                builder.AppendNull();
                continue;
            }

            // A zero divisor raises under ANSI even here. Measured, and not what IEEE 754 alone
            // would suggest: `g / 0.0` and `g / g2` where the column holds 0.0 both report
            // DIVIDE_BY_ZERO rather than yielding infinity.
            if (op is "/" or "%" && b.Value == 0d)
            {
                if (!_options.Ansi) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.DivideByZero();
            }

            builder.Append(op switch
            {
                "+" => a.Value + b.Value,
                "-" => a.Value - b.Value,
                "*" => a.Value * b.Value,
                "/" => a.Value / b.Value,
                "%" => a.Value % b.Value,
                _ => throw new NotSupportedException($"'{op}' over doubles"),
            });
        }

        return builder.Build();
    }

    private IArrowArray FloatArithmetic(string op, IArrowArray left, IArrowArray right, int rowCount)
    {
        var builder = new FloatArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var a = SparkArrays.ReadDouble(left, i);
            var b = SparkArrays.ReadDouble(right, i);

            if (a is null || b is null)
            {
                builder.AppendNull();
                continue;
            }

            var x = (float)a.Value;
            var y = (float)b.Value;

            if (op is "/" or "%" && y == 0f)
            {
                if (!_options.Ansi) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.DivideByZero();
            }

            builder.Append(op switch
            {
                "+" => x + y,
                "-" => x - y,
                "*" => x * y,
                "/" => x / y,
                "%" => x % y,
                _ => throw new NotSupportedException($"'{op}' over floats"),
            });
        }

        return builder.Build();
    }

    /// <summary>
    /// Decimal arithmetic, computed on the unscaled integers so that the whole of Spark's
    /// precision range is evaluable rather than only the part <see cref="decimal"/> can hold.
    /// </summary>
    private IArrowArray DecimalArithmetic(
        string op, IArrowArray left, IArrowArray right, Decimal128Type resultType, int rowCount)
    {
        var results = new Int128?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            var a = SparkWideDecimals.Read(left, i);
            var b = SparkWideDecimals.Read(right, i);

            if (a is null || b is null)
                continue;

            if (op is "/" or "%" && b.Value.IsZero)
            {
                if (!_options.Ansi) continue;
                throw SparkEvaluationException.DivideByZero();
            }

            var computed = SparkWideDecimals.Evaluate(op, a.Value, b.Value, resultType);

            if (computed is null)
            {
                if (!_options.Ansi) continue;

                // Spark's own message names the exact result, which we no longer hold once it has
                // been rejected. The operands are as informative and cost nothing to keep: the
                // error CLASS is the part a caller matches on, not the wording.
                throw SparkEvaluationException.NumericValueOutOfRange(
                    $"{Show(a.Value)} {op} {Show(b.Value)}", resultType);
            }

            results[i] = computed;
        }

        return SparkWideDecimals.Build(results, resultType, rowCount);
    }

    /// <summary>An operand as Spark would print it, for an overflow message.</summary>
    private static string Show(SparkWideDecimals.Operand operand) => SparkWideDecimals.Render(operand);

    private IArrowArray Negate(IArrowArray operand, int rowCount)
    {
        var type = SparkNumericTypes.NegateResult(operand.Data.DataType);

        return type switch
        {
            Decimal128Type d => DecimalArithmetic("-", ZeroLike(d, rowCount), operand, d, rowCount),
            DoubleType => DoubleArithmetic("-", ZeroLike(DoubleType.Default, rowCount), operand, rowCount),
            FloatType => FloatArithmetic("-", ZeroLike(FloatType.Default, rowCount), operand, rowCount),
            _ => IntegralArithmetic("-", ZeroLike(type, rowCount), operand, type, rowCount),
        };
    }

    /// <summary>An all-zero array of <paramref name="type"/>, so negation reuses subtraction.</summary>
    /// <remarks>
    /// Subtracting from zero rather than negating in place is what makes
    /// <c>-(-2147483648)</c> raise instead of wrapping back to itself, since the range check on
    /// the subtraction catches a result the operand's own width cannot hold.
    /// </remarks>
    private static IArrowArray ZeroLike(IArrowType type, int rowCount)
    {
        if (type is Decimal128Type d)
        {
            var decimals = new Decimal128Array.Builder(d);
            for (var i = 0; i < rowCount; i++) decimals.Append(0m);
            return decimals.Build();
        }

        if (type is DoubleType)
        {
            var doubles = new DoubleArray.Builder();
            for (var i = 0; i < rowCount; i++) doubles.Append(0d);
            return doubles.Build();
        }

        if (type is FloatType)
        {
            var floats = new FloatArray.Builder();
            for (var i = 0; i < rowCount; i++) floats.Append(0f);
            return floats.Build();
        }

        var values = new long?[rowCount];
        for (var i = 0; i < rowCount; i++) values[i] = 0L;
        return SparkArrays.BuildIntegral(values, type, rowCount);
    }

    // ── COMPARISON COERCION ────────────────────────────────────────────────────────────────

    /// <inheritdoc />
    /// <remarks>
    /// Two rules, and they move different operands.
    /// <list type="bullet">
    ///   <item>A STRING against a number, a boolean or an instant is cast to it —
    ///     <see cref="StringComparisonTarget"/> for which type exactly, since the dialects
    ///     disagree.</item>
    ///   <item>A BINARY against a string is rendered AS a string, and it is the binary that
    ///     moves. Measured: <c>CAST(X'FF' AS STRING) = X'FF'</c> is true, which only holds if
    ///     both sides became text — cast the other way, U+FFFD's three UTF-8 bytes are not
    ///     <c>FF</c>. Both dialects agree, and so do <c>'A' = bin</c> (true against
    ///     <c>X'41'</c>) and <c>'B' &gt; bin</c>.</item>
    ///   <item>An EXACT NUMERIC against another — a decimal, or an integral read as one — is
    ///     compared through their least common type, and <b>both</b> operands can move.
    ///     <see cref="LossyDecimalTarget"/>; #280.</item>
    /// </list>
    /// A pair with no rule gets null from both operands and is compared as it stands.
    /// </remarks>
    public IArrowType? ComparisonTarget(IArrowType operand, IArrowType other)
    {
        if (operand is null)
            throw new ArgumentNullException(nameof(operand));
        if (other is null)
            throw new ArgumentNullException(nameof(other));

        if (operand is StringType)
            return StringComparisonTarget(other);

        // Checked after the string case: a string operand is never the one that moves here.
        if (operand is BinaryType && other is StringType)
            return StringType.Default;

        return LossyDecimalTarget(operand, other);
    }

    /// <summary>
    /// The least common type an exact numeric must be rounded to before comparison, or null when
    /// comparing the values as they stand gives the same answer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// #280. Spark casts both operands of a comparison to their least common type and compares
    /// the results, so once that type gives up scale — see
    /// <see cref="SparkNumericTypes.ClampPreferringIntegralDigits"/> — the comparison is made on
    /// ROUNDED values. Measured, <c>CAST(1.005 AS DECIMAL(4,3)) = CAST(1 AS DECIMAL(38,0))</c>
    /// is TRUE, and <c>&gt;</c> over the same pair is FALSE: the common type is decimal(38,0)
    /// and 1.005 rounds to 1 before either question is asked.
    /// </para>
    /// <para>
    /// <b>An integral counts, and its WIDTH decides the answer.</b> An integral unifies as the
    /// decimal that holds it, so it widens the common type's integer part and squeezes the
    /// scale. Measured against <c>CAST(4E-32 AS DECIMAL(38,38))</c>, <c>= 0</c> is TRUE for an
    /// <c>int</c> (common decimal(38,28), the value rounds away) and TRUE for a <c>bigint</c>
    /// (decimal(38,18)) but FALSE for a <c>tinyint</c> (decimal(38,35), which still holds it).
    /// Three answers from one comparison is why this cannot be special-cased to decimal pairs.
    /// </para>
    /// <para>
    /// Null whenever the common type keeps at least this operand's scale, which is the ordinary
    /// case: casting to a scale that loses nothing cannot change an ordering, and skipping it
    /// keeps a per-row cast out of the comparison path that predicate pushdown runs.
    /// </para>
    /// </remarks>
    private static IArrowType? LossyDecimalTarget(IArrowType operand, IArrowType other)
    {
        // Floating point is not exact and does not unify as a decimal -- a decimal against a
        // double is compared as a double, which is a rule this must not intercept. #277.
        if (!SparkWideDecimals.IsExact(operand) || !SparkWideDecimals.IsExact(other))
            return null;

        // Two integrals share a scale of zero and can never round, so there is nothing to do.
        if (!SparkNumericTypes.IsDecimal(operand) && !SparkNumericTypes.IsDecimal(other))
            return null;

        if (SparkNumericTypes.CommonType(operand, other) is not Decimal128Type common)
            return null;

        return common.Scale < SparkNumericTypes.AsDecimal(operand).Scale ? common : null;
    }

    /// <inheritdoc />
    /// <remarks>
    /// <para>
    /// One type over the operand and every member, which is Spark's model for <c>IN</c> and not
    /// the pairwise one a comparison uses. The dialects reach it differently:
    /// </para>
    /// <list type="bullet">
    ///   <item><b>ANSI</b> takes the members that are not strings, finds their common type, and
    ///     applies the comparison rule to it — so <c>ns IN (1.5, 2)</c> resolves through
    ///     <c>double</c> because a decimal and an int do, while <c>ns IN (1, 2)</c> resolves
    ///     through <c>bigint</c>.</item>
    ///   <item><b>The legacy dialect</b> promotes everything to STRING instead. That is what
    ///     makes <c>a IN ('01')</c> false where <c>a = '01'</c> is true, and
    ///     <c>d1 IN ('12.340')</c> false where the numbers are equal.</item>
    /// </list>
    /// <para>
    /// Spark's string promotion excludes boolean and binary, and measured, it refuses those sets
    /// outright rather than answering: <c>bl IN ('true')</c> and <c>bin IN ('A')</c> are
    /// analysis errors under the legacy dialect and answers under ANSI. Refusing is not
    /// reproduced here — this returns null, the set is compared as it stands, and the difference
    /// is declared in <c>SparkEvaluationCorpusTests</c>.
    /// </para>
    /// </remarks>
    public IArrowType? SetComparisonTarget(IReadOnlyList<IArrowType> memberTypes)
    {
        if (memberTypes is null)
            throw new ArgumentNullException(nameof(memberTypes));

        var common = CommonTypeOfNonStrings(memberTypes, out bool anyString);
        if (common is null)
            return null;   // all strings already, or no common type

        if (!anyString)
        {
            // #280. No string to promote, but a set of exact numerics still resolves through one
            // type, and that type can give up scale. Measured,
            // `CAST(1.005 AS DECIMAL(4,3)) IN (CAST(1 AS DECIMAL(38,0)))` is TRUE -- the same
            // rounding a comparison against that operand does, which is what makes IN and `=`
            // agree here even though they disagree over a string. Null unless some member
            // actually loses scale, so an ordinary set is still compared as it stands.
            if (common is not Decimal128Type decimalCommon)
                return null;

            // EVERY member has to be one the exact-decimal path can actually move, not just the
            // one that loses scale, because the caller casts them all. A decimal256 is the case
            // that separates the two: `CommonType` reads its precision and scale happily and
            // hands back a decimal128 common type, but `SparkWideDecimals` cannot read one, so
            // returning a target here turns an answer into a throw. Measured before this guard:
            // a decimal(38,38) column `IN` a decimal256(38,0) column threw where it used to
            // answer. Bailing leaves the exact comparison, which is what the pair had before.
            var anyRounds = false;
            foreach (var type in memberTypes)
            {
                if (!SparkWideDecimals.IsExact(type))
                    return null;

                if (decimalCommon.Scale < SparkNumericTypes.AsDecimal(type).Scale)
                    anyRounds = true;
            }

            return anyRounds ? decimalCommon : null;
        }

        if (_options.Ansi)
        {
            // A binary renders as text here for the same reason it does in a comparison, and
            // measured it answers rather than refusing: `bin IN ('A')` is false under ANSI.
            return common is BinaryType ? StringType.Default : StringComparisonTarget(common);
        }

        return SparkNumericTypes.IsNumeric(common)
            || SparkArrays.IsDateType(common)
            || common is TimestampType
                ? StringType.Default
                : null;
    }

    /// <summary>
    /// The type the non-string members share, or null when they have none.
    /// </summary>
    /// <remarks>
    /// Two members of the same type need no unification, which is what keeps a set of dates or
    /// booleans out of <see cref="SparkNumericTypes.CommonType"/> — it speaks for numbers, and
    /// refuses everything else by design.
    /// </remarks>
    private static IArrowType? CommonTypeOfNonStrings(
        IReadOnlyList<IArrowType> memberTypes, out bool anyString)
    {
        anyString = false;
        IArrowType? common = null;

        foreach (var type in memberTypes)
        {
            if (type is StringType)
            {
                anyString = true;
                continue;
            }

            if (common is null)
            {
                common = type;
                continue;
            }

            if (common.GetType() == type.GetType() && !SparkNumericTypes.IsDecimal(type))
                continue;

            try
            {
                common = SparkNumericTypes.CommonType(common, type);
            }
            catch (NotSupportedException)
            {
                return null;
            }
        }

        return common;
    }

    /// <inheritdoc />
    /// <remarks>
    /// The cast is the SAME one <c>CAST(...)</c> reaches, dialect and all, so a comparison and an
    /// explicit cast cannot drift apart: <c>s = a</c> refuses exactly the strings
    /// <c>CAST(s AS BIGINT)</c> refuses.
    /// </remarks>
    public IArrowArray CastForComparison(IArrowArray operand, IArrowType target, int rowCount)
    {
        if (operand is null)
            throw new ArgumentNullException(nameof(operand));
        if (target is null)
            throw new ArgumentNullException(nameof(target));

        return Cast(operand, target, rowCount, raising: _options.Ansi, legacy: !_options.Ansi);
    }

    /// <summary>
    /// The type a string is cast to before being compared against <paramref name="other"/>, or
    /// null when the pair takes no coercion.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One rule with one exception: the string takes the other operand's OWN type, and ANSI
    /// widens a numeric one first — to <c>BIGINT</c> for every integral width, and to
    /// <c>DOUBLE</c> for float, double and decimal alike. Boolean, date and timestamp take the
    /// other side's type under both dialects; the dialects then differ only in what a malformed
    /// value does, which is the ordinary raise-or-null split.
    /// </para>
    /// <para>
    /// <b>The widening is not cosmetic, and it is the half the issue did not record.</b> Each of
    /// these is one expression with two answers, measured against Spark 4.0 and pinned by the
    /// <c>string-coercion</c> group of the corpus:
    /// </para>
    /// <list type="bullet">
    ///   <item><c>'32768' = sh</c> over a <c>smallint</c>: false under ANSI, where the string
    ///     becomes a <c>bigint</c>; null under the legacy dialect, where it overflows the
    ///     <c>smallint</c> it is cast to.</item>
    ///   <item><c>'0.1' = CAST(0.1 AS FLOAT)</c>: false under ANSI, where <c>0.1</c> as a double
    ///     is not <c>0.1f</c> widened; true under the legacy dialect, which casts to
    ///     <c>float</c>.</item>
    ///   <item><c>'1000000000000000000000000000001' = d4</c> over a <c>decimal(38,0)</c>: true
    ///     under ANSI, where both sides collapse to the same double; false under the legacy
    ///     dialect, which keeps the decimal exact.</item>
    /// </list>
    /// <para>
    /// Binary is not here because a binary does not cast a string: it is the binary that moves.
    /// See <see cref="ComparisonTarget"/>.
    /// </para>
    /// </remarks>
    private IArrowType? StringComparisonTarget(IArrowType other)
    {
        // No widening for these: a string against a boolean, a date or a timestamp is cast
        // straight to it, under both dialects.
        if (other is BooleanType || other is TimestampType || SparkArrays.IsDateType(other))
            return other;

        if (!_options.Ansi)
            return SparkNumericTypes.IsNumeric(other) ? LegacyTarget(other) : null;

        if (SparkNumericTypes.IsIntegral(other))
            return Int64Type.Default;

        if (SparkNumericTypes.IsFloatingPoint(other) || SparkNumericTypes.IsDecimal(other))
            return DoubleType.Default;

        return null;
    }

    /// <summary>
    /// The legacy dialect's target: the operand's own type, or null when there is no cast to it.
    /// </summary>
    /// <remarks>
    /// A decimal needs reading at the width <see cref="Cast"/> dispatches on, since a
    /// <c>decimal(38,0)</c> can arrive as a <see cref="Decimal256Type"/> and the cast has a case
    /// only for <see cref="Decimal128Type"/>. Precision and scale are what it actually needs.
    /// <para>
    /// <b>Past Spark's maximum precision there is no rule and no cast.</b> Parquet's decimal runs
    /// wider than Spark's — <c>ArrowSchemaConverter</c> builds a <see cref="Decimal256Type"/> for
    /// precision above 38 — and no Spark expression can name such a type, so nothing measured
    /// says what comparing a string against one means. Handing it on would be worse than
    /// declining: <see cref="Decimal128Type"/> does not validate its precision, so a
    /// <c>decimal(50,0)</c> became a 16-byte decimal claiming fifty digits and the comparison
    /// answered from it. Measured, before this returned null.
    /// </para>
    /// </remarks>
    private static IArrowType? LegacyTarget(IArrowType numeric)
    {
        if (!SparkNumericTypes.IsDecimal(numeric))
            return numeric;

        var (precision, scale) = SparkNumericTypes.AsDecimal(numeric);
        return precision <= SparkNumericTypes.MaxPrecision
            ? new Decimal128Type(precision, scale)
            : null;
    }

    // ── CAST ───────────────────────────────────────────────────────────────────────────────

    /// <summary>Reads the target type out of the cast's second argument.</summary>
    /// <remarks>
    /// The parser carries it as a string literal — <c>cast(expr, 'DECIMAL(10,2)')</c> — because
    /// a type is not an expression and this tree has nowhere else to put it.
    /// </remarks>
    private static IArrowType TargetTypeOf(IArrowArray argument)
    {
        if (argument is not StringArray names || names.Length == 0)
            throw new ArgumentException("cast expects its target type as a string literal");

        var text = names.GetString(0);
        if (string.IsNullOrWhiteSpace(text))
            throw new ArgumentException("cast target type is empty");

        return SparkArrays.ParseTypeName(text!);
    }

    /// <summary>
    /// Casts a column, where <paramref name="raising"/> and <paramref name="legacy"/> together say
    /// what a failure does.
    /// </summary>
    /// <remarks>
    /// Three states, not two: <c>raising</c> is ANSI's cast, <c>legacy</c> is the non-ANSI
    /// dialect's, and NEITHER is try_cast, which yields null whichever dialect is in force. Only
    /// <see cref="CastToIntegral"/> tells the last two apart — every other target nulls under both
    /// — so <paramref name="legacy"/> reaches only that one.
    /// </remarks>
    private IArrowArray Cast(IArrowArray source, IArrowType target, int rowCount, bool raising, bool legacy)
    {
        // A `void` source is null at every row, so it is null at the target too -- whatever the
        // target is, and in either dialect, since there is no value to be malformed. Answered here
        // rather than in each CastToX because it is the same answer for all of them, and because
        // two of those refuse a source they do not recognise: measured, `CAST(NULL AS BINARY)` is
        // a binary null in Spark where CastToBinary would have said VOID cannot become one. #293.
        if (source.Data.DataType is NullType)
            return ArrowCompute.MakeNullArray(target, rowCount);

        if (target is Decimal128Type decimalTarget)
            return CastToDecimal(source, decimalTarget, rowCount, raising);

        if (target is StringType)
            return CastToString(source, rowCount);

        if (target is BinaryType)
            return CastToBinary(source, rowCount, legacy);

        if (SparkNumericTypes.IsIntegral(target))
            return CastToIntegral(source, target, rowCount, raising, legacy);

        if (target is DoubleType or FloatType)
            return CastToFloatingPoint(source, target, rowCount, raising);

        if (target is BooleanType)
            return CastToBoolean(source, rowCount, raising);

        if (SparkArrays.IsDateType(target))
            return CastToDate(source, rowCount, raising);

        if (target is TimestampType)
            return CastToTimestamp(source, rowCount, raising);

        throw new NotSupportedException(
            $"cast to {SparkArrays.Describe(target)} is not implemented.");
    }

    /// <summary>
    /// Casts to a calendar date, taking the date the instant falls on in the resolved timezone.
    /// </summary>
    /// <remarks>
    /// This is where <see cref="SparkDialectOptions.TimeZone"/> is load-bearing rather than
    /// decorative. The instant 2026-08-11T03:00Z is 2026-08-11 in UTC and 2026-08-10 in
    /// America/Los_Angeles, so a generated column defined as <c>CAST(ts AS DATE)</c> stores a
    /// different value depending on which zone resolves it. UTC is the fixed choice; see the
    /// option for why it is not settable.
    /// </remarks>
    private IArrowArray CastToDate(IArrowArray source, int rowCount, bool raising)
    {
        var instants = new DateTimeOffset?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);
            if (value is null)
                continue;

            if (value.Value.Instant is { } instant)
            {
                instants[i] = instant;
                continue;
            }

            if (value.Value.FromString
                && DateTimeOffset.TryParse(value.Value.Text.Trim(), Invariant,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                instants[i] = parsed;
                continue;
            }

            if (!raising) continue;
            throw SparkEvaluationException.InvalidCast(value.Value.Text, "DATE");
        }

        return SparkArrays.BuildDate32(instants, rowCount);
    }

    private IArrowArray CastToTimestamp(IArrowArray source, int rowCount, bool raising)
    {
        var instants = new DateTimeOffset?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);
            if (value is null)
                continue;

            if (value.Value.Instant is { } instant)
            {
                instants[i] = instant;
                continue;
            }

            if (value.Value.FromString)
            {
                if (DateTimeOffset.TryParse(value.Value.Text.Trim(), Invariant,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
                {
                    instants[i] = parsed;
                    continue;
                }

                if (!raising) continue;
                throw SparkEvaluationException.InvalidCast(value.Value.Text, "TIMESTAMP");
            }

            // A number is epoch seconds, which is the inverse of casting a timestamp to a number.
            if (value.Value.IsNumeric)
            {
                // From the exact decimal, not the double: past 2^53 a double can no longer hold
                // every integer, and a shifted instant is worse than a refused one.
                if (value.Value.Exact is not { } seconds
                    || seconds < MinEpochSecond || seconds > MaxEpochSecond)
                {
                    if (!raising) continue;
                    throw SparkEvaluationException.CastOverflow(value.Value.Text, "TIMESTAMP");
                }

                instants[i] = DateTimeOffset.FromUnixTimeSeconds((long)decimal.Truncate(seconds));
                continue;
            }

            if (!raising) continue;
            throw SparkEvaluationException.InvalidCast(value.Value.Text, "TIMESTAMP");
        }

        return SparkArrays.BuildTimestamp(instants, rowCount);
    }

    /// <summary>Casts a column to an integral type.</summary>
    /// <remarks>
    /// Under ANSI an overflow raises and the source type only decides the error class. With the
    /// legacy dialect it decides the ANSWER, and there are four rules rather than one — see
    /// <see cref="SparkIntegralCasts"/>, where each is measured. #243.
    /// </remarks>
    private IArrowArray CastToIntegral(
        IArrowArray source, IArrowType target, int rowCount, bool raising, bool legacy)
    {
        var values = new long?[rowCount];
        var described = SparkArrays.Describe(target);
        var family = SparkIntegralCasts.FamilyOf(source.Data.DataType);

        // Every failure of a STRING source is CAST_INVALID_INPUT and never CAST_OVERFLOW, whether
        // the text was malformed or merely too large. Measured, and it follows from the parse
        // being what failed: Spark's integral parser does not read a value it cannot hold, so
        // there is no overflow for it to report.
        SparkEvaluationException Refuse(string text) =>
            family == SparkIntegralCasts.Source.Text
                ? SparkEvaluationException.InvalidCast(text, described)
                : SparkEvaluationException.CastOverflow(text, described);

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);
            if (value is null)
                continue;

            // Spark refuses a date-to-integer cast outright, while a timestamp becomes epoch
            // seconds. Measured: CAST(DATE'…' AS LONG) is an error.
            if (value.Value.IsDate)
                throw new NotSupportedException($"cast from DATE to {described} is not allowed");

            if (!value.Value.IsNumeric)
            {
                if (!raising) continue;
                throw SparkEvaluationException.InvalidCast(value.Value.Text, described);
            }

            // Spark's integral parse is stricter than the one that produced this value: it takes
            // a sign, digits and an optional point, and NO exponent -- so `CAST('1e3' AS BIGINT)`
            // fails where `CAST('1e3' AS DOUBLE)` is 1000. Both dialects refuse it; only what
            // the failure looks like differs. #258.
            var form = value.Value.FromString
                ? SparkIntegralCasts.Classify(value.Value.Text)
                : SparkIntegralCasts.TextForm.Integer;

            if (form == SparkIntegralCasts.TextForm.Invalid)
            {
                if (!raising) continue;
                throw SparkEvaluationException.InvalidCast(value.Value.Text, described);
            }

            // No exact form means the magnitude is past decimal's range, and so past every
            // integral type's range too.
            if (value.Value.Exact is not { } exact)
            {
                if (raising) throw Refuse(value.Value.Text);

                values[i] = Overflowed(i, value.Value.AsDouble);
                continue;
            }

            // A string carrying a decimal POINT is where the dialects part: ANSI refuses it, and
            // the legacy dialect TRUNCATES toward zero and carries on to the range check below.
            // Both measured — CAST('12.5' AS INT) is an error under ANSI and 12 without it,
            // while CAST('300.5' AS TINYINT) is null because 300 does not fit rather than
            // because of the fraction. try_cast takes ANSI's reading of the value and nulls it.
            //
            // The point, not the fraction: ANSI refuses '1.0', '0.0' and '10.' too, so asking
            // whether the VALUE survives truncation accepted all three. #258.
            if (!legacy && form == SparkIntegralCasts.TextForm.Fractional)
            {
                if (!raising) continue;
                throw SparkEvaluationException.InvalidCast(value.Value.Text, described);
            }

            var truncated = decimal.Truncate(exact);

            if (truncated < long.MinValue || truncated > long.MaxValue
                || !SparkArrays.FitsIn((long)truncated, target))
            {
                if (raising) throw Refuse(value.Value.Text);

                values[i] = Overflowed(i, value.Value.AsDouble);
                continue;
            }

            values[i] = (long)truncated;
        }

        return SparkArrays.BuildIntegral(values, target, rowCount);

        // What the legacy dialect answers for this row: a different rule per source family, and
        // null both for the two families that have none and for try_cast, which never answers.
        long? Overflowed(int index, double asDouble) => !legacy ? null : family switch
        {
            SparkIntegralCasts.Source.Exact => SparkIntegralCasts.Wrap(source, index, target),
            SparkIntegralCasts.Source.Floating => SparkIntegralCasts.Saturate(asDouble, target),
            _ => null,
        };
    }

    private IArrowArray CastToFloatingPoint(IArrowArray source, IArrowType target, int rowCount, bool raising)
    {
        var doubles = target is DoubleType ? new DoubleArray.Builder() : null;
        var floats = target is FloatType ? new FloatArray.Builder() : null;

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);

            // Java's floating-point literal takes a trailing type suffix, and Spark's parse is
            // Java's: CAST('1d' AS DOUBLE) is 1.0 and CAST('1.5f' AS FLOAT) is 1.5, where .NET
            // reads neither. It attaches to a NUMERIC form only -- 'NaNd' and 'Infinityf' are
            // refused -- and only a floating target takes it, CAST('1d' AS DECIMAL(20,4)) being
            // an error. Measured; #258.
            if (value is { IsNumeric: false, FromString: true }
                && SparkArrays.TryReadTypeSuffixed(value.Value.Text, out var suffixed))
            {
                doubles?.Append(suffixed);
                floats?.Append((float)suffixed);
                continue;
            }

            if (value is null || !value.Value.IsNumeric)
            {
                if (value is not null && !raising)
                {
                    doubles?.AppendNull();
                    floats?.AppendNull();
                    continue;
                }

                if (value is not null)
                    throw SparkEvaluationException.InvalidCast(
                        value.Value.Text, SparkArrays.Describe(target));

                doubles?.AppendNull();
                floats?.AppendNull();
                continue;
            }

            doubles?.Append(value.Value.AsDouble);
            floats?.Append((float)value.Value.AsDouble);
        }

        return (IArrowArray?)doubles?.Build() ?? floats!.Build();
    }

    private IArrowArray CastToDecimal(IArrowArray source, Decimal128Type target, int rowCount, bool raising)
    {
        // A decimal or integral source has an exact unscaled form, so the cast is a rescale and the
        // whole precision range is reachable.
        if (SparkWideDecimals.IsExact(source.Data.DataType))
            return CastExactToDecimal(source, target, rowCount, raising);

        // A string is the only other source that can spell a value past System.Decimal's ~7.9e28,
        // and #174 measured what Spark does when one does, so it reads exactly too.
        if (source is StringArray strings)
            return CastStringToDecimal(strings, target, rowCount, raising);

        // Floating point goes through its RENDERING, which is what Spark converts. #244.
        if (source.Data.DataType is FloatType or DoubleType)
            return CastFloatingToDecimal(source, target, rowCount, raising);

        // What is left keeps the System.Decimal path below, and cannot reach its ~7.9e28 ceiling:
        // a boolean is 0 or 1 and a temporal is epoch seconds.
        var builder = new Decimal128Array.Builder(target);

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);

            if (value is null)
            {
                builder.AppendNull();
                continue;
            }

            if (!value.Value.IsNumeric)
            {
                if (!raising) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.InvalidCast(value.Value.Text, SparkArrays.Describe(target));
            }

            // Both refusals below are NUMERIC_VALUE_OUT_OF_RANGE rather than CAST_OVERFLOW, which
            // is what this used to report. Measured: a cast to a decimal names that condition
            // whatever the source is — decimal, double, string or integer all reach it — while
            // CAST_OVERFLOW belongs to casts targeting an integral type.
            if (value.Value.Exact is not { } exact)
            {
                if (!raising) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.NumericValueOutOfRange(value.Value.Text, target);
            }

            try
            {
                builder.Append(SparkArrays.Rescale(exact, target.Scale));
            }
            catch (OverflowException)
            {
                if (!raising) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.NumericValueOutOfRange(value.Value.Text, target);
            }
        }

        return builder.Build();
    }

    /// <summary>
    /// Casts a floating-point column to a decimal, through Spark's own rendering of the value.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Spark converts the RENDERING, not the binary expansion.</b> It reaches a decimal through
    /// <c>BigDecimal.valueOf(d)</c>, which is <c>new BigDecimal(Double.toString(d))</c>, so the
    /// answer is the shortest decimal that round-trips the double rather than the exact binary
    /// value. The float row of the corpus is the proof: <c>1e30f</c> widens to the double
    /// 1.0000000150474662E30 and Spark answers those digits, where the exact value of the float
    /// and the digits of <c>1e30</c> are both something else.
    /// </para>
    /// <para>
    /// This replaces a <see cref="decimal"/> conversion that was wrong in two directions. Past
    /// <see cref="decimal"/>'s ~7.9e28 there was no exact form at all and the cast was REFUSED —
    /// the gap #244 was filed for. Inside it, <c>(decimal)double</c> rounds to 15 significant
    /// digits where Spark keeps up to 17, so it quietly lost digits: measured over ~1e6 doubles,
    /// it disagreed with Spark's rendering on 93% of the values a decimal could hold at all.
    /// </para>
    /// <para>
    /// <b>The JVM is part of the answer here, and #244 held the fix back until that was
    /// measured.</b> <c>Double.toString</c> did not produce the shortest representation before
    /// JDK 19, so a Spark on 17 and one on 21 do not agree. Measured against this corpus's JDK
    /// over ~1e6 doubles: they differ on 2.4% of them — needing 17 or 18 digits where the
    /// shortest form needs 16 or 17 — and on NONE of the 130,152 sampled past 7.9e28, which is
    /// the whole of the range the refusal covered. So the fix lands where the JVM does not
    /// matter, and shrinks the disagreement below it from 93% to that 2.4% band.
    /// <c>java_version</c> now sits beside <c>conf</c> in the fixture, and the three corpus rows
    /// that land in the band are declared differences rather than a remark.
    /// </para>
    /// </remarks>
    private IArrowArray CastFloatingToDecimal(
        IArrowArray source, Decimal128Type target, int rowCount, bool raising)
    {
        var mantissas = new Int128?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            // Widened for a float source, because that is what Spark renders — the corpus's
            // 1e30f row answers the double's digits and not the float's.
            if (SparkArrays.ReadDouble(source, i) is not { } value)
                continue;

            // NaN and the infinities yield NULL rather than raising, EVEN UNDER ANSI. Measured,
            // and it is the one refusal on this path that is not an error.
            if (double.IsNaN(value) || double.IsInfinity(value))
                continue;

            var text = SparkFloatText.ShortestRoundTrip(value);

            if (SparkDecimalText.TryRead(text, target, out var unscaled) == SparkDecimalText.Result.Ok)
            {
                mantissas[i] = unscaled;
                continue;
            }

            if (!raising) continue;

            // Every failure here is NUMERIC_VALUE_OUT_OF_RANGE, including the one a STRING source
            // reports as NUMERIC_OUT_OF_SUPPORTED_RANGE: measured, CAST(1e39 AS DOUBLE) to a
            // DECIMAL(38,0) names the first. The two sources reach the decimal by different
            // routes and only the string one meets Spark's digit-count fast-fail.
            throw SparkEvaluationException.NumericValueOutOfRange(text, target);
        }

        return SparkWideDecimals.Build(mantissas, target, rowCount);
    }

    /// <summary>Casts a string column to a decimal type, reading the text exactly.</summary>
    /// <remarks>
    /// Three refusals with three different error classes, all measured rather than reasoned —
    /// see <see cref="SparkDecimalText"/> for what each one is and why the middle one is not the
    /// one anybody would have guessed.
    /// </remarks>
    private IArrowArray CastStringToDecimal(
        StringArray source, Decimal128Type target, int rowCount, bool raising)
    {
        var mantissas = new Int128?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            if (source.IsNull(i))
                continue;

            var text = source.GetString(i);

            switch (SparkDecimalText.TryRead(text, target, out var unscaled))
            {
                case SparkDecimalText.Result.Ok:
                    mantissas[i] = unscaled;
                    break;

                case SparkDecimalText.Result.Malformed:
                    if (!raising) continue;
                    throw SparkEvaluationException.InvalidCast(text, SparkArrays.Describe(target));

                case SparkDecimalText.Result.TooManyDigits:
                    if (!raising) continue;
                    throw SparkEvaluationException.NumericOutOfSupportedRange(text);

                default:
                    if (!raising) continue;
                    throw SparkEvaluationException.NumericValueOutOfRange(text, target);
            }
        }

        return SparkWideDecimals.Build(mantissas, target, rowCount);
    }

    /// <summary>Casts a decimal or integral column to a decimal type, on the unscaled integers.</summary>
    private IArrowArray CastExactToDecimal(
        IArrowArray source, Decimal128Type target, int rowCount, bool raising)
    {
        var mantissas = new Int128?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            if (SparkWideDecimals.Read(source, i) is not { } value)
                continue;

            var cast = SparkWideDecimals.Cast(value, target);

            if (cast is null)
            {
                if (!raising) continue;

                throw SparkEvaluationException.NumericValueOutOfRange(
                    SparkWideDecimals.Render(value), target);
            }

            mantissas[i] = cast;
        }

        return SparkWideDecimals.Build(mantissas, target, rowCount);
    }

    private static IArrowArray CastToString(IArrowArray source, int rowCount)
    {
        var builder = new StringArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);
            if (value is null) builder.AppendNull();
            else builder.Append(value.Value.Text);
        }

        return builder.Build();
    }

    /// <summary>Casts a column to BINARY, which only two source families reach.</summary>
    /// <remarks>
    /// <para>
    /// <b>A string is a UTF-8 encode and a binary is the identity</b> — those two in both
    /// dialects. Measured: <c>CAST('é' AS BINARY)</c> is <c>C3A9</c>, <c>CAST('' AS BINARY)</c> is
    /// empty rather than null, and a null stays null. The reverse direction,
    /// <c>CAST(bin AS STRING)</c>, already worked and is a UTF-8 DECODE that replaces what is not
    /// valid, so <c>CAST(X'FF' AS STRING)</c> is U+FFFD — which means the round trip is not the
    /// identity and the corpus carries both halves.
    /// </para>
    /// <para>
    /// <b>An integral is the legacy dialect's ALONE, and this is the part #295 did not name.</b>
    /// Measured on 4.0.3: with ansi off, <c>CAST(CAST(1 AS INT) AS BINARY)</c> is
    /// <c>00000001</c> — big-endian, at the source type's own width, so a TINYINT gives one byte
    /// and a BIGINT eight, and a negative value gives its two's complement (<c>CAST(-2 AS
    /// SMALLINT)</c> is <c>FFFE</c>). With ANSI on the same cast is refused outright, as
    /// <c>DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION</c>, whose whole content is "turn ANSI off".
    /// </para>
    /// <para>
    /// <b>try_cast is refused too, under BOTH dialects</b>, which is why this keys on
    /// <paramref name="legacy"/> rather than on <c>!raising</c>: measured,
    /// <c>TRY_CAST(CAST(1 AS INT) AS BINARY)</c> is <c>CAST_WITHOUT_SUGGESTION</c> even with ansi
    /// off, because try_cast type-checks as ANSI does. The three-state <c>raising</c>/
    /// <paramref name="legacy"/> pair from #243 already distinguishes exactly those three callers.
    /// </para>
    /// <para>
    /// Everything else — float, double, decimal, boolean, date, timestamp — is refused in both
    /// dialects, and refused as a TYPE error rather than a per-row one, which is what Spark does
    /// with it. Same treatment as <c>SparkNumericTypes</c>'s "no common type", and deliberately
    /// not a <see cref="SparkEvaluationException"/>: nothing here depends on the row's value.
    /// </para>
    /// </remarks>
    private static IArrowArray CastToBinary(IArrowArray source, int rowCount, bool legacy)
    {
        var type = source.Data.DataType;

        if (SparkNumericTypes.IsIntegral(type))
        {
            if (!legacy)
            {
                throw new NotSupportedException(
                    $"Spark refuses CAST({SparkArrays.Describe(type)} AS BINARY) unless " +
                    "spark.sql.ansi.enabled is false, and try_cast refuses it either way");
            }

            return CastIntegralToBinary(source, type, rowCount);
        }

        // One check, not one per row -- and a StringArray satisfies it, because it derives from
        // BinaryArray and its buffer already holds the UTF-8 this cast wants.
        if (source is not BinaryArray bytes)
        {
            throw new NotSupportedException(
                $"Spark has no cast from {SparkArrays.Describe(type)} to BINARY in either dialect");
        }

        var builder = new BinaryArray.Builder();
        for (var i = 0; i < rowCount; i++)
        {
            if (bytes.IsNull(i)) builder.AppendNull();
            else builder.Append(bytes.GetBytes(i));
        }

        // The spans above point into `bytes`'s buffer; see doc/arrow-span-lifetime.md.
        GC.KeepAlive(bytes);
        return builder.Build();
    }

    /// <summary>An integral column as big-endian bytes at its own width, for the legacy dialect.</summary>
    private static IArrowArray CastIntegralToBinary(IArrowArray source, IArrowType type, int rowCount)
    {
        // The WIDTH is the source type's, not the value's: 1 as a BIGINT is eight bytes and 1 as a
        // TINYINT is one. Measured, and it is why this reads the declared type rather than
        // shrinking to the significant bytes.
        var width = type switch
        {
            Int8Type => 1,
            Int16Type => 2,
            Int32Type => 4,
            _ => 8,
        };

        var builder = new BinaryArray.Builder();
        var buffer = new byte[width];

        for (var i = 0; i < rowCount; i++)
        {
            if (SparkArrays.ReadInt64(source, i) is not { } value)
            {
                builder.AppendNull();
                continue;
            }

            for (var b = 0; b < width; b++)
                buffer[width - 1 - b] = unchecked((byte)(value >> (8 * b)));

            builder.Append(buffer.AsSpan());
        }

        return builder.Build();
    }

    private IArrowArray CastToBoolean(IArrowArray source, int rowCount, bool raising)
    {
        var builder = new BooleanArray.Builder();

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);

            if (value is null)
            {
                builder.AppendNull();
                continue;
            }

            if (value.Value.IsNumeric)
            {
                builder.Append(value.Value.AsDouble != 0d);
                continue;
            }

            var text = value.Value.Text.Trim();
            if (bool.TryParse(text, out var parsed))
            {
                builder.Append(parsed);
                continue;
            }

            if (!raising) { builder.AppendNull(); continue; }
            throw SparkEvaluationException.InvalidCast(value.Value.Text, "BOOLEAN");
        }

        return builder.Build();
    }

    // ── Conditionals ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Unifies a conditional's branches and picks the chosen cell from each row.
    /// </summary>
    /// <remarks>
    /// The whole conditional family goes through here - <c>coalesce</c>/<c>nvl</c>/<c>ifnull</c>,
    /// <c>if</c> and <c>CASE</c> - because they share one rule. <c>greatest</c>/<c>least</c> do
    /// NOT: measured, they REFUSE a string against a number in both dialects
    /// (<c>DATATYPE_MISMATCH.DATA_DIFF_TYPES</c>) where the family here coerces, so they keep
    /// <see cref="UnifiedType"/> and its refusal. #278.
    /// </remarks>
    private IArrowArray UnifyBranches(
        IReadOnlyList<IArrowArray> branches, bool[] nullLiterals, int[] choice, int rowCount)
    {
        var type = ConditionalType(branches, nullLiterals);
        return SparkFunctions.Unify(
            type, CoerceBranches(type, branches, choice, rowCount), choice, rowCount);
    }

    /// <summary>The type a conditional's branches unify to, string coercion included.</summary>
    /// <remarks>
    /// A bare <c>NULL</c> is <c>void</c> in Spark and constrains nothing, so
    /// <c>coalesce(a, NULL)</c> is an <c>int</c>, and such a branch is left out of the fold.
    /// <para>
    /// <b>Which branch that is comes from the TYPE</b>, through
    /// <see cref="IConditionalArguments.IsNullLiteral"/>, rather than from noticing that a branch
    /// came back all null. It has to: a branch no row selected is all null by construction once
    /// the family stopped evaluating every branch, so a content test would swallow every unreached
    /// branch and retype the result -- measured, a zero-row <c>coalesce(a, s)</c> would come back
    /// <c>int</c> where Spark says <c>bigint</c>. That test was also wrong for a string column
    /// that merely held nothing in this batch, which is #293; #279 is what made it unworkable.
    /// Both are gone now that a bare NULL is materialised as a <c>void</c> column, and the
    /// two entry points answer the same way rather than the eager one guessing.
    /// </para>
    /// <para>
    /// <b>A typed null is not one.</b> It carries its type and still constrains the result:
    /// measured, <c>coalesce(CAST(NULL AS INT), '2')</c> is a <c>bigint</c> in Spark, not the
    /// string that dropping the all-null int would give, and
    /// <c>coalesce(CAST(NULL AS INT), CAST(NULL AS STRING), '7')</c> is a bigint too. Asking the
    /// expression gets this right by construction, where the content test got it right only
    /// because the placeholder happened to be spelled as a string.
    /// </para>
    /// </remarks>
    private IArrowType ConditionalType(IReadOnlyList<IArrowArray> branches, bool[] nullLiterals)
    {
        IArrowType? type = null;

        for (var i = 0; i < branches.Count; i++)
        {
            if (nullLiterals[i] || branches[i].Data.DataType is NullType)
                continue;

            var candidate = branches[i].Data.DataType;
            type = type is null ? candidate : UnifyBranchTypes(type, candidate);
        }

        // Every branch was a bare NULL, so there is no type to unify to. Spark says `void` --
        // measured, `coalesce(NULL, NULL)` and `CASE WHEN true THEN NULL ELSE NULL END` are both
        // void -- where this used to say `string` because that is what the placeholder was. #293.
        return type ?? NullType.Default;
    }

    private IArrowType UnifyBranchTypes(IArrowType left, IArrowType right)
    {
        if (left is StringType && right is StringType)
            return StringType.Default;

        // A `void` branch that reached here rather than being skipped -- a nested conditional
        // whose every branch was a bare NULL, so the answer is void without being a LITERAL null.
        // It still constrains nothing. #293.
        if (left is NullType)
            return right;

        if (right is NullType)
            return left;

        if (left is StringType || right is StringType)
        {
            return ConditionalStringTarget(left is StringType ? right : left)
                ?? throw new NotSupportedException(
                    $"no common type for {left.Name} and {right.Name}");
        }

        return SparkNumericTypes.CommonType(left, right);
    }

    /// <summary>
    /// What a string branch unifies with another type to, or null when Spark refuses the pair.
    /// </summary>
    /// <remarks>
    /// <b>The two dialects choose OPPOSITE directions</b>, which is the whole difficulty. Under
    /// ANSI the STRING moves to the other type; under the legacy dialect the OTHER TYPE moves to
    /// string. Measured on 4.0.3: <c>coalesce(CAST(1 AS INT), '2')</c> is a <c>bigint</c> holding
    /// 1 under ANSI and a <c>string</c> holding <c>'1'</c> under legacy. Same shape as #180/#259,
    /// where a comparison picked different cast targets per dialect.
    /// <para>
    /// ANSI widens rather than casting to the operand's own type: every integral width goes to
    /// <c>bigint</c> and every fractional one - float, double and decimal alike - to
    /// <c>double</c>. Boolean, binary, date and timestamp take the string directly.
    /// </para>
    /// <para>
    /// The refusals are measured too, not a fallback. Legacy refuses a string against a boolean
    /// and against a binary while ANSI answers both, so the null here is a real answer.
    /// </para>
    /// <para>
    /// This is NOT <see cref="StringComparisonTarget"/>, though the ANSI halves agree. That one's
    /// legacy half moves the string to the number, the opposite of this one, and it excludes
    /// binary because in a comparison it is the binary that moves. Two rules that look alike and
    /// were measured apart.
    /// </para>
    /// </remarks>
    private IArrowType? ConditionalStringTarget(IArrowType other)
    {
        if (!_options.Ansi)
        {
            return SparkNumericTypes.IsNumeric(other)
                   || SparkArrays.IsDateType(other)
                   || other is TimestampType
                ? StringType.Default
                : null;
        }

        if (SparkNumericTypes.IsIntegral(other))
            return Int64Type.Default;

        if (SparkNumericTypes.IsFloatingPoint(other) || SparkNumericTypes.IsDecimal(other))
            return DoubleType.Default;

        // BINARY joins these as of #295, which built the cast and the Unify branch that were
        // missing when #278 measured the rule and had to decline it. Measured: ANSI resolves
        // `coalesce(X'00', '2')` to BINARY, moving the string into it as UTF-8, in either operand
        // order -- `coalesce('2', X'00')` is binary too, holding 32.
        if (other is BooleanType or BinaryType or TimestampType || SparkArrays.IsDateType(other))
            return other;

        return null;
    }

    /// <summary>
    /// Casts the string branches to the unified type, over the rows that actually select them.
    /// </summary>
    /// <remarks>
    /// <b>Masked to the winning rows, which is the point.</b> Spark evaluates only the branch a
    /// row chooses, so a string no row selects is never converted and never fails: measured,
    /// <c>coalesce(CAST(1 AS INT), 'abc')</c> answers 1 under ANSI, while
    /// <c>coalesce(CAST(NULL AS INT), 'abc')</c> raises <c>CAST_INVALID_INPUT</c> because there
    /// the string IS chosen. Casting the column whole would fail the first one too.
    /// <para>
    /// Only the ANSI direction reaches the cast. Under legacy a string branch makes the unified
    /// type <c>string</c>, and <see cref="SparkFunctions.Unify"/> renders each branch itself.
    /// </para>
    /// <para>
    /// The cast is the SAME one <c>CAST(...)</c> reaches, dialect and all, so the string rules
    /// paid for in #174, #243 and #258 apply here without being restated.
    /// </para>
    /// </remarks>
    private IArrowArray[] CoerceBranches(
        IArrowType type, IReadOnlyList<IArrowArray> branches, int[] choice, int rowCount)
    {
        var prepared = new IArrowArray[branches.Count];
        for (var i = 0; i < branches.Count; i++)
            prepared[i] = branches[i];

        if (type is StringType)
            return prepared;

        for (var i = 0; i < prepared.Length; i++)
        {
            if (prepared[i] is not StringArray strings)
                continue;

            var masked = new StringArray.Builder();
            for (var row = 0; row < rowCount; row++)
            {
                if (choice[row] == i && !strings.IsNull(row))
                    masked.Append(strings.GetString(row)!);
                else
                    masked.AppendNull();
            }

            prepared[i] = Cast(
                masked.Build(), type, rowCount, raising: _options.Ansi, legacy: !_options.Ansi);
        }

        return prepared;
    }

    /// <summary>The type a set of branches unifies to.</summary>
    private static IArrowType UnifiedType(IEnumerable<IArrowArray> branches)
    {
        IArrowType? type = null;
        foreach (var branch in branches)
        {
            type = type is null
                ? branch.Data.DataType
                : SparkNumericTypes.CommonType(type, branch.Data.DataType);
        }

        return type ?? NullType.Default;
    }

    /// <summary>
    /// <c>round(x)</c> and <c>round(x, s)</c> — half away from zero, at a scale that may be
    /// negative.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Half away from zero</b>, which is what <c>round</c> is; Spark spells the half-even one
    /// <c>bround</c> and it is a different function. Measured on the inputs that tell them apart:
    /// <c>round(2.5)</c> is 3, <c>round(-2.5)</c> is -3 and <c>round(1.45, 1)</c> is 1.5.
    /// </para>
    /// <para>
    /// <b>The result type depends on the source, and only the decimal one moves.</b> A double
    /// stays a double and an integral type stays itself — <c>round(12345, -2)</c> is an
    /// <c>int</c> holding 12300. A decimal(p,s) rounded to <c>d</c> becomes
    /// decimal(p - s + s' + 1, s'), where s' is min(s, d), capped at 38. Measured:
    /// decimal(10,2) goes to decimal(10,1) at d=1 and decimal(9,0) at d=0 — and, the shape that
    /// is not obvious, to decimal(11,2) at d=5, because a scale WIDER than the value already has
    /// leaves the scale alone and still widens the precision by one.
    /// </para>
    /// <para>
    /// A negative scale on a DECIMAL is the one shape here that was not harvested; it takes the
    /// integral rule of rounding to a multiple of a power of ten. Everything else on this path is
    /// an answer from the corpus's <c>round-greatest-least</c> group.
    /// </para>
    /// </remarks>
    private IArrowArray Round(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var source = args[0];
        var scale = args.Count > 1 ? RoundScale(args[1], rowCount) : 0;

        // A string argument is read as a DOUBLE, in both dialects -- measured, `round('1.5', 2)`
        // is a double 1.5 under each. Only the failure differs, and the shared cast already knows
        // it: ANSI raises on `round('abc', 2)` where legacy answers null. #278.
        if (source.Data.DataType is StringType)
        {
            source = Cast(
                source, DoubleType.Default, rowCount, raising: _options.Ansi, legacy: !_options.Ansi);
        }

        if (source.Data.DataType is Decimal128Type decimals)
            return RoundDecimal(source, decimals, scale, rowCount);

        if (source.Data.DataType is FloatType or DoubleType)
            return RoundFloating(source, scale, rowCount);

        if (SparkNumericTypes.IsIntegral(source.Data.DataType))
            return RoundIntegral(source, scale, rowCount);

        // `round(NULL, 2)` resolves to a double in Spark, and a bare NULL reaches here as the
        // `void` column the evaluator materialises one as. This used to test the CONTENT instead,
        // and was dead code: the placeholder was spelled as a string, so the string branch above
        // cast it to a double and nothing ever reached here. #293.
        if (source.Data.DataType is NullType)
            return NullDoubles(rowCount);

        throw new NotSupportedException($"round over {source.Data.DataType.Name} is not supported");
    }

    private static IArrowArray NullDoubles(int rowCount)
    {
        var builder = new DoubleArray.Builder();
        for (var i = 0; i < rowCount; i++) builder.AppendNull();
        return builder.Build();
    }

    /// <summary>
    /// The scale argument, which Spark requires to be a constant.
    /// </summary>
    /// <remarks>
    /// <b>Measured:</b> <c>round(g, a)</c> is <c>DATATYPE_MISMATCH.NON_FOLDABLE_INPUT</c> — Spark
    /// refuses a scale that is not foldable, so one that varies by row cannot come from an
    /// expression it accepted. This registry sees materialised columns rather than the tree, so
    /// it cannot check foldability; what it can see is a scale that actually differs between
    /// rows, and refusing that keeps the answer fail-closed instead of silently using the first
    /// row's scale for all of them. Raised in review of #255.
    /// </remarks>
    private static int RoundScale(IArrowArray argument, int rowCount)
    {
        if (SparkArrays.ReadInt64(argument, 0) is not { } first)
            throw new ArgumentException("the scale of round must not be null", nameof(argument));

        for (var i = 1; i < rowCount; i++)
        {
            if (SparkArrays.ReadInt64(argument, i) != first)
            {
                throw new NotSupportedException(
                    "the scale of round must be the same for every row; Spark requires a constant "
                    + "there and refuses a column with NON_FOLDABLE_INPUT");
            }
        }

        return checked((int)first);
    }

    private static IArrowArray RoundFloating(IArrowArray source, int scale, int rowCount)
    {
        var isFloat = source.Data.DataType is FloatType;
        var doubles = isFloat ? null : new DoubleArray.Builder();
        var floats = isFloat ? new FloatArray.Builder() : null;

        for (var i = 0; i < rowCount; i++)
        {
            if (SparkArrays.ReadDouble(source, i) is not { } value)
            {
                doubles?.AppendNull();
                floats?.AppendNull();
                continue;
            }

            var rounded = RoundHalfUp(value, scale);
            doubles?.Append(rounded);
            floats?.Append((float)rounded);
        }

        return (IArrowArray?)doubles?.Build() ?? floats!.Build();
    }

    /// <summary>
    /// Half away from zero at a decimal scale, leaving alone anything with no fraction to lose.
    /// </summary>
    /// <remarks>
    /// NaN and the infinities pass through — measured, <c>round(NaN, 2)</c> is NaN — and so does
    /// a value scaled past 2^53, where a double has no fractional part left to round. That is
    /// what keeps <c>round(g, 20)</c> equal to <c>g</c> rather than turning it into an infinity.
    /// </remarks>
    private static double RoundHalfUp(double value, int scale)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
            return value;

        var factor = Math.Pow(10, scale);

        // A scale below about -324 underflows the factor to zero, and dividing by it at the end
        // would turn a rounded 0 into 0/0 = NaN -- a value out of nowhere, for an input that has
        // a perfectly ordinary answer. Everything is nearer to zero than to the first multiple of
        // a power of ten that large, so zero is that answer. #285.
        if (factor == 0d)
            return 0d;

        var scaled = value * factor;

        if (double.IsInfinity(scaled) || Math.Abs(scaled) >= 9007199254740992d)
            return value;

        return Math.Round(scaled, MidpointRounding.AwayFromZero) / factor;
    }

    private IArrowArray RoundIntegral(IArrowArray source, int scale, int rowCount)
    {
        var type = source.Data.DataType;
        var values = new long?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            if (SparkArrays.ReadInt64(source, i) is not { } value)
                continue;

            // A non-negative scale has nothing to round: the value is already an integer.
            if (scale >= 0)
            {
                values[i] = value;
                continue;
            }

            var rounded = RoundToPowerOfTen(value, PlacesFor(scale, 20), out var exact);

            if (!exact || !SparkArrays.FitsIn(rounded, type))
            {
                // Measured: `round(a, -1)` over INT_MIN reports ARITHMETIC_OVERFLOW rather than
                // CAST_OVERFLOW — rounding is arithmetic here, not a conversion. And plain
                // ARITHMETIC_OVERFLOW at EVERY width, including the narrow ones: unlike `a + b`,
                // this does not report BINARY_ARITHMETIC_OVERFLOW for a TINYINT.
                if (_options.Ansi)
                {
                    throw SparkEvaluationException.Overflow(
                        narrowerThanInt: false,
                        $"rounding {value} to {scale} places overflows {SparkArrays.Describe(type)}");
                }

                // THE LEGACY DIALECT WRAPS, it does not null -- which is the half of #285 the
                // issue did not name and the shape #243 already found for an integral cast.
                // Measured: `round(CAST(127 AS TINYINT), -1)` is -126 with ansi off, the byte
                // wrap of 130, and `round(9223372036854775807, -1)` is -9223372036854775806.
                // `rounded` already carries the low 64 bits, so the wrap is the same narrowing an
                // overflowing cast takes.
                values[i] = SparkArrays.Truncate(rounded, type);
                continue;
            }

            values[i] = rounded;
        }

        return SparkArrays.BuildIntegral(values, type, rowCount);
    }

    /// <summary>
    /// How many places a scale rounds to, negated in 64 bits and clamped.
    /// </summary>
    /// <remarks>
    /// <b><paramref name="scale"/> is an <see cref="int"/>, so <c>-scale</c> overflows back to a
    /// NEGATIVE for <see cref="int.MinValue"/></b> — and everything downstream then reads as
    /// nonsense: the integral loop runs zero times and returns the value unrounded, and the
    /// decimal path builds a <c>Decimal128Type</c> with a negative precision. Negating in 64 bits
    /// and clamping fixes both and changes no answer, because past <paramref name="ceiling"/>
    /// every value already rounds to zero.
    /// <para>
    /// <b>There is no Spark behaviour to reproduce this far out</b>, only a crash to avoid.
    /// Measured: <c>round(1, -2147483648)</c> is a bare <c>ArithmeticException: Underflow</c> with
    /// no error class under the legacy dialect, and
    /// <c>round(CAST(12.34 AS DECIMAL(10,2)), -2147483647)</c> is one under both — uncaught JVM
    /// exceptions rather than anything Spark defines. The scales that ARE defined still agree:
    /// <c>round(1, -100)</c> is 0 and <c>round(CAST(12.34 AS DECIMAL(10,2)), -39)</c> is a
    /// decimal(38,0) holding 0.
    /// </para>
    /// </remarks>
    private static int PlacesFor(int scale, int ceiling) =>
        scale >= 0 ? 0 : (int)Math.Min(-(long)scale, ceiling);

    /// <summary>
    /// Rounds to a multiple of 10^<paramref name="places"/>, half away from zero, in 64 bits.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Returns the low 64 bits whether or not the exact answer fits them, and reports which
    /// through <paramref name="exact"/>. Both halves are needed: ANSI raises on an overflow and
    /// the legacy dialect WRAPS to the target's width, so a function that could only refuse would
    /// leave the second dialect with nothing to return. #285.
    /// </para>
    /// <para>
    /// <b>Rounding at the top of the range is where the overflow lives</b>, and it used to be
    /// invisible for a BIGINT: <c>9223372036854775807</c> rounds to <c>...810</c>, the addition
    /// wrapped silently, and the <c>FitsIn</c> check that follows sees only a <c>long</c> — which
    /// always fits a BIGINT. The narrower widths were caught because their arithmetic still had
    /// room in a <see cref="long"/>; only the widest one did not.
    /// </para>
    /// <para>
    /// <b>The three <paramref name="places"/> bands are measured, not defensive.</b> At 19 the
    /// step is 10^19, which no <see cref="long"/> holds, so the only answers are 0 and ±10^19 and
    /// the second always overflows — but the TEST is exact, because half of 10^19 is 5e18 and
    /// that does fit. Measured: <c>round(4999999999999999999, -19)</c> is 0 and
    /// <c>round(5000000000000000000, -19)</c> is ARITHMETIC_OVERFLOW. At 20 and beyond, half the
    /// step is past a long's ceiling altogether, so every value rounds to zero —
    /// <c>round(9223372036854775807, -20)</c> is 0. The old code answered 0 for everything past
    /// 18, which got the 19 band wrong in both dialects.
    /// </para>
    /// </remarks>
    private static long RoundToPowerOfTen(long value, int places, out bool exact)
    {
        exact = true;

        // Half of 10^20 is 5e19, past long.MaxValue, so nothing reaches the first multiple.
        if (places >= 20)
            return 0;

        if (places == 19)
        {
            // Written as a bound rather than Math.Abs, which throws on long.MinValue — the very
            // value this band has to get right.
            if (value > -5_000_000_000_000_000_000L && value < 5_000_000_000_000_000_000L)
                return 0;

            exact = false;

            // 10^19 modulo 2^64, which is what the wrap produces, and its negation for the other
            // sign. Both fit a long once wrapped, so neither needs any more care than `unchecked`.
            const long WrappedTenPow19 = unchecked((long)10_000_000_000_000_000_000UL);
            return value < 0 ? unchecked(-WrappedTenPow19) : WrappedTenPow19;
        }

        var step = 1L;
        for (var i = 0; i < places; i++) step *= 10;

        // Neither of these can overflow: |remainder| < step, and truncated is between value and
        // zero. The step AWAY from zero below is the only part that can.
        var remainder = value % step;
        var truncated = value - remainder;

        if (Math.Abs(remainder) * 2 < step)
            return truncated;

        var away = value < 0 ? -step : step;
        var result = unchecked(truncated + away);

        // Adding a positive step can only decrease the result by wrapping, and vice versa.
        exact = value < 0 ? result < truncated : result > truncated;
        return result;
    }

    /// <summary>
    /// <c>round</c> over a decimal, where a NEGATIVE scale rounds to a multiple of a power of ten.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A negative scale used to do nothing at all.</b> The result scale clamps to 0 either way,
    /// so the old code rescaled to scale 0 and stopped — measured, <c>round(12.34, -1)</c> answered
    /// 12 where Spark answers 10, and every negative scale answered the same as scale 0. #285.
    /// </para>
    /// <para>
    /// <b>The integral digits the type reserves are <c>max(p - s, places)</c>, not <c>p - s</c>.</b>
    /// Measured, and the row that says so is the extreme one: <c>round(CAST(99 AS DECIMAL(2,0)),
    /// -20)</c> resolves to <c>decimal(21,0)</c> — Spark sizes the type for a multiple of 10^20
    /// even though no <c>decimal(2,0)</c> can reach one, and the answer is 0. The ordinary rows
    /// agree with the old formula because <c>p - s</c> is the larger term there.
    /// </para>
    /// <para>
    /// <b>Both dialects RAISE on a decimal overflow here</b>, unlike the integral path beside it,
    /// which raises under ANSI and wraps under legacy. Measured:
    /// <c>round(99999999999999999999999999999999999999, -1)</c> is
    /// NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION with ansi both on and off — and the
    /// <c>WITHOUT_SUGGESTION</c> half of that name is Spark saying so itself, since the other
    /// variant is the one that tells you to turn ANSI off.
    /// </para>
    /// </remarks>
    private IArrowArray RoundDecimal(IArrowArray source, Decimal128Type type, int scale, int rowCount)
    {
        var places = PlacesFor(scale, SparkNumericTypes.MaxPrecision + 1);
        var resultScale = Math.Max(Math.Min(type.Scale, scale), 0);
        var integralDigits = Math.Max(type.Precision - type.Scale, places);
        var resultPrecision = Math.Min(
            SparkNumericTypes.MaxPrecision, integralDigits + resultScale + 1);
        var target = new Decimal128Type(resultPrecision, resultScale);

        var mantissas = new Int128?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            if (SparkWideDecimals.Read(source, i) is not { } value)
                continue;

            // The same rescale every other exact path uses, so the rounding mode is the one
            // SparkWideDecimals pins rather than a second opinion about it.
            var rounded = places == 0
                ? SparkWideDecimals.Cast(value, target)
                : SparkWideDecimals.RoundToPowerOfTen(value, places, target);

            if (rounded is null)
            {
                throw SparkEvaluationException.NumericValueOutOfRangeWithoutSuggestion(
                    SparkWideDecimals.Render(value), target);
            }

            mantissas[i] = rounded;
        }

        return SparkWideDecimals.Build(mantissas, target, rowCount);
    }

    /// <summary>
    /// <c>greatest</c> and <c>least</c> — the largest or smallest argument, SKIPPING nulls.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Nulls are skipped, not propagated</b>, which is the opposite of almost everything else
    /// here: measured, <c>greatest(a, NULL)</c> is <c>a</c> and keeps <c>a</c>'s type, and only
    /// <c>greatest(NULL, NULL)</c> is null. That makes this the same shape as <c>coalesce</c> —
    /// choose an argument per row, then unify — rather than the same shape as arithmetic.
    /// </para>
    /// <para>
    /// <b>Two arguments at least.</b> <c>greatest(a)</c> is <c>WRONG_NUM_ARGS</c> in Spark, not
    /// the identity, and so is <c>greatest()</c>. Measured, because accepting one would have been
    /// the obvious reading.
    /// </para>
    /// </remarks>
    private static IArrowArray Extreme(string name, IReadOnlyList<IArrowArray> args, int rowCount)
    {
        if (args.Count < 2)
            throw new ArgumentException($"{name} needs at least two arguments", nameof(args));

        // Converted to the common type FIRST, so the comparison below is between two values of
        // one type rather than across a promotion. `greatest(d1, a)` is a decimal(12,2) in Spark,
        // and comparing the decimal against the raw int would be a different question.
        //
        // A `void` argument is DROPPED rather than unified with. A bare NULL constrains nothing
        // in Spark — measured, `greatest(a, NULL)` is an INT holding a — and dropping it changes
        // no VALUE either, because a row that is null can never be the greatest or the least.
        //
        // THE TEST IS THE TYPE, NOT THE CONTENT, and that is #293. This used to drop any argument
        // that held no value in this batch, which got two things wrong at once and in opposite
        // directions. A real string column with no populated row was dropped, so `greatest(a, s)`
        // ANSWERED an int over such a batch where Spark refuses it outright
        // (DATATYPE_MISMATCH.DATA_DIFF_TYPES) — an answer that depended on what the batch
        // happened to hold. And over NO rows the test was vacuous, since nothing can be anything
        // else there, so it had to be suppressed entirely: a bare NULL then constrained the type
        // it should not, and `greatest(a, NULL)` inside a branch no row reaches REFUSED instead
        // of answering `a`. A void column is void at every row count, so both cases fall out.
        var typed = args.Where(a => a.Data.DataType is not NullType).ToList();

        // Every argument was void, so there is no type to find and no value to pick. Spark types
        // `greatest(NULL, NULL)` as void and answers null. Built at the row count rather than
        // handed back as args[0], which a literal makes one row long even over an empty batch.
        if (typed.Count == 0)
            return new NullArray(rowCount);

        var type = UnifiedType(typed);
        var here = new int[rowCount];
        var unified = new IArrowArray[typed.Count];
        for (var i = 0; i < typed.Count; i++)
            unified[i] = SparkFunctions.Unify(type, new[] { typed[i] }, here, rowCount);

        var wanted = name == "greatest" ? 1 : -1;
        var choice = new int[rowCount];

        for (var row = 0; row < rowCount; row++)
        {
            choice[row] = -1;

            for (var i = 0; i < unified.Length; i++)
            {
                if (SparkFunctions.IsNull(unified[i], row))
                    continue;

                if (choice[row] < 0
                    || Math.Sign(SparkFunctions.CompareAt(unified[i], unified[choice[row]], row)) == wanted)
                {
                    choice[row] = i;
                }
            }
        }

        return SparkFunctions.Unify(type, unified, choice, rowCount);
    }

    /// <summary>
    /// <c>coalesce</c>/<c>nvl</c>/<c>ifnull</c> -- the first argument that is not null, per row.
    /// </summary>
    /// <remarks>
    /// Written as a sweep per ARGUMENT rather than per row, which is what lets an argument be
    /// evaluated once over exactly the rows still looking for a value. Spark evaluates them in the
    /// same order and stops at the same place, one row at a time; measured,
    /// <c>coalesce(a, z, CAST(s AS DOUBLE))</c> answers where <c>z</c> covers every row <c>a</c>
    /// left null, and raises where it does not.
    /// </remarks>
    private IArrowArray Coalesce(IConditionalArguments args, int rowCount)
    {
        if (args.Count == 0)
            throw new ArgumentException("coalesce needs at least one argument", nameof(args));

        var remaining = new bool[rowCount];
        for (var row = 0; row < rowCount; row++) remaining[row] = true;

        var choice = new int[rowCount];
        for (var row = 0; row < rowCount; row++) choice[row] = -1;

        var branches = new IArrowArray[args.Count];
        var nullLiterals = new bool[args.Count];

        for (var i = 0; i < args.Count; i++)
        {
            branches[i] = args.Evaluate(i, remaining);
            nullLiterals[i] = args.IsNullLiteral(i);

            for (var row = 0; row < rowCount; row++)
            {
                if (!remaining[row] || SparkFunctions.IsNull(branches[i], row))
                    continue;

                choice[row] = i;
                remaining[row] = false;
            }
        }

        return UnifyBranches(branches, nullLiterals, choice, rowCount);
    }

    /// <summary>
    /// <c>nullif(a, b)</c> — null when the two are equal, otherwise the first.
    /// </summary>
    private static IArrowArray NullIf(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var choice = new int[rowCount];
        for (var row = 0; row < rowCount; row++)
        {
            if (SparkFunctions.IsNull(args[0], row))
            {
                choice[row] = -1;
                continue;
            }

            // Compared in the operands' own terms, not as rendered text. A decimal(10,2)
            // holding 1.00 and an int holding 1 render differently but are equal, and Spark
            // agrees: nullif(CAST(1.00 AS DECIMAL(10,2)), 1) is null.
            choice[row] = !SparkFunctions.IsNull(args[1], row)
                && SparkFunctions.AreEqual(args[0], args[1], row)
                ? -1
                : 0;
        }

        return SparkFunctions.Unify(args[0].Data.DataType, args, choice, rowCount);
    }

    private IArrowArray If(IConditionalArguments args, int rowCount)
    {
        var everyRow = new bool[rowCount];
        for (var row = 0; row < rowCount; row++) everyRow[row] = true;

        var condition = args.Evaluate(0, everyRow);

        var thenRows = new bool[rowCount];
        var elseRows = new bool[rowCount];
        var choice = new int[rowCount];

        for (var row = 0; row < rowCount; row++)
        {
            var taken = IsTrue(condition, row);
            thenRows[row] = taken;
            elseRows[row] = !taken;
            choice[row] = taken ? 0 : 1;
        }

        var branches = new[] { args.Evaluate(1, thenRows), args.Evaluate(2, elseRows) };
        var nullLiterals = new[] { args.IsNullLiteral(1), args.IsNullLiteral(2) };

        return UnifyBranches(branches, nullLiterals, choice, rowCount);
    }

    /// <summary>
    /// <c>nvl2(x, a, b)</c> — <c>a</c> where <c>x</c> is not null, <c>b</c> where it is.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>if(x IS NOT NULL, a, b)</c>, and not merely by analogy: Spark rewrites it to exactly
    /// that before resolution, and says so when the rewrite fails —
    /// <c>nvl2(a, a, bin)</c> is refused as <c>Cannot resolve "(IF((a IS NOT NULL), a, bin))"</c>.
    /// So it belongs in the short-circuiting family and unifies through <see cref="UnifyBranches"/>
    /// like every other member; #308.
    /// </para>
    /// <para>
    /// Two things separate it from <see cref="If"/>, both measured on 4.0.3. The first argument is
    /// read for NULLNESS rather than for truth, so it is not required to be boolean and is not
    /// required to be scalar either — <c>nvl2(bin, 1, 2)</c>, <c>nvl2(ts, 1, 2)</c> and
    /// <c>nvl2(nested, a, 0)</c> all answer. And it takes no part in the result type:
    /// <c>nvl2(s, a, a)</c> is an <c>int</c>, not a string.
    /// </para>
    /// <para>
    /// The laziness is the whole point of the issue, and it runs in both directions.
    /// <c>nvl2(s, 0, CAST(s AS INT))</c> answers <c>[0, null, 0]</c> over the corpus batch, where
    /// the two rows that would raise never reach the cast; <c>nvl2(a, a, 1/0)</c> raises, because
    /// the null row does reach it.
    /// </para>
    /// </remarks>
    private IArrowArray Nvl2(IConditionalArguments args, int rowCount)
    {
        var everyRow = new bool[rowCount];
        for (var row = 0; row < rowCount; row++) everyRow[row] = true;

        // Over every row, and always: the first argument is what decides, so there is nothing to
        // skip. Measured, `nvl2(CAST(t AS INT), 1, 2)` raises under ANSI even though neither
        // branch is in any doubt.
        var subject = args.Evaluate(0, everyRow);

        var presentRows = new bool[rowCount];
        var absentRows = new bool[rowCount];
        var choice = new int[rowCount];

        for (var row = 0; row < rowCount; row++)
        {
            var present = !SparkFunctions.IsNull(subject, row);
            presentRows[row] = present;
            absentRows[row] = !present;
            choice[row] = present ? 0 : 1;
        }

        var branches = new[] { args.Evaluate(1, presentRows), args.Evaluate(2, absentRows) };
        var nullLiterals = new[] { args.IsNullLiteral(1), args.IsNullLiteral(2) };

        return UnifyBranches(branches, nullLiterals, choice, rowCount);
    }

    /// <summary>
    /// <c>CASE</c>, as the parser emits it: condition and value in pairs, with an odd argument
    /// count meaning a trailing ELSE.
    /// </summary>
    /// <remarks>
    /// A CASE with no ELSE and no matching branch is null — measured,
    /// <c>CASE WHEN a &gt; 0 THEN 1 END</c> gives null where the condition fails.
    /// </remarks>
    private IArrowArray Case(IConditionalArguments args, int rowCount)
    {
        if (args.Count < 2)
            throw new ArgumentException("case needs at least one when/then pair", nameof(args));

        var hasElse = args.Count % 2 == 1;
        var pairs = args.Count / 2;
        var branchCount = pairs + (hasElse ? 1 : 0);

        // The rows no WHEN has claimed yet. A later condition is evaluated only over these, which
        // is Spark's own order: measured, the second WHEN of
        // `CASE WHEN a > 0 THEN a WHEN CAST(s AS INT) > 0 THEN 2 ELSE 3 END` never runs on a batch
        // whose rows all satisfy the first, and does run -- and raises -- on one where a row
        // falls through.
        var remaining = new bool[rowCount];
        for (var row = 0; row < rowCount; row++) remaining[row] = true;

        var choice = new int[rowCount];
        for (var row = 0; row < rowCount; row++) choice[row] = -1;

        var taken = new bool[branchCount][];
        for (var branch = 0; branch < branchCount; branch++) taken[branch] = new bool[rowCount];

        for (var pair = 0; pair < pairs; pair++)
        {
            var condition = args.Evaluate(pair * 2, remaining);
            for (var row = 0; row < rowCount; row++)
            {
                // Asked of every row, decided or not, so that a condition of the wrong type is
                // refused rather than skipped past on a batch that never needed it.
                var matched = IsTrue(condition, row);
                if (!remaining[row] || !matched)
                    continue;

                choice[row] = pair;
                taken[pair][row] = true;
                remaining[row] = false;
            }
        }

        if (hasElse)
        {
            for (var row = 0; row < rowCount; row++)
            {
                if (!remaining[row])
                    continue;

                choice[row] = branchCount - 1;
                taken[branchCount - 1][row] = true;
            }
        }

        var branches = new IArrowArray[branchCount];
        var nullLiterals = new bool[branchCount];
        for (var branch = 0; branch < branchCount; branch++)
        {
            // The value of pair `branch` is the odd argument beside it; the ELSE is the last one.
            var index = branch < pairs ? branch * 2 + 1 : args.Count - 1;
            branches[branch] = args.Evaluate(index, taken[branch]);
            nullLiterals[branch] = args.IsNullLiteral(index);
        }

        return UnifyBranches(branches, nullLiterals, choice, rowCount);
    }

    /// <summary>A condition is taken only when it is true — null is not.</summary>
    /// <remarks>
    /// A non-boolean condition fails rather than being read as false. Spark rejects one outright
    /// (<c>if(int_col, …)</c> is a DATATYPE_MISMATCH analysis error), and treating it as false
    /// here would silently take the ELSE branch on every row — a wrong answer that looks like a
    /// deliberate one.
    /// </remarks>
    private static bool IsTrue(IArrowArray condition, int row)
    {
        if (condition is not BooleanArray booleans)
        {
            throw new NotSupportedException(
                $"a condition must be boolean, not {SparkArrays.Describe(condition.Data.DataType)}");
        }

        return !booleans.IsNull(row) && booleans.GetValue(row)!.Value;
    }
}
