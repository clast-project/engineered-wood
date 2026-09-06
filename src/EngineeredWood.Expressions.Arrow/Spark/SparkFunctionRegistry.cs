// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;

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
public sealed class SparkFunctionRegistry : IFunctionRegistry
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
        "coalesce" or "nvl" or "ifnull" or "nullif" or "if" or "case" => true,
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
                return Coalesce(args, rowCount);

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
                return If(args, rowCount);

            case "case":
                return Case(args, rowCount);

            default:
                throw new NotSupportedException(
                    $"'{name}' is not implemented by SparkFunctionRegistry.");
        }
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
        if (target is Decimal128Type decimalTarget)
            return CastToDecimal(source, decimalTarget, rowCount, raising);

        if (target is StringType)
            return CastToString(source, rowCount);

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

            // No exact form means the magnitude is past decimal's range, and so past every
            // integral type's range too.
            if (value.Value.Exact is not { } exact)
            {
                if (raising) throw Refuse(value.Value.Text);

                values[i] = Overflowed(i, value.Value.AsDouble);
                continue;
            }

            // A string carrying a fraction is where the dialects part: ANSI refuses it, and the
            // legacy dialect TRUNCATES toward zero and carries on to the range check below. Both
            // measured — CAST('12.5' AS INT) is an error under ANSI and 12 without it, while
            // CAST('300.5' AS TINYINT) is null because 300 does not fit rather than because of
            // the fraction. try_cast takes ANSI's reading of the value and nulls it.
            if (!legacy && value.Value.FromString && exact != decimal.Truncate(exact))
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

        return type ?? StringType.Default;
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
        var scale = args.Count > 1 ? RoundScale(args[1]) : 0;

        if (source.Data.DataType is Decimal128Type decimals)
            return RoundDecimal(source, decimals, scale, rowCount);

        if (source.Data.DataType is FloatType or DoubleType)
            return RoundFloating(source, scale, rowCount);

        if (SparkNumericTypes.IsIntegral(source.Data.DataType))
            return RoundIntegral(source, scale, rowCount);

        // `round(NULL, 2)` resolves to a double in Spark, and an all-null literal reaches here as
        // the placeholder column the evaluator builds when it has no type to infer.
        if (AllNull(source, rowCount))
            return NullDoubles(rowCount);

        throw new NotSupportedException($"round over {source.Data.DataType.Name} is not supported");
    }

    private static bool AllNull(IArrowArray source, int rowCount)
    {
        for (var i = 0; i < rowCount; i++)
        {
            if (!SparkFunctions.IsNull(source, i))
                return false;
        }

        return true;
    }

    private static IArrowArray NullDoubles(int rowCount)
    {
        var builder = new DoubleArray.Builder();
        for (var i = 0; i < rowCount; i++) builder.AppendNull();
        return builder.Build();
    }

    /// <summary>The scale argument, which is a constant integer.</summary>
    private static int RoundScale(IArrowArray argument) =>
        SparkArrays.ReadInt64(argument, 0) is { } value
            ? checked((int)value)
            : throw new ArgumentException("the scale of round must not be null", nameof(argument));

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

            var rounded = RoundToPowerOfTen(value, -scale);

            if (!SparkArrays.FitsIn(rounded, type))
            {
                if (!_options.Ansi) continue;

                // Measured: `round(a, -1)` over INT_MIN reports ARITHMETIC_OVERFLOW rather than
                // CAST_OVERFLOW — rounding is arithmetic here, not a conversion.
                throw SparkEvaluationException.Overflow(
                    SparkArrays.NarrowerThanInt(type),
                    $"rounding {value} to {scale} places overflows {SparkArrays.Describe(type)}");
            }

            values[i] = rounded;
        }

        return SparkArrays.BuildIntegral(values, type, rowCount);
    }

    /// <summary>Rounds to a multiple of 10^places, half away from zero.</summary>
    private static long RoundToPowerOfTen(long value, int places)
    {
        if (places > 18)
            return 0;

        var step = 1L;
        for (var i = 0; i < places; i++) step *= 10;

        var remainder = value % step;
        var truncated = value - remainder;

        if (Math.Abs(remainder) * 2 < step)
            return truncated;

        return truncated + (value < 0 ? -step : step);
    }

    private IArrowArray RoundDecimal(IArrowArray source, Decimal128Type type, int scale, int rowCount)
    {
        var resultScale = Math.Max(Math.Min(type.Scale, scale), 0);
        var resultPrecision = Math.Min(
            SparkNumericTypes.MaxPrecision, type.Precision - type.Scale + resultScale + 1);
        var target = new Decimal128Type(resultPrecision, resultScale);

        var mantissas = new Int128?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            if (SparkWideDecimals.Read(source, i) is not { } value)
                continue;

            // The same rescale every other exact path uses, so the rounding mode is the one
            // SparkWideDecimals pins rather than a second opinion about it.
            var rounded = SparkWideDecimals.Cast(value, target);

            if (rounded is null)
            {
                if (!_options.Ansi) continue;
                throw SparkEvaluationException.NumericValueOutOfRange(
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
        // A bare NULL is typed `void` in Spark and constrains nothing: measured,
        // `greatest(a, NULL)` is an INT holding a, not an error about a common type. The
        // evaluator materialises a null literal as an all-null placeholder column, so that is
        // how `void` arrives here, and it is left out of the unification rather than unified
        // with. If every argument is one, there is no type to find and the answer is null —
        // which is what Spark says for `greatest(NULL, NULL)`.
        // An argument that is null in every row is DROPPED rather than unified with. A bare NULL
        // is typed `void` in Spark and constrains nothing — measured, `greatest(a, NULL)` is an
        // INT holding a, where unifying int with the all-null placeholder column the evaluator
        // materialises a null literal as would be an error about int and string. Dropping it
        // changes no answer, because a row that is null can never be the greatest or the least.
        var typed = args.Where(a => !AllNull(a, rowCount)).ToList();

        // Every argument was one, so there is no type to find and no value to pick. Spark types
        // `greatest(NULL, NULL)` as void and answers null, which is what the placeholder already
        // is.
        if (typed.Count == 0)
            return args[0];

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

    private static IArrowArray Coalesce(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        if (args.Count == 0)
            throw new ArgumentException("coalesce needs at least one argument", nameof(args));

        var choice = new int[rowCount];
        for (var row = 0; row < rowCount; row++)
        {
            choice[row] = -1;
            for (var i = 0; i < args.Count; i++)
            {
                if (SparkFunctions.IsNull(args[i], row))
                    continue;

                choice[row] = i;
                break;
            }
        }

        return SparkFunctions.Unify(UnifiedType(args), args, choice, rowCount);
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

    private static IArrowArray If(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        var branches = new[] { args[1], args[2] };
        var choice = new int[rowCount];

        for (var row = 0; row < rowCount; row++)
            choice[row] = IsTrue(args[0], row) ? 0 : 1;

        return SparkFunctions.Unify(UnifiedType(branches), branches, choice, rowCount);
    }

    /// <summary>
    /// <c>CASE</c>, as the parser emits it: condition and value in pairs, with an odd argument
    /// count meaning a trailing ELSE.
    /// </summary>
    /// <remarks>
    /// A CASE with no ELSE and no matching branch is null — measured,
    /// <c>CASE WHEN a &gt; 0 THEN 1 END</c> gives null where the condition fails.
    /// </remarks>
    private static IArrowArray Case(IReadOnlyList<IArrowArray> args, int rowCount)
    {
        if (args.Count < 2)
            throw new ArgumentException("case needs at least one when/then pair", nameof(args));

        var hasElse = args.Count % 2 == 1;
        var values = new List<IArrowArray>();
        for (var i = 1; i < args.Count; i += 2)
            values.Add(args[i]);

        if (hasElse)
            values.Add(args[args.Count - 1]);

        var choice = new int[rowCount];
        for (var row = 0; row < rowCount; row++)
        {
            choice[row] = hasElse ? values.Count - 1 : -1;

            for (var branch = 0; branch * 2 + 1 < args.Count; branch++)
            {
                if (!IsTrue(args[branch * 2], row))
                    continue;

                choice[row] = branch;
                break;
            }
        }

        return SparkFunctions.Unify(UnifiedType(values), values, choice, rowCount);
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
