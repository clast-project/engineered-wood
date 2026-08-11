// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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

            // try_cast is a cast that never raises: the ANSI failures become null, which is
            // exactly what the non-ANSI dialect does for every cast.
            case "cast":
                Expect(name, args, 2);
                return Cast(args[0], TargetTypeOf(args[1]), rowCount, raising: _options.Ansi);

            case "try_cast":
                Expect(name, args, 2);
                return Cast(args[0], TargetTypeOf(args[1]), rowCount, raising: false);

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

    private IArrowArray DecimalArithmetic(
        string op, IArrowArray left, IArrowArray right, Decimal128Type resultType, int rowCount)
    {
        var builder = new Decimal128Array.Builder(resultType);

        for (var i = 0; i < rowCount; i++)
        {
            var a = SparkArrays.ReadDecimal(left, i);
            var b = SparkArrays.ReadDecimal(right, i);

            if (a is null || b is null)
            {
                builder.AppendNull();
                continue;
            }

            if ((op is "/" or "%") && b.Value == 0m)
            {
                if (!_options.Ansi)
                {
                    builder.AppendNull();
                    continue;
                }

                throw SparkEvaluationException.DivideByZero();
            }

            decimal computed;
            try
            {
                computed = op switch
                {
                    "+" => a.Value + b.Value,
                    "-" => a.Value - b.Value,
                    "*" => a.Value * b.Value,
                    "/" => a.Value / b.Value,
                    "%" => a.Value % b.Value,
                    _ => throw new NotSupportedException($"'{op}' over decimals"),
                };
            }
            catch (OverflowException)
            {
                if (!_options.Ansi)
                {
                    builder.AppendNull();
                    continue;
                }

                throw SparkEvaluationException.Overflow(
                    false, $"{a.Value} {op} {b.Value} overflows {SparkArrays.Describe(resultType)}");
            }

            builder.Append(SparkArrays.Rescale(computed, resultType.Scale));
        }

        return builder.Build();
    }

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

    private IArrowArray Cast(IArrowArray source, IArrowType target, int rowCount, bool raising)
    {
        if (target is Decimal128Type decimalTarget)
            return CastToDecimal(source, decimalTarget, rowCount, raising);

        if (target is StringType)
            return CastToString(source, rowCount);

        if (SparkNumericTypes.IsIntegral(target))
            return CastToIntegral(source, target, rowCount, raising);

        if (target is DoubleType or FloatType)
            return CastToFloatingPoint(source, target, rowCount, raising);

        if (target is BooleanType)
            return CastToBoolean(source, rowCount, raising);

        throw new NotSupportedException(
            $"cast to {SparkArrays.Describe(target)} is not implemented. " +
            "Temporal casts need the timezone policy settled first.");
    }

    private IArrowArray CastToIntegral(IArrowArray source, IArrowType target, int rowCount, bool raising)
    {
        var values = new long?[rowCount];

        for (var i = 0; i < rowCount; i++)
        {
            var value = SparkArrays.ReadForCast(source, i);
            if (value is null)
                continue;

            if (!value.Value.IsNumeric)
            {
                if (!raising) continue;
                throw SparkEvaluationException.InvalidCast(
                    value.Value.Text, SparkArrays.Describe(target));
            }

            // No exact form means the magnitude is past decimal's range, and so past every
            // integral type's range too.
            if (value.Value.Exact is not { } exact)
            {
                if (!raising) continue;
                throw SparkEvaluationException.CastOverflow(
                    value.Value.Text, SparkArrays.Describe(target));
            }

            // A string must already be an integer. Numbers truncate toward zero, but
            // CAST('12.5' AS INT) is refused rather than becoming 12 — measured.
            if (value.Value.FromString && exact != decimal.Truncate(exact))
            {
                if (!raising) continue;
                throw SparkEvaluationException.InvalidCast(
                    value.Value.Text, SparkArrays.Describe(target));
            }

            var truncated = decimal.Truncate(exact);

            if (truncated < long.MinValue || truncated > long.MaxValue
                || !SparkArrays.FitsIn((long)truncated, target))
            {
                if (!raising) continue;
                throw SparkEvaluationException.CastOverflow(
                    value.Value.Text, SparkArrays.Describe(target));
            }

            values[i] = (long)truncated;
        }

        return SparkArrays.BuildIntegral(values, target, rowCount);
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

            if (value.Value.Exact is not { } exact)
            {
                if (!raising) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.CastOverflow(
                    value.Value.Text, SparkArrays.Describe(target));
            }

            try
            {
                builder.Append(SparkArrays.Rescale(exact, target.Scale));
            }
            catch (OverflowException)
            {
                if (!raising) { builder.AppendNull(); continue; }
                throw SparkEvaluationException.CastOverflow(
                    value.Value.Text, SparkArrays.Describe(target));
            }
        }

        return builder.Build();
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

}
