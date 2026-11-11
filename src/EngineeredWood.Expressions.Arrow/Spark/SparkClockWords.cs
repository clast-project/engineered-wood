// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow.Spark;

/// <summary>
/// Resolves the clock words of an expression -- <c>now</c>, <c>today</c>, <c>yesterday</c>, <c>tomorrow</c> in a cast
/// to DATE or TIMESTAMP -- against one instant, so that every batch, file and dictionary one operation evaluates sees
/// the same one (#801).
/// </summary>
/// <remarks>
/// <para>
/// Spark resolves them once per query: <c>SpecialDatetimeValues</c> is an optimizer rule that rewrites the cast at
/// planning. <see cref="SparkFunctionRegistry"/> folds the same casts when they are evaluated
/// (<see cref="IConstantFoldedFunctions"/>), and an evaluation is a batch, so without this one operation read the clock
/// once per batch: <c>DELETE WHERE ts &lt; CAST('now' AS TIMESTAMP)</c> compared rows in different files against
/// different instants. An operation resolves its expressions once, when it starts, with this.
/// </para>
/// <para>
/// Only the cast's operand is replaced: the word becomes the text of the value it names at the instant, which the
/// cast reads as that same value. The call keeps its name, its target type and everything the evaluator and the
/// coercion rules decide from them; a literal in its place would carry no declared type. Covered: <c>cast</c> and
/// <c>try_cast</c> to DATE or TIMESTAMP over a string literal, the targets <see cref="SparkFunctionRegistry"/> folds.
/// A word reached any other way -- <c>CAST(concat('n', 'ow') AS TIMESTAMP)</c>, a foldable expression rather than a
/// literal -- is left to the per-evaluation fold, as before. <c>epoch</c> needs no clock and is left as it is.
/// </para>
/// </remarks>
public static class SparkClockWords
{
    /// <summary><paramref name="predicate"/> with every clock word resolved at <paramref name="now"/>; the same
    /// instance when it holds none.</summary>
    public static Predicate Resolve(Predicate predicate, DateTimeOffset now)
    {
        if (predicate is null)
            throw new ArgumentNullException(nameof(predicate));
        return (Predicate)Resolve((Expression)predicate, now);
    }

    /// <summary><paramref name="expression"/> with every clock word resolved at <paramref name="now"/>; the same
    /// instance when it holds none. A node of a kind this does not know is left as it is.</summary>
    public static Expression Resolve(Expression expression, DateTimeOffset now)
    {
        if (expression is null)
            throw new ArgumentNullException(nameof(expression));

        switch (expression)
        {
            case AndPredicate and:
                return Children(and.Children, now) is { } andChildren ? new AndPredicate(andChildren) : and;
            case OrPredicate or:
                return Children(or.Children, now) is { } orChildren ? new OrPredicate(orChildren) : or;
            case NotPredicate not:
            {
                var child = (Predicate)Resolve(not.Child, now);
                return ReferenceEquals(child, not.Child) ? not : new NotPredicate(child);
            }

            case ComparisonPredicate cmp:
            {
                var left = Resolve(cmp.Left, now);
                var right = Resolve(cmp.Right, now);
                return ReferenceEquals(left, cmp.Left) && ReferenceEquals(right, cmp.Right)
                    ? cmp
                    : new ComparisonPredicate(left, cmp.Op, right);
            }

            case UnaryPredicate unary:
            {
                var operand = Resolve(unary.Operand, now);
                return ReferenceEquals(operand, unary.Operand) ? unary : new UnaryPredicate(operand, unary.Op);
            }

            case SetPredicate set:
            {
                var operand = Resolve(set.Operand, now);
                var values = Children(set.Values, now);
                return ReferenceEquals(operand, set.Operand) && values is null
                    ? set
                    : new SetPredicate(operand, values ?? set.Values, set.Op);
            }

            case FunctionCall call:
            {
                if (ResolveCast(call, now) is { } resolved)
                    return resolved;
                var arguments = Children(call.Arguments, now);
                return arguments is null ? call : new FunctionCall(call.Name, arguments);
            }

            default:
                return expression;
        }
    }

    /// <summary>The children resolved, or null when none changed.</summary>
    private static List<T>? Children<T>(IReadOnlyList<T> children, DateTimeOffset now) where T : Expression
    {
        List<T>? result = null;
        for (int i = 0; i < children.Count; i++)
        {
            var child = (T)Resolve(children[i], now);
            if (!ReferenceEquals(child, children[i]))
            {
                result ??= [.. children];
                result[i] = child;
            }
        }

        return result;
    }

    /// <summary>A cast of a clock word with its operand replaced by the value's text; null when it is not one.</summary>
    private static FunctionCall? ResolveCast(FunctionCall call, DateTimeOffset now)
    {
        if (call.Name is not ("cast" or "try_cast") || call.Arguments.Count != 2
            || call.Arguments[0] is not LiteralExpression { Value.Type: LiteralValue.Kind.String } operand
            || call.Arguments[1] is not LiteralExpression { Value.Type: LiteralValue.Kind.String } target)
        {
            return null;
        }

        string word = operand.Value.AsString;
        IArrowType type;
        try
        {
            type = SparkArrays.ParseTypeName(target.Value.AsString);
        }
        catch (Exception e) when (e is ArgumentException or NotSupportedException or FormatException)
        {
            return null;   // the cast refuses its type itself, when it is evaluated
        }

        string? text = type switch
        {
            Date32Type when SparkSpecialDatetimeValues.TryReadDate(word.AsSpan(), now, out var date)
                && !IsEpoch(word) => DateText(date),
            TimestampType when SparkSpecialDatetimeValues.TryReadTimestamp(word.AsSpan(), now, out var instant)
                && !IsEpoch(word) => TimestampText(instant),
            _ => null,
        };

        return text is null
            ? null
            : new FunctionCall(call.Name, [new LiteralExpression(LiteralValue.Of(text)), call.Arguments[1]]);
    }

    // `epoch` names the same value at every instant: left as the word, which the fold reads as it always has.
    private static bool IsEpoch(string word) =>
        SparkSpecialDatetimeValues.TryReadTimestamp(word.AsSpan(), DateTimeOffset.MinValue.AddYears(1), out var a)
        && SparkSpecialDatetimeValues.TryReadTimestamp(word.AsSpan(), DateTimeOffset.MaxValue.AddYears(-1), out var b)
        && a == b;

    // The day a DATE value names: TryReadDate gives midnight of it in the session zone.
    private static string DateText(DateTimeOffset date) =>
        date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static readonly long EpochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

    // The instant in UTC, to the microsecond a TIMESTAMP holds; the explicit zone makes the session zone irrelevant.
    // Truncated as the fold's TIMESTAMP is built, toward zero from the epoch, before formatting (#828 review):
    // formatting six digits of the calendar's fraction truncates toward the past instead, which before 1970 named the
    // microsecond below the fold's.
    private static string TimestampText(DateTimeOffset instant)
    {
        long micros = (instant.UtcTicks - EpochTicks) / 10;
        var truncated = new DateTime(EpochTicks + micros * 10, DateTimeKind.Utc);
        return truncated.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "Z";
    }
}
