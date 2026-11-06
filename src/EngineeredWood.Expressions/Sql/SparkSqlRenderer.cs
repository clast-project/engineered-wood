// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Numerics;
using System.Text;

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// Renders an <see cref="Expression"/> as Spark SQL text: the inverse of <see cref="SparkSqlParser"/>.
/// </summary>
/// <remarks>
/// <para>
/// The text follows Spark's own <c>Expression.sql</c>, the form Spark records for <c>INSERT … REPLACE WHERE</c>:
/// every operator node in parentheses, so precedence never needs reading, and literals typed by their spelling
/// (<c>2L</c> is a bigint, <c>1.50BD</c> a decimal(3,2), <c>1.0D</c> a double). <c>(id = 2L)</c>,
/// <c>((v &gt;= 3) AND (v &lt; 10))</c>.
/// </para>
/// <para>
/// Parsing the text gives back the tree it was rendered from, with these exceptions, each the same condition
/// spelled the way the parser builds it:
/// </para>
/// <list type="bullet">
/// <item><description>A <see cref="ComparisonOperator.StartsWith"/> comparison renders as Spark's
///   <c>startswith(a, b)</c>, and <see cref="UnaryOperator.IsNaN"/> as <c>isnan(a)</c>. Spark SQL has no operator
///   for either, and the parser reads the call back as a call. <see cref="UnaryOperator.IsNotNaN"/> renders as
///   <c>CASE WHEN (a IS NULL) THEN NULL ELSE (NOT isnan(a)) END</c>, because Spark's <c>isnan(NULL)</c> is false,
///   and a plain <c>NOT isnan(a)</c> would turn a NULL into true.</description></item>
/// <item><description>A <c>DATE</c> value (<see cref="LiteralValue.Kind.DateOnly"/>) renders as
///   <c>DATE '2026-01-02'</c>, which the parser lowers to a cast of a timestamp. That cast renders as the same
///   <c>DATE</c> literal again, so the text is stable from the second rendering on.</description></item>
/// <item><description>A double or float NaN or infinity renders as <c>CAST('NaN' AS DOUBLE)</c>, as in Spark: no
///   numeric literal spells one.</description></item>
/// <item><description>A reference made from one dotted name renders back-quoted (<c>`a.b`</c>, a column whose
///   name has a dot in it) and one made from parts as a path (<c>a.b</c>), as the parser reads each. A field
///   access on a reference (<c>getfield(a, 'b')</c>) renders as the path, and reads back as the reference.</description></item>
/// </list>
/// <para>
/// Some trees have no Spark SQL: a <see cref="BoundReference"/> (its name alone cannot say which nested field
/// it is), a literal of a kind Spark has no type for (unsigned integers, <see cref="LiteralValue.Kind.Half"/>, a time of day, a
/// GUID), a timestamp finer than Spark's microsecond, and a function name no identifier spells.
/// <see cref="Render"/> refuses those and <see cref="TryRender"/> answers false, rather than write text that
/// reads as a different condition.
/// </para>
/// </remarks>
public static class SparkSqlRenderer
{
    /// <summary>Renders <paramref name="expression"/> as Spark SQL.</summary>
    /// <exception cref="NotSupportedException">The tree holds a node Spark SQL cannot spell; the message
    /// names it.</exception>
    public static string Render(Expression expression)
    {
        if (expression is null)
            throw new ArgumentNullException(nameof(expression));

        var builder = new StringBuilder();
        if (Append(builder, expression) is { } reason)
            throw new NotSupportedException($"'{expression}' has no Spark SQL form: {reason}.");
        return builder.ToString();
    }

    /// <summary>Renders <paramref name="expression"/> as Spark SQL, or answers false when the tree holds a node
    /// Spark SQL cannot spell.</summary>
    public static bool TryRender(Expression expression, [NotNullWhen(true)] out string? sql)
    {
        if (expression is null)
            throw new ArgumentNullException(nameof(expression));

        var builder = new StringBuilder();
        if (Append(builder, expression) is not null)
        {
            sql = null;
            return false;
        }
        sql = builder.ToString();
        return true;
    }

    // Each Append returns null when it rendered, or why it could not. A reason rather than an exception, so
    // TryRender costs no throw on the refusals it exists for.
    private static string? Append(StringBuilder sql, Expression expression) => expression switch
    {
        TruePredicate => Text(sql, "true"),
        FalsePredicate => Text(sql, "false"),
        AndPredicate and => Junction(sql, and.Children, " AND ", "true"),
        OrPredicate or => Junction(sql, or.Children, " OR ", "false"),
        NotPredicate not => Wrapped(sql, "(NOT ", not.Child, ")"),
        ComparisonPredicate comparison => Comparison(sql, comparison),
        UnaryPredicate unary => Unary(sql, unary),
        SetPredicate set => Set(sql, set),
        UnboundReference reference => Reference(sql, reference.NameParts),
        BoundReference bound => $"the bound reference {bound} names no path",
        LiteralExpression literal => Literal(sql, literal.Value),
        FunctionCall call => Call(sql, call),
        _ => $"{expression.GetType().Name} is not a node this renderer knows",
    };

    private static string? Text(StringBuilder sql, string text)
    {
        sql.Append(text);
        return null;
    }

    private static string? Wrapped(StringBuilder sql, string before, Expression inner, string after)
    {
        sql.Append(before);
        if (Append(sql, inner) is { } reason)
            return reason;
        sql.Append(after);
        return null;
    }

    private static string? Junction(
        StringBuilder sql, IReadOnlyList<Predicate> children, string separator, string empty)
    {
        // The factories never build these, but the records can be constructed directly. An empty junction is its
        // identity, and a single child is itself.
        if (children.Count == 0)
            return Text(sql, empty);
        if (children.Count == 1)
            return Append(sql, children[0]);

        sql.Append('(');
        for (int i = 0; i < children.Count; i++)
        {
            if (i > 0)
                sql.Append(separator);
            if (Append(sql, children[i]) is { } reason)
                return reason;
        }
        sql.Append(')');
        return null;
    }

    private static string? Comparison(StringBuilder sql, ComparisonPredicate comparison)
    {
        string? op = comparison.Op switch
        {
            ComparisonOperator.Equal => " = ",
            ComparisonOperator.NotEqual => " <> ",
            ComparisonOperator.LessThan => " < ",
            ComparisonOperator.LessThanOrEqual => " <= ",
            ComparisonOperator.GreaterThan => " > ",
            ComparisonOperator.GreaterThanOrEqual => " >= ",
            ComparisonOperator.NullSafeEqual => " <=> ",
            _ => null,
        };

        if (op is null)
        {
            bool negated = comparison.Op == ComparisonOperator.NotStartsWith;
            if (!negated && comparison.Op != ComparisonOperator.StartsWith)
                return $"the comparison operator {comparison.Op} has no SQL spelling";

            if (negated)
                sql.Append("(NOT ");
            sql.Append("startswith(");
            if (Append(sql, comparison.Left) is { } leftReason)
                return leftReason;
            sql.Append(", ");
            if (Append(sql, comparison.Right) is { } rightReason)
                return rightReason;
            sql.Append(negated ? "))" : ")");
            return null;
        }

        sql.Append('(');
        if (Append(sql, comparison.Left) is { } left)
            return left;
        sql.Append(op);
        if (Append(sql, comparison.Right) is { } right)
            return right;
        sql.Append(')');
        return null;
    }

    private static string? Unary(StringBuilder sql, UnaryPredicate unary) => unary.Op switch
    {
        UnaryOperator.IsNull => Wrapped(sql, "(", unary.Operand, " IS NULL)"),
        UnaryOperator.IsNotNull => Wrapped(sql, "(", unary.Operand, " IS NOT NULL)"),
        UnaryOperator.IsNaN => Wrapped(sql, "isnan(", unary.Operand, ")"),
        UnaryOperator.IsNotNaN => NotNaN(sql, unary.Operand),
        _ => $"the unary operator {unary.Op} has no SQL spelling",
    };

    /// <summary>
    /// <c>IS NOT NaN</c>, which is NULL for a NULL operand. Spark's <c>isnan(NULL)</c> is false rather than NULL,
    /// so a plain <c>NOT isnan(x)</c> would be true there, and <c>NOT (x IS NOT NaN)</c> would select different
    /// rows. The CASE keeps the NULL.
    /// </summary>
    private static string? NotNaN(StringBuilder sql, Expression operand)
    {
        sql.Append("CASE WHEN (");
        if (Append(sql, operand) is { } reason)
            return reason;
        sql.Append(" IS NULL) THEN NULL ELSE (NOT isnan(");
        Append(sql, operand);
        sql.Append(")) END");
        return null;
    }

    private static string? Set(StringBuilder sql, SetPredicate set)
    {
        if (set.Values.Count == 0)
            return "an IN list needs at least one value";

        sql.Append('(');
        if (Append(sql, set.Operand) is { } operand)
            return operand;
        sql.Append(set.Op == SetOperator.In ? " IN (" : " NOT IN (");
        for (int i = 0; i < set.Values.Count; i++)
        {
            if (i > 0)
                sql.Append(", ");
            if (Append(sql, set.Values[i]) is { } reason)
                return reason;
        }
        sql.Append("))");
        return null;
    }

    // ── References ─────────────────────────────────────────────────────────────────────────────────

    private static string? Reference(StringBuilder sql, IReadOnlyList<string> parts)
    {
        for (int i = 0; i < parts.Count; i++)
        {
            if (i > 0)
                sql.Append('.');
            if (string.IsNullOrEmpty(parts[i]))
                return "a reference has an empty name part";
            AppendIdentifier(sql, parts[i]);
        }
        return null;
    }

    /// <summary>Appends <paramref name="name"/> bare when the parser reads it back as that name, else back-quoted
    /// with each back-quote doubled.</summary>
    private static void AppendIdentifier(StringBuilder sql, string name)
    {
        if (IsPlainIdentifier(name) && !Keywords.Contains(name))
        {
            sql.Append(name);
            return;
        }
        sql.Append('`').Append(name.Replace("`", "``")).Append('`');
    }

    private static bool IsPlainIdentifier(string name)
    {
        if (name.Length == 0 || !(IsAsciiLetter(name[0]) || name[0] == '_'))
            return false;
        foreach (char c in name)
        {
            if (!(IsAsciiLetter(c) || c is >= '0' and <= '9' || c == '_'))
                return false;
        }
        return true;
    }

    private static bool IsAsciiLetter(char c) => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z';

    // Words the parser reads as something other than a column wherever a column can stand, or that would end the
    // operand before it: quoting them costs nothing, and a bare `true` or `null` would be a literal.
    private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "and", "as", "between", "case", "cast", "date", "else", "end", "false", "ilike", "in", "interval", "is",
        "like", "not", "null", "or", "over", "rlike", "select", "then", "timestamp", "true", "try_cast", "when",
        "x",
    };

    // ── Literals ───────────────────────────────────────────────────────────────────────────────────

    private static string? Literal(StringBuilder sql, LiteralValue value)
    {
        switch (value.Type)
        {
            case LiteralValue.Kind.Null:
                return Text(sql, "NULL");
            case LiteralValue.Kind.Boolean:
                return Text(sql, value.AsBoolean ? "true" : "false");
            case LiteralValue.Kind.Int32:
                return Text(sql, value.AsInt32.ToString(CultureInfo.InvariantCulture));
            case LiteralValue.Kind.Int64:
                return Text(sql, value.AsInt64.ToString(CultureInfo.InvariantCulture) + "L");
            case LiteralValue.Kind.Double:
                return FloatingPoint(sql, value.AsDouble, "D", "DOUBLE");
            case LiteralValue.Kind.Float:
                return FloatingPoint(sql, value.AsFloat, "F", "FLOAT");
            case LiteralValue.Kind.Decimal:
                // decimal's own text keeps the scale, as the literal's type needs: 1.50m is "1.50", a decimal(3,2).
                return Text(sql, value.AsDecimal.ToString(CultureInfo.InvariantCulture) + "BD");
            case LiteralValue.Kind.HighPrecisionDecimal:
                var (unscaled, scale) = value.AsHighPrecisionDecimal;
                // Checked first, so the arithmetic below cannot overflow (-int.MinValue): no scale past 38 either way
                // fits in 38 digits.
                if (scale > MaxDecimalPrecision || scale < -MaxDecimalPrecision)
                    return $"a decimal of scale {scale} is wider than Spark's {MaxDecimalPrecision} digits";
                // Spark's widest decimal is 38 digits, the scale counted among them (0 <= scale <= precision <= 38),
                // and a negative scale is written out as trailing zeros. Past that, Spark refuses the literal.
                int digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).Length;
                int precision = scale >= 0 ? Math.Max(digits, scale) : (unscaled.IsZero ? 1 : digits - scale);
                if (precision > MaxDecimalPrecision)
                    return $"a decimal of {precision} digits is wider than Spark's {MaxDecimalPrecision}";
                return Text(sql, DecimalText(unscaled, scale) + "BD");
            case LiteralValue.Kind.String:
                AppendString(sql, value.AsString);
                return null;
            case LiteralValue.Kind.Binary:
                sql.Append("X'");
                foreach (byte b in value.AsBinary)
                    sql.Append(b.ToString("X2", CultureInfo.InvariantCulture));
                sql.Append('\'');
                return null;
            case LiteralValue.Kind.DateTimeOffset:
                return Timestamp(sql, value.AsDateTimeOffset);
#if NET6_0_OR_GREATER
            case LiteralValue.Kind.DateOnly:
                return Text(sql, "DATE '" + value.AsDateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'");
#endif
            default:
                return $"Spark has no type for a {value.Type} literal";
        }
    }

    private const int MaxDecimalPrecision = 38;

    private static string? FloatingPoint(StringBuilder sql, double value, string suffix, string type)
    {
        if (double.IsNaN(value))
            return Text(sql, $"CAST('NaN' AS {type})");
        if (double.IsPositiveInfinity(value))
            return Text(sql, $"CAST('Infinity' AS {type})");
        if (double.IsNegativeInfinity(value))
            return Text(sql, $"CAST('-Infinity' AS {type})");

        // "R" is the shortest text that reads back as the same value. A float is formatted as a float, or its
        // widened double would print digits the float never had (0.1F is 0.100000001490116 as a double). Except
        // the largest: its shortest text, 3.4028235E38, is past the exact value Spark bounds a float literal by,
        // and Spark refuses it. Any smaller float's shortest text lies within half a step of it, below the bound.
        string text;
        if (value == 0)
        {
            // Spelled out: .NET Framework prints negative zero as "0", and a sign it drops cannot be checked for
            // below, since 0.0 equals -0.0.
            text = BitConverter.DoubleToInt64Bits(value) < 0 ? "-0.0" : "0.0";
        }
        else if (suffix == "F" && Math.Abs(value) != float.MaxValue)
        {
            float single = (float)value;
            text = single.ToString("R", CultureInfo.InvariantCulture);
            // .NET Framework's "R" is not always the round trip it promises (a subnormal can lose its last digit),
            // so the text is read back the way the parser will read it, and the always-exact width used if not.
            if (!SparkDoubleText.TryParseSingle(text, out float back) || back != single)
                text = single.ToString("G9", CultureInfo.InvariantCulture);
        }
        else
        {
            text = value.ToString("R", CultureInfo.InvariantCulture);
            if (!SparkDoubleText.TryParse(text, out double back) || back != value)
                text = value.ToString("G17", CultureInfo.InvariantCulture);
        }
        return Text(sql, text + suffix);
    }

    private static string? FloatingPoint(StringBuilder sql, float value, string suffix, string type) =>
        FloatingPoint(sql, (double)value, suffix, type);

    private static string DecimalText(BigInteger unscaled, int scale)
    {
        bool negative = unscaled.Sign < 0;
        string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
        if (scale > 0)
        {
            if (digits.Length <= scale)
                digits = new string('0', scale - digits.Length + 1) + digits;
            digits = digits.Substring(0, digits.Length - scale) + "." + digits.Substring(digits.Length - scale);
        }
        else if (scale < 0)
        {
            digits += new string('0', -scale);
        }
        return negative ? "-" + digits : digits;
    }

    /// <summary>A single-quoted string literal, escaped so Spark's unescaping gives back exactly
    /// <paramref name="value"/>.</summary>
    /// <remarks>Only the quote and the backslash need escaping. Every other character, a newline included, stands
    /// for itself inside a literal, and escaping every backslash also keeps <c>\%</c>, which Spark leaves alone,
    /// from being read as an escape.</remarks>
    private static void AppendString(StringBuilder sql, string value)
    {
        sql.Append('\'');
        foreach (char c in value)
        {
            if (c is '\'' or '\\')
                sql.Append('\\');
            sql.Append(c);
        }
        sql.Append('\'');
    }

    /// <summary>A timestamp in UTC, marked <c>Z</c>, so the text names one instant whatever the session's time
    /// zone. The parser keeps the instant and not the offset, so the offset is not rendered either.</summary>
    private static string? Timestamp(StringBuilder sql, DateTimeOffset value)
    {
        value = value.ToUniversalTime();
        // Spark's timestamps are microseconds. A tick finer would be dropped by Spark, or refused.
        long subSecondTicks = value.Ticks % TimeSpan.TicksPerSecond;
        if (subSecondTicks % 10 != 0)
            return "Spark's timestamps hold microseconds, and this one is finer";

        sql.Append("TIMESTAMP '").Append(value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        if (subSecondTicks != 0)
            sql.Append('.').Append((subSecondTicks / 10).ToString("D6", CultureInfo.InvariantCulture).TrimEnd('0'));
        sql.Append("Z'");
        return null;
    }

    // ── Calls ──────────────────────────────────────────────────────────────────────────────────────

    private static string? Call(StringBuilder sql, FunctionCall call)
    {
        var args = call.Arguments;
        switch (call.Name)
        {
            case "+" or "-" or "*" or "/" or "%" or "||" when args.Count == 2:
                sql.Append('(');
                if (Append(sql, args[0]) is { } left)
                    return left;
                sql.Append(' ').Append(call.Name).Append(' ');
                if (Append(sql, args[1]) is { } right)
                    return right;
                sql.Append(')');
                return null;

            case "cast" or "try_cast" when args.Count == 2:
                return Cast(sql, call.Name, args[0], args[1]);

            case "case" when args.Count >= 2:
                return Case(sql, args);

            case "getfield" when args.Count == 2
                && args[1] is LiteralExpression { Value.Type: LiteralValue.Kind.String } field:
                if (Append(sql, args[0]) is { } fieldOf)
                    return fieldOf;
                sql.Append('.');
                if (field.Value.AsString.Length == 0)
                    return "a field access names an empty field";
                AppendIdentifier(sql, field.Value.AsString);
                return null;

            case "getitem" when args.Count == 2:
                if (Append(sql, args[0]) is { } itemOf)
                    return itemOf;
                sql.Append('[');
                if (Append(sql, args[1]) is { } index)
                    return index;
                sql.Append(']');
                return null;
        }

        // Every other call is spelled as one. The names the parser treats as syntax rather than calls, met here
        // with the wrong shape, would read back as that syntax.
        if (!IsPlainIdentifier(call.Name) || SyntaxNames.Contains(call.Name))
            return $"no call is spelled '{call.Name}'";

        sql.Append(call.Name).Append('(');
        for (int i = 0; i < args.Count; i++)
        {
            if (i > 0)
                sql.Append(", ");
            if (Append(sql, args[i]) is { } reason)
                return reason;
        }
        sql.Append(')');
        return null;
    }

    private static readonly HashSet<string> SyntaxNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "case", "cast", "false", "getfield", "getitem", "interval", "not", "null", "select", "true", "try_cast",
    };

    private static string? Cast(StringBuilder sql, string name, Expression operand, Expression type)
    {
        if (type is not LiteralExpression { Value.Type: LiteralValue.Kind.String } typeLiteral
            || !IsTypeName(typeLiteral.Value.AsString))
        {
            return $"{name} takes a type name, not {type}";
        }

        string typeName = typeLiteral.Value.AsString;

        // The parser's own lowering of DATE '2026-01-02', which reads more plainly as what was written.
        if (name == "cast"
            && typeName.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            && operand is LiteralExpression { Value.Type: LiteralValue.Kind.DateTimeOffset } instant
            && instant.Value.AsDateTimeOffset is { Offset.Ticks: 0, TimeOfDay.Ticks: 0 } midnight)
        {
            return Text(sql, "DATE '" + midnight.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'");
        }

        sql.Append(name == "cast" ? "CAST(" : "TRY_CAST(");
        if (Append(sql, operand) is { } reason)
            return reason;
        sql.Append(" AS ").Append(typeName).Append(')');
        return null;
    }

    /// <summary>A type name as the parser reads one: an identifier, then optionally numbers in parentheses
    /// (<c>DECIMAL(10,2)</c>).</summary>
    private static bool IsTypeName(string text)
    {
        int open = text.IndexOf('(');
        if (open < 0)
            return IsPlainIdentifier(text);
        if (!IsPlainIdentifier(text.Substring(0, open)) || text[text.Length - 1] != ')')
            return false;
        foreach (char c in text.Substring(open + 1, text.Length - open - 2))
        {
            if (!(c is >= '0' and <= '9' || c == ',' || c == ' '))
                return false;
        }
        return true;
    }

    /// <summary><c>case(when1, then1, …, whenN, thenN[, else])</c>, as the parser builds it.</summary>
    private static string? Case(StringBuilder sql, IReadOnlyList<Expression> args)
    {
        sql.Append("CASE");
        int pairs = args.Count / 2;
        for (int i = 0; i < pairs; i++)
        {
            sql.Append(" WHEN ");
            if (Append(sql, args[2 * i]) is { } when)
                return when;
            sql.Append(" THEN ");
            if (Append(sql, args[2 * i + 1]) is { } then)
                return then;
        }
        if (args.Count % 2 == 1)
        {
            sql.Append(" ELSE ");
            if (Append(sql, args[args.Count - 1]) is { } otherwise)
                return otherwise;
        }
        sql.Append(" END");
        return null;
    }
}
