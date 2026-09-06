// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Base type for boolean-valued expressions (predicates). A predicate is also
/// an expression so it can appear as a function argument or CASE branch.
/// </summary>
public abstract record Predicate : Expression;

// ── Constant predicates ──

/// <summary>The constant predicate <c>true</c>.</summary>
public sealed record TruePredicate : Predicate
{
    public static readonly TruePredicate Instance = new();
    public override string ToString() => "true";
}

/// <summary>The constant predicate <c>false</c>.</summary>
public sealed record FalsePredicate : Predicate
{
    public static readonly FalsePredicate Instance = new();
    public override string ToString() => "false";
}

// ── Boolean connectives ──

/// <summary>
/// Logical AND. By design carries an N-ary list rather than a binary tree so
/// the evaluator can short-circuit on the first <c>AlwaysFalse</c>.
/// </summary>
public sealed record AndPredicate(IReadOnlyList<Predicate> Children) : Predicate
{
    /// <remarks>
    /// Hand-written because the generated version compares <see cref="Children"/> by reference —
    /// see <see cref="SequenceEquality"/>.
    /// </remarks>
    public bool Equals(AndPredicate? other) =>
        other is not null && SequenceEquality.Equal(Children, other.Children);

    public override int GetHashCode() => SequenceEquality.HashOf(Children);

    public override string ToString() => $"({string.Join(" AND ", Children)})";
}

/// <summary>
/// Logical OR. By design carries an N-ary list rather than a binary tree so
/// the evaluator can short-circuit on the first <c>AlwaysTrue</c>.
/// </summary>
public sealed record OrPredicate(IReadOnlyList<Predicate> Children) : Predicate
{
    /// <remarks>
    /// Hand-written because the generated version compares <see cref="Children"/> by reference —
    /// see <see cref="SequenceEquality"/>.
    /// </remarks>
    public bool Equals(OrPredicate? other) =>
        other is not null && SequenceEquality.Equal(Children, other.Children);

    public override int GetHashCode() => SequenceEquality.HashOf(Children);

    public override string ToString() => $"({string.Join(" OR ", Children)})";
}

/// <summary>Logical NOT.</summary>
public sealed record NotPredicate(Predicate Child) : Predicate
{
    public override string ToString() => $"NOT {Child}";
}

// ── Comparison predicates ──

/// <summary>
/// A binary comparison: <c>left op right</c>.
/// </summary>
public sealed record ComparisonPredicate(
    Expression Left,
    ComparisonOperator Op,
    Expression Right) : Predicate
{
    public override string ToString() => $"{Left} {OperatorText(Op)} {Right}";

    private static string OperatorText(ComparisonOperator op) => op switch
    {
        ComparisonOperator.Equal => "=",
        ComparisonOperator.NotEqual => "<>",
        ComparisonOperator.LessThan => "<",
        ComparisonOperator.LessThanOrEqual => "<=",
        ComparisonOperator.GreaterThan => ">",
        ComparisonOperator.GreaterThanOrEqual => ">=",
        ComparisonOperator.NullSafeEqual => "<=>",
        ComparisonOperator.StartsWith => "STARTS WITH",
        ComparisonOperator.NotStartsWith => "NOT STARTS WITH",
        _ => op.ToString(),
    };
}

// ── Unary predicates ──

/// <summary>
/// A unary predicate: <c>op(operand)</c>. Used for IS NULL, IS NOT NULL,
/// IS NaN, IS NOT NaN.
/// </summary>
public sealed record UnaryPredicate(
    Expression Operand,
    UnaryOperator Op) : Predicate
{
    public override string ToString() => Op switch
    {
        UnaryOperator.IsNull => $"{Operand} IS NULL",
        UnaryOperator.IsNotNull => $"{Operand} IS NOT NULL",
        UnaryOperator.IsNaN => $"{Operand} IS NAN",
        UnaryOperator.IsNotNaN => $"{Operand} IS NOT NAN",
        _ => $"{Op}({Operand})",
    };
}

// ── Set predicates ──

/// <summary>
/// A set membership predicate: <c>operand IN (v1, v2, ...)</c> or
/// <c>operand NOT IN (v1, v2, ...)</c>.
/// </summary>
public sealed record SetPredicate(
    Expression Operand,
    IReadOnlyList<Expression> Values,
    SetOperator Op) : Predicate
{
    /// <summary>A set over literal values, which is what most <c>IN</c> lists are.</summary>
    /// <remarks>
    /// The list holds expressions so that <c>x IN (a, b)</c> can exist at all; this spelling
    /// exists so that <c>x IN (1, 2)</c> does not have to say so twice.
    /// </remarks>
    public SetPredicate(Expression operand, IReadOnlyList<LiteralValue> values, SetOperator op)
        : this(operand, AsExpressions(values), op)
    {
    }

    private static IReadOnlyList<Expression> AsExpressions(IReadOnlyList<LiteralValue> values)
    {
        if (values is null)
            throw new ArgumentNullException(nameof(values));

        var wrapped = new Expression[values.Count];
        for (var i = 0; i < values.Count; i++)
            wrapped[i] = new LiteralExpression(values[i]);

        return wrapped;
    }

    /// <summary>
    /// The list as literal values, when every member is one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The list holds EXPRESSIONS, because <c>IN</c> takes them: <c>x IN (a, b)</c> is as
    /// ordinary as <c>x IN (1, 2)</c>, and it cannot be rewritten as a disjunction of equalities
    /// without changing its meaning. Spark resolves one type over the operand and the whole
    /// list, where a disjunction resolves each pair on its own — measured, <c>a IN ('01')</c> is
    /// false under the legacy dialect and <c>a = '01'</c> is true. #261.
    /// </para>
    /// <para>
    /// Everything that reads the list as data rather than evaluating it — statistics pruning,
    /// Lance's index pruning — needs the literal case and can do nothing with the rest, so this
    /// is the shape they ask in. A list with an expression in it answers false and they fall
    /// back to "no information", which is the same answer they already gave for a set they could
    /// not use.
    /// </para>
    /// </remarks>
    public bool TryGetLiteralValues(out IReadOnlyList<LiteralValue> literals)
    {
        // Asked first, so that a list this cannot answer for costs nothing. Its callers use it
        // as a capability check -- pruning and Bloom probing ask before doing anything else --
        // and a list holding a column is exactly the case that would have paid for the array.
        for (var i = 0; i < Values.Count; i++)
        {
            if (Values[i] is not LiteralExpression)
            {
                literals = Array.Empty<LiteralValue>();
                return false;
            }
        }

        var found = new LiteralValue[Values.Count];
        for (var i = 0; i < Values.Count; i++)
            found[i] = ((LiteralExpression)Values[i]).Value;

        literals = found;
        return true;
    }

    /// <remarks>
    /// Hand-written because the generated version compares <see cref="Values"/> by reference —
    /// see <see cref="SequenceEquality"/>.
    /// </remarks>
    public bool Equals(SetPredicate? other) =>
        other is not null
        && Op == other.Op
        && Operand.Equals(other.Operand)
        && SequenceEquality.Equal(Values, other.Values);

    public override int GetHashCode() =>
        unchecked((Operand.GetHashCode() * 31 + (int)Op) * 31 + SequenceEquality.HashOf(Values));

    public override string ToString() =>
        $"{Operand} {(Op == SetOperator.In ? "IN" : "NOT IN")} ({string.Join(", ", Values)})";
}

// ── Operator enums ──

/// <summary>Binary comparison operators.</summary>
public enum ComparisonOperator
{
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    /// <summary>
    /// Spark's <c>&lt;=&gt;</c>: <c>NULL &lt;=&gt; NULL</c> is true, and
    /// <c>x &lt;=&gt; NULL</c> is false for any non-null x. Used by Delta
    /// generated column validation.
    /// </summary>
    NullSafeEqual,

    StartsWith,
    NotStartsWith,
}

/// <summary>Unary predicate operators.</summary>
public enum UnaryOperator
{
    IsNull,
    IsNotNull,
    IsNaN,
    IsNotNaN,
}

/// <summary>Set membership operators.</summary>
public enum SetOperator
{
    In,
    NotIn,
}
