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
    public override string ToString() => $"({string.Join(" AND ", Children)})";
}

/// <summary>
/// Logical OR. By design carries an N-ary list rather than a binary tree so
/// the evaluator can short-circuit on the first <c>AlwaysTrue</c>.
/// </summary>
public sealed record OrPredicate(IReadOnlyList<Predicate> Children) : Predicate
{
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
    IReadOnlyList<LiteralValue> Values,
    SetOperator Op) : Predicate
{
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
