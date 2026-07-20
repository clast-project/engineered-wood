// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Convenience factory methods for building <see cref="Expression"/> and
/// <see cref="Predicate"/> trees. The <c>(string column, LiteralValue value)</c>
/// overloads cover the common case of comparing a column against a constant;
/// the <c>(Expression left, Expression right)</c> overloads support arbitrary
/// expression composition.
/// </summary>
public static class Expressions
{
    // ── References and literals ──

    public static UnboundReference Ref(string name) => new(name);
    public static LiteralExpression Lit(LiteralValue value) => new(value);

    // ── Comparisons against a column ──

    public static ComparisonPredicate Equal(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.Equal, new LiteralExpression(value));

    public static ComparisonPredicate NotEqual(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.NotEqual, new LiteralExpression(value));

    public static ComparisonPredicate LessThan(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.LessThan, new LiteralExpression(value));

    public static ComparisonPredicate LessThanOrEqual(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.LessThanOrEqual, new LiteralExpression(value));

    public static ComparisonPredicate GreaterThan(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.GreaterThan, new LiteralExpression(value));

    public static ComparisonPredicate GreaterThanOrEqual(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.GreaterThanOrEqual, new LiteralExpression(value));

    public static ComparisonPredicate NullSafeEqual(string column, LiteralValue value) =>
        new(new UnboundReference(column), ComparisonOperator.NullSafeEqual, new LiteralExpression(value));

    public static ComparisonPredicate StartsWith(string column, string prefix) =>
        new(new UnboundReference(column), ComparisonOperator.StartsWith, new LiteralExpression(prefix));

    public static ComparisonPredicate NotStartsWith(string column, string prefix) =>
        new(new UnboundReference(column), ComparisonOperator.NotStartsWith, new LiteralExpression(prefix));

    // ── Comparisons between expressions ──

    public static ComparisonPredicate Equal(Expression left, Expression right) =>
        new(left, ComparisonOperator.Equal, right);

    public static ComparisonPredicate NotEqual(Expression left, Expression right) =>
        new(left, ComparisonOperator.NotEqual, right);

    public static ComparisonPredicate LessThan(Expression left, Expression right) =>
        new(left, ComparisonOperator.LessThan, right);

    public static ComparisonPredicate LessThanOrEqual(Expression left, Expression right) =>
        new(left, ComparisonOperator.LessThanOrEqual, right);

    public static ComparisonPredicate GreaterThan(Expression left, Expression right) =>
        new(left, ComparisonOperator.GreaterThan, right);

    public static ComparisonPredicate GreaterThanOrEqual(Expression left, Expression right) =>
        new(left, ComparisonOperator.GreaterThanOrEqual, right);

    public static ComparisonPredicate NullSafeEqual(Expression left, Expression right) =>
        new(left, ComparisonOperator.NullSafeEqual, right);

    // ── Unary predicates ──

    public static UnaryPredicate IsNull(string column) =>
        new(new UnboundReference(column), UnaryOperator.IsNull);

    public static UnaryPredicate IsNotNull(string column) =>
        new(new UnboundReference(column), UnaryOperator.IsNotNull);

    public static UnaryPredicate IsNull(Expression operand) =>
        new(operand, UnaryOperator.IsNull);

    public static UnaryPredicate IsNotNull(Expression operand) =>
        new(operand, UnaryOperator.IsNotNull);

    public static UnaryPredicate IsNaN(Expression operand) =>
        new(operand, UnaryOperator.IsNaN);

    public static UnaryPredicate IsNotNaN(Expression operand) =>
        new(operand, UnaryOperator.IsNotNaN);

    // ── Set predicates ──

    public static SetPredicate In(string column, params LiteralValue[] values) =>
        new(new UnboundReference(column), values, SetOperator.In);

    public static SetPredicate NotIn(string column, params LiteralValue[] values) =>
        new(new UnboundReference(column), values, SetOperator.NotIn);

    public static SetPredicate In(Expression operand, IReadOnlyList<LiteralValue> values) =>
        new(operand, values, SetOperator.In);

    public static SetPredicate NotIn(Expression operand, IReadOnlyList<LiteralValue> values) =>
        new(operand, values, SetOperator.NotIn);

    // ── Boolean connectives ──

    /// <summary>
    /// Logical AND with constant folding (<c>true</c> children dropped, any
    /// <c>false</c> child collapses to <see cref="False"/>) and flattening of
    /// nested AND. Empty input returns <see cref="True"/>.
    /// </summary>
    public static Predicate And(params Predicate[] children)
    {
        List<Predicate>? flat = null;
        foreach (var c in children)
        {
            switch (c)
            {
                case TruePredicate:
                    continue;
                case FalsePredicate:
                    return False;
                case AndPredicate inner:
                    flat ??= new List<Predicate>(children.Length + inner.Children.Count);
                    flat.AddRange(inner.Children);
                    break;
                default:
                    flat ??= new List<Predicate>(children.Length);
                    flat.Add(c);
                    break;
            }
        }

        if (flat is null || flat.Count == 0) return True;
        if (flat.Count == 1) return flat[0];
        return new AndPredicate(flat);
    }

    /// <summary>
    /// Logical OR with constant folding (<c>false</c> children dropped, any
    /// <c>true</c> child collapses to <see cref="True"/>) and flattening of
    /// nested OR. Empty input returns <see cref="False"/>.
    /// </summary>
    public static Predicate Or(params Predicate[] children)
    {
        List<Predicate>? flat = null;
        foreach (var c in children)
        {
            switch (c)
            {
                case FalsePredicate:
                    continue;
                case TruePredicate:
                    return True;
                case OrPredicate inner:
                    flat ??= new List<Predicate>(children.Length + inner.Children.Count);
                    flat.AddRange(inner.Children);
                    break;
                default:
                    flat ??= new List<Predicate>(children.Length);
                    flat.Add(c);
                    break;
            }
        }

        if (flat is null || flat.Count == 0) return False;
        if (flat.Count == 1) return flat[0];
        return new OrPredicate(flat);
    }

    /// <summary>
    /// Logical NOT. Pushes through constants: <c>NOT true → false</c>,
    /// <c>NOT false → true</c>, <c>NOT NOT x → x</c>.
    /// </summary>
    public static Predicate Not(Predicate child) => child switch
    {
        TruePredicate => False,
        FalsePredicate => True,
        NotPredicate inner => inner.Child,
        _ => new NotPredicate(child),
    };

    public static Predicate True => TruePredicate.Instance;
    public static Predicate False => FalsePredicate.Instance;

    // ── Function calls ──

    public static FunctionCall Call(string name, params Expression[] arguments) =>
        new(name, arguments);

    public static FunctionCall Call(string name, IReadOnlyList<Expression> arguments) =>
        new(name, arguments);

}
