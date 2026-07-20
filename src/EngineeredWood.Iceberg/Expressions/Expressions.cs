// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using Shared = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Expressions;

/// <summary>
/// Iceberg-flavored factory methods for building expressions. Produces
/// <see cref="EngineeredWood.Expressions.Expression"/> trees that are
/// consumed by <see cref="TableScan"/> (which delegates to the shared
/// <see cref="StatisticsEvaluator"/>).
/// </summary>
/// <remarks>
/// This is a convenience layer over <see cref="Shared"/> that adds Iceberg-
/// specific function call helpers (<c>Apply</c>) and preserves the historical
/// Iceberg API surface. New code should generally prefer the shared factory
/// directly.
/// </remarks>
public static class Expressions
{
    // ── References and literals ──

    public static Expression Ref(string name) => Shared.Ref(name);
    public static Expression Ref(int fieldId, string name) => new BoundReference(fieldId, name);
    public static Expression Lit(LiteralValue value) => new LiteralExpression(value);
    public static Expression Lit(long value) => new LiteralExpression(LiteralValue.Of(value));
    public static Expression Lit(int value) => new LiteralExpression(LiteralValue.Of(value));
    public static Expression Lit(double value) => new LiteralExpression(LiteralValue.Of(value));
    public static Expression Lit(string value) => new LiteralExpression(LiteralValue.Of(value));

    // ── Function calls ──

    public static Expression Apply(string funcName, params Expression[] arguments) =>
        new FunctionCall(funcName, arguments);

    // ── Boolean connectives ──

    public static Predicate AlwaysTrue() => Shared.True;
    public static Predicate AlwaysFalse() => Shared.False;

    public static Predicate And(Predicate left, Predicate right) => Shared.And(left, right);
    public static Predicate Or(Predicate left, Predicate right) => Shared.Or(left, right);
    public static Predicate Not(Predicate child) => Shared.Not(child);

    // ── Comparison predicates (column name + literal convenience) ──

    public static Predicate Equal(string column, long value) => Shared.Equal(column, value);
    public static Predicate Equal(string column, int value) => Shared.Equal(column, value);
    public static Predicate Equal(string column, string value) => Shared.Equal(column, value);
    public static Predicate Equal(string column, double value) => Shared.Equal(column, value);
    public static Predicate Equal(string column, float value) => Shared.Equal(column, value);
    public static Predicate Equal(string column, bool value) => Shared.Equal(column, value);

    public static Predicate NotEqual(string column, long value) => Shared.NotEqual(column, value);
    public static Predicate NotEqual(string column, string value) => Shared.NotEqual(column, value);

    public static Predicate LessThan(string column, long value) => Shared.LessThan(column, value);
    public static Predicate LessThan(string column, int value) => Shared.LessThan(column, value);
    public static Predicate LessThan(string column, double value) => Shared.LessThan(column, value);
    public static Predicate LessThan(string column, string value) => Shared.LessThan(column, value);

    public static Predicate LessThanOrEqual(string column, long value) => Shared.LessThanOrEqual(column, value);
    public static Predicate LessThanOrEqual(string column, int value) => Shared.LessThanOrEqual(column, value);

    public static Predicate GreaterThan(string column, long value) => Shared.GreaterThan(column, value);
    public static Predicate GreaterThan(string column, int value) => Shared.GreaterThan(column, value);
    public static Predicate GreaterThan(string column, double value) => Shared.GreaterThan(column, value);
    public static Predicate GreaterThan(string column, string value) => Shared.GreaterThan(column, value);

    public static Predicate GreaterThanOrEqual(string column, long value) => Shared.GreaterThanOrEqual(column, value);
    public static Predicate GreaterThanOrEqual(string column, int value) => Shared.GreaterThanOrEqual(column, value);

    // ── Unary predicates ──

    public static Predicate IsNull(string column) => Shared.IsNull(column);
    public static Predicate IsNotNull(string column) => Shared.IsNotNull(column);

    /// <summary>Backwards-compatible alias for <see cref="IsNotNull(string)"/>.</summary>
    public static Predicate NotNull(string column) => Shared.IsNotNull(column);

    // ── Set predicates ──

    public static Predicate In(string column, params long[] values) =>
        Shared.In(column, values.Select(v => (LiteralValue)v).ToArray());

    public static Predicate In(string column, params string[] values) =>
        Shared.In(column, values.Select(v => (LiteralValue)v).ToArray());

    public static Predicate NotIn(string column, params long[] values) =>
        Shared.NotIn(column, values.Select(v => (LiteralValue)v).ToArray());

    // ── String predicates ──

    public static Predicate StartsWith(string column, string prefix) => Shared.StartsWith(column, prefix);
    public static Predicate NotStartsWith(string column, string prefix) => Shared.NotStartsWith(column, prefix);

    // ── Generic expression-based predicates ──

    public static Predicate Equal(Expression left, Expression right) => Shared.Equal(left, right);
    public static Predicate LessThan(Expression left, Expression right) => Shared.LessThan(left, right);
    public static Predicate GreaterThan(Expression left, Expression right) => Shared.GreaterThan(left, right);
}
