// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Base type for all value-producing expressions. Predicates (boolean
/// expressions) derive from <see cref="Predicate"/>, which itself extends
/// <see cref="Expression"/> so predicates can appear wherever expressions can
/// (e.g. as function arguments or CASE branches).
/// </summary>
public abstract record Expression;

// ── Leaf expressions ──

/// <summary>
/// A column reference by name. Used before schema binding.
/// </summary>
public sealed record UnboundReference(string Name) : Expression
{
    public override string ToString() => Name;
}

/// <summary>
/// A column reference resolved against a schema. Carries both the original
/// name and a stable field identifier (e.g. an Iceberg field ID or a Parquet
/// column index).
/// </summary>
public sealed record BoundReference(int FieldId, string Name) : Expression
{
    public override string ToString() => $"{Name}#{FieldId}";
}

/// <summary>
/// A literal value expression.
/// </summary>
public sealed record LiteralExpression(LiteralValue Value) : Expression
{
    public override string ToString() => Value.ToString();
}

/// <summary>
/// A function call expression: <c>name(arg1, arg2, ...)</c>.
/// Used for type casts (<c>CAST(expr AS type)</c>), date/time extraction
/// (<c>YEAR(expr)</c>), partition transforms (<c>bucket(expr, 16)</c>), and
/// any other named operation. The evaluator looks up the implementation in a
/// function registry.
/// </summary>
public sealed record FunctionCall(string Name, IReadOnlyList<Expression> Arguments) : Expression
{
    /// <remarks>
    /// Hand-written because the generated version compares <see cref="Arguments"/> by reference —
    /// see <see cref="SequenceEquality"/>.
    /// </remarks>
    public bool Equals(FunctionCall? other) =>
        other is not null
        && Name == other.Name
        && SequenceEquality.Equal(Arguments, other.Arguments);

    public override int GetHashCode() =>
        unchecked((Name?.GetHashCode() ?? 0) * 31 + SequenceEquality.HashOf(Arguments));

    public override string ToString() => $"{Name}({string.Join(", ", Arguments)})";
}
