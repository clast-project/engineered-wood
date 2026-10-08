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
/// <remarks>
/// <para><see cref="Name"/> is the dotted form, which statistics and pruning key on. <see cref="NameParts"/> is the
/// path it stands for: <c>a.b</c> in SQL is field <c>b</c> of struct <c>a</c> (parts <c>a</c>, <c>b</c>), while
/// <c>`a.b`</c> is one top-level column whose name contains a dot (one part). The dotted text cannot tell the two
/// apart, so anything that resolves a reference against a schema reads the parts.</para>
/// <para>Built from a name alone, a reference is one part: the column with exactly that name, as before parts
/// existed. Use <see cref="FromParts"/> for a path.</para>
/// </remarks>
public sealed record UnboundReference(string Name) : Expression
{
    // Set only by FromParts, with the name it joins to: a `with { Name = ... }` copy keeps the field but not the
    // name, and then reads as the one-part reference its new name is.
    private readonly IReadOnlyList<string>? _parts;
    private readonly string? _partsName;

    /// <summary>The path this reference names: a top-level column, then a field of each struct below it.</summary>
    public IReadOnlyList<string> NameParts =>
        _parts is not null && string.Equals(_partsName, Name, StringComparison.Ordinal) ? _parts : [Name];

    /// <summary>A reference to the field at <paramref name="parts"/>; its <see cref="Name"/> joins them with dots.</summary>
    public static UnboundReference FromParts(IReadOnlyList<string> parts)
    {
        if (parts.Count == 0)
            throw new ArgumentException("A reference needs at least one name part.", nameof(parts));
        return parts.Count == 1
            ? new UnboundReference(parts[0])
            : new UnboundReference(string.Join(".", parts), parts.ToArray());
    }

    private UnboundReference(string name, IReadOnlyList<string> parts) : this(name)
    {
        _parts = parts;
        _partsName = name;
    }

    /// <summary>Equal when both name the same path: <c>a.b</c> and <c>`a.b`</c> share a <see cref="Name"/> and
    /// differ.</summary>
    public bool Equals(UnboundReference? other) =>
        other is not null
        && string.Equals(Name, other.Name, StringComparison.Ordinal)
        && NameParts.SequenceEqual(other.NameParts, StringComparer.Ordinal);

    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(Name);

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
