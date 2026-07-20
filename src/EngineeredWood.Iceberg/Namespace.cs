// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json.Serialization;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// A multi-level namespace identifier used to organize Iceberg tables, represented as
/// an ordered list of string levels (e.g. <c>["db", "schema"]</c>).
/// </summary>
[JsonConverter(typeof(NamespaceConverter))]
public sealed class Namespace : IEquatable<Namespace>
{
    /// <summary>The empty (root) namespace.</summary>
    public static readonly Namespace Empty = new([]);

    /// <summary>The ordered hierarchy levels that make up this namespace.</summary>
    public IReadOnlyList<string> Levels { get; }

    /// <summary>Initializes a new <see cref="Namespace"/> from the given levels.</summary>
    /// <param name="levels">The namespace hierarchy levels.</param>
    public Namespace(IReadOnlyList<string> levels) => Levels = levels;

    /// <summary>Creates a <see cref="Namespace"/> from one or more level strings.</summary>
    /// <param name="levels">The namespace hierarchy levels.</param>
    /// <returns>A new <see cref="Namespace"/>.</returns>
    public static Namespace Of(params string[] levels) => new(levels);

    /// <summary>Gets whether this namespace has zero levels.</summary>
    public bool IsEmpty => Levels.Count == 0;

    /// <summary>The number of levels in this namespace.</summary>
    public int Length => Levels.Count;

    /// <summary>Returns the parent namespace by removing the last level, or <see cref="Empty"/> if this namespace has one or zero levels.</summary>
    public Namespace Parent()
    {
        if (Levels.Count <= 1)
            return Empty;

        return new Namespace(Levels.Take(Levels.Count - 1).ToList());
    }

    /// <summary>Returns whether this namespace is a strict prefix of <paramref name="other"/>.</summary>
    /// <param name="other">The namespace to test.</param>
    /// <returns><see langword="true"/> if every level in this namespace matches the corresponding level in <paramref name="other"/> and <paramref name="other"/> has more levels.</returns>
    public bool IsAncestorOf(Namespace other)
    {
        if (Levels.Count >= other.Levels.Count)
            return false;

        for (int i = 0; i < Levels.Count; i++)
        {
            if (Levels[i] != other.Levels[i])
                return false;
        }

        return true;
    }

    public bool Equals(Namespace? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Levels.Count != other.Levels.Count) return false;
        return Levels.SequenceEqual(other.Levels);
    }

    public override bool Equals(object? obj) => Equals(obj as Namespace);

    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 17;
            foreach (var level in Levels)
                hash = hash * 31 + (level?.GetHashCode() ?? 0);
            return hash;
        }
    }

    public override string ToString() => string.Join(".", Levels);

    public static bool operator ==(Namespace? left, Namespace? right) =>
        left is null ? right is null : left.Equals(right);

    public static bool operator !=(Namespace? left, Namespace? right) => !(left == right);
}
