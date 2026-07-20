// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// A fully-qualified table name consisting of a <see cref="Iceberg.Namespace"/> and a table name.
/// </summary>
public sealed class TableIdentifier : IEquatable<TableIdentifier>
{
    /// <summary>The namespace that contains this table.</summary>
    public Namespace Namespace { get; }

    /// <summary>The table name within its namespace.</summary>
    public string Name { get; }

    /// <summary>Initializes a new <see cref="TableIdentifier"/>.</summary>
    /// <param name="ns">The containing namespace.</param>
    /// <param name="name">The table name.</param>
    public TableIdentifier(Namespace ns, string name)
    {
        if (ns is null) throw new ArgumentNullException(nameof(ns));
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Value cannot be null or empty.", nameof(name));
        Namespace = ns;
        Name = name;
    }

    /// <summary>Creates a <see cref="TableIdentifier"/> from a table name and namespace levels.</summary>
    /// <param name="name">The table name.</param>
    /// <param name="namespaceLevels">The namespace hierarchy levels.</param>
    /// <returns>A new <see cref="TableIdentifier"/>.</returns>
    public static TableIdentifier Of(string name, params string[] namespaceLevels) =>
        new(Namespace.Of(namespaceLevels), name);

    public bool Equals(TableIdentifier? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Namespace.Equals(other.Namespace) && Name == other.Name;
    }

    public override bool Equals(object? obj) => Equals(obj as TableIdentifier);

    public override int GetHashCode()
    {
        unchecked
        {
            return (Namespace.GetHashCode() * 397) ^ (Name?.GetHashCode() ?? 0);
        }
    }

    public override string ToString() =>
        Namespace.IsEmpty ? Name : $"{Namespace}.{Name}";

    public static bool operator ==(TableIdentifier? left, TableIdentifier? right) =>
        left is null ? right is null : left.Equals(right);

    public static bool operator !=(TableIdentifier? left, TableIdentifier? right) => !(left == right);
}
