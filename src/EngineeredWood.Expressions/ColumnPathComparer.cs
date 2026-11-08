// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Compares column paths (<see cref="UnboundReference.NameParts"/>) part by part, ordinally, for the tables
/// that look a reference's statistics up by its path (see <see cref="IStatisticsAccessor{TStats}"/>).
/// </summary>
public sealed class ColumnPathComparer : IEqualityComparer<IReadOnlyList<string>>
{
    /// <summary>The ordinal comparer.</summary>
    public static ColumnPathComparer Ordinal { get; } = new();

    private ColumnPathComparer() { }

    /// <inheritdoc/>
    public bool Equals(IReadOnlyList<string>? x, IReadOnlyList<string>? y)
    {
        if (ReferenceEquals(x, y))
            return true;
        if (x is null || y is null || x.Count != y.Count)
            return false;
        for (int i = 0; i < x.Count; i++)
        {
            if (!string.Equals(x[i], y[i], StringComparison.Ordinal))
                return false;
        }
        return true;
    }

    /// <inheritdoc/>
    public int GetHashCode(IReadOnlyList<string> path)
    {
        int hash = path.Count;
        foreach (string part in path)
            hash = unchecked(hash * 31 + StringComparer.Ordinal.GetHashCode(part));
        return hash;
    }
}
