// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Collections.Generic;

namespace EngineeredWood.Expressions;

/// <summary>
/// Element-wise equality and hashing for the list members of the expression tree.
/// </summary>
/// <remarks>
/// A record's generated <c>Equals</c> compares members with <c>EqualityComparer&lt;T&gt;.Default</c>,
/// which for an <c>IReadOnlyList</c> is reference equality — so a node holding one compares by
/// identity while every other node compares by value. These helpers exist so the four nodes that
/// hold a list can say what they mean, in one place, rather than four subtly different loops.
/// </remarks>
internal static class SequenceEquality
{
    /// <summary>Whether two lists hold equal elements in the same order.</summary>
    /// <remarks>
    /// Order-sensitive on purpose. <c>AND(a, b)</c> and <c>AND(b, a)</c> are logically equivalent
    /// but not the same tree, and treating them as one value would be a claim about commutativity
    /// and evaluation order that an equality operator is not the place to make.
    /// </remarks>
    public static bool Equal<T>(IReadOnlyList<T>? left, IReadOnlyList<T>? right)
    {
        if (ReferenceEquals(left, right))
            return true;

        if (left is null || right is null || left.Count != right.Count)
            return false;

        var comparer = EqualityComparer<T>.Default;
        for (var i = 0; i < left.Count; i++)
        {
            if (!comparer.Equals(left[i], right[i]))
                return false;
        }

        return true;
    }

    /// <summary>A hash over the elements, consistent with <see cref="Equal{T}"/>.</summary>
    /// <remarks>
    /// Order-sensitive to match, and it has to move in step with the comparison: two values that
    /// compare equal must hash equal, or the type breaks as a dictionary key in a way that is
    /// harder to notice than the reference equality it replaced.
    /// </remarks>
    public static int HashOf<T>(IReadOnlyList<T>? items)
    {
        if (items is null)
            return 0;

        var hash = 17;
        var comparer = EqualityComparer<T>.Default;
        foreach (var item in items)
            hash = unchecked((hash * 31) + (item is null ? 0 : comparer.GetHashCode(item)));

        return hash;
    }
}
