// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

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

        // Indexed rather than foreach, matching Equal above: enumerating through the interface
        // boxes the struct enumerator a list or array would otherwise hand back directly, and
        // this runs once per hash of every node that holds a list.
        //
        // No per-item null check either. The comparer handles it — verified, GetHashCode(null)
        // returns 0 rather than throwing — and testing for it would box each element of a
        // value-type list such as SetPredicate's LiteralValues.
        var hash = 17;
        var comparer = EqualityComparer<T>.Default;
        for (var i = 0; i < items.Count; i++)
            hash = unchecked((hash * 31) + comparer.GetHashCode(items[i]!));

        return hash;
    }
}
