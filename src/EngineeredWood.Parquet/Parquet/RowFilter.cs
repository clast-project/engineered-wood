// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet;

/// <summary>
/// <see cref="RowPredicate"/>s ANDed in order, with SQL's three-valued AND short-circuited per row: the
/// way <c>ArrowRowEvaluator</c> (and Spark) evaluates <c>a AND b AND c</c>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>A row is dropped as soon as one predicate answers FALSE, and the predicates after it never see it,
/// so they cannot raise on it.</item>
/// <item>A row answered NULL stays live: the predicates after it still evaluate it, and can still raise on
/// it, but it is never returned.</item>
/// <item>A row is returned only when every predicate answered TRUE.</item>
/// </list>
/// <para>So splitting a predicate's top-level conjuncts into separate <see cref="RowPredicate"/>s, in their
/// order, answers and raises exactly as evaluating the conjunction would. Reordering them can change which
/// errors are raised, unless the conjuncts moved cannot raise.</para>
/// </remarks>
public sealed class RowFilter
{
    /// <summary>Creates a filter of <paramref name="predicates"/>, applied in this order.</summary>
    /// <exception cref="ArgumentException">There are no predicates, or one is null.</exception>
    public RowFilter(params RowPredicate[] predicates)
        : this((IReadOnlyList<RowPredicate>)predicates)
    {
    }

    /// <summary>Creates a filter of <paramref name="predicates"/>, applied in this order.</summary>
    /// <exception cref="ArgumentException">There are no predicates, or one is null.</exception>
    public RowFilter(IReadOnlyList<RowPredicate> predicates)
    {
        if (predicates is null)
            throw new ArgumentNullException(nameof(predicates));
        if (predicates.Count == 0)
            throw new ArgumentException("A row filter has at least one predicate.", nameof(predicates));
        foreach (var p in predicates)
        {
            if (p is null)
                throw new ArgumentException("A predicate is null.", nameof(predicates));
        }

        Predicates = predicates.ToArray();
    }

    /// <summary>The predicates, in the order they are applied.</summary>
    public IReadOnlyList<RowPredicate> Predicates { get; }
}
