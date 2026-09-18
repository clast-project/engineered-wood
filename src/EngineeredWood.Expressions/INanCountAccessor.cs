// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Optional companion to <see cref="IStatisticsAccessor{TStats}"/> for formats
/// that track the number of NaN values in floating-point columns (e.g. Parquet
/// per PARQUET-2249). <see cref="StatisticsEvaluator"/> uses it, when the
/// supplied accessor also implements this interface, to resolve
/// <see cref="UnaryOperator.IsNaN"/> / <see cref="UnaryOperator.IsNotNaN"/>
/// predicates.
/// </summary>
/// <remarks>
/// It also decides how far a float column can be pruned by comparisons. A format that excludes NaN
/// from min/max -- Parquet's spec requires it, and Vortex does the same -- writes a file holding
/// <c>[1.0, NaN]</c> as <c>min = max = 1.0</c>. NaN is above every value in SQL's order, so
/// <c>col &gt; 5.0</c> is true of that hidden row while the bounds say the file cannot match. This
/// interface lets the evaluator tell "no NaN here" from "no NaN recorded" and keep pruning the
/// former.
/// </remarks>
/// <typeparam name="TStats">The format-specific statistics carrier.</typeparam>
public interface INanCountAccessor<TStats>
{
    /// <summary>
    /// Returns the count of NaN values for a floating-point column, or
    /// <see langword="null"/> if unknown. A <see langword="null"/> result means
    /// NaNs may be present (the evaluator stays conservative); a value of 0
    /// means the column provably contains no NaN.
    /// </summary>
    /// <remarks>
    /// Answer <see langword="null"/> unless the count is genuinely recorded. Zero is a claim the
    /// evaluator prunes on; do not infer it from bounds that merely look finite.
    /// </remarks>
    long? GetNanCount(TStats stats, string column);
}
