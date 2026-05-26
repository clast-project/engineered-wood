// Copyright (c) Curt Hagenlocher. All rights reserved.
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
/// <typeparam name="TStats">The format-specific statistics carrier.</typeparam>
public interface INanCountAccessor<TStats>
{
    /// <summary>
    /// Returns the count of NaN values for a floating-point column, or
    /// <see langword="null"/> if unknown. A <see langword="null"/> result means
    /// NaNs may be present (the evaluator stays conservative); a value of 0
    /// means the column provably contains no NaN.
    /// </summary>
    long? GetNanCount(TStats stats, string column);
}
