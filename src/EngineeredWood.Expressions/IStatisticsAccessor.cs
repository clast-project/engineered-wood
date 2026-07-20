// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Format-agnostic adapter that extracts column statistics from a
/// format-specific stats carrier (<typeparamref name="TStats"/>) for use by
/// <see cref="StatisticsEvaluator"/>.
/// </summary>
/// <typeparam name="TStats">
/// The format-specific statistics type — e.g. <c>RowGroup</c> for Parquet,
/// <c>DataFileStats</c> for Iceberg, <c>ColumnStats</c> for Delta Lake.
/// </typeparam>
/// <remarks>
/// All accessors return <c>null</c> when the requested statistic is not
/// available (column missing, statistic absent, or unable to decode). The
/// evaluator treats <c>null</c> as "unknown" and produces
/// <see cref="FilterResult.Unknown"/> for the affected predicate.
/// </remarks>
public interface IStatisticsAccessor<TStats>
{
    /// <summary>Returns the minimum value for a column, or null if unknown.</summary>
    LiteralValue? GetMinValue(TStats stats, string column);

    /// <summary>Returns the maximum value for a column, or null if unknown.</summary>
    LiteralValue? GetMaxValue(TStats stats, string column);

    /// <summary>Returns the count of null values, or null if unknown.</summary>
    long? GetNullCount(TStats stats, string column);

    /// <summary>
    /// Returns the total value count (including nulls), or null if unknown.
    /// Used to determine whether all values are null (<c>NullCount == ValueCount</c>).
    /// </summary>
    long? GetValueCount(TStats stats, string column);

    /// <summary>
    /// Returns true if the minimum value is exact (not truncated). When false,
    /// the stored min is a lower bound on the actual min; the evaluator may
    /// still derive <see cref="FilterResult.AlwaysFalse"/> but is conservative
    /// about <see cref="FilterResult.AlwaysTrue"/> on equality predicates.
    /// </summary>
    bool IsMinExact(TStats stats, string column);

    /// <summary>
    /// Returns true if the maximum value is exact (not truncated). See
    /// <see cref="IsMinExact"/> for semantics.
    /// </summary>
    bool IsMaxExact(TStats stats, string column);
}
