// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;

namespace EngineeredWood.Parquet;

/// <summary>
/// One read's intent, for <see cref="ParquetFileReader.ReadSelectedAsync"/>: which columns to return, how to
/// prune, which rows to return exactly. Given per call rather than on <see cref="ParquetReadOptions"/>, so
/// that one reader can serve reads with different intents.
/// </summary>
public sealed record ParquetScan
{
    /// <summary>The columns to return, as <see cref="ParquetFileReader.ReadWithPositionsAsync(Predicate?, IReadOnlyList{string}?, CancellationToken)"/>
    /// resolves them; null returns every column. Columns <see cref="RowFilter"/> reads but this does not name
    /// are read for the filter and left out of the result.</summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>
    /// Prunes row groups and pages, as <see cref="ParquetReadOptions.Filter"/> does: a superset, never applied
    /// to rows. Null prunes by <see cref="ParquetReadOptions.Filter"/> instead, as the other reads do. To have
    /// a predicate both prune and filter rows, pass it here and as a <see cref="RowPredicate"/>.
    /// </summary>
    public Predicate? Prune { get; init; }

    /// <summary>The rows to return: only those every predicate answers TRUE for. Null returns every row
    /// pruning and <see cref="ExcludedRows"/> leave.</summary>
    public RowFilter? RowFilter { get; init; }

    /// <summary>Rows never to return, by file position; removed before any predicate sees them.</summary>
    public IExcludedRows? ExcludedRows { get; init; }
}
