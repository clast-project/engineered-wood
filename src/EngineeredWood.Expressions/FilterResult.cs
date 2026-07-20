// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions;

/// <summary>
/// Three-valued result of evaluating a <see cref="Predicate"/> against
/// aggregated statistics for a unit of data (file, row group, stripe).
/// </summary>
public enum FilterResult
{
    /// <summary>
    /// All rows in this unit satisfy the predicate. The unit can be returned
    /// without re-evaluating the predicate per row.
    /// </summary>
    AlwaysTrue,

    /// <summary>
    /// No rows in this unit can satisfy the predicate. The unit can be skipped
    /// entirely.
    /// </summary>
    AlwaysFalse,

    /// <summary>
    /// Some rows may satisfy the predicate; statistics alone cannot determine
    /// the answer. The unit must be read and the predicate re-evaluated per
    /// row (or with finer-grained statistics like a column index).
    /// </summary>
    Unknown,
}
