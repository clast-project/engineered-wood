// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet;

/// <summary>
/// A run of rows within one row group: <see cref="Start"/> inclusive, <see cref="End"/> exclusive,
/// both counted from the row group's first row.
/// </summary>
/// <param name="Start">The first row in the range.</param>
/// <param name="End">The row after the last row in the range.</param>
public readonly record struct RowRange(long Start, long End)
{
    /// <summary>The number of rows in the range.</summary>
    public long Length => End - Start;
}
