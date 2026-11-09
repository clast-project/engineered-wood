// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet;

/// <summary>
/// Rows of a file a read never returns, by file position (counted from the file's first row): a deletion
/// vector, or positional deletes. Excluded rows are removed before any <see cref="RowPredicate"/> sees them.
/// </summary>
public interface IExcludedRows
{
    /// <summary>
    /// True when every row of [<paramref name="firstRow"/>, <paramref name="firstRow"/> +
    /// <paramref name="rowCount"/>) is excluded, so the reader may skip the span without reading it. Answering
    /// false is always safe: <see cref="ClearExcluded"/> then removes the rows one by one.
    /// </summary>
    bool ExcludesAll(long firstRow, long rowCount);

    /// <summary>
    /// Clears, in <paramref name="selection"/>, the bit of every excluded row of [<paramref name="firstRow"/>,
    /// <paramref name="firstRow"/> + <paramref name="rowCount"/>), leaving the others as they are. Bit <c>i</c>
    /// (LSB-first, word <c>i / 64</c>, Arrow's bitmap layout) stands for row <c>firstRow + i</c>.
    /// </summary>
    void ClearExcluded(long firstRow, int rowCount, Span<ulong> selection);
}
