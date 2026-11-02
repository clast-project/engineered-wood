// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet;

/// <summary>
/// Whether a caller needs none of the rows [<paramref name="firstRow"/>, <paramref name="firstRow"/> +
/// <paramref name="rowCount"/>) of a file, so a reader may skip them without reading them: for example, the
/// rows a deletion vector deletes. Positions count from the file's first row.
/// </summary>
/// <remarks>Answering false is always safe: the rows are read as usual, and the caller filters them itself.
/// Answering true for a row the caller does need loses that row.</remarks>
/// <param name="firstRow">The file position of the span's first row.</param>
/// <param name="rowCount">How many rows the span holds; never zero.</param>
/// <returns>True to skip every row of the span.</returns>
public delegate bool RowSkipPredicate(long firstRow, long rowCount);
