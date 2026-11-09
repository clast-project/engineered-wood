// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Parquet;

/// <summary>
/// The file positions (counted from the file's first row) of a batch's rows: contiguous from
/// <see cref="FirstRow"/>, or listed in <see cref="Explicit"/> when rows were dropped from among them.
/// </summary>
public readonly struct RowPositions
{
    internal RowPositions(long firstRow, Int64Array? explicitPositions)
    {
        FirstRow = firstRow;
        Explicit = explicitPositions;
    }

    /// <summary>The position of the batch's first row.</summary>
    public long FirstRow { get; }

    /// <summary>Every row's position, ascending; null when the rows are contiguous.</summary>
    public Int64Array? Explicit { get; }

    /// <summary>True when row <c>i</c> is at <see cref="FirstRow"/> + <c>i</c>.</summary>
    public bool IsContiguous => Explicit is null;

    /// <summary>The position of row <paramref name="row"/> of the batch.</summary>
    public long this[int row] => Explicit is { } positions ? positions.GetValue(row)!.Value : FirstRow + row;
}
