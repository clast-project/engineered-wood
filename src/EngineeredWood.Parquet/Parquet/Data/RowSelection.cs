// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// EXPERIMENT (perf/selection-pushdown-ab): the rows of one flat column chunk that a read keeps,
/// one byte per row (1 = keep), handed to <see cref="ColumnChunkReader.ReadColumn"/> so the rows it
/// drops are never materialised into the Arrow arrays.
/// </summary>
internal sealed class RowSelection
{
    private readonly byte[] _keep;

    public RowSelection(byte[] keep, int rowCount)
    {
        _keep = keep;
        RowCount = rowCount;
        int kept = 0;
        for (int i = 0; i < rowCount; i++)
            kept += keep[i];
        KeptCount = kept;
    }

    public int RowCount { get; }

    public int KeptCount { get; }

    internal byte[] KeepArray => _keep;

    public ReadOnlySpan<byte> Keep => _keep.AsSpan(0, RowCount);

    /// <summary>The selection that drops <paramref name="deletedRows"/> (sorted, chunk-relative).</summary>
    public static RowSelection FromDeleted(ReadOnlySpan<long> deletedRows, int rowCount)
    {
        var keep = new byte[rowCount];
        keep.AsSpan().Fill(1);
        foreach (long row in deletedRows)
            keep[row] = 0;
        return new RowSelection(keep, rowCount);
    }
}
