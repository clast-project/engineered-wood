// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Arrow;

/// <summary>Sizes of Arrow data in memory, for budgets that bound what a read may hold.</summary>
internal static class ArrowSizes
{
    /// <summary>
    /// The bytes of every buffer a batch's columns reference, children and dictionaries included. A buffer
    /// shared by two columns, or by a slice and its source, is counted once per reference, so this can
    /// overstate what the batch alone keeps alive; it never understates it.
    /// </summary>
    internal static long EstimatedBytes(RecordBatch batch)
    {
        long bytes = 0;
        for (int c = 0; c < batch.ColumnCount; c++)
            bytes += EstimatedBytes(batch.Column(c).Data);
        return bytes;
    }

    private static long EstimatedBytes(ArrayData data)
    {
        long bytes = 0;
        foreach (var buffer in data.Buffers)
            bytes += buffer.Length;
        if (data.Children is { } children)
        {
            foreach (var child in children)
                bytes += EstimatedBytes(child);
        }
        if (data.Dictionary is { } dictionary)
            bytes += EstimatedBytes(dictionary);
        return bytes;
    }
}
