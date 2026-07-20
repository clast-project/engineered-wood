// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Represents a byte range within a file: an offset from the start and a length.
/// </summary>
public readonly record struct FileRange(long Offset, long Length)
{
    /// <summary>
    /// The exclusive end position (Offset + Length).
    /// </summary>
    public long End => Offset + Length;

    /// <summary>
    /// Returns true if this range overlaps or is directly adjacent to <paramref name="other"/>.
    /// </summary>
    public bool OverlapsOrAdjacent(FileRange other) =>
        Offset <= other.End && other.Offset <= End;

    /// <summary>
    /// Returns true if the gap between this range and <paramref name="other"/>
    /// is at most <paramref name="maxGapBytes"/>.
    /// </summary>
    public bool IsWithinGap(FileRange other, long maxGapBytes)
    {
        if (OverlapsOrAdjacent(other))
            return true;

        long gap = Offset < other.Offset
            ? other.Offset - End
            : Offset - other.End;

        return gap <= maxGapBytes;
    }

    /// <summary>
    /// Returns the smallest range that covers both this range and <paramref name="other"/>.
    /// </summary>
    public FileRange Merge(FileRange other)
    {
        long start = Math.Min(Offset, other.Offset);
        long end = Math.Max(End, other.End);
        return new FileRange(start, end - start);
    }
}
