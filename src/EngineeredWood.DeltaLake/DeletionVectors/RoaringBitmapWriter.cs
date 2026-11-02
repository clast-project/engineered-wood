// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Serializes row positions into the RoaringBitmapArray format Delta Lake deletion vectors are stored in.
/// </summary>
internal static class RoaringBitmapWriter
{
    /// <summary>
    /// The serialized deletion vector deleting <paramref name="rowIndices"/> (any order; duplicates count once):
    /// 4-byte magic, int64 sub-bitmap count, then per sub-bitmap its high 32 bits and a portable 32-bit roaring
    /// bitmap. Containers take their smallest form (array, bitmap, or runs when <paramref name="allowRuns"/>).
    /// </summary>
    public static byte[] Serialize(IEnumerable<long> rowIndices, bool allowRuns = true) =>
        DeletionVectorBuilder.Build(null, DeletionVectorBuilder.SortedDistinct(rowIndices), null, allowRuns)
            .SerializedBytes();
}
