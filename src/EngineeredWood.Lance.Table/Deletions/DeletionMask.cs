// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table.Deletions;

/// <summary>
/// Set of deleted row offsets (relative to the start of one fragment).
/// Backed by a <see cref="HashSet{Int32}"/> for now; switch to a
/// <c>BitArray</c> if this becomes a hot path for dense deletes.
/// </summary>
internal sealed class DeletionMask
{
    private readonly HashSet<int> _deleted;

    public DeletionMask(HashSet<int> deletedRowOffsets)
    {
        _deleted = deletedRowOffsets;
    }

    /// <summary>An empty mask (no rows deleted).</summary>
    public static DeletionMask Empty { get; } = new(new HashSet<int>());

    public int DeletedCount => _deleted.Count;

    public bool IsDeleted(int rowOffset) => _deleted.Contains(rowOffset);
}
