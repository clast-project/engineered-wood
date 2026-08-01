// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The on-disk paths a transaction's OWN writers created and has not committed — the provenance
/// <see cref="DeltaTransaction.AbortAsync"/> deletes by.
///
/// <para><b>Why provenance rather than inference.</b> The staged action list cannot answer this question, and
/// gets it wrong in the direction that destroys data. A deletion-vector DELETE stages an <c>add</c> naming an
/// EXISTING data file, re-added with a new vector: that parquet is live table data and only the fresh
/// <c>.bin</c> beside it is ours. An add staged by <see cref="DeltaTransaction.StageDataFiles"/> names a file
/// the HOST wrote, which is equally not ours to delete, and nothing in the action distinguishes it from an
/// append's. Diffing the staged adds against the base snapshot would classify both wrongly. So each writer
/// records what it just created, and nothing else is ever a candidate.</para>
///
/// <para>Paths are the DECODED, table-relative form <see cref="EngineeredWood.IO.ITableFileSystem"/> speaks —
/// not the URL-encoded form an <c>add.path</c> or <c>cdc.path</c> carries.</para>
/// </summary>
internal sealed class WrittenFileLedger
{
    private readonly List<string> _paths = [];

    /// <summary>The paths recorded so far, in write order.</summary>
    public IReadOnlyList<string> Paths => _paths;

    /// <summary>
    /// Records a file by its table-relative on-disk path. Call this BEFORE the write where the path is known
    /// in advance: a write that fails part-way leaves bytes at that path, and those bytes are exactly as
    /// orphaned as a complete file's.
    /// </summary>
    public void Record(string relativePath) => _paths.Add(relativePath);

    /// <summary>Records a file named the way an action names it (<c>add.path</c> / <c>cdc.path</c>), which is
    /// URL-encoded and has to be decoded before the filesystem will find it.</summary>
    public void RecordEncoded(string encodedPath) => _paths.Add(DeltaPath.Decode(encodedPath));

    /// <summary>
    /// Records the <c>.bin</c> backing a deletion vector this transaction just wrote. An INLINE vector lives
    /// in the log and has no file of its own, and an absolute (<c>p</c>) vector — which engineered-wood never
    /// writes — cannot be resolved against the table root; <see cref="DeletionVectorPath"/> yields no path for
    /// either, so neither is recorded.
    /// </summary>
    public void RecordDeletionVector(DeletionVector deletionVector)
    {
        if (DeletionVectorPath.GetRelativePath(deletionVector) is { } path)
            _paths.Add(path);
    }

    /// <summary>
    /// Forgets everything recorded. Called once a commit SUCCEEDS — those files are live table data now, so
    /// the ledger must no longer name any of them — and after an abort has deleted them.
    /// </summary>
    public void Clear() => _paths.Clear();
}
