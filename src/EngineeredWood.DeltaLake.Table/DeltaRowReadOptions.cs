// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Everything a <see cref="DeltaTable.ReadRowsAsync"/> call can vary beyond the selection itself: which
/// per-row metadata columns to append, what to name them, and which version to resolve the selection's paths
/// against. The <see cref="DeltaReadOptions"/> shape, for the same reason — a read that has grown twice
/// grows again, and an options object absorbs the next addition without moving the
/// <see cref="System.Threading.CancellationToken"/> or renumbering anyone's positional arguments.
/// </summary>
public sealed record DeltaRowReadOptions
{
    /// <summary>
    /// Which per-row metadata columns to append, exactly as <see cref="DeltaReadOptions.Metadata"/> takes it
    /// — same flags, same column names, same combinability. Combinable is the point: a host wanting a
    /// mutation key AND a stable identity gets both in ONE pass.
    /// </summary>
    public DeltaRowMetadata Metadata { get; init; } = DeltaRowMetadata.None;

    /// <summary>Prefix for the <see cref="DeltaRowMetadata.Locator"/> and
    /// <see cref="DeltaRowMetadata.RowTracking"/> column names. See
    /// <see cref="DeltaReadOptions.MetadataPrefix"/>. <see cref="DeltaRowMetadata.RowAddress"/> is not
    /// prefixed.</summary>
    public string MetadataPrefix { get; init; } = DeltaMetadataColumns.DefaultPrefix;

    /// <summary>
    /// The snapshot to resolve the selection's paths against — ordinarily
    /// <see cref="DeltaTransaction.Snapshot"/>, the same version the selection was built from. Null defaults
    /// to <see cref="DeltaTable.CurrentSnapshot"/>, which is right for a one-shot read but wrong inside a
    /// transaction: a concurrent rewrite would make the selection's paths look stale when they are exactly
    /// the ones the transaction is still validating against.
    ///
    /// <para>Named for what it does rather than for what it is, matching
    /// <c>ComputeDeletionVectorActionsAsync</c>' parameter of the same purpose;
    /// <see cref="DeltaReadOptions.Snapshot"/> is the same capability on a read that has no paths to
    /// resolve.</para>
    /// </summary>
    public Snapshot.Snapshot? ResolveAgainst { get; init; }
}
