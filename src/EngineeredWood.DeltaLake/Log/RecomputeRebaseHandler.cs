// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// The rebase for a commit whose actions are a FUNCTION OF THE SNAPSHOT rather than a fixed list:
/// re-runs the caller's builder against the version the commit is about to land on, and commits what it
/// returns.
///
/// <para>The case this exists for is an overwrite. "Remove every active file and add these" cannot be
/// re-committed verbatim after a concurrent append — the removes would name the active set as it was, and
/// the file the other writer just added would survive an overwrite that was supposed to replace
/// everything. Re-deriving the removes from the newest active set is the whole fix. Row-tracking's
/// high-water mark has the same shape: a per-snapshot quantity, correct only for the snapshot it was read
/// from.</para>
///
/// <para><b>The builder runs once per collision</b>, so it must be safe to run more than once. One that
/// merely reads a snapshot and shapes actions is; one that WRITES something — a deletion vector, a data
/// file — leaves the previous attempt's output behind on every retry, and is responsible for recording it
/// somewhere the caller can collect.</para>
/// </summary>
public sealed class RecomputeRebaseHandler : ICommitRebaseHandler
{
    private readonly Func<Snapshot.Snapshot, CancellationToken, ValueTask<IReadOnlyList<DeltaAction>>> _build;

    /// <param name="build">Builds the actions to commit against a given snapshot. Called with the NEWEST
    /// snapshot on each collision; the commit is then attempted at that snapshot's version + 1.</param>
    public RecomputeRebaseHandler(
        Func<Snapshot.Snapshot, CancellationToken, ValueTask<IReadOnlyList<DeltaAction>>> build)
    {
        _build = build ?? throw new ArgumentNullException(nameof(build));
    }

    /// <summary>Always — the builder is defined over a snapshot, not over a version number.</summary>
    public bool NeedsLatestSnapshot => true;

    /// <inheritdoc/>
    public async ValueTask<CommitRebase> RebaseAsync(
        CommitRebaseContext context, CancellationToken cancellationToken)
        => new(await _build(context.LatestSnapshot!, cancellationToken).ConfigureAwait(false));
}
