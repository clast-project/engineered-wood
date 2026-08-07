// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// What a rebase gets to see: the version the commit was planned against, the commits that landed since,
/// and the actions as they stand.
/// </summary>
/// <param name="BaseSnapshot">The version the staged work was planned against. Unchanged across retries —
/// every rebase re-derives from the SAME base, so a fourth attempt is not a rebase of a rebase.</param>
/// <param name="LatestSnapshot">The newest snapshot, built only when
/// <see cref="ICommitRebaseHandler.NeedsLatestSnapshot"/> asked for it; null otherwise. Building it costs a
/// log replay, so a handler that needs nothing but the version should not ask.</param>
/// <param name="LatestVersion">The newest committed version. The commit will next be attempted at
/// <see cref="NextAttemptVersion"/>.</param>
/// <param name="Concurrent">Every commit in <c>(BaseSnapshot.Version, LatestVersion]</c>, ascending — the
/// same list the <see cref="Concurrency.ConflictChecker"/> is about to judge against.</param>
/// <param name="StagedActions">The caller's original actions, exactly as handed to
/// <see cref="LogCommitter.CommitAsync"/>. The STABLE source a rebase should re-derive from.</param>
/// <param name="AttemptedActions">What the attempt that just collided tried to write — the previous
/// rebase's output, or <paramref name="StagedActions"/> on the first collision. Handed over so a handler
/// can see what it is superseding (which deletion vectors it is about to orphan, say), not as a base to
/// re-derive from.</param>
/// <param name="Isolation">The isolation level this commit is validated at. Bounds what a rebase may
/// reconcile away — see <see cref="IsolationLevel"/>.</param>
/// <param name="Attempt">Zero-based attempt index: 0 on the first collision.</param>
public sealed record CommitRebaseContext(
    Snapshot.Snapshot BaseSnapshot,
    Snapshot.Snapshot? LatestSnapshot,
    long LatestVersion,
    IReadOnlyList<(long Version, IReadOnlyList<DeltaAction> Actions)> Concurrent,
    IReadOnlyList<DeltaAction> StagedActions,
    IReadOnlyList<DeltaAction> AttemptedActions,
    IsolationLevel Isolation,
    int Attempt)
{
    /// <summary>The version the rebased actions will be committed at, if the conflict check passes.</summary>
    public long NextAttemptVersion => LatestVersion + 1;
}

/// <summary>What a rebase produced.</summary>
/// <param name="Actions">The actions to validate and write at
/// <see cref="CommitRebaseContext.NextAttemptVersion"/>.</param>
/// <param name="RowLevelResolvedPaths">Files whose concurrent remove/re-add this rebase reconciled at ROW
/// granularity, so the conflict checker should not judge them at file granularity. Null when no such
/// reconciliation ran — which is the normal case; only deletion-vector union / row-id remap produces
/// these. Passed straight through to
/// <see cref="Concurrency.ConflictChecker.Check"/>'s <c>rowLevelResolvedPaths</c>.</param>
public readonly record struct CommitRebase(
    IReadOnlyList<DeltaAction> Actions,
    ISet<string>? RowLevelResolvedPaths = null);

/// <summary>
/// Re-derives a commit's actions against the version a concurrent writer just took, between the collision
/// and the conflict check that decides whether the commit may still land.
///
/// <para>A commit needs one only when its actions ENCODE the version they were planned against. Plain
/// actions do not: an <c>add</c> naming a file, a <c>remove</c> naming a path, and a <c>metaData</c> mean
/// the same thing at version 8 as at version 7, so the committer re-commits them verbatim and no handler
/// is needed. Row tracking's <c>baseRowId</c> does encode it — the id range a concurrent commit consumed is
/// no longer free — as does a deletion vector computed against a file whose current vector has moved.</para>
///
/// <para><b>Run order.</b> The handler runs BEFORE the conflict check, so what it reconciles (see
/// <see cref="CommitRebase.RowLevelResolvedPaths"/>) is what the checker is told to ignore. It may
/// therefore do work for a commit that then aborts — which is why a handler that writes files should
/// record them somewhere the caller can collect: a losing attempt's output is as orphaned as a failed
/// commit's.</para>
///
/// <para>Throwing <see cref="DeltaConflictException"/> from a handler aborts the commit, and is the right
/// answer when the rebase cannot be expressed — the same row was concurrently deleted, the file was
/// rewritten away. The committer does not catch it: a rebase that failed will not succeed by retrying.</para>
/// </summary>
public interface ICommitRebaseHandler
{
    /// <summary>
    /// Whether <see cref="CommitRebaseContext.LatestSnapshot"/> must be built. False buys the cheap path —
    /// the committer asks the log for its latest VERSION instead of replaying it into a snapshot.
    /// </summary>
    bool NeedsLatestSnapshot { get; }

    /// <summary>Re-derives the actions against <paramref name="context"/>.</summary>
    ValueTask<CommitRebase> RebaseAsync(CommitRebaseContext context, CancellationToken cancellationToken);
}
