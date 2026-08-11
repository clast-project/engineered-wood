// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Concurrency;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// A caller-supplied precondition, re-asked before every attempt: once against the base version with
/// <paramref name="concurrent"/> null, then again on each collision with the commits that landed since.
/// Throw to abort the commit.
///
/// <para>Distinct from a conflict on purpose. A conflict is transient — the commit loop answers it by
/// rebasing — so a precondition failure must NOT be reported as one, or the loop would retry a decision
/// that cannot change. Throw something other than <see cref="DeltaConflictException"/>.</para>
/// </summary>
/// <param name="baseSnapshot">The version the commit was planned against.</param>
/// <param name="concurrent">The commits in <c>(baseSnapshot.Version, latest]</c>, or null on the check
/// that runs before the first attempt. Reading these is how a precondition sees the table's current state
/// without paying to materialise a whole snapshot.</param>
public delegate void CommitPrecondition(
    Snapshot.Snapshot baseSnapshot,
    IReadOnlyList<(long Version, IReadOnlyList<DeltaAction> Actions)>? concurrent);

/// <summary>
/// One commit's worth of input to <see cref="LogCommitter.CommitAsync"/>: what to write, what the
/// transaction read (so a concurrent commit can be judged against it), and what may be re-derived if it
/// has to rebase.
/// </summary>
public sealed record LogCommitRequest
{
    /// <summary>
    /// The version the actions were planned against. The commit is first attempted at
    /// <c>BaseSnapshot.Version + 1</c>, and every conflict verdict is asked about the commits that landed
    /// after it.
    /// </summary>
    public required Snapshot.Snapshot BaseSnapshot { get; init; }

    /// <summary>
    /// The actions to commit, WITHOUT a <c>commitInfo</c> — the committer adds one (carrying the
    /// in-commit timestamp when the table enables it) on every attempt. An empty list commits nothing and
    /// returns <see cref="LogCommitResult.Committed"/> false.
    /// </summary>
    public required IReadOnlyList<DeltaAction> Actions { get; init; }

    /// <summary>
    /// What the transaction read. Defaults to <see cref="ReadSet.Blind"/> — reads nothing, so only a
    /// concurrent metadata change, protocol change, or delete/delete can conflict with it.
    /// </summary>
    public ReadSet Reads { get; init; } = ReadSet.Blind;

    /// <summary>
    /// Paths this commit removes, for the delete/delete check. Must name ONLY what this commit removes:
    /// adding a merely-READ path here reports a concurrent delete of it as delete/delete rather than the
    /// concurrentDeleteRead it is. Put read paths in <see cref="Reads"/>.
    /// </summary>
    public ISet<string> PlannedRemovePaths { get; init; } = EmptyPaths;

    /// <summary>The level the conflict check is run at. See <see cref="IsolationLevel"/>.</summary>
    public IsolationLevel Isolation { get; init; } = IsolationLevel.WriteSerializable;

    /// <summary>The <c>commitInfo.operation</c> string — "WRITE", "DELETE", "UPDATE", "OPTIMIZE", …</summary>
    public string Operation { get; init; } = "WRITE";

    /// <summary>
    /// Whether the staged actions may be committed at a LATER version than they were planned for.
    ///
    /// <para>False when they encode the version — row tracking's <c>baseRowId</c> /
    /// <c>defaultRowCommitVersion</c>, a deletion vector computed against a specific file state — and no
    /// <see cref="Rebase"/> handler re-derives them. Such a commit succeeds only uncontended: a collision
    /// that the conflict check would otherwise have forgiven still aborts, rather than write actions that
    /// are quietly wrong at the version they land on.</para>
    /// </summary>
    public bool RebaseSafe { get; init; } = true;

    /// <summary>
    /// Re-derives the actions against the version a concurrent writer took. Null — the common case —
    /// re-commits the staged actions verbatim, which is valid precisely because the conflict check passed:
    /// nothing this commit read or removed was touched.
    /// </summary>
    public ICommitRebaseHandler? Rebase { get; init; }

    /// <summary>
    /// A caller precondition re-asked before every attempt. Null to make no such demand.
    /// </summary>
    public CommitPrecondition? Precondition { get; init; }

    /// <summary>
    /// How many times the commit may be attempted. 1 disables retrying altogether: the collision from the
    /// first attempt propagates unexamined, without reading the concurrent commits or consulting the
    /// conflict checker — which is what a caller wants when its actions are coupled to an exact version
    /// and a conflict is the whole answer.
    /// </summary>
    public int MaxAttempts { get; init; } = 100;

    /// <summary>
    /// Whether a landed version that is a multiple of <see cref="LogCommitOptions.CheckpointInterval"/>
    /// writes a checkpoint. Set false for a commit that should never trigger one — a caller that
    /// checkpoints on its own schedule, or one writing into a log another process owns the compaction of.
    /// </summary>
    public bool WriteCheckpointOnInterval { get; init; } = true;

    /// <summary>
    /// Invoked THE INSTANT the commit is durable, before the snapshot refresh and before anything else can
    /// throw. That gap is the point: a caller holding a list of files to clean up on failure must forget
    /// them here, because from this instant an <c>add</c> the table publishes names them — and a
    /// cancellation between the write and the refresh would otherwise surface as a failed commit whose
    /// cleanup deletes live data.
    ///
    /// <para>Runs once per successful commit. Must not throw.</para>
    /// </summary>
    public Action? OnCommitDurable { get; init; }

    /// <summary>
    /// This transaction's own claim about whether it READ anything, written to
    /// <c>commitInfo.isBlindAppend</c>. A LATER writer consults it: under
    /// <see cref="IsolationLevel.WriteSerializable"/> a declared-blind commit's added files are excluded
    /// from the predicate check, because a transaction that looked at nothing cannot have depended on
    /// anything the other one changed.
    ///
    /// <para><b>⚠ Three states, and <c>null</c> is the default for a reason.</b> <c>null</c> writes NO
    /// field, which is the honest answer for a caller that does not track its reads. Claiming <c>true</c>
    /// wrongly is the UNSAFE direction — another engine then SKIPS a check it owes.</para>
    ///
    /// <para><b>⚠ But absent does not mean the same thing to every reader, so silence is not uniformly
    /// safe.</b> delta-spark reads <c>isBlindAppendOption.getOrElse(false)</c> and examines the commit, so
    /// against it silence costs only spurious conflicts. This library's own
    /// <see cref="Concurrency.ConflictChecker"/> instead falls back to inferring from the commit's shape,
    /// and an adds-only commit infers as BLIND — so against a later EW reader, silence on an adds-only
    /// commit is read as a blind-append claim we never made. <b>A caller that knows it read should say
    /// <c>false</c> rather than say nothing.</b> Silence is for a caller that genuinely does not know.</para>
    ///
    /// <para><b>⚠ It is NOT derived from <see cref="Reads"/>, deliberately.</b> <see cref="ReadSet.Blind"/>
    /// is the DEFAULT, so it means "this caller said nothing about its reads" — not "this caller declares
    /// it read nothing". Sourcing a spec field off a defaulted value would turn every silent caller into
    /// an assertive one, and the callers most likely to be silent are exactly the hosts with their own
    /// data plane, whose reads this library never sees. The claim has to be made, not inferred.</para>
    ///
    /// <para>Delta's own definition is <c>onlyAddFiles &amp;&amp; !dependsOnFiles</c> — note it is
    /// conjunctive, so a commit that adds files AND read the table is not blind. Delta computes
    /// <c>onlyAddFiles</c> separately and pointedly does not use it alone.</para>
    /// </summary>
    public bool? IsBlindAppend { get; init; }

    /// <summary>
    /// Prunes a concurrent add against <see cref="Reads"/>'s predicates. Null builds one from the base
    /// snapshot's schema on the first collision — correct for any commit planned against that schema, so
    /// supply one only to reuse a pruner across commits or to change how it reads statistics.
    /// </summary>
    public DeltaFilePruner? Pruner { get; init; }

    /// <summary>
    /// The snapshot the post-commit refresh starts from. The refresh is INCREMENTAL, so passing the newest
    /// snapshot the caller already holds replays fewer versions. Defaults to
    /// <see cref="BaseSnapshot"/>; any snapshot of the same table is equivalent in result.
    /// </summary>
    public Snapshot.Snapshot? RefreshFrom { get; init; }

    private static readonly HashSet<string> EmptyPaths = new(StringComparer.Ordinal);
}
