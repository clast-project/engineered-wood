// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.Snapshot;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// The Delta optimistic-concurrency commit loop: attempt, and on a collision decide whether the commit is
/// still valid at the version a concurrent writer just took.
///
/// <para><see cref="TransactionLog.WriteCommitAsync"/> gives the primitive underneath — one writer creates
/// version N and everyone else fails — but a writer that stops there can only ever report "someone beat
/// me". This is the layer that turns that into an answer: read the commits that landed since, ask the
/// <see cref="ConflictChecker"/> whether any of them invalidated what this transaction read or removed,
/// and either abort (first committer wins) or rebase onto the newer version and retry. A commit that
/// rebases is committing the SAME work at a later version, which is sound precisely because the check
/// found nothing it depended on had moved.</para>
///
/// <para><b>What it owns.</b> Protocol gating before the first write, the retry loop, the conflict verdict,
/// the post-commit snapshot refresh, and checkpoint-on-interval. What it deliberately does NOT own is the
/// meaning of the actions: it never inspects them beyond handing them to the log, so a caller with a data
/// plane of its own — its own parquet, its own statistics, its own deletion vectors — can use this without
/// bringing one along.</para>
///
/// <para><b>What a caller must still do.</b> Actions whose CONTENT depends on the version they land at
/// cannot simply be re-committed. Either mark the request <see cref="LogCommitRequest.RebaseSafe"/> false,
/// so a collision aborts rather than writes something quietly wrong, or supply an
/// <see cref="ICommitRebaseHandler"/> that re-derives them. Row tracking and deletion vectors are the two
/// cases in practice; the table layer implements both.</para>
///
/// <para>Stateless and reusable across commits. Safe to share between callers that do not otherwise
/// interfere — the loop's only mutable state is per-call.</para>
/// </summary>
public sealed class LogCommitter
{
    private readonly TransactionLog _log;
    private readonly LogCommitOptions _options;
    private readonly CheckpointWriter? _checkpointWriter;

    /// <param name="log">The log to commit to.</param>
    /// <param name="options">Checkpoint interval, protocol gating, statistics preference. Null takes
    /// <see cref="LogCommitOptions.Default"/>.</param>
    public LogCommitter(TransactionLog log, LogCommitOptions? options = null)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _options = options ?? LogCommitOptions.Default;
        if (_options.CheckpointInterval > 0)
            _checkpointWriter = _options.CheckpointWriter ?? new CheckpointWriter(log.FileSystem);
    }

    /// <summary>
    /// Commits <paramref name="request"/>'s actions, rebasing past non-conflicting concurrent commits.
    /// </summary>
    /// <returns>The version it landed at and the table state there — or the base version with
    /// <see cref="LogCommitResult.Committed"/> false when there was nothing to commit.</returns>
    /// <exception cref="DeltaConflictException">A concurrent commit invalidated this one (the message names
    /// the version and the rule), or the request is not rebase-safe and the version was taken, or the
    /// attempt budget ran out.</exception>
    /// <exception cref="DeltaFormatException">The table declares writer features this library does not
    /// implement, and <see cref="LogCommitOptions.ValidateWriteProtocol"/> is on.</exception>
    public async ValueTask<LogCommitResult> CommitAsync(
        LogCommitRequest request, CancellationToken cancellationToken = default)
    {
        if (request is null)
            throw new ArgumentNullException(nameof(request));
        if (request.MaxAttempts < 1)
            throw new ArgumentOutOfRangeException(
                nameof(request), request.MaxAttempts,
                $"{nameof(LogCommitRequest.MaxAttempts)} must be at least 1 — a commit has to be tried once.");

        var baseSnapshot = request.BaseSnapshot;
        var stagedActions = request.Actions;

        // Before anything is written: a table whose writer features we do not implement must not be
        // committed to at all. See LogCommitOptions.ValidateWriteProtocol for why this lives here.
        if (_options.ValidateWriteProtocol)
            ProtocolVersions.ValidateWriteSupport(baseSnapshot.Protocol);

        // Once against the base version, before anything is attempted: a precondition already false is
        // worth reporting before burning an attempt.
        request.Precondition?.Invoke(baseSnapshot, null);

        if (stagedActions.Count == 0)
            return new LogCommitResult(baseSnapshot.Version, baseSnapshot, Committed: false);

        // The actions actually written this attempt. Starts as the staged actions; a rebase replaces it
        // with the handler's re-derivation against the latest snapshot. Always sourced from the ORIGINAL
        // staged actions (the handler is given both), never from a prior rebase, so each retry rebases the
        // stable staged work onto whatever the newest version holds.
        var currentActions = stagedActions;

        // Built on the first collision only, and only because the checker may need it: a commit that never
        // collides pays nothing for it, and its construction walks the whole table schema.
        var pruner = request.Pruner;

        long attemptVersion = baseSnapshot.Version + 1;
        for (int attempt = 0; ; attempt++)
        {
            var finalActions = InCommitTimestamp.EnsureCommitInfo(
                currentActions, baseSnapshot.Metadata.Configuration, request.Operation,
                request.IsBlindAppend);
            try
            {
                await _log.WriteCommitAsync(attemptVersion, finalActions, cancellationToken)
                    .ConfigureAwait(false);

                // Durable. Everything the commit names is the table's data now, readable by everyone —
                // signalled HERE rather than after the refresh below, because the refresh reads the log and
                // a token cancelled between the two lines is enough to make it throw. A caller that learned
                // of the commit only from a return value would then be cleaning up live files.
                request.OnCommitDurable?.Invoke();

                var snapshot = await SnapshotBuilder.UpdateAsync(
                    request.RefreshFrom ?? baseSnapshot, _log, cancellationToken).ConfigureAwait(false);

                if (request.WriteCheckpointOnInterval
                    && _checkpointWriter is not null
                    && attemptVersion % _options.CheckpointInterval == 0)
                {
                    await _checkpointWriter.WriteCheckpointAsync(snapshot, cancellationToken)
                        .ConfigureAwait(false);

                    // Cleanup runs ONLY here, immediately after a checkpoint, for two reasons that both
                    // matter: this is the moment older commits become redundant, so it is the earliest
                    // point the work is legitimate; and it bounds the cost, since a listing per COMMIT
                    // would put an O(log) scan on the hot path for something that can only change every
                    // CheckpointInterval versions. Delta triggers it the same way.
                    //
                    // Ordering is not incidental either — the checkpoint must be DURABLE before anything
                    // is deleted, or a failure between the two leaves commits removed with nothing
                    // covering them.
                    await LogCleanup.RunAsync(
                        _log, snapshot.Metadata.Configuration, attemptVersion,
                        DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                }

                return new LogCommitResult(attemptVersion, snapshot, Committed: true);
            }
            catch (DeltaConflictException) when (attempt + 1 < request.MaxAttempts)
            {
                // A rebase that needs the concurrent files' current state (their deletion vectors, the
                // advanced row-id high-water mark) has to replay the log into a snapshot; one that only
                // needs somewhere to land asks for the version, which is a listing rather than a replay.
                long latest;
                Snapshot.Snapshot? latestSnapshot = null;
                if (request.Rebase is { NeedsLatestSnapshot: true })
                {
                    latestSnapshot = await SnapshotBuilder.UpdateAsync(
                        baseSnapshot, _log, cancellationToken).ConfigureAwait(false);
                    latest = latestSnapshot.Version;
                }
                else
                {
                    latest = await _log.GetLatestVersionAsync(cancellationToken).ConfigureAwait(false);
                }

                var concurrent = new List<(long, IReadOnlyList<DeltaAction>)>();
                for (long v = baseSnapshot.Version + 1; v <= latest; v++)
                {
                    concurrent.Add((v,
                        await _log.ReadCommitAsync(v, cancellationToken).ConfigureAwait(false)));
                }

                // The rebase runs BEFORE the verdict so the verdict can ignore what it reconciled. With no
                // handler there is nothing to do: currentActions is still the staged list, and re-committing
                // it verbatim at a later version is exactly what a passing conflict check licenses.
                ISet<string>? resolvedPaths = null;
                if (request.Rebase is { } rebase)
                {
                    var rebased = await rebase.RebaseAsync(
                        new CommitRebaseContext(
                            baseSnapshot, latestSnapshot, latest, concurrent,
                            stagedActions, currentActions, request.Isolation, attempt),
                        cancellationToken).ConfigureAwait(false);
                    currentActions = rebased.Actions;
                    resolvedPaths = rebased.RowLevelResolvedPaths;
                }

                // Before the conflict verdict: a violated precondition is not a conflict and must not be
                // reported as one, whatever the checker would have said.
                request.Precondition?.Invoke(baseSnapshot, concurrent);

                pruner ??= new DeltaFilePruner(
                    baseSnapshot.Schema, baseSnapshot.Metadata.PartitionColumns,
                    _options.PreferTypedCheckpointStats);

                var verdict = ConflictChecker.Check(
                    request.Reads, pruner, request.Isolation,
                    currentActions, concurrent, resolvedPaths);
                if (verdict.HasConflict)
                {
                    throw new DeltaConflictException(
                        verdict.ErrorCode, verdict.Message!, ConflictRecovery.Replan,
                        conflictingVersion: verdict.ConflictingVersion,
                        attemptedVersion: attemptVersion);
                }

                if (!request.RebaseSafe)
                {
                    // Replan rather than Replay, and the distinction is the whole reason this throws:
                    // the checker found nothing wrong, so the WORK is fine — it is these particular
                    // actions that cannot move version. Rebuilding them against the newer snapshot is
                    // exactly what would fix it.
                    throw new DeltaConflictException(
                        DeltaErrorCodes.RebaseUnsafe,
                        "A concurrent commit landed and this operation cannot be safely rebased onto it; "
                        + "retry the operation.",
                        ConflictRecovery.Replan,
                        conflictingVersion: latest,
                        attemptedVersion: attemptVersion);
                }

                attemptVersion = latest + 1; // no conflict — rebase and retry
            }
        }
    }
}
