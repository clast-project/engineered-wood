// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// An optimistic-concurrency transaction over a <see cref="DeltaTable"/>, pinned to the table version
/// it was started at (see <see cref="DeltaTable.StartTransaction"/>).
///
/// <para>Stage read-dependent operations on it, then <see cref="CommitAsync"/>. At commit the
/// transaction is validated against every commit that landed since it started: if none invalidated
/// what it read, it commits — rebasing onto the newer version if another writer got there first —
/// otherwise it aborts with a <see cref="DeltaConflictException"/>. This is the standard Delta
/// OptimisticTransaction shape: record a read version, do the work, and let the commit fail only when a
/// concurrent change actually conflicts, rather than on every race.</para>
///
/// <para>The transaction holds a read snapshot; concurrent commits by others (including via the same
/// <see cref="DeltaTable"/> handle) do not disturb it. It is single-use — once committed it cannot be
/// reused. Not thread-safe: drive one transaction from one thread, though many transactions may race
/// across threads, which is the point.</para>
///
/// <para><b>Scope.</b> Appends (<see cref="WriteAsync"/>), deletes (<see cref="DeleteAsync"/>), and
/// updates (<see cref="UpdateAsync"/>) can be staged, including several on one transaction. An append is
/// a blind write with no read dependency, so two concurrent transactional appends both land; a
/// delete/update reads the files it rewrites, so it aborts only if a concurrent commit removed one of
/// them. Overwrite modes and row-level (same-file, disjoint-row) concurrency are planned additions; a
/// row-tracking table's staged work still commits uncontended but aborts rather than rebase (its
/// <c>baseRowId</c> would need recomputing against the advanced high-water mark).</para>
/// </summary>
public sealed class DeltaTransaction
{
    private readonly DeltaTable _table;
    private readonly Snapshot.Snapshot _baseSnapshot;
    private readonly List<DeltaAction> _dataActions = [];
    private readonly HashSet<string> _removedPaths = new(StringComparer.Ordinal);
    // The operations staged so far, so the commitInfo records what the transaction actually did rather
    // than a fixed label. A single-operation transaction reports that operation; a mixed one reports
    // "WRITE" (Delta's operation field is one string, and no engine has a name for a fused DELETE+INSERT).
    private readonly HashSet<string> _operations = new(StringComparer.Ordinal);
    // Analyzable read predicates staged by the Expressions.Predicate overloads of DeleteAsync/UpdateAsync.
    // They become the transaction's ReadSet.Predicates so a concurrent add matching one is a
    // concurrentAppend conflict. Left empty by the functional-predicate and append-only paths.
    private readonly List<Expressions.Predicate> _readPredicates = [];
    private bool _committed;

    internal DeltaTransaction(
        DeltaTable table, Snapshot.Snapshot baseSnapshot, IsolationLevel isolationLevel)
    {
        _table = table;
        _baseSnapshot = baseSnapshot;
        IsolationLevel = isolationLevel;
    }

    /// <summary>The table version this transaction reads from and validates against.</summary>
    public long ReadVersion => _baseSnapshot.Version;

    /// <summary>The isolation level this transaction is validated at.</summary>
    public IsolationLevel IsolationLevel { get; }

    internal Snapshot.Snapshot BaseSnapshot => _baseSnapshot;

    internal IReadOnlyList<DeltaAction> DataActions => _dataActions;

    internal ISet<string> RemovedPaths => _removedPaths;

    internal IReadOnlyList<Expressions.Predicate> ReadPredicates => _readPredicates;

    internal string Operation => _operations.Count == 1 ? _operations.First() : "WRITE";

    /// <summary>
    /// Stages an append of <paramref name="batches"/>, evaluated against this transaction's pinned read
    /// version. An append is a blind write — it depends on nothing the table currently holds — so it
    /// never conflicts with a concurrent delete or append and two concurrent transactional appends both
    /// land. It aborts only if a concurrent commit changed the table's metadata or protocol.
    ///
    /// <para>Nothing is committed until <see cref="CommitAsync"/>, but the data files ARE written now (an
    /// aborted transaction leaves them as vacuum-able orphans, like the auto-committer). Returns the
    /// number of rows staged.</para>
    /// </summary>
    public async ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches, CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        _table.ValidateWritable(_baseSnapshot, isAppend: true);

        var actions = await _table.ComputeWriteActionsAsync(
            _baseSnapshot, batches, DeltaWriteMode.Append,
            overwritePartitions: null, dynamicPartitionOverwrite: false, repartitionTo: null,
            cancellationToken).ConfigureAwait(false);

        _dataActions.AddRange(actions);
        _operations.Add("WRITE");

        long rows = 0;
        foreach (var batch in batches)
            rows += batch.Length;
        return rows;
    }

    /// <summary>
    /// Stages a delete of the rows matching <paramref name="predicate"/>, evaluated against this
    /// transaction's pinned read version. The predicate receives each batch (logical column names) and
    /// returns a <see cref="BooleanArray"/> where <c>true</c> marks a row for deletion.
    ///
    /// <para>Nothing is written until <see cref="CommitAsync"/>. The files this delete rewrites become
    /// the transaction's read-set: a concurrent commit that removed any of them aborts the commit.
    /// Returns the number of rows this delete matched.</para>
    /// </summary>
    public async ValueTask<long> DeleteAsync(
        Func<RecordBatch, BooleanArray> predicate, CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        _table.ValidateWritable(_baseSnapshot, isAppend: false);

        var plan = await _table.ComputeDeleteActionsAsync(_baseSnapshot, predicate, cancellationToken)
            .ConfigureAwait(false);

        _dataActions.AddRange(plan.DataActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _operations.Add("DELETE");

        return plan.TotalDeleted;
    }

    /// <summary>
    /// Stages a delete of the rows matching an analyzable <see cref="Expressions.Predicate"/>. Beyond the
    /// functional overload the predicate is recorded as a read dependency: a concurrent commit that adds a
    /// file matching it aborts this transaction (concurrentAppend), precise to the isolation level. Files
    /// whose statistics prove no row matches are skipped without being read. Returns the rows matched.
    /// </summary>
    public async ValueTask<long> DeleteAsync(
        Expressions.Predicate predicate, CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        _table.ValidateWritable(_baseSnapshot, isAppend: false);

        var plan = await _table.ComputeDeleteActionsAsync(
            _baseSnapshot, DeltaTable.MaskFor(predicate), cancellationToken, prunePredicate: predicate)
            .ConfigureAwait(false);

        _dataActions.AddRange(plan.DataActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _readPredicates.Add(predicate);
        _operations.Add("DELETE");

        return plan.TotalDeleted;
    }

    /// <summary>
    /// Stages an update of the rows matching <paramref name="predicate"/> via <paramref name="updater"/>,
    /// evaluated against this transaction's pinned read version. Like a delete it reads exactly the files
    /// it rewrites, so a concurrent commit that removed one of them aborts the commit.
    ///
    /// <para>Nothing is committed until <see cref="CommitAsync"/>, but the rewritten files ARE written
    /// now. Returns the number of rows this update matched.</para>
    /// </summary>
    public async ValueTask<long> UpdateAsync(
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        _table.ValidateWritable(_baseSnapshot, isAppend: false);

        var plan = await _table.ComputeUpdateActionsAsync(
            _baseSnapshot, predicate, updater, cancellationToken).ConfigureAwait(false);

        _dataActions.AddRange(plan.Actions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _operations.Add("UPDATE");

        return plan.TotalUpdated;
    }

    /// <summary>
    /// Stages an update of the rows matching an analyzable <see cref="Expressions.Predicate"/> via
    /// <paramref name="updater"/>. Like the analyzable delete, the predicate is recorded as a read
    /// dependency (concurrentAppend precision) and files that cannot match are skipped. Returns the rows
    /// matched.
    /// </summary>
    public async ValueTask<long> UpdateAsync(
        Expressions.Predicate predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        _table.ValidateWritable(_baseSnapshot, isAppend: false);

        var plan = await _table.ComputeUpdateActionsAsync(
            _baseSnapshot, DeltaTable.MaskFor(predicate), updater, cancellationToken,
            prunePredicate: predicate).ConfigureAwait(false);

        _dataActions.AddRange(plan.Actions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _readPredicates.Add(predicate);
        _operations.Add("UPDATE");

        return plan.TotalUpdated;
    }

    /// <summary>
    /// Validates and commits the staged work. Returns the committed version, or the read version
    /// unchanged when nothing was staged. Throws <see cref="DeltaConflictException"/> if a concurrent
    /// commit invalidated this transaction's reads.
    /// </summary>
    public async ValueTask<long> CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        _committed = true;
        return await _table.CommitTransactionAsync(this, cancellationToken).ConfigureAwait(false);
    }

    private void EnsureNotCommitted()
    {
        if (_committed)
            throw new InvalidOperationException("This transaction has already been committed.");
    }
}
