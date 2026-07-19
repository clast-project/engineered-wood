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
/// <para><b>Scope.</b> This first cut supports <see cref="DeleteAsync"/> — the read-modify-write
/// operation optimistic concurrency exists to protect, and the one whose file-level conflicts are
/// well-defined. Staging appends and updates on a transaction, and row-level (same-file, disjoint-row)
/// concurrency, are planned additions.</para>
/// </summary>
public sealed class DeltaTransaction
{
    private readonly DeltaTable _table;
    private readonly Snapshot.Snapshot _baseSnapshot;
    private readonly List<DeltaAction> _dataActions = [];
    private readonly HashSet<string> _removedPaths = new(StringComparer.Ordinal);
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

    internal string Operation => "DELETE";

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

        var plan = await _table.ComputeDeleteActionsAsync(_baseSnapshot, predicate, cancellationToken)
            .ConfigureAwait(false);

        _dataActions.AddRange(plan.DataActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);

        return plan.TotalDeleted;
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
