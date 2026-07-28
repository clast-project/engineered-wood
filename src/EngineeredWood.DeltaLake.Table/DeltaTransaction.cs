// Copyright (c) clast-project. All rights reserved.
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
/// them — and a delete of DIFFERENT rows in a file someone else also deleted from reconciles row-by-row
/// rather than aborting, including across a concurrent compaction that rewrote the file away. Row ids stay
/// correct throughout: staged work reserves a contiguous range, and a rebase re-derives it against the
/// advanced high-water mark. Overwrite modes are not stageable — they remove the whole active set, which
/// is exactly what a rebase cannot re-derive.</para>
///
/// <para><b>Host-staged work.</b> A host that owns its own data plane — its own parquet codec behind
/// <see cref="IDataFileWriter"/>, its own engine deciding which rows to delete — stages work it has
/// ALREADY done rather than handing over batches and predicates: <see cref="StageDataFiles"/>,
/// <see cref="StageRowDeletesAsync"/>, <see cref="StageSchemaChange"/>,
/// <see cref="StageChangeDataAsync"/>, and <see cref="StageActions"/>. These commit through the same
/// conflict-check, rebase, and retry loop as the computed ones, so an embedding host does not reimplement
/// it. Plan against <see cref="Snapshot"/> so the file ordinals a staged delete is keyed by agree with
/// what the transaction validates.</para>
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
    // Per-file row-level edits from staged DELETEs (the rows each removed, by absolute position). They let
    // the commit loop rebase this delete's deletion vectors onto a concurrent DV-delete of the same file
    // (row-level concurrency) instead of aborting. Only DELETEs contribute; appends and updates do not.
    private readonly List<DeltaTable.DeleteDvEdit> _dvEdits = [];
    // Where the NEXT staged add starts reserving stable row ids. Each staging call must continue from here
    // rather than restart at the base snapshot's high-water mark: two appends staged on one transaction
    // otherwise reserve the SAME ids, and the duplicate is invisible until a spec reader resolves two rows
    // to one identity. Null until the first row-tracking stage reads the base mark.
    private long? _nextRowId;
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

    /// <summary>
    /// The pinned snapshot this transaction reads, plans, and validates against. Pass it wherever a
    /// host-driven step needs to agree with the transaction on what the table looks like — most importantly
    /// <see cref="DeltaTable.PlanFiles"/>, whose <see cref="PlannedFile.FileOrdinal"/> values are the keys
    /// <see cref="StageRowDeletesAsync"/> expects. Planning against
    /// <see cref="DeltaTable.CurrentSnapshot"/> instead would silently key positions to a different file
    /// ordering once another writer commits.
    /// </summary>
    public Snapshot.Snapshot Snapshot => _baseSnapshot;

    /// <summary>The isolation level this transaction is validated at.</summary>
    public IsolationLevel IsolationLevel { get; }

    internal Snapshot.Snapshot BaseSnapshot => _baseSnapshot;

    internal IReadOnlyList<DeltaAction> DataActions => _dataActions;

    internal ISet<string> RemovedPaths => _removedPaths;

    internal IReadOnlyList<Expressions.Predicate> ReadPredicates => _readPredicates;

    internal IReadOnlyList<DeltaTable.DeleteDvEdit> DvEdits => _dvEdits;

    internal string Operation => _operations.Count == 1 ? _operations.First() : "WRITE";

    /// <summary>
    /// The row id the next staged add would reserve, or null if nothing staged advanced it. The commit emits
    /// ONE high-water-mark action from this — see <see cref="StageInternal"/> for why the per-operation ones
    /// are held back.
    /// </summary>
    internal long? NextRowId => _nextRowId;

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

        var (actions, nextRowId) = await _table.ComputeWriteActionsAsync(
            _baseSnapshot, batches, DeltaWriteMode.Append,
            overwritePartitions: null, dynamicPartitionOverwrite: false, repartitionTo: null,
            cancellationToken, rowIdStart: _nextRowId).ConfigureAwait(false);

        _nextRowId = nextRowId;
        StageInternal(actions);
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

        StageInternal(plan.DataActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _dvEdits.AddRange(plan.DvEdits);
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

        StageInternal(plan.DataActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _dvEdits.AddRange(plan.DvEdits);
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
            _baseSnapshot, predicate, updater, cancellationToken, rowIdStart: _nextRowId)
            .ConfigureAwait(false);

        _nextRowId = plan.NextRowId;
        StageInternal(plan.Actions);
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
            prunePredicate: predicate, rowIdStart: _nextRowId).ConfigureAwait(false);

        _nextRowId = plan.NextRowId;
        StageInternal(plan.Actions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _readPredicates.Add(predicate);
        _operations.Add("UPDATE");

        return plan.TotalUpdated;
    }

    // ── Host-staged work ───────────────────────────────────────────────────────────────────────────────
    //
    // The methods above compute their own data: they are the shape for a caller that hands engineered-wood
    // batches and predicates. A host that owns its data plane (its own parquet codec behind IDataFileWriter,
    // its own execution engine deciding which rows to delete) arrives with the work ALREADY DONE, and needs to
    // put it into the same transaction so it commits atomically with everything else — and, more importantly,
    // so it goes through the same conflict-check/rebase/retry loop instead of a hand-rolled one.

    /// <summary>
    /// Stages data files the caller has already written — by <see cref="DeltaTable.WriteDataFilesAsync"/>, or
    /// straight to storage by the host's own writer — as an append. The files exist; this records the
    /// <c>add</c> actions that will publish them, reserving each file's stable row-id range if the table has
    /// row tracking.
    ///
    /// <para>Append-shaped only, like <see cref="WriteAsync"/>: the overwrite family removes the whole active
    /// set, which is precisely what a rebase cannot re-derive. Throws if the table has identity columns or
    /// IcebergCompat, which need write-time per-row processing an outside writer did not do — check
    /// <see cref="DeltaTable.SupportsExternalDataFileCommit"/> first.</para>
    /// </summary>
    public void StageDataFiles(IReadOnlyList<WrittenDataFile> files)
    {
        EnsureNotCommitted();
        if (files is null)
            throw new ArgumentNullException(nameof(files));
        _table.ValidateWritable(_baseSnapshot, isAppend: true);
        if (files.Count == 0)
            return;

        var (actions, nextRowId) = _table.BuildStagedAppendActions(_baseSnapshot, files, _nextRowId);
        _nextRowId = nextRowId;
        StageInternal(actions);
        _operations.Add("WRITE");
    }

    /// <summary>
    /// Stages a deletion-vector DELETE of rows the caller identified itself — the host-driven counterpart of
    /// <see cref="DeleteAsync(Func{RecordBatch, BooleanArray}, CancellationToken)"/>, for an engine that
    /// evaluated its own predicate and knows which rows must go. Rows are addressed as ABSOLUTE in-file
    /// positions keyed by the file's ordinal in this transaction's pinned snapshot — the ordinals
    /// <see cref="DeltaTable.PlanFiles"/> reports when planned against <see cref="Snapshot"/>, and the high
    /// bits of the transient rowids the read paths emit. Returns the rows newly hidden.
    ///
    /// <para>Each touched file's existing vector is unioned with the new positions, so repeated deletes
    /// compose, and a position already covered is not counted or replayed. The per-file edits are recorded, so
    /// at commit a concurrent delete of DIFFERENT rows in the same file reconciles row-by-row and a concurrent
    /// rewrite relocates these rows by stable id — the caller does not drive that rebase.</para>
    /// </summary>
    public async ValueTask<long> StageRowDeletesAsync(
        IReadOnlyDictionary<int, IReadOnlyCollection<long>> positionsByOrdinal,
        CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        if (positionsByOrdinal is null)
            throw new ArgumentNullException(nameof(positionsByOrdinal));
        _table.ValidateWritable(_baseSnapshot, isAppend: false);
        if (positionsByOrdinal.Count == 0)
            return 0;

        var result = await _table.ComputeDvActionsWithEditsAsync(
            positionsByOrdinal, _baseSnapshot, cancellationToken).ConfigureAwait(false);
        if (result.RowsDeleted == 0)
            return 0;

        StageInternal(result.Actions);
        _dvEdits.AddRange(result.Edits);
        foreach (string path in result.TouchedPaths)
            _removedPaths.Add(path);
        _operations.Add("DELETE");
        return result.RowsDeleted;
    }

    /// <summary>
    /// Stages a schema change computed by one of <see cref="DeltaTable"/>'s <c>Compute*</c> methods
    /// (<see cref="DeltaTable.ComputeAddColumn"/>, <see cref="DeltaTable.ComputeRenameColumn"/>, …), so an
    /// ALTER lands in the SAME version as the data written under it. Compute the change against this
    /// transaction's <see cref="Snapshot"/>; a concurrent commit that changes metadata or protocol aborts the
    /// transaction rather than silently overwriting it.
    /// </summary>
    public void StageSchemaChange(DeltaTable.DeferredSchemaChange change)
    {
        EnsureNotCommitted();
        StageInternal(change.Actions);
        _operations.Add("ALTER");
    }

    /// <summary>
    /// Stages Change Data Feed rows for the statement the caller just executed — its deleted rows, or an
    /// update's pre- and post-images — as <c>_change_data</c> file(s) fused into this transaction's commit.
    /// <paramref name="changeType"/> is one of <c>delete</c> / <c>update_preimage</c> / <c>update_postimage</c>
    /// / <c>insert</c>; the <c>_change_type</c> column is added for you.
    ///
    /// <para><paramref name="rows"/> carry the table's logical columns INCLUDING partition columns: the rows
    /// are split per partition and each file's <c>partitionValues</c> encoded the way a data file's are, which
    /// is work a caller cannot do from outside the assembly. A commit carrying any cdc action is read
    /// cdc-only, so a change file staged here replaces — rather than adds to — what the reader would otherwise
    /// infer from this version's adds and removes.</para>
    /// </summary>
    public async ValueTask StageChangeDataAsync(
        RecordBatch rows, string changeType, CancellationToken cancellationToken = default)
    {
        EnsureNotCommitted();
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));
        if (rows.Length == 0)
            return;
        _table.ValidateChangeDataStageable(_baseSnapshot, changeType);

        var files = await _table.WriteChangeDataFilesForAsync(
            _baseSnapshot, rows, changeType, cancellationToken).ConfigureAwait(false);
        StageInternal(files);
    }

    /// <summary>
    /// Stages arbitrary pre-built actions — the escape hatch for what the typed methods do not cover
    /// (a <see cref="TransactionId"/> for an idempotent producer, a <see cref="DomainMetadata"/> of the host's
    /// own). The actions are committed verbatim, so the caller owns their correctness; anything carrying
    /// snapshot-relative state (row-id ranges, deletion-vector positions) belongs in a typed method instead,
    /// which is what lets the commit loop rebase it.
    /// </summary>
    public void StageActions(IReadOnlyList<DeltaAction> actions, string? operation = null)
    {
        EnsureNotCommitted();
        if (actions is null)
            throw new ArgumentNullException(nameof(actions));
        StageInternal(actions);
        if (!string.IsNullOrEmpty(operation))
            _operations.Add(operation!);
    }

    /// <summary>
    /// Adds staged actions, holding back the row-tracking high-water mark. That action is a per-domain
    /// SINGLETON — every staging call computes one, and a version carrying two <c>domainMetadata</c> entries
    /// for one domain is malformed — so the transaction re-emits exactly one from its running counter at
    /// commit time.
    /// </summary>
    private void StageInternal(IEnumerable<DeltaAction> actions)
    {
        foreach (var action in actions)
        {
            if (action is DomainMetadata dm && string.Equals(
                    dm.Domain, DeltaLake.RowTracking.RowTrackingConfig.DomainName, StringComparison.Ordinal))
            {
                continue;
            }
            _dataActions.Add(action);
        }
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
