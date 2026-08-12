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
/// <para><b>Staging writes files; abandoning it takes them back.</b> A staged append's parquet, a staged
/// update's post-image, deletion vectors and change files are all on storage BEFORE
/// <see cref="CommitAsync"/> — nothing is visible until the commit, but the bytes are there. So the
/// transaction is <see cref="IAsyncDisposable"/>: <c>await using var txn = table.StartTransaction();</c>
/// deletes them if the transaction is refused, aborts, or is simply dropped on an exception path, and does
/// nothing at all once a commit has succeeded. <see cref="AbortAsync"/> is the same cleanup, named. Without
/// one of the two, every abandoned transaction's files sit on storage until VACUUM's retention horizon
/// passes — a crash-looping producer's whole batch, on every restart.</para>
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
/// it. Plan against <see cref="Snapshot"/> so the rows a staged delete names agree with what the
/// transaction validates.</para>
/// </summary>
public sealed class DeltaTransaction : IAsyncDisposable
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
    // Preconditions: facts about the base version that must hold on EVERY commit attempt. Distinct from
    // staged effects, which are rebased and retried — a precondition cannot become true by retrying.
    private readonly List<AppTransactionRequirement> _appTransactions = [];
    // Declarations: what the HOST read, which the commit loop cannot infer because it never saw the scan.
    private bool _declaredWholeTableRead;
    // Files the host declared its own scan read, keyed by add.path. DELIBERATELY a set of its own rather than
    // more entries in _removedPaths: that collection doubles as the commit loop's plannedRemovePaths, which
    // drives the delete/delete check, so a file merely READ would be taken for one this transaction plans to
    // REMOVE — turning a concurrent delete of it into a spurious ConcurrentDeleteDelete instead of the
    // ConcurrentDeleteRead it is. The two are unioned into ReadSet.Files at commit time and nowhere else.
    private readonly HashSet<string> _declaredReadPaths = new(StringComparer.Ordinal);
    // The base snapshot's active paths, materialized on the first DeclareFilesRead call so a host declaring
    // one file at a time does not walk the active set per call. The snapshot is pinned, so this cannot go stale.
    private HashSet<string>? _basePaths;
    // What commitInfo records, when the caller says rather than letting it be inferred.
    private string? _operation;
    // The files this transaction's OWN writers put on storage — what an abort deletes. Recorded as they are
    // written rather than derived from the staged actions at abort time, which cannot tell a fresh append's
    // add from a deletion-vector delete's re-add of LIVE data; see WrittenFileLedger.
    private readonly WrittenFileLedger _written = new();
    // Whether staging is closed. Set by a commit ATTEMPT (successful or not) and by an abort: a transaction
    // whose commit ran has already been rebased and validated, and one whose files are deleted has nothing
    // left to stage onto.
    private bool _completed;
    // Whether the commit SUCCEEDED. The distinction _completed cannot make, and the one that matters most:
    // after a successful commit the staged files are live table data, so an abort must not touch them.
    private bool _committed;
    // Whether the files have already been collected, so a second abort — or the dispose that follows one —
    // does no I/O and reports nothing.
    private bool _aborted;

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
    /// <see cref="RowSelection.FromRowAddresses"/> resolves into the paths
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

    /// <summary>The files this transaction's own writers created, for the commit loop to keep recording into
    /// and for <see cref="AbortAsync"/> to delete.</summary>
    internal WrittenFileLedger Written => _written;

    /// <summary>
    /// What this transaction's <c>commitInfo</c> records as its operation. Null — the default — keeps the
    /// inference: a transaction that staged exactly one kind of work reports that kind, and a mixed one
    /// reports <c>"WRITE"</c>, because Delta's operation field is ONE string per commit and no engine has a
    /// name for a fused DELETE+INSERT. Set it and it wins.
    ///
    /// <para>A property rather than a per-call argument because it describes the TRANSACTION, not any one
    /// staged thing: several staging calls cannot each name the commit's operation.</para>
    /// </summary>
    /// <exception cref="ArgumentException">The value is an empty or whitespace string. Null is how you ask
    /// for the inference; <c>""</c> would silently commit an operation-less commitInfo.</exception>
    public string? Operation
    {
        get => _operation;
        set
        {
            if (value is not null && string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException(
                    "Operation must be null (infer it) or a non-empty name. An empty string would commit a "
                    + "commitInfo naming no operation.",
                    nameof(value));
            }
            _operation = value;
        }
    }

    /// <summary>The operation the commit actually records: the caller's if set, else the inference.</summary>
    internal string EffectiveOperation =>
        _operation ?? (_operations.Count == 1 ? _operations.First() : "WRITE");

    /// <summary>
    /// This transaction's own claim about whether it READ anything, recorded as
    /// <c>commitInfo.isBlindAppend</c> for later writers to consult. Null — the default — records NOTHING.
    ///
    /// <para>A property rather than a per-call argument for the same reason as <see cref="Operation"/>: it
    /// describes the TRANSACTION, and several staging calls cannot each answer for the commit.</para>
    ///
    /// <para><b>⚠ Null is not false, and the difference is not uniform across readers.</b> delta-spark
    /// takes an absent flag as "not blind" and examines the commit, so against it silence costs only
    /// spurious conflicts. This library's own <see cref="Concurrency.ConflictChecker"/> instead infers from
    /// the commit's shape when the flag is absent, and an adds-only commit infers as BLIND — so against a
    /// later EW reader, silence on a staged append is read as a claim this transaction never made. <b>A
    /// host that knows it read should set <c>false</c> rather than leave this null</b>; null is for a host
    /// that genuinely does not know.</para>
    ///
    /// <para>Claiming <c>true</c> wrongly is the UNSAFE direction whichever reader sees it: the concurrent-
    /// append check is skipped and the conflict that was owed silently does not happen. Declare
    /// <c>true</c> only where the transaction genuinely read nothing.</para>
    ///
    /// <para>This library cannot derive it for a host with its own data plane. A transaction records the
    /// reads made THROUGH it, but a host that scanned the table itself and then staged the result has made
    /// a read this library never saw — which is exactly the case the claim exists for.</para>
    /// </summary>
    public bool? IsBlindAppend { get; set; }

    /// <summary>
    /// What the commit records: the host's claim if it made one, else <c>false</c> when this transaction
    /// itself recorded a read, else nothing.
    /// </summary>
    /// <remarks>
    /// <para><b>The derivation runs in ONE direction only, and that asymmetry is the whole design.</b> A
    /// read recorded through this transaction — a staged DELETE or UPDATE, a declared whole-table read, a
    /// read predicate — proves the transaction was not blind, so <c>false</c> is a fact rather than a
    /// guess. The converse does not hold: having recorded no read proves only that none came through THIS
    /// object, and a host that scanned the table itself and staged the result read something we never saw.
    /// So the absence of recorded reads stays silent rather than becoming <c>true</c>.</para>
    ///
    /// <para>Which is what makes the autocommit DML surface honest for free: <c>DeleteAsync</c> and
    /// <c>UpdateAsync</c> stage their work through a transaction, so their commits declare <c>false</c>
    /// without anyone passing anything, while a bare staged append — where only the host knows — declares
    /// nothing until the host says otherwise.</para>
    /// </remarks>
    internal bool? EffectiveIsBlindAppend =>
        IsBlindAppend
        ?? (_declaredWholeTableRead || _removedPaths.Count > 0 || _dvEdits.Count > 0
                || _readPredicates.Count > 0
            ? false
            : null);

    /// <summary>The app-transaction preconditions to re-check on every commit attempt.</summary>
    internal IReadOnlyList<AppTransactionRequirement> AppTransactions => _appTransactions;

    /// <summary>Whether the host declared that its scan read the whole table.</summary>
    internal bool DeclaredWholeTableRead => _declaredWholeTableRead;

    /// <summary>The files the host declared its scan read, by <c>add.path</c>. Unioned into the read-set at
    /// commit time — never into the planned removes; see the field's own remarks.</summary>
    internal ISet<string> DeclaredReadPaths => _declaredReadPaths;

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
    /// <para>Nothing is committed until <see cref="CommitAsync"/>, but the data files ARE written now —
    /// <see cref="AbortAsync"/> (or disposal) deletes them again if this transaction never commits. Returns
    /// the number of rows staged.</para>
    /// </summary>
    public async ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        // ComputeWriteActionsAsync below evaluates the table's constraints against these batches,
        // so the gate must not refuse the table first. The staging and DML paths deliberately do
        // not say this: none of them route through that method, so none of them validate.
        _table.ValidateWritable(
            _baseSnapshot, isAppend: true,
            rowsWillBeValidated: WriteTimeExpressions.Declares(_baseSnapshot));

        var (actions, nextRowId) = await _table.ComputeWriteActionsAsync(
            _baseSnapshot, batches, DeltaWriteMode.Append,
            overwritePartitions: null, dynamicPartitionOverwrite: false, repartitionTo: null,
            cancellationToken, rowIdStart: _nextRowId, written: _written).ConfigureAwait(false);

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
        EnsureOpen();
        _table.ValidateWritable(_baseSnapshot, isAppend: false);

        var plan = await _table.ComputeDeleteActionsAsync(
            _baseSnapshot, predicate, cancellationToken, written: _written)
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
        EnsureOpen();
        _table.ValidateWritable(_baseSnapshot, isAppend: false);

        var plan = await _table.ComputeDeleteActionsAsync(
            _baseSnapshot, DeltaTable.MaskFor(predicate), cancellationToken, prunePredicate: predicate,
            written: _written)
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
    /// now — and deleted again by <see cref="AbortAsync"/> if this transaction never commits. The files this
    /// update REPLACES are untouched by an abort: they are the table's data until the commit lands. Returns
    /// the number of rows this update matched.</para>
    /// </summary>
    public async ValueTask<long> UpdateAsync(
        Func<RecordBatch, BooleanArray> predicate,
        Func<RecordBatch, RecordBatch> updater,
        CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        // ComputeUpdateActionsAsync re-validates the post-image and recomputes generated
        // columns, so a constrained table is writable here.
        _table.ValidateWritable(
            _baseSnapshot, isAppend: false,
            rowsWillBeValidated: WriteTimeExpressions.Declares(_baseSnapshot));

        var plan = await _table.ComputeUpdateActionsAsync(
            _baseSnapshot, predicate, updater, cancellationToken, rowIdStart: _nextRowId, written: _written)
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
        EnsureOpen();

        // ComputeUpdateActionsAsync re-validates the post-image and recomputes generated
        // columns, so a constrained table is writable here.
        _table.ValidateWritable(
            _baseSnapshot, isAppend: false,
            rowsWillBeValidated: WriteTimeExpressions.Declares(_baseSnapshot));

        var plan = await _table.ComputeUpdateActionsAsync(
            _baseSnapshot, DeltaTable.MaskFor(predicate), updater, cancellationToken,
            prunePredicate: predicate, rowIdStart: _nextRowId, written: _written).ConfigureAwait(false);

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
    /// <param name="constraintsEnforcedByCaller">
    /// Declares that the caller has already enforced the table's CHECK constraints, invariants and
    /// generated columns over the rows in <paramref name="files"/>. Nothing here can verify it —
    /// the files are finished — so it is an assertion, and a false one commits rows every later
    /// reader will trust. Left false, a table declaring any of those is refused, and
    /// <see cref="DeltaTable.SupportsExternalDataFileCommit"/> reports false for it.
    /// </param>
    public void StageDataFiles(
        IReadOnlyList<WrittenDataFile> files, bool constraintsEnforcedByCaller = false)
    {
        EnsureOpen();
        if (files is null)
            throw new ArgumentNullException(nameof(files));

        _table.ValidateWritable(
            _baseSnapshot, isAppend: true, rowsWillBeValidated: constraintsEnforcedByCaller);

        if (files.Count == 0)
            return;

        var (actions, nextRowId) = _table.BuildStagedAppendActions(_baseSnapshot, files, _nextRowId);
        _nextRowId = nextRowId;
        StageInternal(actions);
        _operations.Add("WRITE");
    }

    /// <summary>
    /// Stages already-written data files with full parity to
    /// <see cref="DeltaTable.CommitDataFilesAsync"/> — the same append, plus the two things only the
    /// auto-committing surface could express. Use <see cref="StageDataFiles"/> for the plain case; reach for
    /// this one only when you need an argument below.
    ///
    /// <para>Returns the rows the commit will make VISIBLE: every file's row count, less anything
    /// <paramref name="bornDeleted"/> hides.</para>
    /// </summary>
    /// <param name="bornDeleted">Rows this transaction inserted and then deleted, so they never appear in any
    /// committed version — keyed by <see cref="WrittenDataFile.RelativePath"/>, which is what
    /// <c>add.path</c> becomes. The add is born with an inline deletion vector and its stats marked
    /// <c>tightBounds=false</c>, as the spec requires once a vector hides rows the bounds were computed over.
    /// The key can only name a file in this same call — these files are in no snapshot yet — and a path or
    /// position that does not is reported rather than dropped.</param>
    /// <param name="identityValuesPreGenerated">The caller generated the identity-column values itself (see
    /// <see cref="DeltaTable.GenerateIdentityValues"/>), so the write-time per-row processing an outside
    /// writer skipped has already happened. Without this an identity table's appends cannot be staged at
    /// all — the reason this overload exists.</param>
    public async ValueTask<long> StageDataFilesAsync(
        IReadOnlyList<WrittenDataFile> files,
        RowSelection? bornDeleted = null,
        bool identityValuesPreGenerated = false,
        CancellationToken cancellationToken = default,
        bool constraintsEnforcedByCaller = false)
    {
        EnsureOpen();
        if (files is null)
            throw new ArgumentNullException(nameof(files));

        _table.ValidateWritable(
            _baseSnapshot, isAppend: true, rowsWillBeValidated: constraintsEnforcedByCaller);
        if (files.Count == 0)
            return 0;

        var (actions, nextRowId, liveRows) = await _table.BuildStagedAppendActionsAsync(
            _baseSnapshot, files, bornDeleted, identityValuesPreGenerated, _nextRowId, cancellationToken,
            // Only the born-deleted vector is ours. The DATA files came in already written by the host, so an
            // abort leaves them where they are — it never staged them, and it does not own them.
            written: _written)
            .ConfigureAwait(false);

        _nextRowId = nextRowId;
        StageInternal(actions);
        _operations.Add("WRITE");
        return liveRows;
    }

    /// <summary>
    /// Stages a deletion-vector DELETE of rows the caller identified itself — the host-driven counterpart of
    /// <see cref="DeleteAsync(Func{RecordBatch, BooleanArray}, CancellationToken)"/>, for an engine that
    /// evaluated its own predicate and knows which rows must go. Rows are named by
    /// <see cref="RowSelection"/>: absolute in-file positions per <c>add.path</c>. Build it against
    /// <see cref="Snapshot"/> — this transaction's pinned version — so the addresses and what the commit
    /// validates agree. Returns the rows newly hidden.
    ///
    /// <para>Each touched file's existing vector is unioned with the new positions, so repeated deletes
    /// compose, and a position already covered is not counted or replayed. The per-file edits are recorded, so
    /// at commit a concurrent delete of DIFFERENT rows in the same file reconciles row-by-row and a concurrent
    /// rewrite relocates these rows by stable id — the caller does not drive that rebase.</para>
    /// </summary>
    public async ValueTask<long> StageRowDeletesAsync(
        RowSelection selection,
        CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        if (selection is null)
            throw new ArgumentNullException(nameof(selection));
        _table.ValidateWritable(_baseSnapshot, isAppend: false);
        if (selection.IsEmpty)
            return 0;

        var result = await _table.ComputeDvActionsWithEditsAsync(
            selection, _baseSnapshot, cancellationToken, written: _written).ConfigureAwait(false);
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
        EnsureOpen();
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
    /// <param name="rowIds">On a ROW TRACKING table, each row's stable row id — one per row of
    /// <paramref name="rows"/>, ordinarily what a <see cref="DeltaRowMetadata.RowTracking"/> read reported
    /// for it. A change file is the only place a change row's identity can live (a <c>cdc</c> action has no
    /// <c>baseRowId</c>), so omitting these leaves the staged rows with NULL ids on the feed.</param>
    /// <param name="rowCommitVersions">The commit-version companion of <paramref name="rowIds"/>: the version
    /// each row was last changed in. Omitted, every row defaults to the version this transaction commits —
    /// correct for a post-image, wrong for a pre-image.</param>
    public async ValueTask StageChangeDataAsync(
        RecordBatch rows, string changeType, CancellationToken cancellationToken = default,
        Int64Array? rowIds = null, Int64Array? rowCommitVersions = null)
    {
        EnsureOpen();
        if (rows is null)
            throw new ArgumentNullException(nameof(rows));
        if (rows.Length == 0)
            return;
        _table.ValidateChangeDataStageable(_baseSnapshot, changeType);

        var files = await _table.WriteChangeDataFilesForAsync(
            _baseSnapshot, rows, changeType, cancellationToken, rowIds, rowCommitVersions, written: _written)
            .ConfigureAwait(false);
        StageInternal(files);
    }

    /// <summary>
    /// Stages arbitrary pre-built actions — the escape hatch for what the typed methods do not cover
    /// (a <see cref="TransactionId"/> for an idempotent producer, a <see cref="DomainMetadata"/> of the host's
    /// own). The actions are committed verbatim, so the caller owns their correctness; anything carrying
    /// snapshot-relative state (row-id ranges, deletion-vector positions) belongs in a typed method instead,
    /// which is what lets the commit loop rebase it.
    /// </summary>
    public void StageActions(IReadOnlyList<DeltaAction> actions)
    {
        EnsureOpen();
        if (actions is null)
            throw new ArgumentNullException(nameof(actions));
        StageInternal(actions);
    }

    // ── Preconditions ──────────────────────────────────────────────────────────────────────────────────
    //
    // Not effects. An effect is staged output: the commit loop rebases it onto a newer version and retries.
    // A precondition is a fact about the base version, re-checked before EVERY attempt — and it cannot
    // become true by retrying, so a violation throws InvalidOperationException rather than
    // DeltaConflictException, which the loop would retry.

    /// <summary>
    /// Idempotent-producer compare-and-set: commits a <c>txn</c> action recording that
    /// <paramref name="appId"/> has reached <paramref name="version"/>, atomically with everything else this
    /// transaction stages, guarded by <paramref name="precondition"/>.
    ///
    /// <para>This is what lets a streaming producer make "write this batch exactly once" true across
    /// retries: the batch's data and the record of having written it land in ONE version, so there is no
    /// window in which one exists without the other.</para>
    ///
    /// <para><b>Why the violation is not a conflict.</b> A failed precondition throws
    /// <see cref="AppTransactionPreconditionException"/> — an
    /// <see cref="InvalidOperationException"/>, not a <see cref="DeltaConflictException"/>. The commit loop
    /// RETRIES the latter, and retrying cannot make an already-committed batch un-commit — a producer told
    /// "conflict" would keep trying to write a batch that is already in the table.</para>
    ///
    /// <para>Requirements for DIFFERENT appIds in one transaction are evaluated independently, and the first
    /// that fails aborts the commit naming itself. There is no combining rule to reason about because there
    /// is no correct one: a transaction is a single commit, so partially applying it is not available.</para>
    /// </summary>
    /// <param name="appId">The producer's identifier. One transaction may require at most one version per
    /// appId: two <c>txn</c> actions for one appId in a single commit is malformed, and the surviving one
    /// would be whichever the reader saw last.</param>
    /// <param name="version">The version to record for <paramref name="appId"/>.</param>
    /// <param name="precondition">What the table must ALREADY record for <paramref name="appId"/>,
    /// re-checked against every concurrent commit before each attempt. Defaults to
    /// <see cref="AppTransactionPrecondition.None"/> — write unconditionally.
    ///
    /// <para>A producer's FIRST batch wants <see cref="AppTransactionPrecondition.Absent"/>: without it that
    /// batch has no guard available at all, since a replay of it has no prior version to name, so it writes
    /// unconditionally and the batch lands TWICE — and quietly, because the recorded version is rewritten
    /// with the same value, leaving nothing in the table to say it happened. Later batches want
    /// <see cref="AppTransactionPrecondition.Exactly"/> or
    /// <see cref="AppTransactionPrecondition.NotApplied"/>; see
    /// <see cref="AppTransactionPrecondition"/> for which, and why it matters.</para></param>
    public void RequireAppTransaction(
        string appId, long version, AppTransactionPrecondition precondition = default)
    {
        EnsureOpen();
        if (string.IsNullOrEmpty(appId))
            throw new ArgumentException("appId must be a non-empty identifier.", nameof(appId));

        foreach (var existing in _appTransactions)
        {
            if (string.Equals(existing.AppId, appId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"This transaction already requires an app transaction for '{appId}'. A commit carries "
                    + "at most one txn action per appId; requiring two would leave which one lands to the "
                    + "reader's action ordering.");
            }
        }

        _appTransactions.Add(new AppTransactionRequirement(appId, version, precondition));
    }

    /// <summary>
    /// Whether this transaction's base version already records <paramref name="version"/> or higher for
    /// <paramref name="appId"/> — that is, whether <see cref="AppTransactionPrecondition.NotApplied"/> would
    /// refuse a commit of it.
    ///
    /// <para>Ask this BEFORE staging. <see cref="WriteAsync"/> writes its data files immediately, so a batch
    /// staged and then refused at commit is parquet written for nothing; a producer that expects to replay
    /// routinely should skip the batch rather than write it and be told no. The commit-time precondition stays
    /// necessary either way — it closes the race between this answer and the commit, which a concurrent twin
    /// producer can still lose you — and when it does refuse, <see cref="AbortAsync"/> takes the batch's files
    /// back off storage rather than leaving them for VACUUM.</para>
    ///
    /// <para>Answered against this transaction's PINNED base snapshot, not the table's current version, so it
    /// answers the same question the first commit attempt will ask. Those differ whenever the transaction was
    /// started at an explicit base version, or the table's snapshot has since moved.</para>
    /// </summary>
    public bool IsAppTransactionApplied(string appId, long version)
    {
        if (string.IsNullOrEmpty(appId))
            throw new ArgumentException("appId must be a non-empty identifier.", nameof(appId));

        long? recorded = _baseSnapshot.AppTransactions.TryGetValue(appId, out var txn)
            ? txn.Version
            : null;
        return !AppTransactionPrecondition.NotApplied.Holds(recorded, version);
    }

    // ── Declarations ───────────────────────────────────────────────────────────────────────────────────
    //
    // Neither effects nor preconditions: they WIDEN the read set the conflict checker tests concurrent
    // commits against. The library cannot infer them — the host's own engine did the scan — and, unlike a
    // precondition, a declaration is subject to POLICY: the isolation level decides what conflicts with it.

    /// <summary>
    /// Declares that this transaction's work depended on the rows matching <paramref name="predicate"/> —
    /// what the HOST's own scan read, which the commit loop has no other way to know.
    ///
    /// <para>A concurrent commit that ADDS a file matching the predicate is then a
    /// <c>concurrentAppend</c> conflict under <see cref="IsolationLevel.Serializable"/>; under
    /// <see cref="IsolationLevel.WriteSerializable"/> a blind append is exempt, which is what distinguishes
    /// the two levels. Declaring several predicates widens the set — any one matching conflicts.</para>
    /// </summary>
    public void DeclareRead(Expressions.Predicate predicate)
    {
        EnsureOpen();
        if (predicate is null)
            throw new ArgumentNullException(nameof(predicate));
        _readPredicates.Add(predicate);
    }

    /// <summary>
    /// Declares that this transaction's work depended on exactly these FILES — the middle ground between
    /// <see cref="DeclareRead"/>'s predicate and <see cref="DeclareWholeTableRead"/>'s everything, and the
    /// form a host already holds: <see cref="DeltaTable.PlanFiles"/> hands it precisely this list.
    ///
    /// <code>
    /// var planned = table.PlanFiles(predicate, snapshot: txn.Snapshot);
    /// txn.DeclareFilesRead(planned.Select(p => p.File.Path).ToList());
    /// </code>
    ///
    /// <para>A concurrent commit that makes a <c>dataChange=true</c> REMOVE of any declared file is then a
    /// <c>concurrentDeleteRead</c> conflict, at both isolation levels — the data this transaction decided on
    /// is gone. A remove of a file NOT declared is not, which is the whole point:
    /// <see cref="DeclareWholeTableRead"/> aborts on every concurrent delete anywhere in the table, and a
    /// host that scanned three files of three hundred was saying far more than the truth.</para>
    ///
    /// <para><b>It buys no protection against concurrent ADDS,</b> and that is semantically right rather than
    /// a gap: a file that did not exist when you scanned was not in your read set, so a file-level
    /// declaration cannot speak to it. A host that also cares about phantom rows declares BOTH — these paths
    /// for what it read, <see cref="DeclareRead"/> for what it WOULD have read. They compose; they do not
    /// compete.</para>
    ///
    /// <para>Repeated calls accumulate, and <see cref="DeclareWholeTableRead"/> is strictly stronger: calling
    /// both keeps whole-table rather than erroring, since everything these paths would catch is already
    /// caught.</para>
    /// </summary>
    /// <param name="paths">The files read, keyed by <c>add.path</c> — the snapshot's own path, as
    /// <see cref="Actions.AddFile.Path"/> / <see cref="PlannedFile"/> report it, not a decoded or absolutized
    /// form of it. This is the same key <see cref="RowSelection"/> speaks. An empty collection declares
    /// nothing.</param>
    /// <exception cref="ArgumentException">A path is null or empty, or names a file that is not active at
    /// <see cref="ReadVersion"/>. Unlike a stale ordinal, a stale path is DETECTABLE, so it is reported
    /// rather than absorbed: such a path can never match a concurrent remove, so a declaration built from
    /// one would silently protect nothing. Nothing is declared when this throws — validate-then-apply, so a
    /// rejected list leaves the transaction as it was.</exception>
    public void DeclareFilesRead(IReadOnlyCollection<string> paths)
    {
        EnsureOpen();
        if (paths is null)
            throw new ArgumentNullException(nameof(paths));
        if (paths.Count == 0)
            return;

        _basePaths ??= BaseActivePaths();

        foreach (string path in paths)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentException("paths contains a null or empty path.", nameof(paths));

            if (!_basePaths.Contains(path))
            {
                throw new ArgumentException(
                    $"'{path}' is not an active file at version {_baseSnapshot.Version}, which is the version "
                    + "this transaction reads and validates against. Declare the paths PlanFiles returned for "
                    + "this transaction's Snapshot: a path from another version — or a decoded or absolutized "
                    + "form of one — can never match a concurrent remove, so the declaration would silently "
                    + "protect nothing.",
                    nameof(paths));
            }
        }

        foreach (string path in paths)
            _declaredReadPaths.Add(path);
    }

    /// <summary>
    /// The base snapshot's active files by <c>add.path</c>. Not
    /// <see cref="Snapshot.Snapshot.ActiveFiles"/>' own keys, which are the spec's reconciliation key
    /// <c>(path, deletionVector.uniqueId)</c> — a file carrying a deletion vector is not found under its bare
    /// path there, and a host that read such a file would be told its own planned path does not exist.
    /// </summary>
    private HashSet<string> BaseActivePaths()
    {
        // No capacity overload: HashSet(int, IEqualityComparer) is not in netstandard2.0.
        var paths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var add in _baseSnapshot.ActiveFiles.Values)
            paths.Add(add.Path);
        return paths;
    }

    /// <summary>
    /// Declares that this transaction's work depended on the WHOLE table — stronger than any set of
    /// predicates, since every concurrent add and remove becomes relevant. The honest declaration when a
    /// scan had no pushable predicate, and the one a host reaches for precisely when it has nothing better
    /// to say.
    ///
    /// <para>Say less when you can say less. <see cref="DeclareFilesRead"/> states the files a scan actually
    /// touched — which <see cref="DeltaTable.PlanFiles"/> already handed you — and a concurrent delete
    /// somewhere else then does not abort you. Reach for whole-table when the scan genuinely had no bound,
    /// not merely because the file list was inconvenient to carry.</para>
    ///
    /// <para><b>Caveat, and it is undecided rather than merely undocumented.</b> A proposal carried
    /// downstream (issue #15, open question 4) would DROP this declaration when the transaction also stages
    /// row-level deletes and runs at <see cref="IsolationLevel.WriteSerializable"/> — which is the default —
    /// on the reasoning that commits may be reordered relative to reads at that level. It is NOT implemented
    /// here: today the declaration is honoured at both levels, and a <c>dataChange=true</c> remove of a file
    /// covered by it raises <c>concurrentDeleteRead</c>, which is what Delta does (Spark gates only
    /// <c>concurrentAppend</c> on the isolation level). The proposal is a DEPARTURE from that, gated on the
    /// level rather than an implementation of it, and if it is ever adopted it should be an explicit
    /// per-transaction opt-in rather than an inference — a library must not claim on a host's behalf that it
    /// read less than it declared. Recorded here because it is the one behaviour in this seam a host cannot
    /// observe from the outside.</para>
    /// </summary>
    public void DeclareWholeTableRead()
    {
        EnsureOpen();
        _declaredWholeTableRead = true;
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
    ///
    /// <para>A commit that THROWS ends the transaction too, whatever it threw: a transaction is committed at
    /// most once. A conflict and a refused precondition are answers, not transient failures — and after an I/O
    /// failure or a cancellation the commit may have reached storage even though the caller never learned so,
    /// which is precisely when a blind second attempt would duplicate it. The files it wrote are still on
    /// storage at that point: abort or dispose the transaction to take them back.</para>
    /// </summary>
    public async ValueTask<long> CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        _completed = true;
        long version = await _table.CommitTransactionAsync(this, cancellationToken).ConfigureAwait(false);

        // Reached only when the commit LANDED. Everything the ledger names is live table data now, so it must
        // no longer name anything: a subsequent abort or dispose is then a no-op twice over — by state, and by
        // there being nothing left to delete.
        _committed = true;
        _written.Clear();
        return version;
    }

    /// <summary>
    /// Abandons this transaction and deletes the files it wrote but never committed — a staged append's
    /// parquet, an update's post-image, the deletion vectors and change files, including any a losing commit
    /// attempt wrote while rebasing. The transaction is then closed: a later <see cref="CommitAsync"/> fails
    /// cleanly rather than committing <c>add</c> actions whose files are gone.
    ///
    /// <para><b>What it does NOT delete</b> is everything this transaction did not itself write. A
    /// deletion-vector DELETE re-adds an EXISTING data file under a new vector — that parquet is live table
    /// data, and only the vector beside it is ours. Files handed in by <see cref="StageDataFiles"/> are the
    /// host's, written before they were staged and not this transaction's to collect. The distinction is
    /// recorded as the writers run, not inferred from the staged actions, which cannot make it.</para>
    ///
    /// <para><b>Best-effort, and deliberately quiet.</b> A delete that fails is swallowed: an orphaned file is
    /// what the old behaviour left behind anyway, so failing to collect one costs nothing that was not already
    /// lost — but throwing here would replace the exception that caused the abort with a cleanup error. This
    /// never throws.</para>
    ///
    /// <para>Idempotent, and a NO-OP after a successful <see cref="CommitAsync"/>: those files are the table's
    /// now. That is what makes <c>await using var txn = table.StartTransaction();</c> safe to write
    /// unconditionally — see <see cref="DisposeAsync"/>.</para>
    ///
    /// <para><paramref name="cancellationToken"/> bounds a cleanup that is running, but an ALREADY-cancelled
    /// one does not skip it: a host aborting on a cancellation path naturally passes the token that was just
    /// cancelled, and honouring it would make every delete fail, be swallowed, and leave an abort that
    /// collected nothing while reporting success.</para>
    /// </summary>
    public async ValueTask AbortAsync(CancellationToken cancellationToken = default)
    {
        if (_committed || _aborted)
            return;

        _aborted = true;
        _completed = true;
        // An already-cancelled token still cleans up — the guard lives in DeleteWrittenFilesAsync, since every
        // caller of it reaches it from a failure path and can arrive holding one.
        await _table.DeleteWrittenFilesAsync(_written, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Aborts the transaction unless it has already been committed — the reason to write
    /// <c>await using var txn = table.StartTransaction();</c>. A host that drops a transaction on an exception
    /// path then has its data files collected without having to remember, and one that commits pays nothing:
    /// disposal after a successful commit does no I/O at all.
    ///
    /// <para>Cleanup runs with no cancellation token, on purpose: disposal happens while an exception (often a
    /// cancellation) is already unwinding, and that is exactly when the files most need collecting. Call
    /// <see cref="AbortAsync"/> directly to pass one.</para>
    /// </summary>
    public ValueTask DisposeAsync() => AbortAsync(CancellationToken.None);

    /// <summary>
    /// Guards every staging call: work may only be added to a transaction that has neither committed nor
    /// aborted. The three closed states are reported apart because the remedies differ.
    /// </summary>
    private void EnsureOpen()
    {
        if (_aborted)
        {
            throw new InvalidOperationException(
                "This transaction was aborted: the files it wrote have been deleted, so nothing staged on it "
                + "can still be committed. Start a new transaction.");
        }
        if (_committed)
            throw new InvalidOperationException("This transaction has already been committed.");
        if (_completed)
        {
            // Deliberately says nothing about WHY. _completed is set before the commit runs, so a conflict, a
            // refused precondition, an I/O failure and a cancellation all land here — and after an I/O failure
            // or a cancellation the commit may even have reached storage, which is the other reason a second
            // attempt is not on offer.
            throw new InvalidOperationException(
                "This transaction's commit has already run and did not succeed. A transaction is committed at "
                + "most once, so it cannot be added to or re-committed; abort or dispose it to take back the "
                + "files it wrote, then start a new transaction.");
        }
    }

    /// <summary>One <see cref="RequireAppTransaction"/> call: the <c>txn</c> action to write, plus the
    /// compare-and-set guard the commit loop re-checks before every attempt.</summary>
    internal sealed record AppTransactionRequirement(
        string AppId, long Version, AppTransactionPrecondition Precondition);
}
