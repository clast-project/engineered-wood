// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// An optimistic-concurrency transaction over a <see cref="DeltaTable"/>, pinned to the table version
/// it was started at (see <see cref="DeltaTable.StartTransaction(IsolationLevel?)"/>).
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
/// <para><b>Scope.</b> Appends (<see cref="WriteAsync"/>), deletes (<see cref="DeleteAsync(Expressions.Predicate, CancellationToken)"/>), and
/// updates (<see cref="UpdateAsync(Expressions.Predicate, Func{RecordBatch, RecordBatch}, CancellationToken)"/>) can be staged, including several on one transaction — so long as no two of them delete
/// from or rewrite the same data file. Each is computed against the base snapshot rather than against what
/// earlier ones staged, so a second operation on a file is refused; combine them into one. An append is
/// a blind write with no read dependency, so two concurrent transactional appends both land; a
/// delete/update reads the files it rewrites, so it aborts only if a concurrent commit removed one of
/// them — and a delete of DIFFERENT rows in a file someone else also deleted from reconciles row-by-row
/// rather than aborting, including across a concurrent compaction that rewrote the file away. Row ids stay
/// correct throughout: staged work reserves a contiguous range, and a rebase re-derives it against the
/// advanced high-water mark. The full, partition-scoped and dynamic overwrites are not stageable — they
/// remove the whole active set, or every file of a partition, which is exactly what a rebase cannot
/// re-derive. A predicate overwrite (Spark's <c>replaceWhere</c>) is: <see cref="OverwriteWhereAsync"/> reads
/// only the files its predicate can match and stages a predicate delete plus an append, which rebase like
/// any other.</para>
///
/// <para><b>Host-staged work.</b> A host that owns its own data plane — its own parquet codec behind
/// <see cref="IDataFileWriter"/>, its own engine deciding which rows to delete — stages work it has
/// ALREADY done rather than handing over batches and predicates: <see cref="StageDataFiles"/>,
/// <see cref="StageRowDeletesAsync(RowSelection, CancellationToken)"/>, <see cref="StageSchemaChange"/>,
/// <see cref="StageChangeDataAsync"/>, and <see cref="StageActions"/>. These commit through the same
/// conflict-check, rebase, and retry loop as the computed ones, so an embedding host does not reimplement
/// it. Plan against <see cref="Snapshot"/> so the rows a staged delete names agree with what the
/// transaction validates.</para>
///
/// <para><b>Change Data Feed.</b> A reader takes a version's change files as its whole change feed whenever it
/// has any, and infers the feed from adds and removes only when it has none. Spark and delta-rs commit one
/// operation per version, so the two never mix there; a transaction can mix them. The DELETEs and UPDATEs
/// computed here, and a copy-on-write staged delete, write their own change files, as each does committed alone.
/// Appends and deletion-vector deletes write none, also as alone, and stay inferred, unless the commit carries
/// some change file anyway. Then the commit writes their change rows itself — <c>insert</c> for an append's
/// rows, <c>delete</c> for the rows a vector newly hides — so nothing staged drops out of the feed. A host that
/// stages such rows itself with <see cref="StageChangeDataAsync"/> passes <c>changeDataStagedByCaller: true</c>
/// to the staging call so they do not appear twice. Actions staged raw with <see cref="StageActions"/> are the
/// host's to cover.</para>
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
    // Set by the functional-predicate DeleteAsync/UpdateAsync: they read through a condition we cannot
    // inspect, so every concurrent add is taken to match it (ReadSet.OpaquePredicate). Not a whole-table
    // read — removes stay scoped to the files the operation rewrote.
    private bool _opaqueReadPredicate;
    private readonly HashSet<string> _readDomains = new(StringComparer.Ordinal);
    // Per-file row-level edits from staged DELETEs (the rows each removed, by absolute position). They let
    // the commit loop rebase this delete's deletion vectors onto a concurrent DV-delete of the same file
    // (row-level concurrency) instead of aborting. Only DELETEs contribute; appends and updates do not.
    private readonly List<DeltaTable.DeleteDvEdit> _dvEdits = [];
    // Staged work that wrote no change rows of its own and whose host did not say it covered them: appends (the
    // adds, as staged) and deletion-vector deletes (the rows each newly hid). A commit that carries any cdc
    // action is read from its change files alone, so the commit writes change rows for these then (#530).
    private readonly List<AddFile> _uncoveredAppends = [];
    private readonly List<DeltaTable.DeleteDvEdit> _uncoveredVectorDeletes = [];
    // Where the NEXT staged add starts reserving stable row ids. Each staging call must continue from here
    // rather than restart at the base snapshot's high-water mark: two appends staged on one transaction
    // otherwise reserve the SAME ids, and the duplicate is invisible until a spec reader resolves two rows
    // to one identity. Null until the first row-tracking stage reads the base mark.
    private long? _nextRowId;
    // The same for identity columns: the high-water marks the appends staged so far left pending, and the one
    // metaData action that carries them. A later append generates values from these marks, not from the base
    // snapshot's (which would repeat values), and its metaData action replaces the earlier one, since a
    // commit must not carry two.
    private Dictionary<string, long>? _identityMarks;
    // The one metaData action the staged writes carry: an append's identity marks, or a mergeSchema write's merged
    // schema (which also carries the marks staged before it). Each write that emits one supersedes the last.
    private MetadataAction? _stagedIdentityMetadata;
    // Set once a mergeSchema write staged a schema change. Later writes then run against the merged schema
    // (PendingWriteSnapshot), and anything that would stage a metaData or protocol computed from the BASE schema
    // is refused, since it would drop the merge from the commit.
    private bool _schemaMerged;
    // The protocol upgrade the merges staged (typeWidening, timestampNtz, ...), cumulative: a later merge's
    // replaces it.
    private ProtocolAction? _stagedMergeProtocol;
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
    /// <see cref="StageRowDeletesAsync(RowSelection, CancellationToken)"/> expects. Planning against
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

    /// <summary>Whether a staged DELETE or UPDATE selected its rows through an opaque delegate.</summary>
    internal bool HasOpaqueReadPredicate => _opaqueReadPredicate;

    /// <summary>The <c>domainMetadata</c> domains staged schema changes were validated against; see
    /// <see cref="Concurrency.ReadSet.Domains"/>.</summary>
    internal IReadOnlyCollection<string> ReadDomains => _readDomains;

    internal IReadOnlyList<DeltaTable.DeleteDvEdit> DvEdits => _dvEdits;

    /// <summary>The staged appends whose rows no change file covers.</summary>
    internal IReadOnlyList<AddFile> UncoveredAppends => _uncoveredAppends;

    /// <summary>The staged deletion-vector deletes whose rows no change file covers.</summary>
    internal IReadOnlyList<DeltaTable.DeleteDvEdit> UncoveredVectorDeletes => _uncoveredVectorDeletes;

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
        _operation ?? (_operations.Count == 1 ? RecordedOperation(_operations.First()) : "WRITE");

    // A replaceWhere is staged as a kind of its own, so its metrics are not mistaken for a plain append's, and
    // records the operation Spark records for it.
    private const string ReplaceWhereKind = "REPLACE WHERE";

    private static string RecordedOperation(string kind) => kind == ReplaceWhereKind ? "WRITE" : kind;

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
    /// The caller's own metadata for this transaction's commit: <c>commitInfo.userMetadata</c> and any other
    /// keys. Read when <see cref="CommitAsync"/> runs, so it can be set at any point before then.
    /// </summary>
    public CommitProperties? CommitProperties { get; set; }

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
                || _readPredicates.Count > 0 || _opaqueReadPredicate || _mergeStaged
            ? false
            : null);

    // A staged MERGE makes the commit non-blind, as Spark records isBlindAppend=false for every MERGE: even
    // one that read nothing because no source key could match (all NULL), and so recorded no read set.
    private bool _mergeStaged;

    // The row counts of the staged DML, summed across staging calls, for the commit's operationMetrics.
    private long _deletedRows;
    private long _replaceWhereRows;
    private long _updatedRows;
    private MergeMetrics? _mergeMetrics;

    // Whether StageActions staged a data-changing add or remove. Its rows are not counted by any staging call,
    // so the commit's metrics would fold them into the DML's (an added file's rows into copied rows).
    private bool _rawDataActionsStaged;

    /// <summary>
    /// The <c>commitInfo.operationMetrics</c> the commit records, computed from the actions an attempt writes:
    /// Spark's metrics for a DELETE, UPDATE or MERGE. Only for a transaction whose staged work is that one kind
    /// of operation, and whose recorded operation is that kind. Spark records one operation per commit, and the
    /// metrics of a DELETE and an append together would describe neither. Several staging calls of the one kind
    /// sum: no file is staged twice in a transaction, so their file counts cannot overlap.
    ///
    /// <para>None either when <see cref="StageActions"/> staged a data-changing add or remove, whose rows no
    /// staging call counted, or when a commitInfo staged there names a different operation: that commitInfo is
    /// the one the commit records, and the DML's metrics would misdescribe it.</para>
    /// </summary>
    internal IReadOnlyDictionary<string, string>? OperationMetricsFor(IReadOnlyList<DeltaAction> actions)
    {
        if (_operations.Count != 1 || _rawDataActionsStaged)
            return null;
        string kind = _operations.First();
        string recorded = RecordedOperation(kind);
        if (EffectiveOperation != recorded)
            return null;
        foreach (var action in actions)
        {
            if (action is CommitInfo info
                && !(info.GetValue("operation") is { ValueKind: System.Text.Json.JsonValueKind.String } op
                    && op.GetString() == recorded))
            {
                return null;
            }
        }

        return kind switch
        {
            "DELETE" => DmlOperationMetrics.ForDelete(actions, _deletedRows),
            "UPDATE" => DmlOperationMetrics.ForUpdate(actions, _updatedRows),
            "MERGE" when _mergeMetrics is { } merge => DmlOperationMetrics.ForMerge(actions, merge),
            ReplaceWhereKind => DmlOperationMetrics.ForReplaceWhere(actions, _deletedRows, _replaceWhereRows),
            _ => null,
        };
    }

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
    ///
    /// <para><paramref name="mergeSchema"/> evolves the schema to fit the batches, in this transaction's commit,
    /// as <see cref="DeltaTable.WriteAsync(IReadOnlyList{RecordBatch}, DeltaWriteMode, CancellationToken, IReadOnlyList{string}, bool?, CommitProperties, bool)"/>
    /// does. Several merging writes compose, and every append staged after one sees the merged schema. A merge
    /// cannot share a transaction with a schema change staged any other way (<see cref="StageSchemaChange"/>, a
    /// <c>metaData</c> through <see cref="StageActions"/>), in either order: the commit carries one
    /// <c>metaData</c>, and each was computed without the other. Nor with a later MERGE or replaceWhere that
    /// advances identity marks, for the same reason.</para>
    /// </summary>
    public async ValueTask<long> WriteAsync(
        IReadOnlyList<RecordBatch> batches, CancellationToken cancellationToken = default, bool mergeSchema = false)
    {
        EnsureOpen();

        // ComputeWriteActionsAsync below evaluates the table's constraints against these batches,
        // so the gate must not refuse the table first. The staging and DML paths deliberately do
        // not say this: none of them route through that method, so none of them validate.
        _table.ValidateWritable(
            _baseSnapshot, isAppend: true,
            handling: WriteTimeExpressionHandling.ValidatedHere);

        var writeSnapshot = _schemaMerged || mergeSchema ? PendingWriteSnapshot() : _baseSnapshot;
        DeltaTable.DeferredSchemaChange? merge = null;
        if (mergeSchema)
        {
            merge = _table.ComputeSchemaMerge(writeSnapshot, batches);
            if (merge is { } change)
            {
                RefuseSchemaMergeAfterOtherMetadata();
                writeSnapshot = DeltaTable.WithStagedMetadata(writeSnapshot, change.Actions);
            }
        }

        var (actions, nextRowId) = await TakingBackOnFailureAsync(_written.Paths.Count, () => _table.ComputeWriteActionsAsync(
            writeSnapshot, batches, DeltaWriteMode.Append,
            overwritePartitions: null, dynamicPartitionOverwrite: false, repartitionTo: null,
            cancellationToken, rowIdStart: _nextRowId, written: _written, identityHighWaterMarks: _identityMarks),
            cancellationToken).ConfigureAwait(false);

        if (merge is { } merged)
        {
            actions = DeltaTable.WithSchemaMerge(actions, merged);
            if (merged.ProtocolUpgrade is { } protocol)
            {
                // Cumulative: computed on top of the previous merge's upgrade, so it replaces it.
                if (_stagedMergeProtocol is not null)
                    _dataActions.Remove(_stagedMergeProtocol);
                _stagedMergeProtocol = protocol;
            }
            _schemaMerged = true;
        }

        _nextRowId = nextRowId;
        StageInternal(actions);
        ChainIdentityMarks(actions);
        RecordUncoveredAppends(actions);
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
    /// the transaction's read-set: a concurrent commit that removed any of them aborts the commit. And since
    /// the delegate cannot be inspected, a concurrent commit that adds ANY file is taken to match it
    /// (concurrentAppend, precise to the isolation level: under the default
    /// <see cref="IsolationLevel.WriteSerializable"/> a concurrent blind append is still exempt). The
    /// <see cref="Expressions.Predicate"/> overload tests the added file against the predicate instead.
    /// Returns the number of rows this delete matched.</para>
    ///
    /// <para>A file the predicate matches entirely is removed. One it matches only in part gets a deletion
    /// vector when the table enables them, and is otherwise rewritten without the matched rows
    /// (copy-on-write); <see cref="DeleteAsync(Func{RecordBatch, BooleanArray}, RowDeleteMode, CancellationToken)"/>
    /// chooses explicitly.</para>
    /// </summary>
    public ValueTask<long> DeleteAsync(
        Func<RecordBatch, BooleanArray> predicate, CancellationToken cancellationToken = default)
        => StageDeleteAsync(predicate, analyzable: null, mode: null, cancellationToken);

    /// <summary>
    /// Stages a delete of the rows matching <paramref name="predicate"/>, as
    /// <see cref="DeleteAsync(Func{RecordBatch, BooleanArray}, CancellationToken)"/> does, choosing how a file
    /// the predicate matches only in part is changed.
    ///
    /// <para><see cref="RowDeleteMode.CopyOnWrite"/> rewrites it now without the matched rows — deleted again by
    /// <see cref="AbortAsync"/> if the transaction never commits — and stages <c>remove</c>(old) +
    /// <c>add</c>(rewritten), with the survivors' row ids preserved and a <c>delete</c> change file on a Change
    /// Data Feed table. It needs no deletion vectors, so the table stays readable by engines without them, and on
    /// a table that has vectors it removes the file's existing one along with the file.
    /// <see cref="RowDeleteMode.DeletionVector"/> soft-deletes the rows in place, and on a table without
    /// deletion vectors refuses such a delete instead of rewriting. A file the predicate matches entirely is
    /// removed whole in both modes, with no rewrite.</para>
    /// </summary>
    public ValueTask<long> DeleteAsync(
        Func<RecordBatch, BooleanArray> predicate, RowDeleteMode mode,
        CancellationToken cancellationToken = default)
        => StageDeleteAsync(predicate, analyzable: null, mode, cancellationToken);

    /// <summary>
    /// Stages a delete of the rows matching an analyzable <see cref="Expressions.Predicate"/>. Beyond the
    /// functional overload the predicate is recorded as a read dependency: a concurrent commit that adds a
    /// file matching it aborts this transaction (concurrentAppend), precise to the isolation level. Files
    /// whose statistics prove no row matches are skipped without being read. Returns the rows matched.
    ///
    /// <para>A file the predicate matches only in part gets a deletion vector when the table enables them, and
    /// is otherwise rewritten without the matched rows (copy-on-write);
    /// <see cref="DeleteAsync(Expressions.Predicate, RowDeleteMode, CancellationToken)"/> chooses explicitly.</para>
    /// </summary>
    public ValueTask<long> DeleteAsync(
        Expressions.Predicate predicate, CancellationToken cancellationToken = default)
        => StageDeleteAsync(MaskForNonNull(predicate), predicate, mode: null, cancellationToken);

    /// <summary>
    /// Stages a delete of the rows matching an analyzable <see cref="Expressions.Predicate"/>, as
    /// <see cref="DeleteAsync(Expressions.Predicate, CancellationToken)"/> does, choosing how a file the predicate
    /// matches only in part is changed — see
    /// <see cref="DeleteAsync(Func{RecordBatch, BooleanArray}, RowDeleteMode, CancellationToken)"/>.
    /// </summary>
    public ValueTask<long> DeleteAsync(
        Expressions.Predicate predicate, RowDeleteMode mode, CancellationToken cancellationToken = default)
        => StageDeleteAsync(MaskForNonNull(predicate), predicate, mode, cancellationToken);

    private static Func<RecordBatch, BooleanArray> MaskForNonNull(Expressions.Predicate predicate) =>
        DeltaTable.MaskFor(predicate ?? throw new ArgumentNullException(nameof(predicate)));

    /// <param name="predicate">The row mask: TRUE marks a row for deletion.</param>
    /// <param name="analyzable">The predicate <paramref name="predicate"/> evaluates, when the caller gave one: it
    /// prunes the scan and is recorded as the read dependency. Null for a delegate, which reads as opaque.</param>
    /// <param name="mode">Null lets the table decide: see <see cref="DeltaTable.DeletesByCopyOnWrite"/>.</param>
    private async ValueTask<long> StageDeleteAsync(
        Func<RecordBatch, BooleanArray> predicate, Expressions.Predicate? analyzable, RowDeleteMode? mode,
        CancellationToken cancellationToken)
    {
        EnsureOpen();
        if (predicate is null)
            throw new ArgumentNullException(nameof(predicate));
        _table.ValidateWritable(_baseSnapshot, isAppend: false);
        bool copyOnWrite = DeltaTable.DeletesByCopyOnWrite(_baseSnapshot, mode);

        int mark = _written.Paths.Count;
        var plan = await TakingBackOnFailureAsync(mark, () => _table.ComputeDeleteActionsAsync(
            _baseSnapshot, predicate, cancellationToken, prunePredicate: analyzable, written: _written,
            copyOnWrite: copyOnWrite, rowIdStart: _nextRowId), cancellationToken)
            .ConfigureAwait(false);
        await RefuseRestagedFilesAsync(plan.RemovedPaths, mark, "DELETE", cancellationToken)
            .ConfigureAwait(false);

        // A rewrite reserved row ids past the mark; the next staged write in this transaction starts after them.
        if (plan.NextRowId is long nextRowId)
            _nextRowId = nextRowId;
        StageInternal(plan.DataActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _dvEdits.AddRange(plan.DvEdits);
        if (analyzable is not null)
            _readPredicates.Add(analyzable);
        else
            _opaqueReadPredicate = true;
        _operations.Add("DELETE");
        _deletedRows += plan.TotalDeleted;

        return plan.TotalDeleted;
    }

    /// <summary>
    /// Stages an update of the rows matching <paramref name="predicate"/> via <paramref name="updater"/>,
    /// evaluated against this transaction's pinned read version. Like a delete it reads exactly the files
    /// it rewrites, so a concurrent commit that removed one of them aborts the commit; and as with the
    /// functional delete, a concurrent add of any file is taken to match the opaque predicate
    /// (concurrentAppend, precise to the isolation level).
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
            handling: WriteTimeExpressionHandling.ValidatedHere);

        int mark = _written.Paths.Count;
        var plan = await TakingBackOnFailureAsync(mark, () => _table.ComputeUpdateActionsAsync(
            _baseSnapshot, predicate, updater, cancellationToken, rowIdStart: _nextRowId, written: _written),
            cancellationToken).ConfigureAwait(false);
        await RefuseRestagedFilesAsync(plan.RemovedPaths, mark, "UPDATE", cancellationToken)
            .ConfigureAwait(false);

        _nextRowId = plan.NextRowId;
        StageInternal(plan.Actions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _opaqueReadPredicate = true;
        _operations.Add("UPDATE");
        _updatedRows += plan.TotalUpdated;

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
            handling: WriteTimeExpressionHandling.ValidatedHere);

        int mark = _written.Paths.Count;
        var plan = await TakingBackOnFailureAsync(mark, () => _table.ComputeUpdateActionsAsync(
            _baseSnapshot, DeltaTable.MaskFor(predicate), updater, cancellationToken,
            prunePredicate: predicate, rowIdStart: _nextRowId, written: _written), cancellationToken)
            .ConfigureAwait(false);
        await RefuseRestagedFilesAsync(plan.RemovedPaths, mark, "UPDATE", cancellationToken)
            .ConfigureAwait(false);

        _nextRowId = plan.NextRowId;
        StageInternal(plan.Actions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _readPredicates.Add(predicate);
        _operations.Add("UPDATE");
        _updatedRows += plan.TotalUpdated;

        return plan.TotalUpdated;
    }

    /// <summary>
    /// Stages a key-based MERGE of <paramref name="source"/>, evaluated against this transaction's pinned read
    /// version: see <see cref="DeltaTable.MergeAsync"/> and <see cref="MergeOptions"/>. Each file holding a row it
    /// updates or deletes is rewritten now, once, with all of that file's changes, and the unmatched source rows
    /// are written as an append; <see cref="AbortAsync"/> deletes them again if the transaction never commits.
    ///
    /// <para>What it read becomes the transaction's read set: every file it read, and the source's key range as a
    /// read predicate, so a concurrent commit that removed a file it read, or added one that could hold a source
    /// key, is a conflict (precise to the isolation level). With a not-matched-by-source clause it read the whole
    /// table. Like any staged operation it refuses to touch a file an earlier one on this transaction already
    /// rewrote.</para>
    /// </summary>
    /// <returns>What the MERGE did, as Spark's MERGE metrics name it.</returns>
    public async ValueTask<MergeMetrics> MergeAsync(
        IReadOnlyList<RecordBatch> source, MergeOptions options, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        // The rewrite re-validates the post-image and the insert goes through the append path, so a
        // constrained table is writable here. An append-only table is refused only when a row is updated or
        // deleted, before any file is rewritten: as Spark, whose MERGE fails on such a table only if it removes
        // a file, so one that only inserts lands.
        _table.ValidateWritable(
            _baseSnapshot, isAppend: false,
            handling: WriteTimeExpressionHandling.ValidatedHere, appendOnlyCheckedLater: true);

        int mark = _written.Paths.Count;
        var plan = await TakingBackOnFailureAsync(mark, () => _table.ComputeMergeActionsAsync(
            _baseSnapshot, source, options, cancellationToken, rowIdStart: _nextRowId, written: _written,
            identityHighWaterMarks: _identityMarks), cancellationToken).ConfigureAwait(false);
        await RefuseRestagedFilesAsync(plan.RemovedPaths, mark, "MERGE", cancellationToken)
            .ConfigureAwait(false);

        await RefuseBaseSchemaMetadataAfterMergeAsync(plan.InsertActions, mark, "MERGE", cancellationToken)
            .ConfigureAwait(false);
        if (plan.NextRowId is long nextRowId)
            _nextRowId = nextRowId;
        StageInternal(plan.RewriteActions);
        StageInternal(plan.InsertActions);
        ChainIdentityMarks(plan.InsertActions);
        RecordUncoveredAppends(plan.InsertActions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        foreach (string path in plan.ReadPaths)
        {
            if (!plan.RemovedPaths.Contains(path))
                _declaredReadPaths.Add(path);
        }
        if (plan.ReadsWholeTable)
            _declaredWholeTableRead = true;
        else if (plan.ReadPredicate is { } readPredicate)
            _readPredicates.Add(readPredicate);
        else if (plan.ScannedTarget)
            _opaqueReadPredicate = true;
        _operations.Add("MERGE");
        _mergeStaged = true;
        _mergeMetrics = _mergeMetrics is { } earlier ? DmlOperationMetrics.Sum(earlier, plan.Metrics) : plan.Metrics;

        return plan.Metrics;
    }

    /// <summary>
    /// Stages a replaceWhere: deletes the rows matching <paramref name="predicate"/> and appends
    /// <paramref name="batches"/>, which must all satisfy it. See
    /// <see cref="DeltaTable.OverwriteWhereAsync"/> for the semantics, which are Spark's.
    ///
    /// <para>The new rows are checked before anything is written, so a row outside the window stages nothing and
    /// writes nothing. The predicate becomes a read dependency, as for
    /// <see cref="DeleteAsync(Expressions.Predicate, CancellationToken)"/>. A transaction that stages only this
    /// records the operation <c>WRITE</c> with Spark's metrics for it. Like any staged operation it refuses to
    /// touch a file an earlier one on this transaction already rewrote.</para>
    /// </summary>
    /// <returns>The rows deleted and the rows written.</returns>
    public async ValueTask<(long RowsDeleted, long RowsWritten)> OverwriteWhereAsync(
        IReadOnlyList<RecordBatch> batches, Expressions.Predicate predicate,
        CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        if (batches is null)
            throw new ArgumentNullException(nameof(batches));
        if (predicate is null)
            throw new ArgumentNullException(nameof(predicate));

        // Not an append, so an append-only table is refused here, before reading, even when the window turns out
        // empty: Spark refuses it the same way. The append below validates the table's constraints itself.
        _table.ValidateWritable(
            _baseSnapshot, isAppend: false, handling: WriteTimeExpressionHandling.ValidatedHere);
        DeltaTable.RefuseReplaceWhere(predicate, _baseSnapshot);

        int mark = _written.Paths.Count;
        var (write, delete) = await TakingBackOnFailureAsync(mark, async () =>
        {
            // The new rows first: they are checked against the predicate before anything is written, so a row
            // outside the window fails the call before the delete reads a file.
            var w = await _table.ComputeWriteActionsAsync(
                _baseSnapshot, batches, DeltaWriteMode.Append,
                overwritePartitions: null, dynamicPartitionOverwrite: false, repartitionTo: null,
                cancellationToken, rowIdStart: _nextRowId, written: _written, identityHighWaterMarks: _identityMarks,
                replaceWhere: predicate).ConfigureAwait(false);
            bool assignRowIds = DeltaLake.RowTracking.RowTrackingConfig.IsSupported(_baseSnapshot.Protocol);
            var d = await _table.ComputeDeleteActionsAsync(
                _baseSnapshot, DeltaTable.MaskFor(predicate), cancellationToken, prunePredicate: predicate,
                written: _written, copyOnWrite: DeltaTable.DeletesByCopyOnWrite(_baseSnapshot, mode: null),
                rowIdStart: assignRowIds ? w.NextRowId : null).ConfigureAwait(false);
            return (w, d);
        }, cancellationToken).ConfigureAwait(false);
        await RefuseRestagedFilesAsync(delete.RemovedPaths, mark, "replaceWhere", cancellationToken)
            .ConfigureAwait(false);

        await RefuseBaseSchemaMetadataAfterMergeAsync(write.Actions, mark, "replaceWhere", cancellationToken)
            .ConfigureAwait(false);
        _nextRowId = delete.NextRowId ?? write.NextRowId;
        StageInternal(write.Actions);
        ChainIdentityMarks(write.Actions);
        RecordUncoveredAppends(write.Actions);
        StageInternal(delete.DataActions);
        foreach (string path in delete.RemovedPaths)
            _removedPaths.Add(path);
        _dvEdits.AddRange(delete.DvEdits);
        _readPredicates.Add(predicate);
        _operations.Add(ReplaceWhereKind);
        _deletedRows += delete.TotalDeleted;

        long rows = 0;
        foreach (var batch in batches)
            rows += batch.Length;
        _replaceWhereRows += rows;
        return (delete.TotalDeleted, rows);
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
    /// <param name="changeDataStagedByCaller">
    /// Declares that the caller stages these rows' <c>insert</c> change rows itself with
    /// <see cref="StageChangeDataAsync"/>. Left false, a commit that carries any change file writes them for
    /// the caller; see the Change Data Feed remarks on <see cref="DeltaTransaction"/>.
    /// </param>
    public void StageDataFiles(
        IReadOnlyList<WrittenDataFile> files, bool constraintsEnforcedByCaller = false,
        bool changeDataStagedByCaller = false)
    {
        EnsureOpen();
        if (files is null)
            throw new ArgumentNullException(nameof(files));

        _table.ValidateWritable(
            _baseSnapshot,
            isAppend: true,
            handling: constraintsEnforcedByCaller
                ? WriteTimeExpressionHandling.AssertedByCaller
                : WriteTimeExpressionHandling.Refuse);

        if (files.Count == 0)
            return;

        var (actions, nextRowId) = _table.BuildStagedAppendActions(_baseSnapshot, files, _nextRowId);
        _nextRowId = nextRowId;
        StageInternal(actions);
        if (!changeDataStagedByCaller)
            RecordUncoveredAppends(actions);
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
    /// position that does not is reported rather than dropped. Naming any rows requires a table with deletion
    /// vectors enabled; on one without them this throws <see cref="InvalidOperationException"/>.</param>
    /// <param name="identityValuesPreGenerated">The caller generated the identity-column values itself (see
    /// <see cref="DeltaTable.GenerateIdentityValues"/>), so the write-time per-row processing an outside
    /// writer skipped has already happened. Without this an identity table's appends cannot be staged at
    /// all — the reason this overload exists.</param>
    /// <param name="changeDataStagedByCaller">As for <see cref="StageDataFiles"/>: the caller stages the
    /// visible rows' <c>insert</c> change rows itself, so the commit never writes them.</param>
    public async ValueTask<long> StageDataFilesAsync(
        IReadOnlyList<WrittenDataFile> files,
        RowSelection? bornDeleted = null,
        bool identityValuesPreGenerated = false,
        CancellationToken cancellationToken = default,
        bool constraintsEnforcedByCaller = false,
        bool changeDataStagedByCaller = false)
    {
        EnsureOpen();
        if (files is null)
            throw new ArgumentNullException(nameof(files));

        _table.ValidateWritable(
            _baseSnapshot,
            isAppend: true,
            handling: constraintsEnforcedByCaller
                ? WriteTimeExpressionHandling.AssertedByCaller
                : WriteTimeExpressionHandling.Refuse);
        if (files.Count == 0)
            return 0;

        var (actions, nextRowId, liveRows) = await TakingBackOnFailureAsync(_written.Paths.Count, () => _table.BuildStagedAppendActionsAsync(
            _baseSnapshot, files, bornDeleted, identityValuesPreGenerated, _nextRowId, cancellationToken,
            // Only the born-deleted vector is ours. The DATA files came in already written by the host, so an
            // abort leaves them where they are — it never staged them, and it does not own them.
            written: _written), cancellationToken)
            .ConfigureAwait(false);

        _nextRowId = nextRowId;
        StageInternal(actions);
        if (!changeDataStagedByCaller)
            RecordUncoveredAppends(actions);
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
    /// <para>Each touched file's existing vector is unioned with the new positions, so a delete composes with
    /// the deletes COMMITTED before it, and a position already covered is not counted or replayed. It does not
    /// compose with work staged earlier on this transaction: a selection naming a file an earlier operation
    /// here already deleted from or rewrote throws <see cref="InvalidOperationException"/> — pass all of a
    /// file's positions in one selection. The per-file edits are recorded, so
    /// at commit a concurrent delete of DIFFERENT rows in the same file reconciles row-by-row and a concurrent
    /// rewrite relocates these rows by stable id — the caller does not drive that rebase.</para>
    ///
    /// <para>The table must have deletion vectors enabled; on one without them this throws
    /// <see cref="InvalidOperationException"/> rather than commit a vector its protocol never declared. Stage
    /// the delete with <see cref="RowDeleteMode.CopyOnWrite"/> instead.</para>
    ///
    /// <para>No change data is written at staging, as with Spark's deletion-vector DML: on a Change Data Feed
    /// table the deleted rows are what the new vector hides beyond the old one. If the commit carries some
    /// change file anyway, the commit writes these rows as <c>delete</c> change rows, so they stay in the feed.
    /// A host that stages them itself with <see cref="StageChangeDataAsync"/> says so with the
    /// <see cref="StageRowDeletesAsync(RowSelection, RowDeleteMode, CancellationToken, bool)"/> overload's
    /// <c>changeDataStagedByCaller</c>.</para>
    /// </summary>
    public ValueTask<long> StageRowDeletesAsync(
        RowSelection selection,
        CancellationToken cancellationToken = default) =>
        StageRowDeletesAsync(selection, RowDeleteMode.DeletionVector, cancellationToken);

    /// <summary>
    /// Stages a DELETE of rows the caller identified itself, as
    /// <see cref="StageRowDeletesAsync(RowSelection, CancellationToken)"/> does, choosing how the rows go.
    /// <see cref="RowDeleteMode.DeletionVector"/> is that overload exactly.
    ///
    /// <para><see cref="RowDeleteMode.CopyOnWrite"/> needs no deletion vectors, so it is the way to stage a
    /// delete on a table that must stay readable by engines without them. Each touched file is rewritten NOW,
    /// without the selected rows — and deleted again by <see cref="AbortAsync"/> if the transaction never
    /// commits — and the commit carries <c>remove</c>(old) + <c>add</c>(rewritten), just the <c>remove</c>
    /// when every row of a file goes. On a row-tracking table the survivors keep their ids. The rewritten
    /// files become the transaction's read-set: a concurrent change to one aborts the commit, with no
    /// row-by-row reconciliation as for a vector. On a Change Data Feed table each rewritten file also gets a
    /// <c>delete</c> change file holding the rows it lost, as an immediate copy-on-write delete writes.</para>
    ///
    /// <para>A selection naming a file an earlier operation on this transaction already deleted from or
    /// rewrote throws <see cref="InvalidOperationException"/>, in either mode. Returns the rows deleted.</para>
    /// </summary>
    /// <param name="changeDataStagedByCaller">Declares that the caller stages this delete's <c>delete</c>
    /// change rows itself with <see cref="StageChangeDataAsync"/>, so none are written for it: not the
    /// copy-on-write change file, and not the rows a commit carrying some other change file would otherwise
    /// write for a vector delete. Left false, the deleted rows reach the feed without the caller's help; staging
    /// them as well would put them in it twice.</param>
    public async ValueTask<long> StageRowDeletesAsync(
        RowSelection selection,
        RowDeleteMode mode,
        CancellationToken cancellationToken = default,
        bool changeDataStagedByCaller = false)
    {
        if (mode == RowDeleteMode.CopyOnWrite)
        {
            return await StageCopyOnWriteRowDeletesAsync(selection, changeDataStagedByCaller, cancellationToken)
                .ConfigureAwait(false);
        }
        if (mode != RowDeleteMode.DeletionVector)
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown RowDeleteMode.");

        EnsureOpen();
        if (selection is null)
            throw new ArgumentNullException(nameof(selection));
        _table.ValidateWritable(_baseSnapshot, isAppend: false);
        if (selection.IsEmpty)
            return 0;
        // The selection names its files up front, so this refusal costs no I/O and leaves nothing to collect.
        if (FirstRestagedPath(selection.Paths) is { } restaged)
            throw RestagedFile(restaged, "DELETE");

        var result = await TakingBackOnFailureAsync(_written.Paths.Count, () => _table.ComputeDvActionsWithEditsAsync(
            selection, _baseSnapshot, cancellationToken, written: _written), cancellationToken).ConfigureAwait(false);
        if (result.RowsDeleted == 0)
            return 0;

        StageInternal(result.Actions);
        _dvEdits.AddRange(result.Edits);
        if (!changeDataStagedByCaller)
            _uncoveredVectorDeletes.AddRange(result.Edits);
        foreach (string path in result.TouchedPaths)
            _removedPaths.Add(path);
        _operations.Add("DELETE");
        _deletedRows += result.RowsDeleted;
        return result.RowsDeleted;
    }

    private async ValueTask<long> StageCopyOnWriteRowDeletesAsync(
        RowSelection selection, bool changeDataStagedByCaller, CancellationToken cancellationToken)
    {
        EnsureOpen();
        if (selection is null)
            throw new ArgumentNullException(nameof(selection));
        _table.ValidateWritable(_baseSnapshot, isAppend: false);
        if (selection.IsEmpty)
            return 0;
        // Refused before the rewrite, so a refusal writes nothing that would have to be taken back.
        if (FirstRestagedPath(selection.Paths) is { } restaged)
            throw RestagedFile(restaged, "DELETE");

        var plan = await TakingBackOnFailureAsync(_written.Paths.Count, () => _table.ComputeCopyOnWriteDeleteActionsAsync(
            selection, _baseSnapshot, cancellationToken, rowIdStart: _nextRowId, written: _written,
            writeChangeData: !changeDataStagedByCaller), cancellationToken).ConfigureAwait(false);
        if (plan.RowsDeleted == 0)
            return 0;

        _nextRowId = plan.NextRowId;
        StageInternal(plan.Actions);
        foreach (string path in plan.RemovedPaths)
            _removedPaths.Add(path);
        _operations.Add("DELETE");
        _deletedRows += plan.RowsDeleted;
        return plan.RowsDeleted;
    }

    /// <summary>
    /// Stages a schema change computed by one of <see cref="DeltaTable"/>'s <c>Compute*</c> methods
    /// (<see cref="DeltaTable.ComputeAddColumn(Schema.StructField, MetadataAction, ProtocolAction)"/>, <see cref="DeltaTable.ComputeRenameColumn"/>, …), so an
    /// ALTER lands in the SAME version as the data written under it. Compute the change against this
    /// transaction's <see cref="Snapshot"/>; a concurrent commit that changes metadata or protocol aborts the
    /// transaction rather than silently overwriting it.
    /// </summary>
    public void StageSchemaChange(DeltaTable.DeferredSchemaChange change)
    {
        EnsureOpen();
        RefuseBaseSchemaMetadataAfterMerge(change.Actions, "StageSchemaChange");
        StageInternal(change.Actions);
        foreach (var domain in change.ReadDomains ?? [])
            _readDomains.Add(domain);
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
    /// cdc-only, so staging a change file here makes the commit write change rows for the staged appends and
    /// deletion-vector deletes whose staging call did not pass <c>changeDataStagedByCaller: true</c>. Rows for
    /// an operation staged with that flag are this call's to supply; rows for any other would appear
    /// twice.</para>
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
        DeltaTable.ValidateChangeDataRows(_baseSnapshot, rows, nameof(StageChangeDataAsync));

        var files = await TakingBackOnFailureAsync(_written.Paths.Count, () => _table.WriteChangeDataFilesForAsync(
            _baseSnapshot, rows, changeType, cancellationToken, rowIds, rowCommitVersions, written: _written),
            cancellationToken).ConfigureAwait(false);
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
        RefuseBaseSchemaMetadataAfterMerge(actions, "StageActions");
        StageInternal(actions);
        foreach (var action in actions)
        {
            if (action is AddFile { DataChange: true } or RemoveFile { DataChange: true })
                _rawDataActionsStaged = true;
        }
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

    // ── One operation per file ─────────────────────────────────────────────────────────────────────────
    //
    // Every staging call computes against the BASE snapshot, not against what earlier calls staged. Two
    // operations on one file therefore each remove the base file and each add their own version of it, and
    // both adds stay active: rows come back and survivors are duplicated (#523). Composing them would mean
    // reading through this transaction's own staged state — and rebasing that at commit — so a second
    // operation on a file is refused instead, and the caller combines its selections or predicates.

    /// <summary>The first of <paramref name="paths"/> an earlier staged operation already removed or
    /// rewrote, or null.</summary>
    private string? FirstRestagedPath(IEnumerable<string> paths)
    {
        foreach (string path in paths)
        {
            if (_removedPaths.Contains(path))
                return path;
        }
        return null;
    }

    private static InvalidOperationException RestagedFile(string path, string operation) => new(
        $"{operation} would modify '{path}', which an earlier operation staged on this transaction already "
        + "deleted from or rewrote. Each staged operation is computed against the transaction's base snapshot, "
        + "not against what earlier ones staged, so committing both would keep both versions of the file and "
        + "duplicate its rows. Combine the operations into one (a single RowSelection or predicate), or commit "
        + "the first and start a new transaction. Nothing from this call was staged.");

    /// <summary>
    /// Refuses an operation that turned out to touch a file an earlier one already staged — known only once
    /// the operation has run, since a predicate's files are found by reading. By then its deletion vectors,
    /// rewritten parquet and change files are on storage, so they are taken back before throwing: the
    /// transaction is left exactly as it was, and a caller that catches this and commits the rest leaks
    /// nothing. <paramref name="mark"/> is the ledger's count from before the operation ran.
    /// </summary>
    private async ValueTask RefuseRestagedFilesAsync(
        IEnumerable<string> paths, int mark, string operation, CancellationToken cancellationToken)
    {
        if (FirstRestagedPath(paths) is not { } restaged)
            return;

        await TakeBackSinceAsync(mark, cancellationToken).ConfigureAwait(false);
        throw RestagedFile(restaged, operation);
    }

    /// <summary>
    /// Runs one staging call's compute, which writes its data files, deletion vectors and change files as it
    /// goes. If it throws partway through (a refusal found on a later file, a predicate or updater that throws,
    /// an I/O failure, cancellation) nothing was staged that references what it already wrote, so that is
    /// taken back before the exception leaves. Otherwise a caller that catches the failure and commits the rest
    /// of the transaction would clear the ledger and leave those files orphaned. <paramref name="mark"/> is the
    /// ledger's count from before the call.
    /// </summary>
    private async ValueTask<T> TakingBackOnFailureAsync<T>(
        int mark, Func<ValueTask<T>> compute, CancellationToken cancellationToken)
    {
        try
        {
            return await compute().ConfigureAwait(false);
        }
        catch
        {
            await TakeBackSinceAsync(mark, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Deletes the files the ledger recorded since <paramref name="mark"/>, best-effort, and forgets
    /// them, leaving the ledger as it was at the mark.</summary>
    private async ValueTask TakeBackSinceAsync(int mark, CancellationToken cancellationToken)
    {
        var taken = new WrittenFileLedger();
        foreach (string path in _written.TakeRecordedSince(mark))
            taken.Record(path);
        await _table.DeleteWrittenFilesAsync(taken, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The protocol's rule for a single version: at most one <c>add</c> and at most one <c>remove</c> per
    /// reconciliation key (<c>path</c> plus deletion-vector id). The typed staging methods cannot break it
    /// now, but <see cref="StageActions"/> can, and a version that breaks it is not a recoverable state —
    /// readers keep every add, so the table reads duplicated rows from then on. Checked before the commit
    /// rather than left to a reader to discover.
    /// </summary>
    private void ValidateOneActionPerFile()
    {
        var adds = new HashSet<string>(StringComparer.Ordinal);
        var removes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var action in _dataActions)
        {
            (string Kind, string Key)? duplicate = action switch
            {
                AddFile add when !adds.Add(add.ReconciliationKey) => ("add", add.ReconciliationKey),
                RemoveFile remove when !removes.Add(remove.ReconciliationKey) => ("remove", remove.ReconciliationKey),
                _ => null,
            };
            if (duplicate is { } d)
            {
                throw new InvalidOperationException(
                    $"This transaction stages two '{d.Kind}' actions for the same file ('{d.Key}'). A version may "
                    + "carry at most one add and one remove per file and deletion vector; committing both would "
                    + "leave the table reading duplicated rows. Nothing was committed.");
            }
        }
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
    /// After an append was staged: when it advanced identity marks (the metaData action it emitted, already
    /// staged), records them for the next append and drops the metaData action an earlier append staged, which
    /// the new one supersedes (it carries the earlier marks too).
    /// </summary>
    private void ChainIdentityMarks(IEnumerable<DeltaAction> actions)
    {
        if (actions.OfType<MetadataAction>().LastOrDefault() is not { } metadata)
            return;
        if (_stagedIdentityMetadata is not null)
            _dataActions.Remove(_stagedIdentityMetadata);
        _stagedIdentityMetadata = metadata;

        _identityMarks ??= new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var field in DeltaSchemaSerializer.Parse(metadata.SchemaString).Fields)
        {
            if (IdentityColumn.GetConfig(field) is { HighWaterMark: { } mark })
                _identityMarks[field.Name] = mark;
        }
    }

    /// <summary>The base snapshot with the metaData and protocol the staged writes carry, which a write after a
    /// schema merge (or a merge after an identity-advancing write) must build on.</summary>
    private Snapshot.Snapshot PendingWriteSnapshot()
    {
        var pending = new List<DeltaAction>(2);
        if (_stagedMergeProtocol is not null)
            pending.Add(_stagedMergeProtocol);
        if (_stagedIdentityMetadata is not null)
            pending.Add(_stagedIdentityMetadata);
        return DeltaTable.WithStagedMetadata(_baseSnapshot, pending);
    }

    /// <summary>Refuses a schema merge on a transaction that already staged a metaData or protocol some other
    /// way: the merge was computed without it, and the commit can carry only one.</summary>
    private void RefuseSchemaMergeAfterOtherMetadata()
    {
        foreach (var action in _dataActions)
        {
            if ((action is MetadataAction && !ReferenceEquals(action, _stagedIdentityMetadata))
                || (action is ProtocolAction && !ReferenceEquals(action, _stagedMergeProtocol)))
            {
                throw new InvalidOperationException(
                    "mergeSchema: this transaction already stages a schema or protocol change, and a merge "
                    + "computed without it would replace it in the commit. Stage the merging write on a "
                    + "transaction of its own, or fold the change into one staged schema change.");
            }
        }
    }

    /// <summary>Refuses, after a schema merge, staging a metaData or protocol computed from the BASE schema: it
    /// would carry the schema as it was before the merge, and replace the merge in the commit.</summary>
    private void RefuseBaseSchemaMetadataAfterMerge(IEnumerable<DeltaAction> actions, string operation)
    {
        if (StagesBaseSchemaMetadataAfterMerge(actions))
            throw BaseSchemaMetadataAfterMerge(operation);
    }

    /// <summary>As <see cref="RefuseBaseSchemaMetadataAfterMerge"/>, for an operation that already wrote its
    /// files: they are taken back first, as <see cref="RefuseRestagedFilesAsync"/> does, or a caller that catches
    /// the refusal and commits would leave them orphaned.</summary>
    private async ValueTask RefuseBaseSchemaMetadataAfterMergeAsync(
        IEnumerable<DeltaAction> actions, int mark, string operation, CancellationToken cancellationToken)
    {
        if (!StagesBaseSchemaMetadataAfterMerge(actions))
            return;
        await TakeBackSinceAsync(mark, cancellationToken).ConfigureAwait(false);
        throw BaseSchemaMetadataAfterMerge(operation);
    }

    private bool StagesBaseSchemaMetadataAfterMerge(IEnumerable<DeltaAction> actions) =>
        _schemaMerged && actions.Any(a => a is MetadataAction or ProtocolAction);

    private static InvalidOperationException BaseSchemaMetadataAfterMerge(string operation) =>
        new($"{operation}: this transaction already merged the schema (a mergeSchema write), and this "
            + "operation stages a metaData or protocol of its own, computed from the schema as it was "
            + "before the merge. The commit can carry only one; stage it on a transaction of its own.");

    /// <summary>Records an append's adds as work no change file covers, for the commit's fill-in.</summary>
    private void RecordUncoveredAppends(IEnumerable<DeltaAction> actions)
    {
        foreach (var action in actions)
        {
            if (action is AddFile { DataChange: true } add)
                _uncoveredAppends.Add(add);
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
        // Before _completed: a malformed staging is the caller's to abort, not a commit that ran.
        ValidateOneActionPerFile();
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
