// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Concurrency;

/// <summary>
/// The kind of conflict a validation found, or <see cref="None"/> when the transaction may proceed.
/// </summary>
public enum ConflictType
{
    None,

    /// <summary>A concurrent commit changed the table metadata (schema, partitioning, properties).</summary>
    MetadataChanged,

    /// <summary>A concurrent commit changed the protocol (reader/writer versions or features).</summary>
    ProtocolChanged,

    /// <summary>A concurrent commit removed a file this transaction had read (concurrentDeleteRead).</summary>
    ConcurrentDeleteRead,

    /// <summary>A concurrent commit removed a file this transaction also plans to remove (delete/delete).</summary>
    ConcurrentDeleteDelete,

    /// <summary>A concurrent commit added a file matching this transaction's read predicates (concurrentAppend).</summary>
    ConcurrentAppend,

    /// <summary>
    /// A concurrent commit wrote a <c>domainMetadata</c> action for a domain this transaction also writes.
    /// </summary>
    DomainMetadataChanged,
}

/// <summary>Result of a conflict check: the type, the version that caused it, and a human-readable reason.</summary>
public sealed record ConflictResult(ConflictType Type, long ConflictingVersion, string? Message)
{
    public static readonly ConflictResult None = new(ConflictType.None, -1, null);

    public bool HasConflict => Type != ConflictType.None;

    /// <summary>
    /// The <c>DELTA_*</c> code for this verdict — the single point where the checker's closed
    /// vocabulary becomes the open one <see cref="DeltaConflictException.ErrorCode"/> carries.
    ///
    /// <para>Keeping the two separate is deliberate. <see cref="ConflictType"/> is what
    /// <see cref="ConflictChecker.Check"/> can conclude, and is closed because the rules are; the
    /// error-code namespace is flat and open, shared with conditions the checker never sees (a lost
    /// version slot, a failed row-level reconciliation) and extended independently by the table
    /// layer.</para>
    /// </summary>
    /// <exception cref="InvalidOperationException"><see cref="Type"/> is <see cref="ConflictType.None"/>,
    /// which is not a conflict and has no code.</exception>
    public string ErrorCode => Type switch
    {
        ConflictType.MetadataChanged => DeltaErrorCodes.MetadataChanged,
        ConflictType.ProtocolChanged => DeltaErrorCodes.ProtocolChanged,
        ConflictType.ConcurrentDeleteRead => DeltaErrorCodes.ConcurrentDeleteRead,
        ConflictType.ConcurrentDeleteDelete => DeltaErrorCodes.ConcurrentDeleteDelete,
        ConflictType.ConcurrentAppend => DeltaErrorCodes.ConcurrentAppend,
        ConflictType.DomainMetadataChanged => DeltaErrorCodes.DomainMetadataConflict,
        _ => throw new InvalidOperationException(
            $"{nameof(ConflictType)}.{Type} is not a conflict and has no error code; "
            + $"check {nameof(HasConflict)} first."),
    };
}

/// <summary>
/// What a transaction read, expressed so that concurrent commits can be tested against it.
///
/// <para>Two independent facets, because Delta's conflict rules use them differently:
/// <list type="bullet">
/// <item><see cref="Files"/> — the exact set of files that were read. A concurrent <i>remove</i> of any
/// of these is a conflict (the transaction's decision was based on data that is now gone).</item>
/// <item><see cref="Predicates"/> — the filters that selected what to read. A concurrent <i>add</i> that
/// could satisfy one of them is a conflict (a strict serial order might have required reading it).</item>
/// </list>
/// <see cref="WholeTable"/> is the "read everything" shortcut: every concurrent remove and every
/// concurrent add matches. A blind append (<see cref="Blind"/>) reads nothing, so only metadata,
/// protocol, and delete/delete conflicts can touch it.</para>
/// </summary>
public sealed record ReadSet
{
    /// <summary>Read predicates; a concurrent add satisfying any of them conflicts (concurrentAppend).</summary>
    public IReadOnlyList<Predicate> Predicates { get; init; } = [];

    /// <summary>Exact file paths read; a concurrent remove of any conflicts (concurrentDeleteRead).</summary>
    public ISet<string> Files { get; init; } = new HashSet<string>();

    /// <summary>The transaction read the entire table — every concurrent add and remove is relevant.</summary>
    public bool WholeTable { get; init; }

    /// <summary>
    /// A transaction with no read dependency (an INSERT with no predicate).
    ///
    /// <para><b>A fresh instance per access, deliberately, and NOT a cached singleton.</b>
    /// <see cref="Files"/> is an <see cref="ISet{T}"/> — mutable — so one shared instance would hand every
    /// blind commit in the process the same set to mutate. The realistic accident is not
    /// <c>ReadSet.Blind.Files.Add(…)</c> but the idiomatic way to derive one: this is a record, so
    /// <c>ReadSet.Blind with { WholeTable = true }</c> copies SHALLOWLY and the copy shares the original's
    /// set. Adding a path to that copy would silently give every later blind commit a read dependency it
    /// never declared, and the symptom — spurious <c>concurrentDeleteRead</c> conflicts, process-wide and
    /// nowhere near the code that caused them — is about as hard to trace back as this library gets.</para>
    ///
    /// <para><b>⚠ This changes identity, and callers must not depend on it.</b> An earlier version of
    /// this comment claimed no caller could tell the difference; that was wrong, and specifically wrong
    /// about equality. Measured:</para>
    /// <code>
    /// ReferenceEquals(ReadSet.Blind, ReadSet.Blind)   // was true, now FALSE
    /// ReadSet.Blind == ReadSet.Blind                  // was true, now FALSE
    /// </code>
    /// <para>The equality one is the surprise, because <see cref="ReadSet"/> is a record and record
    /// equality reads as value equality. It is not, here: <see cref="Files"/> is an <see cref="ISet{T}"/>
    /// whose runtime type does not override <c>Equals</c>, so the synthesized comparison falls through to
    /// reference equality on two distinct sets. <b>Treat a <see cref="ReadSet"/> as something to READ, never
    /// to compare</b> — its equality was never meaningful (two independently-built identical read sets
    /// have never compared equal), and it is now not even reflexive through this property.</para>
    ///
    /// <para>The cost is 104 measured bytes per access, and it is not paid speculatively:
    /// <see cref="Log.LogCommitRequest.Reads"/> defers this default rather than assigning it in a property
    /// initializer, so a caller that supplies its own read set constructs no blind one at all, and a
    /// caller that does not pays only if the commit actually collides and the checker asks.</para>
    /// </summary>
    public static ReadSet Blind => new();
}

/// <summary>
/// Delta optimistic-concurrency conflict detection. Given what a transaction read and what it plans to
/// write, plus the commits that landed since it started, decides whether it may still commit.
///
/// <para>This is a pure function of its inputs — no I/O, no snapshot mutation — so its verdicts can be
/// unit-tested directly against synthetic commit ranges. It is the correctness core the
/// <c>DeltaTransaction</c> commit loop runs before writing: a conflict aborts (first committer wins), no
/// conflict lets the transaction rebase onto the newer version and retry.</para>
///
/// <para>Modeled on Spark's <c>ConflictChecker</c> and the Delta protocol's concurrency section. The
/// checks, per concurrent commit, in order:</para>
/// <list type="number">
/// <item>metadata change → conflict, unconditionally.</item>
/// <item>protocol change → conflict, unconditionally.</item>
/// <item>delete/delete — the concurrent commit removed a file this transaction also plans to remove.
/// Counts removes regardless of <c>dataChange</c>: a compaction that removed the file still makes our
/// remove target a file that no longer exists.</item>
/// <item>concurrentDeleteRead — the concurrent commit made a <c>dataChange=true</c> remove of a file this
/// transaction read. <c>dataChange=false</c> removes (compaction) are exempt: they rearrange bytes
/// without changing which rows the table contains, so a read stays valid.</item>
/// <item>concurrentAppend — the concurrent commit made a <c>dataChange=true</c> add matching one of this
/// transaction's read predicates. Skipped only when the concurrent commit is itself a blind append, this
/// transaction runs at <see cref="IsolationLevel.WriteSerializable"/>, AND this transaction does not
/// itself change the metadata — see <see cref="ExamineConcurrentAdds"/> for the third term, which is the
/// one that judges us rather than the winning commit.</item>
/// <item>domainMetadata — the concurrent commit wrote a <c>domainMetadata</c> action for a domain this
/// transaction also writes. See <see cref="WrittenDomains"/> for the row-tracking exemption.</item>
/// </list>
/// </summary>
public static class ConflictChecker
{
    /// <summary>
    /// Validates a transaction against the commits that landed since it started.
    /// </summary>
    /// <param name="reads">What the transaction read. Not derivable from
    /// <paramref name="currentActions"/> — a commit does not record what it looked at — which is exactly
    /// why this stays a parameter and <c>plannedRemovePaths</c> no longer is.</param>
    /// <param name="pruner">Matches a concurrent add against the read predicates. May be null when
    /// <paramref name="reads"/> has no predicates (a blind append or whole-table read).</param>
    /// <param name="isolation">This transaction's isolation level.</param>
    /// <param name="currentActions">The actions THIS transaction is about to commit. Two things are read
    /// off it: whether the transaction changes the metadata (see <see cref="ExamineConcurrentAdds"/>), and
    /// which paths it removes, for the delete/delete check.
    /// <para>Post-rebase, if a rebase ran — it is what would actually be committed that matters, and a
    /// rebase that remapped a deletion vector onto a concurrent one changed which paths those are.</para></param>
    /// <param name="concurrent">The commits in <c>(readVersion, latestVersion]</c>, ascending.</param>
    /// <param name="rowLevelResolvedPaths">Paths whose concurrent remove/re-add has been reconciled at
    /// row granularity by deletion-vector union (Databricks row-level concurrency) before this check.
    /// A concurrent <c>RemoveFile</c> or <c>AddFile</c> of such a path is ignored here: the delete's DV
    /// was rebased onto the concurrent one, so it neither conflicts (delete/delete, concurrentDeleteRead)
    /// nor counts as a foreign add (concurrentAppend). May be null when no row-level resolution ran.</param>
    public static ConflictResult Check(
        ReadSet reads,
        DeltaFilePruner? pruner,
        IsolationLevel isolation,
        IReadOnlyList<DeltaAction> currentActions,
        IReadOnlyList<(long Version, IReadOnlyList<DeltaAction> Actions)> concurrent,
        ISet<string>? rowLevelResolvedPaths = null)
    {
        // All hoisted: properties of THIS transaction, identical for every concurrent commit examined.
        bool currentChangesMetadata = ChangesMetadata(currentActions);
        var plannedRemovePaths = RemovedPaths(currentActions);
        var writtenDomains = WrittenDomains(currentActions);

        foreach (var (version, actions) in concurrent)
        {
            // 1, 2 & 6 — a concurrent metadata or protocol change conflicts unconditionally; a concurrent
            // domainMetadata conflicts when it names a domain this transaction also writes.
            foreach (var action in actions)
            {
                if (action is MetadataAction)
                    return new ConflictResult(ConflictType.MetadataChanged, version,
                        $"Concurrent commit {version} changed the table metadata.");
                if (action is ProtocolAction)
                    return new ConflictResult(ConflictType.ProtocolChanged, version,
                        $"Concurrent commit {version} changed the protocol.");
                if (action is DomainMetadata concurrentDomain
                    && writtenDomains?.Contains(concurrentDomain.Domain) == true)
                {
                    return new ConflictResult(ConflictType.DomainMetadataChanged, version,
                        $"Concurrent commit {version} wrote the metadata domain "
                        + $"'{concurrentDomain.Domain}', which this transaction also writes.");
                }
            }

            bool examineAdds = ExamineConcurrentAdds(
                isolation, IsBlindAppend(actions), currentChangesMetadata);

            foreach (var action in actions)
            {
                switch (action)
                {
                    case RemoveFile remove:
                        // A file whose concurrent remove was reconciled at row granularity (DV union) is
                        // no longer a conflict source: this transaction's delete rebased its own DV onto it.
                        if (rowLevelResolvedPaths is not null && rowLevelResolvedPaths.Contains(remove.Path))
                            break;

                        // 3 — delete/delete. A removed file is removed whatever its dataChange flag.
                        if (plannedRemovePaths?.Contains(remove.Path) == true)
                            return new ConflictResult(ConflictType.ConcurrentDeleteDelete, version,
                                $"Concurrent commit {version} already removed '{remove.Path}', "
                                + "which this transaction also removes.");

                        // 4 — concurrentDeleteRead. Only data-changing removes invalidate a read.
                        if (remove.DataChange && WasRead(reads, remove.Path))
                            return new ConflictResult(ConflictType.ConcurrentDeleteRead, version,
                                $"Concurrent commit {version} removed '{remove.Path}', "
                                + "which this transaction read.");
                        break;

                    case AddFile add:
                        // The re-add of a row-level-resolved file (the concurrent delete's own AddFile with
                        // its new DV) is not a foreign append — the union already accounts for it.
                        if (rowLevelResolvedPaths is not null && rowLevelResolvedPaths.Contains(add.Path))
                            break;

                        // 5 — concurrentAppend. Only data-changing adds, and only when the isolation level
                        // says a concurrent append of this shape is visible to us.
                        if (examineAdds && add.DataChange && Matches(reads, pruner, add))
                            return new ConflictResult(ConflictType.ConcurrentAppend, version,
                                $"Concurrent commit {version} added '{add.Path}', "
                                + "which matches this transaction's read predicates.");
                        break;
                }
            }
        }

        return ConflictResult.None;
    }

    /// <summary>
    /// Whether a concurrent commit's <c>dataChange</c> adds have to be tested against our read predicates,
    /// or may be skipped because the concurrent commit was a blind append.
    /// </summary>
    /// <remarks>
    /// <para>Delta's gate, which has three terms and not two:</para>
    /// <code>
    /// val addedFilesToCheckForConflicts = isolationLevel match {
    ///   case WriteSerializable if !currentTransactionInfo.metadataChanged =>
    ///     winningCommitSummary.changedDataAddedFiles
    ///   case Serializable | WriteSerializable =>
    ///     winningCommitSummary.changedDataAddedFiles ++ winningCommitSummary.blindAppendAddedFiles
    ///   case SnapshotIsolation =>
    ///     Seq.empty
    /// }
    /// </code>
    /// <para>The third term is about the CURRENT transaction, not the concurrent one, which is what makes
    /// it easy to miss: everything else here judges the winning commit. Under
    /// <see cref="IsolationLevel.WriteSerializable"/> a transaction that itself changes the metadata falls
    /// through to the <c>Serializable</c> branch and examines blind appends too. The justification for
    /// exempting a blind append is that it cannot have depended on anything we did; a schema change is not
    /// local to the files we read, so an append written against the OLD schema is not necessarily still
    /// valid under the new one, and the exemption stops being safe to grant.</para>
    /// <para><b>Metadata only, not protocol.</b> Delta's <c>metadataChanged</c> is
    /// <c>newMetadata.nonEmpty</c>, assigned by a loop whose only case is <c>case m: Metadata</c>; a
    /// <c>Protocol</c> action never sets it and no separate protocol term feeds this gate (checked against
    /// the <c>v4.0.0</c> tag). Including protocol here would be STRICTER than Delta — a transaction that
    /// only enables a table feature would start conflicting with concurrent appends where Delta does not —
    /// and being gratuitously strict about concurrency is its own defect, not a safe direction to err in.
    /// </para>
    /// <para><c>SnapshotIsolation</c> has no counterpart in <see cref="IsolationLevel"/>, which is why the
    /// Scala reads as three cases and this reads as two. Adding the level is a separate question.</para>
    /// </remarks>
    internal static bool ExamineConcurrentAdds(
        IsolationLevel isolation, bool concurrentIsBlindAppend, bool currentChangesMetadata) =>
        isolation == IsolationLevel.Serializable
        || currentChangesMetadata
        || !concurrentIsBlindAppend;

    /// <summary>
    /// The paths a set of actions removes, for the delete/delete check.
    /// </summary>
    /// <remarks>
    /// <para>Used to be a caller-supplied <c>plannedRemovePaths</c> parameter, restating what the actions
    /// already said. Both call sites wrote the duplication out longhand —
    /// <c>Actions = [Remove("doomed.parquet")], PlannedRemovePaths = { "doomed.parquet" }</c> — and the
    /// parameter came with a documented way to get it wrong: naming a merely-READ path there reported a
    /// concurrent delete of it as delete/delete rather than the concurrentDeleteRead it actually is. A
    /// derived set cannot be wrong in that way, because a read path is not a <see cref="RemoveFile"/>.</para>
    /// <para>Same test as <see cref="ChangesMetadata"/>: derive what the actions record, require a
    /// declaration for what they do not. Reads are the second kind, which is why <see cref="ReadSet"/> is
    /// still passed in.</para>
    /// </remarks>
    /// <remarks>
    /// Null — rather than a shared empty set — for the common commit that removes nothing. See
    /// <see cref="WrittenDomains"/> for why the shared empty went away.
    /// </remarks>
    private static HashSet<string>? RemovedPaths(IReadOnlyList<DeltaAction> actions)
    {
        HashSet<string>? paths = null;
        foreach (var action in actions)
        {
            if (action is RemoveFile remove)
                (paths ??= new HashSet<string>(StringComparer.Ordinal)).Add(remove.Path);
        }

        return paths;
    }

    /// <summary>
    /// The metadata domains a set of actions writes, for the domainMetadata check — MINUS the row-tracking
    /// high-water mark, which is reconciled rather than contested.
    /// </summary>
    /// <remarks>
    /// <para>Delta's rule, from <c>ConflictChecker.checkIfDomainMetadataConflict</c> (source-verified
    /// against the <c>delta-spark_4.2_2.13-4.4.0</c> bytecode): for each <c>DomainMetadata</c> the current
    /// transaction writes, look the domain up in the winning commit's domain map — absent, keep it; the
    /// row-tracking domain, keep it; otherwise throw <c>ConcurrentTransactionException("A conflicting
    /// metadata domain &lt;domain&gt; is added.")</c>.</para>
    /// <para><b>Why the row-tracking exemption is load-bearing rather than a nicety.</b> Every commit that
    /// adds files to a row-tracking table advances the <c>delta.rowTracking</c> high-water mark, so without
    /// it two ordinary concurrent appends would conflict on a domain neither writer ever named — turning
    /// row tracking on would cost a table its concurrency. The mark is not contested state: a rebase
    /// re-derives it from the version that landed (see the table layer's rebase handlers), which is
    /// precisely why it can be reconciled where a user domain cannot.</para>
    /// <para><b>Derived, not declared</b> — the same call as <see cref="ChangesMetadata"/> and
    /// <see cref="RemovedPaths"/>: what a commit writes is fully visible in the actions about to be
    /// written, so there is nothing only the writer could know.</para>
    /// <para><b>Delta additionally gates the whole check</b> on the protocol supporting the
    /// <c>domainMetadata</c> feature — <c>checkIfDomainMetadataConflict</c> returns immediately when it is
    /// absent. Not reproduced here, for the plain reason that this is a pure function with no protocol in
    /// hand, and erring STRICT is the safe direction for a concurrency check.</para>
    /// <para>It is not purely theoretical, though, and worth naming rather than glossing: through Delta the
    /// case cannot arise (it refuses to write <c>domainMetadata</c> to a table lacking the feature at all),
    /// but <c>DeltaTable.SetDomainMetadataAsync</c> does not declare the feature before writing one, so a
    /// table this library wrote can reach here with domain actions and no feature. That is a gap in that
    /// method rather than in this rule, and this rule's answer for it — conflict — is the conservative
    /// one.</para>
    /// <para><b>Returns null rather than a shared empty set</b>, as does <see cref="RemovedPaths"/>.
    /// Both used to hand back a <c>static readonly ISet&lt;string&gt;</c> empty — one process-wide instance,
    /// behind a MUTABLE interface, returned to a caller. Nothing mutates it today, and the allocation it
    /// saved was on a path that only runs when a commit collides, so it was buying very little and
    /// risking a shared-state corruption that would surface as spurious conflicts far from its cause.
    /// Null costs nothing and cannot be mutated.</para>
    /// </remarks>
    private static HashSet<string>? WrittenDomains(IReadOnlyList<DeltaAction> actions)
    {
        HashSet<string>? domains = null;
        foreach (var action in actions)
        {
            if (action is DomainMetadata domain
                && !string.Equals(
                    domain.Domain, RowTracking.RowTrackingConfig.DomainName, StringComparison.Ordinal))
            {
                (domains ??= new HashSet<string>(StringComparer.Ordinal)).Add(domain.Domain);
            }
        }

        return domains;
    }

    /// <summary>
    /// Whether a set of actions changes the table metadata — Delta's <c>currentTransactionInfo</c>
    /// <c>metadataChanged</c>, for <see cref="ExamineConcurrentAdds"/>.
    /// </summary>
    /// <remarks>
    /// Derived from the actions rather than declared by the caller, and unlike <c>isBlindAppend</c> that
    /// is the right call: whether a commit carries a <see cref="MetadataAction"/> is fully visible in what
    /// is about to be written, so there is nothing only the writer could know and no defaulted value to
    /// get wrong. Blind-append is the opposite — a property of the transaction's READS, which the actions
    /// do not record — which is why that one has to be declared.
    /// </remarks>
    internal static bool ChangesMetadata(IReadOnlyList<DeltaAction> actions)
    {
        foreach (var action in actions)
        {
            if (action is MetadataAction)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Whether a concurrent commit was a blind append — i.e. whether the transaction that produced it READ
    /// nothing, which is what makes it safe to linearize after ours under WriteSerializable.
    /// </summary>
    /// <remarks>
    /// <para>Blind-append is a property of the WRITER's transaction, not of the actions it emitted, so the
    /// writer is the only party that actually knows it: Delta defines it as
    /// <c>readPredicates.isEmpty &amp;&amp; readFiles.isEmpty</c> and records the answer in
    /// <c>commitInfo.isBlindAppend</c>. When the flag is there we must BELIEVE it rather than re-derive it.</para>
    /// <para><b>Why the inference below cannot be the primary answer.</b> Deriving "blind" from the action
    /// shape ("only adds") errs in the UNSAFE direction: an <c>INSERT INTO t SELECT ... FROM t</c> — the
    /// standard incremental/dedupe anti-join, i.e. the common case, not an exotic one — emits nothing but
    /// adds and plainly read the table. Inferring blind there makes us SKIP a concurrentAppend check we owe,
    /// and the conflict we were supposed to raise silently does not happen.</para>
    /// <para>Delta itself does NOT make this inference: it reads <c>isBlindAppendOption.getOrElse(false)</c>,
    /// and it computes an equivalent <c>onlyAddFiles</c> and pointedly does not use it here (checked at the
    /// <c>v4.2.0</c> tag).</para>
    /// <para>The two directions are deliberately NOT symmetric, and this is where we depart from Delta: an
    /// ABSENT flag falls back to the inference rather than to "not blind". A PRESENT flag outranks anything
    /// we could infer — including a <c>false</c> on an adds-only commit, which is exactly the read-then-append
    /// case above.</para>
    /// <para><b>Why absent still infers, now that this library DOES emit the flag.</b> Two populations of
    /// unflagged commit remain and neither is going away. Every commit written before that landed, on every
    /// existing table — <c>getOrElse(false)</c> would make ordinary appends among them start conflicting.
    /// And delta-rs writes no flag at all: <c>is_blind_append</c> is an <c>Option&lt;bool&gt;</c> with
    /// <c>skip_serializing_if = "Option::is_none"</c> that nothing computes (checked 2026-08-11, and
    /// observed on every commit shape it writes), so on a table delta-rs maintains the inference is not a
    /// fallback, it is the whole answer. That is why <see cref="InferBlindAppend"/> is worth improving on
    /// its own terms rather than treated as a legacy path.</para>
    /// </remarks>
    internal static bool IsBlindAppend(IReadOnlyList<DeltaAction> actions)
    {
        foreach (var action in actions)
        {
            if (action is not CommitInfo info)
                continue;
            if (info.GetValue("isBlindAppend") is not { } flag)
                continue;
            // Only an actual boolean is a statement; anything else is a malformed field, and a hint we
            // cannot read is no better than one that is absent — fall through to the inference.
            if (flag.ValueKind is JsonValueKind.True or JsonValueKind.False)
                return flag.GetBoolean();
        }

        return InferBlindAppend(actions);
    }

    /// <summary>
    /// Fallback for a commit whose writer did not declare <c>isBlindAppend</c>: assume a commit that only
    /// adds files did not read anything. At least one add, and no remove, cdc, metadata, or protocol action.
    /// </summary>
    /// <remarks>
    /// <para><b>Why <c>cdc</c> disqualifies.</b> A change-data file records row-level changes, which only a
    /// statement that located those rows can produce — an UPDATE, a DELETE, a MERGE. The rest of this
    /// inference reads the ABSENCE of evidence ("nothing here says it read"); a cdc action is the presence
    /// of it, and the only positive evidence available at all.</para>
    /// <para>It is corrective rather than defensive, because the case it catches is one an engine actually
    /// writes. Measured against delta-rs 1.6.2 on a table with <c>delta.enableChangeDataFeed</c>: an
    /// insert-only MERGE commits <c>add</c> + <c>cdc</c> and NO remove — it scanned the target to decide
    /// what was missing, so it plainly read, and every clause above it sees only adds and calls it blind.
    /// The same shape Spark records <c>isBlindAppend=false</c> for (<c>BlindAppendGroundTruthTests</c>), and
    /// the same shape #88 identified as the one the inference misjudges. UPDATE, DELETE and matched MERGE
    /// all carry removes as well, so this changes no verdict for them.</para>
    /// <para>It matters because delta-rs declares no flag on ANY commit — measured, not inferred: not on
    /// create, append, UPDATE, DELETE, or MERGE — so on a table delta-rs maintains this inference is not a
    /// fallback, it is the whole answer, and an insert-only MERGE there is a concurrent-append check we
    /// would otherwise skip. Pinned by <c>DeltaRsBlindAppendGroundTruthTests</c>.</para>
    /// </remarks>
    private static bool InferBlindAppend(IReadOnlyList<DeltaAction> actions)
    {
        bool hasAdd = false;
        foreach (var action in actions)
        {
            switch (action)
            {
                case AddFile:
                    hasAdd = true;
                    break;
                case RemoveFile:
                case CdcFile:
                case MetadataAction:
                case ProtocolAction:
                    return false;
            }
        }

        return hasAdd;
    }

    private static bool WasRead(ReadSet reads, string path) =>
        reads.WholeTable || reads.Files.Contains(path);

    private static bool Matches(ReadSet reads, DeltaFilePruner? pruner, AddFile add)
    {
        if (reads.WholeTable)
            return true;

        if (reads.Predicates.Count == 0)
            return false;

        // A null pruner with predicates present is a caller error; treat it conservatively as "matches"
        // so a checker that cannot prune never silently passes a real conflict.
        if (pruner is null)
            return true;

        foreach (var predicate in reads.Predicates)
        {
            if (pruner.ShouldInclude(add, predicate))
                return true;
        }

        return false;
    }
}
