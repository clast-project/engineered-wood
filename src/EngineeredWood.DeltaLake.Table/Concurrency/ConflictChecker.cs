// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Table.Concurrency;

/// <summary>
/// The kind of conflict a validation found, or <see cref="None"/> when the transaction may proceed.
/// </summary>
internal enum ConflictType
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
}

/// <summary>Result of a conflict check: the type, the version that caused it, and a human-readable reason.</summary>
internal sealed record ConflictResult(ConflictType Type, long ConflictingVersion, string? Message)
{
    public static readonly ConflictResult None = new(ConflictType.None, -1, null);

    public bool HasConflict => Type != ConflictType.None;
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
internal sealed record ReadSet
{
    /// <summary>Read predicates; a concurrent add satisfying any of them conflicts (concurrentAppend).</summary>
    public IReadOnlyList<Predicate> Predicates { get; init; } = [];

    /// <summary>Exact file paths read; a concurrent remove of any conflicts (concurrentDeleteRead).</summary>
    public ISet<string> Files { get; init; } = new HashSet<string>();

    /// <summary>The transaction read the entire table — every concurrent add and remove is relevant.</summary>
    public bool WholeTable { get; init; }

    /// <summary>A transaction with no read dependency (an INSERT with no predicate).</summary>
    public static ReadSet Blind { get; } = new();
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
/// transaction's read predicates. Skipped only when the concurrent commit is itself a blind append and
/// this transaction runs at <see cref="IsolationLevel.WriteSerializable"/>.</item>
/// </list>
/// </summary>
internal static class ConflictChecker
{
    /// <summary>
    /// Validates a transaction against the commits that landed since it started.
    /// </summary>
    /// <param name="reads">What the transaction read.</param>
    /// <param name="plannedRemovePaths">Paths the transaction plans to remove (for delete/delete).</param>
    /// <param name="pruner">Matches a concurrent add against the read predicates. May be null when
    /// <paramref name="reads"/> has no predicates (a blind append or whole-table read).</param>
    /// <param name="isolation">This transaction's isolation level.</param>
    /// <param name="concurrent">The commits in <c>(readVersion, latestVersion]</c>, ascending.</param>
    /// <param name="rowLevelResolvedPaths">Paths whose concurrent remove/re-add has been reconciled at
    /// row granularity by deletion-vector union (Databricks row-level concurrency) before this check.
    /// A concurrent <c>RemoveFile</c> or <c>AddFile</c> of such a path is ignored here: the delete's DV
    /// was rebased onto the concurrent one, so it neither conflicts (delete/delete, concurrentDeleteRead)
    /// nor counts as a foreign add (concurrentAppend). May be null when no row-level resolution ran.</param>
    public static ConflictResult Check(
        ReadSet reads,
        ISet<string> plannedRemovePaths,
        DeltaFilePruner? pruner,
        IsolationLevel isolation,
        IReadOnlyList<(long Version, IReadOnlyList<DeltaAction> Actions)> concurrent,
        ISet<string>? rowLevelResolvedPaths = null)
    {
        foreach (var (version, actions) in concurrent)
        {
            // 1 & 2 — a concurrent metadata or protocol change conflicts unconditionally.
            foreach (var action in actions)
            {
                if (action is MetadataAction)
                    return new ConflictResult(ConflictType.MetadataChanged, version,
                        $"Concurrent commit {version} changed the table metadata.");
                if (action is ProtocolAction)
                    return new ConflictResult(ConflictType.ProtocolChanged, version,
                        $"Concurrent commit {version} changed the protocol.");
            }

            bool concurrentIsBlindAppend = IsBlindAppend(actions);

            // Whether a concurrent add can conflict with our reads depends on the isolation level: a
            // blind append is exempt under WriteSerializable, examined under Serializable.
            bool examineAdds = isolation == IsolationLevel.Serializable || !concurrentIsBlindAppend;

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
                        if (plannedRemovePaths.Contains(remove.Path))
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
    /// A commit is a blind append when it only adds files: at least one add, and no remove, metadata, or
    /// protocol action. That is the reader-side inference the protocol relies on — such a commit cannot
    /// have depended on a read, so it is safe to linearize after a concurrent transaction.
    /// </summary>
    private static bool IsBlindAppend(IReadOnlyList<DeltaAction> actions)
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
