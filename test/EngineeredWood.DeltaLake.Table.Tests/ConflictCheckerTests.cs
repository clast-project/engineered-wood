// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.DeltaLake.Table.Concurrency;
using EngineeredWood.Expressions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Verdict tests for <see cref="ConflictChecker"/> — the optimistic-concurrency core. Each pins one of
/// the rules Delta's <c>ConflictChecker</c> applies when a transaction tries to commit against commits
/// that landed since it started. Pure input→verdict, no table or I/O, so they run instantly and isolate
/// the decision from the transaction plumbing that will drive it.
///
/// <para>These are the seven cases parked in <see cref="PendingCoverageTests"/> under "Logical rebase /
/// ConflictChecker parity".</para>
/// </summary>
public class ConflictCheckerTests
{
    // A one-column table (id: long) is enough for every predicate here.
    private static readonly StructType Schema = new()
    {
        Fields =
        [
            new StructField { Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = false },
        ],
    };

    private static DeltaFilePruner Pruner() => new(Schema, partitionColumns: []);

    /// <summary>An AddFile carrying id-range stats, so the pruner can decide whether a predicate matches.</summary>
    private static AddFile Add(string path, long minId, long maxId, bool dataChange = true) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        ModificationTime = 0,
        DataChange = dataChange,
        Stats = $"{{\"numRecords\":1,\"minValues\":{{\"id\":{minId}}},"
              + $"\"maxValues\":{{\"id\":{maxId}}},\"nullCount\":{{\"id\":0}}}}",
    };

    private static RemoveFile Remove(string path, bool dataChange = true) => new()
    {
        Path = path,
        DataChange = dataChange,
        DeletionTimestamp = 0,
    };

    private static (long, IReadOnlyList<DeltaAction>) Commit(long version, params DeltaAction[] actions) =>
        (version, actions);

    private static ConflictResult Check(
        ReadSet reads,
        ISet<string> plannedRemoves,
        IsolationLevel isolation,
        params (long, IReadOnlyList<DeltaAction>)[] concurrent) =>
        ConflictChecker.Check(reads, plannedRemoves, Pruner(), isolation, concurrent);

    private static readonly ISet<string> NoRemoves = new HashSet<string>();

    // ── concurrentAppend + isolation: the blind-append cases ──

    /// <summary>A concurrent blind append whose file matches our read predicate conflicts under Serializable.</summary>
    [Fact]
    public void BlindAppend_MatchingReads_Conflicts_Serializable()
    {
        // We read "id = 5"; a concurrent blind append adds a file whose id range covers 5.
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6, Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, NoRemoves, IsolationLevel.Serializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentAppend, result.Type);
        Assert.Equal(6, result.ConflictingVersion);
    }

    /// <summary>...but the identical situation passes under WriteSerializable — the whole distinction.</summary>
    [Fact]
    public void BlindAppend_MatchingReads_Passes_WriteSerializable()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6, Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, NoRemoves, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>A concurrent add whose stats prove it cannot match our predicate passes even under Serializable.</summary>
    [Fact]
    public void BlindAppend_NonMatchingPredicate_Passes_Serializable()
    {
        // We read "id = 5"; the added file's id range is 100..200, which the pruner rules out.
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6, Add("part-far.parquet", minId: 100, maxId: 200));

        var result = Check(reads, NoRemoves, IsolationLevel.Serializable, concurrent);

        Assert.False(result.HasConflict);
    }

    // ── concurrentDeleteRead + delete/delete ──

    /// <summary>A concurrent data-changing remove of a file we read conflicts (concurrentDeleteRead).</summary>
    [Fact]
    public void ConcurrentDeleteOfReadFile_Conflicts()
    {
        var reads = new ReadSet { Files = new HashSet<string> { "part-read.parquet" } };
        var concurrent = Commit(6, Remove("part-read.parquet"));

        var result = Check(reads, NoRemoves, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentDeleteRead, result.Type);
        Assert.Equal(6, result.ConflictingVersion);
    }

    /// <summary>Two transactions removing the same file conflict (delete/delete).</summary>
    [Fact]
    public void DeleteDelete_SameFile_Conflicts()
    {
        var plannedRemoves = new HashSet<string> { "part-target.parquet" };
        var concurrent = Commit(6, Remove("part-target.parquet"));

        var result = Check(ReadSet.Blind, plannedRemoves, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentDeleteDelete, result.Type);
    }

    // ── metadata + compaction exemption ──

    /// <summary>A concurrent metadata change conflicts unconditionally.</summary>
    [Fact]
    public void ConcurrentMetadataChange_Conflicts()
    {
        var metadata = new MetadataAction
        {
            Id = "t",
            Format = Format.Parquet,
            SchemaString = "{}",
            PartitionColumns = [],
        };

        // Even a transaction that read nothing (a blind append) conflicts with a metadata change.
        var result = Check(ReadSet.Blind, NoRemoves, IsolationLevel.WriteSerializable, Commit(6, metadata));

        Assert.Equal(ConflictType.MetadataChanged, result.Type);
    }

    /// <summary>
    /// A dataChange=false commit (compaction) is exempt from the read checks: it rearranges files
    /// without changing rows, so a file we read being compacted away does not invalidate our read.
    /// </summary>
    [Fact]
    public void Compaction_ExemptFromReadChecks()
    {
        // We read part-a; a concurrent compaction removes it (dataChange=false) and adds a compacted
        // file (dataChange=false). Neither the remove nor the add may count against us.
        var reads = new ReadSet
        {
            Files = new HashSet<string> { "part-a.parquet" },
            Predicates = [Ex.Equal("id", LiteralValue.Of(5L))],
        };
        var concurrent = Commit(6,
            Remove("part-a.parquet", dataChange: false),
            Add("part-compacted.parquet", minId: 1, maxId: 10, dataChange: false));

        var result = Check(reads, NoRemoves, IsolationLevel.Serializable, concurrent);

        Assert.False(result.HasConflict);
    }

    // ── a couple of guards beyond the seven parked cases ──

    /// <summary>No concurrent commits ⇒ nothing to conflict with.</summary>
    [Fact]
    public void NoConcurrentCommits_Passes()
    {
        var reads = new ReadSet { WholeTable = true };
        var result = Check(reads, NoRemoves, IsolationLevel.Serializable);
        Assert.False(result.HasConflict);
    }

    /// <summary>The first conflicting version is reported, not a later one.</summary>
    [Fact]
    public void EarliestConflictingVersion_IsReported()
    {
        var plannedRemoves = new HashSet<string> { "part-target.parquet" };
        var result = Check(ReadSet.Blind, plannedRemoves, IsolationLevel.WriteSerializable,
            Commit(6, Add("part-x.parquet", 1, 10)),
            Commit(7, Remove("part-target.parquet")),
            Commit(8, Remove("part-target.parquet")));

        Assert.Equal(ConflictType.ConcurrentDeleteDelete, result.Type);
        Assert.Equal(7, result.ConflictingVersion);
    }
}
