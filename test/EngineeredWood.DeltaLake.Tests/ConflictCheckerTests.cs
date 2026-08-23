// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using EngineeredWood.Expressions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Verdict tests for <see cref="ConflictChecker"/> — the optimistic-concurrency core. Each pins one of
/// the rules Delta's <c>ConflictChecker</c> applies when a transaction tries to commit against commits
/// that landed since it started. Pure input→verdict, no table or I/O, so they run instantly and isolate
/// the decision from the transaction plumbing that will drive it.
///
/// <para>These are the seven logical-rebase / ConflictChecker-parity cases.</para>
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

    /// <summary>A change-data file, as a DML commit on a CDF-enabled table carries alongside its adds.</summary>
    private static CdcFile Cdc(string path) => new()
    {
        Path = path,
        PartitionValues = new Dictionary<string, string>(),
        Size = 100,
        DataChange = false, // per the protocol: a cdc file is not itself a data change
    };

    private static (long, IReadOnlyList<DeltaAction>) Commit(long version, params DeltaAction[] actions) =>
        (version, actions);

    /// <summary>For a transaction whose own actions do not bear on the verdict.</summary>
    private static ConflictResult Check(
        ReadSet reads,
        IsolationLevel isolation,
        params (long, IReadOnlyList<DeltaAction>)[] concurrent) =>
        ConflictChecker.Check(reads, Pruner(), isolation, NoCurrentActions, concurrent);

    /// <summary>
    /// As <see cref="Check"/>, but states what THIS transaction is committing. Two things are read off it —
    /// whether we change the metadata, and which paths we remove — so the delete/delete and metadata-gate
    /// tests go through this one and the rest through the overload above.
    /// </summary>
    private static ConflictResult CheckCommitting(
        IReadOnlyList<DeltaAction> currentActions,
        ReadSet reads,
        IsolationLevel isolation,
        params (long, IReadOnlyList<DeltaAction>)[] concurrent) =>
        ConflictChecker.Check(reads, Pruner(), isolation, currentActions, concurrent);

    /// <summary>A transaction committing nothing that bears on the verdict.</summary>
    private static readonly IReadOnlyList<DeltaAction> NoCurrentActions = [];

    private static MetadataAction Metadata(string schema = "{}") => new()
    {
        Id = "t",
        Format = Format.Parquet,
        SchemaString = schema,
        PartitionColumns = [],
    };

    /// <summary>A commitInfo action from raw JSON. Values are cloned so they outlive the parsed document.</summary>
    private static CommitInfo Info(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return new CommitInfo
        {
            Values = doc.RootElement.EnumerateObject()
                .ToDictionary(p => p.Name, p => p.Value.Clone()),
        };
    }

    // ── concurrentAppend + isolation: the blind-append cases ──

    /// <summary>A concurrent blind append whose file matches our read predicate conflicts under Serializable.</summary>
    [Fact]
    public void BlindAppend_MatchingReads_Conflicts_Serializable()
    {
        // We read "id = 5"; a concurrent blind append adds a file whose id range covers 5.
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6, Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.Serializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentAppend, result.Type);
        Assert.Equal(6, result.ConflictingVersion);
    }

    /// <summary>...but the identical situation passes under WriteSerializable — the whole distinction.</summary>
    [Fact]
    public void BlindAppend_MatchingReads_Passes_WriteSerializable()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6, Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>A concurrent add whose stats prove it cannot match our predicate passes even under Serializable.</summary>
    [Fact]
    public void BlindAppend_NonMatchingPredicate_Passes_Serializable()
    {
        // We read "id = 5"; the added file's id range is 100..200, which the pruner rules out.
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6, Add("part-far.parquet", minId: 100, maxId: 200));

        var result = Check(reads, IsolationLevel.Serializable, concurrent);

        Assert.False(result.HasConflict);
    }

    // ── the DECLARED flag: commitInfo.isBlindAppend outranks the inference above ──
    //
    // Blind-append is a property of the WRITER's transaction, not of the actions it emitted, so only the
    // writer knows it. The three tests above pin the INFERENCE; these pin that a declaration wins, in both
    // directions, and that an absent or unreadable one still falls back.

    /// <summary>
    /// THE UNSAFE DIRECTION, and the reason this group exists. A commit that contains only adds but whose
    /// writer says it was NOT blind — the shape an <c>INSERT INTO t SELECT ... FROM t</c> produces, i.e. the
    /// standard incremental/dedupe anti-join — must be examined even under WriteSerializable. Inference
    /// alone calls it blind and skips a check that is owed, so this case passed before the declaration was
    /// consulted.
    /// </summary>
    [Fact]
    public void DeclaredNotBlind_OnAddsOnlyCommit_Conflicts_WriteSerializable()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":false}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentAppend, result.Type);
        Assert.Equal(6, result.ConflictingVersion);
    }

    /// <summary>A declared blind append behaves exactly as the inferred one did — the common case is unchanged.</summary>
    [Fact]
    public void DeclaredBlind_Passes_WriteSerializable()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// The declaration also outranks the inference in the PERMISSIVE direction: a remove in the commit makes
    /// the inference say "not blind", but the writer's own true wins and the matching add stays exempt. (The
    /// remove here is of a file we neither read nor plan to remove, so it raises no conflict of its own —
    /// otherwise this would be testing the remove rules instead.)
    /// </summary>
    [Fact]
    public void DeclaredBlind_OutranksInference_WhenCommitAlsoRemoves()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Remove("part-unrelated.parquet"),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>Serializable examines every add, so a declared blind append conflicts there regardless.</summary>
    [Fact]
    public void DeclaredBlind_StillConflicts_Serializable()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.Serializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentAppend, result.Type);
    }

    /// <summary>
    /// A commitInfo with no isBlindAppend — the overwhelmingly common case, since most writers (this library
    /// included, so far) never emit it — must still fall back to the inference rather than default to
    /// "not blind" and start conflicting on every concurrent append.
    /// </summary>
    [Fact]
    public void AbsentFlag_FallsBackToInference()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"operation":"WRITE","engineInfo":"some-other-engine"}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict); // inferred blind: adds only
    }

    /// <summary>A non-boolean flag is malformed, and an unreadable declaration is no better than an absent one.</summary>
    [Fact]
    public void MalformedFlag_FallsBackToInference()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":"yes"}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict); // inferred blind: adds only
    }

    // ── the gate's third term: OUR OWN transaction changing metadata ──

    // Everything else here judges the winning commit. This one term judges us, which is what makes it
    // easy to leave out: Delta's `case WriteSerializable if !currentTransactionInfo.metadataChanged`.

    /// <summary>
    /// ⚠ THE CASE #126 IS ABOUT. A transaction that itself changes the schema loses the blind-append
    /// exemption: under WriteSerializable it falls through to the Serializable branch and examines
    /// concurrent blind appends too. The exemption's justification is that a blind append cannot have
    /// depended on anything we did — but a schema change is not local to the files we read, and an append
    /// written against the OLD schema need not still be valid under the new one.
    /// </summary>
    [Fact]
    public void OwnMetadataChange_WithdrawsTheBlindAppendExemption()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""), // genuinely blind, and believed
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = CheckCommitting(
            [Metadata("""{"type":"struct","fields":[]}""")],
            reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentAppend, result.Type);
    }

    /// <summary>
    /// The control: the SAME transaction and the SAME concurrent commit, minus our metadata change, stays
    /// exempt. Without it the test above would pass equally well if the exemption had been dropped
    /// altogether — which would be a far more expensive change than the one intended.
    /// </summary>
    [Fact]
    public void WithoutOwnMetadataChange_TheExemptionHolds()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = CheckCommitting(
            [Add("ours.parquet", minId: 100, maxId: 200)],
            reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// A PROTOCOL change of our own does NOT withdraw the exemption, because Delta's gate does not read
    /// one: <c>metadataChanged</c> is <c>newMetadata.nonEmpty</c>, assigned by a loop whose only case is
    /// <c>case m: Metadata</c> (checked at the <c>v4.0.0</c> tag).
    ///
    /// <para>Worth a test rather than a comment: the issue proposing this fix suggested deriving the term
    /// from "a MetadataAction or ProtocolAction among them", which would be STRICTER than Delta — a
    /// transaction that only enables a table feature would start conflicting with concurrent appends
    /// where Delta lets it through. Erring strict is still erring.</para>
    /// </summary>
    [Fact]
    public void OwnProtocolChange_DoesNotWithdrawTheExemption()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = CheckCommitting(
            [new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7 }],
            reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// The term is about OUR transaction, not the concurrent one, and those are different rules that both
    /// mention metadata. A concurrent metadata change conflicts unconditionally (rule 1, and
    /// <see cref="ConcurrentMetadataChange_Conflicts"/> covers it); ours only widens which adds get
    /// examined. Here our metadata change meets a concurrent blind append whose file does NOT match our
    /// predicate — so the widened gate examines it and still finds nothing.
    /// </summary>
    [Fact]
    public void OwnMetadataChange_WidensTheGate_ItDoesNotManufactureAConflict()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Add("part-far.parquet", minId: 100, maxId: 200)); // cannot contain id = 5

        var result = CheckCommitting(
            [Metadata()], reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    // ── the inference's one piece of POSITIVE evidence: a cdc action ──

    /// <summary>
    /// ⚠ THE CASE #127 IS ABOUT. An insert-only MERGE on a change-data-feed table commits adds and a cdc
    /// file and NO remove — it scanned the target to decide what was missing, so it read. Every other clause
    /// of the inference sees only adds and calls that blind, which skips a concurrent-append check we owe.
    ///
    /// <para>Not hypothetical: this is delta-rs 1.6.2's measured output for that statement, and delta-rs
    /// declares no flag on any commit, so the inference is the whole answer there. Pinned against the real
    /// engine by <c>DeltaRsBlindAppendGroundTruthTests</c>; asserted as a verdict here.</para>
    /// </summary>
    [Fact]
    public void CdcAction_IsNotBlind_ThoughTheCommitOnlyAdds()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"operation":"MERGE","engineInfo":"delta-rs"}"""), // no isBlindAppend, as delta-rs writes none
            Add("part-new.parquet", minId: 1, maxId: 10),
            Cdc("_change_data/cdc-00000.parquet"));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentAppend, result.Type);
    }

    /// <summary>
    /// The control for the test above: the SAME commit without the cdc action is inferred blind and passes.
    /// Without this, that test would pass just as well if the add's stats had stopped matching the read
    /// predicate, and it would be proving nothing about cdc.
    /// </summary>
    [Fact]
    public void SameCommitWithoutCdc_IsStillInferredBlind()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"operation":"MERGE","engineInfo":"delta-rs"}"""),
            Add("part-new.parquet", minId: 1, maxId: 10));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// A DECLARED flag still outranks the cdc evidence, in the direction that costs us nothing to honour:
    /// a writer that says it read nothing is believed even on a cdc-carrying commit. The inference exists
    /// only to answer for writers that said nothing, and #125 settled that a declaration wins; adding
    /// positive evidence to the fallback must not quietly reopen that.
    /// </summary>
    [Fact]
    public void DeclaredBlind_OutranksTheCdcEvidence()
    {
        var reads = new ReadSet { Predicates = [Ex.Equal("id", LiteralValue.Of(5L))] };
        var concurrent = Commit(6,
            Info("""{"isBlindAppend":true}"""),
            Add("part-new.parquet", minId: 1, maxId: 10),
            Cdc("_change_data/cdc-00000.parquet"));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// The tests above hand <see cref="ConflictChecker"/> a <see cref="CommitInfo"/> directly, which proves
    /// the DECISION but not that a real caller ever sees one: the checker reads the concurrent commits
    /// through <c>TransactionLog.ReadCommitAsync</c>, and if that dropped or flattened commitInfo the
    /// declaration would be unreachable and every test above would be verifying dead code. So round-trip a
    /// commit through the log and assert the flag survives, readable exactly the way the checker reads it.
    /// </summary>
    [Fact]
    public async Task IsBlindAppend_SurvivesTheLogRoundTrip()
    {
        string dir = Path.Combine(Path.GetTempPath(), $"delta_blindappend_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            var log = new TransactionLog(new LocalTableFileSystem(dir));
            await log.WriteCommitAsync(0, new List<DeltaAction>
            {
                Info("""{"operation":"WRITE","isBlindAppend":false}"""),
                Add("part-0.parquet", minId: 1, maxId: 10),
            });

            var actions = await log.ReadCommitAsync(0);

            var info = Assert.Single(actions.OfType<CommitInfo>());
            var flag = info.GetValue("isBlindAppend");
            Assert.NotNull(flag);
            Assert.Equal(JsonValueKind.False, flag!.Value.ValueKind);
            Assert.False(flag.Value.GetBoolean());
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    // ── concurrentDeleteRead + delete/delete ──

    /// <summary>A concurrent data-changing remove of a file we read conflicts (concurrentDeleteRead).</summary>
    [Fact]
    public void ConcurrentDeleteOfReadFile_Conflicts()
    {
        var reads = new ReadSet { Files = new HashSet<string> { "part-read.parquet" } };
        var concurrent = Commit(6, Remove("part-read.parquet"));

        var result = Check(reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentDeleteRead, result.Type);
        Assert.Equal(6, result.ConflictingVersion);
    }

    /// <summary>
    /// A file we READ but do not remove, deleted concurrently, is <c>ConcurrentDeleteRead</c> — even though
    /// this transaction is committing removes of its own. The two categories are distinguished by which
    /// paths OUR actions remove, and a read path is not among them.
    ///
    /// <para>This used to be a way to get it wrong. The delete/delete check took a caller-supplied
    /// <c>plannedRemovePaths</c> set restating what the actions already said, and its documentation had to
    /// warn that naming a merely-read path there would report this case as delete/delete. Deriving the set
    /// from the actions makes that unrepresentable — a read path is not a <c>RemoveFile</c> — so this test
    /// pins a property of the API's shape rather than of its implementation.</para>
    /// </summary>
    [Fact]
    public void ReadFile_IsNotAPlannedRemove_EvenWhenTheTransactionRemovesOthers()
    {
        var reads = new ReadSet { Files = new HashSet<string> { "part-read.parquet" } };
        var concurrent = Commit(6, Remove("part-read.parquet"));

        var result = CheckCommitting(
            [Remove("part-other.parquet")], reads, IsolationLevel.WriteSerializable, concurrent);

        Assert.Equal(ConflictType.ConcurrentDeleteRead, result.Type);
    }

    /// <summary>Two transactions removing the same file conflict (delete/delete).</summary>
    [Fact]
    public void DeleteDelete_SameFile_Conflicts()
    {
        var concurrent = Commit(6, Remove("part-target.parquet"));

        // The removes come off the actions this transaction is committing — the same place the commit
        // itself will carry them, so the two cannot disagree.
        var result = CheckCommitting(
            [Remove("part-target.parquet")], ReadSet.Blind, IsolationLevel.WriteSerializable, concurrent);

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
        var result = Check(ReadSet.Blind, IsolationLevel.WriteSerializable, Commit(6, metadata));

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

        var result = Check(reads, IsolationLevel.Serializable, concurrent);

        Assert.False(result.HasConflict);
    }

    // ── a couple of guards beyond the seven parked cases ──

    /// <summary>No concurrent commits ⇒ nothing to conflict with.</summary>
    [Fact]
    public void NoConcurrentCommits_Passes()
    {
        var reads = new ReadSet { WholeTable = true };
        var result = Check(reads, IsolationLevel.Serializable);
        Assert.False(result.HasConflict);
    }

    /// <summary>The first conflicting version is reported, not a later one.</summary>
    [Fact]
    public void EarliestConflictingVersion_IsReported()
    {
        var result = CheckCommitting(
            [Remove("part-target.parquet")], ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(6, Add("part-x.parquet", 1, 10)),
            Commit(7, Remove("part-target.parquet")),
            Commit(8, Remove("part-target.parquet")));

        Assert.Equal(ConflictType.ConcurrentDeleteDelete, result.Type);
        Assert.Equal(7, result.ConflictingVersion);
    }

    // ── domainMetadata (#109) ──

    private static DomainMetadata Domain(string domain, string configuration = "{}", bool removed = false) =>
        new() { Domain = domain, Configuration = configuration, Removed = removed };

    /// <summary>
    /// Two writers editing the SAME domain conflict. Rebasing would not merge them — the later commit's
    /// configuration simply replaces the earlier one, silently losing an edit its author never saw.
    /// </summary>
    [Fact]
    public void ConcurrentWriteOfTheSameDomain_Conflicts()
    {
        var result = CheckCommitting(
            [Domain("acme.retention", "{\"days\":30}")], ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Domain("acme.retention", "{\"days\":7}")));

        Assert.Equal(ConflictType.DomainMetadataChanged, result.Type);
        Assert.Equal(4, result.ConflictingVersion);
        Assert.Equal(DeltaErrorCodes.DomainMetadataConflict, result.ErrorCode);
        Assert.Contains("acme.retention", result.Message);
    }

    /// <summary>A tombstone is an edit of the domain like any other, on either side of the race.</summary>
    [Fact]
    public void ConcurrentRemovalOfTheSameDomain_Conflicts()
    {
        var result = CheckCommitting(
            [Domain("acme.retention", "", removed: true)], ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Domain("acme.retention", "", removed: true)));

        Assert.Equal(ConflictType.DomainMetadataChanged, result.Type);
    }

    /// <summary>
    /// Different domains do not contest anything, so the two commits linearize in either order.
    /// </summary>
    [Fact]
    public void ConcurrentWriteOfADifferentDomain_DoesNotConflict()
    {
        var result = CheckCommitting(
            [Domain("acme.retention")], ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Domain("acme.lineage")));

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// A transaction that writes NO domain metadata is untouched by one that does — the rule is symmetric
    /// intersection, not "a concurrent domainMetadata is dangerous".
    /// </summary>
    [Fact]
    public void ConcurrentDomainWrite_AgainstATransactionThatWritesNone_DoesNotConflict()
    {
        var result = CheckCommitting(
            [Add("part-new.parquet", 1, 10)], ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Domain("acme.retention")));

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// The row-tracking high-water mark is EXEMPT, and this is the case that makes the exemption
    /// load-bearing: every commit that adds files to a row-tracking table advances it, so without the
    /// exemption two ordinary appends would conflict on a domain neither writer ever named — turning row
    /// tracking on would cost the table its concurrency. Delta special-cases the same domain in the same
    /// place.
    /// </summary>
    [Fact]
    public void ConcurrentAdvanceOfTheRowTrackingHighWaterMark_DoesNotConflict()
    {
        string rowTracking = RowTracking.RowTrackingConfig.DomainName;
        var result = CheckCommitting(
            [Add("part-mine.parquet", 1, 10), Domain(rowTracking, "{\"rowIdHighWaterMark\":200}")],
            ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Add("part-theirs.parquet", 20, 30), Domain(rowTracking, "{\"rowIdHighWaterMark\":150}")));

        Assert.False(result.HasConflict);
    }

    // ── ReadSet.Blind is not shared state ──

    /// <summary>
    /// <see cref="ReadSet.Blind"/> hands back a FRESH instance, so deriving from it cannot corrupt the
    /// default every other blind commit uses.
    ///
    /// <para>It used to be a cached singleton whose <see cref="ReadSet.Files"/> is a mutable
    /// <see cref="ISet{T}"/>. The accident this guards is not someone writing
    /// <c>ReadSet.Blind.Files.Add(…)</c> on purpose — it is the idiomatic derivation below: a record
    /// <c>with</c> copies SHALLOWLY, so the copy shared the original's set, and adding one path to the
    /// copy gave every later blind commit in the process a read dependency it never declared.</para>
    ///
    /// <para>The second half is the part that matters. A leaked path would make a concurrent remove of it
    /// report <c>concurrentDeleteRead</c> against a transaction that read nothing — a spurious conflict,
    /// process-wide, arising nowhere near the code that caused it.</para>
    /// </summary>
    [Fact]
    public void ReadSetBlind_IsFreshPerAccess_SoADerivedCopyCannotPoisonIt()
    {
        var derived = ReadSet.Blind with { WholeTable = true };
        derived.Files.Add("part-leaked.parquet");

        Assert.Empty(ReadSet.Blind.Files);

        var result = CheckCommitting(
            [Add("part-mine.parquet", 1, 10)], ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Remove("part-leaked.parquet")));

        Assert.False(result.HasConflict);
    }

    /// <summary>
    /// The exemption is for that ONE domain, not for "a commit that also advances the mark": a user domain
    /// contested alongside it still conflicts.
    /// </summary>
    [Fact]
    public void RowTrackingExemption_DoesNotCoverAUserDomainInTheSameCommit()
    {
        string rowTracking = RowTracking.RowTrackingConfig.DomainName;
        var result = CheckCommitting(
            [Domain(rowTracking, "{\"rowIdHighWaterMark\":200}"), Domain("acme.retention")],
            ReadSet.Blind, IsolationLevel.WriteSerializable,
            Commit(4, Domain(rowTracking, "{\"rowIdHighWaterMark\":150}"), Domain("acme.retention")));

        Assert.Equal(ConflictType.DomainMetadataChanged, result.Type);
        Assert.Contains("acme.retention", result.Message);
    }
}
