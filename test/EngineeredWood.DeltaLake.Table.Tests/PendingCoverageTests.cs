// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <para><b>Coverage that Christoph Mettler's PR #4 has and master does not — parked, not lost.</b></para>
///
/// <para>Every test here pins a behaviour the PR demonstrated (in several cases a real bug it found), but
/// whose implementation has not been landed on master yet. They are <c>Skip</c>ped rather than failing so
/// that a red suite still means a real regression; each Skip reason names the exact API that must exist
/// before it can be un-skipped. When that work lands, grep this file — the bodies are described precisely
/// enough to write directly from the comment, and the originals are in
/// <c>pr-4:test/EngineeredWood.DeltaLake.Table.Tests/</c>.</para>
///
/// <para>Deliberately NOT parked here: tests master already covers under different names (the clustering
/// suite, partitioned compaction, the overwrite modes, nested-stats pruning, timestamp resolution,
/// appendOnly enforcement), and <c>DecimalReadTests.Int32Decimal_ReadsAsDecimal128</c> /
/// <c>Int64Decimal_ReadsAsDecimal128</c>, which master intentionally diverges from — the widening is an
/// opt-in <c>DecimalOutputKind</c> option, covered by <c>DecimalOutputKindTests</c>.</para>
/// </summary>
public class PendingCoverageTests
{
    private const string BufferedTxn =
        "Blocked: needs the buffered-transaction seam (WriteDataFilesAsync / CommitDataFilesAsync / the " +
        "Compute* family / ReadRowsByRowIdsAsync / ReconcileBatchToFields) — PR #4 slice 9.";

    private const string LogicalRebase =
        "Blocked: needs CheckLogicalRebaseAsync (the Spark-parity ConflictChecker) — PR #4 slice 9.";

    private const string RowLevelConcurrency =
        "Blocked: needs row-level concurrency (ComputeDeletionVectorActionsAsync(resolveAgainst:) + the " +
        "row-level conflict/retry path) — PR #4 slice 9.";

    private const string SetSchema = "Blocked: needs DeltaTable.SetSchemaAsync — PR #4 slice 8 leftover.";

    private const string CommitDataFiles =
        "Blocked: needs CommitDataFilesAsync(dataChange:, clusteringProvider:) — PR #4 slice 10, itself " +
        "gated on the buffered-transaction seam (slice 9).";

    // ── Buffered (multi-statement) transactions — pr-4 BufferedTransactionTests ──

    /// <summary>An ALTER + INSERT + DELETE buffered together must commit as ONE atomic Delta version —
    /// the fused metaData + protocol + add + DV-remove commit shape delta-kernel validates.</summary>
    [Fact(Skip = BufferedTxn)]
    public void FusedCommit_AlterInsertDelete_IsOneAtomicVersion() { }

    /// <summary>A second ComputeAddColumn must compose against the PENDING schema of the first (whose
    /// maxColumnId it already bumped), not against the committed snapshot.</summary>
    [Fact(Skip = BufferedTxn)]
    public void ChainedComputes_SecondAddComposesOnPendingSchema() { }

    /// <summary>CommitDataFilesAsync(expectedVersion:) must turn the append OCC retry into a conflict
    /// ABORT — first-committer-wins snapshot isolation for snapshot-coupled actions.</summary>
    [Fact(Skip = BufferedTxn)]
    public void ExpectedVersion_ConcurrentWriter_ConflictAborts() { }

    /// <summary>ReadRowsByRowIdsAsync(atVersion:) must read back exactly the addressed rows — the
    /// mechanism an UPDATE post-image is built from.</summary>
    [Fact(Skip = BufferedTxn)]
    public void ReadRowsByRowIds_AtVersion_ExactReadBack() { }

    /// <summary>The public ReconcileBatchToFields export must let a host overlay a PENDING schema onto
    /// committed reads (backfilling a column the buffered transaction has added but not yet committed).</summary>
    [Fact(Skip = BufferedTxn)]
    public void ReconcileBatchToFields_BackfillsPendingColumn() { }

    /// <summary>A txn (application transaction id) action must round-trip through a fused commit.</summary>
    [Fact(Skip = BufferedTxn)]
    public void AppTransactionAction_RoundTrips() { }

    // ── Identity columns across buffered statements — pr-4 IdentityTransactionSeamsTests ──

    /// <summary>GenerateIdentityValues must chain across statements in one transaction: the second
    /// statement continues the first's high-water mark rather than restarting from the snapshot.</summary>
    [Fact(Skip = BufferedTxn)]
    public void GenerateIdentityValues_ChainsAcrossStatements() { }

    /// <summary>The fused commit must carry exactly ONE metaData action with the persisted identity
    /// high-water mark (two conflicting metaData actions in a commit is invalid).</summary>
    [Fact(Skip = BufferedTxn)]
    public void FusedIdentityCommit_OneMetadataAction_HwmPersists() { }

    /// <summary>The schema-seeded pending-CREATE form must generate identity values before any table
    /// exists.</summary>
    [Fact(Skip = BufferedTxn)]
    public void GenerateIdentityValuesForSchema_PendingCreate_ChainsWithoutATable() { }

    /// <summary>WriteDataFilesAsync must REJECT an identity table unless the caller flags that it
    /// pre-generated the values — silently writing un-valued identity rows would corrupt the contract.</summary>
    [Fact(Skip = BufferedTxn)]
    public void WriteDataFiles_WithoutPreGeneratedFlag_RejectsIdentityTable() { }

    // ── Logical rebase / ConflictChecker parity — pr-4 LogicalRebaseTests ──

    /// <summary>A blind append whose files match the transaction's read predicates conflicts under
    /// Serializable.</summary>
    [Fact(Skip = LogicalRebase)]
    public void BlindAppend_MatchingReads_Conflicts_Serializable() { }

    /// <summary>...but PASSES under WriteSerializable — the isolation level is the whole distinction.</summary>
    [Fact(Skip = LogicalRebase)]
    public void BlindAppend_MatchingReads_Passes_WriteSerializable() { }

    /// <summary>A blind append whose files cannot match the read predicates passes even under
    /// Serializable (stats/partition-based read matching, conservative).</summary>
    [Fact(Skip = LogicalRebase)]
    public void BlindAppend_NonMatchingPredicate_Passes_Serializable() { }

    /// <summary>A concurrent data-changing REMOVE of a file the transaction READ conflicts
    /// (concurrentDeleteReadCheck).</summary>
    [Fact(Skip = LogicalRebase)]
    public void ConcurrentDeleteOfReadFile_Conflicts() { }

    /// <summary>Two transactions removing the same file conflict (delete/delete).</summary>
    [Fact(Skip = LogicalRebase)]
    public void DeleteDelete_SameFile_Conflicts() { }

    /// <summary>A concurrent metadata change conflicts unconditionally.</summary>
    [Fact(Skip = LogicalRebase)]
    public void ConcurrentMetadataChange_Conflicts() { }

    /// <summary>dataChange=false commits (compaction) are EXEMPT from the read checks — they rearrange
    /// files without changing rows, so they cannot invalidate another transaction's reads.</summary>
    [Fact(Skip = LogicalRebase)]
    public void Compaction_ExemptFromReadChecks() { }

    // ── Row-level concurrency — pr-4 RowLevelConcurrencyTests ──

    /// <summary>Two concurrent deletes touching DISJOINT rows of the same file must BOTH land (the point
    /// of row-level concurrency: file-level conflict detection would reject the second).</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void ConcurrentDeletes_SameFile_DisjointRows_BothLand() { }

    /// <summary>Two concurrent deletes of the SAME row must conflict.</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void ConcurrentDeletes_SameRow_RowLevelConflict() { }

    /// <summary>A concurrent UPDATE and DELETE on disjoint rows must both land.</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void ConcurrentUpdateAndDelete_DisjointRows_BothLand() { }

    /// <summary>A delete whose target file was concurrently COMPACTED must be remapped onto the
    /// compacted file rather than failing.</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void DeleteThroughConcurrentCompaction_Remapped() { }

    /// <summary>...but if the row was also concurrently deleted, that is a genuine row-level conflict.</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict() { }

    /// <summary>Without the row-level retry the plain version conflict must still surface — the retry is
    /// an addition, not a silencer.</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void WithoutRowLevelRetry_VersionConflictSurfaces() { }

    /// <summary>The buffered flow composes: Compute* → rebase → commit, against a concurrent delete.</summary>
    [Fact(Skip = RowLevelConcurrency)]
    public void BufferedFlow_ComputeThenRebaseThenCommit_ComposesWithConcurrentDelete() { }

    // ── SetSchemaAsync — pr-4 SchemaWriteModesTests ──

    /// <summary>SetSchemaAsync must adopt an incoming schema as a metadata-only commit, computing the
    /// drops and adds relative to the current one.</summary>
    [Fact(Skip = SetSchema)]
    public void SetSchema_AdoptsIncomingSchema_DropAndAdd() { }

    /// <summary>A logically identical schema must be a no-op (no commit written).</summary>
    [Fact(Skip = SetSchema)]
    public void SetSchema_LogicallyIdentical_IsNoOp() { }

    // ── Clustering rewrite-commit shape — pr-4 ClusteredTableTests ──

    /// <summary>A clustering OPTIMIZE commits Overwrite-shaped with dataChange=false on BOTH removes and
    /// adds (so CDF readers exclude it, and appendOnly still permits it — it removes files, not rows) and
    /// stamps add.clusteringProvider = "liquid".</summary>
    [Fact(Skip = CommitDataFiles)]
    public void CommitDataFiles_RewriteShape_DataChangeFalseAndClusteringProvider() { }
}
