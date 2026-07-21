// Copyright (c) clast-project. All rights reserved.
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

    // ── Buffered (multi-statement) transactions — pr-4 BufferedTransactionTests ──

    /// <summary>An ALTER + INSERT + DELETE buffered together must commit as ONE atomic Delta version —
    /// the fused metaData + protocol + add + DV-remove commit shape delta-kernel validates.</summary>
    [Fact(Skip = BufferedTxn)]
    public void FusedCommit_AlterInsertDelete_IsOneAtomicVersion() { }

    // UN-PARKED (ChainedComputes_SecondAddComposesOnPendingSchema + ReconcileBatchToFields_BackfillsPendingColumn):
    // the compute-only schema-ALTER family (ComputeAddColumn chaining on a pending base) and the public
    // ReconcileBatchToFields overlay are live in BufferedSchemaSeamTests.
    //
    // UN-PARKED (ExpectedVersion_ConcurrentWriter_ConflictAborts): the CommitDataFilesAsync(expectedVersion:)
    // conflict-abort is live in ExternalDataFileCommitTests, against the Milestone-A external-commit seam.

    /// <summary>ReadRowsByRowIdsAsync(atVersion:) must read back exactly the addressed rows — the
    /// mechanism an UPDATE post-image is built from.</summary>
    [Fact(Skip = BufferedTxn)]
    public void ReadRowsByRowIds_AtVersion_ExactReadBack() { }

    /// <summary>A txn (application transaction id) action must round-trip through a fused commit.</summary>
    [Fact(Skip = BufferedTxn)]
    public void AppTransactionAction_RoundTrips() { }

    // ── Identity columns across buffered statements — pr-4 IdentityTransactionSeamsTests ──
    //
    // UN-PARKED: the identity seam (GenerateIdentityValues chaining, GenerateIdentityValuesForSchema for a
    // pending CREATE, BuildIdentityMetadataAction folding the final marks into one metaData, and the
    // WriteDataFilesAsync pre-generated-flag guard) is live in IdentityTransactionSeamsTests.

    // ── Logical rebase / ConflictChecker parity — pr-4 LogicalRebaseTests ──
    //
    // UN-PARKED (verdict logic): the seven ConflictChecker cases these tests pinned — blind-append
    // matching under Serializable vs WriteSerializable, non-matching pass, concurrentDeleteRead,
    // delete/delete, metadata change, and the dataChange=false compaction exemption — are now live in
    // ConflictCheckerTests, against the ConflictChecker that slice 9 layer 1 introduced.
    //
    // UN-PARKED (integration): the end-to-end transaction path — a transaction that reads, has a
    // concurrent commit land in the log, then either rebases onto it and commits or aborts — is now live
    // in DeltaTransactionTests (two overlapping transactions: disjoint files both commit, same file the
    // second aborts). Remaining gap: that path currently covers DELETE only; staging appends/updates on
    // a transaction, and row-level (same-file disjoint-row) concurrency, are still parked below.

    // ── Row-level concurrency — pr-4 RowLevelConcurrencyTests ──
    //
    // UN-PARKED (sub-problem A — DELETE/DELETE deletion-vector union): two disjoint-row deletes of the same
    // file both landing, two same-row deletes conflicting, and a non-reconcilable conflict still surfacing
    // are live in RowLevelConcurrencyTests. DV positions are stable across a concurrent DV-delete, so no row
    // tracking is needed for that half.
    //
    // UN-PARKED (sub-problem B — remap across a rewrite): a delete whose target file was concurrently
    // COMPACTED or UPDATE-rewritten is now relocated by STABLE ROW ID onto the new file (requires row
    // tracking), and a target row concurrently deleted/updated is a row-level conflict — all live in
    // RowLevelConcurrencyTests (ConcurrentUpdateAndDelete_DisjointRows_BothLand,
    // DeleteThroughConcurrentCompaction_Remapped, DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict),
    // against DeltaTable.RemapRowLevelDeletesAsync. This also retired the row-tracking rebaseSafe:false
    // limitation for DELETE-only transactions.
    //
    // STILL PARKED (the buffered-transaction seam): the case below drives the explicit Compute* → rebase →
    // commit surface (ComputeDeletionVectorActionsAsync / RebaseDvDmlActionsAsync / CheckLogicalRebaseAsync /
    // CommitDataFilesAsync), which is the deferred multi-statement seam, not row-level concurrency itself.

    /// <summary>The buffered flow composes: Compute* → rebase → commit, against a concurrent delete.</summary>
    [Fact(Skip = BufferedTxn)]
    public void BufferedFlow_ComputeThenRebaseThenCommit_ComposesWithConcurrentDelete() { }

    // ── SetSchemaAsync — pr-4 SchemaWriteModesTests ──
    //
    // UN-PARKED: SetSchemaAsync (adopt an incoming schema as a metadata-only drop/add commit, with a logical
    // no-op compare) is live in BufferedSchemaSeamTests.

    // ── Clustering rewrite-commit shape — pr-4 ClusteredTableTests ──
    //
    // UN-PARKED: the Overwrite-shaped rewrite commit (dataChange=false on BOTH removes and adds, so CDF readers
    // exclude it and appendOnly still permits it — it removes files, not rows; add.clusteringProvider stamped)
    // is live in ExternalDataFileCommitTests.CommitDataFiles_RewriteShape_DataChangeFalseAndClusteringProvider,
    // against the Milestone-A CommitDataFilesAsync(dataChange:, clusteringProvider:) surface.
}
