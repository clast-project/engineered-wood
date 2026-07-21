# Resuming the Delta optimistic-concurrency work (PR #4 slice 9)

Handoff for continuing or completing EngineeredWood's Delta Lake concurrency support. Read this
first; it records the correct framing, what has landed, what remains, and the facts that were
established by *measurement* so they are not re-derived (several were wrong when reasoned from first
principles — see "Measure, don't assume" below).

## The correct framing

This is **optimistic-concurrency (OCC) correctness**, not multi-statement transactions. The PR's own
test naming (`BufferedTransactionTests`) over-emphasises fused multi-statement commits; that is one
*consumer* of the machinery, not its purpose. The heart is the classic Delta OptimisticTransaction: a
transaction records the version it read from, does its work, and at commit validates that nothing it
read was invalidated by a commit that landed in between — aborting only on a real conflict, otherwise
rebasing onto the newer version and committing.

### What master did before this work (verified)

- `DeleteAsync`/`WriteCoreAsync` compute actions against `CurrentSnapshot` and commit at
  `readVersion + 1`.
- `TransactionLog.WriteCommitAsync` uses an atomic write-temp-then-rename and throws
  `DeltaConflictException` on a name collision.
- **Nothing caught that exception.** No retry, no read-set validation, no rebase.

So master was **safe but fragile**: the atomic rename prevents lost updates, but any concurrent commit
failed an in-flight write even when there was no real conflict, and there was no notion of "did my
read stay valid". This work upgrades fragile-but-safe to robust-and-correct. It is *not* fixing active
data corruption.

## The three layers (and where "layer 1" really sits)

- **Layer 1 — OCC correctness** (the important part): read-version tracking + the ConflictChecker +
  bounded rebase-retry. This is what makes concurrent writers safe. **Steps 1 and 2 below are done.**
- **Layer 2 — full ConflictChecker parity**: the verdict rules. Landed as the checker in step 1.
- **Layer 3 — row-level concurrency** (Databricks extension): two deletes on the *same file* touching
  *disjoint rows* both land, instead of the second aborting. Needs DV re-union (`resolveAgainst:`) and
  remap-across-rewrite by stable row id. OSS Spark and delta-rs also conflict at file granularity here, so
  the pre-existing file-level abort is *correct*, just not maximally permissive.
  **Sub-problem (A) — DELETE/DELETE deletion-vector union — DONE (see "What landed", step 6).**
  Sub-problem (B) — rewrite-preservation (remap through a concurrent compaction/UPDATE by stable row id) —
  **not started.**

The buffered multi-statement surface (`Compute*` schema ALTERs, identity chaining,
`ReconcileBatchToFields`, `ReadRowsByRowIds`) is **not needed for OCC correctness** and is deferred.

## What has landed

| Step | Commit | What |
|---|---|---|
| 1 | `705a4e2` | `ConflictChecker` (pure verdict function) + `IsolationLevel` + `ConflictCheckerTests` (9). |
| 2 | `b8e4fc6` | `DeltaTransaction` + `StartTransaction()` + the OCC commit loop + `DeltaTransactionTests` (4). |
| 3 | `7964d07` | Auto-committers routed through the OCC loop: `DeleteAsync` and the blind-append `WriteAsync` now rebase-retry instead of throwing on any collision. OCC loop generalised to `CommitOccAsync`; `AutoCommitConcurrencyTests` (6) + a rebased-commit tier-3 Spark test. |
| 4 | `3bfb9fb` | Appends + updates stageable on `DeltaTransaction` (`WriteAsync`/`UpdateAsync`), closing limitation 1. Extracted `ComputeWriteActionsAsync` (shared by `WriteCoreAsync` + txn append) and `ComputeUpdateActionsAsync` (shared by `UpdateAsync` + txn update); auto-committer `UpdateAsync` now also rebase-retries via `CommitOccAsync`. Operation-label tracking on the txn; `ValidateWritable(snapshot, isAppend)` shared gate. `DeltaTransactionTests` +5. |
| 5 | `5a8445f` | Analyzable-predicate `DeleteAsync`/`UpdateAsync` overloads (`Expressions.Predicate`), closing limitation 3. The predicate becomes the operation's `ReadSet.Predicates`, so concurrentAppend is now precise (a concurrent add matching it conflicts, per isolation level); files that can't match are pruned from the read. `EngineeredWood.Expressions.Arrow` project ref added for the `ArrowRowEvaluator` row mask. `ExpressionPredicateTests` (6). |
| 8 | *(pending commit, 2026-07-20)* | **Row-tracking append/update rebase — closes limitation 2's other half.** An append or an UPDATE's copy-on-write post-image add now rebases under row tracking instead of aborting. `CommitOccAsync` builds the latest snapshot whenever row tracking is enabled and, on a no-conflict rebase, calls the new `RebaseRowTrackingAddIds`: it re-derives each POST-IMAGE add's `baseRowId` from `latestSnapshot.RowIdHighWaterMark` and `defaultRowCommitVersion` from the attempt version, then rebuilds the `delta.rowTracking` high-water-mark domain — leaving a row-level delete's own re-adds (existing files / remapped new files) untouched (excluded by `resolvedPaths` + base-active membership). All callers pass `rebaseSafe: true` (transaction, auto `DeleteAsync`/`WriteAsync`/`UpdateAsync`); the abort-under-RT path is gone (only the overwrite family stays single-attempt, for the whole-active-set read-set reason — unrelated to row tracking). Tests: new `RowTrackingRebaseTests` (3 — concurrent appends re-derive contiguous baseRowIds, transactional append rebase, UPDATE rebases past a concurrent append preserving ids). **MEASURED:** `SparkInteropTests.EwConcurrentAppends_RowTracking_Rebased_SparkReadsContiguousIds` — Spark 4.0.1 reads 0,1,2 (contiguous, unique) after a rebased append, proving the re-derived baseRowId is spec-correct (a wrong one would duplicate the winner's ids). Full matrix green: 313 non-interop (net10 + net472), 22 Spark + 12 delta-rs. |
| 7 | *(pending commit, 2026-07-20)* | **Layer 3 sub-problem (B) — remap a losing DELETE across a concurrent REWRITE (row-tracking Milestone 3).** A DELETE whose target file was concurrently COMPACTED or UPDATE-rewritten is now relocated by STABLE ROW ID onto the new file(s) instead of aborting, and its commit version is the concurrent-modification discriminator (untouched-relocated both-lands; concurrently updated/deleted row → row-level conflict). `ReadFileAsync`/`ProcessFileBatchesAsync` gained a third out-param `strippedAbsPositionsOut` (absolute in-file position per surviving row) so the remap can correlate a delete's target positions with the M2 resolved stable ids/versions. `ResolveRowLevelDeletesAsync` (now takes `baseSnapshot`) SPLITS a delete's edits into still-active (DV union, path (A)) vs rewritten-away (new `RemapRowLevelDeletesAsync`): phase 1 resolves target stable ids+versions from the tombstoned source read at base, phase 2 finds them in the new files (compaction `dataChange=false` first, early-exit; a DV-deleted relocated row is filtered → not found → conflict), phase 3 builds remove/add DV pairs on the new files (added to `rowLevelResolvedPaths` so the checker ignores their concurrent add). `CommitTransactionAsync` relaxed: a DELETE-only transaction (`Operation=="DELETE"`) is now `rebaseSafe` and row-level-resolvable EVEN under row tracking (its rebase only re-adds existing files, keeping their baseRowId) — **retires limitation 2 for deletes**; appends/updates under RT still abort (fresh baseRowId). Un-parked the 3 (B) tests into `RowLevelConcurrencyTests` (`ConcurrentUpdateAndDelete_DisjointRows_BothLand`, `DeleteThroughConcurrentCompaction_Remapped`, `DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict`). **MEASURED (not assumed):** new tier-3 test `SparkInteropTests.EwDeleteRemappedThroughCompaction_SparkReadsSurvivorsWithPreservedIds` — Spark 4.0.1 reads the remapped result (DV on the compacted materialized-id file), returns the survivors with ORIGINAL ids and omits exactly the remapped-away row. Full matrix green: 310 non-interop (net10 + net472), 21 Spark + 12 delta-rs interop. |
| 6 | *(pending commit)* | **Layer 3 sub-problem (A) — DELETE/DELETE deletion-vector union.** Two concurrent deletes of *disjoint rows of the same file* now BOTH land instead of the second aborting; same-row deletes still conflict. `ComputeDeleteActionsAsync` records per-file newly-deleted row positions (`DeleteDvEdit`); `CommitOccAsync` grew a `rowLevelDeletes` param and `ResolveRowLevelDeletesAsync`, which on a delete/delete collision rebases each staged delete's DV onto the file's *current* DV (union) rather than throwing — returning null (→ abort) only on same-row overlap or a rewritten-away file. `ConflictChecker.Check` gained `rowLevelResolvedPaths` (skips the reconciled file's concurrent remove/re-add). Withheld under row tracking (`rowLevelDeletes: null`, still `rebaseSafe:false`). `RowLevelConcurrencyTests` (4) + a delta-rs interop test; the two former same-file-abort tests (`TwoTransactions_SameFile_SecondAborts`, `TwoHandles_SameFileDeletes_SecondAborts`) were repurposed to same-*row* (they now correctly both-land at disjoint rows). **Found (measured):** EW writes DVs but never declares the `deletionVectors` protocol feature, so delta-rs ignores *all* EW deletion vectors — pre-existing, orthogonal, recorded in `known-issues.md`. |

Entry points:

- `src/EngineeredWood.DeltaLake.Table/Concurrency/ConflictChecker.cs` — pure, no I/O. Rules, in order:
  metadata change, protocol change, delete/delete, concurrentDeleteRead (dataChange=false compaction
  exempt), concurrentAppend (blind append exempt under WriteSerializable). Modeled on Spark's
  `ConflictChecker`.
- `src/EngineeredWood.DeltaLake.Table/IsolationLevel.cs` — public enum, `WriteSerializable` (default) /
  `Serializable`. The two differ only on whether a concurrent blind append matching read predicates
  conflicts.
- `src/EngineeredWood.DeltaLake.Table/DeltaTransaction.cs` — public; thin recorder of staged actions +
  read-set. `ReadVersion`, `IsolationLevel`, `WriteAsync`, `DeleteAsync`/`UpdateAsync` (each with a
  functional and an `Expressions.Predicate` overload), `CommitAsync`. Accumulates `_dataActions` +
  `_removedPaths` + `_operations` + `_readPredicates` (the analyzable predicates → `ReadSet.Predicates`).
  Several ops can be staged on one transaction; `_operations` drives the commitInfo label (single-op → that
  op, mixed → "WRITE").
- `src/EngineeredWood.DeltaLake.Table/DeltaTable.cs` — `StartTransaction()`, the internal
  `CommitTransactionAsync` (thin wrapper) → `CommitOccAsync` (the OCC loop), and the extracted compute
  halves each shared by an auto-committer and the transaction: `ComputeDeleteActionsAsync`,
  `ComputeWriteActionsAsync` (all write modes; the txn calls it with `Append` only), and
  `ComputeUpdateActionsAsync` (the last two take an optional `prunePredicate` for stats-based file
  pruning). `WriteCoreAsync` = validate + compute + `CommitWriteAsync` (append → `CommitOccAsync`;
  overwrite family → single-attempt). `ValidateWritable(snapshot, isAppend)` is the shared
  write-precondition gate (protocol + writer features), validated against the txn's base snapshot.
  `MaskFor(Expressions.Predicate)` + the shared static `ArrowRowEvaluator` turn a predicate into the
  row-mask delegate the compute halves consume.

Design facts worth keeping:

- A DELETE's read-set is exactly the files it rewrites, so the removed paths serve as **both** the
  concurrentDeleteRead read-set and the delete/delete planned-removes.
- On a no-conflict rebase the staged actions are re-committed **verbatim** — valid precisely because
  "no conflict" means nothing the transaction read or removed was touched. No action re-resolution is
  needed at file-level granularity (that is only for row-level, layer 3).
- The checker takes the concurrent commits as a parameter (stays pure/testable); the loop in
  `CommitTransactionAsync` owns reading `readVersion+1..latest` from the log.

## Deliberate limitations currently in place

1. ~~**DELETE-only on the transaction.**~~ **CLOSED (step 4).** `WriteAsync` (append) and `UpdateAsync`
   are now stageable alongside `DeleteAsync`. Overwrite modes remain auto-committer-only (their read-set
   is the whole active-file set / a partition predicate — not yet expressible for the checker; the txn's
   `WriteAsync` calls `ComputeWriteActionsAsync` with `Append` only). A transactional append is blind
   (reads empty → equivalent to `ReadSet.Blind`); an update reads the files it rewrites, exactly like a
   delete.
2. ~~**Row-tracking tables abort on rebase.**~~ **CLOSED (step 7 = deletes, step 8 = appends/updates).** A
   DELETE-only transaction rebases via DV union / stable-id remap (both keep the file's own `baseRowId`). An
   append or an UPDATE's copy-on-write post-image add now rebases too: `CommitOccAsync` RE-DERIVES each
   post-image add's `baseRowId` from the latest snapshot's high-water mark and its `defaultRowCommitVersion`
   from the new version, rebuilding the `delta.rowTracking` domain (`RebaseRowTrackingAddIds`, mirroring pr-4's
   `RebaseDvDmlActionsAsync` post-image branch). All auto-committers + the transaction now pass
   `rebaseSafe: true`. **The only remaining non-rebase-safe case is unrelated to row tracking:** the overwrite
   family (full / partition / dynamic) still single-attempts because its remove-set is a read of the whole
   active-file set (needs partition-predicate plumbing — limitation on the overwrite side, not row tracking).
3. ~~**concurrentAppend is inert for DELETE.**~~ **CLOSED (step 5).** `DeleteAsync`/`UpdateAsync` now have
   `Expressions.Predicate` overloads (functional overloads unchanged). The analyzable predicate is
   evaluated to a row mask via `ArrowRowEvaluator` (new `EngineeredWood.Expressions.Arrow` project ref),
   used to prune files that can't match, AND recorded in the transaction's `ReadSet.Predicates` — so a
   concurrent add matching it conflicts (concurrentAppend), precise to the isolation level (blind append
   exempt under WriteSerializable, examined under Serializable). Verified three ways in
   `ExpressionPredicateTests`. **New constraint from this:** `ArrowRowEvaluator` only evaluates a limited
   column-type set (bool / (u)int / float / double / string / binary) — a predicate over a
   date/decimal/timestamp column throws `NotSupportedException` at row-eval time. `DeltaFilePruner` (stats
   pruning) is independent and broader, so pruning may work where row-eval does not; the fix is to extend
   the evaluator's type coverage.

## Remaining work, in suggested order

1. ~~**Step 3 — route the auto-committers through the OCC loop.**~~ **DONE (`7964d07`).** Single-shot
   `DeleteAsync` now delegates to `StartTransaction()` + `DeleteAsync` + `CommitAsync`, and blind-append
   `WriteAsync` (mode `Append`, not dynamic-overwrite) commits through the shared `CommitOccAsync` — both
   rebase-retry instead of throwing on a non-conflicting collision, and abort with `DeltaConflictException`
   only on a real conflict. Covered by `AutoCommitConcurrencyTests`. **Deliberately NOT rebased:** the
   overwrite family (full / partition-scoped / dynamic) keeps the single-attempt commit — its remove-set is
   a read of the active-file set, so a verbatim rebase could silently drop a concurrent append; making it
   rebase-safe needs the partition-predicate plumbing of step 3-below (was limitation 3). Row-tracking
   writes also stay single-attempt (`rebaseSafe: false`) for the baseRowId reason (limitation 2).
2. ~~**Appends/updates on the transaction**~~ **DONE (`3bfb9fb`).** `DeltaTransaction` now
   has `WriteAsync` (blind append) and `UpdateAsync` in addition to `DeleteAsync`, via the extracted
   `ComputeWriteActionsAsync` / `ComputeUpdateActionsAsync` (each shared with its auto-committer). Two
   concurrent transactional appends both land; a transactional update aborts only if a concurrent commit
   removed a file it rewrote. Auto-committer `UpdateAsync` gained rebase-retry (routed through
   `CommitOccAsync`) as a side benefit. `DeltaTransactionTests` +5. Still NOT stageable: overwrite modes
   (see limitation 1).
3. ~~**Expression-predicate DELETE/UPDATE overload**~~ **DONE (`5a8445f`).** Analyzable
   `Expressions.Predicate` overloads of `DeleteAsync`/`UpdateAsync` (auto-committer + transaction), read
   predicate recorded so concurrentAppend is precise. Follow-ups:
   - ~~extend `ArrowRowEvaluator` to date/decimal/timestamp columns~~ **DONE (`119242e`).** Added
     Date32/Date64/Timestamp + Decimal32/64/128/256 cases (mapped to the same `LiteralValue` kinds the stats
     decoder uses — `DateTimeOffset`/`Decimal`/`HighPrecisionDecimal`), plus `LiteralValue` cross-type
     comparison for Decimal↔HighPrecisionDecimal, integer↔decimal, and DateOnly↔DateTimeOffset.
     **Gotcha found:** the Delta/parquet reader NARROWS a decimal column to the smallest
     `Decimal{32,64,128,256}Array` that fits its precision, so all four array widths must be handled (a
     `decimal(12,2)` reads back as `Decimal64Array`). Tests: `ArrowRowEvaluatorTests` +5, `LiteralValueTests`
     +4, `ExpressionPredicateTests` +3 (incl. an end-to-end timestamp concurrentAppend-precision case).
   - **stats-pruning gaps for these types (measured, then fixed/pending):** timestamp stats always
     round-tripped and pruned. Date stats were emitted as a raw day-number the decoder rejects → no
     pruning (SAFE); **fixed (`18c2de8`):** `StatsCollector` now emits date bounds as Spark's `"yyyy-MM-dd"`
     strings (verified by a tier-3 test that Spark skips a file on a date predicate). Decimal was likewise
     not collected → no decimal pruning; **fixed (`a5010ed`):** decimal min/max now emitted as JSON
     numbers — Decimal32/64 via `System.Decimal`, Decimal128/256 via exact unscaled `BigInteger` +
     `Utf8JsonWriter.WriteRawValue` so high-precision (measured: Spark writes even 38-digit decimals as raw
     numbers) survives. Tier-3 test confirms Spark prunes on EW decimal stats. Both stats fixes complete.
   - **decimal DECODE precision (`DeltaLiteralDecoder`) — fixed (pending commit):** measured that
     `JsonElement.TryGetDecimal`/`decimal.TryParse` silently ROUND a >28-29 sig-digit value (e.g. a
     `decimal(38,30)` stat) to `System.Decimal` precision → a shifted min/max bound could wrongly prune a
     file (data loss). Rewrote the decoder to parse the exact digits from the raw text into an unscaled
     `BigInteger` at the value's own scale, materializing `System.Decimal` only when lossless else
     `HighPrecisionDecimal` (relies on the cross-type comparison added in `119242e`). Pre-existing bug (hit
     any decimal stats EW read, e.g. Spark's), now also relevant since EW writes decimal stats.
     Checkpoint is safe (the `stats` JSON string is preserved on read; the `stats_parsed` decimal→double
     approx is unused by EW's pruner — a separate pre-existing quirk).
   - rebasing partition/dynamic overwrite now has the predicate machinery it needed, but still needs the
     overwrite read-set expressed as a partition predicate.
4. **Layer 3 — row-level concurrency. DONE.** Sub-problem (A) (step 6), sub-problem (B) remap (step 7 /
   Milestone 3), and the row-tracking append/update rebase (step 8, limitation 2's other half) all landed and
   are measured on Spark.

5. **Buffered-transaction seam. DONE (2026-07-21).** The multi-statement OptimisticTransaction surface, landed
   as 6 focused commits: **M-A `d1a7fb0`** external write/commit core — `WriteDataFilesAsync` /
   `CommitDataFilesAsync` (+ `WrittenDataFile`, the `IsIcebergCompat`/`HasIdentityColumns`/
   `SupportsExternalDataFileCommit` gates, `StatsWithLooseBounds`); **M-B `efe50a8`** compute-only schema ALTERs
   — `DeferredSchemaChange` + `ComputeAddColumn/RenameColumn/DropColumn` + `ReconcileBatchToFields` +
   `SetSchemaAsync`; **M-C `309437b`** identity seam — `GenerateIdentityValues[ForSchema]` +
   `BuildIdentityMetadataAction`; **M-D1 `61a4efa`** `ComputeDeletionVectorActionsAsync` + `ReadRowsByRowIdsAsync`
   (adapted to master's `strippedAbsPositionsOut` read path, not pr-4's trailing row-id column) +
   `OrderedActiveFiles`/`RowIdPositionBits`; **M-D2 `ce44d26`** `RebaseDvDmlActionsAsync` +
   `CheckLogicalRebaseAsync` + `MetadataEquals`/`ProtocolEquals`; **M-D3 `3729835`** the `txn` round-trip (no new
   code — master already reconciled `TransactionId`). **MEASURED (`22e79b2`):** Spark 4.0.1 reads a fused
   ALTER+INSERT+DELETE as one atomic version; delta-rs 1.6.2 reads the DV-free fused ALTER+INSERT. **Gap 1 —
   nested-field ALTER. DONE (2026-07-21).** Ported from `pr-4`: the auto-commit `AddFieldAsync` /
   `RenameFieldAsync` / `DropFieldAsync` (metadata-only, nested analogs of the top-level column ALTERs) AND the
   compute-only buffered halves `ComputeAddField` / `ComputeRenameField` / `ComputeDropField`, sharing the
   `TransformStructAt` / `PathText` helpers (master's recursive read reconcile in `SchemaEvolution.cs` already
   handled the read side). `AddField` uses master's recursive `AssignMappedField` so struct/array/map descendants
   each get ids (dropped pr-4's inconsistent struct rejection — matches `AddColumnAsync`); rename/drop require
   column mapping and refuse to empty a struct. 12 tests in `NestedFieldEvolutionTests` (two-level add, chained
   compute-adds distinct ids, mapped rename/drop reconcile old files, guards); full non-interop suite green (358).
   **Deferred (no parked test, follow-up):** the EXPLICIT buffered remap-across-rewrite — `RebaseDvDmlActionsAsync`
   conflicts when a modified file was concurrently rewritten (the AUTO commit path already remaps by stable id).

Remaining slice-9 tail (unrelated to row tracking OR the buffered seam): the overwrite-family rebase
(partition-predicate read-set) and M4 read-side `_metadata.row_id`.

The parked ledger `test/EngineeredWood.DeltaLake.Table.Tests/PendingCoverageTests.cs` is now **FULLY RETIRED**
— every stub landed in a real suite (the buffered cases in `ExternalDataFileCommitTests`,
`BufferedSchemaSeamTests`, `IdentityTransactionSeamsTests`, `BufferedTransactionTests`); the file is a
comments-only audit trail mapping each former stub to its live home.

**Deletion-vector protocol declaration — FIXED (opt-in), and the interop reality measured.** The gap (A)'s
interop test surfaced — EW wrote DVs without declaring the `deletionVectors` reader feature, so a conformant
reader silently returned the deleted rows — is closed. Deletion vectors are now **opt-in**:
`CreateAsync(..., enableDeletionVectors: true)` sets `delta.enableDeletionVectors` and declares the
reader+writer feature (new `DeletionVectorConfig`). A partial DELETE soft-deletes with a DV only when
enabled; with DVs off it removes WHOLE files (metadata-only, no DV) and **throws** on a partial match rather
than write a non-conformant vector. Deliberate "for now" gaps (per the user): no enable-on-existing-table,
no copy-on-write DELETE. **Measured, not assumed:** declaring the feature flipped delta-rs 1.6.2 from
silently-wrong to a **safe refusal** — its reader does not support `deletionVectors` and rejects the table
(`DeltaProtocolError: ... not yet supported`). So the delta-rs test now pins that safe refusal
(`EwUnionedDeletionVector_EwApplies_DeltaRsSafelyRefusesUnsupportedFeature`), and the actual cross-engine
read of the union DV is validated on Spark 4.0 (`SparkInteropTests.EwWritten_UnionedDeletionVector_SparkReadsSurvivingRow`,
tier 3). Brief's cross-engine claim (a) thus holds on a DV-capable engine (Spark); delta-rs will validate it
once it gains DV read support.

## Layer 3 — starting brief (read first for row-level concurrency)

**What it is.** The Databricks *row-level concurrency* extension: two writers touching **disjoint rows of
the same file** should both land instead of the second aborting. Today the second aborts with a file-level
`ConcurrentDeleteDelete` verdict (`ConflictChecker.cs`, the `RemoveFile … plannedRemovePaths.Contains(path)`
case). This is a strict **extension beyond OSS** — OSS Spark and delta-rs *also* conflict at file
granularity here, so the current abort is spec-correct, just not maximally permissive.

**Measure-first, with a Layer-3-specific twist.** The standing "verify against Spark before Layer 3" rule
still holds, but note the trap: OSS Spark will **not** demonstrate the both-land target behavior (it aborts
too), so a naive "does Spark also let them both land?" measurement misleads. The measurable cross-engine
claims are (a) the *resulting* commit — a unioned/remapped deletion vector — is spec-legal and **reads
correctly in Spark 4.0 + delta-rs**, and (b) the row-tracking-through-rewrite artifacts (baseRowId on
rewritten files) match the protocol. Tier-3 setup is in [[spark-interop-toolchain]] (JAVA_HOME / HADOOP_HOME
/ `EW_REQUIRE_SPARK_INTEROP=1`).

**Two sub-problems, very different difficulty — do (A) first:**

- **(A) DELETE/DELETE disjoint rows → deletion-vector union (tractable, no row-tracking needed). DONE.**
  DELETE is DV-based and does **not** rewrite the file, so row positions are stable. Two concurrent deletes
  compute `DV1`/`DV2` against the same base file. tx1 commits `RemoveFile(path) + AddFile(path, DV1)`; on
  rebase tx2 collides (its planned `RemoveFile(path)` matches tx1's, and the base entry it removed is gone).
  Implemented as: `CommitOccAsync` takes `rowLevelDeletes` (the per-file newly-deleted positions recorded by
  `ComputeDeleteActionsAsync` as `DeleteDvEdit`), and on any collision calls `ResolveRowLevelDeletesAsync`,
  which re-stages each edited file as `RemoveFile(path, currentDV) + AddFile(path, currentDV ∪ ourRows)`
  against the *latest* snapshot. Disjoint → both land; overlap (same row) → returns null → abort; file
  rewritten away → returns null → abort (that is (B)'s territory). `ConflictChecker.Check` gained
  `rowLevelResolvedPaths` so the reconciled file's concurrent remove/re-add no longer counts as a conflict.
  (This filled the role the parked tests' `ComputeDeletionVectorActionsAsync(resolveAgainst:)` named, under a
  different shape — a resolver on the commit loop, not a public compute method.) Lives in `DeltaTable.cs`.
  **Now-live tests** (in `RowLevelConcurrencyTests`, not the parked file): `ConcurrentDeletes_SameFile_DisjointRows_BothLand`,
  `ConcurrentDeletes_SameRow_RowLevelConflict`, `WithoutRowLevelRetry_VersionConflictSurfaces` (a
  rewritten-away file — a conflict the row-level path deliberately does NOT silence).

- **(B) Rewrite-preservation (UPDATE, compaction remap). DONE (2026-07-20, step 7 / Milestone 3).** A losing
  DELETE whose target file was concurrently compacted/UPDATE-rewritten is remapped by stable row id onto the
  new file (`DeltaTable.RemapRowLevelDeletesAsync`, called from `ResolveRowLevelDeletesAsync` when a file is
  rewritten away); the row's commit version discriminates concurrent modification; MEASURED on Spark 4.0.1.
  Adapted from pr-4's `RemapRowsAcrossRewriteAsync` but onto master's cleaner M2 read path — the resolved
  stable id/version come straight from `ReadFileAsync`'s out-params (no transient `(fileOrdinal<<40)|pos`
  column needed; a third out-param `strippedAbsPositionsOut` supplies the absolute position). The remaining
  pr-4 payload NOT ported: rebasing a losing UPDATE/append's post-image add (`RebaseDvDmlActionsAsync`'s
  baseRowId re-derivation) — the other half of limitation 2. Original deferral notes kept below for context.

  **(originally, 2026-07-20) Rewrite-preservation (UPDATE, compaction remap).**
  **The full brief is `doc/row-tracking-conformance-brief.md` — read that first.**
  **UPDATE (2026-07-20): row-tracking Milestone 2 LANDED (uncommitted) — the copy-on-write REWRITE now
  PRESERVES ids.** A rewrite (UPDATE / OVERWRITE / compaction) materializes each moved row's original id +
  commit version into the declared hidden columns; the read path strips them and can surface resolved
  ids/versions via out-params (so a second rewrite preserves a first rewrite's ids). MEASURED against Spark 4.0.1
  (`SparkInteropTests.EwUpdated_/EwCompacted_RowTracking_SparkReadsPreservedIds` read the original 0,1,2 back
  through the rewrite) + delta-rs (no column leak). The write gate now refuses a rewrite only when the declared
  materialized names are absent. **What REMAINS for row-level concurrency (the actual Layer 3 (B) payload):**
  the id-based *remap across a rewrite* — `ReadAllWithRowIdsAsync` (stable id = `(fileOrdinal<<40)|absPos`) +
  `RemapRowsAcrossRewriteAsync` inside the OCC retry, using a row's commit version as the concurrent-modification
  discriminator, and relaxing `rebaseSafe:false` for row-tracking deletes (retires limitation 2). Port from pr-4
  (`RebaseDvDmlActionsAsync`). Materialization (M2) is the prerequisite that is now done; the remap is next.
  Below is the ORIGINAL deferral note, kept for context.

  **(originally, 2026-07-20) Rewrite-preservation — DEFERRED behind a write fail-fast.**
  **The full brief is `doc/row-tracking-conformance-brief.md` — read that when building (B).**
  **UPDATE (2026-07-20): row-tracking Milestone 1 LANDED — appends are now writable + spec-conformant.**
  `CreateAsync(..., enableRowTracking: true)` generates/stores the two `materialized*ColumnName` properties
  and declares `rowTracking`+`domainMetadata`; appends assign `baseRowId`/`defaultRowCommitVersion` with NO
  materialized column; `RejectRowTrackingWrite` now fires only for `!isAppend` + compaction. Spark 4.0.1
  reads the appended ids correctly (`SparkInteropTests.EwAppended_RowTracking_SparkReadsBaseRowIdPositionIds`).
  (B) itself — the copy-on-write rewrite preservation — is still deferred; the write fail-fast still guards it.
  **CORRECTION (2026-07-20): this is a PORT + VALIDATE, not greenfield.** The `pr-4` branch ALREADY implements
  row-tracking-through-rewrite (the `RowTrackingWriter` materialization overloads, the `materializeIds` UPDATE/
  DELETE/compaction paths keyed off `delta.rowTracking.materializedRowIdColumnName`, and the remap —
  `RemapRowsAcrossRewriteAsync` inside `RebaseDvDmlActionsAsync`, its "v2"; "v1" == the DV re-union already
  landed as Layer 3 (A)). pr-4's own tests are green. **Master lacks it only because the write-path refactor
  was landed refactor-only (`808b944`), stripping the buffered-transaction/logical-rebase machinery — and the
  row-tracking materialization went out with it.** So master's `ComputeUpdateActionsAsync` strips the row-id
  column and builds an `AddFile` with **no `BaseRowId`/`DefaultRowCommitVersion`** (loses ids) — that is a
  *landing artifact*, not pr-4's design. Two gaps remain even after a port: pr-4 **consumes** the
  materialized-column metadata but never **generates** it (no `CreateAsync` enablement — handles a foreign
  row-tracking table, can't create one), and pr-4 has **no interop coverage** (spec-conformance unproven). The
  only way to reach master's write path is opening a foreign (Spark/Databricks) row-tracking table, exactly
  where writing wrong would corrupt real invariants.
  **Decision (user):** rather than ship a half-built feature, make row tracking **READ-ONLY** — refuse any
  data-changing write to a `delta.enableRowTracking=true` table (`DeltaTable.RejectRowTrackingWrite`, gating
  `ValidateWritable` + `CompactAsync`, `NotSupportedException`). Reads are unaffected. This is strictly safer
  than the prior silent corruption. `RowTrackingTests`/`RowTrackingHighWaterMarkTests` reworked: write/compact
  paths assert the refusal; the read-side HWM-reconciliation test is preserved by seeding the log directly
  (`SeedTwoFileTableAsync`).
  **When (B) is actually built** (per the brief), in order: (1) port pr-4's rewrite-writer + remap onto
  master's `CommitOccAsync`/`ResolveRowLevelDeletesAsync`; (2) ADD a `CreateAsync` enablement that generates
  the materialized-column-name metadata (pr-4 lacks this); (3) tier-3 Spark/delta-rs validation of
  `baseRowId`/materialized artifacts (pr-4 lacks this — highest risk); (4) relax `rebaseSafe` for row-tracking
  deletes and lift the gate. Only then un-skip
  `ConcurrentUpdateAndDelete_DisjointRows_BothLand`, `DeleteThroughConcurrentCompaction_Remapped`,
  `DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict`. Closing (B) also retires **limitation 2**
  (`rebaseSafe: false` for row tracking).

**The 7 acceptance tests.** SIX are now LIVE in `RowLevelConcurrencyTests` (master-idiomatic, via the public
`DeleteAsync`/`UpdateAsync`/`CompactAsync` API + racing handles): the (A) set —
`ConcurrentDeletes_SameFile_DisjointRows_BothLand`, `ConcurrentDeletes_SameRow_RowLevelConflict`,
`WithoutRowLevelRetry_VersionConflictSurfaces` — plus the (B) set (step 7) —
`ConcurrentUpdateAndDelete_DisjointRows_BothLand`, `DeleteThroughConcurrentCompaction_Remapped`,
`DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict`. Only
`BufferedFlow_ComputeThenRebaseThenCommit_ComposesWithConcurrentDelete` remains parked (now
`[Fact(Skip = BufferedTxn)]` — it needs the buffered-transaction seam, not row-level concurrency). The
`RowLevelConcurrency` skip-reason const was retired.

**Entry points.** For (A) as built: `DeltaTable.ResolveRowLevelDeletesAsync` (the DV-union resolver — where
same-file-disjoint-DV is split from same-row, and where a rewritten-away file falls out to abort — this is
the natural hook to extend for (B)); `DeltaTable.CommitOccAsync` (`rowLevelDeletes` param + the catch/retry
that calls the resolver before the checker); `ComputeDeleteActionsAsync` (records `DeleteDvEdit` per file);
`Concurrency/ConflictChecker.cs` (`rowLevelResolvedPaths` skips a reconciled file). For (B):
`ComputeUpdateActionsAsync` (the bare-AddFile rewrite that drops baseRowId — the gap); `RowTracking.RowTrackingWriter`
(`AddRowIdColumn`/`StripRowIdColumn`); `DeletionVectors/DeletionVectorReader`+`Writer` (DV read/union).

## Measure, don't assume

This effort repeatedly found that reasoning about another implementation's behaviour was wrong, and
measuring corrected it. During slice 9 specifically: the VACUUM landing-notes plan said to protect
unexpired tombstones — Spark measurement showed it must not. Before starting layer 3, **verify against
Spark** (tier 3) rather than trust the PR notes or these notes. The interop tiers make measuring cheap;
use them.

**Step 3's cross-engine posture (measured).** Step 3 deliberately introduces **no new on-disk artifact**: a
rebased commit is byte-identical to a sequential one (same add/remove actions, a higher version number). The
only new *policy* is the blind-append conflict rule, which lives in the step-1 `ConflictChecker` (modeled on
Spark's own) and is unit-tested. The claim that a rebased commit reads correctly in a foreign engine was
**measured, not assumed**: `SparkInteropTests.EwConcurrentAppends_Rebased_SparkReadsAllRows` drives two racing
EW handles so the second commit rebases through `CommitOccAsync`, then has Spark 4.0.1 / delta-spark 4.0.0 read
the table — all rows present, versions consecutive. The full tier-3 suite (13) and delta-rs tier (9) also pass
against the rewired write path. Running it needs `JAVA_HOME` (JDK 17) + `HADOOP_HOME` (winutils) + `EW_REQUIRE_SPARK_INTEROP=1`.

## Running the tests

- Concurrency unit + integration tests are pure/local — no external toolchain:
  `dotnet test test/EngineeredWood.DeltaLake.Table.Tests -f net10.0 --filter "FullyQualifiedName~ConflictCheckerTests|FullyQualifiedName~DeltaTransactionTests"`
- Full validation uses the Delta interop tiers (delta-rs + PySpark). Setup and the
  `EW_REQUIRE_DELTA_INTEROP` / `EW_REQUIRE_SPARK_INTEROP` CI flags are in `doc/running-tests.md`. Tier 3
  needs `JAVA_HOME` (JDK 17+) and, on Windows, `HADOOP_HOME` with winutils on `PATH`.
- The Spark tier is slow (~25s); running the whole `DeltaLake.Table` suite twice across TFMs can exceed
  a 10-minute command budget. Filter out `SparkInteropTests` when iterating on non-interop code.

## Broader context

This slice 9 work is one thread of the larger PR #4 landing + Delta interop-hardening effort tracked in
`doc/upstream-landing-notes.md`. That doc also lists the unrelated remaining items (the ORC/Avro/Iceberg
sections of `doc/known-issues.md` were never verified against code; small interop divergences).
