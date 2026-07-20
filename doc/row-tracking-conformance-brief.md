# Row tracking: implementation brief for a spec-conformant writer

**Status: deferred.** Row tracking is currently **read-only** in EngineeredWood — any data-changing write to
a `delta.enableRowTracking=true` table is refused (`DeltaTable.RejectRowTrackingWrite`). This document
captures everything learned while making that call, so a future session can build a correct writer without
re-deriving it. It is also the **prerequisite for Layer 3 (B)** (row-level concurrency across rewrites) —
see `doc/slice9-concurrency-resume.md`.

Read this first. Then **measure against Spark before trusting any spec detail below** — the whole slice-9
effort repeatedly found that reasoning about other implementations was wrong and only measurement corrected
it, and row tracking has *zero* interop coverage today, so nothing here has been validated cross-engine.

## TL;DR

- EW's row tracking today is **EW-internal and not spec-conformant**, and its rewrite paths are broken. It
  was never cross-validated against Spark.
- Rather than ship that, writes are refused (read stays fine — `baseRowId` is just log metadata). This is
  strictly safer than the prior behavior, which **silently corrupted** foreign row-tracking tables.
- A correct writer is a real project: spec materialized-column naming, stop writing a bogus column, expose
  row IDs on read, preserve IDs through UPDATE/compaction rewrites, and validate on Spark tier 3.
- The tractable/hard split: **UPDATE preservation is cheap** (preserve row order + `baseRowId`, no
  materialized column). **Compaction preservation is the hard part** (multi-file merge genuinely needs a
  materialized column).

## Why it's read-only right now

`RejectRowTrackingWrite(snapshot)` (in `DeltaTable.cs`) throws `NotSupportedException` when
`RowTrackingConfig.IsEnabled(config)`. It gates:
- `ValidateWritable` — covers append / overwrite / delete / update (every data write funnels through it), and
- `CompactAsync` — a separate entry point that does not call `ValidateWritable`.

Reads are untouched, and the `delta.rowTracking` high-water mark is still reconciled on read. There is **no
`CreateAsync` surface to enable row tracking**, so the only way to reach the write path at all is opening a
table a *foreign* engine (Spark/Databricks) created — which is exactly where writing wrong corrupts real
invariants. Lifting the gate is the last step of the work below.

## Current EW state (precise)

Config + helpers — `src/EngineeredWood.DeltaLake/RowTracking/RowTrackingConfig.cs`:
- `EnableKey = "delta.enableRowTracking"`, `DomainName = "delta.rowTracking"`.
- `BuildHighWaterMarkAction(nextAvailableRowId)` → domainMetadata `{"rowIdHighWaterMark": next-1}` (stores
  the **highest assigned** id; EW's internal counter is the **next** id). `TryReadHighWaterMark`,
  `ComputeHighWaterMark(activeFiles)` (derives from `baseRowId + estimatedRowCount`, estimate from
  `stats.numRecords`). Reconciled into `Snapshot.RowIdHighWaterMark` at snapshot-build time; the domain HWM
  holds the line when the highest-id file leaves the active set (so ids are never reassigned). This read-side
  reconciliation is correct and worth keeping.
- **Dead constants**: `RowIdColumnName`/`VirtualRowIdColumn = "_metadata.row_id"` and
  `VirtualRowCommitVersionColumn` are defined but **never used** — EW exposes no row IDs to readers.

Writer helper — `src/EngineeredWood.DeltaLake.Table/RowTracking/RowTrackingWriter.cs`:
- `RowIdColumn = "__delta_row_id"` — a **hardcoded, non-spec** physical name.
- `AddRowIdColumn(batch, baseRowId)` appends `__delta_row_id = baseRowId + i`. `StripRowIdColumn`,
  `GetOrGenerateRowIds`, `BuildCommitVersionArray` (commit-version column, unused end-to-end).

Write path — `ComputeWriteActionsAsync` (`DeltaTable.cs`, ~line 2196+):
- When `rowTrackingEnabled`: `AddRowIdColumn(physicalBatch, fileBaseRowId)`, then **writes the batch
  INCLUDING `__delta_row_id` into the parquet**, sets `AddFile.BaseRowId = fileBaseRowId` and
  `DefaultRowCommitVersion = newVersion`, advances the counter, and emits the `delta.rowTracking` HWM
  domainMetadata.

Read path — `ReadFileAsync` (`DeltaTable.cs`, ~line 2777): `StripRowIdColumn(result)` drops `__delta_row_id`
after reading. So EW writes a spurious column and hides it again; a foreign reader sees an undeclared
physical column.

UPDATE — `ComputeUpdateActionsAsync` (`DeltaTable.cs`, ~1695 / ~1742): **strips** the row-id column and
builds `new AddFile { … }` with **no `BaseRowId`/`DefaultRowCommitVersion`**, and **reorders** rows (matched
rows first, then kept). A copy-on-write rewrite therefore loses row identity entirely.

Compaction — `Compaction/CompactionExecutor.cs` (~110 / ~306 / ~343): assigns **fresh** `baseRowId`s and
emits the HWM, but does **not** carry each surviving row's original id — weak "preservation" only.

Protocol — `ProtocolVersions.cs`: `rowTracking` and `domainMetadata` are in `SupportedWriterFeatures` (not
reader features). A table listing `rowTracking` in `readerFeatures` is still rejected by
`ValidateReadSupport` (see `doc/known-issues.md`, "rowTracking read-side classification").

**No interop test exists** for row tracking, in any direction.

## What the Delta spec requires (verify before relying)

Row tracking is a **writer feature** (`rowTracking`) that depends on the `domainMetadata` writer feature.
Enabled by table property `delta.enableRowTracking=true`. Every row has a stable **row ID** and a **row
commit version**, each carried in one of two ways:

1. **Default (fresh) values — no column.** `add.baseRowId` + physical position gives the row ID
   (`rowId = baseRowId + positionInFile`); `add.defaultRowCommitVersion` gives the commit version. A
   freshly-appended file needs **only** these two `add` fields — **no materialized column**. (Verify how
   position interacts with a deletion vector — physical position in the file, DV does not renumber.)

2. **Materialized values — a hidden column.** When a row's ID/version can't be derived from position (it was
   *moved* by a rewrite), it is stored per-row in a hidden physical column. The column **physical names are
   stored in table metadata**: `delta.rowTracking.materializedRowIdColumnName` and
   `delta.rowTracking.materializedRowCommitVersionColumnName`. Names are UUID-based to avoid colliding with
   user columns; under column mapping they carry field IDs. A non-null materialized value **overrides** the
   default for that row.

High-water mark: `delta.rowTracking` domainMetadata `{"rowIdHighWaterMark": highestAssignedId}` — already
emitted/reconciled by EW. Reader exposure: the generated columns `_metadata.row_id` /
`_metadata.row_commit_version` (EW has the constant names but never populates them).

## Gap analysis (current → conformant)

| # | Gap | Fix |
|---|---|---|
| 1 | Writes a hardcoded non-spec `__delta_row_id` column, even for default-id files that need none | Stop writing it for fresh appends; rely on `baseRowId` + position. Only ever write a **materialized** column, under its metadata-declared name, when ids are non-derivable (rewrites). |
| 2 | No `materializedRowIdColumnName` / `…CommitVersionColumnName` metadata; no field IDs | Assign UUID physical names at enablement, store in metadata; stamp field IDs under column mapping. |
| 3 | Reader exposes no row IDs | Populate `_metadata.row_id` (= `baseRowId + pos`, overridden by the materialized column) if/when readers should see them. Not strictly required to *write* correctly, but needed for read-side row-id features. |
| 4 | UPDATE strips ids, drops `baseRowId`, reorders rows | Preserve row **order**, keep all rows, propagate `baseRowId`/`defaultRowCommitVersion` (see "cheap path"). |
| 5 | Compaction re-assigns ids instead of preserving | Write a materialized column carrying each surviving row's **original** id (the hard path). |
| 6 | No `CreateAsync` enablement | Add `enableRowTracking: true` → set property + declare `rowTracking` + `domainMetadata` writer features + seed materialized-column-name metadata. |
| 7 | No interop coverage | Tier-3 Spark tests both directions (EW writes → Spark reads ids; Spark writes → EW reads/preserves ids). |
| 8 | The write gate | Remove `RejectRowTrackingWrite` once the above hold. |

## Implementation plan (ordered)

1. **Spec-conformant foundation (do first).** Gaps 1, 2, 6. Enablement path + metadata + stop writing the
   bogus column. Freshly-appended files become conformant (baseRowId only). **Validate with Spark tier 3**
   that Spark reads EW-appended row IDs correctly — this is claim (b) and must hold before going further.
2. **UPDATE preservation — the cheap path.** Gap 4. An UPDATE keeps every row and can preserve order, so if
   the rewritten file keeps the **source's `baseRowId`**, every row's default id (`base + pos`) is preserved
   with **no materialized column**. Concretely: don't reorder (apply the updater in place, keep matched and
   unmatched rows in original position), set `AddFile.BaseRowId = source.BaseRowId` and propagate
   `DefaultRowCommitVersion`. Caveat: a source with a pre-existing DV shifts positions — handle or reject.
3. **Compaction preservation — the hard path.** Gap 5. Multi-file merge: rows come from different source
   `baseRowId`s, so a single `baseRowId` can't cover them → write a **materialized row-id column** holding
   each surviving row's original id. Same for commit versions.
4. **Read-side row IDs (optional).** Gap 3, if row-id read exposure is wanted.
5. **Lift the gate + remove dead code.** Gap 8. Delete `RejectRowTrackingWrite`; retire the now-unnecessary
   `__delta_row_id` machinery in `RowTrackingWriter`.

## Relationship to Layer 3 (B) — row-level concurrency across rewrites

Once rewrites preserve ids, Layer 3 (B) becomes buildable on top of the (A) resolver
(`DeltaTable.ResolveRowLevelDeletesAsync`):

- **DELETE already records positions** (`DeleteDvEdit`) and DELETE preserves `baseRowId` (its `AddFile` is
  `addFile with { DeletionVector }`). On a row-tracking table, additionally record the **row IDs** deleted
  (`id = baseRowId + pos` at read time).
- **Remap on a delete/rewrite collision:** the delete's target file is gone, replaced by successor file(s).
  Find where each deleted **row ID** now lives — for the UPDATE cheap path, the successor has the same
  `baseRowId`, so position is unchanged and remap is just "redirect the DV from the old path to the file now
  covering that `baseRowId` range"; for compaction, read the materialized column to find each id's new
  position. Build a DV against the successor. **Overlap** (the row was also concurrently deleted) → genuine
  row-level conflict.
- **Relax `rebaseSafe: false` for row-tracking deletes** (retires limitation 2): a rebased delete's `AddFile`
  carries the concurrent file's `baseRowId`, which is correct once rewrites preserve ids.

Un-skips (all `[Fact(Skip = RowLevelConcurrency)]` in `PendingCoverageTests.cs`):
`ConcurrentUpdateAndDelete_DisjointRows_BothLand` (needs step 2),
`DeleteThroughConcurrentCompaction_Remapped` + `DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict`
(need step 3).

## Interop validation (measure, don't assume)

Nothing about EW row tracking has been checked cross-engine. Before trusting the spec details above:
- **Spark 4.0 supports row tracking** — use it as the oracle. Setup: `JAVA_HOME` (JDK 17) + `HADOOP_HOME`
  (winutils) + `EW_REQUIRE_SPARK_INTEROP=1` (see `reference_spark_interop_toolchain` memory / `doc/running-tests.md`).
  Add tests to `test/EngineeredWood.DeltaLake.Table.Tests/Interop/SparkInteropTests.cs`, modeled on the DV
  ones. Key claims to measure: (a) Spark reads EW-appended `baseRowId` ids correctly; (b) after an
  EW rewrite, ids Spark reads match the originals; (c) EW reads Spark-written row-tracking tables
  (materialized columns, non-default ids) correctly.
- **delta-rs**: measure whether/which version reads row tracking. Recall delta-rs 1.6.2 *refuses* deletion
  vectors (safe), so it may refuse or ignore row tracking too — do not assume, check, and pin the observed
  behavior like the DV tests do.

## Entry points

- `src/EngineeredWood.DeltaLake/RowTracking/RowTrackingConfig.cs` — property/domain/HWM (read-side keep).
- `src/EngineeredWood.DeltaLake.Table/RowTracking/RowTrackingWriter.cs` — the `__delta_row_id` machinery to
  replace with a spec materialized column.
- `DeltaTable.ComputeWriteActionsAsync` (~2196), `ComputeUpdateActionsAsync` (~1596), `ReadFileAsync` (~2777),
  `RejectRowTrackingWrite`, `CreateAsync`.
- `Compaction/CompactionExecutor.cs` — compaction rewrite.
- `ProtocolVersions.cs` — feature classification.
- Tests: `RowTrackingTests.cs`, `RowTrackingHighWaterMarkTests.cs` (currently assert the read-only refusal +
  read-side HWM reconciliation), `PendingCoverageTests.cs` (the parked (B) stubs), `Interop/SparkInteropTests.cs`.
