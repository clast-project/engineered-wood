# Row tracking: brief for landing a spec-conformant writer (port + validate)

**Status: deferred, and read-only on master.** Any data-changing write to a `delta.enableRowTracking=true`
table is refused on master (`DeltaTable.RejectRowTrackingWrite`). This document captures everything learned
while making that call, so a future session can land a correct writer without re-deriving it. It is also the
**prerequisite for Layer 3 (B)** (row-level concurrency across rewrites) — see `doc/slice9-concurrency-resume.md`.

**IMPORTANT (corrected 2026-07-20): this is PORT + VALIDATE, not greenfield.** The `pr-4` branch (local:
`git show pr-4:…`) **already implements** row-tracking-through-rewrite and both row-level-concurrency
mechanisms. Master lacks it only because the write-path refactor was landed *refactor-only* (`808b944`),
which stripped the buffered-transaction / logical-rebase machinery — and the row-tracking materialization
went out with it. So the work is: **port pr-4's machinery onto master's write path, then add the
cross-engine (Spark) validation pr-4 never had.** See "What pr-4 already implements" below.

Then **measure against Spark before trusting any spec detail below** — the whole slice-9 effort repeatedly
found that reasoning about other implementations was wrong and only measurement corrected it, and row
tracking has *zero* interop coverage in either master or pr-4, so **nothing here has been validated
cross-engine** — including pr-4's implementation.

## TL;DR

- **Master's** row tracking is broken on rewrite (strips the row-id column, drops `baseRowId`) and is refused
  for writes. Reads stay fine — `baseRowId` is just log metadata. Refusing is strictly safer than the prior
  behavior, which **silently corrupted** foreign row-tracking tables.
- **pr-4** is not the broken version: it materializes each survivor's original id + commit version through
  UPDATE / copy-on-write DELETE / compaction, keyed off the spec `delta.rowTracking.materializedRowIdColumnName`
  metadata, and remaps DML across rewrites by stable row id (`RemapRowsAcrossRewriteAsync`). Its own tests are
  green.
- **Two real gaps remain even in pr-4**: (a) it *consumes* the materialized-column metadata but does not
  *generate* it — there is no `CreateAsync` enablement, so it handles writing to a FOREIGN (Spark-created)
  row-tracking table but cannot create a spec-conformant one; (b) **no interop validation** — spec-conformance
  is unproven, not proven, and this effort's history says unproven cross-engine assumptions are usually wrong.
- So the landing plan is: port pr-4's writer + remap, add a `CreateAsync` enablement (generate the metadata),
  and validate on Spark/delta-rs tier 3 — then lift the gate.

## Why it's read-only right now

`RejectRowTrackingWrite(snapshot)` (in `DeltaTable.cs`) throws `NotSupportedException` when
`RowTrackingConfig.IsEnabled(config)`. It gates:
- `ValidateWritable` — covers append / overwrite / delete / update (every data write funnels through it), and
- `CompactAsync` — a separate entry point that does not call `ValidateWritable`.

Reads are untouched, and the `delta.rowTracking` high-water mark is still reconciled on read. There is **no
`CreateAsync` surface to enable row tracking**, so the only way to reach the write path at all is opening a
table a *foreign* engine (Spark/Databricks) created — which is exactly where writing wrong corrupts real
invariants. Lifting the gate is the last step of the work below.

## Current EW state on MASTER (precise)

This section is **master**, which is the broken/refused version. What pr-4 has instead is the next section.

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

## What pr-4 already implements (the real starting point)

Inspect with `git show pr-4:<path>` / `git grep … pr-4`. pr-4's branch is green (its own tests pass); it was
never taken onto master because it is entangled with the buffered-transaction + `IDataFileRewriter` machinery
that the write-path refactor deliberately stripped.

- **Writer** (`pr-4:.../RowTracking/RowTrackingWriter.cs`): adds `RowCommitVersionColumn =
  "__delta_row_commit_version"` and the materialization overloads master lacks — `AddRowIdColumn(batch,
  Int64Array rowIds)` (materialize explicit original ids on a CoW rewrite) and
  `AddRowIdAndCommitVersionColumns(batch, rowIds, commitVersions, nullable)` (both columns for compaction,
  where rows from several source files mix so a single `baseRowId`/`defaultRowCommitVersion` can't represent
  them). The `nullable` form handles a source predating row tracking (per-row NULL → reader falls back to the
  new file's `baseRowId + position`).
- **DML rewrite paths** (`pr-4:.../DeltaTable.cs`): a `materializeIds` flag gated on the config key
  `delta.rowTracking.materializedRowIdColumnName` drives UPDATE (`UpdateByRowIdsAsync`, ~2042), copy-on-write
  DELETE (`DeleteByRowIdsAsync` ~1592, `DeleteByRowIdsViaVectorsAsync` ~1872), and the append path (~2104,
  ~2372). Fresh `baseRowId`/`defaultRowCommitVersion` still go on the new `add`; survivors carry their
  ORIGINAL id/version in the materialized columns. `__delta_row_id` is an internal working name reconciled to
  the configured physical name.
- **Row-level concurrency** (the Layer 3 payload): `ReadAllWithRowIdsAsync` (~2987) exposes stable ids with
  the encoding `(fileOrdinal << 40) | absolutePositionInFile`; `RebaseDvDmlActionsAsync` (~4195) is the
  rebase, which calls `RemapRowsAcrossRewriteAsync` (~4369) — relocate rows by stable id (materialized, else
  `baseRowId + position`), using the row's COMMIT VERSION as the concurrent-modification discriminator
  (relocated-untouched keeps its version; a concurrently updated/deleted row conflicts). pr-4's own
  `RowLevelConcurrencyTests` describes both mechanisms: **v1** DV re-union (== the Layer 3 (A) already landed
  on master) and **v2** remap-across-rewrite (== Layer 3 (B)). Its note: *"Databricks' own row-level
  concurrency still conflicts with compaction — the remap goes beyond it."*
- **Compaction** (`pr-4:.../Compaction/CompactionExecutor.cs`): reads the materialized-column name (~93) and
  carries original ids/versions through the merge.
- **Entangled with the codec seam**: the rewrite paths have a `nativeRewrite` branch that delegates to
  `IDataFileRewriter.ReadRewriteAsync(…, RowTrackingRewrite)` (`pr-4:.../IDataFileRewriter.cs`). The built-in
  (non-native) path is independent, so the port need not take the seam — but be aware the two are interwoven
  in pr-4's source.

**What pr-4 does NOT do** (the two gaps that remain after a port): it *consumes* the
`materializedRowIdColumnName` metadata but never *generates* it (no `CreateAsync` enablement — the tests
hand-configure it to `__delta_row_id`; a Spark table supplies its own UUID-based name), and it has **no
Spark/delta-rs interop test** — so "spec-conformant" is unproven.

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

## Implementation plan (ordered) — port pr-4, then validate

The gap table's "fix" column describes the destination; pr-4 already implements most of it (gaps 1-partial,
4, 5, and the materialization). The order below is what to PORT and, critically, what to ADD (enablement +
validation) that pr-4 lacks.

1. **Port pr-4's row-tracking-through-rewrite writer.** Gaps 4, 5, and the materialization half of 1. Bring
   the `RowTrackingWriter` overloads + the `materializeIds` paths (UPDATE / CoW DELETE / compaction) onto
   master's write path. Note pr-4 **materializes always** (uniform, proven by its tests) rather than the
   "preserve row order + `baseRowId`, no materialized column" *alternative* the gap table's row 4 sketches —
   the order-preservation trick avoids the materialized column but is unproven and fragile (a pre-existing DV
   shifts positions); prefer porting pr-4's materialize-always unless there's a reason not to.
2. **Add `CreateAsync` enablement — pr-4 does NOT have this.** Gaps 2, 6. Generate UUID materialized-column
   physical names, store them in `delta.rowTracking.materializedRowIdColumnName` /
   `…materializedRowCommitVersionColumnName`, declare `rowTracking`+`domainMetadata`, stamp field IDs under
   column mapping. Without this EW can only preserve ids on a table a foreign engine already set up.
3. **Stop writing the spurious column for default-id appends** (gap 1, if pr-4 still does — verify): a fresh
   append needs only `baseRowId`, no materialized column.
4. **VALIDATE on Spark/delta-rs tier 3 — pr-4 never did this, and it is the highest-risk step.** Gap 7. Both
   directions: EW writes → Spark reads ids/versions correctly (incl. after a rewrite); Spark writes a
   row-tracking table → EW reads and preserves ids. This effort's history says unproven cross-engine
   assumptions are usually wrong; do not lift the gate until this is green.
5. **Port the remap + row-level concurrency (Layer 3 B).** See next section.
6. **Read-side row IDs (optional).** Gap 3.
7. **Lift the gate.** Gap 8. Remove `RejectRowTrackingWrite`.

## Relationship to Layer 3 (B) — row-level concurrency across rewrites

**pr-4 already implements this** (`RemapRowsAcrossRewriteAsync` inside `RebaseDvDmlActionsAsync`); the port is
onto master's `CommitOccAsync` / `ResolveRowLevelDeletesAsync` (Layer 3 (A), which == pr-4's "v1" DV
re-union). What pr-4's "v2" adds on top:

- **Record row IDs, not just positions.** Master's `DeleteDvEdit` records absolute positions (stable across a
  DV swap — that is what makes (A) work). (B) also needs the **row IDs** so a rewrite that relocates rows can
  be followed; pr-4 exposes them via `ReadAllWithRowIdsAsync` (`(fileOrdinal << 40) | absolutePosition`).
- **Remap on a delete/rewrite collision** (pr-4's `RemapRowsAcrossRewriteAsync`): the target file is gone,
  replaced by successor file(s); relocate each deleted row ID by the successor's materialized column (else
  `baseRowId + position`), and use the row's **commit version** as the concurrent-modification discriminator
  — a relocated-untouched row keeps its version and both land; a concurrently updated/deleted row conflicts.
- **Relax `rebaseSafe: false` for row-tracking deletes** (retires limitation 2): correct once rewrites
  preserve ids.

Un-skips (all `[Fact(Skip = RowLevelConcurrency)]` in `PendingCoverageTests.cs`), once the rewrite-writer
port (plan step 1) and the remap port (step 5) land: `ConcurrentUpdateAndDelete_DisjointRows_BothLand`,
`DeleteThroughConcurrentCompaction_Remapped`, `DeleteThroughCompaction_RowConcurrentlyDeleted_RowLevelConflict`.
pr-4's own `RowLevelConcurrencyTests` is the reference for what these should assert.

## Interop validation (measure, don't assume)

Nothing about EW row tracking has been checked cross-engine — **including pr-4's implementation** (it has no
interop tests either). Before trusting the spec details above OR pr-4's port:
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

**pr-4 (the source to port from — `git show pr-4:<path>`):**
- `RowTracking/RowTrackingWriter.cs` — the materialization overloads (`AddRowIdColumn(batch, Int64Array)`,
  `AddRowIdAndCommitVersionColumns`) + `RowCommitVersionColumn`.
- `DeltaTable.cs` — `DeleteByRowIdsAsync` (~1592), `DeleteByRowIdsViaVectorsAsync` (~1872),
  `UpdateByRowIdsAsync` (~2042), `ReadAllWithRowIdsAsync` (~2987), `RebaseDvDmlActionsAsync` (~4195),
  `RemapRowsAcrossRewriteAsync` (~4369); the `materializeIds` flag + append materialization (~2104, ~2372).
- `Compaction/CompactionExecutor.cs` (~93) — compaction id/version materialization.
- `IDataFileRewriter.cs` — `RowTrackingRewrite` + `ReadRewriteAsync` (the native path the built-in port can ignore).
- `test/…/RowLevelConcurrencyTests.cs` — the reference for the two mechanisms and the required config.

**master (the destination):**
- `src/EngineeredWood.DeltaLake/RowTracking/RowTrackingConfig.cs` — property/domain/HWM (read-side keep).
- `src/EngineeredWood.DeltaLake.Table/RowTracking/RowTrackingWriter.cs` — master's positional-only writer.
- `DeltaTable.ComputeWriteActionsAsync` (~2196), `ComputeUpdateActionsAsync` (~1596), `ReadFileAsync` (~2777),
  `RejectRowTrackingWrite`, `CreateAsync`, `CommitOccAsync` / `ResolveRowLevelDeletesAsync` (Layer 3 (A) hook).
- `Compaction/CompactionExecutor.cs`, `ProtocolVersions.cs`.
- Tests: `RowTrackingTests.cs`, `RowTrackingHighWaterMarkTests.cs` (currently assert the read-only refusal +
  read-side HWM reconciliation), `PendingCoverageTests.cs` (the parked (B) stubs), `Interop/SparkInteropTests.cs`.
