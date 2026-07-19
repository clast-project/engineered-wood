# Landing cmettler PR #4 — progress & remaining work

Working notes for the effort to land Christoph Mettler's PR #4
(`https://github.com/CurtHagenlocher/engineered-wood/pull/4`, branch `pr-4`, 55 commits,
~9.7k additions) onto `master` **slice by slice**. The PR's own review decomposition is in
`doc/upstream-candidates.md` on the `pr-4` branch (9 slices). Overall the PR is high quality —
mostly spec-compliance / interop bug fixes validated against Spark 4.x, delta-kernel-rs and DuckDB;
the full `pr-4` branch is green (DeltaLake 168, Table 182, Parquet 585/585).

**Slices are a review lens, not commit boundaries.** The 55 commits interleave themes, so most
slices are landed by reconstructing the net diff of the relevant files (or hand-porting a
cross-cutting thread) and committing as one focused change — not a clean cherry-pick. Commits are
authored as Christoph Mettler with a `Co-Authored-By: Claude` trailer and a body note where the
change diverges from the original PR.

## Landed on master (as of 2026-07-18)

| Slice | Commit(s) | Notes |
|---|---|---|
| 1 — Parquet writer correctness | `ced8d34`, `a742796` | Verbatim cherry-picks (null-struct alignment, all-null page → PLAIN, ExpandArray widths, VariantType.specification_version, ns-timestamp, deprecated min/max). |
| 2 — Spec DV serialization | `c4a5a07` | 64-bit RoaringBitmapArray + on-disk `.bin` framing. **Both legacy fallbacks removed** (library never shipped → no legacy EW DVs; fallbacks were fragile/OOM-prone). |
| 3 — Spec checkpoint content + tombstone DVs | `b41f5ad` | Preserve add.deletionVector/baseRowId/features/config, NULLABLE action structs, retained tombstones. **Added `remove.deletionVector` round-trip** beyond the PR. Row-tracking HWM reconciliation deliberately excluded (deferred to slice 5). |
| 4 — Spec path encoding (`DeltaPath`) | `74bc1aa` | Hive-escaped partition dirs + URL-encoded `add.path`, decoded at every read site + vacuum. Hand-ported cross-cutting wiring. **See non-ASCII follow-up below.** |
| 5 — writer features + protocol declaration | `cc8d6fe`, `c1b1474`, `70d2384`, `8e3fa8d` | `cc8d6fe`: ToArrowField preserves field metadata (PR #21, prerequisite). `c1b1474`: allowlist appendOnly/invariants/checkConstraints/generatedColumns + `HonorWriterFeatures` on Write/Delete/Update. `70d2384`: declare schema-driven features (timestampNtz/identityColumns/columnMapping-in-v7) at `CreateAsync` + protocol upgrade on `AddColumnAsync`. `8e3fa8d`: `delta.rowTracking` HWM emission + `SnapshotBuilder` reconciliation. **Did NOT allowlist variantType/rowTracking** as supported features (need later slices). |
| 6 — column mapping | `aa3f0e2`, `7a327f0`, `4754a72` | `aa3f0e2`: physical names in BOTH modes + new `ColumnMappingRecursive.ToPhysical/ToLogical` (nested struct children) + numeric `delta.columnMapping.id`. `7a327f0`: compaction re-stamps field ids & widens against the physical-named target schema. `4754a72`: `AddColumnAsync`/`RenameColumnAsync`/`DropColumnAsync` metadata-only + `SchemaEvolution.BackfillMissingColumns` read-path reconcile. **See slice-6 leftovers below.** |

Verification standard for each: builds on net10.0/net8.0/netstandard2.0, Delta suites green on both
TFMs. GOTCHA: `parquet-testing` is a git submodule — worktrees don't auto-populate it, and without it
~98 Parquet reader tests spuriously fail (`git submodule update --init parquet-testing`, or test in
the main checkout).

## The inflection point (slice 5 onward)

Slices 1–4 were cleanly separable from the PR's central `DeltaTable.cs` write-path refactor
(`WriteCoreAsync` + the commit seams). Slice 5 onward weave *into* that refactor and into each other —
but slices 5 and 6 were nonetheless landed piecemeal without taking the refactor, by hand-porting each
thread onto master's existing write path. Slice 5 is now **complete**; the three pieces that were
deferred as refactor-/slice-6-coupled all landed once slice 6 supplied `AddColumnAsync`:

- ~~Create-time feature declaration~~ — `70d2384`. `variantType` omitted (no variant support on master;
  the `RequiredSchemaFeatures` hook is in place for when there is).
- ~~Row-tracking HWM emission + `SnapshotBuilder` reconciliation~~ — `8e3fa8d`.
- ~~Protocol-upgrade-on-ALTER~~ — `70d2384` (on `AddColumnAsync`; `SetSchemaAsync` still doesn't exist —
  see slice-6 leftovers).

### Strategic decision pending for the remainder (slices 7–9)

Slices 1–6 are landed piecemeal and fully tested, so the "foundation-first vs piecemeal" question is now
only about 7–9 — and piecemeal has held up better than expected (slices 5 and 6 both landed without the
`WriteCoreAsync` refactor). Options:

1. **Continue piecemeal** — slice 8 is a bag of independent bug fixes and is the natural next unit; it
   needs no refactor.
2. **Foundation-first for 7+9** — the codec seam and row-level concurrency are the two that genuinely
   want pr-4's `WriteCoreAsync` refactor underneath. Take it as a base commit before those.
3. **Pause** — slices 1–6 are a coherent, high-value, fully-tested set (standalone spec/interop bug fixes,
   safe writes to Spark tables, spec-correct column mapping + schema evolution). A defensible stopping point.

Remaining slices: 7 (pluggable codec seam — **STRATEGIC**, discuss project positioning first), 8 (misc
reader/DML: S3 conditional-write fix, ListVersions ascending sort, thrift wire-type guards, Decimal128
reads, Time64, nested stats), 9 (row-level concurrency — **STRATEGIC**, most complex).

### Slice-6 leftovers (deliberately not landed)

- **Physical-keyed `add.partitionValues`.** The PR keys committed partition values by the PHYSICAL column
  name under mapping (`metaData.partitionColumns` stays logical) — matched to Spark empirically. Not landed
  because it ripples into `DeltaFilePruner` (looks up `PartitionValues` by logical name) and the read path's
  partition-column injection; master is currently self-consistent with logical keys. Interop gap, not an EW
  correctness bug. Land as one commit covering writer + pruner + read path together.
- **`SetSchemaAsync`**, the nested-struct `AddFieldAsync` variant, and the buffered-transaction
  (deferred-commit) forms of the ALTER operations — the last are slice-9-coupled.
- Compaction's non-mapping thread from the same PR diff (DV exclusion, row-tracking id materialization,
  pluggable reader/writer, HWM action, OPTIMIZE commitInfo) belongs to slices 7/8/9.

## Deferred follow-ups (do after the PR-landing work)

### A. VACUUM spec alignment (`VacuumExecutor.cs`)

EW's `VacuumExecutor` diverges from official Delta (under-deletes): it only considers `.parquet` files
(so orphaned `deletion_vector_*.bin` leak forever), protects only active-add data paths, has no
tombstone model, and is file-mtime-based (ignores `delta.deletedFileRetentionDuration`).

Official algorithm (verified against delta-io/delta `VacuumCommand.scala` + PROTOCOL.md): mark-and-sweep
driven by snapshot state — build the keep-set over every FileAction (active `AddFile`s AND unexpired
`RemoveFile` tombstones), adding each one's data path AND its deletion-vector path
(`getDeletionVectorRelativePathAndSize`); list the whole table dir recursively with NO extension filter;
delete anything not in the keep-set past the cutoff. Retention defaults to
`delta.deletedFileRetentionDuration` = 1 week; tombstone protection keyed on `delTimestamp`.

Rewrite plan: (1) drop the `.parquet`-only filter; (2) keep-set = active adds' data+DV paths PLUS
unexpired tombstones' data+DV paths (use `Snapshot.Tombstones`, added in slice 3 / `b41f5ad`); (3) honor
`delta.deletedFileRetentionDuration` (default 1 week), gate tombstoned files on `deletionTimestamp`.
Behavioral change to flag to Curt: EW VACUUM will then actually delete orphaned DV `.bin` and removed
data files past retention (it doesn't today). Correct/spec-matching; default 7-day retention protects
recent files. Ship with tests. Curt's preference: match the official implementation, not EW's.

### B. Percent-encode non-ASCII in `DeltaPath.Encode`

`DeltaPath.Encode` (`src/EngineeredWood.DeltaLake/DeltaPath.cs`) currently escapes only
`% space # ?` + control chars and leaves **non-ASCII characters literal**. Spark/delta-rs percent-encode
non-ASCII (as UTF-8 bytes) in `add.path`, so an EW-written table whose partition values / paths contain
non-ASCII may not be readable by strict foreign readers — an interop gap. EW's own reader round-trips
fine (`Uri.UnescapeDataString` is a no-op on literals), so this only matters for cross-engine reads.

Fix approach (needs a short research pass first, like the VACUUM one, to confirm Spark's exact behavior):
confirm Spark UTF-8 percent-encodes non-ASCII in `add.path`, then update `DeltaPath.Encode` to
percent-encode each non-ASCII char's UTF-8 bytes as `%XX`, and verify `Decode` (`Uri.UnescapeDataString`)
round-trips. Don't guess the encoding — a wrong "fix" is worse than the current faithful port.
