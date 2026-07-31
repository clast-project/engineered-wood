# Delta row tracking — conformance record

**Status: complete and measured in both directions.** Row tracking is spec-conformant on the write
side, exposed on the read side, carried through the Change Data Feed, and validated against Spark 4.0.1
/ 4.1 and delta-rs 1.6.2 with EngineeredWood as both producer and consumer.

This document records what the spec requires, what EngineeredWood does, and — the part worth not
re-deriving — the facts about Spark's behaviour that were established by **probing rather than
reasoning**. Several of them contradict what the spec text suggests.

## What the spec requires

Row tracking is a writer feature (`rowTracking`) depending on the `domainMetadata` writer feature,
enabled by `delta.enableRowTracking=true`. Every row has a stable **row ID** and a **row commit
version**, carried one of two ways:

1. **Default values — no column.** `rowId = add.baseRowId + positionInFile`, and
   `add.defaultRowCommitVersion` gives the version. A freshly appended file needs only these two `add`
   fields and no materialized column. Position is the physical position in the file; a deletion vector
   does not renumber.

2. **Materialized values — a hidden column.** When a row's identity cannot be derived from position
   because a rewrite moved it, it is stored per row in a hidden physical column. The physical names
   live in table metadata as `delta.rowTracking.materializedRowIdColumnName` and
   `delta.rowTracking.materializedRowCommitVersionColumnName`. A non-null materialized value overrides
   the default for that row.

High-water mark: the `delta.rowTracking` domainMetadata carries `{"rowIdHighWaterMark":
highestAssignedId}`. Note that this stores the highest *assigned* id while EngineeredWood's internal
counter holds the *next* id. The domain value holds the line when the highest-id file leaves the
active set, so ids are never reassigned.

## What EngineeredWood does

- **Append** assigns `baseRowId` + `defaultRowCommitVersion` and writes no materialized column.
- **Copy-on-write rewrite** (UPDATE / OVERWRITE / DELETE / compaction) materializes each moved row's
  original id and commit version into the declared hidden columns. A changed row's commit version
  advances to the rewrite's version; an untouched-but-relocated row keeps its original. A fresh
  `baseRowId` still goes on the new `add` as the fallback for null-materialized rows.
- **Read** strips the hidden columns before any caller sees them, and can surface each surviving row's
  resolved id and version through out-params — which is what lets a second rewrite preserve a first
  rewrite's ids.
- **`ReadAllWithRowTrackingAsync` / `ReadAtVersionWithRowTrackingAsync`** append the spec's two
  generated columns, resolved materialized-else-positional. Both are nullable, since a file predating
  row tracking has no derivable id. A non-row-tracking table and a user column colliding with a
  generated name are both refused rather than served nulls.
- **CDF** materializes both columns into every `_change_data` file on a row-tracking table, and
  `ReadChangesWithRowTrackingAsync` emits them on the feed.
- **Column names** are configurable via `DeltaReadOptions.MetadataPrefix` /
  `DeltaChangeReadOptions.MetadataPrefix` (default `_metadata.`); a prefix colliding with a table
  column is refused rather than shadowed.
- **Concurrency** rebases under row tracking rather than aborting — see
  [`delta-concurrency.md`](delta-concurrency.md).

`RejectRowTrackingWrite` now refuses only a rewrite of a table that does not declare its materialized
column-name properties — a spec-invalid shape EngineeredWood never creates, but a foreign engine could.
Without those names ids cannot be preserved through the rewrite. Appending to and reading such a table
is still allowed.

## Measured facts about Spark

Each of these was probed before any code was written, and each contradicts something that was
previously believed or reasoned from the spec text.

- **No parquet `field_id` on the materialized columns**, or on `_change_type`, even under `id`-mode
  column mapping — in data files and `_change_data` files alike, with `delta.columnMapping.maxColumnId`
  reserving none for them. The declared physical *name* is the sole lookup key in both mapping modes.
  This had been recorded as a deferred gap on the reasoning that an `id`-mode reader would need field
  ids to resolve them; the reasoning was wrong and the gap never existed.
- **A `cdc` action carries no `baseRowId` or `defaultRowCommitVersion`**, unlike `add` and `remove`. So
  materializing is the *only* way identity reaches a change row — there is no positional fallback on
  the feed.
- **Spark writes an `update_postimage` row's commit version as NULL**, meaning "the version being
  committed", while pre-image and delete rows carry the row's original version explicitly.
  EngineeredWood writes the post-image version explicitly instead; it resolves to the same number
  without relying on a reader implementing the fallback.
- **Spark's own `readChangeFeed` does not expose row ids at all** — `_metadata` does not resolve on the
  feed. EngineeredWood's exposure goes beyond the reference implementation here.
- **Spark populates `baseRowId` / `defaultRowCommitVersion` on `remove` actions.** EngineeredWood did
  not, which is what made an OVERWRITE's inferred delete rows unresolvable; it does now.
- **Spark leaves a `_change_type` column inside the rewritten main data file** when CDF is on (verified
  on a committed `add`, not an orphan). EngineeredWood does not — it is stricter here.
- **Spark's materialized column names are hyphenated** — `_row-id-col-<uuid>` /
  `_row-commit-version-col-<uuid>` — unlike EngineeredWood's `_row_id_<hex>`. A Spark table declares
  `rowTracking` + `domainMetadata` + `appendOnly` + `invariants` at reader 1 / writer 7, and after an
  UPDATE leaves one file whose `baseRowId` is 3 while its rows' materialized ids are 0, 1, 2. That gap
  is what makes the foreign-read tests discriminating: a reader ignoring the materialized column
  reports 3, 4, 5.

## Two bugs this work found

Both are worth keeping because their *shape* can recur.

**A partitioned row-tracking table lost stable ids on the second rewrite.** `ReadFileAsync` names the
file's columns explicitly whenever the table is partitioned or the read is projected — and the hidden
materialized columns are not schema fields, so they were never requested, `StripMaterializedColumns`
found nothing, and every id fell back to `baseRowId + position`, which on a rewrite output is a *fresh*
id. One UPDATE could not expose this, because its source is a fresh append with no materialized column
to miss; the second silently changed every row's identity while preserving its data. Fixed by naming
the declared (and legacy) materialized columns in the file-level column list, with the existing
file-present intersect dropping them again for appends that carry none. The codec-seam branch has no
footer to intersect against, so it reads all columns when the table declares materialized names.
Compaction was never affected — it reads raw parquet with no column list.

**A row-tracking table's hidden columns leaked into the CDF feed.** `CdfReader` opens change files and
data files directly rather than through the main read path, so the materialized columns surfaced as two
extra Int64 columns. Unpartitioned tables only: `AddPartitionColumns` consumes data columns positionally
against the table schema, which takes exactly the user columns and leaves the hidden ones off the end,
so a partitioned table was already dropping them by accident. That accident is why the partitioned
coverage asserts ids rather than just absence.

## Interop coverage

Both directions are measured, and each test was confirmed to fail with the relevant production code
neutered rather than merely passing.

- **EngineeredWood → foreign.** Spark reads appended ids and rewrite-preserved ids, including on
  partitioned tables (plain and Name-mode-mapped) rewritten *twice* — the shape that caught the id loss
  above, which a single rewrite cannot. Spark resolves ids through `id`-mode column mapping after a
  materializing UPDATE. Spark reads an EngineeredWood change feed and reports exactly the table's
  columns. delta-rs reads a rewritten table with no leaked columns.
- **Foreign → EngineeredWood.** Spark creates, writes and materializes; EngineeredWood reads the same
  ids and preserves them through its own UPDATE and compaction, and resolves Spark's hyphenated names
  out of Spark's own change files. This direction needed **no production change** — the capability was
  already correct, only unmeasured. What it retired was a risk, not a defect.

## Entry points

- `src/EngineeredWood.DeltaLake/RowTracking/RowTrackingConfig.cs` — property/domain keys, high-water
  mark build and reconcile, `TryGetMaterializedColumnNames`.
- `src/EngineeredWood.DeltaLake.Table/RowTracking/RowTrackingWriter.cs` — `AddRowIdColumn`,
  `AddRowIdAndCommitVersionColumns` (the nullable form handles a source predating row tracking), and
  the name-parameterized `StripMaterializedColumns`.
- `DeltaTable.cs` — `ComputeWriteActionsAsync`, `ComputeUpdateActionsAsync`, `ReadFileAsync`,
  `RejectRowTrackingWrite`, `CreateAsync(enableRowTracking:)`.
- `Compaction/CompactionExecutor.cs` — carries original ids and versions through the merge. Note it
  bypasses `ProcessFileBatchesAsync` and strips the columns itself.
- `Cdf/CdfWriter.cs` / `CdfReader.cs` — change-file materialization and feed resolution.
- Tests: `RowTrackingTests`, `RowTrackingHighWaterMarkTests`, `RowTrackingPartitionedIdLossTests`,
  `CdfRowTrackingTests`, and the row-tracking cases in `SparkInteropTests` / `DeltaRsInteropTests`.
