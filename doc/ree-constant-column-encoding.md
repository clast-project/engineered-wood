# Run-end encoding for constant columns — phases 2 and 3

Phase 1 has landed. This note carries the remaining design so the work can be picked up cold.

## Why this exists

The write path materializes columns that hold one value in every row:

| source | column | type |
| --- | --- | --- |
| `CdfWriter.AddChangeTypeColumn` | `_change_type` | string |
| `CdfReader.AddMetadataColumns` | `_commit_version`, `_commit_timestamp` | int64 |
| `PartitionUtils.BuildConstantArray` | partition values (read path) | any |
| IcebergCompat | partition values materialized INTO the data file | any |
| `DeltaTable.ConstInt64` | commit version on rewritten rows | int64 |

All of them now build through `ArrowCompute.Repeat`, which tiles the value rather than appending per row.
That is O(N) in memory but with a memcpy-shaped constant: a 1M-row `_change_type` column is 24 MB
(16 bytes of value + 4 of offset per row).

Run-end encoding is the representation that makes it O(1) — one run, ~32 bytes, whatever the row count.
Apache.Arrow 23 ships `RunEndEncodedArray` and `RunEndEncodedType`.

## What phase 1 already took

Phase 1 deliberately did NOT introduce a new array type. It made the Parquet writer recognise a constant
column from its existing plain-Arrow form and skip the per-row hashing that dominated encoding it.
Measured on `DictionaryEncoder.TryEncode` alone, 1M rows, min of 30 runs:

| column | before | after |
| --- | --- | --- |
| constant string | 11.0 ms | 1.1 ms |
| constant int64 | 3.6 ms | 0.2 ms |
| non-constant (5 shapes) | — | unchanged within noise |

**This matters for scoping phases 2–3: the encode-speed argument for REE is largely spent.** What REE still
buys is the *memory* — the 24 MB above — and a general run-aware path for low-cardinality columns that are
not constant. Do not re-justify this work on encode speed without re-measuring against the phase 1 baseline.

## Phase 2 — accept REE at the writer boundary

`ColumnChunkWriter` rejects an REE array today. Two places:

- `EncodeByteArrayValues` (`ColumnChunkWriter.cs:1247`) tests `array is StringArray || array is BinaryArray`
  and throws at `:1254` otherwise.
- `PhysicalType` resolution has no mapping from `RunEndEncodedType` to its value type.

Work:

1. Map `RunEndEncodedType` to the physical type of its values.
2. In `DictionaryEncoder.TryEncode`, add an REE arm that walks runs instead of rows. A single-run REE
   reduces to the phase 1 constant result. Multi-run REE builds the dictionary from run values and expands
   indices per run — this is the part that generalizes past constants.
3. Optionally teach `RleBitPackedEncoder` to emit a run directly (`EncodeRun(value, count)`), so a run does
   not round-trip through a materialized `int[]`. Phase 1 left `Indices` as a zeroed `int[]`; killing that
   allocation needs a constant/run marker on `DictionaryResult` and a change to the page loop in
   `WriteDictionaryColumn`, which is why it was not done.

Non-dictionary fallback also needs an arm, or an explicit refusal: if a column is REE and the dictionary
bails, something must expand the runs.

## Phase 3 — produce REE at the sources

**Decide where REE is introduced before writing any of this.** It determines the blast radius:

- **Late** — construct the REE array immediately before `WriteRowGroupAsync`. It never reaches
  `StatsCollector`, `ColumnMappingRecursive.ToPhysical`, the schema-evolution reconcile, or
  `ArrowCompute.Take`. Small, independently shippable. Captures nothing extra on encode speed after phase 1,
  so with phase 1 landed **this option now buys almost nothing** — the memory is still materialized.
- **Early** — build the column as REE in `CdfWriter` / partition materialization and let it flow. This is
  where the 24 MB actually goes away. Cost: every consumer on the path needs an REE arm, including
  `ArrowCompute.Take` (DML rewrites and compaction gather CDF batches), and the batch schema must declare
  `RunEndEncodedType`, which is visible to anything inspecting the Arrow schema.

Given phase 1, early introduction is the only variant that pays. Scope it as such or not at all.

## Constraints and gotchas

- **Read stays plain.** No REE exists in a Parquet file, so the reader never produces one. REE is a
  write-side representation only; do not expect round-tripping.
- **`_change_type` is spec-defined.** Spark and delta-rs read it as a plain string column. That is the
  *Parquet* type and is unaffected — but only as long as nothing on the write path reinterprets the Arrow
  type.
- **`ArrowCompute.Take` has no REE arm** and currently throws `NotSupportedException` for types it cannot
  gather, by design. Adding REE there is a prerequisite for early introduction, not an optional extra.
- **Nulls.** Phase 1's constant probe requires `defLevels is null` because it reads every row. An REE arm
  has the same question to answer: a run-encoded column with nulls needs its def levels reconciled against
  run boundaries.

## Suggested first step

Re-measure before committing to phase 2. The phase 1 numbers moved the goalposts: benchmark a
*low-cardinality, non-constant* column (the `low-card string` / `low-card int64` shapes in the phase 1
harness, ~5 ms per 1M rows) to see what a run-aware dictionary path would actually save there. If that
number is small, phases 2–3 are a memory optimization only, and should be justified on memory alone.
