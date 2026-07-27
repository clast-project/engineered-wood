# Run-end encoding for constant columns

All three phases have landed. This note records what the write path now does, what it measures, and which
of the constant-column sources deliberately did NOT adopt the encoding.

## Why this exists

The write path materializes columns that hold one value in every row:

| source | column | type |
| --- | --- | --- |
| `CdfWriter.AddChangeTypeColumn` | `_change_type` | string |
| `CdfReader.AddMetadataColumns` | `_commit_version`, `_commit_timestamp` | int64 |
| `PartitionUtils.BuildConstantArray` | partition values (read path) | any |
| IcebergCompat | partition values materialized INTO the data file | any |
| `DeltaTable.ConstInt64` | commit version on rewritten rows | int64 |

`ArrowCompute.Repeat` tiles the value rather than appending per row, which is O(N) in memory with a
memcpy-shaped constant: a 1M-row `_change_type` column is 24 MB (16 bytes of value + 4 of offset per row).

Run-end encoding is the representation that makes it O(1) — one run, ~840 bytes measured, whatever the row
count. Apache.Arrow 23 ships `RunEndEncodedArray` and `RunEndEncodedType`.

## What landed

**Phase 1** (`df70b7e`) made the Parquet writer recognise a constant column from its plain-Arrow form and
skip the per-row hashing, without introducing a new array type: constant string 11.0 → 1.1 ms per 1M rows,
constant int64 3.6 → 0.2 ms. **That spent the encode-speed argument**, and phases 2–3 were re-scoped onto
the memory alone before they were written.

**Phase 2** — the writer accepts run-end encoded columns:

- `ArrowToSchemaConverter.MapArrowType` maps `RunEndEncodedType` to its VALUE type, so the Parquet schema,
  and therefore the file, is exactly what the plain form produces. Run-end encoding over a nested value
  type is refused rather than unwrapped (`NestedLevelWriter` walks rows and has no run-aware path).
- `ColumnChunkWriter` derives definition levels a run at a time, compacts a sliced window, and applies the
  Int8/16 widening and the decimal-FLBA byte reversal to the VALUES child rather than to the rows.
- `DictionaryEncoder.TryEncodeRuns` builds the dictionary from run values — one hash per run — and returns
  indices in run form (`DictionaryIndexRuns`) rather than one per row.
- `RleBitPackedEncoder` gained `EncodeRuns` / `BeginRuns` / `AppendRun` / `EndRuns`, so a run reaches the
  page as an RLE run without a materialized `int[]`. The page loop walks runs with a cursor, since a page
  boundary can fall inside one.
- Everything else expands: FLOAT/DOUBLE up front (they are always full-scanned for the NaN count), and any
  column the dictionary declines, which lands it on the per-row path unchanged.

**Phase 3** — `CdfWriter` builds `_change_type` run-end encoded, via `RunEndEncoding.Constant`.

`RunEndEncoding` (in `EngineeredWood.Core`) carries the primitives: `Constant`, `Expand`, `Compact`,
`Take`, and an allocation-free `EnumerateRuns`. `ArrowCompute.Take` dispatches to it, and gathering keeps
the column run-end encoded — even where that is the larger representation, because a column whose type
disagrees with its field is a mismatch nothing downstream checks.

## Measured

1M rows, `"update_postimage"`, minimum of 5 after 3 warm-up writes:

| | plain | run-encoded |
| --- | --- | --- |
| build the column | 24,000,280 B | **840 B** |
| write it (allocated) | 5,091,960 B | **1,092,528 B** |
| write it (elapsed) | 3.87 ms | **1.75 ms** |
| five-run low-cardinality column, allocated | 13,481,128 B | **1,092,944 B** |
| five-run low-cardinality column, elapsed | 12.15 ms | **1.04 ms** |

The ~1 MB residue in both run-encoded writes is the output `MemoryStream`'s initial capacity, which is
sized from the row count and has nothing to do with the column. The five-run row is the part that
generalizes past constants — the run-aware dictionary path the phase-2 note asked to be measured before
committing to it.

Files are byte-identical in every case, which is what
`RunEndEncodedWriteTests` asserts throughout rather than round-tripping through our own reader.

**The hash table was the trap.** The per-row arms size `BytesHashTable` from the cardinality cap — a fifth
of the ROW count — because that is all they know before they start hashing. Sized that way, encoding a
1M-row constant column allocated 8.4 MB of table for a one-entry dictionary and swallowed the entire
saving, with every correctness test still passing. The run count is an exact upper bound on the distinct
values, and `TryEncode_ConstantRunEndEncodedColumn_AllocatesNothingPerRow` fails at 8,389,152 bytes if the
sizing regresses (verified).

## What did NOT adopt it, and why

Run-end encoding is contained to the write path, and to the sources whose columns reach nothing but our own
Parquet writer. Three of the five constant-column sources above were left on `ArrowCompute.Repeat`:

- **`CdfReader.AddMetadataColumns` and `PartitionUtils.AddPartitionColumns`** are the READ path. Their
  batches go to whoever asked for the data, and the Arrow schema they declare is part of that contract —
  `_change_type` as `RunEndEncodedType(int32, utf8)` would be a breaking change to every consumer.
  `CdfWriter.AddChangeTypeColumn` therefore kept its plain form (the read path calls it) and the encoded
  form is a separate, private method.
- **`PartitionUtils.AppendPartitionColumns`** (IcebergCompat) can flow to a caller-supplied
  `DeltaTableOptions.DataFileWriter` instead of our writer. A host writer given a run-end encoded column
  would have to handle a layout it never agreed to.
- **`DeltaTable.ConstInt64`** returns a typed `Int64Array` that rides through the row-tracking plumbing as
  `(Int64Array, Int64Array)` tuples. Adopting the encoding there is a refactor of that plumbing for 8
  bytes a row, which is the smallest of the five.

All three can adopt it without new machinery — `RunEndEncoding.Constant` plus a field type — if the
containment argument ever changes. That is a decision about blast radius, not a missing capability.

## Constraints and gotchas

- **Read stays plain.** No Parquet file holds runs, so the reader never produces one. Do not expect
  round-tripping: a column written from runs reads back as its value type.
- **Nulls are not where the rest of Arrow puts them.** A run-end encoded array has no validity bitmap; its
  nulls are in the values child, one per RUN. `array.IsNull(row)` answers FALSE for every row of a column
  that is entirely null. Anything deriving definition levels, statistics or a null count from `IsNull` is
  silently wrong on such a column — pinned by
  `IsNull_AnswersFalseForANullRun_WhichIsWhyNullsAreReadFromTheValues`.
- **A slice is a view.** The children still hold every run in the original; only `Data.Offset` says which
  rows are in scope. Anything reading the children directly must `Compact` first.
- **`BufferedParquetWriter` cannot take one.** It accumulates rows and builds an index per row; it throws
  rather than silently mishandling run-form indices.
- **`_change_type` is spec-defined.** Spark and delta-rs read it as a plain string column, and they still
  do — that is the *Parquet* type, and `CdfChangeTypeLayoutTests` asserts the written `_change_data` file
  holds a required UTF8 column.
