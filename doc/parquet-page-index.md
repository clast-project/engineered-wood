# Parquet Page Index (ColumnIndex / OffsetIndex): Implementation Plan

**Status:** Not implemented. Written against `main` @ `201ab13` (2026-09-26). This plan
supersedes the "Future: Page-Level Pushdown" section and phases 11–13 of
[`predicate-pushdown-design.md`](predicate-pushdown-design.md), and corrects one claim made
there (see [W-2](#w-2-per-page-statistics-are-new-work)).

**Prerequisite:** [#389](https://github.com/clast-project/engineered-wood/issues/389). EW
splits rows of repeated columns across data pages, and an OffsetIndex requires every page
to start at a row boundary.

## Why build it

Page indexes are already **common in files**. parquet-mr has written them by default since
1.11 (2019), and arrow-rs does too. 26 of the `parquet-testing/data/*.parquet` fixtures carry
one. The readers that *use* them are fewer:

- Trino (on by default);
- parquet-mr column-index filtering (so Spark);
- Impala;
- DataFusion (`datafusion.execution.parquet.enable_page_index = true` by default, verified
  on DataFusion 54).

DuckDB does not use them.

The two halves have very different value and cost:

| | Value | Cost |
|---|---|---|
| **Writing** | Every EW-written file (including Delta tables) prunes better in Trino, Spark and DataFusion. Needs no EW reader work | Small–medium |
| **Reading** | Page skipping *inside* surviving row groups. Pays only when data is clustered on the predicate column (time-ordered appends, Z-order, liquid clustering) | Large: needs a row-range decode path |

So writing ships first, and reading is sequenced behind the shared page-reader refactor.

---

## 1. The structures

(Quoted from `parquet.thrift`; field numbers matter because EW has a hand-written codec.)

```
ColumnChunk   4: offset_index_offset i64   5: offset_index_length i32
              6: column_index_offset i64   7: column_index_length i32

struct PageLocation {
  1: required i64 offset                // file offset of the page HEADER
  2: required i32 compressed_page_size  // header + compressed payload
  3: required i64 first_row_index       // row within the row group; pages must start at rep = 0
}
struct OffsetIndex {
  1: required list<PageLocation> page_locations         // data pages only, increasing offset,
                                                         // strictly increasing first_row_index
  2: optional list<i64> unencoded_byte_array_data_bytes
}
struct ColumnIndex {
  1: required list<bool>   null_pages       // all-null page: min/max entries are empty byte[]
  2: required list<binary> min_values       // bounds, may be truncated, must stay valid values
  3: required list<binary> max_values
  4: required BoundaryOrder boundary_order  // UNORDERED 0 / ASCENDING 1 / DESCENDING 2
  5: optional list<i64> null_counts         // writers SHOULD always write it
  6: optional list<i64> repetition_level_histograms
  7: optional list<i64> definition_level_histograms
  8: optional list<i64> nan_counts          // FLOAT/DOUBLE/FLOAT16 (PARQUET-2249)
}
```

Rules that bite:

- **Row boundaries.** "When an OffsetIndex is present, pages must begin on row boundaries."
  This applies to V1 pages too. See #389.
- **NaN.**
  - Under `TYPE_ORDER`, if any page's non-null values are *all* NaN, the ColumnIndex must
    be **omitted for that chunk**. The OffsetIndex is still written.
  - Under `IEEE754_TOTAL_ORDER`, such a page's bounds are the smallest and largest NaN.
- **INT96.** No ColumnIndex unless the column order is `INT96_TIMESTAMP_ORDER`. EW's
  `ColumnOrder` enum has no such member, so EW never writes one for INT96.
- **Truncated bounds** "must still be valid values within the column's logical type". A
  truncated UTF-8 max must remain valid UTF-8.
- **No exactness flags.** Unlike `Statistics`, the ColumnIndex has no `is_*_exact`, so a
  reader must treat every page bound as inexact.

---

## 2. Current state (what exists to build on)

| Area | Today |
|---|---|
| Thrift | `MetadataDecoder` skips ColumnChunk fields 4–7 (`MetadataDecoder.cs:520-545`); `MetadataEncoder` never writes them. There is no model for any §1 struct |
| Chunk stats | `StatisticsCollector.Compute(array, physicalType, typeLength, defLevels, nonNullCount, rowCount, floatingPointTotalOrder, extendedTimestamp)`, plus `ComputeFromDictEntries` / `ComputeFloatingPointFromDictEntries`. Each is called **once per chunk** (`ColumnChunkWriter.cs:248`, `:532`) |
| Page emission | Every page goes through `ColumnChunkOutput.EmitPage`, which returns each page's offset in the chunk and its data-page ordinal |
| Page cut points | `ColumnChunkWriter.cs:350-378` loop, which counts levels, not rows (#389) |
| Chunk placement + footer | `ParquetFileAssembler`, shared by `ParquetFileWriter` and `BufferedParquetWriter` |
| Filter | `ParquetReadOptions.Filter` is evaluated per row group by `StatisticsEvaluator.Evaluate<RowGroup>` through `ParquetStatisticsAccessor`. The contract is a **superset**: the caller re-applies the predicate |
| Page walkers | Three independent loops (`ColumnChunkReader.ReadColumn`, `TryReadFixedListColumn`, `PageMapBuilder.Build`) plus the lazy `*FromEntry` decode |
| Truncation | None anywhere. Chunk statistics are written full-length |

The shared refactors in [`encryption-design.md`](encryption-design.md) (Phase 1a: one
`PageReader`; Phase 1b: `EmitPage` plus de-duplicating the two writers) are prerequisites
here too.

---

## 3. Write side

### W-1. Page cut points on row boundaries (#389)

This must land first. Cut pages only at `repLevels[i] == 0`. A single record larger than the
target page size gets a page of its own.

### W-2. Per-page statistics are new work

`predicate-pushdown-design.md` says index writing is "mostly serialising per-page statistics
the `StatisticsCollector` already computes". **It does not compute them.** The collector
runs once per chunk. The work:

- **Plain path:** call `StatisticsCollector.Compute` on each page's value slice (dense
  values `valueIndex .. valueIndex + pageNonNull`, and that page's def levels).
- **Dictionary path:** track, per page, which dictionary entries the indices reference. Then
  take min/max over those entries with the existing `ComputeFromDictEntries` comparator.
  Iterating a page's indices is cheap next to encoding them.
- **Keep one comparator.** Page min/max must use exactly the column-order semantics the
  chunk stats use: signed vs unsigned, `FloatingPointColumnOrder`, the extended-timestamp
  carrier, and NaN exclusion. Share the code rather than re-deriving it.
- **Invariant, asserted in tests:** chunk min = min over page mins, and the same for max
  and null count. A later optimization could derive the chunk stats from the page stats and
  drop the second pass. That is not required.
- **Scope follows the chunk stats.** If a column gets no chunk statistics (for example
  `ColumnWriteStatistics[col] = false`, or FLOAT16 until its statistics gap closes; see the
  float-stats follow-ups), it gets **no ColumnIndex**. It still gets an OffsetIndex, which
  needs no statistics.

### W-3. Truncation

**Decided:** the default limit is **64 bytes** (parquet-mr's
`parquet.columnindex.truncate.length`). Callers can override it with
`ParquetWriteOptions.PageIndexTruncateLength` (`int?`, default 64). A positive value sets
the limit, `null` turns truncation off (full-length bounds), and zero or a negative value
is rejected at option validation. The limit applies to every truncatable column in the
file; a per-column override can be added later if someone needs it.

- Truncate **only** physical `BYTE_ARRAY` columns whose order is unsigned lexicographic
  (STRING, BINARY, JSON, BSON, ENUM).
  - Min: take the prefix.
  - Max: take the prefix and increment its last byte with carry. If every byte is `0xFF`,
    keep the full value.
  - STRING/JSON: cut on a code-point boundary and increment a code point, so the result
    stays valid UTF-8. This is what parquet-java's `BinaryTruncator` does.
- **Never truncate FIXED_LEN_BYTE_ARRAY**: decimals are signed big-endian, and the
  extended-timestamp carrier is signed little-endian (already noted in `known-issues.md`).
  UUID and FLOAT16 are fixed-width and small anyway.
- Chunk-level `Statistics` stay untruncated, as today. Only the index truncates.

### W-4. The rest of the ColumnIndex

- `null_pages[i]` is `pageNonNull == 0`, with empty min/max entries.
- `null_counts` is always written.
- `boundary_order`: walk the non-null pages with the column comparator. `ASCENDING` if both
  `min[i] <= min[i+1]` and `max[i] <= max[i+1]` hold everywhere, `DESCENDING`
  symmetrically, otherwise `UNORDERED`. A chunk with one or zero non-null pages satisfies the
  ascending test trivially, so it is `ASCENDING`.
- `nan_counts` is written for FLOAT/DOUBLE, from per-page NaN counts that the collector
  already knows how to count (PARQUET-2249 work).
- The TYPE_ORDER all-NaN-page rule (§1) omits the ColumnIndex for the chunk.
- INT96 gets no ColumnIndex.
- **Deferred:** level histograms (fields 6/7), `SizeStatistics` (ColumnMetaData 16) and
  `unencoded_byte_array_data_bytes`. They are optional, and arrow-rs writes them, but no
  pruning reader needs them.

### W-5. OffsetIndex

- One `PageLocation` per **data page**. Dictionary and FSST symbol-table pages are
  excluded; they are located via `dictionary_page_offset` / `symbol_table_page_offset`.
- `EmitPage` records the page's offset **relative to the chunk**, its header + payload
  size, and `first_row_index`: a running row count, which after W-1 is exact because every
  page starts a row. Chunk placement adds `chunkStart`.

### W-6. Placement and options

- Indexes go **after the last row group, before the footer**: all ColumnIndexes, then all
  OffsetIndexes, in row-group/column order. This is parquet-mr's layout, and it lets a
  reader fetch every index of a row group in one contiguous read.
- The per-row-group index bytes stay in memory until `CloseAsync`. They are a few bytes per
  page.
- ColumnChunk fields 4–7 are written in `MetadataEncoder`.
- This goes into the **shared** footer writer from encryption Phase 1b, never into both
  writers separately.
- `ParquetWriteOptions.WritePageIndex` (bool) and `PageIndexTruncateLength` (`int?`,
  default 64; see W-3).
- **Decided: page indexes are written by default, provided the memory and duration
  overhead is nominal.** parquet-mr and arrow-rs also default on; pyarrow defaults off.
  - The writer lands with `WritePageIndex = false` (phase 3). Phase 4 measures the
    overhead and flips the default in its own PR.
  - The measurement uses the Parquet write benchmarks with and without page indexes:
    wall-clock duration, allocated bytes and peak working set (BenchmarkDotNet's
    `MemoryDiagnoser`), over plain, dictionary, string-heavy and nested schemas, with
    V1 and V2 pages. File-size growth is recorded as well, though it is not one of the
    conditions.
  - The phase-4 PR records the numbers and the case for calling them nominal. If they are
    not, the default stays off until the cost is brought down, for example by deriving
    chunk statistics from the page statistics instead of computing both (W-2).
- **Encryption:** when encryption lands, these two structures are module types 6/7. Route
  their bytes through one write helper and one read helper so there is one place to encrypt
  them (encryption plan D8).

### W-7. Write-side oracles

| Oracle | What it proves |
|---|---|
| **DataFusion 54** (arrow-rs, page index on by default), via the existing reader-interop tier (#237) | EW writes a sorted column with many small pages; a filtered `SELECT` in DataFusion must equal the same filter over a full read in DuckDB (which ignores page indexes). EXPLAIN ANALYZE's page-index pruning metrics must show pages actually pruned, or the test proves nothing |
| **Mutation check** | Deliberately write one page's max *below* a value that page really contains. DataFusion must then drop rows, turning the test red. A tier that cannot fail tests nothing |
| pyarrow `ColumnChunkMetaData.has_column_index` / `has_offset_index` | Structural presence only (pyarrow's Python API does not expose index contents) |
| EW self-check (after R-1) | Walk the chunk's page headers and assert that each `PageLocation` offset, size and `first_row_index` matches, and that each page's recomputed min/max lies within the index bounds |
| Optional: Spark (parquet-mr column-index filtering) | Only if DataFusion leaves doubt; a second, independent consumer lineage |

Test matrix: V1/V2 pages, dictionary and plain, nullable/required, repeated leaves (after
#389), truncation boundaries (63/64/65 bytes, all-`0xFF`, multibyte UTF-8 at the cut), a custom
`PageIndexTruncateLength`, truncation turned off with `null`, NaN
pages under both float orders, all-null pages, single-page chunks, and **both writers**.

---

## 4. Read side

### R-1. Parse and expose

- Model types for §1. Decode in `MetadataDecoder`, which then keeps ColumnChunk fields 4–7.
- `ParquetFileReader.ReadPageIndexAsync(rowGroup, columns)` fetches the contiguous index
  region for the requested columns in one ranged read and decodes it lazily. Nothing is
  read unless a caller or the filter path asks.
- Public read-only API, so that tools and tests can inspect indexes.
- Tests over the 26 fixtures that carry indexes. For each, walk the chunk's pages and
  assert that the OffsetIndex agrees (the self-consistency oracle above). Specific
  fixtures:
  - `alltypes_tiny_pages(_plain)`: many tiny pages, built for this;
  - `int32_with_null_pages`: `null_pages`;
  - `binary_truncated_min_max`: truncated bounds, written by parquet-rs;
  - `floating_orders_nan_count`: `nan_counts`, IEEE 754 order;
  - `int96_timestamp_order`;
  - `int96_from_spark`: an OffsetIndex but **no** ColumnIndex;
  - `map_no_value`: an OffsetIndex but no ColumnIndex;
  - `repeated_primitive_no_list` and `old_list_structure`: repeated leaves.

### R-2. Page pruning without new predicate logic

The trick is to **reuse `StatisticsEvaluator` unchanged**.

1. For the columns a predicate references, merge all their pages' `first_row_index`
   boundaries into one sorted set of **elementary row intervals**. Inside one interval,
   every referenced column is covered by exactly one page.
2. Implement `IStatisticsAccessor<RowInterval>`. For `(interval, column)` it returns the
   covering page's min/max/null count. Page bounds are always inexact (§1), so
   `IsMinExact`/`IsMaxExact` return false. A column with no ColumnIndex returns null, which
   the evaluator already treats as Unknown.
3. `Evaluate(filter, interval, accessor)`: an `AlwaysFalse` interval is dropped. Adjacent
   surviving intervals coalesce into the row group's **selected row ranges**.

AND/OR/NOT, NaN, IN-sets and starts-with come for free, and there is no row-range
algebra to write. The number of intervals is at most the total page count of the
referenced columns.

- **Scope limit:** only **non-repeated** columns participate at first. For a repeated
  leaf, `null_counts` counts values, not rows, so `GetValueCount` has no row-level meaning.
  A predicate on a repeated column yields Unknown for it, as it effectively does at row
  group level today.

### R-3. Row-range decode (the large part)

The `Filter` contract stays a **superset**: pruning drops rows that provably don't match,
and the caller still post-filters.

- Build on encryption Phase 1a's `PageReader`, whose page ordinal is **position-derived**
  so that page *k* can be decoded without walking pages 0 to *k*−1. That property is also
  what encryption's AAD needs, so it is designed once.
- Per selected column:
  1. Map the row ranges through the OffsetIndex to the pages that overlap them.
  2. Fetch those pages plus the dictionary or symbol-table page, coalescing adjacent byte
     ranges into one `ReadRangesAsync`.
  3. Decode each page and trim its partially-overlapping ends. For flat columns, trim by
     row position and def levels. For repeated columns, trim by counting `rep = 0`.
- Columns **without** an OffsetIndex (older files) are read whole and then trimmed to the
  same row ranges, so every column in the batch agrees on its rows.
- All read entry points must honour the ranges: `ReadAllAsync`, `ReadRowGroupBatchesAsync`
  (whose `BatchSize`/`MaxBatchByteSize` slicing now runs over selected rows) and the
  fixed-list fast path.
- **Fetching the index costs one extra ranged read per row group.** Do it only when a
  `Filter` is set, the row group survived chunk-level pruning, and the predicate references
  a column with an index. Consider a size gate (skip it for row groups with few pages).
- **Oracles:**
  - differential against a full read + the same predicate (must be a superset, and equal
    after post-filtering);
  - the 26 fixtures with random predicates on their indexed columns;
  - a pages-actually-skipped assertion, via a read counter on a test `IRandomAccessFile`.

### R-4. Later

- `nan_counts`-driven `IsNaN` / `IsNotNaN` page pruning (the row-group logic exists in
  `StatisticsEvaluator.EvaluateNaN`).
- Repeated-column pruning.
- Level histograms.
- Using the OffsetIndex to build `PageMapBuilder`'s map without scanning headers.

**Sequencing on the read side:** page pruning is only reachable from the table layer after
#55 (per-read filters). `predicate-pushdown-design.md` already puts #55 and #57 ahead of
page-level pushdown, and this plan does not change that. R-1 is independent and useful for
testing the writer.

---

## 5. Phases

| # | Phase | Size | Depends on |
|---|---|---|---|
| 0 | #389: row-aligned pages | small | – |
| 1 | Thrift model + codec for §1, ColumnChunk fields 4–7 (both directions) | small | – |
| 2 | Encryption plan Phase 1b: `EmitPage`, shared writer code | medium | – |
| 3 | W-2…W-6, landing with `WritePageIndex = false`, + W-7 oracles | medium | 0, 1, 2 |
| 4 | Measure memory and duration overhead; default on if nominal (W-6) | small | 3 |
| 5 | R-1: parse/expose + fixture self-consistency | small | 1 |
| 6 | Encryption plan Phase 1a: `PageReader` with position-derived ordinals | medium | – |
| 7 | R-2 + R-3: page pruning and row-range decode | large | 5, 6, #55 |
| 8 | R-4 items, on demand | – | 7 |

Phases 0–4 deliver the downstream-reader benefit on their own. Phases 5–7 are the EW-reader
benefit, and whether they are worth it depends on how clustered EW users' data is.

## 6. Decisions and open questions

**Decided:**

1. **Write page indexes by default**, provided the memory and duration overhead is nominal
   (W-6, phase 4).
2. **Shorten string bounds to 64 bytes by default**, and let callers override the limit or
   turn truncation off with `PageIndexTruncateLength` (W-3).

**Open:**

3. **Is R-3 worth building at all**, or do phases 0–5 suffice until a user shows clustered
   data and a filtered-read workload?
