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
  - Under `IEEE754_TOTAL_ORDER`, such a page's bounds are the smallest and largest NaN. EW uses
    the first NaN in row order for both, as its chunk statistics already do (one comparator), and
    records the chunk's `boundary_order` as `UNORDERED`.
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
| Truncation | Chunk statistics already shorten BYTE_ARRAY bounds to 64 bytes, but on a byte boundary, so a STRING bound can be cut inside a code point. The page index truncates by its own UTF-8-aware rule (W-3) |

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
- Chunk-level `Statistics` keep their existing rule (64 bytes, cut on a byte boundary) and are
  not affected by `PageIndexTruncateLength`.

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
  - The writer landed with `WritePageIndex = false` (phase 3, #399). Phase 4 measured the
    overhead and turned the default on (below).
  - The measurement uses the Parquet write benchmarks with and without page indexes:
    wall-clock duration, allocated bytes and peak working set (BenchmarkDotNet's
    `MemoryDiagnoser`), over plain, dictionary, string-heavy and nested schemas, with
    V1 and V2 pages. File-size growth is recorded as well, though it is not one of the
    conditions.
  - The phase-4 PR records the numbers and the case for calling them nominal. If they are
    not, the default stays off until the cost is brought down, for example by deriving
    chunk statistics from the page statistics instead of computing both (W-2).
- **Measured (phase 4): nominal, so the default is on.** 500,000 rows, net10.0, i9-12900K. The
  plain, strings and nested writes were first +6% to +15% slower with the index at the
  default page size. The cause was the second pass over the values: page bounds, then chunk
  statistics. So chunk statistics are now folded from the page bounds when a page index is
  written (`PageIndexCollector.ChunkStatistics`, byte-identical to the scan; see
  `ChunkStatistics_AreIdenticalWithAndWithoutTheIndex`). With the fold, time is within noise,
  except for repeated (list/map) columns (below).

  Index cost, as the median of 25 alternating off/on rounds in one process
  (`dotnet run -c Release -f net10.0 -- pageindex-ab`). Each cell shows three separate runs;
  even with alternating rounds, one run moves by up to ±10 points on this machine:

  | Schema | 1 MiB pages, V1 | 1 MiB pages, V2 | 8 KiB pages, V1 / V2 | Allocated, 1 MiB |
  |---|---|---|---|---|
  | plain | +0.1 / −5.4 / −5.1% | −6.0 / −3.8 / +2.0% | +3% to +13% / −7% to +25% | +0.02% |
  | dictionary | +2.2 / +0.9 / −2.1% | +4.1 / +1.2 / −1.2% | +1% to +5% / +4% | +0.1% |
  | strings | −3.2 / −4.3 / +0.2% | −1.9 / +9.1 / −12.1% | +3% to +14% / +6% to +15% | +0.03% |
  | nested | +0.1 / +4.2 / +4.9% | +2.0 / +4.5 / +6.7% | +2% to +8% / +5% to +7% | +0.01% |

  Before the fold, one run at 1 MiB gave: plain +5.9% / +14.3%, strings +4.6% / +14.4%, and
  nested +12.8% / +11.1%. Now plain, dictionary and strings center on zero. Allocations are
  the median over the rounds and barely vary.

  **The nested schema's remaining cost is its repeated column.** Measured one column at a
  time (`-- pageindex-ab 25 nested nested:list nested:struct nested:id`), from the same data,
  two runs, 1 MiB pages, V1 / V2:

  | Column | Run 1 | Run 2 | Share of the nested write |
  |---|---|---|---|
  | whole `nested` schema | +4.0% / +3.7% | +5.0% / +4.5% | ~98 ms |
  | `list<int64>` (repeated) | −1.6% / +3.7% | +6.9% / +4.8% | ~100 ms on its own |
  | `struct{int32, string}` (nested, not repeated) | +0.8% / +1.0% | +1.8% / +2.2% | ~19 ms |
  | flat `int64` | +5.2% / −3.7% | −1.8% / +1.3% | ~5 ms, too short to resolve |

  So repeated columns keep a small, consistent cost of about +3% to +5%. A non-repeated
  struct costs about +1% to +2%, like the dictionary schema, and flat columns cost nothing
  measurable. The cause inside the repeated path is not profiled. The likely suspect is
  `PageRows`, which counts each page's rows over its repetition levels: a second pass on V2,
  whose header already counted them, and a new pass on V1. Counting once, in the V2 header,
  and reusing that count would be the first thing to try.
  - **File size:** +0.01% at the default page size; +0.4% to +1.7% at 8 KiB.
  - **Peak working set:** the median of 5 fresh processes per configuration
    (`-- pageindex-overhead`) moves −15% to +9% in both directions, with no pattern. That is GC
    heap sizing, which dominates the peak; the index itself holds a few bytes per page until
    `CloseAsync`. There is no measurable cost within that ±10–20% noise floor.
  - **BenchmarkDotNet** (`PageIndexWriteBenchmarks`) agrees, but its methods run one after the
    other, so machine load that changes between them moves the ratio. The alternating run is
    the one to trust. (It first had to run in process, because the generated child project
    could not build through `build/StrongNameUnsignedReferences.targets`; fixed in #403.)
  - **Small pages:** 8 KiB pages still cost several percent, with runs up to +15% for strings
    (and one +25% plain outlier). That is per-page work (bounds, truncation, one index entry)
    and is not the default.
- **Encryption:** when encryption lands, these two structures are module types 6/7. Route
  their bytes through one write helper and one read helper so there is one place to encrypt
  them (encryption plan D8).

### W-7. Write-side oracles

| Oracle | What it proves |
|---|---|
| **DataFusion 54** (arrow-rs, page index on by default), via the existing reader-interop tier (#237) | EW writes a sorted column with many small pages; a filtered `SELECT` in DataFusion must equal the same filter over a full read in DuckDB (which ignores page indexes). EXPLAIN ANALYZE's page-index pruning metrics must show pages actually pruned, or the test proves nothing. Use `page_index_rows_pruned`: DataFusion 54's `page_index_pages_pruned` reported "0 matched" for string and decimal predicates that matched a page's rows |
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

**Done (phase 5).** The model types and decoding landed in phase 1 (#392).
`ParquetFileReader.ReadPageIndexAsync(rowGroup, columnNames)` selects columns by name, as
`ReadRowGroupAsync` does, and refuses a chunk stored in another file (#405). It
merges the requested ranges across gaps of up to 64 KiB, which on a parquet-mr/EW layout is one
request of one or two ranges, and decodes each index on first access. All page-index reads go
through `ReadPageIndexBytesAsync`, the future decryption point. The 27 fixtures read back exactly
what their footers locate. `floating_orders_nan_count` also confirms the TYPE_ORDER rule from a
second writer: parquet-mr omits the ColumnIndex for a TYPE_ORDER chunk with an all-NaN page and
keeps it, with `nan_counts`, under total order.

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
- ~~Using the OffsetIndex to build `PageMapBuilder`'s map without scanning headers.~~ Done,
  ahead of phase 7 because it needs neither #55 nor the `PageReader`. The flat batched read
  (`ReadRowGroupBatchesAsync` when a row group spans several batches) used to read every
  chunk whole just to find its pages, then read the pages again per batch. It now reads the
  OffsetIndexes and each chunk's dictionary or symbol-table prefix, two requests in all, and
  decodes each page header from the page bytes it fetches anyway. Chunks without a usable
  index still scan headers. An index that cannot tile its chunk is ignored. One that
  disagrees with a page header is refused with a `ParquetFormatException`. The implicit
  large-chunk budget keeps the header scan, because the index has no uncompressed page
  sizes and that budget exists to stay under the Arrow limit. An explicit
  `MaxBatchByteSize` scales each page's compressed size by the chunk's ratio instead.
  Measured by `-- pagemap-ab`: with 500k rows and 1 MiB pages, a strings read reads half the
  bytes and runs 28–32% faster, and a plain read is 8–11% faster at 64Ki-row batches.

**Sequencing on the read side:** page pruning is only reachable from the table layer after
#55 (per-read filters), which is now done. Its Delta scan walks row groups itself and carries
each batch's file position, so page pruning must keep reporting positions the same way: a
skipped page range is rows the deletion vector and row ids still count. `predicate-pushdown-design.md` already puts #55 and #57 ahead of
page-level pushdown, and this plan does not change that. R-1 is independent and useful for
testing the writer.

---

## 5. Phases

| # | Phase | Size | Depends on |
|---|---|---|---|
| 0 | #389: row-aligned pages | small | – |
| 1 | Thrift model + codec for §1, ColumnChunk fields 4–7 (both directions) | small | – |
| 2 | Encryption plan Phase 1b: `EmitPage`, shared writer code | medium | – |
| 3 | W-2…W-6, landing with `WritePageIndex = false`, + W-7 oracles | medium | 0, 1, 2, #396 |
| 4 | Measure memory and duration overhead; default on if nominal (W-6) | small | 3 |
| 5 | R-1: parse/expose + fixture self-consistency | small | 1 |
| 6 | Encryption plan Phase 1a: `PageReader` with position-derived ordinals | medium | – |
| 7 | R-2 + R-3: page pruning and row-range decode | large | 5, 6, #55 (done) |
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
