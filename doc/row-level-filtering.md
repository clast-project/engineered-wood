# Exact row-level filtering in the Parquet reader (#502): proposed API and approach

Status: proposal, 2026-11-09, revised the same day after phase 0 (§11). A design note: check the code before relying on it. Covers #502's three parts, cmettler's host-mask request (comment on #502), #608 part 2 (as a non-goal), and the Delta wiring. Written from origin/main at 7370fc4c and arrow-rs at 3db18f75b.

## 1. Where things stand

**Parquet reader.** Planning (`PlanRowGroupsAsync`) prunes by statistics, then dictionary/Bloom (`MightMatchAsync`), then the page index (`NarrowByPageIndexAsync`). It then subtracts `RowSkipPredicate` runs: deletion-vector runs, and only for flat columns that have an OffsetIndex. The result is a `RowGroupPlan(Index, GroupStart, Ranges, OffsetIndexes)`. `ReadPlannedAsync` decodes every projected column over `Ranges` and yields `PositionedRecordBatch(Batch, FirstRow)`. A batch is contiguous and never spans a gap. The result is always a superset.

**Delta scan.** `ProcessFileBatchesAsync` does the following for each batch:
1. renames to logical names;
2. strips the materialized row-tracking columns;
3. applies the deletion vector (`KeptRows` + `ArrowCompute.Take`);
4. builds positions as `firstRow + kept[n]`, and row ids and versions from them;
5. runs `ReconcileToTableShape`;
6. appends the metadata columns.

`DeltaReadOptions.Filter` only prunes; its doc tells callers to filter rows themselves. It does **not** feed conflict detection: hosts declare reads through `txn.DeclareRead`/`DeclareFilesRead`. So cmettler's worry (2) about the read set is already handled, and a host mask cannot widen it.

**DML.** DELETE reads only the predicate's columns. It skips deletion-vector runs, evaluates `MaskFor(predicate)` on each batch, and ANDs out deleted rows afterwards (`MatchingLiveRows`). Row counts come from the footer (#439). UPDATE and MERGE read whole files.

**Lance** is exact. It reads each fragment whole, evaluates the predicate, then calls `Take`. Its `ExtendProjectionForFilter` is the prior art for filter-only columns. **Iceberg** has no data read path.

**Building blocks already on main:**
- `SelectionMask` (Core, internal), an ulong-word bitmap with `And`/`Count`/`ToIndices`, documented as the place where "a deletion vector clears … each predicate mask is ANDed in";
- `DeletionVectorBitmap.ContainsRange` and `ClearDeleted(start, len, Span<ulong>)`;
- `ArrowCompute.Filter` and `SelectedIndices`;
- `DecodedRows` cursors and `BatchPageWindow` in the flat batched path;
- the 10-22 prototype (`ColumnBuildState.ApplySelectionToPage` + `SelectiveDecoder`), branch `perf/selection-pushdown-ab`.

## 2. What arrow-rs does, and what to take from it

| arrow-rs | Take it? |
|---|---|
| `ArrowPredicate { projection(); evaluate(RecordBatch) -> BooleanArray }`, chained in `RowFilter`. Each predicate sees only the rows that earlier ones kept (`selection.and_then(raw)`). A null in the mask counts as false, and the mask must have the batch's length. | **Yes.** It is the shape cmettler asked for, and the `DeleteAsync(Func<RecordBatch, BooleanArray>)` shape. One difference: our predicates get their columns **in the order given**, not in file-schema order (arrow-rs's `ProjectionMask` is unordered, which its docs have to warn about). |
| Evaluated **per row group**: predicate *i* over every batch of the group, then *i+1*, then the projection. A group whose selection empties stops there. Each stage fetches only the pages of its own columns that hold a selected row (`RowSelection::scan_ranges` over the OffsetIndex). | **Yes.** It fits `RowGroupPlan` and the per-row-group concurrency (#612) we already have. |
| Two selection representations: run lists (`RowSelector`) and a bitmap (`Mask`). `Auto { threshold: 32 }` picks the bitmap when the average run is under 32 rows (arrow-rs#8733, arrow-rs#10141, arrow-rs#10852). In mask mode it decodes whole contiguous chunks, unselected rows included, and then runs Arrow's `filter`. Its repeated bugs were mask chunks crossing pages that were never fetched (arrow-rs#10288). | **Internally, yes.** Our measurements argue for skipping inside the decoder rather than decode-then-filter (see §5). Mask chunks must stop at fetched-page boundaries from day one. |
| Decoders only `skip(n)` / `read(n)` runs; individual values are never gathered inside a decoder. DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY skips cost about as much as a decode. | **Go further.** The prototype's per-value `skip` was 26–56% faster than decode-then-`Take`. Encodings that can't skip compact in place instead. |
| Predicate cache: a column that is both filtered on and projected is decoded once. Batches are cached aligned to `batch_size`, with a 100 MB-per-row-group budget, and re-decoded once it is exceeded. Flat columns only, async path only. | **Simpler form.** Keep pass-1 arrays of projected filter columns and compact them, charged to the scan's `MemoryBudget`. Re-decode when over budget. |
| Fusion of consecutive predicates over the same single leaf (arrow-rs#10859): Q25 went from 146.7 to 119.5 ms, against 120.7 ms with pushdown off. | **Yes, as a follow-up.** Our Delta wiring splits a predicate into conjuncts, so `a > 1 AND a < 9` becomes two predicates on one column. |
| Positions come back as a `RowNumber` virtual column. | **Side channel instead**, as `PositionedRecordBatch` does. Delta builds three columns from positions; one `Int64Array` serves them all. |
| Nothing is evaluated against dictionaries inside the reader; only `Dictionary` output lets a predicate see the keys. | **Do better.** See §6. |
| DataFusion's `pushdown_filters` is still off by default; pushdown regressed on non-selective and repeated-column predicates. | Ship opt-in, and measure before flipping any default. |

## 3. Proposed Parquet API

All of this goes in `EngineeredWood.Parquet`, with no new dependency (no reference to `Expressions.Arrow`).

```csharp
namespace EngineeredWood.Parquet;

/// One conjunct of an exact row filter: the columns it reads and a function from them to a mask.
public sealed class RowPredicate
{
    public RowPredicate(IReadOnlyList<string> columns, Func<RecordBatch, BooleanArray> evaluate);

    /// Resolved as `columnNames` are (top-level names or dotted leaf paths). The batch passed to
    /// Evaluate holds exactly these columns, in this order, whether or not they are projected.
    public IReadOnlyList<string> Columns { get; }

    /// Returns a mask of the batch's length; null and false both drop the row. May be called
    /// concurrently (row groups are read in parallel). The batch belongs to the reader: don't keep it.
    /// Exceptions propagate out of the read, as from a post-filter.
    public Func<RecordBatch, BooleanArray> Evaluate { get; }

    /// True when a row's answer depends only on that row's values of Columns and is deterministic.
    /// The reader may then evaluate it once over a column chunk's DICTIONARY and map the indices,
    /// never materializing the column (§6). False by default.
    public bool ValueWise { get; init; }
}

/// Predicates ANDed in order. Each one sees only the rows the earlier ones kept.
public sealed class RowFilter
{
    public RowFilter(params RowPredicate[] predicates);
    public IReadOnlyList<RowPredicate> Predicates { get; }
}

/// Rows a caller never wants, keyed by file position: a deletion vector or positional deletes.
/// DeletionVectorBitmap implements it as-is (ContainsRange / ClearDeleted).
public interface IExcludedRows
{
    bool ExcludesAll(long firstRow, long rowCount);                      // = today's RowSkipPredicate
    void ClearExcluded(long firstRow, int rowCount, Span<ulong> selection); // Arrow bit order
}

/// One read's intent: per call, not on ParquetReadOptions, so a reader serves different reads.
public sealed record ParquetScan
{
    public IReadOnlyList<string>? Columns { get; init; }   // output projection; null = all
    public Predicate? Prune { get; init; }                  // superset pruning, exactly as today
    public RowFilter? RowFilter { get; init; }              // exact (for the rows returned)
    public IExcludedRows? ExcludedRows { get; init; }
    public bool WithPositions { get; init; }
}

/// Batches may span rows that were filtered out, so their rows need not be contiguous.
public readonly record struct SelectedRecordBatch(RecordBatch Batch, RowPositions Positions);

public readonly struct RowPositions
{
    public bool IsContiguous { get; }        // true: FirstRow + i
    public long FirstRow { get; }
    public Int64Array? Explicit { get; }     // set when rows were dropped inside the batch
    public long this[int row] { get; }
}

public IAsyncEnumerable<SelectedRecordBatch> ReadSelectedAsync(ParquetScan scan, CancellationToken ct = default);
```

### Decisions behind the shape

- **A new method, not a changed `Filter`.** `ParquetReadOptions.Filter` and `ReadWithPositionsAsync` keep returning a superset. The doc at `ParquetReadOptions.cs:276` stays true for every caller who doesn't opt in. Over time, `ReadWithPositionsAsync(filter, cols, skipRows)` can become a thin wrapper over `ReadSelectedAsync`.
- **`Prune` and `RowFilter` stay separate.** The caller already translates a `Predicate` to file leaf paths for pruning (`RowGroupPushdown.ToFileColumns`). A row predicate can be something no `Predicate` expresses (DuckDB's Bloom/hash join filter), and a `Predicate` can prune without being exact. A host passes `k BETWEEN lo AND hi` as `Prune` and the join probe as a `RowPredicate`.
- **The reader doesn't need to see the `Predicate` for part 3.** This reverses the 11-01 design note on #502. A value-wise delegate evaluated over the dictionary's values (§6) covers equality and IN. It also covers ranges, `LIKE`, `lower(x) = …` and DuckDB's Bloom probe. `MembershipPredicateEvaluator` handles only `Eq`/`In`.
- **Batches may span filtered-out rows.** #502 step 5 said they can't. At about 8 matches per 20k-row page (cmettler's q8 numbers), that rule yields 1-row batches. Positions travel instead, and they are free when a batch is contiguous.
- **Exclusions are part of the selection.** A deletion vector clears bits before any predicate runs. So no predicate is evaluated on a deleted row, and the second `Take` disappears: the prototype measured deletion-vector reads at 37–41 ms, against 80–86 ms for decode-then-`Take`. `RowSkipPredicate` is the `ExcludesAll` half of the interface.
- **No "exactness" flag in the reader.** The reader returns exactly the rows the predicates accept. Whether a predicate over-approximates (a Bloom probe) is the table layer's business.
- **Not public:** run-vs-bitmap representation, filter batch size, and adaptivity. They are internal knobs we expect to change while tuning.

## 4. Read algorithm (per planned row group)

1. **Seed** a row-group bitmap with the planned `Ranges` (or all rows). Then call `ExcludedRows.ClearExcluded`. At 1M rows this is 128 KB of `SelectionMask`, from `ArrayPool`.
2. **Pass 1**, for each `RowPredicate` in order:
   - Decode its columns that aren't decoded yet, only for pages holding a selected row (OffsetIndex), selection applied in the decoder (§5).
   - Evaluate in batches of the selected rows (`BatchSize`, or an internal 64K default), and AND each mask into the bitmap.
   - If the bitmap is empty, finish the group: no output column is fetched.
   - A filter column that is also projected keeps its decoded rows, charged to the scan's `MemoryBudget`. Over budget, they are dropped and re-decoded in pass 2.
3. **Pass 2:** decode the remaining projected columns under the final bitmap. Pages with no selected row are not fetched or decompressed. Kept filter columns are compacted to the final selection with a bitmap filter, not a `Take`.
4. **Assemble** output batches of up to `BatchSize` / `MaxBatchByteSize` *output* rows, with positions taken from the bitmap's set bits plus `GroupStart`.

Notes:
- **I/O.** Pass 2 is a second dependent request per row group. Row-group concurrency (#612) hides most of it. Measure with `pageindex-read-ab`'s latency and transfer model. If it shows up, add a rule: when pruning left the group dense, fetch filter and output pages in one request.
- **Nested columns.** First cut: a nested output column, or a predicate on a nested leaf, is decoded whole for the group and compacted with `ArrowCompute.Filter`. That is correct, with no saving, which matches what `ReadRowRangesAsync` does today.
- **Why per row group rather than DuckDB's 2048-row interleave.** The bitmap is known before any output page is chosen, so page I/O is planned once. Interleaving would need the selection per window across pages that span windows, against `DecodedRows`. Pass-1 arrays are the cost, and they are bounded by the budget.
- **Thread safety.** Planning stays serial. The two passes run inside `ReadPlannedAsync`, which already runs concurrently per group.

## 5. Selection inside the decoders (part 2)

Port the prototype's hook from whole-chunk `ReadColumn` to the batched path (`ReadColumnBatchFromSlice` / `DecodedRows`). That is the path every scan uses.

- **Per page:** after level decode, drop deselected definition levels and build a value mask. Use a branch-free loop; branchy loops lost 30–70% at 50% selectivity. An all-selected page takes today's code unchanged, and an unselected page is never fetched.
- **Per encoding:**
  - Skip: PLAIN (fixed width and byte array), dictionary indices, DELTA_LENGTH_BYTE_ARRAY, BYTE_STREAM_SPLIT.
  - Compact in place after a full page decode: DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, ALP/PFOR/FSST, until each is shown to skip faster.

  arrow-rs confirms that DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY can't skip cheaply.
- **Representation:** the bitmap is the source of truth. Runs are pulled from it in 64-bit words (arrow-rs#11510-style) for page and long-run skips. The mask path is used when the average run is under about 32 rows, starting from arrow-rs's threshold and tuned with `selection-ab`.

## 6. Answering from the dictionary (part 3)

For a `ValueWise` predicate over **one** column, when a chunk has a dictionary page:
1. Build a one-column batch of the dictionary's values, plus one null row if the column is nullable. Call `Evaluate` once per chunk, giving a lookup table of `bool`s with the null row's answer kept separately.
2. For each dictionary-encoded page, map the RLE/bit-packed indices through the table straight into the bitmap. Null rows (from definition levels) get the null answer. A page that fell back to PLAIN decodes values and calls `Evaluate` as usual, so a mixed chunk is fine.
3. If the column isn't projected, it is never materialized. If it is, pass 2 materializes only the selected rows.

Guards:
- Use the table only when the dictionary has fewer entries than the group's selected rows.
- If evaluating the dictionary throws, fall back to row evaluation. An ANSI cast error on an entry no selected row uses must not fail the read; a real error recurs on the row.

This is generic over the delegate. So EW's Spark predicates (once marked value-wise) and a host's Bloom probe both get it.

It is the 3–5 ms (about 22% of samples) measured on `OneCategory` in #608/#609.

An all-false lookup table proves that no row of the group can pass, provided the null answer is false too or the chunk has no nulls. Treat that as a prune, and skip the group before any data page is read. This is the only way an opaque filter (a host's Bloom probe) can prune a row group. `MembershipPredicateEvaluator` can't, because it needs a `Predicate`.

### Where Bloom filters fit

Two different Bloom filters are involved, at different layers:

- **A host's runtime join filter** (DuckDB's, per cmettler) is a `RowPredicate`, passed through `DeltaReadOptions.RowMasks`. It is evaluated in pass 1 on the key column only and ANDed into the selection; false positives are left for the host's join to recheck. With `ValueWise` set, it is probed once per dictionary entry rather than once per row, and an all-false table prunes the group (above). EW's writer stores integers as DELTA_BINARY_PACKED rather than dictionary-encoding them, so integer keys are still probed per row.
- **Parquet's own split-block Bloom filters** stay where they are: row-group pruning for `Eq`/`In` in `Prune` (`MightMatchAsync`, opt-in `FilterUseBloomFilters`). They never take part in the row pass. A host with a small build side can also pass `k IN (…)` in `Prune`, and get row-group pruning from the file's dictionaries and Bloom filters before anything is decoded. With a large build side, pass the key range instead.

## 7. Delta wiring

`DeltaReadOptions` (public):
- `FilterRows` (bool, default false): applies `Filter` exactly. The current superset contract stays the default, since tests and the host guide rely on it.
- `RowMasks` (`IReadOnlyList<DeltaRowMask>`): host delegates over **logical** column names (cmettler's join filters). They may over-approximate and never enter a read set (reads don't declare one anyway). `DeltaRowMask(IReadOnlyList<string> Columns, Func<RecordBatch, BooleanArray> Mask, bool ValueWise = false)`.

Per file, the scan builds the Parquet `RowFilter`:
- **Split** `Filter` into top-level conjuncts:
  - A conjunct over partition columns only was already answered by file pruning, so it is dropped.
  - A partition column inside a mixed conjunct is folded to the file's literal.
  - A column missing from the file folds to NULL.
  - Every other conjunct becomes a `RowPredicate` over physical leaf names.
- **Wrap each delegate** to rename physical to logical, and to widen types as `ReconcileToTableShape` would, on the predicate's own small batch. Then call `RowEvaluator.EvaluatePredicate`.
  - `ValueWise` is true when the conjunct has no function calls, or only functions the registry declares deterministic. That needs a new optional registry capability, like `IShortCircuitingFunctions`.
- **Order:** host masks after the `Filter` conjuncts. Conjuncts that can be evaluated value-wise over dictionaries go first. Selectivity ordering comes later.
- **Deletion vector:** `ExcludedRows = DeletedRows`. `ProcessFileBatchesAsync` loses steps 3 and 4 above (its `KeptRows`/`Take`).
- **Row ids, versions and `_ew_row_address`** are built from `RowPositions`. Materialized row-tracking columns are ordinary projected columns, so they are masked by the same bitmap as everything else. This removes structurally the "masked out of step" hazard that `predicate-pushdown-design.md` warns about.
- **Filter-only columns** are read and dropped by the reader. `GetReadSchema` doesn't change.
- **Codec seam:** an `IDataFileReader` gets no predicate. Post-filter with the same `RowPredicate` list there. That post-filter is also the test oracle.

DML:
- **DELETE** becomes `ReadSelectedAsync` with `Columns = []`, `RowFilter` set to the predicate, `ExcludedRows` set to the deletion vector, and `WithPositions`. It gets positions only, with no output decode, and possibly no predicate-column materialization (dictionary). Footer row counts are unchanged (#439).
- **UPDATE and MERGE** still need whole files, but can pass `ExcludedRows` instead of `Take`-filtering.
- **Worth verifying first:** DELETE today evaluates the predicate on deleted rows and masks them out afterwards. A value error on a deleted row (an ANSI cast) would then fail a DELETE that Spark, which filters deletion-vector rows first, lets through. The new path fixes that, but check it on main with a test.

## 8. Phasing

| Phase | Content | Gate |
|---|---|---|
| 0 | `rowfilter-ab` harness next to `pageindex-read-ab`. Vary selectivity (point, 10%, 50%, 90%, 99%), clustered vs scattered, projection width, deletion vector on/off, local vs latency/transfer model. Measure the current post-filter baseline. | Numbers in the PR |
| 1a | *(Revised after phase 0, §11.)* `ReadSelectedAsync` + `RowFilter` + `IExcludedRows` + `RowPositions`, with evaluation and compaction moved **inside the row-group unit**. That means they run in parallel across row groups, not on the consumer's thread. Still one decode pass and one fetch, so no I/O risk. A batch whose selection is all set is passed through, not gathered. | Oracle = full read + post-filter, over the test matrix in #502. Mutation: positions off by one; a batch's mask applied to its neighbour |
| 1b | Lazy decode: within a fetched row group, decode the output columns only for pages holding a selected row, then decode-then-compact. Fetch filter and output pages in **one** request by default. Fetch them in two stages only where pass 1 can be expected to save more bytes than a round trip costs (§11, F5). Nested columns are decoded whole. | Same oracle. Mutation: pass 2 decodes the unmasked selection. `rowfilter-ab` local and cloud |
| 2 | In-decoder selection (port the prototype to the batched path); run/bitmap switch. | `selection-ab` on the batched path; no regression at 99% kept |
| 3 | Delta: `FilterRows`, `RowMasks`, deletion vector via `ExcludedRows`, positions-only DELETE. | Delta suites; Spark interop oracle for DELETE counts |
| 4 | Dictionary evaluation (§6) + `IDeterministicFunctions`. | `OneCategory` profile; the dictionary-error fallback test |
| 5 | Fusion of same-column conjuncts, adaptive demotion of a predicate that keeps >~95% (evaluate it after pass 2 instead), limit pushdown, then a decision on whether `FilterRows` should default on. | Measured crossover |

Phase 1b delivers cmettler's main request: other columns are not decoded for pages with no surviving row. Phase 0 showed this skips almost nothing for scattered selections (§11, F2). Phase 2 is what makes them pay. Ask cmettler to test after each of phases 2 and 3.

## 9. Non-goals and open questions

- **`DictionaryArray` output (#608 part 2).** Independent of this design. Phase 4 delivers the filtering half of its benefit without forcing every consumer to accept dictionary arrays.
- **Lance.** Its reader is separate. It could adopt the same `RowFilter` shape later; nothing here depends on it.
- **Open question:** should `RowPredicate.Columns` allow dotted paths into structs from day one, or only top-level and flat leaves? I lean towards flat leaves, with nested predicates decoded whole.
- **Open question:** should `RowFilter` order its predicates itself (by dictionary-evaluability, then observed selectivity), or always respect the caller's order? arrow-rs respects the caller's order; DuckDB reorders adaptively. I propose respecting the caller's order in v1, with Delta choosing the order.

## 10. Before starting phase 1

These are not settled by the design above. Settle them in the phase 0/1 PRs.

- **The internal entry points.** The Delta scan doesn't call the public read methods. It calls the internal `PlanRowGroupsAsync`/`ReadPlannedAsync` pair, with a `ScanReservation` and `DecodedGrowth` (#612). `RowGroupPlan` (or a sibling) must carry the `RowFilter` and `IExcludedRows`, and the two-pass read must live inside `ReadPlannedAsync`, so that the public `ReadSelectedAsync` and the Delta scan share one implementation.
- **How pass 2 reuses the batched path.** The cheapest phase 1 turns the final bitmap into row spans, merging gaps shorter than some G rows, and feeds those to the existing `ReadBatchesAsync` span machinery, which already skips pages outside the spans. It then compacts each decoded piece with the bitmap. Today's batches never cross a span, so output batches would have to be assembled from compacted pieces: either by concatenation (see the Arrow 23 concatenator gaps) or by a cursor that ignores spans. Choose one with the phase 0 numbers.
- **Memory accounting.** `PlannedBytes` scales the reservation by the share of rows the ranges keep. For pass 2, the selected-row share is known only after pass 1. Pass-1 arrays held for projected filter columns have to be reserved too.
- **Verify the DELETE behaviour in §7** with a test on main before relying on it as a motivation.
- **The oracle already exists.** The Parquet test project references `Expressions.Arrow` (since #430), so "full read + `ArrowRowEvaluator` post-filter" can be the phase 1 oracle as it stands.

## 11. Phase 0 measurements (2026-11-09)

Harness: `dotnet run -c Release -f net10.0 -- rowfilter-ab [rounds] [latencyMs] [direct|cloud] [MB/s] [excluded fraction] [case]` (`test/EngineeredWood.Parquet.Benchmarks/RowFilterAb.cs`).

**Data:** 4 row groups of 500,000 rows, default write options (20,000-row pages, page indexes).
- `id` is sorted, so a range on it is **clustered**.
- `r` is uniform over [0, 1M), so a range on it is **scattered**.
- `category` has 50 values and is dictionary-encoded.

**Projections:** narrow is `id, amount`; wide is `id, ts, category, amount, qty, note`.

**Strategies**, all checked every round to return the same rows (count and a checksum of `id`):
- **none:** the projection with no filter. A reference, not an exact read.
- **post:** today's exact read. `ReadWithPositionsAsync` with pruning, then per batch: `SelectionMask`, `ArrowRowEvaluator`, `ArrowCompute.Take`, and dropping the filter-only columns.
- **pass1:** the filter columns only, then evaluate. The floor for any two-pass read.
- **late:** phase 1 emulated with public APIs. Pass 1 builds each row group's selection. Pass 2 calls `ReadRowRangesAsync` over the pages holding a selected row, four row groups at a time, then gathers. It over-states phase 1's cost: a projected filter column is decoded twice, and pass 2 re-reads the OffsetIndexes.

i9-12900K (24 logical processors), net10.0, median of 7 rounds.

**Local, milliseconds**

| Case | Kept | none | post | pass1 | late | late vs post | eval | gather |
|---|---|---|---|---|---|---|---|---|
| clustered 1% narrow | 1% | 4.8 | 0.7 | 0.4 | 0.8 | +7% | 0.3 | |
| clustered 90% narrow | 90% | 5.5 | 19.5 | 3.8 | 12.4 | -36% | 2.0 | |
| clustered 90% wide | 90% | 23.9 | 61.3 | 3.8 | 39.6 | -34% | 2.1 | |
| scattered point narrow | 0% | 4.9 | 11.1 | 7.0 | 7.3 | -34% | 4.8 | 0.2 |
| scattered 0.1% narrow | 0.1% | 5.3 | 9.4 | 4.8 | 10.1 | +10% | 2.4 | 0.3 |
| scattered 1% wide | 1% | 24.2 | 31.8 | 4.9 | 28.4 | -11% | 2.6 | 3.6 |
| scattered 10% wide | 10% | 23.9 | 44.3 | 5.9 | 32.3 | -25% | 3.6 | 13.6 |
| scattered 50% narrow | 50% | 5.3 | 26.7 | 10.3 | 17.8 | -33% | 7.9 | 9.5 |
| scattered 99% narrow | 99% | 5.2 | 30.4 | 4.8 | 14.4 | -51% | 2.4 | 17.5 |
| scattered 99% wide | 99% | 24.2 | 76.0 | 5.0 | 40.4 | -46% | 2.4 | 49.9 |
| scattered point wide | 0% | 23.2 | 30.6 | 7.1 | 8.1 | -73% | 4.9 | 0.2 |
| category = 2% wide | 2% | 25.0 | 32.2 | 7.7 | 33.2 | +2% | 3.7 | |
| category <> 98% wide | 98% | 26.4 | 70.4 | 7.9 | 47.9 | -34% | 3.8 | |

**Cloud model** (20 ms per GET through `CoalescingFileReader`, 100 MB/s), milliseconds:

| Case | post | late | late vs post | GETs post / late | MB read post / late |
|---|---|---|---|---|---|
| clustered 0.1% narrow | 86 | 160 | +86% | 4 / 6 | 0.2 / 0.2 |
| clustered 10% wide | 152 | 218 | +44% | 5 / 7 | 8.3 / 8.3 |
| scattered 1% wide | 308 | 344 | +11% | 6 / 14 | 75.5 / 75.5 |
| scattered point wide | 323 | 168 | -48% | 6 / 16 | 75.5 / 9.5 |
| scattered 99% wide | 360 | 365 | +1% | 6 / 14 | 75.5 / 75.5 |

Excluding 1% of rows at random (as a deletion vector would) changed none of these comparisons. The harness clears excluded rows with a naive per-row loop, so it says nothing about the cost of deletion vectors themselves.

### Findings

- **F1. Today's exact read is bound by the consumer thread, not by decoding.** `post − none` is about `eval + gather`. The gather (index list, `Take`, projection) costs 17.5 ms narrow and 50 ms wide at 99% kept, while the reader decodes four row groups in parallel. `late` gains −34 to −61% at ≥90% kept while decoding the *same* bytes, because its gather runs inside per-row-group tasks. So evaluation and compaction belong inside the row-group unit (`ReadPlannedAsync` under `OrderedScan`). This is phase 1a: the biggest local win, with no I/O change. Separately, `ArrowCompute.Take` allocates its output in managed memory (181 MB managed for the post-filter at 99% wide), which is worth its own look.
- **F2. Page-level skipping does nothing for scattered selections at ≥0.1%.** With 20,000-row pages, every page holds about 20 or more matches. Only the point lookup skipped pages: 5.8 MB read instead of 21.5, −34% narrow, −73% wide. Phase 1b alone does not serve the host's join filter. Phase 2 (selection inside the decoders) does, and it has to come next.
- **F3. The evaluator is no longer the bottleneck.** #502 asked for this re-measurement. `r < k` costs 1.2–4 ms per million rows, and `category = '…'` about 2 ms, down from 40 ms per million before #560. The cost peaks at 50% kept (7.9 ms per 2M rows against 2.4 ms at 1% or 99%), which suggests a branchy comparison or `SelectionMask.And` loop. Check it before tuning.
- **F4. Most of an exact read is decoding and gathering what the filter discards.** `pass1` (filter columns plus evaluation) is 4.6–10 ms, against 9–85 ms for `post` on unclustered predicates.
- **F5. On object storage, a second dependent fetch costs more than lazy decoding saves.** `late` was 44–86% slower on small clustered selections, because pass 2 waits for pass 1, plus the emulation's extra OffsetIndex request. Only the point lookup, which saved 66 MB, won. The design therefore separates I/O from decoding: fetch filter and output pages together by default, and decode output pages lazily. Split the fetch only when pass 1 can be expected to empty enough pages to save more than a round trip. That is roughly `bytes saved / bandwidth > latency`, learned from earlier row groups of the same scan. Clustered predicates are already narrowed by the page index before any fetch.
- **F6. Dictionary strings cost about 3 ms more in pass 1.** `pass1` is 7.8 ms for `category` against 4.7 ms for the integer key, consistent with the #608 profile. That is phase 4's target.
- **F7. A projected filter column decoded twice costs up to 50% more decoding on narrow clustered reads** (24 MB against 16 MB at 50%). Keeping pass-1 arrays (§4 step 2) is worth it.
