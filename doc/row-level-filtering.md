# Exact row-level filtering in the Parquet reader (#502): proposed API and approach

Status: proposal, 2026-11-09, revised after phase 0 (§11); phases 1a-3c built (§12-§18), phase 4 built (§19-§22); end-of-arc checks in §23; phase 5 planned and 5a built in §24, 5b in §25, 5c in §26, 5d in §27; 5e measured and closed in §28; 5f (offset and limit) in §29; 5g decided in §30, which closes phase 5. A design note: check the code before relying on it. Covers #502's three parts, cmettler's host-mask request (comment on #502), #608 part 2 (as a non-goal), and the Delta wiring. Written from origin/main at 7370fc4c and arrow-rs at 3db18f75b.

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

    /// Top-level column names (phase 1a; dotted leaf paths are refused for now). The batch passed to
    /// Evaluate holds exactly these columns, in this order, whether or not they are projected.
    public IReadOnlyList<string> Columns { get; }

    /// Returns a mask of the batch's length. False drops the row; null keeps it for the predicates
    /// after this one but out of the result (see RowFilter). May be called
    /// concurrently (row groups are read in parallel). The batch belongs to the reader: don't keep it.
    /// Exceptions propagate out of the read, as from a post-filter.
    public Func<RecordBatch, BooleanArray> Evaluate { get; }

    /// True when a row's answer depends only on that row's values of Columns and is deterministic.
    /// The reader may then evaluate it once over a column chunk's DICTIONARY and map the indices,
    /// never materializing the column (§6). False by default.
    public bool ValueWise { get; init; }
}

/// Predicates ANDed in order, with SQL's three-valued AND short-circuited per row, as
/// ArrowRowEvaluator's (and Spark's) And is. A row is dropped as soon as one predicate answers
/// FALSE, and later predicates never see it. A row answered NULL stays live, so later predicates
/// still evaluate it (and can still raise on it), but it is never returned. A row is returned only
/// when every predicate answered TRUE. The reader keeps two bitmaps: live, and all-true so far.
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

- **Positions always come back.** Phase 1a dropped the proposed `WithPositions` switch: positions cost nothing for a batch whose rows are contiguous, and one `Int64Array` of the surviving rows otherwise, which is small next to the gather that produced them.
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
   - Evaluate in batches of the live rows (`BatchSize`, or an internal 64K default). Clear FALSE rows from the live bitmap, and clear FALSE and NULL rows from the all-true bitmap (§3, `RowFilter`).
   - If the live bitmap is empty, finish the group: no output column is fetched. After the last predicate, the all-true bitmap is the selection.
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

An all-false lookup table proves that no row of the group can pass, but only when three things hold:
- the null answer is false too, or the chunk has no nulls;
- the chunk is **wholly** dictionary-encoded, as `MembershipPredicateEvaluator.IsWhollyDictionaryEncoded` decides from `encoding_stats`. A chunk that fell back to PLAIN partway through can hold matching values the dictionary doesn't;
- it is the first predicate, or every predicate before it cannot raise (the rule for reordering in §7). Skipping the group skips evaluating the earlier predicates over its rows, which would suppress an error one of them raises.

Then treat it as a prune, and skip the group before any data page is read. If any of the three fails, keep evaluating: fallback pages are decoded and evaluated row by row. This is the only way an opaque filter (a host's Bloom probe) can prune a row group. `MembershipPredicateEvaluator` can't, because it needs a `Predicate`. *(Measured and not built: §32.)*

### Where Bloom filters fit

Two different Bloom filters are involved, at different layers:

- **A host's runtime join filter** (DuckDB's, per cmettler) is a `RowPredicate`, passed through `DeltaReadOptions.RowMasks`. It is evaluated in pass 1 on the key column only and ANDed into the selection; false positives are left for the host's join to recheck. With `ValueWise` set, it is probed once per dictionary entry rather than once per row, and an all-false table prunes the group under the conditions above. EW's writer dictionary-encodes a chunk of any type, integers included, while it holds under a fifth of its rows in distinct values, and falls back to DELTA_BINARY_PACKED for integers beyond that (corrected in §24; this said integers were never dictionary-encoded). So a key column of many distinct values is still probed per row.
- **Parquet's own split-block Bloom filters** stay where they are: row-group pruning for `Eq`/`In` in `Prune` (`MightMatchAsync`, opt-in `FilterUseBloomFilters`). They never take part in the row pass. A host with a small build side can also pass `k IN (…)` in `Prune`, and get row-group pruning from the file's dictionaries and Bloom filters before anything is decoded. With a large build side, pass the key range instead.

## 7. Delta wiring

`DeltaReadOptions` (public):
- `FilterRows` (bool, default false): applies `Filter` exactly. The current superset contract stays the default, since tests and the host guide rely on it.
- `RowMasks` (`IReadOnlyList<DeltaRowMask>`): host delegates over **logical** column names (cmettler's join filters). They may over-approximate and never enter a read set (reads don't declare one anyway). `DeltaRowMask(IReadOnlyList<string> Columns, Func<RecordBatch, BooleanArray> Mask)`; `ValueWise` (init) came with phase 4c (§21).

Per file, the scan builds the Parquet `RowFilter`:
- **Split** `Filter` into top-level conjuncts, **in source order**:
  - Partition columns are folded to the file's typed partition values, and columns missing from the file to NULL.
  - A conjunct left with no column reference is evaluated once per file, on a one-row batch. TRUE drops the conjunct; FALSE or NULL skips the file, since no row can pass. File pruning alone can't stand in for this: it keeps a file whenever it can't decide, so `lower(p) = 'us'` survives pruning for a file partitioned on `p = 'ca'`. An error raises, as it would on the file's rows; an empty file raises nothing, since it has no rows.
  - Every other conjunct becomes a `RowPredicate` over physical leaf names.
  - Because `RowFilter` keeps NULL rows live (§3), splitting `n > 0 AND 1/z > 0` into two predicates still raises on a row with NULL `n` and zero `z`, as `ArrowRowEvaluator`'s `EvalAnd` does (`ShortCircuitEvaluationTests.AndStopsAtFalseAndOrAtTrue`).
- **Wrap each delegate** to rename physical to logical, and to widen types as `ReconcileToTableShape` would, on the predicate's own small batch. Then call `RowEvaluator.EvaluatePredicate`.
  - `ValueWise` is true when the conjunct has no function calls, or only functions the registry declares deterministic. That needs a new optional registry capability, like `IShortCircuitingFunctions`.
- **Order:** host masks after the `Filter` conjuncts. The conjuncts keep their source order unless reordering can't change which errors are raised. Moving conjunct A ahead of B means B is no longer evaluated on rows where A is FALSE, which suppresses an error B would raise there. So only conjuncts that **cannot raise** may move earlier: comparisons, IN and null tests of a column against a literal, with no cast, arithmetic or function call. Among those, the ones that can be evaluated value-wise over dictionaries go first. Selectivity ordering comes later, under the same rule.
- **Analysis errors:** `RefuseUnanalyzable` still runs on the whole predicate before the scan. The reader may stop evaluating a row group once no row is live, whereas `EvalAnd` evaluates every operand even over an empty selection, to surface an operand that can't be evaluated at all.
- **Deletion vector:** `ExcludedRows = DeletedRows`. `ProcessFileBatchesAsync` loses steps 3 and 4 above (its `KeptRows`/`Take`).
- **Row ids, versions and `_ew_row_address`** are built from `RowPositions`. Materialized row-tracking columns are ordinary projected columns, so they are masked by the same bitmap as everything else. This removes structurally the "masked out of step" hazard that `predicate-pushdown-design.md` warns about.
- **Filter-only columns** are read and dropped by the reader. `GetReadSchema` doesn't change.
- **Codec seam:** an `IDataFileReader` gets no predicate. Post-filter with the same `RowPredicate` list there. That post-filter is also the test oracle.

DML:
- **DELETE** becomes `ReadSelectedAsync` with `Columns = []`, `RowFilter` set to the predicate, `ExcludedRows` set to the deletion vector, and gets positions only, with no output decode, and possibly no predicate-column materialization (dictionary). Footer row counts are unchanged (#439).
- **UPDATE and MERGE** still need whole files, but can pass `ExcludedRows` instead of `Take`-filtering.
- **Worth verifying first:** DELETE today evaluates the predicate on deleted rows and masks them out afterwards. A value error on a deleted row (an ANSI cast) would then fail a DELETE that Spark, which filters deletion-vector rows first, lets through. The new path fixes that, but check it on main with a test.

## 8. Phasing

| Phase | Content | Gate |
|---|---|---|
| 0 | `rowfilter-ab` harness next to `pageindex-read-ab`. Vary selectivity (point, 10%, 50%, 90%, 99%), clustered vs scattered, projection width, deletion vector on/off, local vs latency/transfer model. Measure the current post-filter baseline. | Numbers in the PR |
| 1a | *(Revised after phase 0, §11.)* `ReadSelectedAsync` + `RowFilter` + `IExcludedRows` + `RowPositions`, with evaluation and compaction moved **inside the row-group unit**. That means they run in parallel across row groups, not on the consumer's thread. Still one decode pass and one fetch, so no I/O risk. A batch whose selection is all set is passed through, not gathered. | Oracle = full read + post-filter, over the test matrix in #502. Mutation: positions off by one; a batch's mask applied to its neighbour |
| 1b | *(Built as described in §13: decided per row group after a whole-group first pass.)* Lazy decode: within a fetched row group, decode the output columns only for pages holding a selected row, then decode-then-compact. Fetch filter and output pages in **one** request by default. Fetch them in two stages only where pass 1 can be expected to save more bytes than a round trip costs (§11, F5). Nested columns are decoded whole. | Same oracle. Mutation: pass 2 decodes the unmasked selection. `rowfilter-ab` local and cloud |
| 2 | In-decoder selection (port the prototype to the batched path); run/bitmap switch. | `selection-ab` on the batched path; no regression at 99% kept |
| 3 | *(Split into 3a exact scans, §16; 3b `RowMasks`; 3c DELETE.)* Delta: `FilterRows`, `RowMasks`, deletion vector via `ExcludedRows`, positions-only DELETE. | Delta suites; Spark interop oracle for DELETE counts |
| 4 | Dictionary evaluation (§6) + `IDeterministicFunctions`. | `OneCategory` profile; the dictionary-error fallback test |
| 5 | Fusion of same-column conjuncts, adaptive demotion of a predicate that keeps >~95% (evaluate it after pass 2 instead), limit pushdown, then a decision on whether `FilterRows` should default on. *(Re-planned in §24 as 5a-5g.)* | Measured crossover |

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
| clustered 1% narrow | 1% | 4.9 | 0.7 | 0.4 | 0.7 | +6% | 0.3 | 0.1 |
| clustered 90% narrow | 90% | 5.3 | 16.6 | 3.8 | 11.6 | -30% | 2.0 | 11.1 |
| clustered 90% wide | 90% | 24.8 | 64.2 | 3.8 | 37.8 | -41% | 2.0 | 41.4 |
| scattered point narrow | 0% | 5.3 | 11.3 | 6.8 | 7.1 | -38% | 4.5 | 0.2 |
| scattered 0.1% narrow | 0.1% | 5.0 | 9.2 | 4.6 | 9.9 | +7% | 2.3 | 0.3 |
| scattered 1% wide | 1% | 24.7 | 31.1 | 4.9 | 29.7 | -9% | 2.5 | 3.9 |
| scattered 10% wide | 10% | 23.9 | 40.3 | 5.7 | 32.6 | -18% | 3.6 | 15.1 |
| scattered 50% narrow | 50% | 5.5 | 23.6 | 10.3 | 17.6 | -26% | 8.1 | 9.3 |
| scattered 99% narrow | 99% | 5.3 | 26.8 | 4.6 | 13.0 | -50% | 2.4 | 17.4 |
| scattered 99% wide | 99% | 24.5 | 74.0 | 4.8 | 43.8 | -37% | 2.4 | 44.4 |
| scattered point wide | 0% | 23.8 | 29.6 | 6.9 | 7.9 | -73% | 4.8 | 0.1 |
| category = 2% wide | 2% | 25.1 | 31.4 | 7.9 | 32.9 | +5% | 3.8 | 6.0 |
| category <> 98% wide | 98% | 25.8 | 67.9 | 8.0 | 45.4 | -30% | 3.8 | 42.1 |

**Cloud model** (20 ms per GET through `CoalescingFileReader`, 100 MB/s), milliseconds:

| Case | post | late | late vs post | GETs post / late | MB read post / late |
|---|---|---|---|---|---|
| clustered 0.1% narrow | 84 | 159 | +87% | 4 / 6 | 0.2 / 0.2 |
| clustered 10% wide | 151 | 216 | +43% | 5 / 7 | 8.3 / 8.3 |
| scattered 1% wide | 316 | 347 | +9% | 6 / 14 | 75.5 / 75.5 |
| scattered point wide | 318 | 169 | -47% | 6 / 16 | 75.5 / 9.5 |
| scattered 99% wide | 354 | 356 | +3% | 6 / 14 | 75.5 / 75.5 |

Timings exclude the correctness check. Each case's strategies are checked against each other in a separate untimed run (count plus an `id` checksum); an earlier version of the harness ran the checksum inside the timed region, which added 2–6 ms to `post` at high selectivity.

Excluding 1% of rows at random (as a deletion vector would) changed none of these comparisons. The harness clears excluded rows with a naive per-row loop, so it says nothing about the cost of deletion vectors themselves.

### Findings

- **F1. Today's exact read is bound by the consumer thread, not by decoding.** `post − none` is about `eval + gather`. The gather (index list, `Take`, projection) costs 17.4 ms narrow and 44.4 ms wide at 99% kept, while the reader decodes four row groups in parallel. `late` gains 30–51% at ≥90% kept while decoding the *same* bytes, because its gather runs inside per-row-group tasks. So evaluation and compaction belong inside the row-group unit (`ReadPlannedAsync` under `OrderedScan`). This is phase 1a: the biggest local win, with no I/O change. Separately, `ArrowCompute.Take` allocates its output in managed memory (181 MB managed for the post-filter at 99% wide), which is worth its own look.
- **F2. Page-level skipping does nothing for scattered selections at ≥0.1%.** With 20,000-row pages, every page holds about 20 or more matches. Only the point lookup skipped pages: 5.8 MB read instead of 21.5, −38% narrow, −73% wide. Phase 1b alone does not serve the host's join filter. Phase 2 (selection inside the decoders) does, and it has to come next.
- **F3. The evaluator is no longer the bottleneck.** #502 asked for this re-measurement. `r < k` costs 1.2–4 ms per million rows, and `category = '…'` about 2 ms, down from 40 ms per million before #560. The cost peaks at 50% kept (8.1 ms per 2M rows against 2.4 ms at 1% or 99%), which suggests a branchy comparison or `SelectionMask.And` loop. Check it before tuning.
- **F4. Most of an exact read is decoding and gathering what the filter discards.** `pass1` (filter columns plus evaluation) is 4.6–10 ms, against 9–85 ms for `post` on unclustered predicates.
- **F5. On object storage, a second dependent fetch costs more than lazy decoding saves.** `late` was 43–87% slower on small clustered selections, because pass 2 waits for pass 1, plus the emulation's extra OffsetIndex request. Only the point lookup, which saved 66 MB, won. The design therefore separates I/O from decoding: fetch filter and output pages together by default, and decode output pages lazily. Split the fetch only when pass 1 can be expected to empty enough pages to save more than a round trip. That is roughly `bytes saved / bandwidth > latency`, learned from earlier row groups of the same scan. Clustered predicates are already narrowed by the page index before any fetch.
- **F6. Dictionary strings cost about 3 ms more in pass 1.** `pass1` is 7.8 ms for `category` against 4.7 ms for the integer key, consistent with the #608 profile. That is phase 4's target.
- **F7. A projected filter column decoded twice costs up to 50% more decoding on narrow clustered reads** (24 MB against 16 MB at 50%). Keeping pass-1 arrays (§4 step 2) is worth it.

## 12. Phase 1a measurements (2026-11-09)

`selected` in `rowfilter-ab` is phase 1a: `ReadSelectedAsync`, with the predicate as one `RowPredicate` and as `Prune`. It does the same decode as `post`, but the mask and the gather run inside each row group's read, in parallel. Same machine and data as §11, median of 7 rounds.

**Local**, milliseconds:

| Case | post | selected | vs post |
|---|---|---|---|
| clustered 1% narrow | 0.7 | 0.6 | -9% |
| clustered 90% narrow | 16.4 | 5.7 | -65% |
| clustered 90% wide | 59.0 | 22.4 | -61% |
| scattered point wide | 30.6 | 27.1 | -14% |
| scattered 1% narrow | 10.2 | 7.4 | -26% |
| scattered 1% wide | 28.9 | 26.4 | -11% |
| scattered 50% narrow | 24.9 | 12.8 | -52% |
| scattered 99% narrow | 26.2 | 14.5 | -45% |
| scattered 99% wide | 71.4 | 41.2 | -42% |
| category = 2% narrow | 14.0 | 9.4 | -33% |
| category <> 98% wide | 62.0 | 39.9 | -31% |

Phase 1a is faster than `post` in every one of the 30 cases: by 1–65%, and by 31–65% at ≥90% kept. Against the two-pass emulation (`late`): it is ahead at low and middle selectivity, where `late` pays for its second pass. At ≥90% kept the two are within a few points of each other, either way, since both gain from parallel gathering. It falls clearly behind only on the scattered point lookup, where page skipping is what helps (−73% for `late`, against −14%). That is phase 1b's case.

**Cloud model** (20 ms per GET, 100 MB/s): between −14% and +10% against `post`, as expected for a read bound by request latency. The small clustered reads came out 1–10% slower, about 7 ms. `pass1`, which reads only the filter column, shows the same gap, so it is not the filtering. It is unexplained, and should be looked at before the default is flipped.

## 13. Phase 1b: what was built, and measurements (2026-11-09)

### What it does

A selected read goes in two passes for a row group when four conditions hold:
- the group is read whole (pruning narrowed nothing);
- every column read is a flat top-level column;
- some output column is not read by any predicate;
- no returned predicate column comes back as a view or dictionary type.

The two passes (`ParquetFileReader.TwoPass.cs`):

1. **Pass 1** decodes the predicates' columns for the whole group as one batch, as phase 1a does, and evaluates the filter once for the group.
2. **The pass-2 decision.** The group is cut into blocks of 8,192 rows (`TwoPassBlockRows`). When the blocks holding a selected row span at most half of the group (`TwoPassMaxCoveredShare`), the other output columns are decoded only over those blocks, through the existing range read. Otherwise they are decoded whole, as one pass would.
3. **One fetch.** Pass 2's bytes, the output columns' whole chunks, are requested when pass 1 starts, through `PrefetchedFile`. Pass 2 then reads from memory, and deciding what to skip costs no round trip.

Other rows are gathered as in phase 1a. A group whose rows are all kept passes the predicate columns through rather than gathering them.

### First attempt, discarded

Both passes cut into 8,192-row batches, with every batch's output decode skipped or done separately. Locally that was **40–170% slower** than one pass on everything but point lookups. The batched read over a dense selection gives up the whole-group decode, and it decodes 20,000-row pages more than once across batch boundaries. Deciding after a whole-group first pass removed most of that cost.

### Measured

Same harness and machine as §11 and §12: `twopass` against `selected` (1a), median of 7 rounds locally and 5 under the cloud model.

| Case | Local, 1a ms | Local, 1b ms | vs 1a | Cloud vs 1a |
|---|---|---|---|---|
| scattered point narrow | 8.2 | 6.9 | -19% | -21% |
| scattered point wide | 26.7 | 16.8 | -35% | -19% |
| scattered 1% narrow | 7.4 | 8.4 | +11% | -22% |
| scattered 1% wide | 26.2 | 28.3 | +9% | -9% |
| scattered 99% wide | 40.4 | 42.1 | +7% | -3% |
| category = 2% wide | 26.2 | 30.5 | +16% | -3% |
| clustered 90% narrow | 5.2 | 6.6 | +27% | -6% |
| clustered 90% wide | 22.2 | 25.3 | +12% | -1% |
| clustered 1% (page-index narrowed, one pass) | | | ±2% | ±1% |

- **Point and near-point lookups** are the case 1b is for. They gain 19–35% locally and 19–21% in the cloud model, where pass 2 decodes two blocks instead of 500,000 rows.
- **Dense selections** cost between −1% and +16% locally, and up to 27% for row groups the predicate keeps whole. Within a row group, pass 1's columns now decode before pass 2's instead of alongside them, so a group loses some of its column parallelism. The per-column gather and the join add the rest.
- **The cloud model** is 0–28% faster in every case but one (clustered 10% wide, +5%). Part of that is the harness: `LatencyCountingFile` charges each GET its own transfer time, so pass 2's prefetch, a second request in flight beside pass 1's, effectively doubles bandwidth. Real object stores do serve concurrent GETs in parallel, but treat the size of this gain as an upper bound.
- **Unchanged:** page-index-narrowed groups (clustered ≤10%) take the one-pass path.

### Decided: on for object storage only

By default, a selected read uses two passes only for a file that reports object-store concurrency: `IReadConcurrencyHint.PreferredConcurrentReads`, which the S3, Azure and GCS readers report directly and the coalescing and known-length wrappers pass on. Local files read in one pass, as in phase 1a. The internal `ParquetFileReader.TwoPassReads` (a `bool?`) forces two passes either way, for tests and the harness.

The trade-off decided it. Locally, two passes cost a dense selection up to 16%, while on object storage they were faster in all but one case, and point lookups gained in both.

Revisit once phase 2 lands: selection inside the decoders should remove most of pass 2's cost on dense selections too, which may settle this.

## 14. Phase 2a: keeping rows as they are decoded (2026-11-09)

Phase 2 is split in two. **2a** (this) cuts the selection into a column as it is built. **2b** (next) skips dropped values inside the decoders that can skip, so they are never decoded at all.

### What it does

`ColumnBuildState.KeepRows` compacts a flat column's decoded rows in place before the Arrow array is built. It compacts the definition levels and the values together, in every value layout:
- bit-packed booleans;
- fixed widths: 4 and 8 bytes, INT96's 12, and any FIXED_LEN_BYTE_ARRAY length;
- byte arrays with 32-bit or 64-bit offsets;
- 16-byte views.

It runs before the build-time INT96 and extended-timestamp conversions, so they see only the kept values. `ReadColumn` takes a per-row keep mask, and `ReadBatchesAsync` passes it on its whole-group single-pass path.

The two-pass read's dense second pass uses it when the second pass is one batch and keeps **at least half** the group's rows (`TwoPassMinKeptShareToCompact`). The batch then comes back holding only the selected rows, with nothing to gather.

Below half kept, the gather stays. Compacting walks every row and value; a gather walks only the kept ones. The first version compacted at every selectivity and was 5–19% *slower* than the gather on 0.1–10% selections.

### Found on the way: a bug on `main`

With view output, `ColumnBuildState.AddByteArrayValues` counted every DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY and FSST value twice: `WriteOneStringView` counts each value, and the branch then added the count again. The view build derives the null count from that count, so for example `delta_byte_array.parquet` reads back with `NullCount` −1000 on a column with no nulls. Fixed here, and pinned by `KeepRowsTests.AViewColumnsNullCountMatchesItsRows`.

### Measured

`rowfilter-ab`, local, median of 7 rounds. Two-pass against 1a's one-pass read:

| Case | 1b vs 1a | 2a vs 1a |
|---|---|---|
| scattered 90% narrow | +1% | -3% |
| scattered 99% wide | +4% | -13% |
| scattered 90% wide | +4% | -13% |
| clustered 50% wide | +12% | -11% |
| category <> 98% wide | +11% | -1% |
| scattered point wide | -37% | -37% |
| scattered 1% narrow | +14% | +10% (gather path, unchanged) |
| clustered 90% narrow (groups kept whole) | +27% | +27% |

So two passes now beat one at high selectivity as well as for point lookups. The remaining local cost is at low selectivity, which 2b's skipping decoders target, and in row groups kept whole, where a group's two passes decode one after the other. Two passes stay the default on object storage only (§13).

## 15. Phase 2b: skipping dropped values in the decoders (2026-11-09)

### What it does

The selection is now applied **page by page** as a column decodes, replacing 2a's whole-chunk compaction (`ColumnBuildState.SetRowSelection`, `BeginPage`, `EndPage`).

After a page's definition levels are read, its deselected rows' levels are dropped and a mask over its values is built. Then, depending on the encoding:
- **Dictionary pages:** every index is read and checked, as before. The index array is then compacted to the kept rows, and the existing per-type write code runs over only those. Booleans, every fixed width and all three BYTE_ARRAY output kinds come for free, and a dropped value is never looked up.
- **PLAIN BYTE_ARRAY pages:** every value is measured and checked by `PlainDecoder.MeasureByteArrays`, as before; only kept values are copied.
- **DELTA_LENGTH_BYTE_ARRAY pages:** every length is decoded, as before; only kept values are copied.
- **Every other encoding** is decoded whole and its page compacted at once, while still in cache: PLAIN fixed widths, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY (whose prefix carry needs every value), BYTE_STREAM_SPLIT, ALP, PFOR, FSST and RLE booleans.

The skipping decoders reuse the real decoders' validation instead of duplicating it, so a malformed page fails the same way whether or not its rows are kept.

### The 50% gate is gone

2a compacted in the decoder only when at least half the rows were kept, because a gather of a few rows beat a pass over all of them. With values skipped, compacting in the decoder now beats the gather at every selectivity measured:

| Case | Two-pass vs one-pass, gated (2a's rule) | Ungated (2b) |
|---|---|---|
| scattered 0.1% wide | +5% | -18% |
| scattered 1% wide | +2% | -19% |
| scattered 10% wide | +6% | -19% |
| scattered 50% wide | -5% | -19% |
| scattered 99% wide | -15% | -20% |
| scattered 10% narrow | +9% | -7% |
| scattered 50% narrow | +5% | -10% |
| category = 2% wide | +11% | -8% |
| category <> 98% wide | -10% | -16% |
| scattered point wide | -31% | -33% |
| clustered 90% narrow (groups kept whole) | +22% | +16% |

Locally, two passes now beat one at almost every selectivity. The exceptions:
- row groups the predicate keeps whole (clustered 90%, +4% to +16%), where a group's two passes decode one after the other;
- a few narrow cases within a few percent (scattered 0.1% narrow +4%, category = 2% narrow +3%).

### Tests

`KeepRowsTests` (the fixture sweep) now runs through the per-page path. A new many-page test covers DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and FSST, nullable, beside a nullable boolean, under each output kind. No fixture has many pages of these, and a mutation of byte-array compaction past the first page survived the fixture sweep until this test existed.

### Open: the default

Two passes are still the default only on object storage (§13), decided before this phase with a note to revisit. Locally they now win nearly everywhere; see the table above.

## 16. Phase 3a: exact Delta scans (2026-11-09)

Phase 3 is split in three: **3a**, exact scans (this); **3b**, host row masks (`RowMasks`); **3c**, DELETE through a positions-only selected read.

### What it does

**`DeltaReadOptions.FilterRows`** (default false) makes `Filter` exact: a scan returns only the rows the filter answers TRUE for. Without it, `Filter` still only prunes.
- The filter is checked up front with the same analysis refusal a DML predicate gets (`RefuseUnanalyzable`). An invalid filter therefore fails before any file is read, whatever the files contain.
- It is evaluated with the same Spark-configured evaluator as DML.

**Built-in reader, per file** (`OpenDataFileAsync`):
- The filter's logical columns are mapped to the physical columns present in that file, by name or by field id.
- The whole filter becomes **one** `RowPredicate`. Its delegate renames the batch to logical names, brings it to the table's shape for exactly the filter's columns (`ReconcileToTableShape`: partition values, schema-evolution NULLs, widened types), and evaluates. Keeping the filter whole keeps the evaluator's AND semantics exactly; splitting into conjuncts only pays once phase 4's dictionary evaluation exists.
- A filter that reads **no** column of the file (only partition columns, or columns added after the file was written) is asked once per file, over one row of the table's shape. The file is then read unfiltered or skipped whole.
- The deletion vector is the read's `ExcludedRows`: `DeletionVectorBitmap` now implements `IExcludedRows`.
- Row groups are planned with the selected read's columns, and each scan unit reads its row group through `ParquetFileReader.PrepareSelectedAsync`'s per-plan reader. This is a Parquet-internal refactor that `ReadSelectedAsync` now shares.
- `ProcessSelectedFileBatchesAsync` derives each row's position, row id and commit version from `RowPositions`. Materialized row-tracking columns are ordinary read columns, so they are cut by the same selection. Metadata columns cannot fall out of step with their rows.

**Codec seam:** the seam returns every row, so it reads the filter's columns as well. The filter is applied to each batch once it has the table's shape, and the batch is then cut back to the projection.

### Tests

`ExactFilterTests` compares an exact scan against a superset scan of the same table, filtered row by row with `DeltaTable.MaskFor`, comparing rows, values, positions and row ids. The table has three files, partitions, a deletion vector, row tracking and a nullable column. The cases are:
- 7 filters: data columns only, partition only, mixed OR, nullable, two columns, nothing, everything;
- × 3 projections, two of which leave the filter's columns out;
- × serial, parallel and codec seam.

On top of those, there are cases for:
- name-mode column mapping;
- a filter on a column added after some files were written;
- refusal of an unanalyzable filter;
- `Filter` without `FilterRows` still being a superset.

Mutations each fail tests:
- row ids from contiguous positions;
- the once-per-file answer inverted;
- no deletion vector;
- the seam's filter skipped;
- the seam's projection not cut back;
- positions off by one.

The seam-projection mutation survived until the test also asserted that the returned data columns are exactly the projection.

### Not yet

- **Conjunct splitting, value-wise dictionary evaluation and selectivity ordering:** phase 4 and later.
- **`RowMasks`:** 3b, §17.
- **DELETE through the selected read:** 3c, §18.
- **Performance:** the Delta scan now gets 1a–2b's machinery, but this PR records no Delta-level numbers. `FilteredScanBenchmarks` is the place to measure it.

## 17. Phase 3b: host row masks (2026-11-09)

### What it does

**`DeltaReadOptions.RowMasks`** (`IReadOnlyList<DeltaRowMask>`) applies the host's own row conditions, cmettler's join filters among them. A scan returns only the rows every mask answers TRUE for; FALSE or NULL drops a row.
- **When:** masks apply whether or not `FilterRows` is set. With it set they come after the filter, in the order given.
- **What a mask sees:** `DeltaRowMask(Columns, Mask)` names top-level **logical** columns, and the batch holds exactly those, in that order, each as the scan returns it:
  - partition columns carry the file's value;
  - columns added after a file was written are NULL;
  - deleted rows are never passed;
  - a string or binary column read from the file follows `ParquetReadOptions.ByteArrayOutput` (a view or large-offset array), while partition values and backfilled NULLs are plain arrays. The scan's own batches have the same mix, although `GetReadSchema` reports the plain type for every one of them (#793).
- **Refusals:** a column the table doesn't have is refused before any file is read. A mask that answers a different number of rows than its batch raises.
- **Ownership:** the scan owns the returned mask, unless it is one of the batch's own columns.
- **Over-approximation:** a mask may keep rows that don't match (a Bloom filter's false positives). The host's join rechecks them anyway.

**Inside:**
- The `FilterRows` filter and each mask become one internal `RowCondition`: the logical columns it reads, and a function over a table-shaped logical batch. 3a's per-file logic now runs over that list.
- **Each condition reads at least one of the file's columns:** it becomes one `RowPredicate` of the file's `RowFilter`, in order. The reader therefore asks a mask only about rows the filter and the masks before it left live, and the other columns of rows it rejects can go undecoded.
- **NULL answers:** each condition is a whole one, so its NULL answer is turned into FALSE before the next condition runs. The Parquet `RowFilter` keeps a NULL row live for the predicates after it, and that is still right for conjuncts split out of one expression (phase 4); without the conversion, a mask after `n > 50` would be asked about rows where `n` is NULL.
- **A condition reads none of the file's columns** (only partition columns, or columns added after the file was written): its answer is the same for every row of the file, so it is computed once per file, over one row.
  - **Before any row-level condition:** it is asked while the file is opened, and only if the file has a live row. FALSE or NULL skips the file. In that case, a filter error on the file's rows that the filter would otherwise raise is not raised: the file is never read.
  - **After a row-level condition:** it waits until a row survives that condition. It becomes a predicate over the columns the earlier condition already reads, computed the first time it is handed a row and repeated after that. An earlier mask that rejects every row of a file therefore keeps it from being asked about that file at all. The reader also calls a predicate over zero rows once none is live; that call doesn't count.
- **Codec seam:** the conditions are applied in turn to each batch once it has the table's shape, each over the rows the ones before it kept. A batch a condition rejects whole is dropped without being gathered, so a column `ArrowCompute.Take` can't gather (a dictionary the host returned) doesn't fail a scan that returns none of its rows.

`DeltaRowMask` has no `ValueWise` flag yet: it would mean nothing until phase 4 evaluates dictionaries, and adding it then is not a breaking change.

### Tests

`ExactFilterTests` extends 3a's oracle to masks: the superset scan, filtered row by row by the filter and then by each mask over its own columns. The sweep is:
- 5 mask sets: a key column, two columns named out of table order, a partition column, NULL answers, two masks;
- × 3 filters: none, data, nullable;
- × 3 projections;
- × serial, parallel and codec seam.

Also tested:
- without `FilterRows`, the filter still only prunes while the masks apply;
- a mask is never asked about a deleted row (serial, parallel, seam);
- a mask is never asked about a row an earlier condition answered NULL for (serial, parallel, seam);
- a mask over only a partition column is not asked about a file whose rows an earlier mask all rejected (serial, parallel, seam);
- with view output, a mask's columns have the same Arrow types as the scan's (built-in, seam);
- through the seam, a batch a mask rejects whole, holding a dictionary column, is dropped without being gathered;
- a mask on a column added later sees NULL for the older files;
- an unknown column is refused before any mask is asked;
- a mask of the wrong length is refused;
- a mask over no column, or over a repeated one, is refused.

Each of these mutations fails tests:
- masks ignored;
- a mask's columns given in table order instead of its own;
- the once-per-file answer inverted;
- only a file's first condition applied;
- only the seam's first condition applied;
- the length check removed;
- the unknown-column refusal removed.
- NULL answers left live for the conditions after them (review round 1);
- review round 2:
  - a later partition-only condition asked up front instead of after the earlier conditions;
  - that condition computed for a zero-row call;
  - its answer inverted;
  - a rejected seam batch gathered.

### Not yet

- **`ValueWise` and dictionary probing for masks:** phase 4.
- **DELETE through the selected read:** 3c, §18.

## 18. Phase 3c: DELETE through the selected read (2026-11-10)

### Checked first: a bug on `main`

§7 suspected that DELETE evaluates its predicate on rows the file's deletion vector already deletes, and masks them out only afterwards. It does.
- **Repro:** a file with `s` = '5', 'abc', '7' and the 'abc' row deleted by a deletion vector. `DELETE WHERE s = 5` raised `CAST_INVALID_INPUT` on 'abc' (Spark's ANSI rules, which DML follows), though no live row raises. Spark filters deletion-vector rows before evaluating, so it deletes the one '5' row.
- **Scope:** UPDATE was checked the same way and is not affected; it already leaves deleted rows out before evaluating.

### What it does

`ComputeDeleteActionsAsync` matches each file's rows with `ReadSelectedAsync` instead of reading every row and masking afterwards:
- The predicate, an expression's mask or a caller's delegate, is the read's one `RowPredicate`. It reads only the columns the predicate references (every column for a delegate, or with a change feed). It goes through 3a's `ConditionMask`, so it sees each row as a scan returns it.
- The file's existing deletion vector is the read's `ExcludedRows`. A deleted row is never handed to the predicate, which fixes the bug above, and a row group the vector deletes whole is skipped unread, as before.
- **Without a change feed**, the read asks for no columns (`Columns = []`): it returns positions only, and nothing beyond the predicate's columns is decoded. The positions are the rows to delete, already excluding those the vector had.
- **With a change feed**, the read returns the matched rows whole, with the materialized row-tracking columns. They go to the change file as before, each row's id resolved from its materialized value or `baseRowId` plus its position.
- **A predicate that reads no column of the file** (only partition columns, or columns added after the file was written) is answered once per file, over one row of the table's shape, and only if the file has a live row. TRUE deletes every live row. That holds for an expression, whose answer can't differ between rows that have the same values. A function predicate is opaque, so on a file with no column it is asked about every live row instead, over batches of the table's shape (review round 3). With a change feed the rows themselves are needed, so the file is read through its own columns instead, which the predicate never sees: under column mapping, a file can hold none of the table's current columns once every column it was written with has been dropped (review round 1). A file with no column at all, which is what a table whose every column is a partition column writes, has nothing to read through. Its deleted rows go to the change file built from the table's shape alone: partition values and NULLs (review round 2).
- Row-group and page pruning by the predicate (`Prune`), the whole-file remove, copy-on-write rewrites and the vector itself are unchanged.

`ResolveRowTrackingAt` is gone. `MatchingLiveRows` is now used only by its own tests and the #610 deletion-vector benchmark.

### Tests

`DeleteSelectedReadTests`:
- an expression DELETE never evaluates a row the vector deleted, with the default mode, deletion vector and copy-on-write;
- a delegate DELETE is never handed one;
- with a change feed, the change file holds exactly the deleted rows, with their row ids when row tracking is on;
- a predicate over a column added after the file was written deletes the file's live rows, and only those;
- with a change feed, a file holding none of the table's current columns still records its deleted rows, next to a newer file's;
- with a change feed, a file with no column at all (a partitions-only table) still records its deleted rows, with row ids, next to a newer file's.
- a function DELETE of a file with no column is asked about every live row, and its per-row answer is applied (with and without a change feed).
- a delegate DELETE over a file that a copy-on-write rewrite gave materialized row-tracking columns is handed only the table's columns.

Each of these mutations fails tests across the DeltaLake.Table suite:
- the vector not passed as `ExcludedRows` (11 tests);
- positions off by one (167);
- change-file row ids from the batch index instead of the position (2);
- the once-per-file answer inverted (6).

Stripping the materialized columns before the predicate turned out to be redundant: `ReconcileToTableShape` already drops them. The test that guards it stays.

### Not measured

The delete no longer decodes the columns its predicate doesn't read, but this phase has no timings. The gate in §8 also names a Spark interop oracle for DELETE counts; the interop tiers run in CI.

## 19. Phase 4a: the filter as its conjuncts (2026-11-09)

Phase 4 is split in four: **4a**, the `FilterRows` filter split into its conjuncts (this); **4b**, the reader answering a value-wise predicate from the dictionary (§6); **4c**, `ValueWise` set on Delta conjuncts and `DeltaRowMask`; **4d**, chunks that mix dictionary and PLAIN pages, a projected predicate column, and an all-false lookup table as a prune.

Decided before 4a: a conjunct is value-wise when every function it calls `IsFoldable` (`IFoldableFunctions`: deterministic, and reads nothing but its arguments). No new `IDeterministicFunctions`.

arrow-rs (3db18f75b) has no counterpart in its reader: `ReadPlanBuilder::with_predicate` always decodes and calls `evaluate`. Its comparison and `like` kernels on a `DictionaryArray` evaluate every entry of `values()` once and gather through the keys, unused entries included, and nothing handles an error on an unused entry.

### What it does

With the built-in reader, the filter's top-level conjuncts (nested ANDs flattened) go into the Parquet `RowFilter` one each, in source order:
- **A conjunct over none of the file's columns** (only partition columns, or columns added after the file was written) is answered once, when the file is opened and only if it has a live row, wherever it stands in the filter. This is how Spark applies a partition filter, before it reads the file. TRUE drops it. FALSE or NULL skips the file, so a conjunct before it that would raise on the file's rows is never asked. If it raises, it raises right away when no row-level conjunct comes before it, as it would on the file's first row. Otherwise it is deferred to its place, and raises once a row survives the conjuncts before it, but only if it also evaluates over no rows: one that raises there (a wrong argument count, no common type) did not fail on a value, and raises at once. Likewise, before a file is skipped, the filter's other conjuncts are asked over no rows, as `EvalAnd` asks every operand over an empty selection: skipping a file saves its rows, not the analysis (review round 1). A conjunct with no column reference at all (`1 = 1`) is one of these.
- **Every other conjunct** is one row predicate. Its NULL answers stay live for the conjuncts after it (`RowFilter`'s three-valued AND), so `n + 1 > 1000 AND CAST(note AS INT) > 0` still raises on a row where `n` is NULL, as the whole expression does.
- **A host mask after the filter** must not see a row the filter answered NULL for. A new internal `RowPredicate.DropsEarlierNulls` makes the reader drop the rows answered NULL so far (live = all-true) before asking it. 3b turned each condition's NULLs into FALSE, which no longer works once a condition is several predicates: a row that is NULL on the first conjunct and TRUE on the second would reach the mask.

The codec seam still applies the filter whole: it has no dictionary to gain from.

### Measured: splitting wins except at very high selectivity

`ConjunctSplitBenchmarks` (Delta benchmarks): 4 files of 4 row groups x 131,072 rows, scattered uniform columns, the scan returns `id` only, `MaxConcurrentReads` 4. i9-12900K, net10.0, BenchmarkDotNet, 12 iterations. "Together" is the filter's row-level conjuncts as one AND, as 3a did.

| Filter | Kept | Together | Split | Split vs together |
|---|---|---|---|---|
| `amount < 100 AND qty < 90` | 9% | 16.3 ms | 9.3 ms | -43% |
| `amount < 500 AND qty < 50` | 25% | 24.6 ms | 15.2 ms | -38% |
| `amount < 900 AND qty < 90 AND category <> 'cat_03'` | 76% | 29.0 ms | 23.7 ms | -18% |
| `amount < 990 AND qty < 99 AND id >= 0` | 94% | 20.2 ms | 22.5 ms | +12% |
| `amount < 990` (one conjunct, control) | 99% | 11.5 ms | 11.4 ms | 0% |

The proposal for 4a had been to evaluate adjacent conjuncts together whenever neither gained from standing alone, to save the gather of live rows the reader hands each predicate after the first. The numbers reverse that: one AND in the evaluator costs more than the gathers do (it also allocates 2-4 times as much managed memory), except when nearly every row stays live. Every conjunct is therefore split. The 94% case is phase 5's: demote, or stop gathering for, a predicate that keeps almost everything. `DeltaTable.EvaluatesConjunctsTogether` (internal) keeps the old behaviour for this A/B and for tests.

### Tests

`ExactFilterTests`:
- 9 filters of several conjuncts (two data columns, a partition column first or last, `lower(region)` that file pruning can't decide, nested ANDs, `1 = 1`, an OR inside a conjunct, one column twice) x 3 projections x {serial and together, serial split, parallel split, seam}, against 3a's oracle;
- a later conjunct still raises on a row an earlier one answered NULL for (split, together, seam);
- a host mask after split conjuncts sees only rows the whole filter accepted;
- a partition conjunct that rejects a file skips it before an earlier conjunct raises on its rows;
- a file-constant conjunct that raises: up front when first; after a row-level conjunct, only if a row survives it (including a row that conjunct answered NULL for).
- a conjunct that can't be evaluated at all (`length(x, x)`) still raises when no row reaches it, and when a file-constant conjunct skips every file (review round 1; both failed before the fix).

Each of these mutations fails tests:
- conjuncts' NULLs turned into FALSE (3 tests);
- the reader's NULL barrier ignored (4);
- a deferred raise raised up front (1);
- a hoisted FALSE treated as TRUE (12);
- a deferred raise never thrown (2);
- a merged conjunct (`EvaluatesConjunctsTogether`) reading only the first conjunct's columns (13).

## 20. Phase 4b: answers from the dictionary (2026-11-10)

The reader answers a `ValueWise` predicate from a chunk's dictionary, as §6 proposed, within these limits (4d lifts some):
- **Which predicates:** one column, `ValueWise`, over a column no other predicate reads and the read does not return. `BatchSelector.Answerable` lists them once per scan.
- **Which chunks:** a flat top-level column whose `encoding_stats` prove every data page dictionary-encoded (`MembershipPredicateEvaluator.IsWhollyDictionaryEncoded`, as dictionary pruning decides it), not a second pass that keeps rows as it decodes.
- **Guard:** the dictionary holds fewer entries than the rows the read decodes from the chunk: the row group, or what the page index narrowed it to. That is §6's "selected rows" read as decoded rows, since the excluded rows and earlier predicates are not known when the chunk is decoded. EW's own writer falls back to PLAIN past 20% distinct, so the guard binds only on a narrowed read or another writer's file.

### How

- `Data.DictionaryAnswer.Prepare` builds the dictionary's values as the column's Arrow array, plus one NULL when the column is nullable, with the declared time units restored as the read restores them. It calls `Evaluate` once and keeps a lookup table of answers (TRUE, FALSE or NULL per entry, and the NULL row's). If it raises, or returns a mask of the wrong length, the chunk is read as values and the predicate is asked row by row, as before. So an error on an entry no live row uses (a value only an excluded row holds) never fails the read, and an error a live row's value raises still raises there. Building the dictionary falls back the same way: every entry is converted, where a read converts only the rows it decodes, so an INT96 entry past 2262 read as `timestamp[ns]` that no decoded row uses must not fail it (review round 1).
- The chunk is then decoded to its **indices only**: a `ColumnBuildState` in a new `DictionaryIndices` mode collects them as INT32, through the existing page walk (V1/V2, compression, CRC, definition levels). No value is looked up. A data page that is not dictionary-encoded contradicts the `encoding_stats` and raises `ParquetFormatException`.
- `Map` turns the indices into a `BooleanArray` of answers, which stands in the batch where the column's values would have been, under a field marked as answers. `BatchSelector` applies that column as the predicate's mask over every row: a row no longer live is unchanged by it, since its live and all-true bits are already clear. Both reads take this path: the one-batch read decides when it meets the dictionary page, and the batched read when it first decodes the column.
- **Asking every predicate, even over no rows** (§7, "Analysis errors"): the predicate was asked over the dictionary, so one that cannot be evaluated at all has already raised, or made the chunk fall back to row evaluation.

Nothing in Delta sets `ValueWise` yet. That is 4c, reusing `IFoldableFunctions.IsFoldable`.

### Measured

`dictanswer-ab` (Parquet benchmarks): `rowfilter-ab`'s file (4 row groups x 500,000 rows, 50 categories, default options), `category` filter-only, `ArrowRowEvaluator` as the predicate, `rows` = asked per row, `dict` = `ValueWise`. i9-12900K (24 logical), net10.0, local file, median of 9.

| Case | Kept | rows ms | dict ms | dict vs rows | 2-pass rows ms | 2-pass dict ms | dict vs rows |
|---|---|---|---|---|---|---|---|
| `= 'cat-07'`, returns `id, amount` | 2% | 10.9 | 7.7 | -29% | 10.3 | 7.4 | -28% |
| `IN` 5 categories, returns `id, amount` | 10% | 173.5 | 9.3 | -95% | 204.3 | 8.4 | -96% |
| `<> 'cat-07'`, returns `id, amount` | 98% | 21.6 | 20.4 | -6% | 21.6 | 19.4 | -10% |
| `= 'cat-07'`, positions only | 2% | 4.9 | 2.6 | -47% | 4.9 | 2.6 | -47% |
| `IN` 5 categories, positions only | 10% | 175.8 | 3.8 | -98% | 161.1 | 3.7 | -98% |
| `<> 'cat-07'`, positions only | 98% | 14.3 | 12.1 | -15% | 14.5 | 12.3 | -15% |

Positions only is a DELETE's read (3c). The `IN` rows are slow for a reason outside the reader: `ArrowRowEvaluator.EvalSet` still compares boxed `LiteralValue`s per row (166 MB allocated for 2M rows), where #560 gave comparisons their own kernels. The dictionary hides that, but row evaluation of `IN` should get a kernel of its own.

### Tests

`DictionaryAnswerTests` (Parquet):
- 5 answers (`=`, `<`, `IS NULL`, NULL for some values, nothing) x {one batch, batched} x {one pass, two} x {serial, parallel with 5% excluded and a second predicate after it}: the same rows as the predicate asked row by row, the predicate asked exactly once per row group over the 50 entries and the NULL, and the second predicate seeing the same rows (so a NULL answer stays live for it).
- Asked row by row when: not `ValueWise`; the column is returned; two predicates read it; the predicate reads two columns; the column is PLAIN-encoded, by choice or past 20% distinct.
- A dictionary of 1,500 entries is not evaluated for a read the page index narrows to one 1,000-row page.
- An entry only an excluded row holds raises over the dictionary and does not fail the read (one batch and batched); one a live row holds raises.
- An INT96 dictionary with an entry out of `timestamp[ns]`'s range declines without asking the predicate (review round 1; raised before the fix).

Each of these mutations fails tests: never answered (40); NULL rows answered NULL (8); no size guard (1); an error over the dictionary propagated (2); a returned column answered (1); a column two predicates read answered (1); a chunk not wholly dictionary-encoded answered (2); the NULL row's NULL answer turned into FALSE (8); a predicate that isn't `ValueWise` answered (41).

## 21. Phase 4c: value-wise conjuncts, DELETE and host masks (2026-11-10)

4c marks predicates `ValueWise`, so that 4b's reader can answer them from dictionaries:
- **Filter conjuncts:** a row-level conjunct of a `FilterRows` filter is value-wise when `DeltaTable.IsValueWise` holds for it. That means it is built only from column references, literals, the predicate forms (AND, OR, NOT, comparisons, IS [NOT] NULL/NaN, IN), and calls to functions the registry declares foldable. As decided before 4a, that reuses `IFoldableFunctions.IsFoldable`, which the Spark registry answers as `IsRegistered`: every function it registers is deterministic and reads only its arguments. Anything else, a variant path included, is not value-wise, which costs only the speed-up. A run evaluated together (`EvaluatesConjunctsTogether`) is value-wise when every conjunct in it is.
- **DELETE:** the predicate of an expression DELETE is marked the same way. A delegate DELETE is opaque, so it is not.
- **Host masks:** `DeltaRowMask.ValueWise` (public, init, default false) carries the host's promise through. Its remarks say what the mask may then see: values no live row holds, a deleted row's included. They also say that a mask raising over the dictionary is asked over the rows instead.

The partition and type-widening wrapper around each predicate (`ConditionMask`) works row by row, so it keeps a predicate value-wise. A deferred file-constant error and a once-per-file answer repeated over rows are not marked. `DeltaTable.AnswersFromDictionaries` (internal, default true) turns all of this off, for the A/B and as the tests' oracle. `DeltaTable.RowFilterObserver` (internal) shows tests each `RowFilter` a scan or DELETE builds.

### Measured

`ValueWiseBenchmarks` (Delta benchmarks), on `ConjunctSplitBenchmarks`'s table: 4 files of 4 row groups x 131,072 rows, with `category` holding 16 values, dictionary-encoded. The scan returns `id` only, with `MaxConcurrentReads` 4. i9-12900K, net10.0, BenchmarkDotNet, 12 iterations.

| Read | Kept | Rows | Dictionary | Change | Allocated, rows / dictionary |
|---|---|---|---|---|---|
| `category = 'cat_03'` | 6% | 7.5 ms | 4.4 ms | -42% | 3.0 / 3.2 MB |
| `category IN ('cat_01', 'cat_05', 'cat_09')` | 19% | 146.8 ms | 6.8 ms | -95% | 171 / 7.3 MB |
| `category <> 'cat_03'` | 94% | 12.9 ms | 9.1 ms | -29% | 31.1 / 31.4 MB |
| `lower(category) = 'cat_03'` | 6% | 188.4 ms | 4.3 ms | -98% | 392 / 3.2 MB |
| `category = 'cat_03' AND amount < 500` | 3% | 9.5 ms | 7.7 ms | -18% | 3.2 / 3.5 MB |
| value-wise host mask (a hash-set probe of 3 keys) | 19% | 24.2 ms | 5.4 ms | -78% | 87 / 7.0 MB |

The largest gains are where the row path is slowest: `IN` (#799) and a function call, which the evaluator takes row by row through boxed values. Over the dictionary, these cost the same as an equality.

### Tests

`ValueWiseTests` (Delta), on a table of 3 files x 2,000 rows (row groups of 1,000) with a dictionary-encoded `cat` holding 8 values and NULL, a `code` column whose 'x' only deletion-vector-deleted rows hold, and a partition:
- `IsValueWise` is true for comparisons, IN, IS NULL, NOT, `lower`, `CAST` and `length` arithmetic, and false for an unregistered function.
- 7 filters x 2 concurrencies x 2 projections return the same rows with and without `AnswersFromDictionaries`. Every predicate the scan builds is marked with it on, and none with it off. One filter is `CAST(code AS INT) > 3`, which raises over the dictionary (because of 'x') and falls back to the rows. A separate test shows it raises when a row holding 'x' is live.
- 3 DELETEs match the same rows either way, and their predicate is marked exactly when the switch is on.
- A value-wise host mask is asked once per row group over the 8 entries and the NULL, keeps the same rows, and is not answered from the dictionary with the switch off. One that raises on 'x' is asked over the rows instead.

Each of these mutations fails tests:
- conjuncts never marked (7);
- DELETE never marked (3);
- a mask's flag ignored (2), or not passed through `RowConditions` (2);
- the switch ignored for masks (1);
- an unregistered function taken as value-wise (1).

## 22. Phase 4d: answering a column the read returns (2026-11-10)

4d was proposed as three pieces: chunks that mix dictionary and PLAIN pages, a predicate column the read also returns, and an all-false table as a prune. This PR is the second, the one with a common case behind it (`SELECT category, ... WHERE category IN (...)`). The other two are left. EW's own writer never mixes encodings within a chunk: it dictionary-encodes a chunk whole or not at all. And for a filter, dictionary pruning (#57) already skips most of the row groups an all-false table would.

### How

A predicate over a column the read returns is now answerable too (`AnswerPlan.KeepValues`). Its chunk is decoded as ever, to its values, through a `ColumnBuildState` whose `IndexSink` collects each value's dictionary index as the value is written. That is one RLE decode for both. The indices take the values' definition levels, and are mapped to answers as in 4b. The answers come as one more batch column, `AnswerColumnName(column)` (a name starting with NUL; unlikely, but not reserved, so a returned column whose answer name the file already has is not answered, see review round 1). `BatchSelector` applies that column as the predicate's mask and drops it from the output.
- **Batched read:** a second cursor holds the answers beside each answered column's values, appended, restarted, skipped and cut in step with them.
- **View types:** the values are built by the column's own decoder, so view types need no special case.
- **First attempt:** this PR first built the values from the dictionary instead, with `ArrowCompute.Take` over the evaluated dictionary array. That cost more than the decode it replaced, with 38 MB of managed allocation where the decoder's native buffers held 1.7: `= 2%` measured +14% against asking the rows.

### Measured

`dictanswer-ab` (Parquet benchmarks), `category` returned, net10.0, median of 9 runs:

| Case | Kept | rows ms | dict ms | dict vs rows | 2-pass rows ms | 2-pass dict ms | dict vs rows |
|---|---|---|---|---|---|---|---|
| `= 'cat-07'`, returns `id, category` | 2% | 6.3 | 6.4 | +2% | 7.1 | 7.1 | 0% |
| `IN` 5 categories, returns `id, category` | 10% | 154.0 | 8.4 | -95% | 185.5 | 8.7 | -95% |
| `<> 'cat-07'`, returns `id, category` | 98% | 23.8 | 24.7 | +4% | 24.1 | 24.8 | +3% |

`ValueWiseBenchmarks` (Delta benchmarks), `Returned` true, with the table and settings of §21:

| Read | Kept | Rows | Dictionary | Change |
|---|---|---|---|---|
| `category = 'cat_03'` | 6% | 8.2 ms | 8.2 ms | 0% |
| `category IN ('cat_01', 'cat_05', 'cat_09')` | 19% | 151.1 ms | 9.1 ms | -94% |
| `category <> 'cat_03'` | 94% | 15.5 ms | 17.3 ms | +12% |
| `lower(category) = 'cat_03'` | 6% | 195.6 ms | 8.0 ms | -96% |
| `category = 'cat_03' AND amount < 500` | 3% | 10.7 ms | 10.4 ms | -3% |
| value-wise host mask (3 keys) | 19% | 26.4 ms | 8.9 ms | -66% |

Where the row path is a direct kernel (`=`, `<>`), the column is decoded either way, and answering from the dictionary saves only the kernel's compare. Mapping the indices and carrying the answers cost about as much, and at 94% kept a little more. Where the row path is slow (`IN`, a function call, a host delegate), the dictionary wins as it does for a dropped column.

**Left for later:**
- **Choosing between the two paths:** picking the row path when its predicate is a direct kernel needs a cost the reader does not have. That is phase 5's. *(Resolved in §27 without a choice: once the answers are mapped in one pass, the dictionary path is never slower than the row path beyond noise.)*
- **Materializing only the selected rows:** building a returned column's values for the selected rows alone (§6, step 3) would also help `=`. It needs the indices carried to the gather, and a second pass that can hand on a column it does not gather.

### Tests

`DictionaryAnswerTests`:
- the oracle sweep gains 20 cases returning the answered column, with batches of 2,500 rows that do not fall on the 1,000-row page boundaries;
- a returned view-type column is answered (one batch and batched);
- a returned column is answered over a read the page index narrows to two ranges, batched by 700.

Each of these mutations fails tests:
- the sink's NULL rows taken as values (18);
- answers looked up under the column's own name (22);
- the answer column returned (18);
- a returned column never answered (22);
- the batched answers restarted rather than appended (1).

One mutation passes: not skipping the answer cursor at all. Ranges from the page index are whole pages, so a cursor never has to skip into rows it holds; it only moves its start. That start mattered once (review round 1): the answer cursor was first made lazily, at the column's first decode, after that batch's skip had moved the values cursor to a range starting past row 0. Its start was then behind, and a later skip dropped answers still needed (`held.Take` threw). Both cursors are now made before the first batch. The regression, a narrowed read whose first range starts at row 2,000 with 300-row batches, failed before the fix.

### Review round 1

- **A file column with an answer column's name:** NUL is legal in a Parquet field name, so a file could hold a column called `AnswerColumnName("s")`. Projecting it beside an answered `s` dropped it. `BatchSelector` now takes the file's top-level names and does not answer a returned column whose answer name is among them; that predicate is asked row by row. Tested with such a file (it failed before the fix).
- **The lazily made answer cursor:** above.
- **CI build failure:** `CS1705` in `EngineeredWood.DeltaLake` (netstandard2.0). `System.Text.Json` and `Microsoft.Bcl.AsyncInterfaces` both float on `9.*`. nuget.org has STJ 9.0.23, which references AsyncInterfaces 9.0.0.23, but AsyncInterfaces stops at 9.0.22. This is unrelated to the PR and breaks every branch until the matching package is published, or until the two are pinned together.

## 23. End of the arc: oracle, Delta timings, the two-pass default (2026-11-10)

Three items were left for before `feature/row-level-filtering` merges to main.

### Spark DELETE counts (3c's gate)

`DeleteCountInteropTests` (tier 3) builds two identical tables. Each has a value in `s` that no integer cast reads ('abc', 'x9'), held only by rows a deletion vector already deleted. Spark runs a DELETE on one table and EW runs the same DELETE on the other. The four conditions are `s = 5`, `CAST(s AS INT) > 4`, `s = 5 OR p = 'b'` (a partition) and `s IS NULL OR CAST(s AS INT) < 3`. For each, EW deletes as many rows as Spark's `num_affected_rows`, and Spark reads the same ids from both tables afterwards. A premise test shows Spark's DELETE raising once such a row is live. All 5 ran and passed locally with `EW_REQUIRE_SPARK_INTEROP=1` (pyspark 4.0.4, delta-spark 4.0.0; 10m45s). Before 3c, EW raised `CAST_INVALID_INPUT` on these tables.

### Delta timings with `FilterRows`

`FilteredScanBenchmarks` and `LargeScanBenchmarks` (DeltaDotnetBenchmarks) gain `EW_FilterRows`. It runs the same query with `FilterRows` set, returning `id, amount`, beside the existing EW read that post-filters, prune-only, and delta-dotnet. `--verify` agrees on every row count. BenchmarkDotNet, net10.0, local disk:

| Case | Fixture written by | Post-filter | `FilterRows` | Change | delta-dotnet (64K batches) |
|---|---|---|---|---|---|
| Selective (1 file, 10,000 rows) | EW | 1.65 ms | 1.49 ms | -10% | 7.55 ms |
| Selective | delta-dotnet | 1.81 ms | 1.71 ms | -6% | 9.59 ms |
| OneCategory (1M rows, 6%) | EW | 7.07 ms | 4.53 ms | -36% | 15.77 ms |
| OneCategory | delta-dotnet | 7.32 ms | 6.22 ms | -15% | 19.33 ms |
| LargeOneCategory (10M rows) | EW | 56.5 ms | 37.2 ms | -34% | 81.8 ms |
| LargeOneCategory | delta-dotnet | 62.4 ms | 45.9 ms | -26% | 92.9 ms |

`OneCategory` was #608/#609's case. Its predicate is now answered from each chunk's dictionary (4b/4c), and `id, amount` are decoded only for the rows kept.

### The two-pass default for local files

Two passes are still gated to object storage (§13; decided 11-09, to be revisited at the end of the arc). `rowfilter-ab` (local, 7 rounds, i9-12900K, net10.0), `twopass` against `selected` (one pass), on this branch:

| Case | narrow | wide |
|---|---|---|
| clustered point / 0.1% / 1% | -12% / -6% / -5% | -6% / -4% / -2% |
| clustered 10% / 50% / 90% | 0% / +3% / **+24%** | +4% / -26% / **+10%** |
| scattered point / 0.1% / 1% | -12% / +7% / 0% | -33% / -17% / -15% |
| scattered 10% / 50% / 90% / 99% | -5% / -7% / -10% / -11% | -23% / -13% / -20% / -20% |
| category = 2% / <> 98% | -1% / -14% | -4% / -5% |

As §15 found, two passes win nearly everywhere locally. The exception is a selection that keeps row groups whole or nearly whole (clustered 90%: +24% narrow, +10% wide), where the second pass reads everything the first did not.

**Decided (2026-11-10):** the default stays as it is (two passes for object storage only) until phase 5. Demoting a predicate that keeps nearly every row should remove the clustered-90% regression, and the local default is revisited then. The feature branch does not wait on it.

## 24. Phase 5: the plan, and 5a (2026-11-10)

### The plan

§8 defined phase 5 as fusion of same-column conjuncts, demotion of a predicate that keeps more than about 95% (evaluated after pass 2), limit pushdown, and a decision on `FilterRows`'s default. Reading the code against §19, §22 and §23 found that a predicate keeping nearly every row costs three different things, and that each has a direct fix needing no learned selectivity:
- **A gather per later predicate (§19).** `BatchSelector` hands each predicate after the first a gather of the rows still live, because a predicate asked about a row an earlier one rejected could raise there. 5a, below.
- **Carrying dictionary answers for a returned column (§22).** Where the row path is a direct kernel, the dictionary only adds work. 5d.
- **Two passes run one after the other over row groups kept whole (§23).** On sorted data, row-group statistics prove those groups all-true. 5c drops a predicate for a group whose statistics prove it, as DataFusion does (`RowFilterSkippedFullyMatched`).

**Upstream (2026-11-10).**
- arrow-rs never reorders or demotes a predicate by observed selectivity. A cost-aware fallback (arrow-rs#9956) was closed unmerged, and the selectivity metrics issue (#8723) is open.
- arrow-rs fuses consecutive predicates over one flat leaf (#10859).
- arrow-rs pushes a limit into the last predicate and stops handing out row groups once it is met (#9766).
- DataFusion orders conjuncts by the compressed size of their columns. Its only adaptivity is statistics-based: a group proven fully matched gets an empty `RowFilter`. Its selectivity-tracking prototypes (#19639, #21752) were closed: one query 17x faster, others 1.26-1.41x slower, total time flat. The epic for it (#22883) is open.
- Neither project chooses between evaluating over a dictionary and over decoded values.

**The PRs:**

| PR | Content | Measured by |
|---|---|---|
| 5a | Whole-batch answers for conjuncts that cannot raise (below) | `ConjunctSplitBenchmarks`, `WholeBatchShareBenchmarks` |
| 5b | Fusion of same-column conjuncts. It also lets a dictionary answer them: today `category <> 'a' AND category <> 'b'` is asked row by row, since `BatchSelector.Answerable` requires a column read by one predicate | `ValueWiseBenchmarks` plus same-column cases |
| 5c | A predicate proven `AlwaysTrue` by a row group's statistics is dropped for that group; then the local two-pass default is revisited (§23) | `rowfilter-ab` local and cloud model, `FilteredScanBenchmarks` |
| 5d | Dictionary or row path for a returned column: profile the +12% first, else row path for a direct kernel | `ValueWiseBenchmarks` with `Returned`, `dictanswer-ab` |
| 5e | Reordering by observed selectivity, only if a measured gap (a 99% conjunct ahead of a 2% one) exceeds about 10% after 5a | the gap first |
| 5f | Limit pushdown, only if the read-ahead a consumer's early stop wastes is material | `LIMIT 10` over `LargeScan`, local and cloud model |
| 5g | The `FilterRows` default. It is also a behaviour change: a filter that today only prunes would be evaluated with ANSI rules, and could raise | the above, against the post-filter |

"Demote, and evaluate after pass 2" (§8) is replaced by 5a and 5c. Chunks that mix dictionary and PLAIN pages, or lack `encoding_stats`, are #804.

### 5a: what it does

- **`ArrowRowEvaluator.TryEvaluateWithoutRaising`** (public) answers a predicate for every row of a batch, or declines without reading a value. It answers only by a path on which no value can raise:
  - a comparison of a column with a constant that the direct kernels answer (#560). They read the column's buffers and never convert a row's value, and a coercion that would cast the column declines;
  - `IS [NOT] NULL` of a column;
  - AND, OR and NOT of these. Every operand is evaluated over every row, so AND and OR are Kleene's, which is what the short-circuiting evaluation answers too.

  It never raises an error a row's value causes, and an answer is exactly `EvaluatePredicate`'s. A decline is not a verdict: it may decline a predicate `EvaluatePredicate` would refuse, unchecked, and the caller then asks `EvaluatePredicate`, which raises it (review round 3). `MayEvaluateWithoutRaising` says from the form alone whether it can ever answer: a comparison qualifies only with a column on one side and a constant on the other, so `qty > id` is never shaped only to be declined (review round 1). The method is public rather than internal because an `Expressions.Arrow` IVT to `DeltaLake.Table` leaks PolySharp's polyfills.
- **`RowPredicate.EvaluateWholeBatch`** (internal) carries it to the reader. When at least `WholeBatchLiveShare` of a batch's rows are still live (default 0.4), `BatchSelector` asks the predicate about the whole batch rather than a gather of the live rows. Its answers for rows no longer live change nothing, exactly as with dictionary answers (§20). A decline falls back to the gather.
- **Delta** offers it for each run of filter conjuncts, and for an expression DELETE, whose deletion vector leaves most rows live, when the form can answer. The batch is first brought to the table's shape, as `ConditionMask` does. That shaping can itself raise on a value no live row holds: a DATE widened to TIMESTAMP_NTZ overflows, checked, on a far-future day count. So any error while shaping declines, and the live rows are asked instead (review round 1).
- **Knobs (internal):** `DeltaTable.EvaluatesWholeBatches`, `DeltaTable.WholeBatchLiveShare` and `ParquetFileReader.WholeBatchLiveShare`, all per instance.

### Measured

`WholeBatchShareBenchmarks` (Delta benchmarks): `ConjunctSplitBenchmarks`'s table, `amount < X AND <second>`, scan returns `id`, `MaxConcurrentReads` 4, dictionary answers off. i9-12900K, net10.0, BenchmarkDotNet, 2 launches x 25 iterations. "Live" is the share `amount < X` leaves for the second conjunct.

| Live | `qty < 99` (nullable INT32): gather / whole | Change | `id >= 0` (INT64): gather / whole | Change |
|---|---|---|---|---|
| 5% | 7.6 / 7.8 ms | +3% | 6.2 / 6.7 ms | +7% |
| 10% | 8.4 / 8.3 ms | -1% | 7.0 / 7.1 ms | +2% |
| 25% | 10.0 / 10.9 ms | +9% | 8.5 / 8.5 ms | 0% |
| 40% | 12.6 / 11.2 ms | -11% | 11.2 / 9.7 ms | -13% |
| 50% | 14.0 / 11.2 ms | -20% | 12.3 / 10.3 ms | -16% |
| 60% | 14.5 / 11.5 ms | -21% | 11.7 / 10.5 ms | -10% |
| 75% | 15.3 / 12.1 ms | -21% | 12.6 / 11.3 ms | -10% |
| 90% | 17.0 / 13.1 ms | -23% | 14.4 / 11.5 ms | -20% |

Hence the default of 0.4. The whole batch also allocates 18-32% less managed memory from 40% live up.

`ConjunctSplitBenchmarks`, §19's regression, with dictionary answers off in every mode (12 iterations; "whole" asks for the whole batch at any live share, "default" at 5a's 0.4):

| Filter | Kept | Together | Split | Split, whole | Split, default |
|---|---|---|---|---|---|
| `amount < 100 AND qty < 90` | 9% | 15.2 ms | 8.8 ms | 8.8 ms | 8.7 ms |
| `amount < 500 AND qty < 50` | 25% | 24.6 ms | 14.6 ms | 13.5 ms | 12.8 ms |
| `amount < 900 AND qty < 90 AND category <> 'cat_03'` | 76% | 29.3 ms | 22.6 ms | 16.0 ms | 15.9 ms |
| `amount < 990 AND qty < 99 AND id >= 0` | 94% | 21.2 ms | 23.7 ms | 14.2 ms | 13.9 ms |
| `amount < 990` | (one conjunct) | 11.3 ms | 11.1 ms | 11.4 ms | 11.1 ms |

At 94% kept, splitting alone still costs more than evaluating together, +12% as §19 measured; 5a's whole-batch answers turn that into -33%, and at 76% kept -45%. At low live shares the default threshold gathers, as it should, and matches the whole batch within noise.

Corrected 2026-11-11 (#832 review). This table was first measured with dictionary answers on, their default since 4b: Together 20.6 ms, Split 18.2 ms, Split whole 15.3 ms at 94% kept. That read as splitting itself costing 26% less than evaluating together. It didn't: a split leaves `qty < 99` as a single-column conjunct that `qty`'s dictionary answers, which an AND over three columns cannot use, so the comparison credited splitting with what the dictionary saved. The benchmark now turns dictionary answers off in every mode, as `WholeBatchShareBenchmarks` does; 4b's dictionary answers are measured on their own in §20-§21 (`ValueWiseBenchmarks`).

### Tests

- `WholeBatchEvaluationTests` (Expressions.Arrow):
  - Every case of `DirectColumnComparisonTests`' sweep under three evaluators (plain, ANSI, legacy) is answered as the general path answers, or declined, never raised except for an analysis error.
  - For 6 invalid predicates, a decline followed by `EvaluatePredicate`, as the reader does, raises exactly what `EvaluatePredicate` alone raises. Two are Copilot's examples: an IN with no common type, and a missing column in a covered comparison.
  - 14 compound forms answer or decline as expected.
  - A string column against a number under ANSI declines without raising.
  - The form check admits a column against a constant only (11 forms, and a column as the prefix of `STARTSWITH`).
- `SelectedReadTests` (Parquet):
  - The oracle sweep with whole-batch answers at any live share (8 filters x serial and parallel x one and two passes, 5% excluded).
  - The threshold decides which path a predicate is asked by (whole: every row, gathered never; below: live rows only).
  - A decline falls back to the live rows.
  - A whole mask of the wrong length is refused.
- `ValueWiseTests` (Delta):
  - 5 filters x 2 concurrencies x 2 projections return the same rows with whole-batch answers on and off, and are offered them.
  - `code <> 'x' AND CAST(code AS INT) > 3` over live 'x' rows never raises.
  - 3 DELETEs match the same rows either way, offered exactly when the form can answer.
  - A DATE widened to TIMESTAMP_NTZ, with a far-future day on a row an earlier conjunct rejects, and then on a row the deletion vector deletes: the scan and the DELETE succeed with whole batches on, as with them off. Each half raised `OverflowException` before the fix.

Each of these mutations fails tests:
- Delta offering every conjunct whole, through the evaluator that raises (5);
- Delta never offering (7);
- the reader ignoring its threshold (2);
- the reader's live and all-true bitmaps swapped for whole answers (8);
- Kleene NULL dropped (4);
- a declined comparison answered by the general path (3);
- `IS NULL` of any operand taken (1).

### Review round 1

- **Shaping a whole batch can raise.** `WidenDate32ToTimestamp` multiplies checked, so a DATE widened to TIMESTAMP_NTZ overflows on a day count past about year 294,000. On the whole-batch path that happened for rows the gather never reaches (rejected by an earlier conjunct, deleted by the vector). Shaping errors now decline. Regression above.
- **Comparisons that always decline were offered.** `qty > id` and `qty > CAST(code AS INT)` passed the form check, then were shaped and declined on every batch. The form check now requires a column against a structural constant.

### Review round 2

- **An empty batch hid errors no value causes.** `TryEvaluateWithoutRaising` declined any empty batch up front. So over a batch without `i32`, `i32 > 0` came back declined instead of raising as `EvaluatePredicate` does. The direct path also declines an empty batch before resolving its column, under an evaluator with no analysis rules. An empty batch is now answered by `EvaluatePredicate` itself, which reads no row's value and raises what it raises over any batch. The reader never asked about an empty batch: the whole path needs a live row, and a batch with none goes to `EvaluateOverNoRows`. So reads were unaffected; the public method's contract was not. `AnEmptyBatchRaisesWhatTheGeneralPathRaises` (5 forms x 3 evaluators) failed 4 cases before the fix.

### Review round 3

Two more paths where a decline hid an error no value causes:
- forms the method does not cover (`a IN (bl)` declined before any analysis);
- a covered comparison over a missing column (the direct kernel catches the lookup's `ArgumentException` and declines).

Round 2 was the same pattern on empty batches. The cause was the contract, not the code. The method promised that such errors "still raise", a promise no caller needs, because every caller treats a decline as "ask `EvaluatePredicate` instead", and that call raises them. The contract now says so:
- never an error a value causes;
- an answer equal to `EvaluatePredicate`'s;
- any error it does raise is one `EvaluatePredicate` raises too;
- a decline is not a verdict.

A separate analysis pass before each decline was rejected: it would cost the fast path on every batch, and change nothing a reader returns. Round 2's code stays: answering an empty batch through `EvaluatePredicate` is correct, just no longer needed by the contract.

## 25. Phase 5b: a run of conjuncts over one column, answered from its dictionary (2026-11-10)

### What it does

Before 5b, a column read by two predicates was never answered from its dictionary (`BatchSelector.Answerable` required a single reader). `category <> 'a' AND category <> 'b'` was therefore asked row by row: two string comparisons per row, and a gather for the second.

Now the reader groups a **run** of adjacent value-wise predicates over the same single column, none of which drops earlier NULLs, into one `AnswerPlan`. The column is answerable when its run is every predicate that reads it.
- **Evaluating the run.** `DictionaryAnswer` evaluates each predicate of the run over the dictionary and keeps their Kleene AND per entry: FALSE if any answers FALSE, else NULL if any answers NULL, else TRUE.
- **Applying it.** `BatchSelector` applies the answers at the run's first predicate through `AndThreeValued`, as for one predicate, and skips the rest. That is what asking them in turn decides: a row any of them answers FALSE is dropped, and a row answered NULL stays live for the predicates after the run but is never returned.
- **Errors.** Any predicate that raises over the dictionary makes the whole run decline, and the rows are asked as before.

**The fusion is in the reader, not in Delta.** One AND predicate built in Delta would go through the evaluator's general `EvalAnd`, which §19 measured slower than split conjuncts, and would bypass 5a's whole-batch path on chunks that are not dictionary-encoded. In the reader, a run's predicates stay separate wherever no dictionary answers them. This is arrow-rs#10859's fusion (consecutive predicates over one leaf), applied to dictionary answers.

**Only adjacent predicates.** Answering a later predicate where the first of the run stands would hide rows from the predicates between them, and with those rows the errors they would raise. A run is also cut at a predicate that drops earlier NULLs: a host mask is its own condition.

**Knobs (internal):** `ParquetFileReader.FusesAnswers` and `DeltaTable.FusesAnswers`, on by default.

### Measured

`FusionBenchmarks` (new, Delta benchmarks): `ConjunctSplitBenchmarks`'s table, scan returns `id` (the filtered column dropped) or `id` and the filtered column (returned), `MaxConcurrentReads` 4. i9-12900K, net10.0, BenchmarkDotNet, 15 iterations.
- `Rows`: dictionary answers off.
- `Separate`: answers on, fusion off. This is how the code behaved before 5b: the run is asked row by row.
- `Fused`: 5b.

| Filter | Kept | Column | Rows | Separate | Fused | Fused vs separate |
|---|---|---|---|---|---|---|
| `category <> 'cat_03' AND category <> 'cat_07'` | 88% | dropped | 13.0 ms | 14.0 ms | 9.3 ms | -34% |
| same, and `<> 'cat_11'` | 81% | dropped | 13.4 ms | 13.4 ms | 9.1 ms | -32% |
| `category >= 'cat_04' AND category < 'cat_08'` | 25% | dropped | 15.8 ms | 15.6 ms | 6.7 ms | -57% |
| `category <> 'cat_03' AND category <> 'cat_07'` | 88% | returned | 15.4 ms | 14.9 ms | 14.7 ms | -1% |
| same, and `<> 'cat_11'` | 81% | returned | 16.0 ms | 15.9 ms | 15.7 ms | -1% |
| `category >= 'cat_04' AND category < 'cat_08'` | 25% | returned | 16.6 ms | 16.7 ms | 9.9 ms | -40% |
| `qty >= 10 AND qty < 20` | 10% | dropped | 7.9 ms | 7.7 ms | 7.5 ms | -2% |
| `qty >= 10 AND qty < 20` | 10% | returned | 7.9 ms | 7.8 ms | 9.9 ms | +27% (rerun: +13%) |
| `amount > 100 AND amount < 900` (control: not dictionary-encoded) | 80% | either | | | | within noise |

**A returned integer column costs about 12% more answered from its dictionary.** A focused rerun (2 launches x 30 iterations, `qty` returned) measured 8.2 against 9.2 ms at 10% kept (+13%) and 13.4 against 15.0 ms at 80% (+12%). That is §22's cost: building a returned column's values beside its answers costs more than a direct integer kernel saves. Fusion adds no new kind of cost. A lone `qty < 20` over a returned `qty` has paid it since 4d; runs now pay it too. Choosing between the paths for a returned column is 5d's, and these numbers shape it: returned integers lose about 12%, returned strings break even on `<>` and win 40% on a range.

### Tests

- `DictionaryAnswerTests` (Parquet):
  - A run oracle sweep: 4 runs of 2-3 predicates (`<>`/`<>`, a range, NULL answers that a later member turns FALSE, `IS NULL` then nothing) x {one batch, batched} x {one pass, two} x {serial, parallel with 5% excluded}, dropped and returned. Each returns the same rows as the predicates asked row by row; each member is asked once per row group over the 50 entries and the NULL; and a predicate after the run sees the same rows.
  - A run whose second predicate raises over an entry only an excluded row holds declines, and is asked row by row; with that row live, it raises.
  - Asked row by row when two predicates over the column are not adjacent, when a predicate that drops earlier NULLs is between them, and when fusion is off.
- `ValueWiseTests` (Delta): 3 same-column filters (`<>`/`<>`, a range then another column, `IS NOT NULL` then `lower`) return the same rows with dictionary answers on and off.

Each of these mutations fails tests:
- only the run's first predicate evaluated (50);
- NULL combined as TRUE (64);
- a predicate dropping earlier NULLs joining the run (1);
- the run's later predicates asked as well (48);
- the switch ignored (1).

## 26. Phase 5c: dropping a predicate its row group's statistics prove (2026-11-10)

### Decided first: proofs follow `TrustStatisticsProofs`

Dropping a predicate where statistics prove it TRUE for every row trusts the statistics in a direction file skipping never needs. A wrong bound would make an exact scan return a row that does not match. `DeltaTableOptions.TrustStatisticsProofs` already gates that trust for DELETE and ADD CONSTRAINT, and defaults to off, following Spark, which trusts statistics only for skipping. **Decided (2026-11-10):** 5c follows the same option. DataFusion trusts such proofs in its pushdown scans; Spark rechecks every row. So by default nothing changes, and §23's regression remains for a table without the option.

### What it does

- **One set of rules for every proof** (`StatisticsEvaluator.ProvesEveryRowTrue` / `ProvesLeafEveryRowTrue`, public, `EngineeredWood.Expressions`). They are DELETE's rules, moved out of `DeltaFilePruner`:
  - AND needs every child proven, and OR one;
  - a leaf is a comparison of a top-level column with a literal of the column's own type (or, since #808, one that widens to it exactly), or `IS [NOT] NULL`;
  - every leaf but `IS NULL` also needs a null count of 0;
  - NOT and `IN` are never proven.

  The pruning evaluator's AlwaysTrue is unsound for this in two places, which the rules avoid. Under NOT, its AlwaysFalse ignores NULL rows: `NOT (x < 5)` over [10, 20] with NULLs answers AlwaysTrue, and a test pins that. A prefix test is answered without the null count. A NaN outside the bounds is the evaluator's to settle, and it already answers Unknown when one may hide there. `DeltaFilePruner.ProvesEveryRowTrue` keeps only its partition-column refusal (#511) and calls the shared leaf.
- **Parquet:** `RowPredicate.Proof` (internal) is a predicate over the file's column paths. For each planned row group, `SelectedPlanReader` drops the predicates its statistics prove (`ParquetStatisticsAccessor`, with pruning's trust rules: no bounds under an undefined column order, legacy min/max only for signed numerics). The group is then read with the remaining filter and only the columns it still needs (`ReadWithout`, cached per set of proven predicates). A group with none left is a plain read: one pass, no evaluation. A positions-only read (a DELETE's) of such a group reads no column at all.
- **A proof holds only for a literal of the column's own type** (DELETE's rule): `id >= 2500` over a BIGINT `id` compares with an INT literal and was never proven; `id >= 2500L` was. Spark SQL types an unsuffixed integer literal as INT, so this is the common case for BIGINT columns. **Since #808** a literal that widens to the column's type exactly is taken too: an INT over a BIGINT column, a FLOAT over a DOUBLE one. Spark compares in the wider type and the statistics compare the two exactly, so both answer as for a literal of the column's own type. This changes DELETE's and ADD CONSTRAINT's file proofs as well, in the same direction: more files proven, never a different answer.
- **Delta:** a run of filter conjuncts, and an expression DELETE's predicate, carry a proof only when the table trusts statistics proofs and `EvaluatesEverywhere` holds (plain comparisons and null tests of the table's own columns, none casting a string, so dropping it raises nothing it would have raised). The proof is translated by `RowGroupPushdown.ToFileColumns`, which never strengthens it: a reference the file cannot answer (a partition column, a missing column, a changed type) evaluates Unknown and proves nothing.

### Measured

`rowfilter-ab` (local, 9 rounds, i9-12900K, net10.0), the clustered cases, `twopass` against `selected` (one pass), with and without `EW_ROWFILTER_PROOFS=1` (the predicate carries its filter as a proof):

| Case | Without proofs | With proofs |
|---|---|---|
| clustered 90% narrow | +31% (6.5 vs 5.2 ms) | **-5%** (4.7 vs 4.9 ms) |
| clustered 90% wide | +12% (24.0 vs 21.6 ms) | **0%** (20.8 vs 21.2 ms) |
| clustered 50% narrow / wide | +4% / -19% | +10% / -25% |
| clustered point / 0.1% / 1% / 10% | -11% to 0% | -8% to +1% |

The regression §23 left for phase 5 is gone where proofs are trusted. Row groups kept whole are proven, and are read in one pass with no evaluation. The one-pass read itself gains little (5.2 to 4.9 ms narrow): the predicate's evaluation is all it skips there. The 50% rows hold one partial row group each and moved within the noise of a single row group.

### The local two-pass default (§23's open decision)

By default no proof is offered, so a default table would still read clustered 90% locally at +31% narrow and +12% wide with two passes. §23 decided to revisit the local default once phase 5 could remove that regression, and phase 5 removes it only for tables with `TrustStatisticsProofs`. The options were:
- keep two passes for object storage only;
- turn them on locally regardless, accepting +12-31% on row groups kept whole;
- turn them on locally only where proofs are offered.

**Decided (2026-11-10): the third.** A selected read of a local file goes in two passes when its predicates carry proofs (`UsesTwoPasses(proofs)`; `SelectedPlanReader` passes whether any does). Object storage is unchanged, and a local scan without proofs still reads in one pass. With proofs, the local two-pass numbers are the `With proofs` column above for clustered selections, and §23's for scattered ones, which no proof covers: -4% to -33%, except scattered 0.1% narrow, +7%.

### Tests

- `StatisticsEvaluatorTests` (Expressions):
  - a leaf with no NULLs is proven, and NULLs block every leaf but `IS NULL`;
  - NOT is never proven (the pruning evaluator's AlwaysTrue under NOT is pinned beside it);
  - AND needs every child, OR one;
  - `IN`, a literal of another type, and a column as a prefix are not proven;
  - a prefix test needs no NULLs.
- `SelectedReadTests` (Parquet):
  - a predicate proven for row groups 1-2 (sorted `id`) is asked only about group 0, with the oracle's rows: alone and with a second predicate before or after it, one pass and two, serial and parallel, 5% excluded;
  - a proof the statistics do not establish (a column with NULLs, an unsettled range) leaves every row asked;
  - a positions-only read of proven groups returns their live positions with no column read.
- `ValueWiseTests` (Delta):
  - 5 filters (a range, a run, an OR, a conjunct proven beside one that is not) x 2 concurrencies return the same rows with `TrustStatisticsProofs` on and off, offering proofs exactly when on, and with some row group really proven exactly when on (`DeltaTable.ProvenRowGroups`, internal, fed by `ParquetFileReader.OnProvenRowGroup`). Review round 2 added that check: the filters had INT literals over a BIGINT column, so the proofs were offered but never held, and the on/off comparison compared two identical reads;
  - a conjunct casting a string (`CAST(code AS INT) > 3`, `code = 5`) is never offered one;
  - 2 DELETEs match the same rows either way, with a row group proven when on.
- `DeltaFilePruner`'s DELETE and ADD CONSTRAINT proof tests pass unchanged through the shared rules.
- `TwoPassReadTests`: by default, a local file reads in two passes when its predicate carries a proof (the corrupted page past the selected rows is never decoded), and in one pass when it does not.

Each of these mutations fails tests:
- a predicate dropped whether proven or not (8);
- the proof following the pruning evaluator, NOT included (2 + 2);
- a leaf's null count ignored (1, the prefix test; for comparisons the evaluator already requires it);
- the first predicate dropped instead of the proven one (2, once a test put the proven predicate second);
- Delta offering proofs without the option (5);
- Delta offering one for a conjunct that can raise (2);
- the local two-pass default ignoring proofs (1).

### Review round 1

**A proof could hide a widening error.** `EvaluatesEverywhere` checked the logical predicate, but bringing an older file's column to the table's type can raise on a value: `WidenDate32ToTimestamp` multiplies checked, so a far-future DATE widened to TIMESTAMP_NTZ overflows.
- **The case:** with `d` widened that way and holding such a value, `d IS NULL OR id >= 0` raised when asked row by row. With proofs, `id >= 0` proved the OR for the row group, `d` was never read, and the error disappeared.
- **The fix:** `EvaluatesEverywhere` now refuses a column whose type has changed (`delta.typeChanges`). It costs nothing a proof could use: `ToFileColumns` already left such a column Unknown, since the file's statistics are of its old type. Only the unproven branch of an OR referred to it.
- **Scope:** the same rule gates DELETE's and ADD CONSTRAINT's file-level proofs, so it closes the same hole there under `TrustStatisticsProofs`.
- **Regression:** `AProofDoesNotHideAWideningThatOverflows` (`ValueWiseTests`, on and off). As first written (`id >= 0`, an INT literal), no proof held, so it failed without the fix only on its "no proof offered" assertion. Review round 2 made it `id >= 0L`. Its scan and its DELETE now each raise nothing with proofs on without the fix, and each was checked on its own.

### Review round 2

**A proof could skip a malformed partition value.** `ToFileColumns` turns a partition reference into one the file cannot answer, so an OR could still be proven by its other branch. Asking the predicate decodes the partition value from the log; a proof skips that.
- **The case:** with the file's DATE partition value `abc`, `part IS NULL OR id >= 0L` raised row by row but, proven, did not, for a scan and for a positions-only DELETE.
- **The fix:** `ProofFor` refuses a predicate that references a partition column (`ReferencesAnyPartitionColumn`, names compared ignoring case as `EvaluatesEverywhere` compares them). DELETE's file proof already refuses partition columns in its leaves, and evaluates a partition-only part for real.
- **Regression:** `AProofDoesNotSkipAMalformedPartitionValue`, on and off. Its scan and its DELETE each fail without the fix, each checked on its own.

Two traps found while writing it:
- **The DATE literal.** Copilot's example, `part = DATE'2020-01-01' OR ...`, is never offered a proof whatever the fix: a DATE literal parses as a cast, which `EvaluatesEverywhere` refuses. The test uses `IS NULL`, which also decodes the value.
- **INT literals.** The test needed a BIGINT literal (`0L`) for its other branch to be proven at all. Every earlier Delta-level proof test had the same flaw, now corrected (see Tests).

## 27. Phase 5d: mapping dictionary answers in one pass (2026-11-10)

### What was wrong

§22 left a choice for phase 5: answer a returned column from its dictionary, or ask its rows, since a returned `<>` was 12% slower answered. 5b found a returned integer run 12-13% slower too. A new benchmark showed the problem was wider than returned columns. Over an integer column, a dictionary answer was slower even for a column the read drops:

`ReturnedAnswerBenchmarks` (new, Delta benchmarks): `ConjunctSplitBenchmarks`'s table, one conjunct, `MaxConcurrentReads` 4, 2 launches x 20 iterations, i9-12900K, net10.0. `qty` is INT32 with 100 values (5% NULL); `category` is a string with 16 values.

| Filter | Column | Rows | Dictionary, before | Dictionary, after | After vs rows |
|---|---|---|---|---|---|
| `qty < 20` (19%) | dropped | 7.0 ms | 8.9 ms (+27%) | 5.7 ms | -18% |
| `qty < 20` | returned | 7.3 ms | 10.9 ms (+48%) | 7.6 ms | 0% |
| `qty <> 7` (94%) | dropped | 10.3 ms | 12.8 ms (+24%) | 9.9 ms | -5% |
| `qty <> 7` | returned | 13.0 ms | 16.0 ms (+23%) | 13.2 ms | +1% |
| `category = 'cat_03'` (6%) | dropped | 7.1 ms | 4.5 ms (-36%) | 3.9 ms | -44% |
| `category = 'cat_03'` | returned | 7.2 ms | 7.8 ms (+9%) | 7.3 ms | 0% |
| `category <> 'cat_03'` (94%) | dropped | 12.5 ms | 9.4 ms (-24%) | 9.2 ms | -26% |
| `category <> 'cat_03'` | returned | 14.0 ms | 15.2 ms (+9%) | 13.8 ms | -4% |

4b-4d measured only `category`, where the string comparison the dictionary saves is expensive enough to hide the cost. Every value-wise filter over a low-cardinality integer column had been 23-48% slower than asking its rows.

### The cause, and the fix

An answered chunk paid two passes over its rows that the row path does not:
1. `ArrowArrayBuilder.Build` spread the indices of its non-NULL values over the rows, by definition level, into an `Int32Array` with a validity bitmap.
2. `Map` walked that array with a three-way branch per row.

For an integer column, the comparison the dictionary saves is one cheap pass, so two passes lost.

`DictionaryAnswer.Map` now builds the answers straight from the dense indices and the definition levels, in one pass, eight rows to a byte:
- `Prepare` turns the lookup table into a TRUE bit and a known (not NULL) bit per entry.
- A column with no NULL row takes a loop with no definition levels at all.
- A validity bitmap is built only when some entry, or the NULL row, answers NULL.

The returned-column path (4d) maps from its index sink the same way, and no longer copies the definition levels into it. Nothing chooses between the paths: the dictionary is never slower than the rows beyond noise, and it keeps its large wins for `IN`, functions and host delegates.

5b's returned integer run, `FusionBenchmarks` (2 launches x 20 iterations), fused against separate:

| Filter | Column | Before 5d | After 5d |
|---|---|---|---|
| `qty >= 10 AND qty < 20` (10%) | dropped | -2% | -29% (5.4 vs 7.7 ms) |
| same | returned | +13% | **-20%** (6.5 vs 8.1 ms) |
| `qty >= 10 AND qty < 90` (80%) | dropped | | -23% (8.8 vs 11.4 ms) |
| same | returned | +12% | **-8%** (12.5 vs 13.6 ms) |

### Tests

- `DictionaryAnswerTests`: a column with no NULL row (`k`, INT32 with 1,000 values; `w`, 1,500 strings), dropped and returned, batched and not, returns the rows its predicate asked row by row selects, answered once per row group.
- The existing sweeps (nullable `s`, NULL answers, runs, view types, narrowed reads) cover the path with definition levels. The netstandard2.0 build counts bits without `BitOperations.PopCount`; its fallback is checked against every byte, and the suite runs on net472.

Each of these mutations fails tests:
- a NULL row answered FALSE whatever the NULL row's answer (48);
- definition levels ignored (112);
- no validity bitmap (42);
- a null count of 0 with a bitmap (42);
- the TRUE bit read from the known table (6, once the no-NULL test existed; every earlier column held NULLs, so the no-NULL loop was never run).

## 28. Phase 5e: reordering conjuncts by selectivity, measured and not built (2026-11-10)

§24 planned reordering a filter's conjuncts by observed selectivity only if a measured gap, an unselective conjunct ahead of a selective one against the reverse, exceeded about 10% after 5a.

### Why the gap is bounded

Two things bound what reordering can win:
- **Only conjuncts that cannot raise may move** (§7). Moving one that can would hide errors. Those are exactly the cheap ones: direct-kernel comparisons, null tests and dictionary answers. An expensive conjunct (a function call, `IN` without a kernel, a cast) cannot be reordered at all.
- **The order changes no decoding.** Pass 1 decodes every predicate column of a row group before any predicate is asked, so a selective conjunct placed first saves only the evaluation of the conjuncts after it. With 5a, an unselective first conjunct leaves most rows live, so the selective one is asked about the whole batch; the reverse order asks the unselective one about a gather of 2% of the rows.

So the most reordering can save is about one cheap kernel over the batch per misplaced conjunct.

### Measured

`ConjunctOrderBenchmarks` (new, Delta benchmarks): `ConjunctSplitBenchmarks`'s table (2M rows), returning `id` alone or every column, `MaxConcurrentReads` 4, 2 launches x 20 iterations, i9-12900K, net10.0. The first figure is with the unselective conjunct first, against the selective one first (2% kept). The run was repeated, and rows varied by up to 6 points between runs.

| Conjuncts (unselective \| selective) | `id` only | Every column |
|---|---|---|
| `id >= 0 AND amount < 990` \| `amount < 20` | +16% (+1.1 ms) | +5% (+0.55 ms) |
| `amount < 990` \| `amount > 980` | +11% (+0.7 ms) | +7% (+0.8 ms) |
| `amount < 990` \| `qty = 7` (dictionary) | +10% (+0.7 ms) | +8% (+0.9 ms) |
| `id >= 0` \| `amount < 20` | +6% (+0.4 ms) | +2% (+0.2 ms) |
| `category <> 'cat_03'` (dictionary) \| `amount < 20` | -3% | 0% |

- A misplaced direct-kernel conjunct costs 0.4-1.1 ms per 2M rows, as predicted. That is 6-16% of a scan returning `id` alone, and 0-8% of one returning all four of this table's columns; a wider table's share is smaller still.
- A conjunct answered from its dictionary costs nothing in either place.

### Decided (2026-11-10): not built

- **The gate is crossed, narrowly.** For scans returning `id` alone, the measurement exceeds the ~10% gate §24 set: +10-11% for one misplaced conjunct, and +16% (+1.1 ms per 2M rows) for two. Returning every column, it stays under it (0-8%). So reordering is not ruled out by the gate; it is declined on benefit against risk. The benefit is confined to scans that return almost nothing, and is about a millisecond per 2M rows there (review round 1 corrected this bullet, which said the measurement stayed below the gate).
- **Risk.** Reordering needs a static guarantee that a conjunct cannot raise. 5a's is per call (it may decline), so the guarantee would be 5c's proof gate, whose edges took two review rounds (widened types, partition values). Reordering would add a second feature on that surface, plus per-scan selectivity learning and barriers at host masks and dictionary runs.
- **Upstream.** Neither upstream project reorders adaptively. arrow-rs keeps the caller's order. DataFusion's selectivity-tracking prototypes (#19639, #21752) were closed with mixed results.

The conjuncts keep their source order (§7). A host that knows its selectivities can order its filter itself. `ConjunctOrderBenchmarks` stays, to repeat the measurement if evaluation gets more expensive relative to decoding.

## 29. Phase 5f: offset and limit (2026-11-10)

### The gate: what an early stop wastes

§24 planned limit pushdown only if the read-ahead a consumer's early stop wastes turned out material. `LimitScanBenchmarks` (new, Delta benchmarks) measured it on `RowGroupUnitScanBenchmarks`' `FourLarge` table (4 files x 8 row groups x 131,072 rows; the object store simulated at 20 ms per GET and 100 MB/s). A consumer takes 10 rows and leaves the `await foreach`; the time includes disposing the scan.

| Consumer takes 10 rows | Window 1 (serial) | Window 4 | Window 8 |
|---|---|---|---|
| Local, time | 0.71 ms | 2.40 ms | 4.00 ms |
| Local, data read | 1.1 MB, 3 GETs | 5.5 MB, 16 | 17.1 MB, 36 |
| Object store, time | 61 ms | 64 ms | 64 ms |
| Object store, data read | 1.1 MB, 2 GETs | 8.7 MB, 12 | 17.1 MB, 20 |

- On object storage, cancellation is prompt, so the cost is not latency but **6-15x the bytes and GETs**.
- Locally, disposal waits for the row groups already decoding (`OrderedScan` cancels its producers and then waits for them), at 3-6x the time.
- A smaller window cannot fix it: a full scan of the same table with a 0.1% filter took 1.32 s serially and 0.23 s at window 8 on the simulated store.

**Decided (2026-11-10):** build both `Offset` and `Limit`, as arrow-rs (`with_offset`, `with_limit`), Spark's DSv2 (`SupportsPushDownOffset`, `SupportsPushDownLimit`), Polars (slice pushdown) and Lance do. DataFusion pushes only a fetch of `offset + limit` into its scans. The offset is the pagination case: with exact row counts it is almost free.

### What it does

**`DeltaReadOptions.Offset` and `.Limit`** (public, `long` and `long?`) apply to the rows the scan returns, in its order: files in path order, row groups and rows in file order, whatever the window.
- A negative value is refused.
- So is either with a superset `Filter` (no `FilterRows`): the rows skipped would depend on what pruning kept.
- `Limit = 0` returns nothing and reads nothing.

There are three layers, and only the first decides which rows come back:
1. **The output** (`LimitRowsAsync`) skips the offset's rows and stops after the limit's, which ends the scan and cancels what is in flight. A batch cut in two is gathered (`BatchSelector.Gather` / `ReleaseAfterGather`, so a view column's shared buffers stay alive). This alone makes every mode exact.
2. **Counted mode** (no exact filter, no masks, the built-in reader; `FilterRows` without a `Filter` is no filter). Each planned row group's live rows are its plan's rows less those the file's deletion vector deletes among them (counted by clearing a selection bitmap). They come from the file, never from the log's statistics.
   - The planner drops the units wholly inside the offset, crediting their rows to it. Every skip comes before the first unit handed out, so the output reads the credit once.
   - It stops planning once the units handed out cover `Offset + Limit`.
   - The log's `numRecords`, less the deletion vectors' cardinality, only decides how many files are opened ahead. A wrong statistic can make the scan open fewer files ahead; it cannot change a row.
3. **Bounded mode** (an exact filter, masks, or the codec seam). Matches are not known ahead.
   - A parallel scan starts with one unit when there is no offset, and with a whole window when there is one, since the offset's rows have to be read and evaluated anyway.
   - As units end, it estimates the units still needed (rows still needed over the rows per unit so far), and grants that many, up to the window. With no row yet, the units in flight double.
   - A first version doubled from one unit regardless. That cost a deep filtered page 14% locally and 60% on the simulated store: several round trips to reach the window the consumer's own read had from the start.

**Liveness.** In bounded mode only a unit that ends grants more. A first version therefore deadlocked when the output still wanted rows after the units had yielded `Offset + Limit`: the planner waited for a grant and the output for a batch. Correct code cannot get there, but a mutation of the output's arithmetic did, and its tests hung instead of failing. The output now tells the limit when it waits for a batch (`ConsumerWaiting`), and how many rows each batch held. A wait with no unit in flight, after the output has received every row the ended units yielded, grants one; that never happens in a correct scan. (The first version of this granted on any wait with no unit in flight: see review round 1.)

### Measured

`LimitScanBenchmarks`, window 8, 10 rows taken by the consumer stopping itself against `Offset`/`Limit` pushed. "Page" starts 2,000,000 rows in, or 2,000 rows into the filter's ~4,200 matches (`amount < 1`, 0.1%, scattered). 12 iterations.

| Case | Consumer stops itself | Pushed | Change |
|---|---|---|---|
| First 10, local | 4.0 ms, 17.1 MB | 0.74 ms, 1.1 MB | -82% |
| Page, local | 7.1 ms, 32.8 MB | 0.90 ms, 1.2 MB | -87% |
| Filtered, first 10, local | 4.2 ms, 17.1 MB | 1.15 ms, 1.1 MB | -72% |
| Filtered page, local | 8.5 ms, 31.8 MB | 6.4 ms, 19.2 MB | -24% |
| First 10, object store | 64 ms, 17.1 MB / 20 GETs | 64 ms, 1.1 MB / 2 | bytes -94% |
| Page, object store | 107 ms, 25.4 MB / 28 GETs | 64 ms, 1.2 MB / 4 | -40%, bytes -95% |
| Filtered, first 10, object store | 95 ms, 12.9 MB / 21 GETs | 96 ms, 1.1 MB / 3 | bytes -91% |
| Filtered page, object store | 159 ms, 29.7 MB / 39 GETs | 159 ms, 18.1 MB / 26 | bytes -39% |

The pushed options are never slower than the consumer stopping itself, and read less in every case.

### Tests

`OffsetLimitTests` (Delta). The table has 3 files x 4 row groups x 500 rows, a deletion vector deleting every 13th row of two files and a whole row group, and a partition. The oracle is the same scan without offset or limit, skipped and taken in order, row addresses included.
- 8 offsets (0, 1, 499, 500, 1,001, 2,000, 3,500, past the end) x 6 limits (none, 0, 1, 10, 501, 2,500), over {plain, exact filter, host mask} x {serial, 4 at once}.
- With no row condition, a limit inside the first row group reads one unit, an offset past the first file reads none of its row groups, and an offset-only read reads fewer units than a full one (`DeltaTable.ScanUnitsRead`, internal).
- A filtered limit at window 8 reads at most 3 units.
- `Limit = 0` reads nothing.
- A negative offset or limit, and either with a superset filter, are refused.

Each of these mutations fails tests:
- the output ignoring the offset (6);
- skipped units not credited (4);
- live rows ignoring the deletion vector (2);
- the planner never stopping (1);
- never skipping (2);
- no ramp (1);
- the ramp opening the full window (1);
- a superset filter accepted (1);
- the output taking one row too few (9; it hung before the liveness fix).

The benchmarks' table guard now checks for a commit, not just a `_delta_log` directory: an interrupted build had left an empty one that made every case fail.

### Review round 1

- **CI: a filtered `Limit = 1` read 11 units at window 8** (net10.0 on CI; it passed locally, and on net8.0 and net472 there). The liveness reserve granted a unit whenever the output waited with none in flight. But a unit can end with its batch queued before the output wakes to take it, so the output "waited" with nothing in flight and was granted a unit. On a slow runner, each new unit ended before the output ran too, and granted the next. The reserve now also needs the output to have received every row the ended units yielded. `AUnitEndingBeforeTheOutputTakesItsBatchGrantsNothing` replays that sequence deterministically on `ScanLimit`; the real race depends on thread scheduling. The ramp test ran 30 times without a failure.
- **Statistics were parsed for every file** for the open-ahead hints, even for `Limit = 1`, a serial scan or `Limit = 0`. They are now built only for a parallel scan with a positive limit, and parsing stops once the hints cover `Offset + Limit`.
- **Fully pruned files stalled a bounded scan's lookahead.** Opening files ahead followed the units granted, and a file whose row groups are all pruned yields no unit, so grants none. A run of them was opened one at a time. Each such file now doubles the open-ahead allowance, up to the window. `AFilteredScan_PlansTheFilesItOpenedAhead_AtOnce` covers an exact filter with a limit too, and fails without the fix.
- **A covered counted limit still opened the next file**: the file about to be planned is opened whatever the lookahead says. The planner now checks for a covered limit before opening it. `ACoveredLimitOpensNoFurtherFile` (one-row-group files, serial and parallel) fails without the check.

### Review round 2

- **`FilterRows` without a `Filter` counts as no filter.** Counted mode was keyed on `FilterRows` alone. So such a read, which has no row condition, lost row-group skipping for its offset and the open-ahead hints for its limit. It now keys on an exact filter being present. `FilterRowsWithoutAFilterIsCounted` (serial and parallel) reads one unit for `Offset = 2,000, Limit = 10`, returns the same rows as the read without `FilterRows`, and fails with the old classification.
- **The planner's tally no longer adds without bound.** `_planned + rows <= Offset` would wrap if the tally neared `long.MaxValue`. The tally sums real row counts, so that needs a table of about 9.2 x 10^18 rows; unreachable, but it is now compared by subtraction and saturated, as `Need` already was. `AnOffsetOfLongMaxValueReturnsNothing` pins the edge it guards (it passed before too).

## 30. Phase 5g: `FilterRows` stays off by default (2026-11-10)

§8 ended phase 5 with a decision on whether `FilterRows` should default on.

**Decided (2026-11-10): no.** `Filter` keeps its superset contract by default, and `FilterRows` stays opt-in. Its documentation now says when to set it: whenever the caller would otherwise filter the returned rows itself.

For turning it on:
- **Faster than filtering afterwards.** It is 6-36% faster than reading and then filtering (§23), and much faster where a dictionary answers the predicate (§20-§22, §25, §27). It also skips decoding the other columns of rows it rejects.
- **Offset and limit.** `Offset` and `Limit` (§29) are refused with a superset filter, so default-on would let them combine with a `Filter` without a second option.
- **Timing.** The library is pre-1.0, which is when a behaviour change costs least.

Against, and deciding:
- **It changes what a scan returns.** `Filter` is documented as a superset ("filter them yourself"). A caller passing a predicate meant only for pruning, or one whose own evaluation differs from Spark's rules, would silently get fewer rows.
- **It changes what a scan can raise.** The filter would be refused before any file is read where Spark's analyzer refuses it, and evaluated on every row under ANSI rules. A scan that succeeds today could start throwing on one row's value, a cast that fails for instance.
- **The main hosts filter anyway.** A query engine above the scan (DuckDB, the host this work started from) re-applies its pushed filters. Default-on would evaluate each row twice, once in EW and once in the host, for nothing.
- **Upstream defaults to the superset.** DataFusion's `pushdown_filters` is off by default (§2, §24). Spark treats a pushed filter as best effort and re-evaluates it.

### Phase 5, summed up

| Part | What | PR |
|---|---|---|
| 5a | Conjuncts that cannot raise asked about whole batches, not gathers of the live rows (§24) | #805 |
| 5b | Adjacent conjuncts over one column answered from its dictionary together (§25) | #806 |
| 5c | A conjunct its row group's statistics prove dropped there, under `TrustStatisticsProofs`; two passes locally only where proofs are offered (§26) | #807 |
| 5d | Dictionary answers mapped to rows in one pass (§27) | #809 |
| 5e | Reordering by selectivity measured, and not built (§28) | #810 |
| 5f | `DeltaReadOptions.Offset` and `Limit` (§29) | #811 |
| 5g | `FilterRows` stays off by default (this section) | |

Issues opened along the way:
- #804: dictionary answers for chunks that mix dictionary and PLAIN pages, or lack `encoding_stats`.
- #808: a statistics proof refuses an INT literal over a BIGINT column, so `id >= 2500` is never proven. Fixed: an exactly widening literal is now proven.

## 31. Chunks that fall back from the dictionary (#804, 2026-11-11)

Before #804, a chunk was answered from its dictionary only when its `encoding_stats` proved every data page dictionary-encoded (§20). Two kinds of chunk were always asked row by row:
- **Chunks that fall back to another encoding partway through.** parquet-mr, arrow-cpp and arrow-rs all do this once the dictionary passes a size limit. That is the common case in other engines' files: the TPC-H key columns in #502's DuckDB benchmark are written this way.
- **Chunks with no `encoding_stats`.** The field is optional.

### What it does

§6 step 2, as proposed: the dictionary is evaluated once per chunk, as before. The pages that fell back have their values decoded and asked.
- **Which chunks.** Any flat chunk with dictionary-encoded pages (`DictionaryAnswer.MayAnswer`). With `encoding_stats`, they must show a dictionary page and a dictionary-encoded data page. Without them, a dictionary encoding in the chunk's `encodings`, or a dictionary page offset, is enough. A chunk that turns out to have no dictionary page declines at its first data page and is read as values.
- **A fallback page.** Its values are decoded apart (`FallbackValueState`), and every predicate of the run is asked over them (`AnswerValues`). Their answers join the lookup table as entries past the dictionary's. The page's values take those entries as their indices. `Map` then treats both kinds of page alike, and nothing downstream changes: the answers column, `BatchSelector`, and the batched read's cursors. The extra entries are for one `Map` (one chunk, or one batch's pages) and are reused after it. A fallback page may be in any encoding: PLAIN, or DELTA_BYTE_ARRAY from parquet-mr's V2 writer.
- **A returned column.** The values decoded apart are appended to the read's values (`ColumnBuildState.TryAppendValuesOf`), so the page is decoded once. A BOOLEAN column, or a string read as a view or with large offsets, decodes the page again instead.
- **Errors.** A predicate that raises over a fallback page's values, or values that cannot be built apart, makes the chunk decline (`DictionaryAnswer.DeclinedException`, not a decode failure). The rows then decide, as when the dictionary itself raises (§20). Over the whole row group, the chunk is read again as values. In the batched read, the answers already held cover rows this batch still needs. So the column is decoded again as values from the page holding the batch's first row: from the held chunk, from the window, or from a range fetched again. It is read as values from then on. Batches already returned used only pages that answered; a page is decoded by the first batch that reaches it.
- **The all-false prune** (§6) is not built: §32 measured it and decided against it for now. If it is built, it must stay limited to wholly dictionary-encoded chunks: a fallback page can hold values the dictionary does not. Dictionary pruning (#57) keeps `IsWhollyDictionaryEncoded` for the same reason.

Mapping the masks into the table also changed. `Evaluate` writes each predicate's answers straight from the mask's bitmaps into the table's two bits per entry. It no longer goes through a byte per row and `BooleanArray.GetValue`, since a fallback page puts every value through it.

### Measured

`fallback-ab` (new, Parquet benchmarks). arrow-cpp (ParquetSharp) writes 4 row groups x 500,000 rows: `id`, `amount`, and `category` (50 values). In each row group, one filler value larger than the 64 KiB dictionary page limit makes the rest of the chunk PLAIN from the chosen point. `rows` asks the predicate row by row, which is what every mixed chunk got before #804. `dict` marks it value-wise. The host probe is a hash-set delegate, as a join filter would be. i9-12900K (24 logical), net10.0, local file, median of 9.

| PLAIN | Case | Kept | rows ms | dict ms | dict vs rows |
|---|---|---|---|---|---|
| 0% | `= 'cat-07'`, returns `id, amount` | 2% | 16.7 | 11.2 | -33% |
| 0% | host probe, returns `id, amount` | 10% | 33.7 | 11.1 | -67% |
| 0% | `= 'cat-07'`, returns `id, category` | 2% | 9.6 | 9.7 | +1% |
| 0% | host probe, returns `id, category` | 10% | 28.5 | 11.4 | -60% |
| 10% | `= 'cat-07'`, returns `id, amount` | 2% | 13.8 | 11.2 | -19% |
| 10% | `IN` 5, returns `id, amount` | 10% | 18.7 | 12.8 | -32% |
| 10% | `<> 'cat-07'`, returns `id, amount` | 98% | 25.1 | 21.6 | -14% |
| 10% | host probe, returns `id, amount` | 10% | 32.2 | 13.6 | -58% |
| 10% | `= 'cat-07'`, returns `id, category` | 2% | 9.8 | 10.5 | +7% |
| 10% | `<> 'cat-07'`, returns `id, category` | 98% | 22.1 | 22.6 | +2% |
| 10% | host probe, returns `id, category` | 10% | 37.3 | 13.6 | -63% |
| 50% | `= 'cat-07'`, returns `id, amount` | 2% | 14.1 | 12.7 | -10% |
| 50% | `<> 'cat-07'`, returns `id, amount` | 98% | 25.4 | 24.5 | -4% |
| 50% | host probe, returns `id, amount` | 10% | 32.5 | 22.5 | -31% |
| 50% | `= 'cat-07'`, returns `id, category` | 2% | 10.7 | 12.4 | +16% |
| 50% | `<> 'cat-07'`, returns `id, category` | 98% | 22.4 | 24.3 | +9% |
| 50% | host probe, returns `id, category` | 10% | 29.1 | 22.3 | -23% |
| 90% | `= 'cat-07'`, returns `id, amount` | 2% | 15.5 | 15.8 | +2% |
| 90% | `<> 'cat-07'`, returns `id, amount` | 98% | 26.7 | 26.5 | -1% |
| 90% | host probe, returns `id, amount` | 10% | 34.9 | 32.8 | -6% |
| 90% | `= 'cat-07'`, returns `id, category` | 2% | 12.4 | 15.1 | +22% |
| 90% | `IN` 5, returns `id, category` | 10% | 17.6 | 19.7 | +12% |
| 90% | `<> 'cat-07'`, returns `id, category` | 98% | 23.1 | 26.8 | +16% |
| 90% | host probe, returns `id, category` | 10% | 30.5 | 32.3 | +6% |

(The full run, every case at every point, is in the PR.)

- **A dropped column** gains or breaks even at every fallback point. The gain shrinks as the PLAIN share grows, since the fallback pages are asked value by value either way.
- **A host delegate** is the case #804 was raised for. Answered pages skip the delegate, so it gains 58-67% at 10% PLAIN, 23-31% at 50%, and breaks even at 90%.
- **A returned column under a cheap kernel** (`=`, `<>` and `IN`, which now has a kernel of its own) costs more once most pages are PLAIN: +9-16% at 50%, +12-22% at 90%. Each fallback value pays the bookkeeping of a table entry, an index and a lookup in `Map`, on top of the comparison the row path also pays.

Two changes on the way cut that cost. Before them, the same file measured +27% (50%) and +43% (90%) for a returned `=`, and +9% at 90% for a dropped one:
- decoding a returned column's fallback page once, not twice;
- writing the masks' bits straight into the table.

The table above was measured without the switch below. It shows what the dictionary path costs at each fallback point.

### Asking a mostly-PLAIN returned column row by row

A returned column whose chunk has at least `ParquetFileReader.ReturnedFallbackShareLimit` of its rows on fallback pages (default 0.75) is asked row by row. A dropped column is always answered, since it gains or breaks even at every point.

The share is a count of rows, not a guess from sizes. Byte sizes would mislead, since a dictionary-encoded page holds many more rows than a PLAIN page of the same size. `DictionaryAnswer.FallbackRowShare` gets it from what the read already has:
- **`encoding_stats` prove the chunk wholly dictionary-encoded:** zero, with no further work.
- **The one-batch read** holds the whole chunk, so it walks the page headers (encoding and rows) without decoding a page.
- **The batched read's page map,** built from the page headers, already holds each page's encoding and rows.
- **A page map built from the OffsetIndex** knows each page's rows but not its encoding. `encoding_stats` counts the dictionary-encoded data pages. Writers fall back once per chunk and never return to the dictionary, so those are the first pages, and the fallback rows start at the next page's first row. If a writer did return to the dictionary, only the choice of path would be wrong, not the rows.
- **Neither** (an OffsetIndex map and no `encoding_stats`): unknown, and the dictionary answers.

It is a choice per chunk, made before the chunk's first data page is decoded. The reader still cannot tell a cheap kernel from an expensive delegate, so the limit is where even a delegate stops gaining. `fallback-ab` with the switch off (`EW_FALLBACK_SHARE_LIMIT=2`), returned `id, category`:

| PLAIN | `=` | `IN` 5 | `<>` | host probe |
|---|---|---|---|---|
| 50% | +16% | -3% | +9% | -23% |
| 60% | +14% | +4% | +11% | -33% |
| 70% | +18% | +8% | +10% | -12% |
| 80% | +17% | +9% | +11% | -3% |
| 90% | +22% | +12% | +16% | +6% |

From 80% up a delegate gains nothing, while the kernels lose 9-22%. Between 50% and 70% the two pull apart, and the dictionary keeps them. With the switch on at 0.75, the returned column at 80% and 90% measures -4% to +2% against the rows for every case, since both now take the row path.

### Tests

`FallbackPageAnswerTests` (new, Parquet). arrow-cpp (ParquetSharp) writes the fixtures with a 6,000-byte dictionary page limit and 1 KiB pages. Unique filler values overflow each chunk's dictionary partway through. There are four variants: data page V1, V2, V1 with a page index, and V1 with `encoding_stats` stripped from the footer.
- The fixture's chunks of `s`, `k` and `e` hold both kinds of page; the stripped variant has no stats.
- An oracle sweep: 7 answers x 4 variants x 4 shapes (one batch or 700-row batches, dropped or returned, serial or 4-way with 5% excluded and a later predicate, one pass or two). Each returns the same rows as the predicate asked row by row, and the later predicate sees the same rows. The dictionary is asked once per row group, and fallback values are asked with no NULLs among them. Under three quarters of the rows are asked at all. One answer is NULL only for values on fallback pages, which needs a validity bitmap no dictionary entry calls for.
- A returned view-type or large-offset column (decoded twice), and an INT32 column with no NULL row, across the variants.
- A mask that sets a NULL answer's value bit.
- A chunk whose `encodings` claims a dictionary but has no dictionary page is read as values.
- Errors: a value only an excluded row holds, on a fallback page, does not fail the read. This is tested over the whole row group and in batches of 70 to 2,500 rows, which between them take all three recovery routes (held chunk, window, fetched again). One a live row holds raises.
- The switch to rows, across the variants, one batch and batched, plus an OffsetIndex map without `encoding_stats`. At a limit of 5%, a returned `s` is asked row by row (every row once). At 95% it is answered from the dictionary. A dropped `s` is answered at 5% too. With an OffsetIndex map and no stats, the share is unknown, and the dictionary answers.

`SparkFallbackPageInteropTests` (new, Delta tier 3). Spark writes a table with `parquet.dictionary.page.size` 4096 and 500-row pages, set as session confs: a Delta write's options do not reach parquet-mr. The V1 writer falls back to PLAIN and the V2 writer to DELTA_BYTE_ARRAY. Six filters x two projections return the same rows with `AnswersFromDictionaries` on and off. The fillers start halfway through each file, because parquet-mr rewrites a chunk PLAIN whole if its dictionary overflows before the first page is written.

Each of these mutations fails tests:
- the old wholly-encoded gate (108);
- fallback values answered TRUE (91; and both Spark cases);
- a fallback error ignored (13);
- the whole-row-group read propagating a decline (2);
- each batched recovery route made to throw: held chunk (6), window (4), fetched again (2);
- FALSE not winning over NULL in a run (12);
- the fallback entries' NULLs given no validity bitmap (8);
- no decline before a missing dictionary page (1);
- a returned column's appended byte arrays skipped (65), and its fixed-width values zeroed (3);
- the switch to rows never taken (8), taken for a dropped column too (8), or taken when the share is unknown (1);
- an OffsetIndex map's share taken as zero (1); every page counted as fallback in the header walk (36) and in a header-built map (32).

One mutation passes, and is equivalent: taking a NULL answer's value bit as TRUE. Every consumer reads the value bit only where the validity bit is set.

### Review round 1

- **The header walk could loop forever (Copilot).** `PageHeaderDecoder` accepts any compressed size. A page declaring minus its own header's length left the one-batch read's walk at the same offset forever, with no cancellation check. The walk now refuses a negative size, or one past the chunk's end, as `PageReader` does. The share is then unknown, and the read reports the corrupt page itself.
  - Two related fixes: the walk now stops once it has counted the chunk's values, as the read does, rather than reading on into whatever bytes follow the last data page. And the OffsetIndex share sums `encoding_stats` counts as a long. Two counts of `int.MaxValue` summed to -2 and indexed the page map out of range. A negative count never reaches it: the footer decoder drops the list.
  - Tests: a header declaring minus its own length, a negative size, and a size past the end each give no share (the first hung before the fix). A footer whose `encoding_stats` counts overflow an int is read correctly. Each of these mutations fails tests: the size unchecked (3), the walk past the chunk's values (4), the sum as an int (1).

### Review round 2

- **A mask with an uncounted null count (Copilot).** `Evaluate` read a mask's validity bitmap only when `NullCount > 0`. Arrow uses -1 for "not yet counted", so such a mask would have had its NULL answers read as TRUE or FALSE. That is not a live bug: Arrow 23's `NullCount` counts on first read and never returns -1 (measured: a mask built with -1 and four NULLs reports 4). The check is now `!= 0`, as `SelectionMask` and the rest of the repo have it. `ANullAnswersValueBitIsIgnored` now also passes masks built with a null count of -1. It passes either way under Arrow 23, and pins the behaviour should Arrow stop counting lazily.

## 32. The all-false prune, measured and not built (2026-11-12)

§6 proposed treating an all-false dictionary table as a row-group prune: when every entry, and the NULL row, answers FALSE, skip the group before reading any data page. Its conditions:
- the chunk is wholly dictionary-encoded;
- the predicate is the first, or none before it can raise.

Before building it, this measures what it could save.

### Measured

`allfalse-ab` (new, Parquet benchmarks). EW writes 8 row groups x 250,000 rows: `id`, `key`, `amount`, `qty`, `ts` and `note`. `key` holds 20 values local to each row group, so every chunk of it is wholly dictionary-encoded, as the benchmark checks. The predicate is an opaque value-wise host probe: a hash set of 3 keys that only row group 0 holds. Dictionary pruning (#57) cannot see it, since it needs an `Eq` or `In` expression. The variants:
- `rows`: the probe asked row by row.
- `dict`: today's reader, answering every group from its dictionary but still reading it.
- `ideal`: `dict` plus a statistics prune on `id` that skips groups 1-7. This is an upper bound on the all-false prune, which would also read each skipped group's dictionary page.

i9-12900K, net10.0, local file, warm cache, median of 9. Every variant returns the same 37,384 rows.

| Projection | Variant | ms | vs `dict` | MB allocated |
|---|---|---|---|---|
| `id, amount, qty, ts, note` | `rows` | 41.0 | +145% | 83.1 |
| | `dict` | 16.8 | | 19.0 |
| | `dict`, two passes | 11.0 | -34% | 3.1 |
| | `ideal` | 6.1 | -63% | 4.4 |
| | `ideal`, two passes | 5.9 | -65% | 2.5 |
| `id` | `rows` | 33.1 | +707% | 65.3 |
| | `dict` | 4.1 | | 1.1 |
| | `dict`, two passes | 4.0 | -3% | 1.0 |
| | `ideal` | 2.1 | -49% | 0.7 |
| | `ideal`, two passes | 2.6 | -37% | 0.4 |

The ceiling is large where it applies. With a wide projection it saves 63% against a one-pass read and 46% against a two-pass one (11.0 against 5.9 ms), since two passes already skip decoding the other columns of rejected rows. With `id` alone it saves about half. The saving scales with the share of row groups rejected whole: 7 of 8 here is the best case, and a file where every group holds some match gains nothing.

### Decided (2026-11-12): not built

The prune needs three things together, and the common case lacks one:
- **A predicate only the dictionary can answer:** an opaque value-wise delegate. An `Eq` or `In` filter already gets dictionary pruning (#57).
- **Chunks wholly dictionary-encoded.** Join keys, the main source of host masks, are usually high-cardinality. EW writes them without a dictionary past 20% distinct, and other writers fall back mid-chunk (§31), so the prune would never fire on them.
- **Values clustered by row group,** so that whole groups miss.

What fits is a host mask over a low-cardinality column whose values are local to row groups: data sorted, clustered or Z-ordered on a category-like column. Build it when a host needs that case. It is a check before a group's data pages are read, plus the rule about earlier predicates that can raise, and `allfalse-ab` is ready to measure it.
