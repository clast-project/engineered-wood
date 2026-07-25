# Fixed-length list fast path (prototype)

A prototype of the optimisation Gunnar Morling describes in
[*A Fast Path for Fixed-Length Lists in Parquet*](https://www.morling.dev/blog/fast-path-for-fixed-length-lists-in-parquet/),
implemented for the EngineeredWood Parquet reader. Opt-in via
`ParquetReadOptions.FixedListFastPath`.

## The problem

Parquet has no fixed-size list type. A 768-dimensional embedding, an RGB triple, and a ragged
tag list are all written as `LIST`, and the Dremel encoding pays for that generality on every read:

- one **definition level** and one **repetition level** per *element*, not per row;
- both level streams decoded into `byte[]`, then widened into `int[]` for the assembler;
- a full scan of the repetition levels to derive the Arrow list offsets;
- further scans to find "phantom" entries (null/empty-list markers) and to build a validity bitmap.

For `list<float>` with 768 elements, that is three arrays of 768 entries per row to produce one
offset value and one validity bit — work that is entirely redundant when every list happens to be
the same length and nothing is null.

## The technique

Prove, from the **encoded** level streams, that the two things which make the work redundant
actually hold. Neither check decodes levels; both walk the RLE/bit-packing hybrid *runs*.

### 1. Definition levels: everything at max

Every definition level must equal `maxDefLevel`, which rules out null lists, empty lists, and null
elements in one test.

- An **RLE run** is settled in O(1) from its run value. Writers emit exactly one whole-page RLE run
  in the all-defined case, so this is normally the entire check.
- A **bit-packed run** of `g` groups is `g × bitWidth` bytes that must equal a repeating
  `bitWidth`-byte stamp — the packing of eight copies of `maxDefLevel`. The stamp is tiled eight
  times so the comparison buffer is a multiple of 8 bytes, and the comparison itself is a
  vectorised `SequenceEqual`.

### 2. Repetition levels: the `0, 1×(n-1)` pattern

- The list length `n` is derived once per column chunk, by decoding levels only as far as the
  second record boundary (O(n) values, once).
- An **RLE run of 1s** is accepted in O(1): compute the next multiple of `n` at or after the run's
  start and check it lands past the run's end. This is the case that matters for long vectors — a
  768-element list produces one bit-packed byte and one 760-long RLE run per row, so verification is
  ~2 operations per *row* rather than 768 per row.
- A **bit-packed byte** covers eight levels and is compared against the stamp implied by its offset
  within the record. For `n < 8` that is a precomputed table of `n` stamps (several record
  boundaries can fall inside one byte); for `n ≥ 8` at most one boundary can, so the stamp is two
  arithmetic operations. Either way the cost is per byte, i.e. per eight levels.

Writers are free to split pages by value count rather than at record boundaries — EngineeredWood's
own writer does — so the pattern is verified against the **chunk-global** index, not a page-local
one. A page may legitimately open and close part-way through a list. The whole-chunk consistency
check is `rowCount × n == numValues`, applied once at the end.

### 3. What the proof buys

On a match:

- level streams are never decoded, so no `byte[]` is filled and the `byte[] → int[]` widening
  (two allocations of `rowCount × n` ints) never happens;
- `ColumnBuildState` is constructed with `maxDefLevel = 0`, which sends the array builder down its
  non-nullable dense path — no reverse scatter, no validity bitmap;
- list offsets are `offsets[i] = i × n`, an O(rowCount) loop instead of an O(rowCount × n) scan;
- the phantom-entry filter and `Take` pass are skipped entirely.

## Scope and fallback

The detector only fires for `maxRepetitionLevel == 1` — a single list level, which is what
"fixed-length" means in practice. Nested lists, maps, and lists of lists take the general path.

Detection runs **before** any values are decoded for a page. The common rejection — a column whose
first page is ragged or contains nulls — therefore costs only the probe. A column that turns ragged
part-way through a chunk is the worst case: the probe fails late, and the chunk is read again from
the start down the general path. That is why the option is opt-in rather than default; see the
fallback benchmark below for what it costs.

## Files

| File | Role |
| --- | --- |
| `src/EngineeredWood.Parquet/Parquet/Data/FixedListDetector.cs` | The two encoded-stream proofs and the length derivation |
| `src/EngineeredWood.Parquet/Parquet/Data/ColumnChunkReader.cs` | `TryReadFixedListColumn` — speculative chunk read with per-page verification |
| `src/EngineeredWood.Parquet/Parquet/Data/NestedAssembler.cs` | `BuildFixedOffsets` — arithmetic offsets when a length was detected |
| `src/EngineeredWood.Parquet/Parquet/ParquetReadOptions.cs` | `FixedListFastPath` |
| `test/EngineeredWood.Parquet.Tests/Parquet/Data/FixedListFastPathTests.cs` | Detector unit tests + path-equivalence tests |
| `test/EngineeredWood.Parquet.Benchmarks/FixedListReadBenchmarks.cs` | Sweep + fallback-cost benchmarks |

## Benchmarks

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26200.8875/25H2)
12th Gen Intel Core i9-12900K 3.20GHz, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.301, net10.0, Release
```

Run with:

```
dotnet run -c Release -f net10.0 --project test/EngineeredWood.Parquet.Benchmarks -- --filter "*FixedList*"
```

### Fixed-length lists — where the fast path applies

Each file holds 2,000,000 floats, so the **element count is constant across rows of the table** and
only the list shape varies. `Plain` is uncompressed PLAIN floats (the level work is a large share of
the read); `Default` is the library's defaults, Snappy + BYTE_STREAM_SPLIT (what a real embedding
file looks like).

| Length | Layout | General | Fast path | Speedup | Allocated (general → fast) |
| ---: | --- | ---: | ---: | ---: | --- |
| 3 | Plain | 12.774 ms | 5.621 ms | **2.27×** | 20.8 MB → 5.2 MB (0.25×) |
| 3 | Default | 11.999 ms | 5.040 ms | **2.38×** | 20.8 MB → 5.2 MB (0.25×) |
| 8 | Plain | 12.315 ms | 5.087 ms | **2.42×** | 17.6 MB → 1.96 MB (0.11×) |
| 8 | Default | 11.992 ms | 4.963 ms | **2.42×** | 17.6 MB → 1.96 MB (0.11×) |
| 16 | Plain | 12.103 ms | 4.761 ms | **2.54×** | 16.6 MB → 0.98 MB (0.06×) |
| 16 | Default | 12.635 ms | 5.185 ms | **2.44×** | 16.6 MB → 0.99 MB (0.06×) |
| 64 | Plain | 12.784 ms | 5.081 ms | **2.52×** | 15.9 MB → 0.38 MB (0.02×) |
| 64 | Default | 13.368 ms | 3.839 ms | **3.48×** | 15.9 MB → 0.25 MB (0.02×) |
| 256 | Plain | 11.168 ms | 3.381 ms | **3.30×** | 15.7 MB → 0.07 MB (0.004×) |
| 256 | Default | 13.718 ms | 6.467 ms | **2.12×** | 16.6 MB → 1.02 MB (0.06×) |
| 768 | Plain | 11.725 ms | 3.773 ms | **3.11×** | 15.7 MB → 0.028 MB (0.002×) |
| 768 | Default | 13.607 ms | 6.068 ms | **2.24×** | 16.0 MB → 0.36 MB (0.02×) |

Reading the table:

- **The wall-clock win is 2.1×–3.5×.** It is roughly flat in list length because the element count
  is held constant: the general path's level work is O(elements) whatever `n` is. What changes with
  `n` is the *offset-building* work, which drops from O(elements) to O(rows) — visible in the
  allocation column, which falls by 4× at `n = 3` and by ~500× at `n = 768`.
- **Allocation is where the technique is most dramatic.** The general path materialises two `int[]`
  of `rowCount × n` (definition and repetition levels) — ~16 MB per read here, all of it Gen2 LOH
  traffic. The fast path allocates only the offsets array (`rowCount + 1` ints) and the Arrow
  buffers, and at `n = 768` reports **zero** Gen0/1/2 collections.
- **The `Default` (Snappy + BYTE_STREAM_SPLIT) rows show smaller ratios at large `n`** because
  decompression and value decoding — work the fast path cannot remove — become the dominant term.
  That is the honest number for a real embedding file; the `Plain` rows isolate what the technique
  itself is worth.

### Detector scan: scalar vs vectorised comparison

The detector is a sub-1% cost in most shapes, because it walks *runs*: the def check is normally a
single whole-page RLE run, and for `n ≥ 8` each record is one bit-packed byte plus a long RLE run of
1s accepted in O(1). The one genuinely scalar loop is **`n < 8`**, where no run of identical rep
levels reaches length 8, so the whole stream is bit-packed.

Vectorising that loop with a tiled `SequenceEqual` turns out to depend entirely on **who wrote the
file** — 2,000,000 repetition levels, scan only:

| n | Encoding | Scalar | Tiled | Adaptive |
| ---: | --- | ---: | ---: | ---: |
| 3 | DenseRun | 857 µs | 9.60 µs | **9.97 µs** |
| 5 | DenseRun | 857 µs | 9.16 µs | **9.22 µs** |
| 7 | DenseRun | 856 µs | 9.11 µs | **9.21 µs** |
| 3 | EwEncoder | 1,366 µs | 23,997 µs | **1,347 µs** |
| 5 | EwEncoder | 1,366 µs | 25,868 µs | **1,361 µs** |
| 7 | EwEncoder | 1,368 µs | 24,859 µs | **1,357 µs** |

- **`DenseRun`** — one large bit-packed run, as parquet-mr and arrow emit for small lists. A long
  contiguous block, so `SequenceEqual` vectorises it: **~90× faster**.
- **`EwEncoder`** — EngineeredWood's own `RleBitPackedEncoder`, which flushes a bit-packed group the
  moment `pending` reaches 8. For the small-`n` rep pattern that yields `[header][1 byte]` pairs and
  **never a contiguous block**, so the tiled path rebuilds a 32-byte stamp tile per one-byte run:
  **~18× slower**.

So a blanket switch to `SequenceEqual` is a footgun. `RepScanStrategy.Adaptive` — tile only runs of
≥ 32 contiguous bytes (measured break-even ≈ 26) — is strictly best: within noise of scalar on
EW-written files, within ~4% of pure tiled on dense-run files. That is what the reader uses.

Explicit `Vector256`/AVX2 intrinsics were considered and rejected: `SequenceEqual` is already
runtime-vectorised, works on all three target frameworks (netstandard2.0 has no intrinsics), and
needs no scalar fallback path.

### Fallback cost — where it does not apply

62,500 rows of ~32 floats. `Ragged` breaks the pattern within the first page; `LateBreak` is
fixed-length until the final row, so the probe walks the entire chunk before failing.

| Shape | General (option off) | Probe + fall back | Overhead |
| --- | ---: | ---: | ---: |
| Ragged | 27.95 ms | 28.34 ms | **+1.4%** |
| LateBreak | 12.04 ms | 14.35 ms | **+19%** |

The realistic rejection — ragged lists, nulls, or empty lists anywhere near the start of a chunk —
costs about 1%, which is the article's finding too. The worst case is a column that is fixed-length
for all but its last row: the probe decodes the whole chunk before the total-count check rejects it,
and the chunk is then read again. That +19% is the reason the option is opt-in.

## Known limitations of the prototype

1. **The batched/paged read path is not wired up — and cannot be, yet.** The fast path is applied
   only by the whole-row-group entry points (`ReadRowGroupAsync` and the parallel/incremental
   variants). Wiring it into the multi-batch path (`ParquetReadOptions.BatchSize` /
   `MaxBatchByteSize`) is blocked by a **pre-existing, unrelated bug**: that path does not correctly
   handle nested (list/struct/map) columns at all. It decodes whole pages and then slices the
   resulting array by row count, which is wrong for a list leaf (whose array length is the element
   count, not the row count) — reading any list column with `BatchSize` set throws
   `IndexOutOfRangeException` today, fast path or not. Fixing the batched path for nested columns is
   a separate correctness task; the fast path should be extended there only once it works generally.
2. **Late-breaking columns are read twice.** See "Scope and fallback".
3. **Only `maxRepetitionLevel == 1`.** A fixed-length list nested inside another list is not
   detected, though the same reasoning would extend to it.
4. **The length derivation decodes up to `n` levels** on the first page of a chunk. It could be
   done from the encoded bytes directly, as the article's implementation does; at one derivation
   per column chunk it did not seem worth the complexity.
5. **The deprecated `BIT_PACKED` level encoding is not probed** — those V1 pages fall back.
