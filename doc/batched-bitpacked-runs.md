# Batched bit-packed literal runs

A writer-side change, separate from the [fixed-length list fast path](fixed-list-fast-path.md)
but discovered while benchmarking it. Controlled by `ParquetWriteOptions.BatchBitPackedRuns`, which
started as an opt-in prototype and has been **on by default** since November 2026 (see
[Default](#default)). Set it to `false` to write the old framing byte for byte.

## The problem

`RleBitPackedEncoder` flushes a bit-packed group the moment its pending buffer reaches 8 values.
Each flush emits `varint((numGroups << 1) | 1)` with `numGroups == 1` — the single byte `0x03` —
followed by `bitWidth` bytes of packed data. So **every 8 values carry a 1-byte run header**.

That encoder writes every definition level, every repetition level, and every dictionary index in
every file EngineeredWood produces. Two consequences:

- **Size.** For a `bitWidth=1` repetition stream the header *doubles* the stream (1 header byte per
  1 data byte). For `bitWidth=10` dictionary indices it is ~10% overhead.
- **Decode speed.** `RleBitPackedDecoder.ReadNextGroup()` re-parses a varint header every 8 values,
  and the stream is `[header][byte][header][byte]…` — never a contiguous block, which is what
  defeated the vectorised scan in the fixed-list detector (see that doc's SIMD section).

## The change

`RleBitPackedEncoder` takes a `maxLiteralGroups` parameter: how many bit-packed groups may be
batched into one literal run. The cap is **63**, because `(63 << 1) | 1 == 127` is the largest
one-byte varint — the same reason parquet-mr uses `MAX_GROUPS_PER_LITERAL_RUN = 63`.

`maxLiteralGroups: 1` is the default and reproduces the previous bytes **exactly**
(`DefaultDepth_IsByteIdenticalToExplicitOne` asserts this).

The delicate part: mid-stream, only *whole* groups of 8 may be emitted. A partial group padded with
zeros would be decoded as real values and corrupt the stream, so padding is legal only at end of
stream. The encoder therefore still completes a partial group from the incoming run before switching
to RLE — the same interplay the original had, just with a deeper buffer.

The output is fully spec-conformant either way; multi-group literal runs are what parquet-mr and
arrow already emit.

## Measurements

`12th Gen Intel Core i9-12900K`, .NET 10, Release.

### Encoded stream size

| Shape | bitWidth | Unbatched | Batched | Ratio |
| --- | ---: | ---: | ---: | ---: |
| rep-n3 (fixed-length list levels) | 1 | 1,026 | 522 | **50.9%** |
| alternating | 1 | 1,024 | 521 | 50.9% |
| ragged-length | 3 | 504 | 380 | 75.4% |
| straddle-7-8-9 | 3 | 1,000 | 900 | 90.0% |
| mixed | 4 | 1,233 | 1,131 | 91.7% |
| dict-indices | 10 | 5,632 | 5,129 | **91.1%** |
| long-run (pure RLE) | 3 | 3 | 3 | 100.0% |

No shape grew — asserted for every shape by `Batching_NeverGrowsTheStream`, which is the guard
against the "medium runs get swallowed into literal runs" regression.

### Whole-file size

| Data | Config | Plain | Batched | Ratio |
| --- | --- | ---: | ---: | ---: |
| list n=3 | v2-snappy | 5,453 | 4,715 | **86.5%** |
| list n=3 | v2-uncompressed | 25,720 | 24,982 | 97.1% |
| list n=3 | v1-uncompressed | 25,721 | 24,983 | 97.1% |
| list n=3 | v1-snappy | 24,305 | 24,270 | 99.9% |
| dict | v2-snappy | 37,102 | 32,182 | **86.7%** |
| dict | v2-uncompressed | 47,334 | 42,414 | 89.6% |
| dict | v1-uncompressed | 108,695 | 103,775 | 95.5% |
| dict | v1-snappy | 96,209 | 93,649 | 97.3% |

The *absolute* saving is constant per stream (~738 B for these lists, ~4.9 KB for the dictionary
columns); the percentage varies with what else is in the file.

Compression mitigates the win only partly, and not where expected. V1+Snappy compresses levels
together with the payload and does squeeze the redundant `0x03` headers away (99.9% — no benefit).
But **dictionary indices keep their full saving even under V2+Snappy**, because headers interleaved
every 11 bytes do not dedupe well.

### Read speed

400,000 rows of `list<float>` with n=3, uncompressed PLAIN values:

| File | Fast path | Mean | vs baseline |
| --- | --- | ---: | ---: |
| Unbatched | off | 7.947 ms | 1.00× |
| Batched | off | 7.548 ms | **1.05×** |
| Unbatched | on | 3.130 ms | 2.54× |
| Batched | on | 2.617 ms | **3.04×** |

- **~5% on the general decode path** — purely from parsing one run header per 504 values instead of
  per 8. This applies to any reader of these files, not just EngineeredWood's.
- **~16% on the fixed-list fast path** (3.130 → 2.617 ms) — batching produces the contiguous block
  that lets the detector's tiled `SequenceEqual` scan engage. This is the isolated ~90× scan win
  from the other document, diluted to its real share of a whole read.

## Interop

Batched files must be decodable by independent implementations — the disadvantage that would have
killed the change — so this is tested three ways rather than assumed:

- **ParquetSharp (parquet-cpp).** `BatchedRuns_AreReadableByParquetSharp` reads batched files across
  all four page-version × codec combinations, for dictionary-encoded strings, nullable ints,
  booleans, and fixed-length lists.
- **pyarrow + fastparquet.** An out-of-band cross-check writes each shape both plain and batched, and
  reads both with each library. The assertion is *framing-only*: for any reader that can read the
  plain file, the batched file must decode to identical values. Result: **12 pass, 0 fail**, over
  pyarrow (all files) and fastparquet. The 8 fastparquet skips are cases where it cannot read the
  *plain* file either — nullable `DELTA_BINARY_PACKED` and V2 nested lists, pre-existing fastparquet
  limitations independent of batching. No reader could read a plain file but fail on its batched
  twin, which is the exact failure batching would introduce.
- **DuckDB and arrow-rs.** `BatchedRuns_DecodeIdenticallyForAnExternalReader` (and its nested-list
  twin) write each shape both ways across all four page-version × codec combinations and read both
  with **DuckDB**, whose decoder was written from the spec rather than derived from a reference
  implementation, and with **DataFusion**, which reads through the **arrow-rs** `parquet` crate — the
  decoder behind polars, delta-rs and every Rust consumer. Result: **16 pass, 0 fail** against
  duckdb 1.5.5 and datafusion 54.0.0. This tier exists because ParquetSharp, pyarrow and fastparquet
  all descend from — or were written against — the same two reference implementations, so before it
  the entire interop story rested on one lineage.
- **Read-side regression.** The 92-file compatibility corpus (EW reading foreign files) still passes
  135 / 0 fail after these changes.
- **The corpus already contains this framing.** Instrumenting the decoder over
  `parquet-testing/data` shows **32.4% of bit-packed literal runs (2,867 of 8,851) are multi-group**,
  across 14 of the 68 readable files — Impala's and parquet-mr's own output. Multi-group literal runs
  are not a framing EngineeredWood would be introducing; they are one every reader has always had to
  handle. (The largest run in that corpus is 8 groups, not 63, because those files are small.)

## Assessment

The gains are real but modest for typical files: ~13% smaller for V2 dictionary-heavy or small-`n`
list data, ~5% faster general decode, ~16% faster with the fixed-list fast path. The strongest case
is V2 (the default) with dictionaries (also the default).

Against that: this encoder writes every level, dictionary index, and RLE boolean value in every
file, so the blast radius is the whole writer. That risk is covered by the test suite including
byte-identity of the default path, round-trip equivalence across seven data shapes and four batching
depths, encoder reuse across `Reset()`, third-party interop (ParquetSharp, pyarrow, fastparquet), and
the 92-file read-side compatibility corpus — but it remains the reason to land this on its own rather
than bundled with a read-path feature.

Validation against ParquetSharp, pyarrow, fastparquet, DuckDB, arrow-rs (via DataFusion) and the
compatibility corpus is now complete (see Interop). Recommend keeping it opt-in for one release
cycle, then considering it as the default.

The decode-speed half of the trade-off this flag was parked on has since been measured on the reader
side as well: with the group-of-eight bulk unpacker (#236), `ReadBatch` on a dictionary-index stream
runs **2.9× faster** on batched runs than on eight-value ones, and **unchanged** on the eight-value
framing this writer emits by default. So the bulk unpacker only pays on files somebody else wrote —
turning this flag on is what would extend it to EngineeredWood's own. A flip was also dry-run against
the suite: the full Parquet suite passes with `BatchBitPackedRuns` defaulted to `true`, and file size
falls by a constant ~0.123 bytes per value per RLE stream (12.3% / 6.0% / 1.7% of three
representative uncompressed V2 files). Size can never grow: the RLE-versus-literal decision in
`AppendRun` does not consult the batching depth, so batching only ever removes run headers.

## Default

Flipped on in November 2026, after the flag had shipped opt-in through v0.2.0, v0.3.0 and v0.4.0.

**A growth bug came first.** Timing the flip turned up a stream that batching made *larger*:
dictionary indices in runs of 1 to 20 (1M rows, cardinality 1000) grew 551,110 -> 554,432 bytes.
`AppendRun` sent a run of 8 or more straight to RLE only when the literal buffer was *empty*.
Unbatched, the buffer is empty at every group boundary, since each group is flushed as it fills.
Batched, earlier groups are still pending there, so a run of 8 to 15 arriving at a boundary was
bit-packed whole instead. `Batching_NeverGrowsTheStream` missed it because none of its shapes
delivers such a run exactly at a boundary with literals pending. The check is now "at a group
boundary" (`(_pendingCount & 7) == 0`), with any pending whole groups flushed first. Unbatched,
that is the same condition as before, so depth-1 output is unchanged. Batched, each value now goes
RLE or bit-packed exactly as it would unbatched, so batching only ever removes headers.
`Batching_NeverGrowsRandomRuns` sweeps random run structures across widths 1 to 10 and depths 2,
8 and 63 to hold that.

**Measured for the flip** (`dotnet run -c Release -f net10.0 -- batched-write`): 1M rows, one
column, uncompressed, in memory, off and on timed alternately, best of 15. Write and read in ms,
before the growth fix:

| Column | Write off | Write on | Read off | Read on | Size on/off |
| --- | ---: | ---: | ---: | ---: | ---: |
| dict 1000, random | 24.07 | 24.15 | 6.57 | 5.54 | 91.2% |
| dict 1000, random, 14% null | 26.80 | 27.01 | 8.55 | 7.50 | 88.1% |
| dict 16, random | 15.47 | 15.14 | 6.09 | 5.12 | 80.5% |
| dict 1000, runs of 1-20 | 13.35 | 13.44 | 6.18 | 6.08 | 100.6% (the bug) |
| list<int64> length 3 | 81.70 | 77.79 | 17.58 | 16.74 | 95.8% |
| list<int64> length 0-5 | 146.85 | 146.44 | 60.46 | 60.46 | 95.4% |
| bool, 14% null | 20.49 | 19.82 | 8.67 | 8.06 | 61.2% |
| double, 14% null | 8.33 | 8.79 | 3.56 | 3.29 | 99.1% |

Writes cost the same either way, within run-to-run noise. Reads are equal or faster, and files are
smaller. The PR that flipped the default records the numbers after the fix.

## Not covered by the prototype

- **Adaptive depth.** The cap is a flat 63 groups. A writer could choose depth per stream based on
  observed run structure; not explored.
