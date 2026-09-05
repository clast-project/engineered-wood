# Batched bit-packed literal runs (prototype)

A writer-side prototype, separate from the [fixed-length list fast path](fixed-list-fast-path.md)
but discovered while benchmarking it. Opt-in via `ParquetWriteOptions.BatchBitPackedRuns`.

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

## Not covered by the prototype

- **Adaptive depth.** The cap is a flat 63 groups. A writer could choose depth per stream based on
  observed run structure; not explored.
