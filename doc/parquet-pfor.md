# PFOR (Patched Frame of Reference)

Implementation notes for the PFOR encoding, proposed in
[apache/parquet-format#617](https://github.com/apache/parquet-format/pull/617)
(`PforEncoding.md`). Section references below are to that file.

PFOR is **experimental** here: the proposal is a work in progress and the wire format may change.
The public surface is `IntegerEncoding.Pfor`, guarded by `EWPARQUET0005`.

## What is implemented

- **Encoding 11**, for INT32 and INT64, V2 data pages. See
  [the encoding-number note](#encoding-numbers) below.
- Both modes: plain (frame of reference over values) and **delta** (frame of reference over the
  differences), chosen per vector.
- Default vector size 1024 (`log_vector_size` 10); the reader accepts the full `[3, 15]` range.
- Every bit width from 0 to the type's maximum, including 64 — which needs all seven bits of the
  width field, the flag taking the eighth.
- A per-page PLAIN fallback in the writer, so turning the setting on cannot make a file bigger.

## Layout

The page layout is deliberately close to ALP's, and `PforDecoder` is shaped like `AlpDecoder`:

```
+-------------+-----------------------------+------------------------+
|   Header    |        Offset Array         |      Vector Data       |
|  (7 bytes)  |   (num_vectors * 4 bytes)   |       (variable)       |
+-------------+-----------------------------+------------------------+
```

- Header: `packing_mode` (must be 0), `log_vector_size`, `value_byte_width` (4 or 8), then
  `num_elements` as a little-endian uint32.
- Offsets are measured **from the start of the offset array**, not from the start of the page.
- Each vector: `frame_of_reference` (4 or 8 bytes), a width byte, `num_exceptions` (uint16), then
  — only in the delta mode — a `StartValue`, then the packed residuals, the exception positions
  (uint16 each), and the exception values (full width each).

The width byte carries the width in bits 0..6 and the delta flag in bit 7. Seven bits rather than
six because the range is 0..64 inclusive; masking with six reads a full-width INT64 vector as
width 0, which decodes as a constant vector with no error to say so. `PforEncodingTests`
(`Golden_Int64AtFullWidth`) pins that.

Exception values are never residuals: each is the value the packed stream would have carried had
it fitted — the original value in a plain vector, the **difference** in a delta one. So the patch
must run **before** the prefix sum, or the placeholder zero is carried into every value after it.

## Where this diverges from the spec text

Two places, both found by measuring rather than reading. Neither is a difference of opinion about
the spec; both are places where the spec contradicts itself.

### 1. The frame of reference is not the minimum

§Encoding says `frame_of_reference = min(values[])`. On the shape PFOR exists for — a tight
cluster with a low sentinel — that is the wrong answer by a wide margin. The sentinel becomes the
frame, every ordinary value sits tens of thousands above it, and the width is set by the gap
rather than by the cluster. Nothing is left for the exception mechanism to do.

The spec's own Example 3 is that column, and quotes a width of 11 "for range 2450815-2453005" with
"~10 exceptions, the null sentinel outliers". That is only reachable with a frame **above** the
minimum: with the minimum as the frame, the sentinels have residual 0 and it is the 1,014 ordinary
values that would have to be exceptions.

So `PforEncoder` treats the minimum as a *candidate*, not the rule. It buckets the vector's range
into 256 buckets, slides a window over the bucket counts for each candidate width, lowers the
winning window's boundary onto the smallest value it actually covers, and costs that frame
exactly. The minimum is always among the candidates and is the only one costed from a real
histogram, so the search cannot do worse than the naive rule. This is convergent with
[apache/arrow-rs#10977](https://github.com/apache/arrow-rs/pull/10977), which does the same thing;
[apache/parquet-java#3775](https://github.com/apache/parquet-java/pull/3775) uses the minimum.

The measured cost of getting this wrong is in the table below — 0.65x against
DELTA_BINARY_PACKED with the naive frame, 5.31x with the search.

(Example 3 also quotes `ceil(log2(2191)) = 11`; it is 11.1, so its own formula gives 12.)

### 2. Width 0 in the delta mode

§Decoding's "Special case" says a reader may fill a width-0 vector with the start value instead of
running the general path, and "get the same answer for any frame". That is not true: the fill and
the general path agree only when the frame is 0.

A conforming writer cannot produce anything else — `d[0]` is 0 and the frame is the minimum of the
differences, so an all-zero residual array forces a frame of 0 — so the divergence is unreachable
from real data. But arrow-rs takes the fill path, and its own test asserts `5, 8, 11, 14` for a
page where the general path gives `8, 11, 14, 17`.

This library runs the general path, which is what the numbered decode steps say. Pinned in
`DeltaVectorAtWidthZeroWithANonZeroFrameTakesTheGeneralPath`.

## Compression

200,000 INT64 values per shape, one row group, dictionary off, V2 pages. "vs DBP" is
DELTA_BINARY_PACKED's size over PFOR's, so above 1.00x is a PFOR win.

Uncompressed:

| column shape | PLAIN | DELTA_BINARY_PACKED | PFOR | vs DBP |
|---|---:|---:|---:|---:|
| date keys + null sentinel | 1,600,414 | 263,500 | 49,662 | **5.31x** |
| sorted in stretches | 1,600,414 | 21,430 | 6,882 | **3.11x** |
| store keys (tight cluster) | 1,600,414 | 259,802 | 149,470 | **1.74x** |
| sorted ids | 1,600,414 | 8,231 | 6,882 | **1.20x** |
| timestamps, 7ms apart | 1,600,414 | 8,239 | 6,882 | **1.20x** |
| sequence ids with gaps | 1,600,414 | 24,060 | 22,638 | **1.06x** |
| uniform random | 1,600,414 | 1,622,327 | 1,578,366 | 1.03x |

With Zstd:

| column shape | PLAIN | DELTA_BINARY_PACKED | PFOR | vs DBP |
|---|---:|---:|---:|---:|
| date keys + null sentinel | 16,332 | 33,624 | 11,783 | **2.85x** |
| store keys (tight cluster) | 2,044 | 798 | 1,362 | 0.59x |
| sequence ids with gaps | 196,326 | 922 | 1,958 | 0.47x |
| sorted ids | 240,834 | 456 | 1,273 | 0.36x |
| timestamps, 7ms apart | 215,516 | 466 | 1,317 | 0.35x |
| sorted in stretches | 209,888 | 490 | 1,637 | 0.30x |
| uniform random | 1,600,469 | 1,622,385 | 1,578,421 | 1.03x |

**Read this as: PFOR wins uncompressed on every shape, and with an outer codec it wins only where
the column has outliers.** That is not a defect in either encoding. DELTA_BINARY_PACKED's output
on clean sequential data is highly compressible — long runs of identical miniblock widths and
zeros — so zstd finds most of what PFOR had already removed, and then some. PFOR's output is
tightly packed and has little structure left for zstd to exploit.

So the setting is worth turning on for columns that cluster with outliers, and worth leaving alone
for columns that are simply sorted, if the file is compressed. The `sorted in stretches` row is
the interesting middle: it is where PFOR's per-vector delta choice pays and DELTA_BINARY_PACKED's
unconditional differencing does not.

## Encoding numbers

PFOR is 11, which is what both implementations behind the proposal write. FSST used to be 11 here
and moved to 12 to make room; see [parquet-fsst.md](parquet-fsst.md) and the remarks on
`Encoding.Fsst`. ALP is 10 and is settled — merged into `parquet.thrift` on parquet-format `main`.

## Testing

`test/EngineeredWood.Parquet.Tests/Parquet/Data/PforEncodingTests.cs`:

- Round trips over 22 column shapes, including columns spanning the full type range, where the
  differencing and the prefix sum both wrap.
- **Every bit width, 0 to 32 and 0 to 64**, swept rather than sampled, at a vector length that is
  a multiple of eight and one that is not. This is the class of bug that shipped once already in
  the RLE decoder (widths 27, 29, 30, 31 truncated; 32 read as zero), and it is invisible at every
  width but the broken one.
- Three byte-for-byte golden vectors taken from arrow-rs's tests. A round trip through our own
  encoder cannot tell a wire-format disagreement from a self-consistent one, so these are the only
  assertions that would catch us writing a page nobody else can read.
- Malformed-page rejection: bad `packing_mode`, `log_vector_size` out of range, a
  `value_byte_width` that disagrees with the column, a delta vector truncated before its start
  value, an exception position past the end of its vector, and vector offsets outside the page.
