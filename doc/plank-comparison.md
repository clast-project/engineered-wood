# Plank, and where EW's column reads actually spend their time

**Status (2026-11-01): measured; one prototype, not merged.** We compared EW's Parquet reader with
[Plank](https://github.com/Kuinox/Plank), a new MIT-licensed C# Parquet library, looking for anything
worth borrowing. Its headline technique, a fused SIMD dictionary decoder, turned out **not** to be the
reason it out-read EW. The reason was output memory. EW allocates every column's Arrow buffer fresh
from the OS on every read, and paying the page faults for it is often half of a read. A prototype pool
behind `NativeBuffer<T>` makes required-column reads **1.4–2.8× faster** and passes every test with the
pool forced on.

This note records the comparison, the method that kept the benchmark honest, and what is left to do
before the pool could ship. That way neither the first, wrong conclusion nor the right one has to be
rediscovered.

## What Plank is

Surveyed at **`e7697e5f`** (2026-10-24). Plank went public on 2026-08-26, and almost all of it is one
author's work.

- **Size and platform:** about 44k lines of library code, plus 4k lines of Roslyn generators. Targets
  **net10.0 only**. EW's Parquet library is about 34k lines across net10.0, net8.0 and netstandard2.0.
- **API layers.** The primary API is a source-generated row API: `[ParquetSchema]` on a C# class.
  Underneath it sits a column API whose schema is built at runtime (`ParquetSchema` / `ColumnDefinition`,
  `SerializedColumn<T>.Serialize(ReadOnlySpan<T>)`, `RowGroup.Column<T>(ordinal)`). So the cores can be
  compared directly, without the codegen in the way.
- **Data model, not Arrow's.** Nulls are passed as `T?`, strings as one `byte[]` per value, and nesting
  as jagged arrays. A bridge from Arrow would have to copy.
- **I/O.** Synchronous, random-access reads, one read per page. No async, no cloud I/O.
- **Pruning.** It parses statistics, the page index and Bloom filters, and hands them to the caller.
  There is no predicate engine and no dictionary pruning.
- **Writer defaults.** PLAIN with no dictionary unless asked; V2 data pages. It writes no `created_by`
  by default, which makes some readers distrust its statistics (PARQUET-246).
- **Nesting on write.** A struct cannot contain a collection, and a leaf can sit under at most one
  optional level. Each repeated column goes into a single unbounded page with no dictionary.
- **Things EW lacks:**
  - appending to an existing file, including merging into its last row group;
  - a partitioned dataset writer;
  - a pluggable per-column page strategy (`IPageStrategy`).
- **ALP is encoding 10**, the same number EW uses. That makes a cross-reader conformance test cheap.

### Techniques that looked worth borrowing

1. **Dictionary-index decoding with AVX2 + BMI2 `pdep`, then a gather** (`ColumnChunkReader.cs:~4228-4360`).
   - It has dedicated kernels for bit widths 1, 2, 8, 9, 11 and 19/20, and the 11-bit path copies
     repeated blocks.
   - It checks the CPU model and avoids AVX2 gather on Coffee Lake (family 6, model 158), where gather
     is slow.
2. **DELTA_BINARY_PACKED kernels generated and unrolled per bit width** (`Plank.InternalGen`).
3. **Zero-allocation tests.** After warm-up, `GC.GetAllocatedBytesForCurrentThread()` must grow by
   exactly 0. This also covers the writer's worker threads, through a hook that runs when each thread
   starts.
4. **Output buffers that are reused.** A column is read as reusable 256 KB pooled batches, and when the
   source is an in-memory array, page payloads point into it without copying.

Item 4 is the one that mattered; see below.

### Bugs found in Plank (not reported upstream)

Both are at `Plank/Reading/Logical/Internal/ColumnChunkReader.cs` at `e7697e5f`.

- **`:4115`: integer overflow in the generic `DecodeDictionaryIndexesIntoBuffer<T>`.** It multiplies in
  unchecked `uint` arithmetic, while the `int` version beside it (`:4160`) guards against exactly this.
  The wrapped length of zero is then passed to SIMD kernels that use unchecked `Unsafe.Add`.
- **`:4977`: `ReadBitPackedValue` truncates widths 27, 29, 30 and 31.** It gathers 4 bytes, then shifts
  by up to 7 bits. These are the same widths EW fixed in #236. Plank only reaches this for levels and
  booleans today, so it is latent.

## The benchmark, and the mistake it nearly made

### Method

- **Fixtures:** 1,048,576-row single-column files, uncompressed, in one row group.
  - Types: int32, int64 and double; required, and optional with 10% nulls.
  - Dictionary bit widths 1, 8, 11 and 16 (2, 200, 2,000 and 50,000 distinct values, uniformly random
    indices), plus a PLAIN version of each as a baseline.
- **Writer:** ParquetSharp (Arrow C++), so the writer belongs to neither library under test.
  - Every dictionary fixture was confirmed to be RLE_DICTIONARY, with no fallback to PLAIN.
  - All three readers returned identical values on all 30 fixtures.
- **Readers:** EW `main` at `3357a8c1`, through `ParquetFileReader.ReadRowGroupAsync`. Plank, through
  `RowGroup.Column<T>`, draining its batches into an output array. ParquetSharp, as a reference.
- **Machine:** i9-12900K, Windows 11, .NET 10.0.11.
- **Harness:** BenchmarkDotNet 0.15.8 with `InProcessEmitToolchain`; 3 warm-ups, 12 iterations. The
  harness project lived under `%TEMP%`, and BenchmarkDotNet's project lookup failed there on a protected
  folder (`WinSAT`), hence in-process. The A/B comparisons below come from runs in one process, so they
  share JIT and GC settings.

### First result: Plank about 2× faster, including on PLAIN

Required columns, times in µs:

| | EW | Plank | ParquetSharp |
| --- | --- | --- | --- |
| int32 PLAIN | 1,518 | 927 | 1,635 |
| int32 w8 | 1,722 | 662 | 1,606 |
| int32 w11 | 1,757 | 693 | 1,845 |
| int32 w16 | 2,093 | 3,140 | 2,229 |
| double PLAIN | 3,109 | 1,557 | 3,414 |
| double w8 | 2,412 | 1,384 | 1,715 |

It is tempting to read this as "port Plank's kernel". **The PLAIN row rules that out.** Decoding PLAIN
is just a memory copy, so a 2× gap there cannot be about dictionary decoding.

The harness was the cause. Plank copied into an array allocated once and reused on every call. EW
allocates a fresh 4–8 MB native Arrow buffer on every call and takes page faults to touch it.
BenchmarkDotNet's `Allocated` column hid this, because it counts managed memory only: EW showed about
11 KB per call.

### The control: charge Plank the same fresh native buffer

`PlankFreshNative` copies Plank's batches into a buffer from `NativeMemory.AlignedAlloc`, allocated and
freed on every call, the way EW's Arrow memory is. Times in µs:

| Required column | EW | Plank, reused output | Plank, fresh output |
| --- | --- | --- | --- |
| int32 PLAIN | 1,543 | 918 | 1,761 |
| int32 w8 | 1,692 | 652 | 1,485 |
| int32 w11 | 1,726 | 707 | 1,523 |
| int32 w16 | 2,004 | 2,733 | 3,526 |
| double w8 | 2,560 | 1,599 | 3,184 |
| double w11 | 2,557 | 1,178 | 2,766 |

With a fresh output buffer, Plank leads by only about 12% at its specialized int32 widths and is slower
on double. The control slightly overcharges Plank, because it adds one copy from warm to cold memory that
EW does not make. Either way, the conclusion holds: **fresh output memory costs about 0.85 ms per 4 MB
and 1.5 ms per 8 MB**, which is roughly half of EW's read time on these columns.

**The general lesson:** when two libraries differ in who owns the output buffer, a benchmark has to
charge both the same allocation, or it measures allocation policy and calls it decoding speed.

## The prototype: a native buffer pool

Branch `perf/native-buffer-pool`, commit `442d166f`.

- **One hook point.** `NativeBufferPool` sits behind EW's `NativeBuffer<T>` facade, which every EW native
  allocation already goes through: Parquet, ORC, Avro and the Delta checkpoint builders. No caller
  changed.
- **Size classes.** Requests above 64 KiB round up to a power of two. The caller still sees exactly the
  length it asked for, so `ArrowBuffer.Length` does not change. Smaller requests are not pooled, because
  the C runtime heap already reuses small blocks.
- **Return path.** When an Arrow array is disposed, its block goes back to the pool. At most 256 MiB of
  idle memory is kept; anything returned beyond that is freed.
- **Growing a buffer** reuses the existing block when its size class has room, and otherwise rents a
  larger block and copies.
- **Off by default.** It is switched on with `NativeBufferPool.Enabled` or `EW_NATIVE_POOL=1`, so one
  build can run both modes.
- **Arrow access.** Ownership passes to Arrow through `ArrowBuffer(IMemoryOwner<byte>)`, which is
  `internal` in Apache.Arrow 23.0.0. The prototype reaches it with `[UnsafeAccessor]`, so it works on
  net8.0+ only.

### Correctness

The hazard a pool introduces is code that allocates with `zeroFill: false` and relies on fresh OS pages
being zero. A reused block holds stale bytes. With `EW_NATIVE_POOL=1`, every suite passes on net10.0:

| Suite | Passed | Failed |
| --- | --- | --- |
| Core | 516 | 0 |
| Parquet | 2,886 | 0 |
| ORC | 225 | 0 |
| Avro | 294 | 0 |

The three-way value check also passes with the pool on, and the pool served 87 of 90 rents from idle
blocks.

### Measured: one process, the same build, pool on vs off

Times in µs; the ratio is against `main`'s behaviour.

| | main | pooled | Plank |
| --- | --- | --- | --- |
| int32 PLAIN, required | 1,516 | 745 (0.49×) | 961 |
| int32 w1, required | 1,371 | 806 (0.59×) | 639 |
| int32 w8, required | 1,689 | 1,133 (0.67×) | 680 |
| int32 w11, required | 1,719 | 1,143 (0.66×) | 696 |
| int32 w16, required | 2,018 | 1,464 (0.73×) | 2,793 |
| double PLAIN, required | 3,302 | 1,700 (0.52×) | 1,643 |
| double w1, required | 2,091 | 760 (0.36×) | 1,061 |
| double w8, required | 2,409 | 1,096 (0.46×) | 1,380 |
| double w16, required | 2,779 | 1,463 (0.53×) | 2,108 |
| int32 w8, optional | 4,005 | 3,367 (0.84×) | 4,208 |
| double w8, optional | 4,860 | 3,376 (0.69×) | 5,235 |

- **Optional columns:** pooled EW beats Plank at every width. The gain is smaller because the nullable
  path decodes the non-null values densely, then scatters them into a second fresh buffer
  (`ArrowArrayBuilder.cs:154`). That remaining copy is a separate, smaller lever.
- **The dictionary kernel, revisited:** with allocation removed, Plank's fused kernel does show, but
  only for required int32 at widths 1, 8 and 11. There it leads by about 450 µs per million values. For
  double, and for width 16, EW is already faster. **A narrow port (int32, those widths) is now worth
  considering; a general port is not.**
- **Two EW costs that kernel would remove.** EW unpacks all indices into a rented `int[]`, then makes a
  separate `CheckIndices` pass, then runs a scalar lookup loop. `CheckIndices` skips its pass only when
  2^width ≤ the dictionary size, which almost never happens in real data. It ran for the w8, w11 and w16
  fixtures.

## Before the pool could ship

1. **A public Arrow API.** Arrow needs a public constructor that takes ownership, or an equivalent
   factory, in place of the `UnsafeAccessor`. That is an upstream arrow-dotnet change, and it also
   decides whether netstandard2.0 can have the pool.
2. **Linux.** Only Windows has been measured. glibc's `malloc` may already reuse large freed blocks, so
   the gain there may be smaller. WSL can run the same harness.
3. **Default on or off.** The benchmark is the best case: one thread that disposes each batch before the
   next read.
   - A caller that holds batches longer gets fewer reuses.
   - Up to 256 MiB of idle memory stays reserved.
   - Use-after-dispose stops crashing and returns stale data instead.

   An opt-in setting is the safer start.
4. **Realistic workloads.** A multi-column, multithreaded scan (for example a Delta table scan) should
   confirm that the gain holds outside a single-column benchmark.

## Not pursued, and why

- **A general port of the `pdep` + gather dictionary decoder.** After the pool, it helps required int32
  at three widths only. EW is already faster for double and at width 16.
- **Adopting Plank's `T?` output.** It is slower than EW's values-plus-bitmap layout for nullable
  columns, and not Arrow.
- **Plank as a dependency.** It is net10.0 only and synchronous, and its data model would need a copy at
  the Arrow boundary.

Still open from the survey, and not yet measured:

- the per-width DELTA_BINARY_PACKED kernels, against `DeltaBinaryPackedDecoder`;
- an ALP cross-read test between the two libraries;
- Plank as an extra Parquity engine, for two-way differential testing;
- zero-allocation assertions in EW's own tests.
