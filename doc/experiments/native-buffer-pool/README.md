# Native buffer pool under a commit limit (experiment, 2026-11-22)

This is not ready to ship; see "Open items". Background and the first prototype are in
[`doc/plank-comparison.md`](../../plank-comparison.md).

## What the branch adds

`NativePoolMode` (`NativeBufferPool.Mode`, or `EW_NATIVE_POOL=1|2`):

- **Off**: main's behaviour. Every buffer is allocated fresh and freed on dispose.
- **Classic** (`1`): the first prototype. Power-of-two blocks from `NativeMemory.AlignedAlloc`, each fully
  committed and kept whole while idle.
- **Commit** (`2`, Windows only; elsewhere it behaves as Classic):
  - Each block reserves its power-of-two size class with `VirtualAlloc(MEM_RESERVE)` and commits only the pages
    its buffer asked for.
  - A tail beyond max(64 KiB, need/8) is decommitted.
  - Idle blocks count against the cap by their committed bytes.
  - A failed commit frees every idle block and retries once.
  - The default cap is a quarter of (job commit limit − GC hard limit), at most 256 MiB.
    `EW_NATIVE_POOL_CAP_MB` overrides it.

It also fixes two gaps of the first prototype against current main. `GrowExact` used to throw on a pooled buffer;
now it reallocates. `Build(usedBytes)` now trims a pooled buffer that is at most half used.

With each mode forced on, these tests all pass (net10.0): Core 869, Parquet 4,946, ORC 344, Avro 361.

## Harness

[`Program.cs`](Program.cs) / [`poolbench.csproj`](poolbench.csproj). It is not part of the solution.

```
dotnet build doc/experiments/native-buffer-pool/poolbench.csproj -c Release
poolbench gen <dir> [rows]              # ParquetSharp fixtures, uncompressed, one row group
poolbench launch <limitMB> <dir> [reps] # joins a job object with a per-process commit limit,
                                        # then runs each (column, mode, workload) in its own child
```

- **Children** inherit the job, so the GC sees the limit at startup. It took 75%: 768 of 1,024 MiB.
- **Timing:** each child warms up for 3 s, then times at least 3 s of `ReadRowGroupAsync`. It reports the
  median and p90, `PeakPagedMemorySize64` (peak commit) and the pool's counters.
- **Workloads:** `steady` disposes each batch before the next read. `window8` keeps the last 8 batches alive.
- **Row count:** use a count that is not a power of two (1,250,000). At 2²⁰ rows every numeric buffer is exactly a
  size class, which hides the rounding cost.

## Results (i9-12900K, Windows 11, .NET 10)

Raw output is in [`results/`](results/). Below: 1,250,000 rows, `window8`, 1 GiB per-process limit.

| Column | Median read time, ms (off / classic / commit) | Peak commit, MB (off / classic / commit) |
| --- | --- | --- |
| int32 plain | 1.39 / 0.78 / 0.77 | 80 / 109 / 80 |
| int32 dictionary (w11) | 1.70 / 1.18 / 1.21 | 73 / 104 / 74 |
| double plain | 3.26 / 1.93 / 1.86 | 130 / 188 / 131 |
| int64, 10% nulls | 5.86 / 4.27 / 4.15 | 134 / 198 / 140 |
| string | 17.5 / 15.5 / 15.6 | 279 / 430 / 278 |

- **Speed:** commit mode is as fast as classic, at 0.55–0.89x of off.
- **Commit:** commit mode's peak commit is within about 4% of off. Classic's is 20–55% higher.
- **At a 384 MiB limit:** classic `string`/`window8` died with an unhandled CLR exception, most likely out of
  memory. Commit mode survived at off's peak commit.
- **The derived cap is too small at 384 MiB.** It came out at 24 MiB, smaller than one string read (about 25 MB),
  so string blocks mostly missed. The gain fell to 18.1 → 16.7 ms.
- **Correctness:** every mode returned identical bytes (checksums in the raw output).

## Open items

- **Arrow API:** it depends on Arrow's internal `ArrowBuffer(IMemoryOwner<byte>)` constructor, reached through
  `[UnsafeAccessor]`. Shipping needs a public API in arrow-dotnet.
- **Linux:** commit mode needs an `mmap(PROT_NONE)` / `mprotect` / `madvise(MADV_DONTNEED)` version.
- **Retry path:** the trim-and-retry on a failed commit never fired in any run, so it is untested.
- **Cap policy:** the cap should hold at least one working set, not just a fraction of the limit.
- **Use-after-dispose:** a stale read returns another buffer's data silently. Planned: debug poisoning with distinct
  freed and uninitialized patterns, a quarantine that checks the poison is intact, and an opt-in mode that
  decommits blocks in quarantine.
- **Measurement:** one machine, and one rep of the 1,250,000-row sweeps.
