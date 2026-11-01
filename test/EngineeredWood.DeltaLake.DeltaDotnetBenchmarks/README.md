# EngineeredWood vs delta-dotnet

Head-to-head BenchmarkDotNet suite: `EngineeredWood.DeltaLake.Table` against
[delta-dotnet](https://github.com/delta-incubator/delta-dotnet) (NuGet `DeltaLake.Net`, a P/Invoke
wrapper over delta-rs 0.32 + delta-kernel-rs, with DataFusion for SQL).

The project is **not in `engineered-wood.slnx`**: `DeltaLake.Net` is a ~220 MB package of native binaries
that nothing in the build or test gate needs. Build and run it directly.

## Running

```sh
# 1. Check that both engines do the same work (row counts, and the table each mutation leaves behind,
#    read back by EW, by delta-dotnet's kernel scan and by delta-rs's DataFusion scan; for the operations that
#    keep the rows, that the checkpoint was published, the file count fell, no unreferenced data file remains,
#    the constraint is in the metadata, and the restore re-added the 100 small files). Builds the fixtures. Exits non-zero on any mismatch, or when the filter matches no class.
dotnet run -c Release --project test/EngineeredWood.DeltaLake.DeltaDotnetBenchmarks -- --verify [ClassFilter]

# 2. Benchmark (all, or e.g. --filter *FullScan*).
dotnet run -c Release --project test/EngineeredWood.DeltaLake.DeltaDotnetBenchmarks -- --filter "DeltaDotnetComparison.*"
```

Run from the worktree whose code you mean to measure: BenchmarkDotNet locates the project from the cwd.

Fixture tables are cached under `%TEMP%\ew-vs-delta-dotnet\templates` (override with `EWDD_BENCH_ROOT`).
Delete a `<name>.ready` marker, or the folder, to rebuild them.

## What is measured

Every category has EW as the baseline, so **Ratio = delta-dotnet time / EW time** (above 1 means EW is faster).
Fixtures are written by *each* engine with its own default writer settings (`Writer` param), and every reader
runs against both.

| Class | Category | Operation |
|---|---|---|
| `FullScanBenchmarks` | FullScan | open + scan all of a 1M-row, one-commit table. delta-dotnet both ways: kernel `ReadAsArrowTableAsync` and DataFusion `SELECT *` |
| `FilteredScanBenchmarks` | FilteredScan | `SELECT id, amount WHERE …` on a 20-file table: a 1-file id range (`Selective`), and 1-in-16 rows spread everywhere (`OneCategory`) |
| `SnapshotBenchmarks` | OpenLatest, OpenAtVersion, History | log replay of a 100-commit table, with and without a checkpoint |
| `ChangeFeedBenchmarks` | ChangeFeed | the whole CDF of a table with 10 appends + UPDATE + DELETE |
| `WriteBenchmarks` | CreateAndWrite | create + write 1M rows in one commit |
| `AppendBenchmarks` | Append10K | open a 100-commit table + append 10K rows |
| `DmlBenchmarks` | Delete, Update | copy-on-write DELETE / UPDATE of 1 row in 16 on the one-commit 1M-row table |
| `MergeBenchmarks` | Merge | upsert 10K rows on `id` into the one-commit 1M-row table: 5K matched (their `amount` doubled), 5K inserted |
| `ReplaceWhereBenchmarks` | ReplaceWhere | replaceWhere on a non-partition column, the replacements with `amount` doubled: a 10K-row id range on the 20-file table (`Selective`), and one category in 16 on the one-commit 1M-row table (`OneCategory`). delta-dotnet: `InsertAsync` with `SaveMode.Overwrite` and a `Predicate` |
| `ConstraintBenchmarks` | AddConstraint | `ADD CONSTRAINT … CHECK (amount >= 0)` on the one-commit 1M-row table; every row satisfies it, so both engines read the whole column |
| `RestoreBenchmarks` | Restore | RESTORE the compacted 100-commit table to the version before its compaction: 100 small files re-added, the compacted ones removed |
| `DeletionVectorDeleteBenchmarks` | DeleteDeletionVector | EW's own deletion-vector DELETE of the same rows, on an EW-written DV table; no writer parameter and no delta-dotnet counterpart |
| `MaintenanceBenchmarks` | Checkpoint, Compact, Vacuum | on the 100-small-file table |

## Read these numbers with care

- **The two writers lay the "same" table out differently.** Both now write the one-commit 1M-row table as
  **one file**: 22.5 MB when EW writes it, 34 MB when delta-dotnet does. Until #559, EW wrote one file per input
  `RecordBatch`, which made it 16 files of about 1.5 MB. Multi-file tables parallelise better, DataFusion's
  above all, so the full-scan numbers fell for the EW-written table when it became one file.
- **Allocated is managed memory only.** delta-dotnet works in the Rust heap, which BenchmarkDotNet cannot
  see, so its Allocated column is close to zero and **not comparable** to EW's.
- **Parallelism differs.** delta-rs runs on a multi-threaded tokio runtime and DataFusion parallelises scans.
  Wall-clock times are compared; CPU time is not.
- **EW's `DeltaReadOptions.Filter` prunes, it does not filter.** The like-for-like EW method applies the
  predicate per row itself (as an EW host must); `EW_PruneOnly` shows the pruning alone and returns a superset.
- **EW's predicate DELETE copy-on-writes on a table without deletion vectors** (#562), as delta-rs does; before
  #562 it refused a partial-file delete there. `EW_DeleteHostComposed` keeps the copy-on-write a host had to
  compose until then (a Locator scan, evaluating the predicate, then
  `DeleteRowsAsync(…, RowDeleteMode.CopyOnWrite)`) as a reference row. EW's own deletion-vector DELETE is
  measured separately (`DeletionVectorDeleteBenchmarks`), on an EW-written DV table.
- **EW has no "open at version".** It opens the latest snapshot and then builds the older one, so its
  OpenAtVersion includes a latest-snapshot load.
- **delta-dotnet's `InsertOptions.MaxRowsPerGroup` defaults to 100**, which delta-rs uses as its
  `write_batch_size`. The write benchmark measures it at that default and at 64K.
- Construction of the delta-dotnet `DeltaEngine` (its tokio runtime) is a one-time host cost and is not
  measured.

## A delta-dotnet correctness bug the verify pass found

`ITable.ReadAsArrowTableAsync` / `ReadAsDataFrameAsync` **ignore deletion vectors**: on a table with a DV they
return the deleted rows. `ArrowFFIInterOpHandler.ReadParquetAsArrow` receives the kernel's `selectionVector`
and never applies it. delta-rs's DataFusion path (`QueryAsync`) honours the same DV, as EW does, so the DV
itself is fine. `--verify` reports this as a `WARN` on `EW_DeleteDeletionVector`. Filed upstream as
[delta-dotnet#234](https://github.com/delta-incubator/delta-dotnet/issues/234).

## Results, 2026-11-01

i9-12900K (16 cores / 24 threads), Windows 11, .NET 10.0.11, DeltaLake.Net 0.33.1, at e3092d09 (after #568). One
full run of every class. Mean times; the ratio is delta-dotnet's mean over EW's. The
**EW-written / DD-written** columns say which engine wrote the table being read or changed. A ratio above 1
means EW is faster.

| Operation | EW | delta-dotnet | DD/EW ratio (EW-written / DD-written) |
|---|---|---|---|
| Full scan, 1M rows | 17.2 / 21.7 ms | DataFusion 42.9 / 64.1 ms; kernel 90 / 111 ms | 2.5 / 3.0 (DataFusion); 5.2 / 5.1 (kernel) |
| Filtered scan, 1 file of 20 (`Selective`) | 4.7 / 5.0 ms | 9.9 / 11.9 ms | 2.1 / 2.4 |
| Filtered scan, 1 row in 16 (`OneCategory`) | 23.4 / 27.6 ms (prune only: 13.4 / 18.2 ms) | 20.8 / 23.1 ms | **0.89 / 0.84** |
| Open latest, 100 commits, no checkpoint | 18.2 / 17.8 ms | 43.7 / 62.4 ms | 2.4 / 3.5 |
| Open latest, 100 commits, checkpointed | 8.6 / 6.9 ms | 5.4 / 7.7 ms | **0.63** / 1.11 |
| Open at version 50, no checkpoint / checkpointed | 27–28 / 16–18 ms | 31–39 / 30–40 ms | 1.1–1.4 / 1.7–2.5 |
| History, no checkpoint / checkpointed | 33 / 22–23 ms | 40–48 / 17 ms | 1.2–1.5 / **0.73–0.75** |
| Change data feed | 32 / 47 ms | 159 / 182 ms | 4.9 / 3.9 |
| Create + write 1M rows | 179 ms (min 133, noisy) | 202 ms (default) / 178 ms (64K batches) | 1.13 / **1.00** |
| Open + append 10K rows, no checkpoint / checkpointed | 30–38 / 18–25 ms | 56–66 / 25–29 ms | 1.8 / 1.2–1.3 |
| DELETE 1 row in 16, copy-on-write | 290 / 264 ms (min 205 / 213; host-composed: 245 / 248) | 300 / 328 ms | 1.03 / 1.24 |
| DELETE 1 row in 16, EW deletion vector (EW-written DV table only) | 44 ms | none (delta-rs writes no DVs) | — |
| UPDATE 1 row in 16 | 218 / 194 ms | 239 / 246 ms | 1.10 / 1.27 |
| MERGE, upsert 10K rows (5K matched) | 225 / 232 ms | 2,466 / 2,484 ms | **11.0 / 10.7** |
| replaceWhere, 10K-row id range (`Selective`) | 37.7 / 33.9 ms | 48.4 / 51.7 ms | 1.28 / 1.52 |
| replaceWhere, 1 category in 16 (`OneCategory`) | 248 / 258 ms | 243 / 252 ms | **0.98 / 0.98** |
| ADD CONSTRAINT `amount >= 0` over 1M rows | 17.2 / 16.9 ms | 50.3 / 64.0 ms | 2.9 / 3.8 |
| RESTORE to before a compaction | 55 / 42 ms | 109 / 101 ms | 2.0 / 2.4 |
| Checkpoint, 100 commits | 32 / 26 ms | 92 / 92 ms | 2.9 / 3.5 |
| Compact 100 small files | 279 / 253 ms | 474 / 472 ms | 1.7 / 1.9 |
| Vacuum 100 removed files | 60 / 48 ms | Full 128 / 112 ms; Lite 115 / 103 ms | 2.1 / 2.3 (Full) |

Compared with the 2026-10-31 run:

- **New rows:** MERGE (#563), replaceWhere (#566), ADD CONSTRAINT (#565) and RESTORE (#567). EW's MERGE is about
  11× faster than delta-rs's. The target is one file, so neither engine prunes anything, and the gap is in the
  match itself. EW hashes the 10K source keys and probes them as it reads the target. delta-rs plans a DataFusion
  join, which is suspected, not profiled, as the cost. The one-category replaceWhere is level:
  like DELETE, it is one full-file rewrite either way.
- **Commit metadata (#564/#577) and mergeSchema (#568) have no rows.** They add next to nothing to a commit or an
  append, and appends are already measured.
- **Log replay without a checkpoint is about 60% slower for EW in absolute terms than on 10-31** (open latest
  11 to 18 ms, history 19 to 33 ms), and delta-dotnet is 20–25% slower. The code did not cause it. The same
  benchmarks at efda508c (the 10-31 code) measure the same on the same fixtures, and the delta-dotnet-written
  fixtures were not rebuilt. The machine did, so compare the ratios between runs, not the times.
- **Single-shot mutations are noisy for EW.** Each iteration is one invocation, and EW's means carry GC pauses
  (the managed heap) that delta-rs does not have. The min is printed where it differs much from the mean.

Where delta-dotnet wins or draws, the cause is known for some cases and suspected for the rest:

- **Non-selective filtered scans.** Row-level predicate evaluation was EW's bottleneck until #560 (87 ms, about
  100 ns/row, for `category = 'cat_03'` over 1M rows; now 2.4 ms). What remains is the scan itself: EW's prune-only
  read is 13–18 ms, and DataFusion's parallel scan does the whole query in 21–23 ms.
- **Opening a checkpointed table, and history on one:** delta-kernel-rs is up to 1.6× faster at reading a small
  checkpoint.
- **Full-file rewrites** (copy-on-write DELETE, UPDATE, one-category replaceWhere): close to level. Both engines
  read and rewrite the one 1M-row file.
- **Write:** level with delta-dotnet's 64K-batch setting, but EW allocates about 322 MB of managed memory for 1M rows
  (286 MB before #559: joining the batches into one row group costs a copy), and its mean carries GC noise.
