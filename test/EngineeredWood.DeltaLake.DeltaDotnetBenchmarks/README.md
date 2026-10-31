# EngineeredWood vs delta-dotnet

Head-to-head BenchmarkDotNet suite: `EngineeredWood.DeltaLake.Table` against
[delta-dotnet](https://github.com/delta-incubator/delta-dotnet) (NuGet `DeltaLake.Net`, a P/Invoke
wrapper over delta-rs 0.32 + delta-kernel-rs, with DataFusion for SQL).

The project is **not in `engineered-wood.slnx`**: `DeltaLake.Net` is a ~220 MB package of native binaries
that nothing in the build or test gate needs. Build and run it directly.

## Running

```sh
# 1. Check that both engines do the same work (row counts, and the table each mutation leaves behind,
#    read back by EW, by delta-dotnet's kernel scan and by delta-rs's DataFusion scan; for maintenance, that
#    the checkpoint was published, the file count fell, and no unreferenced data file remains). Builds the
#    fixtures. Exits non-zero on any mismatch, or when the filter matches no class.
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
| `DeletionVectorDeleteBenchmarks` | DeleteDeletionVector | EW's own deletion-vector DELETE of the same rows, on an EW-written DV table; no writer parameter and no delta-dotnet counterpart |
| `MaintenanceBenchmarks` | Checkpoint, Compact, Vacuum | on the 100-small-file table |

## Read these numbers with care

- **The two writers lay the "same" table out very differently.** EW writes one Parquet file per input
  `RecordBatch` (`DeltaTableOptions.TargetFileSize` is declared but nothing reads it; #559), so the one-commit 1M-row
  table is **16 files, 23 MB** when EW writes it (65,536-row batches), and **1 file, 34 MB** when delta-dotnet
  does. Multi-file tables parallelise better, DataFusion's above all, which is most of the gap between the two
  `Writer` rows of FullScan.
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

## Results, 2026-10-31

i9-12900K (16 cores / 24 threads), Windows 11, .NET 10.0.11, DeltaLake.Net 0.33.1. Mean times. The
**EW-written / DD-written** columns say which engine wrote the table being read or changed. A ratio above 1
means EW is faster.

| Operation | EW | delta-dotnet | DD/EW ratio (EW-written / DD-written) |
|---|---|---|---|
| Full scan, 1M rows | 13.6 / 24.1 ms | DataFusion 17.3 / 60.9 ms; kernel 93 / 111 ms | 1.27 / 2.53 (DataFusion); 6.9 / 4.6 (kernel) |
| Filtered scan, 1 file of 20 (`Selective`) | 4.1 / 6.3 ms | 8.7 / 12.0 ms | 2.1 / 1.9 |
| Filtered scan, 1 row in 16 (`OneCategory`) | 87 / 94 ms (prune only: 14 / 18 ms) | 19.1 / 22.7 ms | **0.22 / 0.24** |
| Open latest, 100 commits, no checkpoint | 11.1 / 10.7 ms | 35.9 / 50.3 ms | 3.2 / 4.7 |
| Open latest, 100 commits, checkpointed | 8.3 / 6.5 ms | 4.2 / 5.5 ms | **0.51 / 0.86** |
| Open at version 50 | 12–17 ms | 29–33 ms | 1.7–2.8 |
| History, no checkpoint / checkpointed | 18–20 / 14–16 ms | 36–39 / 13–16 ms | ~2 / ~1 |
| Change data feed | 32 / 40 ms | 156 / 166 ms | 4.9 / 4.1 |
| Create + write 1M rows | 246 ms (min 151, noisy) | 199 ms (default) / 174 ms (64K batches) | **0.85 / 0.75** |
| Open + append 10K rows, no checkpoint / checkpointed | 28–34 / 18–23 ms | 56–63 / 25–27 ms | 1.9–2.0 / 1.2–1.4 |
| DELETE 1 row in 16, copy-on-write (`DeleteAsync`, since #562) | 396 / 310 ms (host-composed before #562: 420 / 341) | 268 / 305 ms | **0.68** / 0.99 |
| DELETE 1 row in 16, EW deletion vector (EW-written DV table only) | 174 ms | none (delta-rs writes no DVs) | — |
| UPDATE 1 row in 16 | 432 / 285 ms (noisy: min 299 / 249) | 211 / 248 ms | **0.51 / 0.90** |
| Checkpoint, 100 commits | 31 / 26 ms | 90 / 96 ms | 2.9 / 3.7 |
| Compact 100 small files | 245 / 301 ms | 468 / 466 ms | 1.9 / 1.7 |
| Vacuum 100 removed files | 54 / 48 ms | Full 128 / 114 ms; Lite 116 / 103 ms | 2.4 / 2.4 (Full) |

The filtered-scan, DML and maintenance rows come from a second run, after review fixes:
- EW's exact filter now gathers only the projected columns.
- Evaluator masks are disposed.
- The vacuum benchmark previously compared EW against a delta-rs no-op. delta-dotnet maps
  `VacuumOptions.RetentionHours = 0` to "no override", so delta-rs kept its 7-day default and deleted nothing.
  Both engines now vacuum a table whose own `delta.deletedFileRetentionDuration` is 0, and `--verify` checks
  that no unreferenced data file remains.

Where delta-dotnet wins, the cause was found for one case and is suspected for the rest:

- **Row-level predicate evaluation is EW's bottleneck.** Measured separately on 1M rows: reading
  `id, amount, category` takes about 25 ms, while `ArrowRowEvaluator.EvaluatePredicate` for `category = 'cat_03'`
  takes 90–105 ms, about 100 ns/row (#560). A naive `GetString` loop takes about 40 ms. The same evaluator drives EW's
  own DELETE/UPDATE masks, which probably explains part of their gap too.
- **Opening a checkpointed table:** delta-kernel-rs is up to 2× faster at reading a small checkpoint.
- **Write:** close on minimum times, but EW allocates about 286 MB of managed memory for 1M rows, and its mean
  carries GC noise.
