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

## Results, 2026-10-31

i9-12900K (16 cores / 24 threads), Windows 11, .NET 10.0.11, DeltaLake.Net 0.33.1. Mean times. The
**EW-written / DD-written** columns say which engine wrote the table being read or changed. A ratio above 1
means EW is faster.

| Operation | EW | delta-dotnet | DD/EW ratio (EW-written / DD-written) |
|---|---|---|---|
| Full scan, 1M rows | 17.9 / 22.9 ms | DataFusion 43.6 / 62.5 ms; kernel 94 / 115 ms | 2.44 / 2.73 (DataFusion); 5.3 / 5.0 (kernel) |
| Filtered scan, 1 file of 20 (`Selective`) | 4.1 / 6.3 ms | 8.7 / 12.0 ms | 2.1 / 1.9 |
| Filtered scan, 1 row in 16 (`OneCategory`) | 19.9 / 22.8 ms since #560 (before: 87 / 94 ms; prune only: 10 / 15 ms) | 17.9 / 26.9 ms | 0.90 / 1.18 |
| Open latest, 100 commits, no checkpoint | 11.1 / 10.7 ms | 35.9 / 50.3 ms | 3.2 / 4.7 |
| Open latest, 100 commits, checkpointed | 8.3 / 6.5 ms | 4.2 / 5.5 ms | **0.51 / 0.86** |
| Open at version 50 | 12–17 ms | 29–33 ms | 1.7–2.8 |
| History, no checkpoint / checkpointed | 18–20 / 14–16 ms | 36–39 / 13–16 ms | ~2 / ~1 |
| Change data feed | 32 / 40 ms | 156 / 166 ms | 4.9 / 4.1 |
| Create + write 1M rows | 182 ms (min 132, noisy) | 202 ms (default) / 177 ms (64K batches) | 1.11 / **0.97** |
| Open + append 10K rows, no checkpoint / checkpointed | 28–34 / 18–23 ms | 56–63 / 25–27 ms | 1.9–2.0 / 1.2–1.4 |
| DELETE 1 row in 16, copy-on-write (`DeleteAsync`, since #562) | 236 / 228 ms since #560 (before: 335 / 304; host-composed before #562: 357 / 346) | 297 / 308 ms | 1.26 / 1.35 |
| DELETE 1 row in 16, EW deletion vector (EW-written DV table only) | 129 ms | none (delta-rs writes no DVs) | — |
| UPDATE 1 row in 16 | 196 / 250 ms since #560 (before: 289 / 275, noisy) | 239 / 251 ms | 1.22 / 1.00 |
| Checkpoint, 100 commits | 31 / 26 ms | 90 / 96 ms | 2.9 / 3.7 |
| Compact 100 small files | 245 / 301 ms | 468 / 466 ms | 1.9 / 1.7 |
| Vacuum 100 removed files | 54 / 48 ms | Full 128 / 114 ms; Lite 116 / 103 ms | 2.4 / 2.4 (Full) |

The full-scan, write and DML rows come from a third run, after #559 made EW write the one-commit table as one
file instead of 16. The filtered-scan and maintenance rows are from the second run, after review fixes:
- EW's exact filter now gathers only the projected columns.
- Evaluator masks are disposed.
- The vacuum benchmark previously compared EW against a delta-rs no-op. delta-dotnet maps
  `VacuumOptions.RetentionHours = 0` to "no override", so delta-rs kept its 7-day default and deleted nothing.
  Both engines now vacuum a table whose own `delta.deletedFileRetentionDuration` is 0, and `--verify` checks
  that no unreferenced data file remains.

Where delta-dotnet wins, the cause was found for one case and is suspected for the rest:

- **Row-level predicate evaluation was EW's bottleneck, fixed by #560.** `ArrowRowEvaluator.EvaluatePredicate` for
  `category = 'cat_03'` over 1M rows took 87 ms, about 100 ns/row, against about 25 ms to read the columns. A
  column-against-constant comparison is now answered straight off the Arrow buffers: 2.4 ms. The same evaluator drives
  EW's DELETE/UPDATE masks, and the filtered-scan, DELETE and UPDATE rows above were re-measured after it.
- **Opening a checkpointed table:** delta-kernel-rs is up to 2× faster at reading a small checkpoint.
- **Write:** EW is now ahead of delta-dotnet's default and level with its 64K-batch setting, but allocates about 322 MB of managed memory for 1M rows
  (286 MB before #559: joining the batches into one row group costs a copy), and its mean carries GC noise.
