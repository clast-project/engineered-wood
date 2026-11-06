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
#    the constraint is in the metadata, and the restore re-added the 100 small files; for EW's deletion-vector
#    DML, that it wrote a vector and left the rows its copy-on-write run leaves). Builds the fixtures. Exits non-zero on any mismatch, or when the filter matches no class.
dotnet run -c Release --project test/EngineeredWood.DeltaLake.DeltaDotnetBenchmarks -- --verify [ClassFilter]

# 2. Benchmark (all, or e.g. --filter *FullScan*).
dotnet run -c Release --project test/EngineeredWood.DeltaLake.DeltaDotnetBenchmarks -- --filter "DeltaDotnetComparison.*"
```

Run from the worktree whose code you mean to measure: BenchmarkDotNet locates the project from the cwd.

Fixture tables are cached under `%TEMP%\ew-vs-delta-dotnet\templates` (override with `EWDD_BENCH_ROOT`).
Delete a `<name>.ready` marker, or the folder, to rebuild them.

### Against Azure Blob Storage

Every benchmark can run with its tables in an Azure storage account instead of on local disk: EW through
`EngineeredWood.Azure`'s `AzureTableFileSystem`, delta-dotnet through delta-rs / delta-kernel-rs's object_store
(`az://container/prefix`, with the account name and key as storage options). Both use the Blob endpoint, so the
account does not need a hierarchical namespace, though it may have one: the full `--verify` passes on a
hierarchical-namespace account and on Azurite (flat). The banner says which kind it found. The store is chosen by environment variables, because
BenchmarkDotNet runs each benchmark in a child process that inherits the environment but not the command line:

| Variable | Meaning |
|---|---|
| `EWDD_STORAGE` | `local` (default) or `azure` |
| `EWDD_AZURE_ACCOUNT` | storage account name (required for `azure`) |
| `EWDD_AZURE_KEY` | an account key (required for `azure`); shared-key auth for both engines |
| `EWDD_AZURE_CONTAINER` | container, created if absent (default `ew-vs-delta-dotnet`) |
| `EWDD_AZURE_PREFIX` | blob-name prefix all fixtures and scratch copies go under (default `bench`) |
| `EWDD_AZURE_CONCURRENCY` | EW's `AzureTableFileSystem` `maxConcurrency` (default 16, its own default) |
| `EWDD_AZURE_EMULATOR` | `1`: Azurite's well-known account on `127.0.0.1:10000`, for a smoke test without an account |

The helper script sets them for the current PowerShell session, prompting for the key without echoing it:

```powershell
. .\test\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks\azure-env.ps1 -Account mystorageacct
dotnet run -c Release --project test/EngineeredWood.DeltaLake.DeltaDotnetBenchmarks -- --verify
dotnet run -c Release --project test/EngineeredWood.DeltaLake.DeltaDotnetBenchmarks -- --filter "*FullScan*"
. .\test\EngineeredWood.DeltaLake.DeltaDotnetBenchmarks\azure-env.ps1 -Clear   # back to local disk
```

Every run prints a `// storage: …` line first, so a result can never be mistaken for the other store's.

- **Fixtures live in the container** (`<prefix>/templates/<name>`, marked by a `<name>.ready` blob), built once by
  the first run through both engines' own Azure writers. Delete the `.ready` blob, or the prefix, to rebuild.
- **Each mutation iteration gets a server-side copy** of its template (Copy Blob within the account, so no
  bytes come back through this machine), under `<prefix>/scratch/<pid>/`, deleted after the iteration.
  The copy is not timed. Unlike a local copy, its blobs' last-modified times are new; the only fixture whose
  vacuum keys off file age uses a zero retention, so that changes nothing.
- **Run from a VM in the account's region** if you want numbers about the engines rather than about your
  link: from a laptop, round trips dominate, and the comparison becomes how many requests each engine makes
  and how many it keeps in flight.
- **It costs money**: every mutation iteration copies and deletes a table, and every scan reads one. A full
  run is tens of thousands of transactions, plus egress when run from outside the region.

## What is measured

Every category has EW as the baseline, so **Ratio = delta-dotnet time / EW time** (above 1 means EW is faster).
BenchmarkDotNet's own Ratio column is the mean of the per-iteration ratios, which on the noisy single-shot
mutations can differ from the ratio of the means by 10% or more. The results table below reports the ratio of
the means it shows, so each ratio can be checked against its row.
Fixtures are written by *each* engine with its own default writer settings (`Writer` param), and every reader
runs against both.

| Class | Category | Operation |
|---|---|---|
| `FullScanBenchmarks` | FullScan | open + scan all of a 1M-row, one-commit table. delta-dotnet both ways: kernel `ReadAsArrowTableAsync` and DataFusion `SELECT *`, the latter at DataFusion's default batch size and at 64K |
| `FilteredScanBenchmarks` | FilteredScan | `SELECT id, amount WHERE …` on a 20-file table: a 1-file id range (`Selective`), and 1-in-16 rows spread everywhere (`OneCategory`). delta-dotnet at both batch sizes |
| `LargeScanBenchmarks` | LargeFullScan, LargeOneCategory | the full scan and the `OneCategory` scan at 10M rows (20 files of 500K), where delta-dotnet's fixed costs no longer dominate. delta-dotnet at both batch sizes |
| `ScanWindowBenchmarks` | ScanWindowFull, ScanWindowOneCategory | EW only: the 20-file table read whole, and its `OneCategory` filtered scan, at `MaxConcurrentReads` 1, 2, 4 and 8 (#607). 1 is the serial read. `FullScan` cannot show this, because its table is one file |
| `SnapshotBenchmarks` | OpenLatest, OpenAtVersion, History | log replay of a 100-commit table, with and without a checkpoint |
| `ChangeFeedBenchmarks` | ChangeFeed | the whole CDF of a table with 10 appends + UPDATE + DELETE |
| `WriteBenchmarks` | CreateAndWrite | create + write 1M rows in one commit |
| `AppendBenchmarks` | Append10K | open a 100-commit table + append 10K rows |
| `DmlBenchmarks` | Delete, Update | copy-on-write DELETE / UPDATE of 1 row in 16 on the one-commit 1M-row table |
| `MergeBenchmarks` | Merge | upsert 10K rows on `id` into the one-commit 1M-row table: 5K matched (their `amount` doubled), 5K inserted |
| `MergeKeyRangeBenchmarks` | MergeKeyRange | update-only MERGE of 10K rows into the 20-file table, all matching one file, joined on `id` or on the `ts` timestamp (`Key`): what key-range pruning (#578) and delta-rs's early filter save. `--verify` prints each engine's file metrics |
| `MergeExpressionBenchmarks` | MergeExpressions | `MergeBenchmarks`' source and table, with `WHEN MATCHED AND source.quantity < 100 THEN DELETE`, `WHEN MATCHED THEN UPDATE SET amount = target.amount + source.amount, quantity = source.quantity` and `WHEN NOT MATCHED THEN INSERT *`: several clauses of a kind (#575) and SET expressions (#576) |
| `ReplaceWhereBenchmarks` | ReplaceWhere | replaceWhere on a non-partition column, the replacements with `amount` doubled: a 10K-row id range on the 20-file table (`Selective`), and one category in 16 on the one-commit 1M-row table (`OneCategory`). delta-dotnet: `InsertAsync` with `SaveMode.Overwrite` and a `Predicate` |
| `ConstraintBenchmarks` | AddConstraint | `ADD CONSTRAINT … CHECK (amount >= 0)` on the one-commit 1M-row table; every row satisfies it, so both engines read the whole column |
| `RestoreBenchmarks` | Restore | RESTORE the compacted 100-commit table to the version before its compaction: 100 small files re-added, the compacted ones removed |
| `DeletionVectorDmlBenchmarks` | DeleteDeletionVector, UpdateDeletionVector, MergeDeletionVector | EW's own deletion-vector DELETE, UPDATE (#704) and MERGE (#574) of the same rows as `DmlBenchmarks` and `MergeBenchmarks`, on an EW-written DV table; no writer parameter and no delta-dotnet counterpart |
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
  `DeleteRowsAsync(…, RowDeleteMode.CopyOnWrite)`) as a reference row. EW's own deletion-vector DELETE, UPDATE
  and MERGE are measured separately (`DeletionVectorDmlBenchmarks`), on an EW-written DV table.
- **EW has no "open at version".** It opens the latest snapshot and then builds the older one, so its
  OpenAtVersion includes a latest-snapshot load.
- **delta-dotnet's `InsertOptions.MaxRowsPerGroup` defaults to 100**, which delta-rs uses as its
  `write_batch_size`. The write benchmark measures it at that default and at 64K.
- Construction of the delta-dotnet `DeltaEngine` (its tokio runtime) is a one-time host cost and is not
  measured.
- **At 1M rows, delta-dotnet's fixed costs are a large share of its time.** It builds a DataFusion session for
  every query and collects the whole result before returning it. A query that matches nothing costs it 2.6 ms on a
  one-file table and 7.5–8.8 ms on a 20-file one, against EW's 0.24–2.5 ms. The 10M-row scans
  (`LargeScanBenchmarks`) show the gap with less of that in it, and it is narrower there.

## Is delta-dotnet configured fairly?

An audit on 2026-11-04 asked whether any delta-dotnet setting would make it look better. delta-dotnet 0.33.1
exposes few: the DataFusion execution batch size and spill limits (`EngineOptions`), `WithoutFiles` and
`LogBufferSize` (`TableOptions`), `MaxRowsPerGroup` (`InsertOptions`), and the optimize options. It exposes no
Parquet writer properties (so no compression choice), and no MERGE, DELETE or UPDATE options beyond the SQL text.
Each was tried against the suite's defaults:

| Setting | Effect |
|---|---|
| `DataFusionExecutionBatchSize` 64K (default 8,192) | **The one that matters, at scale.** 10M-row full scan 3.2–3.8× to 1.3–1.4×; 10M-row one-category scan 1.5–2.0× to 1.2–1.4×. No change on the 1M-row scans. A batch of 1M rows was a little faster still at 10M rows (1.1–1.2×) but slower on the 20-file 1M-row table, so 64K is the one measured. Now measured in every scan benchmark (`Dd.TunedEngine`) |
| `WithoutFiles` on open, open-at-version and history | delta-dotnet's no-checkpoint open falls from 37 to 25 ms. EW stays 1.9–8× faster. Not adopted: EW's open loads the file list, so this would compare unlike work |
| `LogBufferSize` 8 or 128 | no effect |
| `MaxRowsPerGroup` 1M on write (suite: 100 and 64K) | no effect (176 vs 178 ms) |
| Fixtures written at `MaxRowsPerGroup` 64K instead of the default 100 | no effect: the same file sizes (35.4 MB for one file, 42.3 MB for 20), and the same read times for both engines |

What no setting reaches, and so remains in the numbers: the per-query session and collect, MERGE's join, and
delta-rs's log replay. What the suite does not measure at all: SQL beyond filtered projections (aggregates,
joins, sorts, DataFusion's strengths, which EW has no counterpart for), object stores (only the local file system),
and tables much larger than 10M rows.

## A delta-dotnet correctness bug the verify pass found

`ITable.ReadAsArrowTableAsync` / `ReadAsDataFrameAsync` **ignore deletion vectors**: on a table with a DV they
return the deleted rows. `ArrowFFIInterOpHandler.ReadParquetAsArrow` receives the kernel's `selectionVector`
and never applies it. delta-rs's DataFusion path (`QueryAsync`) honours the same DV, as EW does, so the DV
itself is fine. `--verify` reports this as a `WARN` on each `DeletionVectorDmlBenchmarks` method. Filed upstream as
[delta-dotnet#234](https://github.com/delta-incubator/delta-dotnet/issues/234).

## Results, 2026-11-06: MERGE variants and deletion-vector DML

Same machine, at fa664592 (after #709). A run of the new classes beside the existing `MergeBenchmarks` and
`DmlBenchmarks`, so the deletion-vector rows have copy-on-write baselines from the same session. The key-range
class was run twice, because one cell of the first run was about 3× its siblings and the second run moved the
slow cell elsewhere: its rows give the range over both runs. Means, with the min in brackets where the
single-shot noise makes the mean misleading.

| Operation | EW | delta-dotnet | DD/EW ratio (EW-written / DD-written) |
|---|---|---|---|
| MERGE, upsert 10K rows (5K matched), rerun | 187 / 187 ms | 2,400 / 2,328 ms | 12.8 / 12.5 |
| **MERGE, expressions + several clauses** (5K matched, ~1 in 10 deleted) | 190 / 189 ms | 2,273 / 2,703 ms (min 2,243) | **11.9 / 14.3** |
| **MERGE, 10K rows into 1 file of 20, on `id`** | 41–80 / 37–55 ms (min 34–61) | 225–270 / 176–251 ms (min 132–222) | **3.4–5.5 / 4.5–4.8** |
| **MERGE, 10K rows into 1 file of 20, on `ts`** | 66–108 / 41–68 ms (min 28–84) | 199–262 / 178–206 ms (min 133–195) | **2.4–3.0 / 2.6–5.0** |
| DELETE 1 row in 16, copy-on-write, rerun | 186 / 177 ms | 304 / 313 ms | 1.64 / 1.76 |
| DELETE 1 row in 16, EW deletion vector | 23.5 ms | none | — (7.9× faster than EW's copy-on-write) |
| UPDATE 1 row in 16, copy-on-write, rerun | 175 / 164 ms | 237 / 250 ms | 1.35 / 1.52 |
| **UPDATE 1 row in 16, EW deletion vector** (#704) | 60.4 ms | none | — (2.9× faster than EW's copy-on-write) |
| **MERGE, upsert 10K rows, EW deletion vector** (#574) | 83.6 ms | none | — (2.2× faster than EW's copy-on-write) |

- **Several clauses and SET expressions cost EW nothing measurable:** 190 ms against the plain upsert's 187 ms.
  delta-dotnet's time is about the same as its upsert too, so MERGE stays the widest gap, at 12–14×.
- **Both engines prune the key-range MERGE**, and the gap narrows to 2.4–5.5× from the one-file upsert's 12×.
  delta-rs reports `num_target_files_skipped_during_scan=19` of 20 (printed by `--verify`). EW records no
  skipped-file count in its commit, but its 28–84 ms (min) against about 180 ms for a one-file 1M-row MERGE
  is the read of one file of 20; a prune-only scan with the same `id` or `ts` range reads 20K rows (one row
  group) on both writers' tables. The timestamp key prunes as well as the `long` id (#578): the spread between
  the two keys is no larger than the run-to-run spread of either.
- **Deletion vectors pay off most for DELETE**, which rewrites nothing (7.9×). UPDATE and MERGE still write the
  changed rows to a new file beside the vector, so they gain less (2.9× and 2.2×), and allocate 50 and 77 MB of
  managed memory where copy-on-write allocates 230–280 MB.
- The rerun rows are within about 6% of 2026-11-04's, so the machine had not drifted.

## Results, 2026-11-04

i9-12900K (16 cores / 24 threads), Windows 11, .NET 10.0.11, DeltaLake.Net 0.33.1, at 9b3acc95 (after #669). One
full run of every class. Mean times; the ratio is delta-dotnet's mean over EW's (over EW's median where the EW
column says median), computed from the BenchmarkDotNet CSVs, not taken from its Ratio column. The
**EW-written / DD-written** columns say which engine wrote the table being read or changed. A ratio above 1
means EW is faster.

How much of EW's change since 2026-11-01 is the code depends on the operation, as delta-dotnet's own times show:

- **Scans, writes, DML and maintenance:** in the full run delta-dotnet is within about 7% of 11-01, so the machine
  did not drift for these, and EW's changes are its code's. The one exception is the append, 5–12% slower for
  delta-dotnet (56–66 to 63–68 ms), which only narrows EW's lead.
- **Log replay** (open, open at version, history, the change feed): delta-dotnet is 6–30% faster than on 11-01
  (no-checkpoint open 38.9 / 63.1 to 36.6 / 50.9 ms, checkpointed open 5.4 / 7.6 to 4.3 / 5.6 ms, checkpointed
  history 19 to 13–14 ms). The machine state that slowed small-file opens on 11-01 has passed, for both engines.
  For these rows only an A/B on today's machine separates EW's code from the machine, and only the open has one
  (see the log-replay note below).

| Operation | EW | delta-dotnet | DD/EW ratio (EW-written / DD-written) |
|---|---|---|---|
| Full scan, 1M rows ‡ | 17.8 / 23.4 ms | DataFusion 40.7 / 60.3 ms (64K batches: 42.7 / 58.0); kernel 87 / 108 ms | 2.3 / 2.6 (DataFusion; 64K: 2.4 / 2.5); 4.9 / 4.6 (kernel) |
| Filtered scan, 1 file of 20 (`Selective`) ‡ | 1.6 / 2.1 ms | 8.6 / 10.4 ms (64K: 8.7 / 10.5) | 5.4 / 5.0 (64K: the same) |
| Filtered scan, 1 row in 16 (`OneCategory`) ‡ | 6.3 / 7.4 ms (prune only: 3.6 / 6.0 ms) | 15.6 / 18.7 ms (64K: 14.9 / 17.7) | **2.5 / 2.5** (64K: 2.4 / 2.4) |
| **Full scan, 10M rows in 20 files** ‡ | 77 / 104 ms | 290 / 334 ms; **64K batches: 103 / 144 ms** | 3.8 / 3.2; **64K: 1.34 / 1.39** |
| **Filtered scan, 1 row in 16, 10M rows** ‡ | 62 / 60 ms | 90 / 118 ms; **64K batches: 72 / 86 ms** | 1.46 / 1.95; **64K: 1.18 / 1.42** |
| Open latest, 100 commits, no checkpoint | 4.0 / 5.1 ms | 36.6 / 50.9 ms | **9.3 / 10.0** |
| Open latest, 100 commits, checkpointed | 1.0 / 0.75 ms | 4.3 / 5.6 ms | 4.1 / 7.5 |
| Open at version 50, no checkpoint / checkpointed | 6.0–7.5 / 3.2–3.5 ms | 28–33 / 28–33 ms | 4.4–4.8 / 8.8–9.6 |
| History, no checkpoint / checkpointed | 7.7–9.8 / 5.0–5.8 ms | 35–39 / 13–14 ms | 4.0–4.6 / 2.3–2.8 |
| Change data feed | 26 / 40 ms | 142 / 167 ms | 5.4 / 4.2 |
| Create + write 1M rows | 136 ms (min 125) | 201 ms (default) / 176 ms (64K batches) | 1.48 / **1.30** |
| Open + append 10K rows, no checkpoint / checkpointed | 24 / 11 ms (median) | 63–68 / 25–29 ms | 2.6–2.8 / 2.3–2.7 |
| DELETE 1 row in 16, copy-on-write | 188 / 187 ms (host-composed: 215 / 223) | 298 / 306 ms | **1.59 / 1.63** |
| DELETE 1 row in 16, EW deletion vector (EW-written DV table only) | 25 ms | none (delta-rs writes no DVs) | — |
| UPDATE 1 row in 16 | 165 / 157 ms | 234 / 248 ms | 1.42 / 1.58 |
| MERGE, upsert 10K rows (5K matched) | 183 / 200 ms | 2,366 / 2,425 ms | 13.0 / 12.2 |
| replaceWhere, 10K-row id range (`Selective`) | 24.1 / 25.4 ms | 48.4 / 51.5 ms | 2.0 / 2.0 |
| replaceWhere, 1 category in 16 (`OneCategory`) | 214 / 207 ms | 244 / 252 ms | **1.14 / 1.22** |
| ADD CONSTRAINT `amount >= 0` over 1M rows | 15.6 / 16.0 ms | 49.0 / 64.1 ms | 3.1 / 4.0 |
| RESTORE to before a compaction | 32 / 35 ms | 114 / 101 ms | 3.6 / 2.9 |
| Checkpoint, 100 commits | 20 / 23 ms | 94 / 92 ms | 4.7 / 4.0 |
| Compact 100 small files | 190 / 261 ms | 468 / 461 ms | 2.5 / 1.8 |
| Vacuum 100 removed files | 34 / 36 ms (median) | Full 130 / 115 ms; Lite 119 / 104 ms | 3.9 / 3.2 (Full, on EW's median) |

‡ From a second run of the three scan classes, after the 64K-batch variants and `LargeScanBenchmarks` were added.
Its default-batch numbers are within about 13% of the full run's.

Two EW means carry a single outlier, so the table gives their medians: the append to the checkpointed DD-written
table (mean 64 ms, median 11 ms) and the vacuum of the EW-written table (mean 74 ms, median 34 ms).

Compared with the 2026-11-01 run, **EW is now faster on every row**, including all the rows delta-dotnet won or
drew. The margin is smallest where delta-dotnet is at its best: on the 10M-row scans with 64K batches, EW leads by
1.2–1.4×, not the 2–4× of the 1M-row scans (see [Is delta-dotnet configured fairly?](#is-delta-dotnet-configured-fairly)).

What each change below rests on is said with it: an A/B on today's machine, an EW-only measurement of the feature,
or only the code that changed in between.

- **Non-selective filtered scans: 1.2–1.3× to 2.5–2.8×.** EW now reads a table's data files in parallel (#607,
  #611) and splits a scan by (file, row group) (#612, #631). The 11-01 run named reading one file after another as
  what remained of delta-dotnet's lead. `ScanWindowBenchmarks` shows the window's effect alone: the 20-file
  `OneCategory` scan takes 13.8 ms at `MaxConcurrentReads = 1` and 6.5 ms at 2 or 4. That isolates the read window
  (#607/#611) on today's code; the commits were not bisected.
- **Write: level to 1.3–1.5×,** and EW allocates 206 MB of managed memory for 1M rows where it allocated 322 MB.
  The changes on this path are string statistics that compare UTF-8 bytes instead of decoding every value (#665)
  and joining a row group's batches into exactly-sized buffers (#668); neither was A/B'd here.
- **Full-file rewrites: about level to 1.1–1.6×.** Copy-on-write DELETE 1.0–1.2× to 1.6×, UPDATE 1.1–1.3× to
  1.4–1.6×, the one-category replaceWhere 0.98× to 1.14–1.22×. Each writes a 1M-row file, so the write-path changes
  above are the likely cause, not measured separately. The one-category replaceWhere is still the closest row.
- **Log replay without a checkpoint: 3.6–5.0× to 9–10×. A/B'd: this is #609's listing fix.** Run today, #609's
  own commit (60182540) opens the 100-commit table in 4.0 ms, as main does, and the code before it (e3092d09) takes
  11.5 ms. The 11-01 re-measure of the same #609 code got 10.8 ms, in the drifted machine state that run notes
  (unchanged code went from 11 to 18 ms then). That state slowed delta-dotnet's opens too, but by far less: its
  no-checkpoint open is 6–19% faster today, where EW's same code is 63% faster. So the 11-01 ratio understated EW,
  and today's is the one to use. Open at version and history have no A/B; they replay the same log through the
  same listing, which is the likely, unmeasured, cause of their gains.

Unchanged: the full scan and the `Selective` filtered scan each read one file, so parallel file reads have
nothing to work on there, and ADD CONSTRAINT is within noise of 11-01. The change feed's ratio rose a little (4.9 /
3.9 to 5.4 / 4.2), but delta-dotnet's own time fell 8–11% with it, so that is the machine as much as the code.

Notes that still hold:

- **Single-shot mutations are noisy for EW.** Each iteration is one invocation, and EW's means carry GC pauses
  (the managed heap) that delta-rs does not have. The min or median is given where it differs much from the mean.
- **MERGE** is the widest gap. The target is one file, so neither engine prunes anything, and the gap is in the
  match itself (see the 2026-11-06 key-range rows for a MERGE both engines prune). EW hashes the 10K source keys and probes them as it reads the target. delta-rs plans a DataFusion
  join, which is suspected, not profiled, as the cost.
- **Commit metadata (#564/#577) and mergeSchema (#568) have no rows.** They add next to nothing to a commit or an
  append, and appends are already measured.
