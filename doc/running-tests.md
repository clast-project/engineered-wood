# Running Tests

## Prerequisites

### .NET SDK

The solution requires **.NET 10 SDK** (or later). The test projects multi-target:

- `net10.0` — primary target
- `net8.0` — LTS target
- `net472` — .NET Framework (Windows only)

### Git Submodules

The Parquet test suite reads files from the `parquet-testing` submodule:

```
git submodule update --init
```

### Python (optional — for cross-validation tests)

Some ORC and Avro tests validate interoperability by invoking Python
libraries as subprocesses. These tests are **skipped** automatically if the
required Python packages are not installed — they will appear as "Skipped"
in the test output.

To enable them, install Python 3.8+ and the following packages:

```
pip install pyarrow fastavro tzdata
```

The `tzdata` package is required on Windows — PyArrow's Arrow C++ ORC
reader needs IANA timezone data that isn't available natively on Windows.
The test harness automatically detects the `tzdata` package and sets
the `TZDIR` environment variable for the Python subprocess. If you still
see timezone-related errors, you can set `TZDIR` manually:

```
# PowerShell (session)
$env:TZDIR = "$(python -c "import os, tzdata; print(os.path.join(os.path.dirname(tzdata.__file__), 'zoneinfo'))")"

# Or set permanently via System Properties → Environment Variables
# Value: C:\Users\<you>\AppData\Local\Programs\Python\Python3XX\Lib\site-packages\tzdata\zoneinfo
```

**Python discovery:** The tests try `python3` and `python` on PATH,
then fall back to scanning `%LOCALAPPDATA%\Programs\Python\Python*\`.
On Windows, you may need to disable the Microsoft Store "python.exe"
app execution alias (Settings → Apps → Advanced app settings → App
execution aliases → turn off "python.exe" and "python3.exe").

| Test suite | Python package | Tests enabled | What they validate |
|---|---|---|---|
| ORC | `pyarrow` | 10 cross-validation tests | EngineeredWood writes → PyArrow reads |
| Avro | `fastavro` | 7 cross-validation tests | EngineeredWood writes → fastavro reads |
| Parquet | *(none — uses ParquetSharp NuGet)* | all tests always run | Bidirectional with ParquetSharp |

### Delta Lake interop tiers (optional)

Delta support is validated against two independent implementations, in
`test/EngineeredWood.DeltaLake.Table.Tests/Interop/`. Both directions are
covered — EngineeredWood writes and the tool reads, and vice versa.

Round-tripping through EngineeredWood's own reader only proves the reader
and writer agree on a dialect, not that the dialect is Delta. Every interop
bug found so far round-tripped perfectly, so these tiers are the only thing
standing between a spec divergence and shipping it.

| Tier | Install | Tests | Reaches |
|---|---|---|---|
| 1 — delta-rs | `pip install "deltalake[pyarrow]"` | 28 | Log/checkpoint replay, path encoding, per-file stats, filtered reads, and (through pyarrow) the on-disk layout of a shredded variant column. Seconds to run. |
| 2 — DuckDB | `pip install duckdb` (1.4+) | 2 | VARIANT parquet reads through an implementation that is not delta-kernel-rs. Sub-second. |
| 3 — PySpark | `pip install pyspark delta-spark` + JDK 17+ | 68 | Writer features, DESCRIBE DETAIL, clustering, OPTIMIZE, column mapping, data skipping, VACUUM survival, variant reads, V2 checkpoints, and Delta's OWN conflict checker driven through py4j (`ConflictSemanticsInteropTests`). ~90s to run. |

**The `[pyarrow]` extra is not optional.** `deltalake` 1.6 made pyarrow an extra,
and the tier-1 driver reads through pyarrow — install the bare package and the
tier does not skip, it FAILS, ~27 tests at once with
`ImportError: Pyarrow is required, install deltalake[pyarrow]`. The availability
probe only imports `deltalake`, which succeeds.

This is not hypothetical: CI hit exactly it on the first run after the tiers
were enabled there (PR #103), because `pip install deltalake` alone had been
written into the workflow. `.github/workflows/ci.yml` now installs `pyarrow`
explicitly, alongside `tzdata` — Windows CPython ships no time-zone database,
so `zoneinfo.ZoneInfo("UTC")`, which the tier-1 driver's `read_epoch_micros`
needs, raises `ZoneInfoNotFoundError` without it. Both are already in the
Python prerequisites above; both were invisible on developer machines that
had them installed for unrelated reasons.

Tier 2 (DuckDB) was evaluated and **dropped as a Delta tier**: delta-rs embeds
delta-kernel-rs, which is what DuckDB's delta extension is, so tier 1
already subsumes it. What would have been genuinely DuckDB-only is a third
parquet reader — already covered at the right layer by
`test/EngineeredWood.Parquet.Compatibility/` — and predicate pushdown through a
foreign planner, which the stats and pruning tests reach from the tiers that
already exist.

It came back for one thing that reasoning does not cover: **VARIANT**. DuckDB's
variant type and parquet reader are its own code, not delta-kernel-rs, so for a
shredded variant column it is a genuinely independent implementation — and it
costs no JVM. `VariantShreddingInteropTests` uses it that way, on raw parquet
files with no Delta log involved. Needs DuckDB **1.4+** (VARIANT does not exist
before that; an older one reads a shredded column as a bare struct, which would
read as a conformance failure rather than a missing feature), so it takes its
own interpreter override:

```
$env:EW_DUCKDB_PYTHON = "…\ew-duckdb\Scripts\python.exe"   # unset -> python on PATH
$env:EW_REQUIRE_DUCKDB_INTEROP = "1"
```

**Version pairing matters.** `delta-spark`'s major version must match
`pyspark`'s — 4.x with Spark 4.0, 3.x with Spark 3.5. The assertions were
established against `deltalake` 1.6.2 and `pyspark` 4.0.1 /
`delta-spark` 4.0.0; those are recorded in `DeltaRs.ValidatedAgainstVersion`
and `Spark.ValidatedAgainstVersion`. If these tests fail after an upgrade,
check the tool version before assuming an EngineeredWood regression.

**Two Spark versions for the VARIANT tests.** Variant is GA in Spark 4.1 and
experimental in 4.0.x, and they disagree on the parquet layout: 4.1 writes and
reads the VARIANT logical-type annotation; 4.0.x writes it unannotated and its
reader throws an NPE on an annotated group (which is why the writer has
`DeltaTableOptions.EmitVariantLogicalType`). `VariantInteropTests` is
version-aware — it writes whichever layout the running Spark reads — so the
tier stays green under either. It also covers **nested** variant (a variant
inside a struct); those cases require GA variant and self-skip on 4.0.x. To run
the GA + nested cases against a Spark 4.1 build without disturbing the 4.0.x
install the rest of tier 3 is pinned to, put 4.1 in its own venv (`pyspark`
bundles its JARs, so two versions cannot share one environment) and point the
tier at it:

```
python -m venv spark41 && spark41\Scripts\pip install pyspark==4.1.3 delta-spark==4.1.0
$env:EW_SPARK_PYTHON = "…\spark41\Scripts\python.exe"   # tier 3 uses this interpreter; unset -> PATH
```

The GA annotated path and the nested-variant cases have been validated against
`pyspark` 4.1.3 / `delta-spark` 4.1.0; the unannotated path against 4.0.1 /
4.0.0. delta-rs (tier 1) reads both layouts regardless, so the compat-mode
writer is covered in every run even without a second Spark.

**Tier 3 on Windows** additionally needs Hadoop's `winutils.exe` +
`hadoop.dll` (Apache does not publish them; community builds exist for each
Hadoop line — match the version bundled in `pyspark/jars/hadoop-client-api-*.jar`):

```
# PowerShell (session)
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.x.x-hotspot"
$env:HADOOP_HOME = "C:\Users\<you>\.hadoop\hadoop-3.4.0"
```

Setting `HADOOP_HOME` alone is **not** enough — `hadoop.dll` must also be
findable on `PATH`, or Hadoop throws `UnsatisfiedLinkError:
NativeIO$Windows.access0` on the first directory listing. The harness
prepends `%HADOOP_HOME%\bin` to the child process `PATH` so callers don't
have to.

#### Making a missing toolchain fail loudly

Like the ORC/Avro tests, these no-op when their toolchain is absent. At this
scale that is a hazard: a whole tier can go dark in CI and quietly leave the
suite back at round-trip-only, with nothing red to show for it. **Set these
in CI:**

```
EW_REQUIRE_DELTA_INTEROP=1
EW_REQUIRE_SPARK_INTEROP=1
EW_REQUIRE_DUCKDB_INTEROP=1
```

With either set, an unavailable toolchain becomes a hard failure naming the
exact missing prerequisite instead of a silent skip.

#### Reaching Delta's internals, not just its SQL

`ConflictSemanticsInteropTests` is the one tier-3 class that drives the JVM directly rather than through
SQL: Delta has no statement for "declare a read, then commit something unrelated", because Spark's own
statements declare their reads implicitly. It goes through py4j to
`DeltaLog.forTable(...).startTransaction()` and calls `readWholeTable()` / `filterFiles()`, which are the
exact analogues of EW's `DeclareWholeTableRead()` / `DeclareRead()`.

Two py4j details that are easy to get wrong and cost an afternoon:

- `DeltaOperations.ManualUpdate` is a **nested Scala case object**, so its JVM name is
  `DeltaOperations$ManualUpdate$` and the instance lives in the static `MODULE$` field. Reach it by
  reflection, not attribute access.
- `Class.forName` must be given **Delta's own classloader** (take it off any live Delta object). delta-spark
  arrives via ivy in a child loader that the py4j gateway's default loader cannot see.

The whole scenario matrix runs once per class via a fixture — six tests reading one measurement — because
each run costs a session plus five table builds.

#### The interop classes share one xUnit collection

`Interop/InteropCollection.cs` puts every interop test class in a single
`[Collection("Interop")]` with parallelization disabled. They share ONE
serve-mode Spark process and one lock, so running the classes concurrently buys
no throughput — it only puts more threads into the 600-second wait described
below, where a command that is merely QUEUED can exhaust its timeout and fail
under the name of an innocent test.

Measured when the fourth interop class was added (2026-07-31): 70 passed /
1 m 13 s before it, 75 passed + 1 timeout / **11 m 28 s** with it and no
collection, 76 passed / 1 m 32 s once collected. Do not add an interop class
without the attribute.

#### A stalled Spark driver looks like a failing assertion

All Spark commands share ONE serve-mode process, serialized by a lock
(`InteropDriver.InvokeOnServer`), with a 600 s per-command timeout (`Spark.cs`).
If that process stalls, the timeout is charged to whichever test happened to
hold the lock — so a hung JVM surfaces as *that* test failing, with no hint that
the cause was infrastructural. **Check the run's total duration before believing
the failure.** A tier-3 full suite is ~55 s on this machine; a stall shows up as
~11 m with one interop test "failing", which is the 600 s timeout plus the
normal run.

**Root cause found and fixed (2026-07-28).** It was not the JVM. `InvokeOnServer`
blocks its calling thread for up to 600 s waiting for the done-marker, and xUnit
runs the three interop test classes in PARALLEL, so several thread-pool threads
sit in that wait at once. The stdout reader used to be a `Task.Run` — a pool
task — and the stderr drain (`BeginErrorReadLine`) dispatches on the pool too.
On **.NET Framework** the pool injects new threads at roughly one per 500 ms, so
under enough concurrent commands the drain goes unscheduled, the driver's stderr
pipe fills, and Spark BLOCKS mid-command writing to it. Hence net472-only,
load-dependent, and never reproducible when running one test.
Measured: adding two Spark-using tests took the net472 `Interop` filter from
67 passed / 1 m 3 s to 68 passed + 1 timeout / 11 m 23 s; moving the reader to a
dedicated `LongRunning` thread took it to 69 passed / 1 m 14 s. If this ever
returns, raise `ThreadPool.SetMinThreads` or put the interop classes in one
xUnit collection — they share a single Spark process, so parallelism across them
buys nothing anyway.

### Regenerating Avro Test Data

The Avro test suite includes pre-generated `.avro` files in
`test/EngineeredWood.Avro.Tests/TestData/`. To regenerate them:

```
cd test/EngineeredWood.Avro.Tests/TestData
python generate_test_data.py
```

This requires `fastavro` to be installed.

## Running Tests

### All tests (all targets)

```
dotnet test
```

Or per project:

```
dotnet test test/EngineeredWood.Parquet.Tests
dotnet test test/EngineeredWood.Orc.Tests
dotnet test test/EngineeredWood.Avro.Tests
```

### Single target framework

```
dotnet test --framework net10.0
dotnet test --framework net8.0
dotnet test --framework net472
```

### Filtered

```
dotnet test --filter "FullyQualifiedName~CrossValidat"
dotnet test --filter "FullyQualifiedName~BatchedRead"
```

## Understanding Test Output

### Skipped tests

The ORC and Avro cross-validation tests show as "Skipped" with a reason
when their Python package is missing:

```
Skipped EngineeredWood.Orc.Tests.CrossValidationTests.CrossValidate_Integers [1 ms]
...
Passed!  - Failed: 0, Passed: 194, Skipped: 10, Total: 204
```

`Skipped: 0` for ORC/Avro means the Python tests **are running** (they
passed); a non-zero count means the package is not installed.

### The Delta interop tiers do NOT skip — they report Passed

**This is the one number in the output you cannot take at face value.**
The Delta interop tests are gated by `if (!Spark.EnsureAvailable()) return;`,
which returns *normally* — so a test whose tier is unreachable is counted
**Passed**, not Skipped. Measured on a machine with no JDK:

```
dotnet test --filter "FullyQualifiedName~SparkInteropTests"
Passed!  - Failed: 0, Passed: 54, Skipped: 0, Total: 54, Duration: 94 ms
```

54 green tests in 94 milliseconds, having validated nothing. Nothing in
that line distinguishes it from real coverage, and a whole tier can go dark
without anyone noticing.

Two defences, and you want both:

- **Set the `EW_REQUIRE_*` variables** (below). They turn an unreachable
  tier into a loud failure, which is the only way a green run *proves* the
  tier ran.
- **Watch the duration.** The Spark tier cannot complete in under a second;
  if `SparkInteropTests` finishes instantly, it did nothing.

CI does this for you now: the delta-rs and DuckDB tiers run on every commit
with their require-variables set, and the full matrix including Spark runs
in the nightly `Interop` workflow. So a tier going dark locally no longer
means it is dark everywhere — but it does mean *your* run proved less than
it looked like it did.

(Reporting these as genuine skips would be better. It needs xunit.v3 or
`SkippableFact` — xUnit 2.9.3 has no `Assert.Skip` — so it has not been
done.)

### Expected test counts

Measured on net10.0, 2026-08-08, with every optional toolchain present:

| Suite | Total |
|---|---|
| **Parquet** | 807 |
| **Core** | 452 |
| **DeltaLake.Table** | 842 (98 of them interop) |
| **DeltaLake** | 484 |
| **Vortex** | 322 |
| **Avro** | 301 |
| **Iceberg** | 243 |
| **ORC** | 237 |
| **Lance** | 209 |
| **Expressions** | 139 |
| **Lance.Table** | 96 |
| **Expressions.Arrow** | 38 |

Counts move with every feature, so treat these as an order-of-magnitude
check rather than a target — the useful signal is `Failed: 0` and, for the
suites above that have optional tiers, `Skipped: 0`.

Of `DeltaLake.Table`'s 98 interop tests, **68 need PySpark** and **2 need
DuckDB**; the remaining 28 need delta-rs. Measured by making each tier
unreachable with its `EW_REQUIRE_*` set and counting the failures, which is
also a quick way to re-derive these after adding tests.

To regenerate the whole table:

```
dotnet test engineered-wood.slnx -f net10.0 --configuration Release
```

## Parquet Compatibility Tool

A separate CLI tool validates the Parquet reader against a corpus of
real-world files from multiple implementations:

```
dotnet run --project test/EngineeredWood.Parquet.Compatibility
```

This downloads ~138 Parquet files on first run (cached in a temp directory)
and validates that the reader can parse metadata, decompress, and decode
each file. It does not require Python or any external tools.

## Benchmarks

```
dotnet run -c Release --project test/EngineeredWood.Parquet.Benchmarks -- --filter "*RowGroupRead*"
dotnet run -c Release --project test/EngineeredWood.Orc.Benchmarks
dotnet run -c Release --project test/EngineeredWood.Avro.Benchmarks
```

Add `--framework net472` to benchmark on .NET Framework.

The Parquet benchmarks also include a cloud benchmark for Azure Blob Storage:

```
dotnet run -c Release --project test/EngineeredWood.Parquet.Benchmarks -- cloud
```

This prompts interactively for an Azure Blob URL and account key.
