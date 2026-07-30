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
| 1 — delta-rs | `pip install "deltalake[pyarrow]"` | 19 | Log/checkpoint replay, path encoding, per-file stats, filtered reads. Seconds to run. |
| 3 — PySpark | `pip install pyspark delta-spark` + JDK 17+ | 36 | Writer features, DESCRIBE DETAIL, clustering, OPTIMIZE, column mapping, data skipping, VACUUM survival. ~40s to run. |

**The `[pyarrow]` extra is not optional.** `deltalake` 1.6 made pyarrow an extra,
and the tier-1 driver reads through pyarrow — install the bare package and the
tier does not skip, it FAILS, all 19 tests at once with
`ImportError: Pyarrow is required, install deltalake[pyarrow]`. The availability
probe only imports `deltalake`, which succeeds.

Tier 2 (DuckDB) was evaluated and **dropped**: delta-rs embeds
delta-kernel-rs, which is what DuckDB's delta extension is, so tier 1
already subsumes it. See [`upstream-landing-notes.md`](upstream-landing-notes.md).

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
```

With either set, an unavailable toolchain becomes a hard failure naming the
exact missing prerequisite instead of a silent skip.

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

Tests that depend on optional Python libraries show as "Skipped" with a
reason:

```
Skipped EngineeredWood.Orc.Tests.CrossValidationTests.CrossValidate_Integers [1 ms]
...
Passed!  - Failed: 0, Passed: 194, Skipped: 10, Total: 204
```

If you see `Skipped: 0` for ORC/Avro, the Python tests **are running**
(they passed). If you see `Skipped: 10` (ORC) or `Skipped: 7` (Avro),
the Python packages are not installed.

### Expected test counts

| Suite | Total | Always run | Tool-dependent |
|---|---|---|---|
| **Parquet** | 590 | 590 | 0 |
| **ORC** | 204 | 194 | 10 (PyArrow) |
| **Avro** | 272 | 265 | 7 (fastavro) |
| **DeltaLake** | 199 | 199 | 0 |
| **DeltaLake.Table** | 290 | 242 | 21 interop (9 delta-rs + 12 PySpark) |

`DeltaLake.Table` also reports 27 **skipped** tests unconditionally — those
are `PendingCoverageTests`, which pin behaviour from PR #4 that has not
landed yet. Each names the exact API it is waiting on. They are skipped
rather than failing so that a red suite still means a real regression.

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
