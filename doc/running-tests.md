# Running Tests

## Prerequisites

### .NET SDK

The solution requires **.NET 10 SDK** (or later). The test projects multi-target:

- `net10.0` — primary target
- `net8.0` — LTS target
- `net472` — .NET Framework (**runs** on Windows only; **compiles** anywhere)

#### Checking net472 off Windows

The net472 distinction is compile vs. run, and only the second half is Windows-only. `dotnet test`
needs Mono to host a .NET Framework test process and aborts without it, but the **compile** works on
macOS and Linux out of the box: the SDK implicitly adds `Microsoft.NETFramework.ReferenceAssemblies`
for any `.NETFramework` target, so no targeting pack (and no Mono) is needed to build.

That compile is worth running before you push, because net472 is where the BCL gaps show up —
`System.Index`/`System.Range`, `ToListAsync`, and the rest of what `netstandard2.0` does not carry.
It has caught real breakage that a net10.0-only run does not.

```
dotnet build engineered-wood.slnx      # every project, every TFM it declares — what CI does
```

⚠ **Do not use `dotnet build -f net472` at the solution level.** It forces `net472` onto the `src/`
libraries, which target `netstandard2.0` and never declare it — so restore produced no `net472`
dependency graph for them, they get no reference assemblies, and the build dies with 17 × `MSB3644`
(*"reference assemblies for .NETFramework,Version=v4.7.2 were not found"*). That error reads like a
missing SDK component and is really a malformed command; installing a targeting pack will not fix it.
`-f net472` is fine on a project that actually declares `net472` — every project under `test/`, plus
`src/EngineeredWood.Parquet.TestTool`:

```
dotnet build test/EngineeredWood.DeltaLake.Tests/EngineeredWood.DeltaLake.Tests.csproj -f net472
```

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

To enable them, install Python 3.8+ (CI uses **3.13**; see the Spark tier
below for the one version constraint that is not negotiable on Windows) and
the following packages:

```
pip install pyarrow fastavro lz4 tzdata
```

`lz4` is needed by `fastavro`, not by us: fastavro ships the LZ4 codec but
not the library it calls, so `AvroPhase6Tests.WriteThenReadWithFastavro_Lz4`
fails with `ValueError: lz4 codec is supported but you need to install one
of the following libraries: ('lz4',)` if you install `fastavro` alone. That
is a hard failure, not a skip — the skip logic only checks that `fastavro`
itself imports.

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
established against `deltalake` 1.6.2 and `pyspark` 4.0.4 /
`delta-spark` 4.0.0; those are recorded in `DeltaRs.ValidatedAgainstVersion`
and `Spark.ValidatedAgainstVersion`. If these tests fail after an upgrade,
check the tool version before assuming an EngineeredWood regression.

**On Windows, `pyspark` 4.0.3 is a hard floor under Python 3.12+.** Earlier
4.0.x hit [SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759) — a
missing flush in the simple-worker path — and the Python worker dies partway
through the JVM handshake. It surfaces as `TASK_WRITE_FAILED` or
`SparkException: Python worker exited unexpectedly`, with the real cause
visible only after setting `spark.python.worker.faulthandler.enabled=true`
(and on 3.12, not even then). Measured: with `pyspark` 4.0.1, Python 3.12 and
3.13 both fail 18 of the 109 interop tests — every Spark-backed one — while
3.11 passes all 109. Fixed in 4.0.3 / 4.1.2 / 3.5.9. Linux is unaffected: it
forks workers via `pyspark.daemon` instead of spawn-and-connect-back, which is
why `interop-nightly.yml` never saw this.

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
python -m venv spark41 && spark41\Scripts\pip install pyspark==4.1.1 delta-spark==4.1.0
$env:EW_SPARK_PYTHON = "…\spark41\Scripts\python.exe"   # tier 3 uses this interpreter; unset -> PATH
```

**Pin `pyspark` to 4.1.1 exactly.** Both neighbours are broken against
`delta-spark` 4.1.0. 4.1.0 cannot be imported on Windows at all
(`socketserver.UnixStreamServer` is Unix-only). 4.1.2 changed the
`ParquetToSparkSchemaConverter` constructor, so `delta-spark` 4.1.0 throws
`java.lang.NoSuchMethodError` from `CheckpointProvider.getParquetSchema` on
any checkpointed table. Measured on both 4.1.2 and 4.1.3. This file
previously recommended 4.1.3, which fails this way on every checkpointed
table the tier touches.

The GA annotated path and the nested-variant cases have been validated against
`pyspark` 4.1.1 / `delta-spark` 4.1.0; the unannotated path against 4.0.1 /
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

Like the ORC/Avro tests, these skip when their toolchain is absent. At this
scale a skip is not enough on its own: a whole tier can go dark in CI and
quietly leave the suite back at round-trip-only, with nothing red to show
for it. **Set these in CI:**

```
EW_REQUIRE_DELTA_INTEROP=1
EW_REQUIRE_SPARK_INTEROP=1
EW_REQUIRE_DUCKDB_INTEROP=1
```

With one set, an unavailable toolchain becomes a hard failure naming the
exact missing prerequisite instead of a skip. They are per tier on purpose:
the per-commit build requires delta-rs and DuckDB but not Spark, because
windows-latest has no JDK, while the nightly requires all three.

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

### Parquet interoperability (optional — not part of the suite)

The same argument as the Delta tiers, one format over. Round-tripping through
our own reader proves the reader and writer agree with each other, not that
they agree with Parquet.

`test/EngineeredWood.Parquet.Bridge` is an executable that lets
[Parquity](https://github.com/sovsparrow/parquity) drive EngineeredWood as one
engine in its writer-by-reader matrix, against PyArrow, DuckDB and Polars. It
is run by hand rather than from `dotnet test`, so nothing here is skipped or
reported — no test in the suite depends on it.

```console
pip install parquity
dotnet build test/EngineeredWood.Parquet.Bridge -c Release
```

See [the bridge's README](../test/EngineeredWood.Parquet.Bridge/README.md) for
the engines file it needs, the commands worth running, and what the exit codes
of the contract mean. Ten of the Parquet bugs fixed so far were found this way,
including one that silently corrupted every value after the first row-group
boundary at default write options.

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

### The Delta interop tiers skip, and the skip count is the number to read

The Delta interop tests are `[SkippableFact]`/`[SkippableTheory]` and open
with `Spark.Require();` (or `DeltaRs`/`DuckDb`), so an unreachable tier
reports **Skipped**, with the reason attached:

```
Skipped ...SparkInteropTests.EwWritten_SimpleTable_SparkReadsSameRowsAndProtocol [1 ms]
  spark_driver.py toolchain unavailable: no python interpreter satisfying
  `import pyspark, delta.tables, json; ...` on PATH
```

**This used to be the one number you could not take at face value.** The
gate was `if (!Spark.EnsureAvailable()) return;`, which returns *normally* —
so an unreachable tier was counted **Passed**. Measured on a machine with no
JDK, before the change:

```
Passed!  - Failed: 0, Passed: 54, Skipped: 0, Total: 54, Duration: 94 ms
```

54 green tests in 94 milliseconds, having validated nothing, and nothing in
that line to distinguish it from real coverage.

A skip is honest, but an honest green is still a green — a job can skip an
entire tier and pass. So you still want both defences:

- **Read `Skipped:`.** It now tells you *which* tests did not run and why,
  instead of hiding them in the Passed column. On the pinned oracles the
  expected count is **5**, not 0 — see the note below. More than that means
  something went dark.
- **Set the `EW_REQUIRE_*` variables** ([above](#making-a-missing-toolchain-fail-loudly)). They turn an unreachable
  tier into a loud failure, which is the only thing that makes a green run
  *prove* the tier ran. The skip says what happened; the variable says
  whether this job was allowed to tolerate it.

Skips are not only about a missing toolchain, and this is the part worth
internalising: **five tests skip on a fully-configured machine**, because
the *feature* is newer than the pinned oracle — GA `VARIANT` needs Spark
4.1+ and DuckDB 1.4+, and `add.stats_parsed` needs delta-spark 4.1+. So the
healthy result on the pinned 4.0 pairing is:

```
Passed!  - Failed: 0, Passed: 104, Skipped: 5, Total: 109
```

`EW_REQUIRE_*` does **not** turn these into failures, and should not: the
toolchain is present and working, the feature simply does not exist in it.
Those five used to report Passed as well, which is why "109/109" on the 4.0
pairing never meant what it appeared to. Point the Spark tier at a 4.1 venv
(above) and they run.

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
suites above that have optional tiers, `Skipped: 0`. The one exception is
`DeltaLake.Table`, where 5 skips are expected on the pinned oracles because
the features they cover postdate those versions; see [Skipped
tests](#skipped-tests).

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
