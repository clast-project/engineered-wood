# Running Tests

## Prerequisites

### .NET SDK

Install the **.NET 10 SDK**; CI also installs the .NET 8 runtime. Most test projects target
`net10.0`, `net8.0` and `net472`. A few are `net10.0` only: `EngineeredWood.Parquet.Bridge`,
`EngineeredWood.Parquet.Bridge.Tests`, `EngineeredWood.Parquet.Compatibility` and
`EngineeredWood.DeltaLake.DeltaDotnetBenchmarks`.

`net472` **runs** only on Windows but **compiles** anywhere: the SDK supplies the .NET Framework
reference assemblies, so you need no targeting pack and no Mono. Build it before pushing. It is
where the `netstandard2.0` gaps show up (`System.Index`/`System.Range`, `ToListAsync`, and
similar), and some results, such as floating-point formatting, differ between frameworks.

```
dotnet build engineered-wood.slnx      # every project, every TFM it declares (what CI does)
```

⚠ **Do not run `dotnet build -f net472` on the solution.** It forces `net472` onto the `src/`
libraries, which target `netstandard2.0`. The build then fails with `MSB3644` ("reference
assemblies for .NETFramework,Version=v4.7.2 were not found"). Installing a targeting pack will not
fix it. Use `-f net472` only on a project that declares it:

```
dotnet build test/EngineeredWood.DeltaLake.Tests/EngineeredWood.DeltaLake.Tests.csproj -f net472
```

### Git submodules

The Parquet tests read files from the `parquet-testing` submodule:

```
git submodule update --init
```

### Python cross-validation (ORC, Avro)

Some ORC and Avro tests run Python libraries as subprocesses. They report **Skipped** when the
package is missing. CI uses Python **3.13**.

```
pip install pyarrow fastavro lz4 tzdata
```

| Suite | Package | What it checks |
|---|---|---|
| ORC | `pyarrow` | EngineeredWood writes, PyArrow reads (and the reverse for nested types) |
| Avro | `fastavro` | EngineeredWood writes, fastavro reads |
| Parquet | none (ParquetSharp NuGet) | always runs, both directions |

- **`lz4` is required with `fastavro`.** The skip check only imports `fastavro`, so without `lz4`
  the test `AvroPhase6Tests.WriteThenReadWithFastavro_Lz4` fails with `ValueError: lz4 codec is
  supported but you need to install one of the following libraries: ('lz4',)`.
- **`tzdata` is required on Windows.** PyArrow's ORC reader needs IANA time-zone data. The harness
  finds the package and sets `TZDIR` for the subprocess. If you still see time-zone errors, set it
  yourself:

  ```
  $env:TZDIR = "$(python -c "import os, tzdata; print(os.path.join(os.path.dirname(tzdata.__file__), 'zoneinfo'))")"
  ```

- **Python discovery:** the tests try `python3`, then `python` on PATH, then
  `%LOCALAPPDATA%\Programs\Python\Python*\`. On Windows, turn off the Microsoft Store
  `python.exe`/`python3.exe` app execution aliases (Settings → Apps → Advanced app settings → App
  execution aliases).

### Delta Lake interop tiers

These tests live in `test/EngineeredWood.DeltaLake.Table.Tests/Interop/`. They check
EngineeredWood against other Delta implementations in both directions: EW writes and the other
tool reads, and the reverse.

| Tier | Install | Covers |
|---|---|---|
| 1: delta-rs | `pip install "deltalake[pyarrow]" tzdata` | Log and checkpoint replay, path encoding, per-file stats, filtered reads, shredded-variant layout. Seconds. |
| 2: DuckDB | `pip install duckdb==2.0.0` | VARIANT in raw Parquet, in both directions (no Delta log). Under a second. |
| 3: PySpark | `pyspark` + `delta-spark` (see below), JDK 17+ | Writer features, DESCRIBE DETAIL, clustering, OPTIMIZE, column mapping, data skipping, VACUUM, variant reads/stats/pruning, V2 checkpoints, Delta's conflict checker. About 1.5 minutes. |

Each tier uses the `python` on PATH unless you name another interpreter. One venv can hold all
three:

```
$env:EW_DELTA_RS_PYTHON = "…\venv\Scripts\python.exe"   # tier 1
$env:EW_DUCKDB_PYTHON   = "…\venv\Scripts\python.exe"   # tier 2
$env:EW_SPARK_PYTHON    = "…\venv\Scripts\python.exe"   # tier 3
```

**Tier 1 needs the `[pyarrow]` extra.** `deltalake` 1.6 made pyarrow optional. The probe imports
only `deltalake`, so without pyarrow the tier fails instead of skipping (`ImportError: Pyarrow is
required`). On Windows the driver also needs `tzdata` for `zoneinfo.ZoneInfo("UTC")`.

**Tier 2 versions.** Reading EW-written VARIANT files needs DuckDB **1.5+**. The cases where DuckDB
writes the file need **2.0+**. Below either version those cases skip. The pinned version is 2.0.0.
The Parquet suite's `DuckDbShreddedVariantTests` covers the same ground through the Parquet readers
tier (below).

#### Tier 3: picking a Spark

`delta-spark` must match the `pyspark` it runs on. These are the pairings in
`Spark.ValidatedAgainstVersion`, and the nightly runs one lane for each:

| Lane | pyspark / delta-spark | Notes |
|---|---|---|
| `spark-4.2` | 4.2.0 / 4.4.1 | Newest Delta. Installs without `--no-deps`. |
| `spark-4.1` | 4.1.2 / 4.3.1 | GA VARIANT. Needs `delta-spark` installed with `--no-deps`. |
| `spark-4.0` | 4.0.4 / 4.0.0 | Spark 4.0's unannotated VARIANT layout. Cases that need 4.1 skip. |

delta-rs is validated against 1.6.2 and DuckDB against 2.0.0 (`DeltaRs`/`DuckDb.ValidatedAgainstVersion`).
If tests fail after you upgrade a tool, check the tool version before you suspect EngineeredWood.

Give each Spark its own venv, because `pyspark` bundles its JARs:

```
# Python 3.13 (what CI uses)
py -3.13 -m venv spark42 && spark42\Scripts\pip install pyspark==4.2.0 delta-spark==4.4.1

py -3.13 -m venv spark41 && spark41\Scripts\pip install pyspark==4.1.2 importlib_metadata==8.7.1
spark41\Scripts\pip install --no-deps delta-spark==4.3.1

# Python 3.11
py -3.11 -m venv spark41 && spark41\Scripts\pip install pyspark==4.1.1 delta-spark==4.1.0

$env:EW_SPARK_PYTHON = "…\spark42\Scripts\python.exe"
```

The versions are not arbitrary:

- **Windows, Python 3.12+:** `pyspark` before 4.0.3 / 4.1.2 / 3.5.9 hits
  [SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759). The Python worker dies during the
  handshake, and you see `TASK_WRITE_FAILED` or `Python worker exited unexpectedly`. Linux is not
  affected. `pyspark` 4.1.0 does not import on Windows at all.
- **Spark 4.1.2+ needs `delta-spark` 4.3.1+.** Older `delta-spark` jars throw `NoSuchMethodError`
  from `CheckpointProvider.getParquetSchema` on any checkpointed table.
- **`--no-deps`:** every `delta-spark` through 4.3.1 declares `pyspark<=4.1.1`, so pip refuses
  4.1.2 + 4.3.1 with `ResolutionImpossible`. The pairing works. Install `delta-spark` alone with
  `--no-deps`, and install its other dependency, `importlib_metadata<=8.7.1`, yourself.
  `delta-spark` 4.4.x allows `pyspark<=4.2.0`, so `pyspark` 4.2.1 would need `--no-deps` again.
- **VARIANT layout:** Spark 4.1 writes and reads the VARIANT logical-type annotation. Spark 4.0.x
  writes it unannotated, and its reader fails on an annotated group (hence
  `DeltaTableOptions.EmitVariantLogicalType`). `VariantInteropTests` writes whichever layout the
  running Spark reads. Nested-variant and shredding cases skip on 4.0.x.

**Tier 3 on Windows** also needs Hadoop's `winutils.exe` and `hadoop.dll`. Apache does not
publish them; use a community build for the Hadoop version in
`pyspark/jars/hadoop-client-api-*.jar` (the 3.4.0 build works with Spark 4.2's Hadoop 3.5.0). The
harness adds `%HADOOP_HOME%\bin` to the child process's PATH.

```
$env:JAVA_HOME   = "C:\Program Files\Microsoft\jdk-17.x.x-hotspot"
$env:HADOOP_HOME = "C:\Users\<you>\.hadoop\hadoop-3.4.0"
```

#### Make a missing toolchain fail

Every optional tier skips when its tool is missing. Set the matching variable to `1` to make a
missing tool a failure that names the missing prerequisite:

| Variable | Tier | Set in |
|---|---|---|
| `EW_REQUIRE_DELTA_INTEROP` | delta-rs | `build`, nightly |
| `EW_REQUIRE_DUCKDB_INTEROP` | DuckDB (Delta) | `build`, nightly |
| `EW_REQUIRE_SPARK_INTEROP` | PySpark | nightly only (`build` has no JDK) |
| `EW_REQUIRE_PARQUET_READERS_INTEROP` | Parquet readers (DuckDB, DataFusion) | `build` |
| `EW_REQUIRE_PARQUET_ALP_ORACLE` | ALP vs. arrow-rs | `parquet-alp-interop` |
| `EW_REQUIRE_CLOUD_EMULATORS` | Azurite, MinIO, storage-testbench | `cloud-io` |
| `EW_REQUIRE_VORTEX_VALIDATOR`, `EW_REQUIRE_VORTEX_UPSTREAM_COMPAT` | Vortex | `vortex-interop` |

These variables do not affect cases that skip because the tool lacks the feature, such as GA
VARIANT on Spark 4.0. The tool is present in that case.

#### Notes on the Spark harness

- **All interop classes share one xUnit collection.** `Interop/InteropCollection.cs` declares
  `[Collection("Interop")]` with parallelization disabled. Every class must carry the attribute:
  they share one Spark process behind a lock, so running them in parallel makes queued commands
  hit the timeout.
- **A stalled driver looks like a failed assertion.** Each Spark command has a 600 s timeout
  (`Spark.cs`), and the test holding the lock takes the blame. If one interop test fails and the
  run took about 10 minutes longer than usual, suspect the driver, not the assertion.
- **`ConflictSemanticsInteropTests`** calls Delta through py4j
  (`DeltaLog.forTable(...).startTransaction()`, then `readWholeTable()` / `filterFiles()`), not
  through SQL. Two details: `DeltaOperations.ManualUpdate` is a nested Scala object, so reach
  `DeltaOperations$ManualUpdate$.MODULE$` by reflection. Pass Delta's own class loader to
  `Class.forName`, because ivy loads delta-spark in a child loader.

### Parquet interop

**External readers.** Some Parquet tests (page index, statistics order, decimals, shredded
variant, and others) hand EW-written files to DuckDB and DataFusion (arrow-rs). Install them into
one interpreter and point the tier at it. Tests for a missing reader skip.

```
pip install duckdb datafusion pyarrow
$env:EW_PARQUET_READERS_PYTHON = "…\venv\Scripts\python.exe"   # unset -> python on PATH
```

CI pins `duckdb==2.0.0` and `datafusion==54.0.0`.

**ALP against arrow-rs.** `AlpArrowRsInteropTests` uses a Rust oracle and skips until you build
it:

```
cd test/EngineeredWood.Parquet.Tests/Rust
cargo build --release --locked
```

**Parquity (manual).** `test/EngineeredWood.Parquet.Bridge` lets
[Parquity](https://github.com/sovsparrow/parquity) run EngineeredWood in its writer-by-reader matrix
against PyArrow, DuckDB and Polars. You run it by hand; no test depends on it. See
[its README](../test/EngineeredWood.Parquet.Bridge/README.md) for setup and exit codes.

```
pip install parquity
dotnet build test/EngineeredWood.Parquet.Bridge -c Release
```

### Cloud backends

The Azure, S3 and GCS tests in `EngineeredWood.Parquet.Tests` (namespace
`EngineeredWood.Tests.IO`) and some `EngineeredWood.DeltaLake.Table.Tests` tests need local
emulators. They skip when an emulator is not reachable. The `cloud-io` CI job uses these:

| Backend | Emulator | Address |
|---|---|---|
| Azure Blob | Azurite 3.36.0 | `127.0.0.1:10000` |
| S3 | MinIO (`RELEASE.2025-09-07T16-13-09Z`, user/password `minioadmin`) | `127.0.0.1:9000` |
| GCS | storage-testbench v0.63.0 | `127.0.0.1:4443` |

See the `cloud-io` job in `.github/workflows/ci.yml` for the install and start commands.

### Vortex interop

Two suites compare EngineeredWood with upstream vortex. Each uses a Rust tool built from
`test/EngineeredWood.Vortex.Tests/Rust`:

- `VortexCrossValidationTests` gives EW-written files to `vortex-validator`.
- `VortexUpstreamCompatTests` reads upstream's compatibility fixtures (one set per release since
  0.64, about 120 MB) and compares each value with what `vortex-oracle` decodes.

```
cd test/EngineeredWood.Vortex.Tests/Rust
cargo build --release --locked --bin vortex-validator --bin vortex-oracle
cd ..
python UpstreamCompat/fetch_fixtures.py
```

`UpstreamCompat/fixtures.lock.json` pins the releases and their hashes. Downloads go to the
git-ignored `UpstreamCompat/fixtures/`, or to `EW_VORTEX_COMPAT_FIXTURES`. To adopt a new upstream
release, run `fetch_fixtures.py --update` and commit the lock. Without the tools or fixtures these
tests skip. A few fixtures are committed under `TestData/upstream-compat/` and always run.

### Regenerating Avro test data

The `.avro` files in `test/EngineeredWood.Avro.Tests/TestData/` are committed. To regenerate them
(needs `fastavro`):

```
cd test/EngineeredWood.Avro.Tests/TestData
python generate_test_data.py
```

## Running tests

```
dotnet test                                     # everything, every target
dotnet test test/EngineeredWood.Parquet.Tests   # one project
dotnet test --framework net10.0                 # one target (net10.0, net8.0, net472)
dotnet test --filter "FullyQualifiedName~CrossValidat"
dotnet test test/EngineeredWood.DeltaLake.Table.Tests -f net10.0 --filter "FullyQualifiedName~Interop"
```

Opt-in tools that skip unless you set a variable:

- `EW_FUZZ_CORPUS=<corpus.json>` runs `SparkFuzzTriage` (Expressions.Arrow) over a corpus from
  `fuzz_expressions.py`.
- `EW_WRITER_CORPUS_DIR=<dir>` writes the Parquet writer byte-identity corpus
  (`WriterByteIdentityCorpus`).

### Only what a branch affects

On a pull request, CI tests only the projects that own the changed files and every project that
references them. Changes it cannot attribute to a project (`Directory.Build.props`, the solution,
`.github/`, unknown paths) run everything, as does every push to `main` that changes code. The
job summary shows what was selected and why. To do the same locally:

```
python .github/scripts/select_projects.py --base origin/main
dotnet test ci-affected.slnf
```

If the script prints `scope: full`, it writes no filter; use `engineered-wood.slnx`. A test that
reads files outside its own project at run time (for example, `EngineeredWood.Parquet.Tests` reads
`parquet-testing/`) must be listed in the script's `RUNTIME_INPUTS`.

## CI jobs

| Workflow / job | Runs on | What it tests |
|---|---|---|
| `ci.yml` / `build` (required) | windows-latest | Affected projects, all TFMs, with delta-rs, DuckDB, DataFusion, pyarrow and fastavro installed. No JDK, so Spark cases skip. |
| `ci.yml` / `cloud-io` (required) | ubuntu-latest | Cloud backends against the emulators, net10.0. |
| `ci.yml` / `vortex-interop` | ubuntu-latest | Vortex against upstream (Rust 1.96.0). |
| `ci.yml` / `parquet-alp-interop` | ubuntu-latest | ALP against arrow-rs (Rust 1.96.0). |
| `interop-nightly.yml` / `interop (spark-4.x)` | ubuntu-latest, JDK 17, Python 3.13 | All Delta interop tiers, net10.0, once per Spark lane. Also runs on PRs that change the workflow or `Interop/`. |

## Reading test output

### Skipped tests

An optional tier that cannot run reports **Skipped** with a reason:

```
Skipped ...SparkInteropTests.EwWritten_SimpleTable_SparkReadsSameRowsAndProtocol [1 ms]
  spark_driver.py toolchain unavailable: no python interpreter satisfying
  `import pyspark, delta.tables, json; ...` on PATH
```

A green run with skips has not validated the skipped tier, so read the `Skipped:` count. With
every tool installed and the `spark-4.2` or `spark-4.1` pairing, `DeltaLake.Table` should report
0 skipped. On `spark-4.0`, the cases that need Spark 4.1 or delta-spark 4.3 (GA variant, shredded
tables, `add.stats_parsed`, variant statistics) skip. Without a JDK, every Spark case skips. The
`build` job is in that state, so its Spark skips are expected.

### Test counts

To get current counts:

```
dotnet test engineered-wood.slnx -f net10.0 --configuration Release
```

To see how many cases depend on one tier, make that tier unreachable (for example, point its
`EW_*_PYTHON` at an interpreter without the package) and count the skips.

## Parquet compatibility tool

A separate CLI checks the Parquet reader against real-world files from other implementations
(fastparquet, parquet-dotnet, parquet-tools, DuckDB, Hugging Face datasets). It needs no Python:

```
dotnet run --project test/EngineeredWood.Parquet.Compatibility
```

The first run downloads 138 files into `%TEMP%\ew-compat-data`. The tool checks that each file's
metadata parses, decompresses and decodes.

## Benchmarks

```
dotnet run -c Release --project test/EngineeredWood.Parquet.Benchmarks -- --filter "*RowGroupRead*"
dotnet run -c Release --project test/EngineeredWood.Orc.Benchmarks
dotnet run -c Release --project test/EngineeredWood.Avro.Benchmarks
```

Add `--framework net472` to run on .NET Framework (not available for the Lance or
DeltaDotnet benchmarks).

`dotnet run -c Release --project test/EngineeredWood.Parquet.Benchmarks -- cloud` runs an Azure
Blob benchmark. It prompts for a blob URL and account key.
