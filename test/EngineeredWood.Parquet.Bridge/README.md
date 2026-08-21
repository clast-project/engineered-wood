# EngineeredWood.Parquet.Bridge

Lets [Parquity](https://github.com/sovsparrow/parquity) drive EngineeredWood as one engine in its
writer-by-reader matrix: write a table with us, read it back with PyArrow, DuckDB or Polars, and
compare what comes out.

Round-tripping through our own reader only proves the reader and writer agree with each other. This
is what checks the agreement is Parquet.

Nothing in the repository depends on it. It is a tool for running against other implementations by
hand, and it is `IsPackable=false` like everything else under `test/`.

## Setup

Parquity is a Python package; the bridge is a .NET executable it launches as a subprocess.

```console
pip install parquity
dotnet build test/EngineeredWood.Parquet.Bridge -c Release
```

Point Parquity at the executable through a TOML file. It has no search path — a declaration names a
command that will be executed, so it must be opted into explicitly.

```toml
# engines.toml
[engines.engineeredwood]
command = ["C:/src/GitHub/engineered-wood/test/EngineeredWood.Parquet.Bridge/bin/Release/net10.0/ew-parquet-bridge.exe"]
```

```console
set PARQUITY_ENGINES_FILE=engines.toml     # PowerShell: $env:PARQUITY_ENGINES_FILE = "engines.toml"
parquity engines
```

`engines` should now list `engineeredwood` alongside the built-in providers. If it does not, nothing
below will work — Parquity probes the bridge once and reports a failed probe as a configuration
error rather than quietly shrinking the matrix.

To point the same name at a different build without editing the file, set
`PARQUITY_ENGINE_ENGINEEREDWOOD_COMMAND`.

## Running it

An external engine is never in the default matrix. Name it explicitly.

```console
# A table you already have. Every writer against every reader.
parquity check case.json --out run --writers engineeredwood,pyarrow,duckdb \
                                   --readers engineeredwood,pyarrow,duckdb

# Generated tables, when you do not know which shape breaks.
parquity fuzz --examples 250 --seed 11 --max-saved 6 --out run \
              --writers engineeredwood --readers pyarrow

# The writer-profile axis: Parquity writes a control table with each profile and
# verifies the artifact honours it, rather than trusting what `info` declares.
parquity check case.json --out run --writers engineeredwood --readers engineeredwood \
    --writer-profiles compression-gzip,compression-brotli,row-group-2,min-max-statistics-off
```

Exit status is the result: `0` nothing found, `1` findings saved under `--out`, `2` a configuration
or usage problem, `3` an internal failure. A run that finds nothing creates no output directory.

### `scan` will refuse

`parquity scan` reads existing Parquet files and compares readers against each other, and it does
**not** accept external engines — it fails with
`external engines are not available to scan: engineeredwood`. Scan runs each reader in a worker
whose contract was written for an in-process provider, and a bridge that stops answering is
classified there by rules that do not fit. That is upstream's guard, deliberately, and it will lift
when the scan worker learns to classify a bridge.

That guard is newer than some of the findings below. Four of them — #156, #157, #167 and #184 —
came from scanning the vendored `parquet-testing` corpus with EngineeredWood as one of several
readers, which needed a local patch to bypass the refusal. Worth knowing before trying to reproduce
them: `check` and `fuzz` are the two commands that work today.

## What the bridge does

Three operations, exchanging tables as Arrow IPC **files** with one JSON control object on stdout.
The contract is `parquity.bridge.v1`; `docs/external-engines.md` in Parquity is the normative
description.

| Operation | Arguments |
|---|---|
| `info` | — |
| `read` | `--parquet IN --arrow OUT` |
| `write` | `--arrow IN --parquet OUT [--profile NAME]` |

You can run these by hand, which is the quickest way to see what a finding is about:

```console
ew-parquet-bridge info
ew-parquet-bridge read --parquet input.parquet --arrow out.arrow
```

### The exit codes are the contract

This is the part to preserve if the bridge is ever changed.

| Exit | Meaning | What Parquity does |
|---:|---|---|
| 0 | succeeded | records the result |
| 1 | **the implementation tried and failed** | records it as evidence, under our own exception type |
| 2 | **the request was not understood** | stops the run |
| other | crashed | evidence, as `ExternalEngineCrash` |

Exit 1 is a fact about EngineeredWood and belongs in a finding. Exit 2 is a fact about the bridge and
must not be, or the evidence names the wrong cause — a bridge bug filed as a Parquet bug, in a report
that gets shared with another project. `BridgeContractTests` covers each side separately for that
reason.

### Two deliberate choices

**`DecimalOutputKind.Decimal128`.** Our reader narrows a decimal to the smallest width that fits, so
a `decimal(6,2)` comes back as `Decimal32Array` and mismatches every other engine. That is the
library's documented default and not something to change for a test tool, so the bridge opts out.

**Only profiles we honour are declared.** Parquity verifies each one against a written artifact and
downgrades a declaration the writer does not honour to `UNSUPPORTED`. Declaring one we cannot keep
would record an effective option that never took effect, which is worse than not offering it.

## What it has found

Every one of these was found by running this. All are fixed except #157, which is partly done —
flat chunks split, nested ones still refuse.

| Found by | Issues |
|---|---|
| `check` — the writer-by-reader matrix | #185, #187, #189, #192 |
| `check --writer-profiles` | #155, #158 |
| `fuzz` | #154, #165 |
| `scan` over the vendored corpus, before the guard above | #156, #157, #167, #184 |

Two upstream Parquity bugs came out of it as well
([#15](https://github.com/sovsparrow/parquity/issues/15),
[#16](https://github.com/sovsparrow/parquity/issues/16)); the first is fixed and released.

#155 is the one that makes the case for keeping this around: silent data corruption at default
options, where every value after the first row-group boundary in a nullable column was wrong. It
needed the `row-group-2` writer profile to surface at a size the fuzzer generates, and no test we
had written was looking there.
