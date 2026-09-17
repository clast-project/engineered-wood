# EngineeredWood.Parquet.SparkBridge

Puts **Spark** into [Parquity](https://github.com/sovsparrow/parquity)'s matrix as a reader, so that
what we write can be checked against the engine most likely to be asked to read it.

Nothing in the repository depends on it. Like `EngineeredWood.Parquet.Bridge` beside it, this is a
tool for running against other implementations by hand.

## Why

[#269](https://github.com/clast-project/engineered-wood/issues/269) was a defect in our default
write path: every `FLOAT` and `DOUBLE` column we wrote was unreadable by Spark. We already had
Parquity, which exists for exactly that class of bug, and **no configuration of it could have caught
this** — its five engines are all Python distributions, and four of the five read the file happily.
The one that did object, fastparquet, objects to PyArrow's version of the same file too, so under
agreement semantics it is correctly triaged as a reader limitation rather than a writer finding.

Agreement between implementations is not the same question as reachability by consumers. This
bridge asks the second one.

Measured here on Spark 4.0.1 / JDK 17, against PyArrow-written files whose encodings were read back
out of the footer rather than assumed from the write option:

| reader | V1 + BSS | V2 + BSS | V1 + PLAIN |
|---|---|---|---|
| vectorized — **the default** | **FAIL** | **FAIL** | OK |
| `spark.sql.parquet.enableVectorizedReader=false` | OK | OK | OK |

So it is not a page-version split and it is not Parquet: it is the vectorized path, which is what a
Spark user gets unless they have turned it off. The bridge therefore leaves that setting alone —
turning it off would make Spark agree with everyone else and report nothing.

## Setup

Needs `pyspark` **and `pyarrow`**, a JDK, and on Windows the same Hadoop `winutils.exe` +
`hadoop.dll` the Delta tier-3 tests need. See `doc/running-tests.md`, which documents all of it.

```console
pip install pyspark pyarrow
```

`pyspark` does not require `pyarrow`, and the read path needs it twice — for `toArrow()` and for
the Arrow IPC output. `info` probes both, so a half-installed environment reports itself at
configuration time rather than dying on the first read.

Declare it to Parquity through a TOML file:

```toml
# engines.toml
[engines.spark]
command = [
  "C:/Users/you/AppData/Local/Programs/Python/Python311/python.exe",
  "C:/src/GitHub/engineered-wood/test/EngineeredWood.Parquet.SparkBridge/spark_parquet_bridge.py",
]
timeout_seconds = 300
```

> **Name the interpreter, do not write `python`.** Parquity runs inside its own virtual environment,
> so a bare `python` resolves to *that* environment — which does not have `pyspark`. The symptom is
> `"available": false, "detail": "info probe exited 2"` from `parquity engines`, which reads like a
> broken bridge and is a wrong interpreter.

```console
set PARQUITY_ENGINES_FILE=engines.toml
parquity engines
```

`spark` should appear with `"tier": "external"`, `"reader": true`, `"writer": false`.

## Using it

```console
parquity check cases/floating-point.case.json --out check-run ^
    --writers engineeredwood,pyarrow --readers spark,pyarrow
```

`cases/floating-point.case.json` is here because of the second half of #274: #269 was invisible
partly because no schema in reach had a floating-point column in it. That case carries `float32` and
`float64` through the values that break naive encoders — a negative zero, both denormal minimums,
both maximums, and the pairs that **bracket** the precision limit: `2^24` beside `2^24 + 2` for
`float32`, `2^53` beside `2^53 + 2` for `float64`.

> The brackets are deliberate. `2^24 + 1` and `2^53 + 1` are not representable, and a JSON case
> cannot carry them: Parquity parses finite JSON numbers through Python `float`, so
> `9007199254740993.0` has already become `9007199254740992.0` before any writer runs. A fixture
> written that way tests the boundary it names only in the comment.

`check` and `fuzz` work. **`scan` does not**: it refuses external engines outright with
`ENGINE_CAPABILITY_ERROR`. That is the right trade here — `check` and `fuzz` are where our own
output is under test, and `scan` is for reading a corpus we did not write.

### What it can and cannot be asked

Spark has **one** timestamp type, at microseconds, and normalizes every Parquet timestamp into it.
The bridge returns Spark's schema, so a case declaring anything else compares as a mismatch that is
Spark's type system rather than a defect in the file. Measured through `parquity check`:

| case type | result |
|---|---|
| `date32` | fine |
| `timestamp[us]` | fine |
| `timestamp[ms]` | `SCHEMA_MISMATCH` — *expected timestamp[ms, tz=UTC], got timestamp[us, tz=UTC]* |
| `timestamp[ns]` | `READ_ERROR`, and a **real** one — Spark refuses nanoseconds with `PARQUET_TYPE_ILLEGAL` |

Casting Spark's output back to the file's schema would make the third row go away and would launder
exactly the evidence this exists to collect, so the limitation is documented rather than hidden.
The fourth row is the tool working: a Spark user cannot read that file at all.

## Why there is a server

Parquity launches a bridge as **one subprocess per operation**, and a JVM plus a `SparkSession`
costs 15–20 seconds against a `timeout_seconds` bounded at 300. A bridge that started Spark per read
would technically work and be useless at fuzz volumes.

So the executable Parquity launches is a thin client of a long-lived server holding one session,
which it starts on first use — the same shape as `spark_driver.py`'s serve mode, over a loopback
socket rather than stdin because there is no parent process here to hold a pipe open. Measured:

| | |
|---|---|
| `info` | 0.17 s — answers from `pyspark.__version__`, no JVM at all |
| first `read` | 6.4 s — starts the server |
| every `read` after | **0.20 s** |

The server exits after 30 minutes idle so a fuzz run does not leave a JVM behind for the day, and
`spark_parquet_bridge.py stop` ends it now. `EW_SPARK_BRIDGE_STATE` points a second checkout — or a
second Spark version — at a server of its own.

Verified to persist **across separate `parquity check` invocations**, which is the thing that
matters and not merely across two calls from a shell: a cold run is 7.3 s and the next is 1.1 s.
Parquity launches a bridge with a plain `subprocess.run`, so the server is not tied to the client's
lifetime. If you drive Parquity from something that *does* put its children in a kill-on-close job,
the server will not survive and every read pays cold start — slower, never wrong.

The endpoint file holds a bearer token that authorizes a read, so the state directory is created
`0700` and the file `0600` — `chmod`-ed even when the directory already exists, since one left
behind by an earlier run is the case that would otherwise stay open. On Windows the per-user temp
directory is already private and the modes are a no-op.

## Checking that it still catches things

```console
python selftest.py
```

A reader bridge that always says OK is worse than no bridge: it turns an unchecked risk into a green
tick. The self-test asserts both directions — a plain file round-trips through Spark byte for byte,
**and** a `BYTE_STREAM_SPLIT` file is reported as a failure naming both the encoding and the
vectorized reader:

```
info: spark 4.0.1 directions=['read']
fixture encodings: ('RLE', 'BYTE_STREAM_SPLIT')
plain: OK, 2000 rows round-tripped through Spark
byte-stream-split: reported SparkUnsupportedOperationException: FAILED_READ_FILE.NO_HINT:
    Unsupported encoding: BYTE_STREAM_SPLIT [raised at VectorizedColumnReader.getValuesReader]
OK: the bridge answers, and still catches BYTE_STREAM_SPLIT
```

It also asserts that the fixture is what it claims. PyArrow will quietly dictionary-encode a
low-cardinality float column and ignore `use_byte_stream_split`, which produces a file with no BSS in
it and a check that passes for the wrong reason — this cost a full measurement cycle to notice, so
the encodings are read back out of the footer and asserted before the file is used.

## Evidence quality

Spark reports a read failure as a stack that buries its own cause: the top is `awaitResult`, the
middle is reflection and py4j, and the one line that says anything is at the bottom, past Parquity's
2 KB evidence cap. The bridge reduces it to the root cause, the frame it was raised in, and Spark's
own error condition — because `VectorizedColumnReader` is the difference between *Spark cannot read
this* and *Spark's default reader cannot read this*.

## Not done here

Whether Parquity should be able to express "readable by N of M engines" as a **writer-side risk
signal**, rather than only as per-reader agreement, is the part that generalises past Spark. It is a
design question for Parquity's maintainer, not something to patch in locally — see #274.
