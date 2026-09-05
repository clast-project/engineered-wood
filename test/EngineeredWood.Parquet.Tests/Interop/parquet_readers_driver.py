#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""Independent-parquet-reader driver for the EngineeredWood test suite.

Invoked as:  python parquet_readers_driver.py <command> <json-args-file> [json-result-file]

Same contract as the delta-rs, DuckDB and Spark drivers under the Delta suite: arguments arrive in a
file, one JSON object comes back in a file, and any failure is reported as {"ok": false, "error":...}
with exit code 0 so the C# side asserts on a message rather than on a process crash.

WHY THESE TWO READERS. ParquetSharp already covers parquet-cpp from inside the test suite, and an
out-of-band check covered pyarrow and fastparquet. Neither of those is the engine most likely to read
an EngineeredWood file in anger:

  - **DuckDB** has its own parquet reader, written from the spec rather than derived from any of the
    reference implementations. It is the most genuinely independent decoder available.
  - **DataFusion** is here as a stand-in for **arrow-rs**, whose `parquet` crate it uses directly.
    That crate is what polars, delta-rs and every Rust parquet consumer reads through, and nothing
    else in this repository exercises it.

Both readers are asked only to decode a file and hand back what they saw. All judgement lives in the
C# assertions.

pyarrow is a REQUIRED dependency of this driver, not an optional one: it is the transport both
readers hand their decoded values back through. It does no parquet decoding here and so cannot mask
a framing bug, but its absence would make the tier fail for a reason that has nothing to do with
parquet — which is why the availability probe checks for it too rather than discovering it here.
"""
import hashlib
import json
import sys
import traceback


# Values per hashed window. Ours, not the reader's — see cmd_read_digest.
_DIGEST_STRIDE = 8192


def _import(reader):
    """Returns (module, error). Absence is data, not a failure — the caller decides whether a
    missing reader skips the test or fails it."""
    try:
        if reader == "duckdb":
            import duckdb
            return duckdb, None
        if reader == "datafusion":
            import datafusion
            return datafusion, None
        return None, f"unknown reader '{reader}'"
    except ImportError as exc:
        return None, f"{type(exc).__name__}: {exc}"


def cmd_probe(args):
    out = {"python": sys.version.split()[0], "readers": {}}
    for reader in ("duckdb", "datafusion"):
        module, error = _import(reader)
        out["readers"][reader] = getattr(module, "__version__", None) if module else None
        if error:
            out["readers"][reader + "_error"] = error
    # The version string the C# side reports for the tier as a whole.
    out["v"] = ", ".join(
        f"{r}={out['readers'][r] or 'absent'}" for r in ("duckdb", "datafusion"))
    return out


def _to_arrow(reader, path):
    """Decodes a parquet file with one reader and returns a pyarrow Table.

    pyarrow is only the transport for values these readers have ALREADY decoded — it does no parquet
    decoding of its own here, so it cannot mask a framing bug in the reader under test.
    """
    if reader == "duckdb":
        import duckdb
        result = duckdb.sql("SELECT * FROM read_parquet(?)", params=[path])
        table = result.arrow()
        # DuckDB 1.5 returns a RecordBatchReader here where older versions returned a Table.
        return table.read_all() if hasattr(table, "read_all") else table

    if reader == "datafusion":
        from datafusion import SessionContext
        return SessionContext().read_parquet(path).to_arrow_table()

    raise ValueError(f"unknown reader '{reader}'")


def cmd_read_digest(args):
    """Decodes a file and returns a per-column digest of the values, plus the row count.

    A digest rather than the values themselves because these files carry hundreds of thousands of
    rows; the question asked of this driver is only ever "did the two framings decode to the same
    thing", which a hash answers as well as a transcript and survives the JSON round trip.
    """
    reader = args["reader"]
    module, error = _import(reader)
    if module is None:
        return {"available": False, "error": error}

    try:
        table = _to_arrow(reader, args["path"])
    except ImportError as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}

    columns = []
    for name, column in zip(table.column_names, table.columns):
        # to_pylist() on the CHUNKED array, then windows of our own STRIDE: the digest must depend
        # on the values alone, never on how the reader chose to chunk them. Hashing chunk by chunk
        # would fold the reader's batch boundaries into the hash, and those can legitimately follow
        # page boundaries — which is the one thing batching changes. That would turn this oracle
        # into a false alarm on exactly the files it exists to clear.
        values = column.to_pylist()
        digest = hashlib.sha256()
        for start in range(0, len(values), _DIGEST_STRIDE):
            digest.update(repr(values[start:start + _DIGEST_STRIDE]).encode("utf-8"))
        columns.append({"name": name, "digest": digest.hexdigest()})

    return {
        "available": True,
        "rows": table.num_rows,
        "columns": columns,
        "version": getattr(module, "__version__", None),
    }


COMMANDS = {
    "probe": cmd_probe,
    "read_digest": cmd_read_digest,
}


def _emit(out_path, payload_obj):
    payload = json.dumps(payload_obj)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(payload)
    else:
        sys.stdout.write(payload)


def main():
    out_path = sys.argv[3] if len(sys.argv) > 3 else None
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        _emit(out_path, {"ok": False,
                         "error": f"unknown command; expected one of {sorted(COMMANDS)}"})
        return 0
    if len(sys.argv) > 2:
        with open(sys.argv[2], "r", encoding="utf-8") as fh:
            args = json.load(fh)
    else:
        args = {}

    try:
        result = COMMANDS[sys.argv[1]](args)
        result["ok"] = True
        _emit(out_path, result)
    except Exception as exc:  # noqa: BLE001 - reported, not raised: see the module docstring
        _emit(out_path, {"ok": False,
                         "error": f"{type(exc).__name__}: {exc}",
                         "traceback": traceback.format_exc()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
