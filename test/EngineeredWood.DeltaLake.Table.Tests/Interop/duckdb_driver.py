#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""DuckDB interop driver for the EngineeredWood test suite.

Invoked as:  python duckdb_driver.py <command> <json-args-file> [json-result-file]

Same contract as the delta-rs and Spark drivers: arguments arrive in a file, a single JSON object
comes back in a file, and any failure is reported as {"ok": false, "error": "..."} with exit code 0
so the C# side asserts on a message rather than on a process crash.

DuckDB is here for one thing the other tiers cannot give: an implementation with a NATIVE VARIANT
type that reads parquet directly. delta-rs surfaces a variant as its physical struct and Spark needs
a JVM, so DuckDB is both the cheapest reader and the one whose type system has the most to disagree
with. Commands stay dumb; all judgement lives in the C# assertions.
"""
import json
import sys
import traceback

# VARIANT is not in older DuckDB at all — a file EW shreds would read as a bare struct there, which
# would look like a conformance failure rather than a missing feature.
MIN_DUCKDB = (1, 4, 0)


def cmd_probe(args):
    import duckdb
    ver = tuple(int(x) for x in duckdb.__version__.split(".")[:3])
    return {
        "duckdb": duckdb.__version__,
        "supported": ver >= MIN_DUCKDB,
        "python": sys.version.split()[0],
    }


def cmd_read_parquet_variant(args):
    """Read a parquet file EW wrote and report what DuckDB made of its variant column.

    `typeof` proves DuckDB recognised the column as a VARIANT rather than accepting the storage
    struct, and the JSON cast proves it DECODED each value — a shredded row's data lives in
    typed_value, so a reader that ignored shredding would surface an empty value here rather than
    fail. Null-ness is read from the raw value, not from the JSON cast: DuckDB 1.5 casts a SQL-NULL
    variant to the JSON TEXT 'null', which would otherwise be indistinguishable from a variant that
    genuinely holds a JSON null.
    """
    import duckdb

    path = args["path"].replace("'", "''")
    col = args.get("col", "v")
    order_by = args.get("order_by")
    order = f" ORDER BY {order_by}" if order_by else ""

    con = duckdb.connect()
    typename = con.sql(f"SELECT typeof({col}) FROM '{path}' LIMIT 1").fetchall()
    rows = con.sql(
        f"SELECT {col} IS NULL AS is_null, CAST({col} AS JSON) AS vjson FROM '{path}'{order}"
    ).fetchall()

    return {
        "duckdb": duckdb.__version__,
        "column_type": typename[0][0] if typename else None,
        "rows": [{"null": bool(r[0]), "vjson": None if r[0] else r[1]} for r in rows],
    }


COMMANDS = {
    "probe": cmd_probe,
    "read_parquet_variant": cmd_read_parquet_variant,
}


def _emit(out_path, obj):
    payload = json.dumps(obj, ensure_ascii=False, default=str)
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
