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

# The first DuckDB measured to read EW's variant files. VARIANT arrived in 1.4, but 1.4.5 cannot
# decode them, which would look like a conformance failure rather than a missing feature. See
# DuckDb.ReadsVariantParquet.
MIN_DUCKDB = (1, 5, 0)


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


def cmd_write_parquet_variant(args):
    """Have DuckDB WRITE a query's rows to parquet, then report its own reading of the file.

    The reverse of read_parquet_variant: here DuckDB is the producer and EW the reader. DuckDB 1.5+
    shreds a VARIANT column on COPY by default, so this is an independent source of shredded files.
    `leaves` lists every column chunk's path so the caller can prove the file is really shredded.
    `copy_options` (optional) is appended to the COPY options.
    """
    import duckdb

    path = args["path"]
    col = args.get("col", "v")
    con = duckdb.connect()
    # One thread, so the shredding DuckDB infers comes from the same rows on every run; with several
    # threads it can pick a different layout from one run to the next.
    con.sql("SET threads TO 1")
    escaped = path.replace("'", "''")
    # `copy_options` adds COPY options (ROW_GROUP_SIZE, say) after FORMAT.
    extra = f", {args['copy_options']}" if args.get("copy_options") else ""
    con.sql(f"COPY ({args['query']}) TO '{escaped}' (FORMAT parquet{extra})")

    leaves = sorted({r[0] for r in con.sql(
        "SELECT path_in_schema FROM parquet_metadata(?)", params=[path]).fetchall()})
    rows = con.sql(
        f"SELECT {col} IS NULL, CAST({col} AS JSON) FROM read_parquet(?) ORDER BY {args['order_by']}",
        params=[path]).fetchall()

    return {
        "duckdb": duckdb.__version__,
        "leaves": leaves,
        "rows": [{"null": bool(r[0]), "vjson": None if r[0] else r[1]} for r in rows],
    }


def cmd_variant_path_bounds(args):
    """DuckDB's own min and max of expressions over a parquet file: the oracle for EW's footer-derived bounds.

    `exprs` is a list of {key, sql}; each `sql` is evaluated per row (`v.a::BIGINT`, say) and its min and max
    come back as text. DuckDB computes them from the values, not from the footer statistics EW reads.
    """
    import duckdb

    con = duckdb.connect()
    out = {}
    for e in args["exprs"]:
        lo, hi = con.sql(
            f"SELECT CAST(min({e['sql']}) AS VARCHAR), CAST(max({e['sql']}) AS VARCHAR) FROM read_parquet(?)",
            params=[args["path"]]).fetchall()[0]
        out[e["key"]] = {"min": lo, "max": hi}
    return {"duckdb": duckdb.__version__, "bounds": out}


COMMANDS = {
    "probe": cmd_probe,
    "read_parquet_variant": cmd_read_parquet_variant,
    "write_parquet_variant": cmd_write_parquet_variant,
    "variant_path_bounds": cmd_variant_path_bounds,
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
