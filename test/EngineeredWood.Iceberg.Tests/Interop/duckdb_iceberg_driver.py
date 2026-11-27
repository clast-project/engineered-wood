#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""DuckDB iceberg_scan driver for the EngineeredWood Iceberg suite.

Invoked as:  python duckdb_iceberg_driver.py <command> <json-args-file> <json-result-file>

The contract of every interop driver (test/Shared/InteropDriver.cs): arguments arrive in a file, one
JSON object goes back in a file, and a failure is reported as {"ok": false, "error": "..."} with exit
code 0, so the C# side asserts on a message rather than on a process crash.

DuckDB is a READER oracle here (doc/iceberg-v3-plan.md, #953 item 6): iceberg_scan reads v3 deletion
vectors, row lineage, nanosecond timestamps and initial defaults straight from a metadata file, with no
catalog. Its writes need a REST catalog and belong to that tier. Commands stay dumb; all judgement lives
in the C# assertions.
"""
import json
import sys
import traceback


def _connect():
    import duckdb

    con = duckdb.connect()
    # INSTALL is a no-op once the extension is in DuckDB's extension directory.
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    return con


def _quote(text):
    return "'" + text.replace("'", "''") + "'"


def _rows(table):
    """A pyarrow table as the iceberg-go driver reports one: columns with their types, then rows of
    JSON-native values. Timestamps and dates are integers in the column's own unit, so nanoseconds
    survive the trip."""
    import pyarrow as pa

    columns = []
    arrays = []
    for field, column in zip(table.schema, table.columns):
        columns.append({"name": field.name, "type": str(field.type)})
        if pa.types.is_timestamp(field.type) or pa.types.is_date32(field.type):
            column = column.cast(pa.int64() if pa.types.is_timestamp(field.type) else pa.int32())
        arrays.append(column.to_pylist())
    return {"columns": columns, "rows": [list(row) for row in zip(*arrays)]}


def cmd_scan(args):
    """Every row of the table whose metadata file is `metadata_location` (a file: URI), as of its
    current snapshot. With `row_lineage`, the v3 metadata columns _row_id and
    _last_updated_sequence_number come last, as they do in the iceberg-go driver's scan."""
    con = _connect()
    source = f"iceberg_scan({_quote(args['metadata_location'])})"
    lineage = ", _row_id, _last_updated_sequence_number" if args.get("row_lineage") else ""
    return _rows(con.sql(f"SELECT *{lineage} FROM {source}").to_arrow_table())


def cmd_version(args):
    import duckdb

    con = _connect()
    ext = con.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'").fetchone()
    return {"v": duckdb.__version__, "iceberg_extension": ext[0] if ext else None}


COMMANDS = {
    "scan": cmd_scan,
    "version": cmd_version,
}


def _emit(out_path, obj):
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False, default=str))


def main():
    out_path = sys.argv[3]
    if sys.argv[1] not in COMMANDS:
        _emit(out_path, {"ok": False, "error": f"unknown command; expected one of {sorted(COMMANDS)}"})
        return 0
    with open(sys.argv[2], "r", encoding="utf-8") as fh:
        args = json.load(fh)

    try:
        result = COMMANDS[sys.argv[1]](args)
        result["ok"] = True
        _emit(out_path, result)
    except Exception as exc:  # noqa: BLE001 - reported, not raised: see the module docstring
        _emit(out_path, {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                         "traceback": traceback.format_exc()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
