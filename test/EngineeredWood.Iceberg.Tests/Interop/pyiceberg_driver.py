#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""pyiceberg driver for the EngineeredWood Iceberg suite.

Invoked as:  python pyiceberg_driver.py <command> <json-args-file> <json-result-file>

The contract of every interop driver (test/Shared/InteropDriver.cs): arguments arrive in a file, one
JSON object goes back in a file, and a failure is reported as {"ok": false, "error": "..."} with exit
code 0, so the C# side asserts on a message rather than on a process crash.

pyiceberg has two roles (doc/iceberg-v3-plan.md, #953 item 6). It READS v3 tables, deletion vectors
included, and it WRITES v2 tables, which makes it the v2 writer oracle: it cannot write v3. Tables live
in a SQL catalog on sqlite, as the plan prescribes; a table another engine wrote is registered into a
throwaway catalog by its metadata file.

MEASURED on pyiceberg 0.12.0, which these commands are written against:
- The default PyArrowFileIO cannot open a `file:///C:/...` location on Windows: it passes
  `/C:/...` to the OS (WinError 123). Every catalog here uses FsspecFileIO, which reads and writes the
  same locations correctly on Windows and elsewhere.
- A scan cannot select `_row_id` or `_last_updated_sequence_number` ("Could not find column"), so this
  tier does not check row lineage.
- A scan of a table with equality deletes refuses ("PyIceberg does not yet support equality deletes").
"""
import json
import os
import shutil
import sys
import tempfile
import traceback

FILE_IO = "pyiceberg.io.fsspec.FsspecFileIO"


def _uri(path):
    """A local directory as the file: URI pyiceberg's warehouse and EW's resolver both take."""
    path = os.path.abspath(path).replace("\\", "/")
    return "file://" + path if path.startswith("/") else "file:///" + path


def _catalog(directory):
    from pyiceberg.catalog.sql import SqlCatalog

    os.makedirs(directory, exist_ok=True)
    db = os.path.join(directory, "catalog.db").replace("\\", "/")
    return SqlCatalog("ew", uri=f"sqlite:///{db}", warehouse=_uri(directory), **{"py-io-impl": FILE_IO})


class _Registered:
    """A table another engine wrote, registered by metadata location into a catalog that is thrown away
    afterwards. Registering reads nothing but the metadata file; the table's files stay where they are."""

    def __init__(self, metadata_location):
        self._dir = tempfile.mkdtemp(prefix="ew_pyiceberg_")
        self._location = metadata_location

    def __enter__(self):
        catalog = _catalog(self._dir)
        catalog.create_namespace("ew")
        return catalog.register_table(("ew", "t"), self._location)

    def __exit__(self, *exc):
        shutil.rmtree(self._dir, ignore_errors=True)


def _rows(table):
    """A pyarrow table as the iceberg-go driver reports one: columns with their types, then rows of
    JSON-native values. Timestamps and dates are integers in the column's own unit, so nanoseconds
    survive the trip (pyarrow will not turn a nanosecond timestamp into a Python datetime)."""
    import pyarrow as pa

    columns = []
    arrays = []
    for field, column in zip(table.schema, table.columns):
        columns.append({"name": field.name, "type": str(field.type)})
        if pa.types.is_timestamp(field.type):
            column = column.cast(pa.int64())
        elif pa.types.is_date32(field.type):
            column = column.cast(pa.int32())
        arrays.append(column.to_pylist())
    return {"columns": columns, "rows": [list(row) for row in zip(*arrays)]}


def cmd_scan(args):
    """Every row of the table whose metadata file is `metadata_location`, as of its current snapshot."""
    with _Registered(args["metadata_location"]) as table:
        return _rows(table.scan().to_arrow())


def cmd_plan(args):
    """pyiceberg's scan plan: each data file it reads and the delete files it applies to it."""
    with _Registered(args["metadata_location"]) as table:
        tasks = [
            {
                "file": task.file.file_path,
                "record_count": task.file.record_count,
                "deletes": sorted(d.file_path for d in task.delete_files),
            }
            for task in table.scan().plan_files()
        ]
    return {"tasks": tasks}


def cmd_describe(args):
    """pyiceberg's view of a table's metadata file."""
    with _Registered(args["metadata_location"]) as table:
        md = table.metadata
        return {
            "format_version": md.format_version,
            "table_uuid": str(md.table_uuid),
            "location": md.location,
            "last_sequence_number": md.last_sequence_number,
            "current_snapshot_id": md.current_snapshot_id,
            "current_schema_id": md.current_schema_id,
            "default_spec_id": md.default_spec_id,
            "last_column_id": md.last_column_id,
            "snapshots": [
                {
                    "snapshot_id": s.snapshot_id,
                    "parent_id": s.parent_snapshot_id,
                    "sequence_number": s.sequence_number,
                    "manifest_list": s.manifest_list,
                    "operation": s.summary.operation.value if s.summary else None,
                }
                for s in md.snapshots
            ],
        }


# --- v2 writes -------------------------------------------------------------------------------------

def _arrow_schema(with_category):
    import pyarrow as pa

    fields = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
    ]
    if with_category:
        fields.append(pa.field("category", pa.string()))
    return pa.schema(fields)


def _batch(ids, with_category=False):
    import pyarrow as pa

    # 2023-11-18T00:00:00Z plus a day per id, and 123 microseconds, as the iceberg-go scenarios do in
    # nanoseconds.
    columns = {
        "id": pa.array(ids, pa.int64()),
        "name": [f"n{i}" for i in ids],
        "ts": pa.array([1_700_265_600_000_123 + i * 86_400_000_000 for i in ids], pa.timestamp("us", tz="UTC")),
    }
    if with_category:
        columns["category"] = ["even" if i % 2 == 0 else "odd" for i in ids]
    return pa.table(columns, schema=_arrow_schema(with_category))


def _scenario_v2_cow(catalog):
    table = catalog.create_table(("ew", "v2_cow"), schema=_arrow_schema(False), properties={"format-version": "2"})
    table.append(_batch([0, 1, 2, 3, 4]))
    table.append(_batch([5, 6, 7, 8, 9]))
    table.delete("id IN (1, 7)")
    return ["create v2", "append ids 0..4", "append ids 5..9", "delete id in (1, 7) (copy-on-write)"]


def _scenario_v2_part(catalog):
    from pyiceberg.transforms import IdentityTransform

    table = catalog.create_table(("ew", "v2_part"), schema=_arrow_schema(True), properties={"format-version": "2"})
    with table.update_spec() as spec:
        spec.add_field("category", IdentityTransform(), "category")
    table.append(_batch(list(range(8)), with_category=True))
    table.delete("id = 5")
    return ["create v2", "partition by identity(category)", "append ids 0..7", "delete id = 5 (copy-on-write)"]


def _scenario_v2_evolve(catalog):
    import pyarrow as pa
    from pyiceberg.types import IntegerType

    table = catalog.create_table(("ew", "v2_evolve"), schema=_arrow_schema(False), properties={"format-version": "2"})
    table.append(_batch([0, 1, 2]))
    with table.update_schema() as update:
        update.add_column("score", IntegerType())
    later = _batch([3, 4])
    later = later.append_column(pa.field("score", pa.int32()), pa.array([30, 40], pa.int32()))
    table.append(later)
    return ["create v2", "append ids 0..2", "add column score int", "append ids 3..4 with score"]


SCENARIOS = {
    "v2_cow": _scenario_v2_cow,
    "v2_part": _scenario_v2_part,
    "v2_evolve": _scenario_v2_evolve,
}


def cmd_scenario(args):
    """Writes one v2 scenario table into a SQL catalog under `warehouse` and returns its newest metadata
    file's location."""
    catalog = _catalog(args["warehouse"])
    catalog.create_namespace_if_not_exists("ew")
    name = args["scenario"]
    steps = SCENARIOS[name](catalog)
    table = catalog.load_table(("ew", name))
    return {"metadata_location": table.metadata_location, "table_location": table.location(), "steps": steps}


def cmd_version(args):
    import pyiceberg

    return {"v": pyiceberg.__version__}


COMMANDS = {
    "describe": cmd_describe,
    "plan": cmd_plan,
    "scan": cmd_scan,
    "scenario": cmd_scenario,
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
