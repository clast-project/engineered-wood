#!/usr/bin/env python3
# Copyright (c) Curt Hagenlocher. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""delta-rs interop driver for the EngineeredWood Delta test suite.

Invoked as:  python delta_rs_driver.py <command> <json-args-file> [json-result-file]

Arguments arrive via a file rather than inline so the command line never contains JSON quoting --
net472 has no ProcessStartInfo.ArgumentList, and hand-quoting embedded quotes for Win32 is a
reliable source of bugs.

Writes a single JSON object to the result file (or stdout if omitted); a file keeps the result
clear of anything a subprocess prints. Any failure is reported as
{"ok": false, "error": "..."} with exit code 0 so the C# side can assert on the
message rather than on a process crash.

Commands are deliberately dumb: they read or write a table and report what
delta-rs actually saw. All judgement lives in the C# assertions.
"""
import glob
import json
import os
import sys
import traceback

MIN_DELTALAKE = (1, 0, 0)


def _rows(tbl):
    """pyarrow Table -> list of dicts, sorted, for order-independent comparison."""
    rows = tbl.to_pylist()
    return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def _raw_log_actions(path):
    """Every action in every commit JSON, in version order.

    Read from the files directly rather than through the API: the whole point of
    several tests is to assert on the ON-DISK encoding, which the API normalizes away.
    """
    out = []
    for f in sorted(glob.glob(os.path.join(path, "_delta_log", "*.json"))):
        version = os.path.basename(f).split(".")[0]
        with open(f, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append({"version": version, "action": json.loads(line)})
    return out


def cmd_probe(args):
    import deltalake
    ver = tuple(int(x) for x in deltalake.__version__.split(".")[:3])
    return {
        "deltalake": deltalake.__version__,
        "supported": ver >= MIN_DELTALAKE,
        "python": sys.version.split()[0],
    }


def cmd_read(args):
    """Read a table EW wrote and report exactly what delta-rs decoded.

    Optional `filters` ([[column, op, value], ...]) exercises the pruning path: delta-rs uses the
    per-file stats in the log to skip files before reading them, so a filtered read is the only way
    to find out whether those stats are TRUE. A read of the whole table never consults them.
    """
    from deltalake import DeltaTable
    dt = DeltaTable(args["path"])
    if "version" in args:
        dt.load_as_version(args["version"])
    filters = [tuple(f) for f in args["filters"]] if args.get("filters") else None
    tbl = dt.to_pyarrow_table(filters=filters)
    return {
        "version": dt.version(),
        "row_count": tbl.num_rows,
        "columns": [f.name for f in tbl.schema],
        "rows": _rows(tbl),
    }


def cmd_add_stats(args):
    """Per-file statistics as delta-rs parses them out of the log.

    Flattened to num_records / `min.<col>` / `max.<col>` / `null_count.<col>` per file. This is the
    direct oracle for EW's stats writer: wrong min/max does not raise anywhere, it just makes a
    foreign engine skip a file it should have read, and the query quietly returns fewer rows.
    """
    import pyarrow as pa
    from deltalake import DeltaTable

    dt = DeltaTable(args["path"])
    # get_add_actions returns an arro3 Table; go through the C stream into pyarrow.
    stats = pa.table(dt.get_add_actions(flatten=True))
    return {"files": stats.to_pylist()}


def cmd_describe(args):
    """Protocol / metadata / raw on-disk paths, without interpreting them."""
    from deltalake import DeltaTable
    path = args["path"]
    dt = DeltaTable(path)
    proto = dt.protocol()
    meta = dt.metadata()

    dirs = []
    for root, dirnames, _ in os.walk(path):
        for d in dirnames:
            if d != "_delta_log":
                dirs.append(os.path.relpath(os.path.join(root, d), path).replace("\\", "/"))

    add_paths = [
        a["action"]["add"]["path"]
        for a in _raw_log_actions(path)
        if "add" in a["action"]
    ]

    return {
        "version": dt.version(),
        "min_reader_version": proto.min_reader_version,
        "min_writer_version": proto.min_writer_version,
        "reader_features": sorted(proto.reader_features or []),
        "writer_features": sorted(proto.writer_features or []),
        "partition_columns": list(meta.partition_columns),
        "configuration": dict(meta.configuration),
        "add_paths": add_paths,
        "directories": sorted(dirs),
    }


def cmd_checkpoint_only_read(args):
    """Force delta-rs to reconstruct state from the checkpoint alone.

    Moves every commit JSON at or below the checkpoint version out of the way, so a
    successful read proves the CHECKPOINT carried the state -- not the JSON commits.
    Renames rather than deletes so a failure leaves the table diagnosable.
    """
    from deltalake import DeltaTable
    path = args["path"]
    logdir = os.path.join(path, "_delta_log")

    checkpoints = sorted(glob.glob(os.path.join(logdir, "*.checkpoint.parquet")))
    if not checkpoints:
        return {"ok": False, "error": "no checkpoint file was written"}
    cp_version = int(os.path.basename(checkpoints[-1]).split(".")[0])

    moved = []
    for f in sorted(glob.glob(os.path.join(logdir, "*.json"))):
        base = os.path.basename(f)
        if base[0].isdigit() and int(base.split(".")[0]) <= cp_version:
            os.rename(f, f + ".hidden")
            moved.append(base)

    dt = DeltaTable(path)
    tbl = dt.to_pyarrow_table()
    return {
        "checkpoint_version": cp_version,
        "hidden_commits": moved,
        "version": dt.version(),
        "row_count": tbl.num_rows,
        "rows": _rows(tbl),
    }


def cmd_raw_log(args):
    """Every log action, parsed from disk WITHOUT constructing a DeltaTable.

    Needed for tables delta-rs declines to open at all (e.g. reader version 2), where the log is
    still worth asserting on even though the kernel will not read the table.
    """
    return {"actions": _raw_log_actions(args["path"])}


def cmd_write(args):
    """Write a table WITH delta-rs, for the tool -> EW direction."""
    import pyarrow as pa
    from deltalake import write_deltalake

    spec = args["columns"]
    arrays, names = {}, []
    for col in spec:
        typ = {
            "int64": pa.int64(),
            "string": pa.string(),
            "double": pa.float64(),
            "bool": pa.bool_(),
        }[col["type"]]
        arrays[col["name"]] = pa.array(col["values"], typ)
        names.append(col["name"])

    tbl = pa.table(arrays)
    write_deltalake(
        args["path"],
        tbl,
        partition_by=args.get("partition_by"),
        mode=args.get("mode", "error"),
    )
    return {"written": tbl.num_rows, "columns": names}


COMMANDS = {
    "probe": cmd_probe,
    "read": cmd_read,
    "describe": cmd_describe,
    "checkpoint_only_read": cmd_checkpoint_only_read,
    "raw_log": cmd_raw_log,
    "add_stats": cmd_add_stats,
    "write": cmd_write,
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
        result.setdefault("ok", True)
    except Exception as exc:  # reported, not raised -- C# asserts on the message
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                  "traceback": traceback.format_exc()}
    _emit(out_path, result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
