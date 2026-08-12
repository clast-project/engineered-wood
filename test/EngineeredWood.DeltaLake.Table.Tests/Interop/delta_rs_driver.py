#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
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

    Reports the reconstructed state (version, file list, add-action count) and, separately, the
    materialized rows -- because those go through different layers with different limits. See below.

    Finds a checkpoint of ANY naming scheme. Globbing `*.checkpoint.parquet` matched only the
    classic name, which a `<n>.checkpoint.<uuid>.json` never does -- so every checkpoint assertion
    reached through here silently applied to V1 only, and a V2 table would have reported "no
    checkpoint file was written" rather than testing anything.

    The `.checkpoint.` guard in the hide loop is not cosmetic either: a UUID-named V2 checkpoint IS
    a `.json` file in the log directory whose name starts with the version, so without it the loop
    hides the very checkpoint the test is trying to read from.
    """
    from deltalake import DeltaTable
    path = args["path"]
    logdir = os.path.join(path, "_delta_log")

    checkpoints = sorted(
        p for p in glob.glob(os.path.join(logdir, "*.checkpoint.*"))
        if os.path.isfile(p))
    if not checkpoints:
        return {"ok": False, "error": "no checkpoint file was written"}
    newest = os.path.basename(checkpoints[-1])
    cp_version = int(newest.split(".")[0])

    moved = []
    for f in sorted(glob.glob(os.path.join(logdir, "*.json"))):
        base = os.path.basename(f)
        if ".checkpoint." in base:
            continue
        if base[0].isdigit() and int(base.split(".")[0]) <= cp_version:
            os.rename(f, f + ".hidden")
            moved.append(base)

    dt = DeltaTable(path)

    # State reconstruction, which is the Rust engine (delta-kernel-rs) and works for every checkpoint
    # form delta-rs understands.
    result = {
        "checkpoint_version": cp_version,
        "checkpoint_file": newest,
        "sidecars": sorted(os.path.basename(p)
                           for p in glob.glob(os.path.join(logdir, "_sidecars", "*"))),
        "hidden_commits": moved,
        "version": dt.version(),
        "file_names": sorted(u.replace("\\", "/").split("/")[-1] for u in dt.file_uris()),
        "num_add_actions": dt.get_add_actions().num_rows,
    }

    # Materializing the DATA is a separate matter: `to_pyarrow_dataset` carries its own reader-feature
    # allowlist in the PYTHON layer -- `SUPPORTED_READER_FEATURES` in deltalake/table.py, which as of
    # 1.6.2 is {timestampNtz, variantType, variantType-preview} and so excludes deletionVectors and
    # columnMapping as well. Reported rather than raised, so a table it declines still yields the
    # state-level answers above instead of failing the whole command.
    try:
        tbl = dt.to_pyarrow_table()
        result["row_count"] = tbl.num_rows
        result["rows"] = _rows(tbl)
    except Exception as exc:  # noqa: BLE001 - the message is the finding
        result["rows_error"] = "{0}: {1}".format(type(exc).__name__, exc)

    return result


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


def cmd_blind_append_ground_truth(args):
    """Per commit shape on a CDF table: what delta-rs declares, and what it emits.

    The C# side asserts on the pair. Deliberately reports the raw shape (which action
    kinds appear, whether isBlindAppend was written at all) rather than a verdict.
    """
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    base = pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                     "val": pa.array(["a", "b", "c"])})

    def fresh(name):
        path = os.path.join(args["path"], name)
        write_deltalake(path, base, mode="overwrite",
                        configuration={"delta.enableChangeDataFeed": "true"})
        return path

    def append(path):
        write_deltalake(path, pa.table({"id": pa.array([4], pa.int64()),
                                        "val": pa.array(["d"])}), mode="append")

    def update(path):
        DeltaTable(path).update(updates={"val": "'X'"}, predicate="id = 2")

    def delete(path):
        DeltaTable(path).delete(predicate="id = 3")

    def merge_insert_only(path):
        src = pa.table({"id": pa.array([99], pa.int64()), "val": pa.array(["z"])})
        (DeltaTable(path).merge(source=src, predicate="t.id = s.id",
                                source_alias="s", target_alias="t")
         .when_not_matched_insert_all().execute())

    def merge_matched_update(path):
        src = pa.table({"id": pa.array([2], pa.int64()), "val": pa.array(["Y"])})
        (DeltaTable(path).merge(source=src, predicate="t.id = s.id",
                                source_alias="s", target_alias="t")
         .when_matched_update_all().execute())

    scenarios = []
    for name, run in [("append", append),
                      ("update", update),
                      ("delete", delete),
                      ("merge_insert_only", merge_insert_only),
                      ("merge_matched_update", merge_matched_update)]:
        path = fresh(name)
        run(path)
        # The LAST commit is the operation under test; earlier ones are table setup.
        log = _raw_log_actions(path)
        last = max(int(a["version"]) for a in log)
        actions = [a["action"] for a in log if int(a["version"]) == last]
        kinds = sorted({next(iter(a)) for a in actions})
        info = next((a["commitInfo"] for a in actions if "commitInfo" in a), {})
        scenarios.append({
            "name": name,
            "operation": info.get("operation"),
            "field_present": "isBlindAppend" in info,
            "is_blind_append": info.get("isBlindAppend"),
            "action_kinds": kinds,
            "only_adds": kinds == ["add"] or kinds == ["add", "commitInfo"],
            "has_cdc": "cdc" in kinds,
            "has_remove": "remove" in kinds,
        })

    import deltalake
    return {"deltalake": deltalake.__version__, "scenarios": scenarios}


def cmd_read_epoch_micros(args):
    """Read a table and report column `col` as exact microseconds since the Unix epoch.

    Used for timestamp PARTITION columns, whose value delta-rs reconstructs by parsing the string in
    `add.partitionValues` -- so this is the oracle for how EW formatted that string.

    Integers rather than rendered datetimes: a formatted comparison can agree while the underlying
    instant is wrong (a truncating format hides exactly the sub-second digits at issue here), and
    Python's datetime is microsecond-resolution, which is precisely Delta's timestamp precision.
    """
    import datetime
    from deltalake import DeltaTable

    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

    def micros(v):
        if v is None:
            return None
        # A timestamp_ntz comes back naive; it is already UTC-normalized, so label it rather than convert.
        if v.tzinfo is None:
            v = v.replace(tzinfo=datetime.timezone.utc)
        d = v - epoch
        return d.days * 86_400_000_000 + d.seconds * 1_000_000 + d.microseconds

    dt = DeltaTable(args["path"])
    tbl = dt.to_pyarrow_table()
    col = tbl.column(args["col"]).to_pylist()
    ids = tbl.column(args.get("id_col", "id")).to_pylist()
    rows = [{"id": i, "micros": micros(v)} for i, v in zip(ids, col)]
    rows.sort(key=lambda r: (r["id"] is None, r["id"]))
    return {
        "version": dt.version(),
        "row_count": tbl.num_rows,
        "type": str(tbl.schema.field(args["col"]).type),
        "partition_columns": list(dt.metadata().partition_columns),
        "rows": rows,
    }


def cmd_read_variant(args):
    """Read a table EW wrote whose column `col` is a VARIANT, reporting the raw variant bytes.

    delta-rs has no Variant type — it surfaces the column as the physical struct<value, metadata>
    (whatever child order the file used). We resolve the two binaries BY NAME and hex-encode them, so
    the C# side can assert the exact bytes regardless of ordering, and can confirm the column read at
    all (an unannotated variant that delta-rs failed to open would raise here, not silently degrade).
    """
    from deltalake import DeltaTable

    dt = DeltaTable(args["path"])
    tbl = dt.to_pyarrow_table()
    col = tbl.column(args["col"]).to_pylist()
    idcol = tbl.column(args.get("id_col", "id")).to_pylist()
    rows = []
    for ident, cell in zip(idcol, col):
        if cell is None:
            rows.append({"id": ident, "null": True})
        else:
            rows.append({
                "id": ident,
                "null": False,
                "value": cell["value"].hex(),
                "metadata": cell["metadata"].hex(),
            })
    rows.sort(key=lambda r: r["id"])
    return {"version": dt.version(), "row_count": tbl.num_rows, "rows": rows}


def cmd_parquet_variant_layout(args):
    """Report a parquet file's PHYSICAL variant layout, as parquet-cpp sees it.

    This tier's environment carries pyarrow (deltalake depends on it), used here as an independent
    implementation of the parquet spec rather than as a Delta reader -- neither Spark nor DuckDB will
    show what is actually on disk, because both materialise the logical value, so a layout only EW
    knew how to read would pass unnoticed there. The shredding spec is a layout contract: `metadata`
    required, `value` nullable and NULL wherever a row shredded cleanly, and one value/typed_value
    pair per hoisted field under `typed_value`.

    Nullability comes from the ARROW schema and physical types from the parquet leaves, because
    pyarrow's ColumnSchema exposes no repetition field of its own.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(args["path"])
    col = args.get("col", "v")

    schema = pf.schema
    leaves = {}
    for i in range(len(schema)):
        c = schema.column(i)
        leaves[c.path] = {
            "physical_type": c.physical_type,
            "logical_type": str(c.logical_type),
            "max_definition_level": c.max_definition_level,
            "max_repetition_level": c.max_repetition_level,
        }

    nullable = {}

    def walk(field, prefix):
        path = f"{prefix}.{field.name}" if prefix else field.name
        nullable[path] = bool(field.nullable)
        if pa.types.is_struct(field.type):
            for child in field.type:
                walk(child, path)

    for field in pf.schema_arrow:
        walk(field, "")

    table = pf.read()
    rows = []
    for cell in table.column(col).to_pylist():
        if cell is None:
            rows.append({"null": True})
        else:
            rows.append({
                "null": False,
                "has_metadata": cell.get("metadata") is not None,
                "residual_value_null": cell.get("value") is None,
                "has_typed_value": cell.get("typed_value") is not None,
            })

    return {
        "num_rows": pf.metadata.num_rows,
        "num_row_groups": pf.metadata.num_row_groups,
        "arrow_type": str(pf.schema_arrow.field(col).type),
        "schema_text": str(schema),
        "leaves": leaves,
        "nullable": nullable,
        "rows": rows,
    }


COMMANDS = {
    "probe": cmd_probe,
    "read": cmd_read,
    "read_epoch_micros": cmd_read_epoch_micros,
    "read_variant": cmd_read_variant,
    "parquet_variant_layout": cmd_parquet_variant_layout,
    "describe": cmd_describe,
    "checkpoint_only_read": cmd_checkpoint_only_read,
    "raw_log": cmd_raw_log,
    "add_stats": cmd_add_stats,
    "write": cmd_write,
    "blind_append_ground_truth": cmd_blind_append_ground_truth,
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
