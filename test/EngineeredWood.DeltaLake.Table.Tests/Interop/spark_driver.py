#!/usr/bin/env python3
# Copyright (c) Curt Hagenlocher. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.
"""PySpark + delta-spark interop driver for the EngineeredWood Delta test suite.

Invoked as:  python spark_driver.py <command> <json-args-file> [json-result-file]
Writes a single JSON object to the result file (or stdout if omitted), same contract as
delta_rs_driver.py.

The result must go to a FILE, not stdout: Spark's JVM writes "SUCCESS: The process with PID ...
has been terminated." directly to the inherited stdout descriptor on shutdown, which no
Python-level redirection can prevent, and that trailing text corrupts the JSON.

This is tier 3: the reference implementation. It is the only tier that exercises writer
features, DESCRIBE DETAIL, clustering and OPTIMIZE, and the only one that reads
column-mapped tables written with the legacy minReader=2/minWriter=5 numbering.

COST: starting a JVM and a SparkSession costs roughly 15-20 seconds. Two ways to run:

  one-shot:  spark_driver.py <command> <args-file> [result-file]
  serve:     spark_driver.py serve

`serve` keeps ONE SparkSession alive and processes commands from stdin, which is what the test
suite uses -- it turns a per-test 15-20s startup into a single one for the whole run. One-shot
remains for manual debugging.

Even so, commands are coarse-grained on purpose: each does a whole scenario rather than a
single operation. Do not split a command into two just to make it read nicer.

SERVE PROTOCOL. One request per stdin line: `<command> <args-file> <result-file>`. The result is
written to `<result-file>.tmp` and then atomically renamed, so the file never exists in a partial
state; a `__EW_DONE__<name>` marker then goes to the real stdout so the caller can stop waiting
without polling. Anything Spark prints to stdout is noise the caller scans past -- which is
exactly why the payload travels by file and only the wakeup goes through stdout.
"""
import json
import os
import sys
import traceback

# Spark and py4j print freely; keep anything they emit away from the result channel.
_real_stdout = sys.stdout
sys.stdout = sys.stderr


_SESSION = None


def _spark():
    """The process-wide SparkSession, created on first use.

    Cached because serve mode's entire purpose is to pay JVM + session startup once. Tests are
    isolated by using a distinct table directory each, not by a fresh session -- Delta's caches are
    keyed by path, so a shared session cannot leak state between them.
    """
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("ew-interop")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _SESSION = spark
    return spark


def _shutdown():
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.stop()
        except Exception:
            pass
        _SESSION = None


def _uri(path):
    """Spark wants forward slashes even on Windows."""
    return os.path.abspath(path).replace("\\", "/")


def _rows(df):
    """DataFrame -> sorted list of dicts, for order-independent comparison."""
    rows = [r.asDict(recursive=True) for r in df.collect()]
    return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def _detail(spark, path):
    """DESCRIBE DETAIL as a plain dict -- tier 3's headline capability."""
    row = spark.sql(f"DESCRIBE DETAIL delta.`{_uri(path)}`").collect()[0].asDict(recursive=True)
    return {
        "format": row.get("format"),
        "partition_columns": list(row.get("partitionColumns") or []),
        "clustering_columns": list(row.get("clusteringColumns") or []),
        "num_files": row.get("numFiles"),
        "min_reader_version": row.get("minReaderVersion"),
        "min_writer_version": row.get("minWriterVersion"),
        "table_features": sorted(row.get("tableFeatures") or []),
        "properties": dict(row.get("properties") or {}),
    }


def cmd_probe(args):
    from importlib.metadata import version
    import pyspark
    spark = _spark()
    # `delta` has no __version__; the version lives in the delta-spark dist metadata.
    return {"spark": spark.version, "delta_spark": version("delta-spark"),
            "java_home": os.environ.get("JAVA_HOME"), "pyspark": pyspark.__version__}


def cmd_read(args):
    """Read an EW-written table and report what Spark decoded, plus DESCRIBE DETAIL."""
    spark = _spark()
    df = spark.read.format("delta").load(_uri(args["path"]))
    return {
        "columns": list(df.columns),
        "row_count": df.count(),
        "rows": _rows(df),
        "detail": _detail(spark, args["path"]),
    }


def cmd_write(args):
    """Write a table WITH Spark, for the reference -> EW direction.

    Optional `sql` is a list of statements run against the table afterwards (DELETE, UPDATE,
    OPTIMIZE, ALTER ...), each with {path} substituted -- this is how the writer-feature and
    deletion-vector scenarios are driven without a second session.
    """
    spark = _spark()
    path = _uri(args["path"])
    df = spark.createDataFrame(args["rows"], args["schema"])
    writer = df.write.format("delta")
    for k, v in (args.get("options") or {}).items():
        writer = writer.option(k, v)
    if args.get("partition_by"):
        writer = writer.partitionBy(*args["partition_by"])
    if args.get("cluster_by"):
        writer = writer.clusterBy(*args["cluster_by"])
    writer.mode(args.get("mode", "errorifexists")).save(path)

    for stmt in args.get("sql") or []:
        spark.sql(stmt.format(path=path))

    return {"detail": _detail(spark, args["path"]),
            "rows": _rows(spark.read.format("delta").load(path))}


def cmd_sql(args):
    """Run statements against an existing EW-written table, then report the result.

    The writer-side half of tier 3: Spark MUTATING a table EngineeredWood created is the only
    way to test that EW's output is not merely readable but writable-through.
    """
    spark = _spark()
    path = _uri(args["path"])
    results = []
    for stmt in args["sql"]:
        rendered = stmt.format(path=path)
        out = spark.sql(rendered)
        results.append({"sql": rendered,
                        "rows": _rows(out) if out.columns else []})
    df = spark.read.format("delta").load(path)
    return {"statements": results, "rows": _rows(df), "row_count": df.count(),
            "detail": _detail(spark, args["path"])}


def cmd_create(args):
    """Create an empty table via the DeltaTable builder.

    Needed for generated columns specifically: `CREATE TABLE delta.`path`` rejects GENERATED ALWAYS AS
    with UNSUPPORTED_FEATURE.TABLE_OPERATION, so the builder API is the only path-based way to make one.

    `columns` entries are {name, type, generated_always_as?, nullable?}.
    """
    from delta.tables import DeltaTable

    spark = _spark()
    builder = DeltaTable.create(spark).location(_uri(args["path"]))
    for col in args["columns"]:
        if col.get("generated_always_as"):
            builder = builder.addColumn(
                col["name"], col["type"], generatedAlwaysAs=col["generated_always_as"])
        else:
            builder = builder.addColumn(col["name"], col["type"])
    builder.execute()
    return {"detail": _detail(spark, args["path"])}


def cmd_scan(args):
    """Read under a filter and report BOTH the rows and how many files Spark actually touched.

    Data skipping is the one place where wrong statistics cause silently wrong ANSWERS rather than
    an error: Spark consults each file's min/max in the log and skips files whose range cannot match,
    so bad stats mean missing rows and no complaint from anyone.

    inputFiles() after a filter reflects that skipping, which is what keeps these tests honest. Row
    correctness alone would also pass on an engine that never pruned; asserting files_scanned dropped
    proves the pruning path was genuinely exercised.
    """
    spark = _spark()
    path = _uri(args["path"])
    df = spark.read.format("delta").load(path)
    filtered = df.filter(args["filter"])
    return {
        "files_total": len(df.inputFiles()),
        "files_scanned": len(filtered.inputFiles()),
        "row_count": filtered.count(),
        "rows": _rows(filtered),
    }


COMMANDS = {
    "probe": cmd_probe,
    "read": cmd_read,
    "write": cmd_write,
    "sql": cmd_sql,
    "scan": cmd_scan,
    "create": cmd_create,
}


DONE_MARKER = "__EW_DONE__"


def _emit(out_path, obj):
    payload = json.dumps(obj, ensure_ascii=False, default=str)
    if not out_path:
        _real_stdout.write(payload)
        return

    # Write-then-rename so the caller can treat the file's existence as "complete". A reader that
    # catches the file mid-write would see truncated JSON, and the failure would look like a bug in
    # whatever command happened to be running.
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(payload)
    os.replace(tmp, out_path)


def _run(command, args_path):
    """Execute one command, converting any failure into a result object."""
    try:
        if command not in COMMANDS:
            return {"ok": False, "error": f"unknown command; expected one of {sorted(COMMANDS)}"}
        with open(args_path, "r", encoding="utf-8") as fh:
            args = json.load(fh)
        result = COMMANDS[command](args)
        result.setdefault("ok", True)
        return result
    except Exception as exc:
        # Spark stacks are enormous and the useful part is the innermost Delta/Java message.
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:4000],
                "traceback": traceback.format_exc()[-4000:]}


def _serve():
    """Process commands from stdin against one long-lived SparkSession.

    Never exits on a command failure: a bad command is a result, not a reason to tear down a
    session the remaining tests still need. Only EOF or an explicit `shutdown` stops the loop.
    """
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            if line == "shutdown":
                break

            parts = line.split("\t")
            if len(parts) != 3:
                # Cannot write a result file without knowing its path; report and carry on.
                _real_stdout.write(f"{DONE_MARKER}<malformed>\n")
                _real_stdout.flush()
                continue

            command, args_path, result_path = parts
            _emit(result_path, _run(command, args_path))
            # The result is already durable; this only tells the caller to stop waiting.
            _real_stdout.write(f"{DONE_MARKER}{os.path.basename(result_path)}\n")
            _real_stdout.flush()
    finally:
        _shutdown()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        _serve()
        return 0

    out_path = sys.argv[3] if len(sys.argv) > 3 else None
    args_path = sys.argv[2] if len(sys.argv) > 2 else None
    try:
        if len(sys.argv) < 2 or not args_path:
            _emit(out_path, {"ok": False, "error": "usage: <command> <args-file> [result-file]"})
            return 0
        _emit(out_path, _run(sys.argv[1], args_path))
        return 0
    finally:
        _shutdown()


if __name__ == "__main__":
    sys.exit(main())
