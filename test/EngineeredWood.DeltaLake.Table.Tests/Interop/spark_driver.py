#!/usr/bin/env python3
# Copyright (c) clast-project. All rights reserved.
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
import glob
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


def cmd_read_row_ids(args):
    """Read an EW row-tracking table via Delta's generated row-tracking metadata columns.

    Row tracking surfaces each row's stable id / commit version through the _metadata.row_id and
    _metadata.row_commit_version generated columns. For a freshly-appended file (which carries NO
    materialized column) a conformant reader computes row_id = add.baseRowId + physical position; reading
    them here is how EW's add.baseRowId is validated as spec-correct cross-engine, not merely round-tripped.
    """
    spark = _spark()
    id_col = args.get("id_col", "id")
    df = (spark.read.format("delta").load(_uri(args["path"]))
          .selectExpr(id_col,
                      "_metadata.row_id AS row_id",
                      "_metadata.row_commit_version AS row_commit_version"))
    rows = [r.asDict(recursive=True) for r in df.collect()]
    return {
        "rows": sorted(rows, key=lambda r: (r["row_id"] is None, r["row_id"])),
        "row_ids": sorted(r["row_id"] for r in rows),
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


def cmd_partition_paths(args):
    """Write a partitioned table with Spark and report the RAW names it chose, uninterpreted.

    This is the measurement that settles how the REFERENCE implementation encodes a partition
    value, which is two distinct layers: the physical directory (Hive escaping) and the `add.path`
    recorded in the log (a URL-encoding of that directory-relative path). Both are reported
    verbatim -- nothing here decodes, normalises or compares them, because the whole point is to
    capture what Spark actually wrote rather than what we believe it writes.
    """
    spark = _spark()
    path = _uri(args["path"])
    df = spark.createDataFrame(args["rows"], args["schema"])
    (df.write.format("delta")
        .partitionBy(*args["partition_by"])
        .mode(args.get("mode", "errorifexists"))
        .save(path))

    dirs = []
    for root, dirnames, _ in os.walk(args["path"]):
        for d in dirnames:
            if d != "_delta_log":
                dirs.append(
                    os.path.relpath(os.path.join(root, d), args["path"]).replace("\\", "/"))

    add_paths = []
    for commit in sorted(glob.glob(os.path.join(args["path"], "_delta_log", "*.json"))):
        with open(commit, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                action = json.loads(line)
                if "add" in action:
                    add_paths.append(action["add"]["path"])

    return {"directories": sorted(dirs), "add_paths": sorted(add_paths)}


def cmd_v2_checkpoint(args):
    """Write a table whose checkpoints follow the V2 spec, and report the raw log layout.

    `delta.checkpointPolicy=v2` selects the UUID-named checkpoint form and pulls in the
    v2Checkpoint table feature; `delta.checkpointInterval=1` makes every commit checkpoint so the
    scenario does not depend on how many commits it takes to trip the default interval.

    The DELETE is deliberate: it leaves remove tombstones, which a checkpoint must carry and which
    no file-set comparison would notice were missing.

    `top_level_file_format` selects the checkpoint's BODY. PROTOCOL.md defines two --
    `n.checkpoint.u.{json/parquet}` -- and this is a session config rather than anything Spark
    derives from the table, so it is the only way to make the reference implementation emit the
    parquet-bodied form. Default (unset) is json, which is what makes that the form seen in practice.

    Nothing here interprets the log -- the file names, the sidecar listing and the raw
    `_last_checkpoint` bytes are reported as-is, because the point is to see what the reference
    implementation actually wrote.
    """
    spark = _spark()
    path = _uri(args["path"])

    # The session outlives this command -- one JVM serves the whole test run -- so a conf set here
    # would silently change the checkpoint body for every later case. Restored in a finally.
    body_conf = "spark.databricks.delta.checkpointV2.topLevelFileFormat"
    body = args.get("top_level_file_format")
    try:
        if body:
            spark.conf.set(body_conf, body)

        df = spark.createDataFrame(args["rows"], args["schema"])
        writer = (df.write.format("delta")
                  .option("delta.checkpointPolicy", "v2")
                  .option("delta.checkpointInterval", "1"))
        if args.get("partition_by"):
            writer = writer.partitionBy(*args["partition_by"])
        writer.mode("errorifexists").save(path)

        for stmt in args.get("sql") or []:
            spark.sql(stmt.format(path=path))
    finally:
        if body:
            spark.conf.unset(body_conf)

    log_dir = os.path.join(args["path"], "_delta_log")
    log_files = sorted(os.path.basename(p) for p in glob.glob(os.path.join(log_dir, "*"))
                       if os.path.isfile(p))
    sidecars = sorted(os.path.basename(p)
                      for p in glob.glob(os.path.join(log_dir, "_sidecars", "*")))

    last_checkpoint = None
    lc_path = os.path.join(log_dir, "_last_checkpoint")
    if os.path.exists(lc_path):
        with open(lc_path, "r", encoding="utf-8") as handle:
            last_checkpoint = handle.read()

    return {
        "log_files": log_files,
        "sidecars": sidecars,
        "last_checkpoint": last_checkpoint,
        "detail": _detail(spark, args["path"]),
        "rows": _rows(spark.read.format("delta").load(path)),
    }


def cmd_checkpoint_only_read(args):
    """Read an EW-written table with the commits its checkpoint subsumes moved out of the way.

    The mirror of the EW-reads-Spark direction. A round trip through one implementation validates
    that implementation's assumptions, not the format, so this is the only thing that says a
    checkpoint EW wrote is one the reference implementation can actually rebuild state from.

    Two things make it a real test rather than a shape check:

    1. The commits at or below the checkpoint version are RENAMED away first, so a successful read
       came from the checkpoint. Names containing `.checkpoint.` are skipped -- a UUID-named V2
       checkpoint is itself a `.json` file in the log directory whose name starts with the version,
       so without that guard this hides the very file it is trying to test.

    2. Spark's own reconstructed TOMBSTONES are reported, twice -- once against the full log, once
       from the checkpoint -- via `DeltaLog.unsafeVolatileSnapshot.tombstones`. Rows alone cannot
       catch a checkpoint that dropped its unexpired removes: the ACTIVE file set is identical
       either way, and what breaks is VACUUM retention safety and streaming/CDF removal detection.
       Comparing the two lists is what makes the loss visible.

       (VACUUM ... DRY RUN was tried first and is NOT a discriminator: MEASURED 2026-08-08, Spark
       lists a file whose only reference is an unexpired tombstone as vacuumable even with the whole
       log present, so the tombstone changes nothing about that answer.)

    CALLER REQUIREMENT: at least one commit must exist ABOVE the checkpoint version. Spark builds a
    log segment as "a checkpoint plus the commits after it" and fails with "Could not find any delta
    files for version N" when the newest version is the checkpoint's own and its commit is gone. So a
    table meant for this command should be checkpointed and then committed to once more -- which also
    makes the test sharper, since everything at or below the checkpoint can then only have come from
    the checkpoint.
    """
    spark = _spark()
    path = _uri(args["path"])
    log_dir = os.path.join(args["path"], "_delta_log")

    checkpoints = sorted(p for p in glob.glob(os.path.join(log_dir, "*.checkpoint.*"))
                         if os.path.isfile(p))
    if not checkpoints:
        return {"ok": False, "error": "no checkpoint file was written"}
    newest = os.path.basename(checkpoints[-1])
    cp_version = int(newest.split(".")[0])

    def tombstones():
        # Spark caches a DeltaLog per path, so without clearing it the second call would report the
        # first snapshot -- built from a log segment that no longer exists on disk.
        jvm = spark._jvm
        jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
        log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, path)
        # `update()` takes Scala default arguments, which py4j cannot supply; forTable has just
        # loaded a fresh snapshot, so the volatile one IS the current one. Collected as RemoveFile
        # case classes and read through the `path()` accessor -- `select("path")` is varargs in
        # Scala, which py4j cannot call with a single argument.
        removes = log.unsafeVolatileSnapshot().tombstones().collect()
        return sorted(r.path() for r in removes)

    # The control, taken while every commit is still present.
    with_commits = tombstones()

    hidden = []
    for f in sorted(glob.glob(os.path.join(log_dir, "*.json"))):
        base = os.path.basename(f)
        if ".checkpoint." in base:
            continue
        if base[0].isdigit() and int(base.split(".")[0]) <= cp_version:
            os.rename(f, f + ".hidden")
            hidden.append(base)

    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    df = spark.read.format("delta").load(path)
    rows = _rows(df)

    from_checkpoint = tombstones()

    return {
        "checkpoint_file": newest,
        "checkpoint_version": cp_version,
        "sidecars": sorted(os.path.basename(p)
                           for p in glob.glob(os.path.join(log_dir, "_sidecars", "*"))),
        "hidden_commits": hidden,
        "row_count": len(rows),
        "rows": rows,
        # Equal lists mean the checkpoint reproduced the log's remove actions, not just its adds.
        "tombstones_with_commits": with_commits,
        "tombstones_from_checkpoint": from_checkpoint,
        "detail": _detail(spark, args["path"]),
    }


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


def _flatten_schema(dtype, prefix=""):
    """Spark StructType -> {dotted field path: type string}, recursing into structs only.

    Arrays/maps stop at their own entry: the checkpoint schema's nesting that matters here is
    struct-shaped (add.stats_parsed.minValues.<column>), and descending into map entries would bury
    that under noise.
    """
    from pyspark.sql.types import StructType

    out = {}
    for field in dtype.fields:
        path = f"{prefix}{field.name}"
        out[path] = field.dataType.simpleString()
        if isinstance(field.dataType, StructType):
            out.update(_flatten_schema(field.dataType, path + "."))
    return out


def _checkpoint_file(path):
    """The newest classic checkpoint parquet under a table's _delta_log, or None."""
    files = sorted(glob.glob(os.path.join(path, "_delta_log", "*.checkpoint.parquet")))
    return files[-1] if files else None


def _stats_parsed_prefix(schema_paths):
    """Where typed per-file stats live in a checkpoint, as a select-expression prefix."""
    for candidate in ("add.stats_parsed", "stats_parsed"):
        if candidate in schema_paths:
            return candidate
    return None


def cmd_checkpoint_stats(args):
    """Read an EW table THROUGH ITS CHECKPOINT and report both the typed stats and the pruning.

    Two things nothing else covers. First, every commit JSON BELOW the checkpoint version is moved
    aside before Spark opens the table, so those files' adds and statistics can only come from the
    checkpoint -- one whose stats are missing or wrong then prunes a file it should have read and
    silently returns FEWER ROWS. (Strictly below, not at or below: unlike delta-rs, Spark's
    SnapshotManagement insists on a commit file at the version it loads and fails with "Could not find
    any delta files for version N" if the checkpoint's own commit is gone too. Tests therefore put the
    file that must be PRUNED in an early, hidden commit.) Second, the checkpoint parquet is read
    directly (as parquet, not as a log) so the test can assert where typed stats live and what types
    they carry, which is invisible from the table read: Delta prefers the JSON `stats` string when a
    checkpoint carries both.

    `stats_column` selects a column whose per-file typed bounds come back in `typed_stats`.
    """
    spark = _spark()
    path = args["path"]

    cp = _checkpoint_file(path)
    if not cp:
        return {"ok": False, "error": "no checkpoint file was written"}
    cp_version = int(os.path.basename(cp).split(".")[0])

    # Read the checkpoint as a plain parquet file first: this must not go through Delta, or the
    # DeltaLog cache would pin a snapshot built while the commits were still visible.
    cp_df = spark.read.parquet(_uri(cp))
    schema_paths = _flatten_schema(cp_df.schema)
    prefix = _stats_parsed_prefix(schema_paths)

    typed_stats = []
    column = args.get("stats_column")
    if prefix and column:
        # add.stats is absent when the table turns JSON stats off -- selecting it unconditionally
        # would fail on exactly the struct-stats-only shape these tests exist to exercise.
        selects = ["add.path AS path"]
        if "add.stats" in schema_paths:
            selects.append("add.stats AS stats_json")
        selects += [
            f"{prefix}.numRecords AS num_records",
            f"{prefix}.minValues.`{column}` AS min_value",
            f"{prefix}.maxValues.`{column}` AS max_value",
            f"{prefix}.nullCount.`{column}` AS null_count",
        ]
        rows = cp_df.where("add.path is not null").selectExpr(*selects).collect()
        typed_stats = sorted((r.asDict(recursive=True) for r in rows),
                             key=lambda r: r["path"])

    moved = []
    for f in sorted(glob.glob(os.path.join(path, "_delta_log", "*.json"))):
        base = os.path.basename(f)
        if base[0].isdigit() and int(base.split(".")[0]) < cp_version:
            os.rename(f, f + ".hidden")
            moved.append(base)

    df = spark.read.format("delta").load(_uri(path))
    filtered = df.filter(args["filter"]) if args.get("filter") else df
    return {
        "checkpoint_version": cp_version,
        "checkpoint_schema": schema_paths,
        "stats_parsed_at": prefix,
        "typed_stats": typed_stats,
        "hidden_commits": moved,
        "files_total": len(df.inputFiles()),
        "files_scanned": len(filtered.inputFiles()),
        "row_count": filtered.count(),
        "rows": _rows(filtered),
    }


def cmd_reference_checkpoint_schema(args):
    """Write a table WITH SPARK at checkpointInterval=1 and report its checkpoint's schema.

    The reference answer for where typed stats belong and what type each column's bounds carry --
    the spec describes `stats_parsed` loosely enough that the implementation is the specification in
    practice. Tests compare EW's checkpoint against this rather than against a hand-copied layout
    that would silently rot when Delta changes it.

    `stats_as_struct` asks for typed stats via `delta.checkpoint.writeStatsAsStruct` (plus the
    session confs that gate it). Delta writes typed stats INSIDE the add struct, as
    `add.stats_parsed` -- `Checkpoints.buildCheckpoint` folds them into the rebuilt `add` column
    alongside `partitionValues_parsed`. Which build you run decides whether they appear at all:

      delta-spark 4.0.0 (this tier's pinned pairing): never. Its `buildCheckpoint` adds only
        `partitionValues_parsed`; the `extractStats` call landed after the tag, so the flag is a no-op.
      delta-spark 4.1.0: yes, and by DEFAULT -- `checkpoint.writeStatsAsStruct` defaults to true, so
        checkpoints carry `add.stats` and `add.stats_parsed` both (measured).

    The flag is kept so the behaviour is pinned either way.
    """
    spark = _spark()
    path = _uri(args["path"])
    props = ["'delta.checkpointInterval' = '1'"]
    if args.get("stats_as_struct"):
        props.append("'delta.checkpoint.writeStatsAsStruct' = 'true'")
        spark.conf.set("spark.databricks.delta.checkpoint.writeStatsAsStruct", "true")
        spark.conf.set("spark.databricks.delta.statsAsStructInCheckpoint.forcedDisabled", "false")
    spark.sql(
        f"CREATE OR REPLACE TABLE delta.`{path}` "
        "(id BIGINT, amount DECIMAL(9,2), d DATE, ts TIMESTAMP, s STRING, b BOOLEAN) "
        f"USING delta TBLPROPERTIES ({', '.join(props)})")
    spark.sql(
        f"INSERT INTO delta.`{path}` VALUES "
        "(1, 1.50, DATE'2021-06-20', TIMESTAMP'2021-06-20 10:00:00', 'a', true)")

    cp = _checkpoint_file(args["path"])
    if not cp:
        return {"ok": False, "error": "Spark wrote no checkpoint despite checkpointInterval=1"}

    schema_paths = _flatten_schema(spark.read.parquet(_uri(cp)).schema)
    prefix = _stats_parsed_prefix(schema_paths)
    return {
        "checkpoint_file": os.path.basename(cp),
        "table_properties": {r["key"]: r["value"] for r in
                             (row.asDict() for row in
                              spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`").collect())},
        "checkpoint_schema": schema_paths,
        "stats_parsed_at": prefix,
        "stats_parsed_fields": {k: v for k, v in schema_paths.items()
                                if prefix and k.startswith(prefix + ".")},
    }


def cmd_read_changes(args):
    """Read an EW table's Change Data Feed via Spark's readChangeFeed.

    On a column-mapping table the _change_data files (and the data files a no-cdc version infers
    inserts/deletes from) are stored in the PHYSICAL layout -- physical names + field ids, partition
    columns absent. Spark resolves them through the table's column mapping and re-materializes the
    partition columns, so the columns it reports back are the LOGICAL names + partition columns +
    _change_type / _commit_version / _commit_timestamp. That is the cross-engine proof that EW writes
    CDF in the spec layout, not merely that it round-trips through its own reader.
    """
    spark = _spark()
    df = (spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", args["start"])
          .option("endingVersion", args["end"])
          .load(_uri(args["path"])))
    rows = [r.asDict(recursive=True) for r in df.collect()]
    return {
        "columns": list(df.columns),
        "rows": rows,
        "detail": _detail(spark, args["path"]),
    }


def cmd_read_variant(args):
    """Read an EW-written variant table; report each row's variant as canonical JSON via to_json(v).

    to_json forces Spark to actually DECODE the variant value (a malformed value raises
    MALFORMED_VARIANT here), so this validates the bytes, not merely that the column was accepted.
    """
    spark = _spark()
    df = spark.read.format("delta").load(_uri(args["path"]))
    id_col, v_col = args.get("id_col", "id"), args.get("col", "v")
    rows = (df.selectExpr(id_col, f"to_json({v_col}) AS vjson")
              .orderBy(id_col).collect())
    return {"spark": spark.version,
            "rows": [{"id": r[id_col], "vjson": r["vjson"]} for r in rows]}


def cmd_read_parquet_variant(args):
    """Read a RAW parquet file EW wrote (no Delta log) and report its variant column.

    The shredded-write path is a parquet concern, not a table-format one, so this reads the file
    directly. Two things are reported and both matter: the column's Spark type, which is `variant`
    only if Spark recognised the annotated group -- a shredded group has a third child, so a reader
    that keyed off the two-child storage shape would fall back to a struct here -- and to_json(v),
    which forces Spark to DECODE each value out of typed_value plus any residual.
    """
    spark = _spark()
    df = spark.read.parquet(_uri(args["path"]))
    id_col, v_col = args.get("id_col", "id"), args.get("col", "v")
    rows = (df.selectExpr(id_col, f"to_json({v_col}) AS vjson", f"{v_col} IS NULL AS is_null")
              .orderBy(id_col).collect())
    return {
        "spark": spark.version,
        "column_type": df.schema[v_col].dataType.simpleString(),
        "rows": [{"id": r[id_col], "vjson": r["vjson"], "null": r["is_null"]} for r in rows],
    }


def cmd_write_variant(args):
    """Write a variant table WITH Spark (parse_json on JSON literals), for the reference -> EW direction.

    `rows` is a list of {id, json} where json is a JSON text (or null for a SQL-NULL variant). Spark's
    own writer decides the physical layout (Spark 4.1 annotates the group; 4.0 does not).
    """
    spark = _spark()
    path = _uri(args["path"])
    spark.sql(f"CREATE OR REPLACE TABLE delta.`{path}` (id BIGINT, v VARIANT) USING delta")
    values = []
    for r in args["rows"]:
        if r.get("json") is None:
            values.append(f"({r['id']}, NULL)")
        else:
            lit = r["json"].replace("'", "''")
            values.append(f"({r['id']}, parse_json('{lit}'))")
    spark.sql(f"INSERT INTO delta.`{path}` VALUES {', '.join(values)}")
    return {"spark": spark.version, "written": len(values)}


def cmd_write_nested_variant(args):
    """Write a table whose VARIANT is NESTED inside a struct: `s STRUCT<v: VARIANT, tag: STRING>`.

    The Spark 4.1 (GA) reference for the nested EW-read direction: Spark GA both annotates the nested
    variant group in parquet and writes its own child order, so EW's parquet VariantNestedWrapper +
    schema-driven coercion must reconcile a real reference file, not just its own output. `rows` is a
    list of {id, json, tag}; json null -> the nested variant is SQL-NULL.
    """
    spark = _spark()
    path = _uri(args["path"])
    spark.sql(
        f"CREATE OR REPLACE TABLE delta.`{path}` "
        f"(id BIGINT, s STRUCT<v: VARIANT, tag: STRING>) USING delta")
    values = []
    for r in args["rows"]:
        tag = "NULL" if r.get("tag") is None else "'" + r["tag"].replace("'", "''") + "'"
        if r.get("json") is None:
            v = "NULL"
        else:
            v = "parse_json('" + r["json"].replace("'", "''") + "')"
        values.append(f"({r['id']}, named_struct('v', {v}, 'tag', {tag}))")
    spark.sql(f"INSERT INTO delta.`{path}` VALUES {', '.join(values)}")
    return {"spark": spark.version, "written": len(values)}


def cmd_conflict_semantics(args):
    """What Delta's OWN conflict checker does with a declared read, and which isolation levels exist.

    EW asserts two things about Delta that it inferred from reading the source rather than from watching
    it run: that `delta.isolationLevel` distinguishes Serializable from WriteSerializable, and that a
    transaction declaring a whole-table read conflicts with a concurrent commit that touches the table.
    Both feed issue #15's open question 4, and neither had ever been observed.

    Driving this needs the JVM's OptimisticTransaction directly -- there is no SQL for "declare a read and
    then commit something unrelated", because Spark's own statements declare their reads implicitly. So
    this reaches through py4j:

      DeltaLog.forTable(spark, path).startTransaction()  ->  txn.readWholeTable() / txn.filterFiles()
      ... a racer commits by ordinary SQL ...
      txn.commit(Seq(SetTransaction(...)), DeltaOperations.ManualUpdate)

    `readWholeTable()` is the exact analogue of EW's `DeltaTransaction.DeclareWholeTableRead()`, which is
    what makes this a measurement rather than an analogy. The staged action is a SetTransaction because it
    is inert: it commits without touching data, so the verdict is entirely about the declaration.
    """
    spark = _spark()
    jvm = spark._jvm
    base = args["path"]

    # ── which isolation levels does this build accept? ──
    levels = {}
    for level in ("Serializable", "WriteSerializable"):
        uri = _uri(os.path.join(base, "lvl_" + level))
        try:
            spark.sql(f"CREATE TABLE delta.`{uri}` (id BIGINT) USING DELTA "
                      f"TBLPROPERTIES ('delta.isolationLevel'='{level}')")
            levels[level] = "accepted"
        except Exception as e:
            levels[level] = "rejected: " + str(e).splitlines()[0][:200]

    def make_table(name, deletion_vectors):
        uri = _uri(os.path.join(base, name))
        props = ("TBLPROPERTIES ('delta.enableDeletionVectors'='true')" if deletion_vectors else "")
        spark.sql(f"CREATE TABLE delta.`{uri}` (id BIGINT) USING DELTA {props}")
        spark.range(0, 5).selectExpr("id").write.format("delta").mode("append").save(uri)
        spark.range(100, 105).selectExpr("id").write.format("delta").mode("append").save(uri)
        return uri

    def run(scenario):
        uri = make_table(scenario["name"], scenario.get("deletion_vectors", False))

        delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, uri)
        txn = delta_log.startTransaction()
        if scenario["declare"] == "whole_table":
            txn.readWholeTable()
        elif scenario["declare"] == "filter_files":
            txn.filterFiles()

        if scenario["racer"] == "delete":
            spark.sql(f"DELETE FROM delta.`{uri}` WHERE id = 101")
        elif scenario["racer"] == "append":
            spark.range(500, 502).selectExpr("id").write.format("delta").mode("append").save(uri)

        # DeltaOperations.ManualUpdate is a NESTED Scala case object: JVM name
        # DeltaOperations$ManualUpdate$, singleton in MODULE$. Class.forName must use DELTA's own
        # classloader -- delta-spark arrives via ivy in a child loader the py4j gateway cannot see.
        loader = delta_log.getClass().getClassLoader()
        op = (jvm.java.lang.Class
              .forName("org.apache.spark.sql.delta.DeltaOperations$ManualUpdate$", True, loader)
              .getField("MODULE$").get(None))
        staged = jvm.java.util.ArrayList()
        staged.add(jvm.org.apache.spark.sql.delta.actions.SetTransaction(
            "ew-conflict-probe", 1, jvm.scala.Option.apply(None)))
        actions = jvm.org.apache.spark.api.python.PythonUtils.toSeq(staged)

        try:
            txn.commit(actions, op)
            return dict(scenario, verdict="committed")
        except Exception as e:
            text = str(e)
            java_exc = getattr(e, "java_exception", None)
            if java_exc is not None:
                text += " " + java_exc.toString()
            kind = next((m for m in ("ConcurrentDeleteRead", "ConcurrentAppend",
                                     "ConcurrentDeleteDelete", "MetadataChanged", "ProtocolChanged",
                                     "ConcurrentTransaction")
                         if m in text), "unrecognised")
            return dict(scenario, verdict=kind)

    scenarios = [
        {"name": "whole_vs_delete_cow", "declare": "whole_table", "racer": "delete"},
        {"name": "whole_vs_delete_dv", "declare": "whole_table", "racer": "delete",
         "deletion_vectors": True},
        {"name": "whole_vs_blind_append", "declare": "whole_table", "racer": "append"},
        {"name": "filtered_vs_delete_dv", "declare": "filter_files", "racer": "delete",
         "deletion_vectors": True},
        {"name": "undeclared_vs_delete", "declare": "none", "racer": "delete",
         "deletion_vectors": True},
    ]

    # ── domainMetadata: whether two writers of the SAME domain conflict, and the row-tracking
    # exemption (EW issue #109) ──
    #
    # Both transactions have to be JVM transactions: there is no SQL that writes a user domain, so
    # unlike the scenarios above the racer cannot be a plain statement. Order is what makes it a race
    # -- `ours` starts (and so reads) at version N, the racer commits N+1, and ours then collides.
    #
    # THE TABLE MUST DECLARE `domainMetadata`. Delta's checkIfDomainMetadataConflict returns
    # immediately when the protocol lacks the feature, so a table without it reports "committed" for
    # every case here and the measurement silently means nothing.
    def domain_action(jvm_, domain, configuration):
        return jvm_.org.apache.spark.sql.delta.actions.DomainMetadata(domain, configuration, False)

    def run_domain(scenario):
        uri = _uri(os.path.join(base, scenario["name"]))
        spark.sql(f"CREATE TABLE delta.`{uri}` (id BIGINT) USING DELTA "
                  f"TBLPROPERTIES ('delta.feature.domainMetadata'='supported')")
        spark.range(0, 5).selectExpr("id").write.format("delta").mode("append").save(uri)

        delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, uri)
        ours = delta_log.startTransaction()

        loader = delta_log.getClass().getClassLoader()
        op = (jvm.java.lang.Class
              .forName("org.apache.spark.sql.delta.DeltaOperations$ManualUpdate$", True, loader)
              .getField("MODULE$").get(None))

        def commit(txn, domain, configuration):
            staged = jvm.java.util.ArrayList()
            staged.add(domain_action(jvm, domain, configuration))
            txn.commit(jvm.org.apache.spark.api.python.PythonUtils.toSeq(staged), op)

        # The racer takes the version `ours` was aiming at.
        commit(delta_log.startTransaction(), scenario["racer_domain"], scenario["racer_config"])

        try:
            commit(ours, scenario["our_domain"], scenario["our_config"])
            return dict(scenario, verdict="committed")
        except Exception as e:
            text = str(e)
            java_exc = getattr(e, "java_exception", None)
            if java_exc is not None:
                text += " " + java_exc.toString()
            kind = next((m for m in ("ConcurrentTransaction", "MetadataChanged", "ProtocolChanged",
                                     "ConcurrentAppend", "ConcurrentDeleteRead",
                                     "ConcurrentDeleteDelete")
                         if m in text), "unrecognised")
            return dict(scenario, verdict=kind, detail=text[:300])

    domain_scenarios = [
        # Two writers of the same user domain. The verdict EW mirrors as
        # DELTA_DOMAIN_METADATA_CONFLICT.
        {"name": "domain_same", "our_domain": "acme.retention", "our_config": '{"days":30}',
         "racer_domain": "acme.retention", "racer_config": '{"days":7}'},
        # Different domains contest nothing.
        {"name": "domain_different", "our_domain": "acme.retention", "our_config": '{"days":30}',
         "racer_domain": "acme.lineage", "racer_config": '{"v":1}'},
        # The row-tracking high-water mark, which resolveConflict special-cases. This is the one that
        # would hurt if EW had it wrong: every commit adding files to a row-tracking table advances
        # this domain, so a NON-exempt answer here means EW is more permissive than Delta on the most
        # common commit shape such a table produces.
        {"name": "domain_row_tracking",
         "our_domain": "delta.rowTracking", "our_config": '{"rowIdHighWaterMark":20}',
         "racer_domain": "delta.rowTracking", "racer_config": '{"rowIdHighWaterMark":10}'},
    ]

    return {
        "isolation_levels": levels,
        "scenarios": [run(sc) for sc in scenarios],
        "domain_scenarios": [run_domain(sc) for sc in domain_scenarios],
    }


def cmd_expr_oracle(args):
    """Ask Spark what an expression MEANS, for differential-testing the EW parser and registry.

    Phase 9 of the predicate-pushdown design replaces a generated grammar with a hand-written
    one, which is only sound if something independent checks it. This is that something. Three
    answers per expression, deliberately separated because they cost different things:

      parse  -- sessionState.sqlParser.parseExpression, the SAME entry point Delta uses on
                delta.constraints.* and delta.generationExpression (verified in the 4.0.0 jar:
                Constraints$$anonfun$getCheckConstraints$1 calls it directly). `sql` comes back
                fully parenthesised, so diffing it against our own rendering tests precedence
                and associativity with no data and no evaluation.
      type   -- resolved output type against an EMPTY frame carrying `schema`. Resolution and
                type-checking still run, so this measures Spark's coercion rules -- decimal
                promotion above all -- without evaluating a single row.
      eval   -- per-row values over `rows`, which is what catches three-valued logic.

    `parse` and `type` need no data, so a corpus can be harvested from `expressions` alone.

    CONFIG IS PART OF THE ANSWER, not a detail. Delta pins nothing: CheckDeltaInvariant carries
    no SQLConf reference, and a constraint is evaluated under whatever session writes the row --
    measured, `a + b < 0` over (2147483647, 1) is ACCEPTED with ansi off and ARITHMETIC_OVERFLOW
    with it on. So `conf` is echoed back in the result; record it with any expectation derived
    from it, and never compare two corpora gathered under different settings.

    args: {expressions: [str], schema?: [{name, type}], rows?: [[sql-literal]], conf?: {k: v}}

    `rows` entries are SQL literal TEXT, one per schema field, each cast to the declared type --
    see the comment at the construction site for why they are not JSON scalars.
    """
    spark = _spark()

    applied = {}
    for key, value in (args.get("conf") or {}).items():
        applied[key] = value
        spark.conf.set(key, value)
    # Report what is actually in force, not merely what was asked for.
    for key in ("spark.sql.ansi.enabled", "spark.sql.session.timeZone",
                "spark.sql.storeAssignmentPolicy"):
        applied.setdefault(key, spark.conf.get(key))

    schema = args.get("schema") or []
    ddl = ", ".join(f"{f['name']} {f['type']}" for f in schema)
    frame = spark.createDataFrame([], ddl) if ddl else None

    # Rows are SQL literal TEXT, not JSON scalars, and each is cast to its declared type. JSON
    # has no faithful form for a decimal, a timestamp or a struct, so routing them through
    # createDataFrame would need a type-mapping layer that has to grow with every type Spark
    # adds. Spark's own literal syntax already covers all of them:
    #   "CAST('12.34' AS decimal(10,2))", "named_struct('a', array(1,2))", "NULL"
    rows = args.get("rows")
    data = None
    if ddl and rows:
        selects = [
            "SELECT " + ", ".join(
                f"CAST({value} AS {field['type']}) AS {field['name']}"
                for value, field in zip(row, schema))
            for row in rows
        ]
        data = spark.sql(" UNION ALL ".join(selects))

    parser = spark._jsparkSession.sessionState().sqlParser()
    results = []
    for expr in args["expressions"]:
        entry = {"expression": expr}

        try:
            tree = parser.parseExpression(expr)
            entry["parse"] = {"ok": True, "node": tree.getClass().getSimpleName(),
                              "sql": tree.sql()}
        except Exception as exc:
            entry["parse"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:400]}

        if frame is not None:
            try:
                entry["type"] = {"ok": True,
                                 "type": frame.selectExpr(f"({expr}) AS r")
                                              .schema[0].dataType.simpleString()}
            except Exception as exc:
                entry["type"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:400]}

        if data is not None:
            try:
                entry["eval"] = {"ok": True,
                                 "values": [r[0] for r in
                                            data.selectExpr(f"({expr}) AS r").collect()]}
            except Exception as exc:
                entry["eval"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:400]}

        results.append(entry)

    return {"conf": applied, "results": results}


def cmd_blind_append_ground_truth(args):
    """What delta-spark actually records in `commitInfo.isBlindAppend`, per operation shape.

    EW infers blind-append from a commit's ACTIONS -- at least one add and no remove/metaData/protocol.
    Delta does not infer: it records `readPredicates.isEmpty && readFiles.isEmpty` at commit time and
    reads the recorded answer back. The claim in issue #88 is that those two disagree on a real and
    common shape, and that the disagreement is in the UNSAFE direction: a statement that READ the table
    and emitted nothing but adds looks blind to EW and is not.

    This measures the disagreement instead of arguing it. Five commits, each a different shape:

      seed                 INSERT INTO ... VALUES        -- writes nothing it read
      append               DataFrame append              -- the genuine blind append
      merge_insert_only    MERGE ... WHEN NOT MATCHED    -- reads the target to decide what to insert
      insert_select_self   INSERT INTO t SELECT FROM t   -- the dedupe anti-join
      delete               DELETE FROM                   -- reads and removes

    For each it reports what Spark recorded AND whether the commit's file actions are adds only, which
    is what EW's inference would conclude. A row where `only_adds` is true and `is_blind_append` is
    false is a commit EW would exempt and Spark would not.

    Read from the raw commit JSON rather than from DESCRIBE HISTORY: the field this is about is the one
    on disk, and a reader consuming it sees the JSON. The history view is reported alongside so the two
    can be compared if they ever disagree.
    """
    spark = _spark()
    base = args["path"]
    uri = _uri(os.path.join(base, "blind_append"))
    local = os.path.join(base, "blind_append")

    spark.sql(f"CREATE TABLE delta.`{uri}` (id BIGINT, v STRING) USING DELTA")

    labels = {}

    def label(name):
        labels[_latest_version(local)] = name

    spark.sql(f"INSERT INTO delta.`{uri}` VALUES (1, 'a'), (2, 'b')")
    label("seed")

    spark.createDataFrame([(10, "x"), (11, "y")], "id BIGINT, v STRING")         .write.format("delta").mode("append").save(uri)
    label("append")

    # Insert-only MERGE: reads the target to decide what is missing. The canonical case in #88.
    spark.createDataFrame([(2, "b2"), (3, "c")], "id BIGINT, v STRING").createOrReplaceTempView("src")
    spark.sql(
        f"MERGE INTO delta.`{uri}` t USING src s ON t.id = s.id "
        f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)")
    label("merge_insert_only")

    # The dedupe anti-join, in plain SQL: reads the table it appends to.
    spark.sql(
        f"INSERT INTO delta.`{uri}` SELECT id + 1000, v FROM delta.`{uri}` WHERE id < 3")
    label("insert_select_self")

    spark.sql(f"DELETE FROM delta.`{uri}` WHERE id = 1")
    label("delete")

    scenarios = []
    for version, name in sorted(labels.items()):
        commit = os.path.join(local, "_delta_log", "%020d.json" % version)
        info, kinds = None, []
        with open(commit, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                action = json.loads(line)
                for key in action:
                    kinds.append(key)
                    if key == "commitInfo":
                        info = action[key]
        file_kinds = [k for k in kinds if k in ("add", "remove", "cdc")]
        scenarios.append({
            "name": name,
            "version": version,
            "operation": (info or {}).get("operation"),
            "field_present": info is not None and "isBlindAppend" in info,
            "is_blind_append": (info or {}).get("isBlindAppend"),
            "action_kinds": sorted(set(kinds)),
            # What EW's reader-side inference concludes from the shape alone.
            "only_adds": len(file_kinds) > 0 and set(file_kinds) == {"add"},
        })

    history = [
        {"version": row["version"], "operation": row["operation"],
         "isBlindAppend": row["isBlindAppend"]}
        for row in (r.asDict() for r in spark.sql(
            f"DESCRIBE HISTORY delta.`{uri}`").select(
                "version", "operation", "isBlindAppend").collect())
    ]

    return {"scenarios": scenarios, "history": sorted(history, key=lambda r: r["version"])}


def _latest_version(local_path):
    """The newest committed version, from the log directory."""
    versions = []
    for entry in glob.glob(os.path.join(local_path, "_delta_log", "*.json")):
        stem = os.path.basename(entry)[:-len(".json")]
        if stem.isdigit():
            versions.append(int(stem))
    return max(versions)


COMMANDS = {
    "probe": cmd_probe,
    "expr_oracle": cmd_expr_oracle,
    "read": cmd_read,
    "read_row_ids": cmd_read_row_ids,
    "read_changes": cmd_read_changes,
    "read_variant": cmd_read_variant,
    "read_parquet_variant": cmd_read_parquet_variant,
    "write_variant": cmd_write_variant,
    "write_nested_variant": cmd_write_nested_variant,
    "write": cmd_write,
    "partition_paths": cmd_partition_paths,
    "v2_checkpoint": cmd_v2_checkpoint,
    "checkpoint_only_read": cmd_checkpoint_only_read,
    "sql": cmd_sql,
    "scan": cmd_scan,
    "checkpoint_stats": cmd_checkpoint_stats,
    "reference_checkpoint_schema": cmd_reference_checkpoint_schema,
    "create": cmd_create,
    "conflict_semantics": cmd_conflict_semantics,
    "blind_append_ground_truth": cmd_blind_append_ground_truth,
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
