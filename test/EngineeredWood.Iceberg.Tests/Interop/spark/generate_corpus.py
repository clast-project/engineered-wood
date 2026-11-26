# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

"""Generates the committed Spark Iceberg corpus: TestData/corpus/spark.

Spark 4.1 + iceberg-spark-runtime-4.1_2.13:1.12.0 is the only engine that writes all of Iceberg v3, so its
tables are the reference a reader is held to. Each table is written into a Hadoop catalog under a neutral
warehouse, file:/tmp/ew-iceberg-corpus/spark/warehouse (C:\\tmp\\... on Windows), so no machine-specific
path lands in the committed metadata. Tests map that prefix onto the copy in TestData.

Next to the warehouse, expected/<table>.json records what Spark itself reads back from each table: rows
per ref, the v3 lineage columns, the `files` metadata table (bounds raw and as readable_metrics),
snapshots and refs. These are the answers EngineeredWood is checked against.

Deliberately absent: timestamp_ns / timestamptz_ns columns (Spark refuses to read them; the iceberg-go
corpus carries them) and the time type (Spark has none).

Run (Windows; JAVA_HOME = a JDK 17, HADOOP_HOME = a dir whose bin holds winutils.exe and hadoop.dll):

    python generate_corpus.py --jar <iceberg-spark-runtime-4.1_2.13-1.12.0.jar> --out <TestData/corpus/spark>

The interpreter must carry pyspark 4.1.x. The output directory is replaced.
"""

import argparse
import json
import os
import shutil
import sys
import traceback

WAREHOUSE_DIR = "/tmp/ew-iceberg-corpus/spark/wh"
WAREHOUSE_URI = "file:" + WAREHOUSE_DIR
NS = "db"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
NTZ_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

spark = None
jvm = None


CORPUS_USER = "ew-corpus"


def session(jar):
    from pyspark.sql import SparkSession

    # Spark stamps the OS user into every table as the "owner" property; keep it neutral.
    os.environ["HADOOP_USER_NAME"] = CORPUS_USER
    os.environ["SPARK_USER"] = CORPUS_USER
    return (
        SparkSession.builder.master("local[1]")
        .appName("ew-iceberg-corpus")
        .config("spark.jars", jar)
        # On the driver's own classpath too, so py4j's gateway sees the Iceberg classes (an ivy
        # package lands in a child loader it cannot reach).
        .config("spark.driver.extraClassPath", jar)
        .config("spark.driver.extraJavaOptions", f"-Duser.name={CORPUS_USER}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.c", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.c.type", "hadoop")
        .config("spark.sql.catalog.c.warehouse", WAREHOUSE_URI)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def sql(text):
    return spark.sql(text)


def fq(name):
    return f"c.{NS}.{name}"


def java_table(name):
    return jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, fq(name))


def tblprops(version, extra=None):
    props = {"format-version": str(version)}
    props.update(extra or {})
    return ", ".join(f"'{k}' = '{v}'" for k, v in props.items())


MOR = {"write.delete.mode": "merge-on-read", "write.update.mode": "merge-on-read", "write.merge.mode": "merge-on-read"}
COW = {"write.delete.mode": "copy-on-write", "write.update.mode": "copy-on-write", "write.merge.mode": "copy-on-write"}

# Values for the typed columns, keyed by id. Chosen to spread across buckets, truncation widths and
# every temporal unit, and to include negatives, a zero and a non-ASCII string.
TYPED_COLUMNS = (
    "id BIGINT, i INT, l BIGINT, d DECIMAL(10, 2), s STRING, bin BINARY, dt DATE, "
    "ts TIMESTAMP_NTZ, tstz TIMESTAMP, f FLOAT, db DOUBLE, flag BOOLEAN"
)


def typed_rows(start, count):
    rows = []
    for k in range(start, start + count):
        year = 1969 + (k * 7) % 60
        rows.append(
            f"({k}, {(k * 37) % 200 - 100}, {k * 1_000_000_007 - 5_000_000_000}, "
            f"CAST({(k * 1234567) % 2000000 / 100 - 10000:.2f} AS DECIMAL(10, 2)), "
            f"'{['alpha', 'beta', 'gamma', 'delta', 'caf' + chr(0xE9), 'zeta', ''][k % 7]}{k}', "
            f"X'{(k * 2654435761) % (1 << 32):08x}', "
            f"DATE '{year}-{1 + k % 12:02d}-{1 + k % 28:02d}', "
            f"TIMESTAMP_NTZ '{year}-{1 + (k * 5) % 12:02d}-{1 + (k * 3) % 28:02d} {k % 24:02d}:{(k * 7) % 60:02d}:{(k * 11) % 60:02d}.{(k * 123457) % 1000000:06d}', "
            f"TIMESTAMP '{year + 1}-{1 + (k * 7) % 12:02d}-{1 + (k * 5) % 28:02d} {(k * 5) % 24:02d}:{(k * 3) % 60:02d}:{k % 60:02d}.{(k * 654321) % 1000000:06d}+00:00', "
            f"CAST({k * 1.5 - 20} AS FLOAT), {k * -2.25 + 0.125}, {'true' if k % 3 else 'false'})"
        )
    return ", ".join(rows)


def short_partition_names(name):
    """Renames the current spec's partition fields to short forms (i_bucket -> i_b), keeping data paths short."""
    table = java_table(name)
    suffixes = {"_bucket": "_b", "_trunc": "_t", "_year": "_y", "_month": "_m", "_day": "_d", "_hour": "_h"}
    update, changed = table.updateSpec(), False
    for field in table.spec().fields():
        old = field.name()
        for long_suffix, short_suffix in suffixes.items():
            if old.endswith(long_suffix):
                update = update.renameField(old, old[: -len(long_suffix)] + short_suffix)
                changed = True
    if changed:
        update.commit()


# ── Tables ──


def t_bucket():
    sql(f"""CREATE TABLE {fq('t_bucket')} ({TYPED_COLUMNS}) USING iceberg
            PARTITIONED BY (bucket(3, i), bucket(3, l), bucket(3, d), bucket(3, s), bucket(3, bin),
                            bucket(3, dt), bucket(3, ts), bucket(3, tstz))
            TBLPROPERTIES ({tblprops(3)})""")
    short_partition_names("t_bucket")
    sql(f"INSERT INTO {fq('t_bucket')} VALUES {typed_rows(1, 12)}")


def t_truncate():
    sql(f"""CREATE TABLE {fq('t_truncate')} ({TYPED_COLUMNS}) USING iceberg
            PARTITIONED BY (truncate(50, i), truncate(1000000000, l), truncate(100000, d),
                            truncate(3, s), truncate(2, bin))
            TBLPROPERTIES ({tblprops(3)})""")
    short_partition_names("t_truncate")
    sql(f"INSERT INTO {fq('t_truncate')} VALUES {typed_rows(1, 12)}")


def t_temporal():
    """year, month, day and hour on date, timestamp and timestamptz. Two temporal transforms of one source
    in one spec are redundant (Iceberg refuses them), so each spec takes one per source and the table
    evolves through four specs, a batch of rows written under each."""
    t = fq("t_temporal")
    sql(f"""CREATE TABLE {t} ({TYPED_COLUMNS}) USING iceberg
            PARTITIONED BY (years(dt), months(ts), days(tstz))
            TBLPROPERTIES ({tblprops(3)})""")
    short_partition_names("t_temporal")
    sql(f"INSERT INTO {t} VALUES {typed_rows(1, 6)}")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD years(dt) WITH months(dt)")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD months(ts) WITH days(ts)")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD days(tstz) WITH hours(tstz)")
    short_partition_names("t_temporal")
    sql(f"INSERT INTO {t} VALUES {typed_rows(7, 6)}")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD months(dt) WITH days(dt)")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD days(ts) WITH hours(ts)")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD hours(tstz) WITH years(tstz)")
    short_partition_names("t_temporal")
    sql(f"INSERT INTO {t} VALUES {typed_rows(13, 6)}")
    sql(f"ALTER TABLE {t} DROP PARTITION FIELD days(dt)")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD hours(ts) WITH years(ts)")
    sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD years(tstz) WITH months(tstz)")
    short_partition_names("t_temporal")
    sql(f"INSERT INTO {t} VALUES {typed_rows(19, 6)}")


def t_javatypes():
    """uuid, fixed and the void transform: Spark SQL has no DDL for them, so the Java API makes the table."""
    T = jvm.org.apache.iceberg.types.Types
    fields = jvm.java.util.ArrayList()
    fields.add(T.NestedField.required(1, "id", T.LongType.get()))
    fields.add(T.NestedField.optional(2, "u", T.UUIDType.get()))
    fields.add(T.NestedField.optional(3, "fx", T.FixedType.ofLength(4)))
    fields.add(T.NestedField.optional(4, "s", T.StringType.get()))
    schema = jvm.org.apache.iceberg.Schema(fields)
    spec = (jvm.org.apache.iceberg.PartitionSpec.builderFor(schema)
            .bucket("u", 3).bucket("fx", 3).identity("fx").alwaysNull("s", "s_void").build())
    props = jvm.java.util.HashMap()
    props.put("format-version", "3")
    catalog().createTable(identifier("t_javatypes"), schema, spec, props)
    short_partition_names("t_javatypes")
    sql(f"""INSERT INTO {fq('t_javatypes')} VALUES
            (1, 'f79c3e09-677c-4bbd-a479-3f349cb785e7', X'00010203', 'a'),
            (2, '00000000-0000-0000-0000-000000000000', X'ffffffff', 'b'),
            (3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', X'7f000001', NULL),
            (4, '123e4567-e89b-12d3-a456-426614174000', X'00010203', 'd'),
            (5, NULL, NULL, 'e')""")


def dv_multi():
    """Deletion vectors: one DELETE over three files writes three DVs into one Puffin file; a second
    DELETE merges into new DVs and removes the old ones."""
    t = fq("dv_multi")
    sql(f"CREATE TABLE {t} (id BIGINT, s STRING) USING iceberg TBLPROPERTIES ({tblprops(3, MOR)})")
    for start in (0, 100, 200):
        sql(f"INSERT INTO {t} SELECT id, concat('row', id) FROM range({start}, {start + 50})")
    sql(f"DELETE FROM {t} WHERE id % 7 = 0")
    sql(f"DELETE FROM {t} WHERE id % 11 = 0")
    sql(f"UPDATE {t} SET s = 'updated' WHERE id IN (1, 101, 201)")


def v2_posdel():
    t = fq("v2_posdel")
    sql(f"CREATE TABLE {t} (id BIGINT, s STRING, part INT) USING iceberg PARTITIONED BY (part) "
        f"TBLPROPERTIES ({tblprops(2, MOR)})")
    sql(f"INSERT INTO {t} SELECT id, concat('row', id), CAST(id % 2 AS INT) FROM range(0, 40)")
    sql(f"INSERT INTO {t} SELECT id, concat('row', id), CAST(id % 2 AS INT) FROM range(40, 80)")
    sql(f"DELETE FROM {t} WHERE id % 5 = 0")
    sql(f"UPDATE {t} SET s = 'updated' WHERE id % 9 = 0")


def v2_eqdel():
    """Equality deletes. Spark never writes them, so they go through the Java API. Partitioned by `part`:
    an equality delete applies only within its partition, and only to data of a lower sequence number,
    so the row re-inserted afterwards survives."""
    t = fq("v2_eqdel")
    # id is an INT: py4j hands every boxed Long back to Java as an Integer, which a long column refuses.
    sql(f"CREATE TABLE {t} (id INT, s STRING, part INT) USING iceberg PARTITIONED BY (part) "
        f"TBLPROPERTIES ({tblprops(2)})")
    sql(f"INSERT INTO {t} SELECT CAST(id AS INT), concat('row', id), CAST(id % 2 AS INT) FROM range(0, 20)")
    table = java_table("v2_eqdel")
    write_equality_deletes(table, "id", {0: [2, 4, 6, 7], 1: [1, 3, 8]})  # 7 in part 0 and 8 in part 1 match nothing
    sql(f"INSERT INTO {t} VALUES (4, 'reinserted', 0)")
    table.refresh()
    write_equality_deletes(table, "s", {1: ["row11", "row13"]})


def write_equality_deletes(table, column, by_partition):
    schema = table.schema()
    names = jvm.java.util.ArrayList()
    names.add(column)
    delete_schema = schema.select(names)
    field_id = schema.findField(column).fieldId()
    ids = spark.sparkContext._gateway.new_array(jvm.int, 1)
    ids[0] = field_id
    # builderFor is package-private; the nested Builder's constructor is public.
    builder = getattr(jvm.org.apache.iceberg.data, "GenericFileWriterFactory$Builder")(table)
    factory = builder.equalityDeleteRowSchema(delete_schema).equalityFieldIds(ids).build()
    delta = table.newRowDelta()
    for part, values in by_partition.items():
        key = jvm.org.apache.iceberg.PartitionKey(table.spec(), schema)
        row = jvm.org.apache.iceberg.data.GenericRecord.create(schema)
        row.setField("part", jvm.java.lang.Integer.valueOf(part))
        key.partition(jvm.org.apache.iceberg.data.InternalRecordWrapper(schema.asStruct()).wrap(row))
        location = table.locationProvider().newDataLocation(
            table.spec(), key, f"eq-delete-{column}-{part}-{jvm.java.util.UUID.randomUUID().toString()}.parquet")
        out = jvm.org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(location))
        writer = factory.newEqualityDeleteWriter(out, table.spec(), key)
        for v in values:
            rec = jvm.org.apache.iceberg.data.GenericRecord.create(delete_schema)
            rec.setField(column, v)
            writer.write(rec)
        writer.close()
        delta.addDeletes(writer.toDeleteFile())
    delta.commit()


def v2_to_v3():
    """A v2 table with position deletes, upgraded to v3. The first DV on a file must absorb its v2
    position deletes, and the first v3 commit assigns row ids to every carried-over file."""
    t = fq("v2_to_v3")
    sql(f"CREATE TABLE {t} (id BIGINT, s STRING) USING iceberg TBLPROPERTIES ({tblprops(2, MOR)})")
    sql(f"INSERT INTO {t} SELECT id, concat('row', id) FROM range(0, 30)")
    sql(f"INSERT INTO {t} SELECT id, concat('row', id) FROM range(30, 60)")
    sql(f"DELETE FROM {t} WHERE id % 4 = 0")
    sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('format-version' = '3')")
    sql(f"INSERT INTO {t} SELECT id, concat('row', id) FROM range(60, 90)")
    sql(f"DELETE FROM {t} WHERE id % 6 = 1")


def defaults():
    """Columns added with an initial-default and a write-default, of every type that can carry one, read
    for rows written before they existed."""
    t = fq("defaults")
    sql(f"CREATE TABLE {t} (id BIGINT) USING iceberg TBLPROPERTIES ({tblprops(3)})")
    sql(f"INSERT INTO {t} SELECT id FROM range(0, 5)")
    table = java_table("defaults")
    T = jvm.org.apache.iceberg.types.Types
    L = jvm.org.apache.iceberg.expressions.Literal
    BB = jvm.java.nio.ByteBuffer
    columns = [
        ("c_int", T.IntegerType.get(), L.of(jvm.java.lang.Integer.valueOf(34)), L.of(jvm.java.lang.Integer.valueOf(35))),
        ("c_long", T.LongType.get(), L.of(jvm.java.lang.Long.valueOf(-9007199254740993)), None),
        ("c_bool", T.BooleanType.get(), L.of(True), L.of(False)),
        ("c_float", T.FloatType.get(), L.of(jvm.java.lang.Float.valueOf(1.5)), None),
        ("c_double", T.DoubleType.get(), L.of(jvm.java.lang.Double.valueOf(-0.1)), None),
        ("c_decimal", T.DecimalType.of(9, 2), L.of(jvm.java.math.BigDecimal("14.20")), None),
        ("c_string", T.StringType.get(), L.of("caf" + chr(0xE9)), L.of("w")),
        ("c_date", T.DateType.get(), L.of("2017-11-16").to(T.DateType.get()), None),
        ("c_ts", T.TimestampType.withoutZone(), L.of("2017-11-16T22:31:08.123456").to(T.TimestampType.withoutZone()), None),
        ("c_tstz", T.TimestampType.withZone(), L.of("2017-11-16T22:31:08.123456+00:00").to(T.TimestampType.withZone()), None),
        ("c_uuid", T.UUIDType.get(), L.of(jvm.java.util.UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7")), None),
        ("c_fixed", T.FixedType.ofLength(3), L.of(BB.wrap(bytes_array([0, 1, 255]))).to(T.FixedType.ofLength(3)), None),
        ("c_binary", T.BinaryType.get(), L.of(BB.wrap(bytes_array([0xCA, 0xFE]))), None),
    ]
    update = table.updateSchema()
    for name, typ, initial, _ in columns:
        update = update.addColumn(name, typ, "added with a default", initial)
    update.commit()
    table.refresh()
    update = table.updateSchema()
    for name, _, _, write in columns:
        if write is not None:
            update = update.updateColumnDefault(name, write)
    update.commit()
    sql(f"INSERT INTO {t} (id, c_int, c_string) VALUES (100, 1, 'explicit')")


def bytes_array(values):
    # py4j sends a Python bytes object as a Java byte[].
    return bytes(values)


def variant():
    t = fq("variant")
    sql(f"CREATE TABLE {t} (id BIGINT, v VARIANT) USING iceberg TBLPROPERTIES ({tblprops(3)})")
    sql(f"""INSERT INTO {t} VALUES
            (1, parse_json('{{"a": 1, "b": "x", "c": [1, 2, 3]}}')),
            (2, parse_json('{{"a": 2.5, "b": null, "nested": {{"k": true}}}}')),
            (3, parse_json('"a string"')),
            (4, parse_json('12345678901234567890')),
            (5, NULL),
            (6, parse_json('null'))""")


def variant_shred():
    t = fq("variant_shred")
    sql(f"CREATE TABLE {t} (id BIGINT, v VARIANT) USING iceberg "
        f"TBLPROPERTIES ({tblprops(3, {'write.parquet.shred-variants': 'true'})})")
    rows = ", ".join(
        f"({k}, parse_json('{{\"event\": \"e{k % 3}\", \"n\": {k}, \"price\": {k * 1.25}, \"tags\": [\"t{k}\"]}}'))"
        for k in range(1, 21))
    sql(f"INSERT INTO {t} VALUES {rows}, (21, parse_json('{{\"event\": 7, \"other\": true}}')), (22, NULL)")


def branches():
    t = fq("branches")
    sql(f"CREATE TABLE {t} (id BIGINT, s STRING) USING iceberg TBLPROPERTIES ({tblprops(3)})")
    sql(f"INSERT INTO {t} SELECT id, 'main' FROM range(0, 5)")
    sql(f"ALTER TABLE {t} CREATE TAG t1")
    sql(f"ALTER TABLE {t} CREATE BRANCH b1")
    sql(f"INSERT INTO {t}.branch_b1 SELECT id, 'b1' FROM range(100, 103)")
    sql(f"INSERT INTO {t} SELECT id, 'main' FROM range(5, 8)")
    sql(f"DELETE FROM {t}.branch_b1 WHERE id = 1")


def compacted():
    """Rows written in several commits, one updated, then compacted: rewritten files must carry the
    original _row_id and _last_updated_sequence_number physically."""
    t = fq("compacted")
    sql(f"CREATE TABLE {t} (id BIGINT, s STRING) USING iceberg TBLPROPERTIES ({tblprops(3, COW)})")
    for start in (0, 10, 20):
        sql(f"INSERT INTO {t} SELECT id, concat('row', id) FROM range({start}, {start + 10})")
    sql(f"UPDATE {t} SET s = 'updated' WHERE id IN (3, 13)")
    sql(f"CALL c.system.rewrite_data_files(table => '{NS}.compacted', options => map('rewrite-all', 'true'))")
    sql(f"INSERT INTO {t} SELECT id, concat('row', id) FROM range(30, 33)")


TABLES = [
    t_bucket, t_truncate, t_temporal, t_javatypes,
    dv_multi, v2_posdel, v2_eqdel, v2_to_v3, defaults,
    variant, variant_shred, branches, compacted,
]


def catalog():
    return spark._jsparkSession.sessionState().catalogManager().catalog("c").icebergCatalog()


def identifier(name):
    return jvm.org.apache.iceberg.catalog.TableIdentifier.parse(f"{NS}.{name}")


# ── What Spark reads back ──


def rows_json(source, order="id", drop=()):
    from pyspark.sql import functions as F

    df = spark.table(source) if not source.startswith("(") else sql(f"SELECT * FROM {source}")
    df = df.drop(*drop).orderBy(*[c.strip() for c in order.split(",")])
    options = {"timestampFormat": TS_FORMAT, "timestampNTZFormat": NTZ_FORMAT}
    # Non-integral numbers stay the text Spark wrote, so a decimal keeps its scale ("14.20", not 14.2)
    # and a double its exact digits.
    return [json.loads(r.j, parse_float=str) for r in df.select(F.to_json(F.struct("*"), options).alias("j")).collect()]


def try_capture(what, fn):
    try:
        return fn()
    except Exception as e:  # recorded, not fatal: a reader-side gap in Spark is itself an answer
        return {"error": f"{type(e).__name__}: {str(e).splitlines()[0][:400]}", "what": what}


def expected(name):
    t = fq(name)
    table = java_table(name)
    version = int(table.operations().current().formatVersion())
    out = {
        "table": name,
        "format_version": version,
        "metadata_location": table.operations().current().metadataFileLocation(),
        "rows": try_capture("rows", lambda: rows_json(t)),
        "refs": try_capture("refs", lambda: rows_json(f"{t}.refs", order="name")),
        "snapshots": try_capture("snapshots", lambda: rows_json(f"{t}.snapshots", order="committed_at")),
        "files": try_capture("files", lambda: rows_json(f"{t}.files", order="content, file_path")),
        "all_delete_files": try_capture("all_delete_files", lambda: rows_json(f"{t}.all_delete_files", order="file_path")),
        "manifests": try_capture("manifests", lambda: rows_json(f"{t}.manifests", order="path")),
    }
    if "error" in out["files"]:
        # Iceberg 1.12's readable_metrics cannot render a uuid column (ClassCastException in
        # MetricsUtil$ReadableColMetricsStruct). The raw bounds are still there without it.
        out["files_error"] = out["files"]
        out["files"] = try_capture("files", lambda: rows_json(
            f"{t}.files", order="content, file_path", drop=("readable_metrics",)))
    if version >= 3:
        out["lineage"] = try_capture("lineage", lambda: rows_json(
            f"(SELECT id, _row_id, _last_updated_sequence_number FROM {t}) AS q"))
        if "error" in out["lineage"]:
            # Spark 4.1 + Iceberg 1.12 cannot project the lineage columns of a shredded-variant table
            # without the variant column; with it, it can.
            out["lineage"] = try_capture("lineage", lambda: [
                {k: v for k, v in r.items() if k != "v"} for r in rows_json(
                    f"(SELECT id, _row_id, _last_updated_sequence_number, v FROM {t}) AS q")])
    ref_rows = {}
    for ref in table.refs().keySet().toArray():
        if ref == "main":
            continue
        kind = "branch" if table.refs().get(ref).isBranch() else "tag"
        source = f"{t}.{kind}_{ref}"
        ref_rows[ref] = try_capture(f"rows@{ref}", lambda s=source: rows_json(s))
    if ref_rows:
        out["rows_by_ref"] = ref_rows
    # The schema as Java serializes it, defaults included: what initial-default / write-default hold.
    out["schema"] = json.loads(jvm.org.apache.iceberg.SchemaParser.toJson(table.schema()))
    return out


def main():
    global spark, jvm
    parser = argparse.ArgumentParser()
    parser.add_argument("--jar", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()

    local_warehouse = os.path.abspath(WAREHOUSE_DIR)  # C:\tmp\... on Windows
    if os.path.exists(local_warehouse):
        shutil.rmtree(local_warehouse)

    spark = session(args.jar)
    jvm = spark._jvm
    sql(f"CREATE NAMESPACE IF NOT EXISTS c.{NS}")

    results, failures = {}, {}
    for build in TABLES:
        name = build.__name__
        if args.only and name not in args.only:
            continue
        print(f"== {name}", flush=True)
        try:
            build()
            results[name] = expected(name)
        except Exception:
            failures[name] = traceback.format_exc()
            print(failures[name], file=sys.stderr, flush=True)

    out = os.path.abspath(args.out)
    if os.path.exists(out):
        shutil.rmtree(out)
    shutil.copytree(local_warehouse, os.path.join(out, "wh"),
                    ignore=shutil.ignore_patterns("*.crc"))
    os.makedirs(os.path.join(out, "expected"))
    for name, data in results.items():
        with open(os.path.join(out, "expected", f"{name}.json"), "w", encoding="utf-8", newline="\n") as fh:
            json.dump(data, fh, indent=1, ensure_ascii=False, sort_keys=True)
            fh.write("\n")
    with open(os.path.join(out, "corpus.json"), "w", encoding="utf-8", newline="\n") as fh:
        json.dump({
            "generator": "test/EngineeredWood.Iceberg.Tests/Interop/spark/generate_corpus.py",
            "spark_version": spark.version,
            "iceberg_version": jvm.org.apache.iceberg.IcebergBuild.version(),
            "java_version": jvm.java.lang.System.getProperty("java.version"),
            "warehouse_uri": WAREHOUSE_URI,
            "namespace": NS,
            "tables": sorted(results),
            "failed": sorted(failures),
        }, fh, indent=1)
        fh.write("\n")
    spark.stop()
    print(f"wrote {len(results)} tables to {out}; {len(failures)} failed: {sorted(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
