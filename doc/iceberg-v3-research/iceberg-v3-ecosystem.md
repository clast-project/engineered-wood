# Iceberg format-version 3: ecosystem status and local oracle availability (2026-11-25)

Scope: (1) where the reference and secondary implementations stand on v3, (2) what is on this
Windows box for a differential v3 oracle, (3) what EW already has and what fixtures exist.

Caveat on sources: release-note summaries below come from the official Iceberg releases page and
GitHub (via `gh`), cross-checked where possible. A few items (Trino, Snowflake, Databricks) come from
vendor docs. Items marked (vendor) or (secondary) are second-hand.

---

## 1. Ecosystem status

### 1.1 iceberg-java (the reference)

Releases ([releases page](https://iceberg.apache.org/releases/)):

| Release | Date | v3 content (headline PRs) |
|---|---|---|
| 1.8.0 | 2025-02-13 | DV read (#11481) + write (#11476, #11561); Variant type defined + encoding (#11324, #11415); row-lineage metadata fields (#11948); default-value reads for Parquet/Avro (#11785/#11786/#11803); UnknownType (#12012) |
| 1.9.0 | 2025-04-28 | Variant moved to API, readers/writers (#12304/#12139/#12323); row lineage enabled for all v3 tables (#12593); nanos + unknown (#12455/#12463); defaults in UpdateSchema (#12211); rewrite v2 deletes to DVs (#12250); geometry/geography types in API (#12346); dropped Spark 3.3 |
| 1.10.0 | 2025-09-11 | The release generally described as "v3 ready" ([Google OSS blog](https://opensource.googleblog.com/2025/09/apache-iceberg-110-maturing-the-v3-spec-the-rest-api-and-google-contributions.html), [Snowflake](https://www.snowflake.com/en/blog/engineering/apache-iceberg-1-10-new-features-fixes/), [Dremio](https://www.dremio.com/blog/whats-new-in-apache-iceberg-1-10-0-and-what-comes-next/)). Spark 4.0 support (#12494); Spark variant read (#13219) using Parquet 1.16's VARIANT annotation (#13941); row lineage in vectorized Arrow reader (#12928) and preserved on compaction (#13555); DV dangling cleanup (#13245); nanos (#13562); unknown type read/write in Spark 4.0 (#13445); encryption table-metadata keys (#12927). Spark `ADD COLUMN ... DEFAULT` explicitly throws (#13464). |
| 1.10.1 | 2025-12-22 | Variant in ParquetTypeVisitor (#14588), unknown during deletes, nanos default overflow |
| 1.10.2 | 2026-05-18 | "Always merge deletion vectors on commit path" (#15654); "Fix row ID assignment for EXISTING entries during manifest merge" (#16304) |
| 1.11.0 | 2026-05-19 | **Spark shredded-variant WRITE** (#14297; properties `write.parquet.shred-variants` / `spark.sql.iceberg.shred-variants`); fix data loss in partial variant shredding (#15087); `_row_id`/`_last_updated_sequence_number` readers incl. ORC (#14148, #15776); row lineage preserved in RewriteDataFiles (#14149); duplicate-DV detect+merge before commit (#15006); Flink DV writes (#14197); KMS types for encryption (#15272); geo bbox types (#12667); **Spark 4.1 support** (#14155); Spark 3.4 deprecated |
| 1.12.0 | 2026-09-29 | **Geo read/write in Parquet (WKB) + Spark 4.1 mapping/DML** (#16982, #16851, #16607); variant shredding "type uniformity" (#17424), vectorized Parquet variant reads; row-lineage fixes ("last updated sequence inheritance" #17039, direct-memory leak in vectorized lineage readers #17296); DV fixes ("delete file references for DVs in the same Puffin file" #17497, colocated DV exposed via DataFile #17928, scans fail when a DV/position delete claims a mismatched partition); nanos predicate pushdown fixes (#16619, #17750); default-value fixes (nested initial-default under null ancestor #17320, initial-default rows dropped when filtering on the defaulted column #16692); encryption (manifest-list keys committed with snapshot #17984, encrypted rewrite_manifests #17987). Removed Spark 3.4 and Flink 2.0; **Java writers no longer write position deletes with row data**. ([Dremio 1.12 guide](https://www.dremio.com/blog/apache-iceberg-1-12-0-whats-new-breaking-changes-and-upgrade-guide)) |

Bottom line: the v3 *spec features* were all present by 1.10.0 at the core level; 1.11 completed
Spark-side shredded-variant writes; 1.12 completed geo end to end (Spark 4.1 only). The 1.10.x-1.12
release notes are dominated by **correctness fixes** in DVs, row lineage, defaults and variant
shredding, so "which Java version" matters for an oracle: pin 1.12.0.

Still-open v3 items in apache/iceberg (via `gh search issues --state open`, 2026-11-25):
- Spark Default-value DDL: Spark 4.1 SQL `DEFAULT` DDL PR #16711 was **closed unmerged**. Defaults are
  settable through the Java `UpdateSchema` API, not Spark SQL. (An oracle that needs defaults must
  call the Java API through py4j, or set them in metadata some other way.)
- Vectorized Spark reads of shredded VARIANT: tracking #18669 (open).
- Row lineage: Spark vectorized ORC reader ignores `_row_id` / `_last_updated_sequence_number` (#18543).
- DVs: "Can't index multiple DVs" permanent scan failure on duplicate DVs (#17206); WAP snapshot
  isolation broken for v3 + DVs (#18713); DV length/offset not validated against malicious input
  (#16475); reading encrypted DVs (#16157); row-granular conflict detection for DV UPDATE/MERGE (#18020);
  MERGE memory growth under streaming upserts (#17241).
- #19055: "Deleted rows reappear ... when binary or fixed partition summaries were written before
  1.11.0" (open; shows that pre-1.11 metadata has latent issues).
- Spark 4.2: no Iceberg runtime published yet; tracking #17959 (Dremio says 4.2 is "included in
  source releases" of 1.12, but Maven Central has no `iceberg-spark-runtime-4.2_2.13`
  ([Maven dir listing](https://repo1.maven.org/maven2/org/apache/iceberg/))).

### 1.2 Spark integration

Published runtimes on Maven Central: `iceberg-spark-runtime-{3.5_2.12, 3.5_2.13, 4.0_2.13, 4.1_2.13}`.
`iceberg-spark-runtime-4.1_2.13` has exactly two versions, 1.11.0 and 1.12.0
([listing](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/)).
1.12.0 supports Spark 3.5.9, 4.0.4, 4.1.3 (Dremio).

Can Spark write v3 with DVs and variant? **Yes**:
- DVs: any `format-version=3` table in merge-on-read mode (`write.delete.mode`/`write.update.mode`/
  `write.merge.mode = merge-on-read`) writes Puffin DVs for DELETE/UPDATE/MERGE (Java 1.8+; Spark 3.5+).
- Variant: needs Spark 4.0+ (Spark's VARIANT type). Unshredded write since 1.10; **shredded write
  since 1.11** behind `write.parquet.shred-variants` (table) / `spark.sql.iceberg.shred-variants`
  (session). The shredded schema is inferred from a buffered sample (most-common-type, 10% field
  frequency floor, cap 300 fields) — so shredded layouts are data-dependent, not declared.
- Row lineage: automatic on v3; `_row_id` and `_last_updated_sequence_number` are selectable metadata
  columns.
- Geo: Spark 4.1 + Iceberg 1.12 only.
- Nanos: Spark has no nanosecond timestamp type, so Spark is at best a microsecond view of
  `timestamp_ns` columns (unverified how 1.12 maps them; do not use Spark as the ns-precision oracle;
  use pyiceberg/DuckDB/Java API).
- Defaults: Spark SQL DDL not supported (see above).
- Unknown type: Spark 4.0+ read/write (#13445).

### 1.3 pyiceberg

Latest: **0.12.0** (2026-09-01; requires Python >=3.10,<4). Releases
([GitHub](https://github.com/apache/iceberg-python/releases)):
- 0.9.0 (2025-03): v3 metadata **read** support (#1554), initial/write-default deserialization (#1432).
- 0.10.0 (2025-09): `timestamp_ns`/`timestamptz_ns` (#1632), **DV read** (#1516), unknown type (#1681).
- 0.11.0 (2026-02): row-lineage fields for v3 (#2129).
- 0.12.0 (2026-09): geometry & geography types (#2859).

**pyiceberg still cannot WRITE v3 tables.** `pyiceberg/table/metadata.py` on main has
`SUPPORTED_TABLE_FORMAT_VERSION = 2`. Open: "Support writing V3 tables" #1551, broken into
#3619 (v3 metadata, PR #3551 open), #3620 (v3 manifests/manifest lists), #3621 (row-lineage
assignment on commit), #3622 (upgrade to v3); DV writing PR #3474 (PuffinWriter) open; VariantType
#1819 open; multi-arg transforms #3628 open. Tracking: [#1818](https://github.com/apache/iceberg-python/issues/1818).
Also open: #4467 (timestamptz_ns scan failure when files label UTC as "+00:00").

So pyiceberg is a **read-side** oracle for v3 (metadata parse, DV application, row-lineage columns,
nanos), not a writer. Use `StaticTable.from_metadata(<path to vN.metadata.json>)` or a `SqlCatalog`
on sqlite.

### 1.4 iceberg-rust

Latest v0.11.0 (2026-11-03). Tracking epic [#2411](https://github.com/apache/iceberg-rust/issues/2411):
- Done: DV **read** (deletion-vector-v1 decode, apply; epic #2792 closed), row lineage (first-row-id
  assignment, inheritance, `_row_id` columns), nanos types, top-level initial-default, unknown type,
  variant/geo type recognition in schema.
- Not done: DV **write** (#3232, #2580), variant read/write (#2880, #3606, #3685), geo read/write,
  write-default on write (#3250), nested initial-default (#3261), date->timestamp promotion bugs
  (#3387, #3404, #3503), CoW rewrite does not carry row lineage (#3401), multi-arg transform metadata
  parse fails (#2801), **v2 manifest Avro schema includes v3-only fields 142/144/145 (#3399)**,
  DV iterator 2^32 boundary bug (#3400).
- Has a v4 epic too (#3513).

### 1.5 iceberg-go

Latest 0.7.0 (2026-10-01), 0.8.0-rc0 on 2026-11-23. v3 tracking #589 is **closed**: declares v3 as max
supported; DV read + write (partitioned and unpartitioned), row lineage write path, defaults,
multi-arg transforms, Puffin reader/writer, encryption-keys metadata (metadata only). 0.7.0 added
variant shredded reader (#1238), WKB/GeoArrow geo (#1138), DV scanner read path (#1053), row-id
renumbering fix (#1237). Gaps: shredded variant writer (#987), variant stats/predicates, geo
stats/bounds/pruning (#989), encryption (#1289). iceberg-go has filed many sharp spec-conformance
bugs (several listed under pitfalls) — useful as a "what could go wrong" checklist.

### 1.6 DuckDB iceberg extension

Per [DuckDB 2.0 docs](https://duckdb.org/docs/2.0/core_extensions/iceberg/writing_to_iceberg) and the
[v1.5.3 feature post](https://duckdb.org/2026/05/29/new-iceberg-features) (2026-05-29): v3 **read and
write** of VARIANT, TIMESTAMP_NS, schema default values, binary deletion vectors, row lineage. Not
supported: GEOGRAPHY (and UNKNOWN in 1.5.x; planned for 2.0), copy-on-write UPDATE/DELETE/MERGE
(merge-on-read only; positional deletes become DVs on v3). **Writes require an attached catalog**
(REST — Polaris, Lakekeeper — or S3 Tables); no local-directory/Hadoop-catalog writes. Reads of a bare
directory work via `iceberg_scan('.../metadata/vN.metadata.json')`.
DuckDB PyPI latest is 2.0.1. Relevant open issues: #1777 (Databricks DVs fail Puffin verification),
#1623 (rewrite_data_files on v3), #1654 (DV scan-planning cost); closed-but-instructive: #1543
(UPDATE wrote a guessed `_row_id`), #1466 (delete manifest got `first_row_id`, spec violation), #1314
(delete manifests labelled `content: "data"`), #1248 (previously-written v3 tables with a delete
unreadable by later version).

### 1.7 Trino

[Trino 484 Iceberg connector docs](https://trino.io/docs/current/connector/iceberg): "Support for
format version 3 is experimental." Row-level updates/deletes on v3 use DVs; VARIANT requires v3;
`TIMESTAMP(9)` for `timestamp_ns`/`timestamptz_ns`; UNKNOWN. `add_files` procedures don't work on v3;
writing encrypted tables not supported. Default column values since release 477
([notes](https://trino.io/docs/current/release/release-477)). Athena (Trino-based) lists v3 as
unsupported ([AWS guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/table-spec-v3.html)).

### 1.8 Managed platforms (vendor; signals what's deployed)

- Databricks: v3 managed Iceberg (DBR 18 LTS+): DVs on by default, row lineage, VARIANT, GEOMETRY/
  GEOGRAPHY, write-defaults. **Not**: initial-defaults, nanos, unknown, multi-arg transforms
  ([docs](https://docs.databricks.com/aws/en/iceberg/iceberg-v3)).
- Snowflake: v3 preview 2026-03-04, GA 2026-05-07; v3 upgrades + expression defaults GA 2026-10-16
  ([GA note](https://docs.snowflake.com/en/release-notes/2026/other/2026-05-07-iceberg-v3-ga)).
- AWS: S3 Tables / EMR / Glue support DVs + row lineage (Nov 2025,
  [announcement](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-apache-iceberg-v3-deletion-vectors-row-lineage));
  S3 Tables later "all v3 data types" ([blog](https://aws.amazon.com/blogs/aws/amazon-s3-tables-now-support-all-apache-iceberg-v3-data-types/)).
  Redshift does not read/write VARIANT in v3 tables.
- Google BigLake: binary DVs on v3 ([docs](https://docs.cloud.google.com/lakehouse/docs/use-iceberg-v3-binary-deletion-vectors)).
- Cloudera: Hive/Impala v3 with DVs + row lineage as tech preview.

### 1.9 Commonly deployed vs niche (judgement from the above)

- **Mainstream, must be production quality:** deletion vectors (every engine that writes v3 writes
  them, Databricks on by default), row lineage (mandatory on every v3 table — `next-row-id`,
  `first-row-id` must be right even if nobody reads `_row_id`), VARIANT (Databricks/Snowflake/Spark/
  DuckDB/Trino; shredding increasingly), default values (at minimum *reading* initial-default;
  Snowflake/Databricks/Trino/DuckDB write them), timestamp_ns (read; writers are DuckDB/Trino/Java
  API, not Spark).
- **Readable but rarely written:** unknown type, multi-arg transforms (no mainstream SQL engine
  creates them; Databricks rejects them).
- **Niche:** geometry/geography (Java needed until 1.12 to be end to end; Spark 4.1 only;
  DuckDB lacks GEOGRAPHY; Databricks/S3 Tables support — growing but specialist), table encryption
  (Java only; Trino can't write; pyiceberg/rust/go metadata-only; requires KMS integration — very
  little deployment outside vendor-managed setups). Treat encryption as "refuse cleanly".

### 1.10 Known interop pitfalls

1. **DVs not in a Puffin container.** Databricks UniForm/IcebergCompatV3 tables reference a Delta
   `deletion_vector_<uuid>.bin` (1 version byte + `deletion-vector-v1` blob) with `file_format=PUFFIN`,
   `content_offset=1`. Java reads `content_size_in_bytes` at `content_offset` and never reads the
   footer, so Spark/Trino work; iceberg-go ([#2070](https://github.com/apache/iceberg-go/issues/2070))
   and DuckDB ([#1777](https://github.com/duckdb/duckdb-iceberg/issues/1777)) failed. **EW should read
   DVs by offset/size, not via the Puffin footer.** (EW already writes IcebergCompatV3 Delta tables,
   so EW's own Delta DVs hit this exact path.)
2. **Multiple DVs per data file in one snapshot** are illegal; Java added merge-on-commit (1.10.2
   #15654, 1.11 #15006) and still has a read-side failure (#17206). Validate on write, decide on read.
3. **Multiple DVs in one Puffin file** — Java 1.12 had to fix delete-file references for DVs in the
   same Puffin (#17497). Don't assume one blob per file.
4. **DV/partition mismatch:** 1.12 scans fail when a DV claims a partition that differs from its
   data file's. EW writers must copy the data file's spec id + partition onto the DV entry.
5. **Dangling DVs** after rewrites (Java #13245, iceberg-go #2238/#1221): rewriting a data file must
   remove its DV.
6. **Row lineage:** first-row-id inheritance for EXISTING entries during manifest merge (Java 1.10.2
   #16304), last-updated-sequence inheritance (1.12 #17039), v1/v2-era manifests in a v3 table must
   inherit first_row_id (iceberg-go #1295), `_row_id` renumbering when row-group pruning drops rows
   (iceberg-go #1237/#1250), CoW rewrites must carry `_row_id` into replacements (iceberg-rust #3401),
   delete manifests must NOT get `first_row_id` (DuckDB #1466), UPDATE must preserve source `_row_id`
   (DuckDB #1543). Also: upgrading v2->v3 does not backfill lineage.
7. **Manifest schema per format version:** v2 manifests must not carry v3-only fields 142/144/145
   (iceberg-rust #3399); delete manifests need Avro metadata `content: "deletes"` (DuckDB #1314);
   write `format-version` in manifest list Avro metadata (DuckDB #1315). (EW's #775 canonical-form
   schema refusal is directly relevant — foreign manifests vary.)
8. **Variant shredding in Iceberg Parquet:** Java uses the Parquet 1.16 VARIANT logical annotation
   (#13941). Shredded schema chosen by sampling (data-dependent). Partial-shredding data loss fixed only
   in 1.11 (#15087); "type uniformity" changed in 1.12 (#17424); decimal high-precision and UTF-8
   bound-ordering fixes in 1.12. Iceberg forbids min/max on the variant column itself but allows them on
   shredded children (iceberg-go #988); identity transform/bound literal predicates on variant are
   rejected (iceberg-go #1206). Same Parquet VARIANT encoding EW already implements for Delta.
9. **Defaults:** nested initial-default under a null ancestor struct (Java #17320), filtering on a
   defaulted column must not drop rows (Java #16692, iceberg-rust #3298/#3301), decimal defaults
   serialized wrongly (iceberg-go #2695), nanos default overflow (Java 1.10.1).
10. **Nanos:** predicate pushdown bugs (Java #16619/#17750), partition values for timestamp_ns not
    converted (pyiceberg #3652), "+00:00" vs "UTC" annotations (pyiceberg #4467).
11. **Type promotion date->timestamp(_ns)** (v3 only) is broken in iceberg-rust (#3387/#3404/#3503).
12. **Field IDs above 2147483447** are reserved for metadata columns (iceberg-rust #3403).

### 1.11 v4

Spec work in progress; Java 1.11/1.12 contain "implementation groundwork": v4 TrackedFile types
(#15049, #15854, #16253, #16952), v4 manifest reader (#16958), Parquet *and* Avro manifests in the v4
layout (#15634), relative paths, content statistics, V4 DV with key_metadata (#17438), a draft
"Mumbling Bitmap" spec with read-only implementation. Dremio: do not enable `format-version=4`.
iceberg-rust has a v4 epic (#3513). Nothing to target yet, but avoid baking v1/v2/v3-Avro-only
assumptions into the manifest abstraction.

---

## 2. Local oracle availability (the maintainer's Windows dev box, 2026-11-25)

### 2.1 What's present

| Item | State |
|---|---|
| JDK | Microsoft OpenJDK 17.0.20 (`JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.8-hotspot`) |
| Hadoop winutils | `HADOOP_HOME=~\.hadoop\hadoop-3.4.0` |
| Maven | 3.10.0 via chocolatey (on PATH); `~/.m2` has no `org/apache` artifacts |
| Ivy cache | `~/.ivy2.5.2/jars` — Delta 4.0-4.4.1, Parquet 1.16.0, Hadoop client 3.4.2; **no Iceberg jars** |
| DuckDB | Python package **1.5.5** in all venvs; extension cache `~/.duckdb/extensions/` has v1.5.2 (httpfs, spatial, quack) and v2.0.0 (delta). **`iceberg` extension not installed** — `LOAD iceberg` under 1.5.5 fails "Install it first" |
| pyiceberg | **Not installed anywhere** |
| EW_* env | `EW_DUCKDB_PYTHON=~\.venvs\ew-duckdb\Scripts\python.exe`; `EW_SPARK_PYTHON` unset |

Venvs (`~\.venvs\` and `~\.ew-venvs\`):

| venv | Python | relevant packages |
|---|---|---|
| ci-sim | 3.11.9 | deltalake 1.6.2, duckdb 1.5.5, fastavro 1.12.2, pyarrow 24.0.0 |
| ci-sim313 | 3.13.15 | same as ci-sim |
| ew-base-py313 | 3.13.15 | pyspark 4.0.3, delta-spark 4.0.0, deltalake 1.6.2, pyarrow 24 |
| ew-duckdb / ew-duckdb-py313 | 3.11 / 3.13 | duckdb 1.5.5 |
| ew-spark41 | 3.11.9 | pyspark 4.1.1, delta-spark 4.1.0 |
| **ew-spark41-py313** | 3.13.15 | **pyspark 4.1.2** (delta-spark via --no-deps) |
| ew-spark42-py313 | 3.13.15 | pyspark 4.2.0, delta_spark 4.4.1 (no Iceberg runtime exists for 4.2) |
| nightly-sim313 | 3.13.15 | pyspark 4.0.4, delta-spark 4.0.0, deltalake, duckdb 1.5.5, pyarrow |
| .ew-venvs/parquet-readers | 3.11.9 | datafusion 54.0.0, duckdb 1.5.5, pyarrow 25.0.1 |
| global Python311 | 3.11 | pyspark 4.0.1, delta-spark 4.0.0, deltalake 1.6.2, pyarrow 24 |
| miniforge3 base | 3.14.8 | none of the above |

### 2.2 What would be needed (nothing installed)

**A. Spark + Iceberg (reference writer/reader; the main oracle)**
- Spark 4.1 (geo needs 4.1; variant needs 4.0+). Reuse `ew-spark41-py313` (pyspark 4.1.2 — note
  Iceberg 1.12 was built against 4.1.3; 4.1.2 is the Windows/Py3.12+ floor per SPARK-53759) or create
  a dedicated venv: `py -3.13 -m venv ew-iceberg-spark41 && ...\pip install pyspark==4.1.3`
  (4.1.4 also on PyPI).
- Jar: `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0` via `spark.jars.packages`
  (Ivy resolves into `~/.ivy2.5.2`, same as the Delta jars; runtime jar includes the SQL extensions).
  Offline alternative: `mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`
  then `spark.jars`.
- Session config (Hadoop catalog on a local dir):
  ```
  spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  spark.sql.catalog.ew = org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.ew.type = hadoop
  spark.sql.catalog.ew.warehouse = file:///C:/.../warehouse
  ```
  Tables: `CREATE TABLE ew.db.t (...) USING iceberg TBLPROPERTIES ('format-version'='3',
  'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')`;
  shredded variant: `'write.parquet.shred-variants'='true'`. Path-based reads of an EW table:
  `spark.read.format("iceberg").load("file:///.../table")` (HadoopTables) or put it under the warehouse.
- EW's `FileSystemCatalog` already uses the Hadoop-catalog layout (`metadata/version-hint.text`,
  `metadata/v{N}.metadata.json`, src/EngineeredWood.Iceberg/FileSystemCatalog.cs:140-167), so a Spark
  Hadoop catalog and EW can share a warehouse directory. (Verify the namespace->directory mapping
  matches `<warehouse>/<ns>/<table>`.)
- Defaults: not settable via Spark SQL; use py4j to the Java API (`table.updateSchema()
  .addColumn(...).setDefault...`) — same trick `ConflictSemanticsInteropTests` uses for Delta.
- Nanos: Spark cannot see ns precision — validate nanos through pyiceberg or DuckDB.

**B. pyiceberg (read-side v3 oracle, cheap, no JVM)**
- `pip install "pyiceberg[sql-sqlite,pyarrow]==0.12.0"` (pyroaring is a core dependency, used for DV
  reads; add `[pyiceberg-core]` for the Rust-accelerated bits, `[geoarrow]` for geo).
- `SqlCatalog("ew", uri="sqlite:///.../catalog.db", warehouse="file:///.../wh")` or
  `StaticTable.from_metadata(".../vN.metadata.json")` for EW-written tables. **Cannot create/append
  v3 tables** until PRs #3551/#3474 land — so it is EW->pyiceberg only.

**C. DuckDB iceberg (second reader; writer only with a REST catalog)**
- `pip install duckdb==2.0.1` (repo pins 2.0.0 elsewhere) + one-time `INSTALL iceberg` (network;
  caches to `~/.duckdb/extensions/v2.0.x/windows_amd64/iceberg.duckdb_extension`).
- Read EW tables via `iceberg_scan('<table>/metadata/vN.metadata.json')`. DuckDB-as-writer needs a REST
  catalog (e.g. `apache/iceberg-rest-fixture` container or Lakekeeper) — docker, out of scope for a
  Windows unit-test tier unless the cloud-emulator pattern is reused.

### 2.3 The existing Delta oracle pattern to copy

- `test/Shared/InteropDriver.cs` (linked into each test project): generic Python-driver runner.
  Contract `python <script> <command> <args.json> <result.json>`, one JSON object written to a FILE
  (`{"ok": false, "error": ...}` on failure, exit 0) because Spark's JVM pollutes stdout. Ctor params:
  `scriptName`, `probeExpression` (Python one-liner printing `{"v": version}` — doubles as availability
  check), `requireEnvVar` (`=1` turns skip into failure), `timeoutMs`, `environment`, `preflight`
  (JDK/winutils checks the import can't see), `persistent` (one long-lived `serve` process, commands
  over stdin, `__EW_DONE__<file>` marker on stdout, result via tmp+rename), `interpreterOverrideEnvVar`.
- `test/EngineeredWood.DeltaLake.Table.Tests/Interop/Spark.cs`: thin static wrapper — probe
  `import pyspark, delta.tables`, `EW_REQUIRE_SPARK_INTEROP`, 600 s timeout, persistent, override
  `EW_SPARK_PYTHON`; sets `PYSPARK_PYTHON` to the same interpreter and prepends `%HADOOP_HOME%\bin`
  to PATH; preflight checks JAVA_HOME/java and winutils.exe.
- `Interop/spark_driver.py`: cached SparkSession (`local[1]`, UI off, shuffle partitions 1),
  `sys.stdout = sys.stderr` to protect the result channel, coarse-grained commands (one scenario per
  command), `serve` loop.
- Tests: `[SkippableFact]`, first statement `Spark.Require()`; feature-version gates via
  `Skip.IfNot(...)` on the probed version (e.g. IcebergCompatV3InteropTests needs delta-spark 4.3+);
  all interop classes in one `[Collection("Interop")]` with parallelization off.
- Docs: `doc/running-tests.md` "Delta Lake interop tiers" (venv recipes, version table, env vars);
  CI: `EW_REQUIRE_*` set in build vs nightly (Spark nightly only).

For Iceberg: add `Interop/iceberg_spark_driver.py` + `IcebergSpark.cs` (probe:
`import pyspark; ...` plus a preflight that the Iceberg runtime is resolvable, env
`EW_REQUIRE_ICEBERG_SPARK_INTEROP`, override `EW_ICEBERG_SPARK_PYTHON`), and a cheap
`pyiceberg_driver.py` / `PyIceberg.cs` (probe `import pyiceberg, pyroaring`, env
`EW_REQUIRE_PYICEBERG_INTEROP`, override `EW_PYICEBERG_PYTHON`, non-persistent) that could run in the
per-commit `build` job like delta-rs does. EW.Iceberg.Tests currently has no `Interop/` folder and does
not link `test/Shared/InteropDriver.cs`.

---

## 3. Fixtures

### 3.1 In the repo
- **No Iceberg tables produced by Java/Spark/pyiceberg** anywhere in the repo (no `*.metadata.json`,
  `version-hint`, `*.puffin`, or Iceberg manifest `.avro` outside `test/EngineeredWood.Avro.Tests/TestData`,
  which are generic Avro files). `test/EngineeredWood.Iceberg.Tests` builds everything in-process
  (V3Tests.cs, Manifest/*, Serialization/*).
- `doc/known-issues.md` "Iceberg" section: EW can only operate on tables whose metadata it wrote
  (manifest writer-schema canonical-form check, #775/#586); no data-file writer; no Puffin/DVs; no
  delete application; V1 manifests unsupported. So any foreign-fixture test first needs #775 lifted.
- Delta-side Iceberg interop exists only for IcebergCompat V1/V2/V3 (Delta tables), e.g.
  `test/EngineeredWood.DeltaLake.Table.Tests/Interop/IcebergCompatV3InteropTests.cs`.

### 3.2 Upstream reusable fixtures
- **No upstream project publishes a reusable v3 table corpus.** Every implementation regenerates
  tables with Spark in docker: iceberg-rust `crates/integration_tests/testdata/spark/provision.py`,
  pyiceberg `dev/provision.py`. Those scripts are good *recipes* to port into the Spark driver.
- Small static fixtures worth vendoring (Apache-2.0):
  - apache/iceberg `core/src/test/resources/org/apache/iceberg/deletes/`: DV/roaring blobs —
    `all-container-types-position-index.bin`, `empty-position-index.bin`,
    `small-alternating-values-position-index.bin`, `small-and-large-values-position-index.bin`,
    `64map32bitvals.bin`, `64mapempty.bin`, `64maphighvals.bin`, `64mapspreadvals.bin`
    (shared lineage with Delta's DV test vectors).
  - apache/iceberg `core/src/test/resources/org/apache/iceberg/puffin/v1/`:
    `empty-puffin-uncompressed.bin`, `sample-metric-data-{uncompressed,compressed-zstd}.bin`;
    iceberg-rust has Java-generated copies under `crates/iceberg/testdata/puffin/java-generated/`.
  - Table-metadata JSON: Java `core/src/test/resources/TableMetadataV3ValidMinimal.json`; iceberg-rust
    `crates/iceberg/testdata/table_metadata/TableMetadataV3Valid{Minimal,Encryption}.json` and
    `manifests_lists/manifest-list-v3-encrypted.avro`.
- Impala has v3 DV query workloads (`testdata/workloads/functional-query/queries/QueryTest/iceberg-v3-*.test`)
  — query tests, not portable tables.

Recommendation: generate a fixture corpus once with the Spark 4.1 + Iceberg 1.12 driver (v3 tables
with DVs incl. multi-DV Puffin, MoR UPDATE/MERGE, row lineage after compaction, unshredded + shredded
variant, geo, a v2->v3 upgraded table) and commit it, so per-commit CI can test reads without a JVM,
while the live Spark tier runs nightly.
