# Iceberg v3: inventory, gaps and implementation plan

Status: proposal, 2026-11-25. Baseline: `main` @ ef5c8c7f.

Research notes behind this plan (written by agents and not reviewed line by line, so check anything
marked "from memory" before code relies on it):

- [iceberg-v3-research/iceberg-v3-spec-digest.md](iceberg-v3-research/iceberg-v3-spec-digest.md): spec
  requirements by feature, with the DV byte layout, row-lineage rules, Appendix C/D mappings and spec
  changes since 1.12.0.
- [iceberg-v3-research/iceberg-v3-ecosystem.md](iceberg-v3-research/iceberg-v3-ecosystem.md): v3 status
  in Java/Spark/pyiceberg/rust/go/DuckDB/Trino, known interop pitfalls, and which oracles are on this machine.
- [iceberg-v3-research/ew-reusable-inventory.md](iceberg-v3-research/ew-reusable-inventory.md): what
  Parquet, Delta, Avro, Expressions and storage already provide, with file:line references.

**Target:** read every v1-v3 table that Spark 4.1 / Iceberg 1.12 writes, correctly. Append to v3 tables,
and do row-level DML on them (deletion vectors and copy-on-write), so that Spark, pyiceberg and DuckDB
read the results back identically. Refuse anything unsupported with a clear error and never misread it.

---

## 1. What we have today

`src/EngineeredWood.Iceberg` is about 3.4k lines, depends on Core, Avro and Expressions, and handles
**metadata only**. Nothing in it reads or writes data files.

| Area | State | Notes |
|---|---|---|
| Type model | Mostly there | Every v3 type is modelled, including variant, geometry(C), geography(C,A), timestamp(tz)_ns and unknown, and they round-trip through JSON. Defaults are kept as raw `JsonElement`, with no typed parse or validation. |
| Table metadata JSON | Partial | Missing: `statistics`, `partition-statistics`, `encryption-keys`, snapshot `key-id`, `stats-fields` (new on main), and partition/sort `source-ids`. There is no unknown-field passthrough, so **rewriting another engine's metadata drops these fields**. Nothing checks `format-version`, so a v4 table would be opened. |
| Row lineage | Fields only | `next-row-id`, `first-row-id`, `added-rows` and `first_row_id` exist in the model. No commit assigns or advances them, and the manifest codec writes none of them. |
| Manifest codec (`ManifestIO`) | **Not conformant** (#586) | Decodes by position against EW's own schema and refuses any other writer schema (#775). Writes an empty `partition` record and no bounds, nan counts, `equality_ids`, `referenced_data_file`, `content_offset/size`, `key_metadata` or v3 fields. Manifest-list `partitions` is written null. No OCF header metadata. Cannot read v1. |
| Commits | **Unsafe** | `FileSystemCatalog` overwrites `vN.metadata.json` and `version-hint.text` unconditionally, so two writers can each lose the other's commit. `TableOperations` rewrites manifests through the lossy codec. Planning and commits silently `continue` past a missing manifest, so a table reads as smaller than it is. |
| Scan planning | Toy | Prunes only on bounds built in-process. Ignores partition summaries and partition values (no transform evaluation). Returns every delete file, with no matching by sequence number, partition or referenced file. Uses the current schema even when time-travelling. |
| Transforms | Data only | No murmur3_x86_32 bucket, truncate or temporal evaluation, and no predicate projection. |
| Catalogs | In-memory and Hadoop-layout filesystem | No REST client. |
| Snapshot maintenance | Expire snapshots, refs, time travel | Expiry ignores delete files and Puffin files. |
| Tests | Unit only | Nothing is checked against another engine, and there are no foreign fixtures. |

Building blocks elsewhere in the repo (details in the inventory note):

- **Parquet.** Reads and writes field ids on every node. `WrittenMetadata` gives per-leaf min/max/null/nan/value
  counts and sizes. Also present: TIMESTAMP(NANOS); variant read and write including shredding (interop-tested);
  `IExcludedRows` row exclusion; row positions.
  *Gaps:* projection by **nested** field id (only top-level today), GEOMETRY/GEOGRAPHY, and a public rolling writer.
- **Delta.** The DV roaring code serializes the *same* portable 64-bit layout Iceberg uses, with the same
  big-endian length + CRC-32 framing, **and the same magic**: Delta's `1681511377` is `0x6439D3D1`, stored
  little-endian as `D1 D3 39 64`. So a Delta DV blob *is* an Iceberg `deletion-vector-v1` blob. The only
  difference is the container: Delta's `.bin` has a leading version byte (hence Databricks' `content_offset=1`),
  while Iceberg normally uses Puffin. (Checked in `DeletionVectorBuilder.cs` / `DeletionVectorWriter.cs`;
  the inventory note says the magic differs, which is wrong.)
  The row-tracking model matches row lineage, and `IcebergCompat` already uses the reserved lineage field ids.
  The table layer (plan → read pipeline → transaction/OCC → DML modes) is the template to mirror.
- **Avro.** Resolves a writer schema against a reader schema (validated against fastavro).
  *Gaps:* `field-id` attributes are dropped on parse and on write, and there is no custom OCF header metadata.
- **Storage.** `TryWriteAllBytesAsync` is a real atomic create-if-absent on local, S3, Azure and GCS, which is
  exactly what a Hadoop-style metadata commit needs.
- **Not present anywhere:** a Puffin reader or writer, and murmur3_x86_32 (ORC has only x64_128).

## 2. What the spec requires, in brief

The digest is the reference. These are the v3 obligations most likely to bite:

1. **Row lineage is mandatory.**
   - Every v3 snapshot carries `first-row-id` and `added-rows`, and commits advance `next-row-id`, on every branch.
   - Manifest-list `first_row_id` (520) is assigned to data manifests that lack one.
   - Entries that are not ADDED must have `first_row_id` (142) materialized.
   - The first commit after a v2→v3 upgrade assigns ids to *every* carried-over manifest.
   - A retry must recompute all of this.
   - Rewrites (compaction, copy-on-write, MERGE) must physically write the old `_row_id`. They null
     `_last_updated_sequence_number` for modified rows and copy it for unmodified ones.
2. **Deletion vectors replace position deletes.**
   - New position-delete files are prohibited in v3.
   - A data file has at most one DV per snapshot. A new DV must absorb the existing DV and any v2 position
     deletes, and the old entry is removed in the same commit.
   - Removing a data file removes its DV.
   - Readers ignore position deletes for any file that has a DV.
   - Read the blob by `content_offset`/`content_size_in_bytes`, not via the Puffin footer: Databricks
     IcebergCompatV3 DVs live in Delta `.bin` files.
3. **Defaults.** A field id missing from a data file resolves in this order: identity-partition constant,
   then name mapping, then `initial-default`, then null. Writers apply `write-default`. Struct defaults are
   `null`/`{}` only, and variant/geo/unknown default to null.
4. **Types.**
   - ns timestamps are converted to µs before bucket hashing.
   - Bounds are decoded by length after a type promotion (date → timestamp_ns).
   - Variant and geo allow only the `void` transform.
   - Variant and geo bounds have their own encodings.
5. **Equality deletes are still legal in v3.** Readers must apply them (data seq < delete seq, scoped by partition).
6. **Encryption keys, `source-ids` and `stats-fields`** must at least be preserved, and refused when they
   would change semantics: an encrypted table must not be read as plaintext.

## 3. Gaps, prioritized

Priorities: **P0** = a correctness or safety hole, or a blocker for everything else. **P1** = needed to read
real v3 tables. **P2** = needed to write v3. **P3** = row-level DML. **P4** = maintenance and breadth.
**P5** = niche.

| # | Gap | Pri | Why |
|---|---|---|---|
| G1 | Non-atomic `FileSystemCatalog` commit; no OCC or retry | P0 | Silent lost updates today |
| G2 | Missing manifest silently skipped in plan and commit | P0 | Reads return wrong (smaller) results |
| G3 | Metadata JSON drops unknown and unmodelled fields on rewrite; no `format-version` > 3 refusal | P0 | A commit corrupts other engines' tables |
| G4 | Lossy, self-only manifest codec (#586), including no v1 | P0 | Blocks every foreign table; commits corrupt them |
| G5 | No single-value binary (Appendix D) or JSON single-value codecs | P0 | Bounds, partition tuples, defaults and summaries all depend on them |
| G6 | No interop oracle or fixture corpus | P0 | "Production-quality" can't be shown without it |
| G7 | Transforms not executable (bucket/murmur3, truncate, temporal incl. ns); no predicate projection | P1 | Partition pruning, and writing partitioned tables |
| G8 | Scan planning: partition-summary and partition pruning; correct delete-file matching; snapshot schema | P1 | Correct, efficient reads |
| G9 | No data read path: id-based nested projection, name mapping, defaults, promotion, identity constants | P1 | The core of a reader |
| G10 | No Puffin or DV reader; no v2 position-delete or equality-delete application | P1 | v3 tables in the wild carry DVs |
| G11 | Row-lineage read columns `_row_id` / `_last_updated_sequence_number`, plus `_file`, `_pos`, `_spec_id`, `_partition` | P1 | Required by spec for readers that expose them; needed by rewrites |
| G12 | No data write path: field-id Parquet, Iceberg metrics (truncation, metrics modes), partition fan-out, rollover | P2 | Append |
| G13 | Commit producer: append, overwrite, delete-files; summaries; lineage assignment; conflict validation; retry recompute | P2 | Append, done right |
| G14 | Schema evolution with defaults; v2→v3 upgrade | P2 | Common v3 workflow |
| G15 | REST catalog client | P2 | Production deployments (Polaris, Unity, Glue, S3 Tables), and how DuckDB writes |
| G16 | DV/Puffin writer and DV merge rules | P3 | Merge-on-read DELETE/UPDATE/MERGE |
| G17 | DELETE/UPDATE/MERGE (DV and copy-on-write modes) with lineage carry-over and isolation checks | P3 | Row-level DML |
| G18 | Expire snapshots aware of delete and Puffin files; orphan removal; compaction carrying lineage; rewrite manifests | P4 | Operating a table |
| G19 | Partition statistics (v3 `dv_count`) and table statistics (Puffin theta) | P4 | Preserve at P0; write later |
| G20 | Variant bounds; ORC/Avro data files (EW has both codecs) | P4 | Breadth |
| G21 | Geometry/geography: Parquet logical types, geo bbox bounds | P5 | Rarely deployed; Parquet work first |
| G22 | Encryption | P5 | Refuse cleanly now (P0, part of G3); implementing it is a project of its own |
| G23 | Equality-delete *writing* | P5 | Streaming writers' need; v4 forbids it |
| G24 | Multi-arg transforms | P5 | Preserve and refuse to evaluate; none exist yet |

## 4. Implementation plan

Work lands in reviewable PRs, roughly one per bullet. Phases overlap where noted.

### Phase 0: make it safe and lossless (G1-G6), #953

1. **Commit safety.**
   - `FileSystemCatalog` writes `vN+1.metadata.json` with `TryWriteAllBytesAsync`; losing the race throws
     `CommitFailedException`. `version-hint.text` stays a best-effort hint, and readers fall back to listing.
   - Add Iceberg-style update requirements (`assert-ref-snapshot-id`, `assert-current-schema-id`, …) and a
     retry loop in `TableOperations`, mirroring Delta's `LogCommitter`.
   - Any missing manifest or manifest list throws.
2. **Metadata JSON.**
   - Model `statistics`, `partition-statistics`, `encryption-keys`, `key-id`, `source-ids`, `stats-fields`.
   - Keep unknown top-level, snapshot and field properties through a round-trip.
   - Accept `decimal(P, S)` with or without whitespace.
   - Refuse `format-version` ≥ 4.
   - Refuse to read data from a table with `encryption-keys`, or a snapshot or file with `key-id`/`key_metadata`.
3. **Avro.** Preserve `field-id`, `element-id`, `key-id` and `value-id` on parse and write, and support
   OCF header metadata on read and write. The Avro package gets a small API addition.
4. **Manifest codec rewrite (#586).** This is the largest Phase 0 item.
   - Decode by **field id** against the file's own writer schema, using `SchemaResolver`, so manifests from
     Java, pyiceberg, rust and go all read.
   - v1 manifests get sequence-number 0 and inheritance from snapshot ids.
   - Decode the partition tuple using the spec id from the header or the manifest list.
   - Encode the full v2 and v3 schemas, including `partition` typed by the spec, bounds, every metric and all
     v3 fields. Write header metadata: `schema`, `partition-spec`, `partition-spec-id`, `format-version`, `content`.
   - Write manifest-list partition summaries.
   - Rewrite a manifest only if every field round-trips. Otherwise carry the manifest forward, or refuse.
   - Then drop the #775 refusal.
5. **Value codecs.** Appendix D single-value binary for every type (including bounds decoded by length after
   promotion) and JSON single-value serialization (defaults, partition stats). Typed `LiteralValue`s, with an
   ns timestamp literal (Expressions currently has none). Bounds decoded from manifests feed the existing
   `StatisticsEvaluator`.
6. **Oracles and corpus.** Per-commit tiers run without a JVM; Spark runs nightly. Each tier copies
   `test/Shared/InteropDriver.cs`, and `EW_REQUIRE_ICEBERG_*` turns its skips into failures in CI.

   | Tier | Role | Cadence | v3 coverage (from the ecosystem note; verify in the spike) |
   |---|---|---|---|
   | **iceberg-go** 0.7/0.8, a small Go CLI driver built once | Live foreign **writer and reader**, the main per-commit oracle | Per commit | Writes DVs (partitioned too), row lineage, defaults, multi-arg transforms, Puffin. Reads shredded variant and WKB geo. **Doesn't write** shredded variant or geo stats. Go 1.x is installed here. |
   | **DuckDB** 2.0 `iceberg_scan` | Reader of EW-written tables | Per commit | Reads DVs, lineage, variant, ns timestamps and defaults. Writes need a REST catalog, so its writes join the REST tier (item 15). The venvs have DuckDB 1.5.5, so this needs an upgrade. |
   | **pyiceberg** 0.12 + sqlite SqlCatalog | Reader of EW-written tables | Per commit | Reads DVs, lineage and geo; no variant. Can't write v3, but **writes v2**, so it's the v2 writer oracle. |
   | **iceberg-rust** 0.11 (Rust driver, or `pyiceberg-core`) | Optional fourth reader | Per commit, if cheap | Reads DVs and lineage. Known promotion/default bugs (#3387, #3404, #3261), so a disagreement is weaker evidence. Write side lags (no DVs). Cargo is installed here. |
   | **Spark 4.1 + Iceberg 1.12** | Generates the committed corpus; live round-trips | Nightly | The only writer of everything: shredded variant, MoR and CoW UPDATE/MERGE, v2 position deletes, geo, compaction carrying lineage. Equality deletes go through the Java API. |

   - A committed corpus, generated by the Spark driver: partitioned (every transform), DVs (including several
     in one Puffin file), v2 position deletes and equality deletes, a v2→v3 upgraded table, schema evolution
     with defaults, unshredded and shredded variant, branches, and a compacted table carrying lineage.
     A second, smaller corpus comes from iceberg-go, so a Java quirk can't be mistaken for the spec.
   - Vendor Java's DV blob and Puffin test vectors.
   - Start with a spike: confirm that iceberg-go's local-filesystem catalog and `file://` paths work on Windows
     (sql catalog with sqlite), and that the claimed v3 write features actually produce tables Spark reads.
   - Coverage gaps without Spark per commit: shredded-variant writes by others, CoW/MERGE written by others, and
     geo. The committed corpus covers the read side of those. Only EW-write → Spark-read of those features
     waits for the nightly run.

**Exit:** EW reads the manifests of every corpus table losslessly. Committing an append to a Spark-written table
leaves Spark reading identical rows and identical other metadata. Two concurrent EW committers never lose a commit.

### Phase 1: a correct v1-v3 reader (G7-G11), #954

7. **Transforms.**
   - murmur3_x86_32 bucket with the per-type rules, checked against the Appendix B vectors.
   - truncate, and year/month/day/hour including ns.
   - Inclusive and strict predicate projection through each transform.
   - Unknown transforms are ignored for filtering but still used for partition equality.
8. **Planner.**
   - Manifest pruning by partition summaries, then file pruning by partition tuple plus metrics.
   - Use the snapshot's schema when time-travelling.
   - Delete index: equality deletes by sequence number and partition; position deletes by sequence number,
     partition and `referenced_data_file`; a DV overrides position deletes.
   - The output is `FileScanTask(data file, deletes, residual)`, the equivalent of Delta's `PlannedFile`.
9. **New `EngineeredWood.Iceberg.Table` project** (Parquet + Iceberg), mirroring `DeltaLake.Table` and `Lance.Table`.
   - Parquet gains an id → nested-path resolver and projection by field id. This is the main Parquet change.
   - Iceberg projection: id match → rename; missing id → identity partition constant / name mapping
     (`schema.name-mapping.default`) / `initial-default` / null; type promotion.
   - Residual evaluation with `ArrowRowEvaluator`.
10. **Deletes.**
    - Use the shared position-delete bitmap (see §6). Iceberg adds only the Puffin container, and reading by
      offset and length means Databricks-style DVs inside Delta `.bin` files work unchanged.
    - A Puffin reader (footer, LZ4/zstd-compressed footers) for statistics. DVs are read by offset and length.
    - v2 position-delete files and equality deletes (anti-join on field ids) feed `IExcludedRows` and a row mask.
11. **Metadata columns.** `_file`, `_pos`, `_spec_id`, `_partition`, `_row_id` and `_last_updated_sequence_number`,
    with inheritance (`first_row_id + _pos`, data sequence number; null before the upgrade).

**Exit:** every corpus table reads row-identical to Spark, including deletes, defaults, promotions, lineage
columns and time travel. Pruning counts are reported and tested.

### Phase 2: v2 + v3 writer and append (G12-G15), #955

12. **Data writer.**
    - Parquet with Iceberg field ids. Name the list element `element`, as Java does.
    - Iceberg metrics from `WrittenMetadata`: binary bounds, truncation to 16 chars or bytes, `write.metadata.metrics.*` modes.
    - Partition fan-out by transform; target file size via a public rolling writer (lift Delta's `RollingDataFileWriter`).
    - New rows get null or omitted lineage columns.
13. **Snapshot producer.**
    - Fast-append, overwrite (by filter or files) and delete-files, with spec summary fields.
    - Row-lineage assignment: snapshot `first-row-id`/`added-rows`, manifest `first_row_id`, materialized
      `first_row_id` on rewritten entries, `next-row-id` advance, the upgrade case.
    - Recompute the manifest list on retry.
    - Conflict validation like Java's (`validateAddedDataFiles`, `validateNoNewDeletes`), with snapshot and
      serializable isolation.
14. **Schema and spec evolution.**
    - Typed defaults: validate `initial-default` is fixed once set, apply `write-default` on write.
    - `UpgradeFormatVersion(3)`.
    - Partition evolution exists already. Check it against the corpus.
15. **REST catalog client** (`ICatalog`): config, load, create, commit with requirements and updates, and
    OAuth2/token auth. Test against the Iceberg REST fixture container or `iceberg-rest-fixture`. This is an
    independent track that can start as soon as Phase 0 item 2 lands.

**Exit:** Spark, pyiceberg and DuckDB read EW-appended v3 tables row-identically, including `_row_id`. Spark can
then commit on top of EW's commits, and EW on top of Spark's. Lineage invariants are checked by a validator
test helper on every commit.

### Phase 3: row-level DML (G16-G17), #956

16. **Puffin writer and DV writer.** One DV per data file. Merge in the existing DV and v2 position deletes, and
    remove the superseded entries. Many DVs can share one Puffin file. Blob properties are `referenced-data-file`
    and `cardinality`, with `snapshot-id`/`sequence-number` set to -1.
17. **DELETE / UPDATE / MERGE.**
    - Chosen by `write.delete.mode`, `write.update.mode` and `write.merge.mode`: merge-on-read writes DVs,
      copy-on-write rewrites files.
    - Copy-on-write and MERGE carry `_row_id` and set `_last_updated_sequence_number` per row.
    - Reuse the shape of Delta's MERGE and its key-range pruning.
    - Concurrency validation: no conflicting DVs for the same file (also avoids Java #17206), and no
      deletes of files we read.

**Exit:** Spark reads identical rows and lineage after an EW DELETE, UPDATE or MERGE in both modes, and EW reads
Spark's. A differential fuzz test runs random DML sequences, alternating engines.

### Phase 4: maintenance and breadth (G18-G20), #957

- Expire snapshots that are aware of delete and Puffin files, plus orphan-file removal.
- Compaction that carries lineage and drops DVs it has applied.
- Rewrite manifests.
- Write partition stats files (including `dv_count`), and keep table statistics.
- Variant bounds.
- ORC and Avro data files on read.

### Phase 5: niche (G21-G24), #958

- Geo needs Parquet GEOMETRY/GEOGRAPHY logical types and geospatial stats first, then the Iceberg mapping and bbox bounds.
- Encryption needs a design document of its own; `doc/encryption-design.md` covers Parquet.
- Equality-delete writing.
- Multi-arg transforms.

### Delta UniForm (#490): after Phase 2, before Phase 3 finishes

Once the Iceberg metadata and manifest writers exist, UniForm means generating an Iceberg snapshot from each
Delta commit. The Delta side already enforces IcebergCompatV1-V3. DVs need **no rewrite**: an Iceberg DV entry
can point straight at the Delta `.bin` with `content_offset = 1` (§1), which is exactly what Databricks writes.
Row tracking maps to row lineage (the reserved field ids are already used). Partition values are already
materialized in the data files (#756). UniForm is also the first real second consumer of the shared table code
(§6), so it is a good forcing function for deciding what moves where.

### Ordering and parallelism

```
P0: 1 ─┬─ 2 ─ 3 ─ 4 ─┬─────────────────────────────────────
       │   5 ────────┤
       │   6 ────────┤   (6 starts immediately; corpus is needed by end of 4)
P1:    │             ├─ 7 ─ 8 ─┬─ 9 ─ 11
       │             │         └─ 10
P2:    │             │                 12 ─ 13 ─ 14
       └─ 15 (REST, independent after 2) ──────────────────
P3:                                          16 ─ 17
```

Items 1, 5 and 6 have no dependencies and can start in parallel.

## 5. Decisions (2026-11-25)

1. **REST catalog: in scope.** Item 15 is part of the core plan. It also unlocks DuckDB as a writer oracle.
2. **Shared DV code: yes, location open.** See §6.
3. **`EngineeredWood.Iceberg.Table`:** a new package for now, alongside the metadata-only `EngineeredWood.Iceberg`.
4. **The writer produces v2 and v3.** Format version is per table: we believe Java still defaults to 2, but
   that is unchecked. What differs:
   - Row deletes: v2 writes file-scoped position-delete files (with `referenced_data_file`); v3 writes DVs.
   - Row lineage and defaults: v3 only.
   - The manifest schema per version: no v3-only fields in v2 manifests, the mistake iceberg-rust #3399 made.

   pyiceberg (which writes v2) is the per-commit v2 writer oracle.
5. **Oracles:** per commit, iceberg-go (writer and reader), DuckDB and pyiceberg, plus a committed
   Spark-generated corpus; live Spark nightly. See item 6.
6. **UniForm: yes.** It is scheduled after Phase 2 (above), and is the first second consumer of shared table code.

Work is tracked on GitHub as one tracking issue per phase, each with a checklist. An item gets an issue of its
own when work on it starts.

| Phase | Issue |
|---|---|
| 0: safe and lossless, oracles | #953 (item 4 = #586) |
| 1: correct v1-v3 reader | #954 |
| 2: v2/v3 writer, append, REST | #955 |
| 3: row-level DML | #956 |
| 4: maintenance and breadth | #957 |
| 5: niche v3 features | #958 |
| UniForm | #490 (AMT: #497) |

## 6. Shared table-format code

**Row-position deletes are already partly shared.**

- `Core/Encodings/RoaringBitmap.cs` (internal, exposed to the format assemblies through `InternalsVisibleTo`)
  parses 32-bit portable Roaring containers. It takes a flag for the spec vs CRoaring cookie variants.
- Lance uses it for `BITMAP` deletion files and bitmap indexes.
- Delta builds its 64-bit `RoaringBitmapArray` on top of it: `RoaringBitmapReader`, `DeletionVectorBuilder`,
  and `DeletionVectorBitmap`, which is an in-place view implementing Parquet's `IExcludedRows`.

**How each format fits:**

| | Position space | Encoding | Container |
|---|---|---|---|
| Delta | 64-bit row index in a file | `MT1d` magic + Roaring64 portable, BE length + CRC | `.bin` with version byte, or inline Z85 |
| Iceberg v3 | 64-bit `_pos` in a file | **identical blob** | Puffin, or a Delta `.bin` (UniForm) |
| Iceberg v2 | 64-bit `pos` | Parquet/Avro position-delete *rows* | data-format file |
| Lance | 32-bit row offset in a fragment | CRoaring 32-bit bitmap, or an Arrow `Int32` array | `_deletions/*.bin` / `*.arrow` |

Lance's positions are a subset (a single high key of 0). It currently expands deletes into a `HashSet<int>`
(`DeletionMask`), the cost Delta removed in #610. So all three formats want the same thing: build a
position set from some encoding, then exclude rows with it cheaply during the scan.

**Proposed layering.** It needs no new assembly yet.

1. **Encoding layer (Core).** Promote the 64-bit view, the builder, and the Roaring64 portable serializer
   from Delta into `Core/Encodings` as one `PositionDeleteBitmap` (name TBD). It holds no framing and no
   Delta types.
2. **Consumption interface (Core).** Move `IExcludedRows` from Parquet to Core; this is a breaking change,
   which is fine pre-1.0. Parquet's scan consumes it, and Lance's row filter and Iceberg's equality-delete
   masks can implement or consume it without a Parquet reference. Lance currently has no Parquet dependency.
3. **Framing layer (per format).** Delta keeps the version byte, Z85 inline and path encoding. Iceberg adds the
   Puffin container. Lance keeps the `.arrow` / `.bin` choice.

Core is the right home for (1) and (2) because they are encodings and scan contracts, like the Roaring code
already there. It is the wrong home for the table-format semantics that UniForm and AMT will push towards
sharing:

- commit protocols and OCC conflict checking;
- a manifest or metadata-tree model (AMT, #497, is Iceberg v4's tree adopted by Delta);
- row tracking and lineage assignment;
- partition transforms and bound encoders;
- Puffin and statistics files;
- "planned file + deletes + residual" scan tasks.

**Recommendation.** Do (1)-(2) now, as the first Phase 1 PR. For the rest:

- Write the Iceberg pieces with clean, format-neutral seams inside the Iceberg assemblies.
- Revisit when UniForm lands. That is the first time Delta code calls Iceberg code, and the point at which a
  shared `EngineeredWood.Table` assembly (or merging the Delta and Iceberg assemblies) can be judged from real
  call sites rather than guessed.
- Build the Iceberg manifest abstraction so that a v4 / AMT root manifest can slot in. Don't bake
  "manifest list + Avro manifests" into the planner's interfaces.
