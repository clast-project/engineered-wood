# Apache Iceberg format-version 3: requirement-level digest (for a .NET reader + writer)

Compiled 2026-11-25 from primary sources downloaded that day:

| Source | URL | Snapshot used |
|---|---|---|
| Table spec (main) | https://github.com/apache/iceberg/blob/main/format/spec.md | raw main, last spec commit 2026-11-21 (#18545) |
| Table spec (last release) | https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md | tag apache-iceberg-1.12.0 (released 2026-09-30) |
| Published page | https://iceberg.apache.org/spec/ | rendered HTML; tracks main (contains the Nov-2026 edits: stats-only fields, explicit identity source list, default value expressions, `total_bytes`, `tight_stats`) |
| Puffin | https://iceberg.apache.org/puffin-spec/ , format/puffin-spec.md | identical on 1.12.0 and main |
| AES GCM Stream | format/gcm-stream-spec.md | main |
| View spec | format/view-spec.md | main (view format-version is still 1) |
| REST OpenAPI | open-api/rest-catalog-open-api.yaml | main |

Section anchors below are relative to https://iceberg.apache.org/spec/ (e.g. `#row-lineage`).

Notation: **MUST/MUST NOT/SHOULD** are my capitalisation of the spec's lower-case "must/should" -- the spec does not use RFC 2119 capitals. Quotes are verbatim unless marked "paraphrase".

---

## 0. Status of the versions

- "Versions 1, 2 and 3 of the Iceberg spec are complete and adopted by the community." (`#format-versioning`)
- "**Version 4 is under active development and has not been formally adopted.**" Main's spec.md now interleaves v4 text into almost every section (tabbed "v1 - v3" / "v4" tables). When implementing v3, read only the "v1 - v3" tabs; several prose paragraphs were rewritten in neutral terms ("snapshot root file", "leaf manifest", "live" status) to cover both. For v3, "snapshot root file" == manifest list, "leaf manifest" == data or delete manifest, "live" == status EXISTING or ADDED.
- v3 feature list (`#version-3-extended-types-and-capabilities`): nanosecond timestamp(tz), unknown, variant, geometry, geography; default values; multi-argument transforms; row lineage; binary deletion vectors; table encryption keys.
- Metadata JSON: "Implementations must throw an exception if a table's version is higher than the supported version." A v3 implementation must refuse `format-version: 4`.

---

## 1. Baseline obligations from v1/v2 that a v3 implementation still carries

Each item: spec text, then reader (R) / writer (W) obligations.

### 1.1 Writer/reader requirement matrix (`#writer-requirements`)
- Blank = "field should be omitted"; optional = may write; required = must write.
- Readers must be permissive for Avro manifests/manifest lists because v1 files can appear in v2+ tables: a field required in v2 but absent in v1 is read as optional. Only `required/required` fields "fill in a default or throw".
- "If a later version is not shown, the requirement for a version is not changed" -- v3 inherits v2 columns unless a v3 column is shown.
- Metadata JSON may be read strictly ("a v2 table that is missing `last-sequence-number` can throw").
- W: "All columns must be written to data files even if they introduce redundancy" (including identity-partition source columns). "Writers are not allowed to commit files with a partition spec that contains a field with an unknown transform."

### 1.2 Sequence numbers and inheritance (`#sequence-numbers`, `#inheritance`, `#sequence-number-inheritance`)
- Every successful commit gets the next sequence number; it is reassigned on retry.
- W: new manifest entries written with `null` `snapshot_id`, `sequence_number`, `file_sequence_number`; the manifest list writes the snapshot's sequence number into `manifest_file.sequence_number` (515) for new manifests.
- R: "if the field is `null` for an entry, then the entry must inherit its value from the manifest file's metadata, stored in the snapshot root file [manifest list]". Data sequence number inherited only when status is ADDED (v4 also MODIFIED-with-column-file); file sequence number only when ADDED.
- W: "When writing an existing file to a new manifest or marking an existing file as deleted, the data and file sequence numbers must be non-null and set to the original values". Same for `snapshot_id` (in practice: copy the inherited value).
- W: a file whose data "logically belongs to an older sequence number" may carry an explicit `sequence_number`; `file_sequence_number` is always assigned at commit.
- R: v1 manifests with no sequence column -> all sequence numbers default to 0. `min_sequence_number` and `sequence_number` in v1 manifest lists default to 0.
- `file_sequence_number` "can't be used for pruning delete files".

### 1.3 Manifest file key-value metadata (v1-v3) (`#manifest-file-format`)
| Key | v1 | v2/v3 |
|---|---|---|
| `schema` (JSON table schema at write time) | required | required |
| `schema-id` (string) | optional | required |
| `partition-spec` (JSON of the spec's `fields` array only) | required | required |
| `partition-spec-id` (string) | optional | required |
| `format-version` (string) | optional | required |
| `content` ("data" or "deletes") | - | required |
- v1-v3: one partition spec per manifest; a manifest is either data or deletes, never both.
- Manifests and manifest lists are Avro (v1-v3) and must follow Iceberg Avro field-id rules (1.13).

### 1.4 `manifest_entry` (v1-v3) (`#manifest-entry-fields`)
| id | name | v1 | v2/v3 | notes |
|---|---|---|---|---|
| 0 | status | req | req | 0 EXISTING, 1 ADDED, 2 DELETED. "Deletes are informational only and not used in scans." |
| 1 | snapshot_id | req | opt | inherited when null |
| 3 | sequence_number | - | opt | data seq; inherited when null and status=1 |
| 4 | file_sequence_number | - | opt | inherited when null and status=1 |
| 2 | data_file | req | req | struct below |

### 1.5 `data_file` (v1-v3) (`#data-file-fields`)
| id | name | v1 | v2 | v3 | type / notes |
|---|---|---|---|---|---|
| 134 | content | - | req | req | 0 DATA, 1 POSITION DELETES, 2 EQUALITY DELETES (DVs use 1) |
| 100 | file_path | req | req | req | full URI with scheme |
| 101 | file_format | req | req | req | `avro`, `orc`, `parquet`, or `puffin` |
| 102 | partition | req | req | req | struct, field ids = partition field ids |
| 103 | record_count | req | req | req | "or the cardinality of a deletion vector" |
| 104 | file_size_in_bytes | req | req | req | |
| 105 | block_size_in_bytes | req | - | - | deprecated, "Do not write in v2 or v3" |
| 106 | file_ordinal | opt | - | - | do not write |
| 107 | sort_columns | opt | - | - | do not write |
| 108 | column_sizes | opt | opt | opt | `map<117: int, 118: long>`; excludes footers; null for Avro |
| 109 | value_counts | opt | opt | opt | `map<119,120>` incl. null and NaN |
| 110 | null_value_counts | opt | opt | opt | `map<121,122>` |
| 137 | nan_value_counts | opt | opt | opt | `map<138,139>` |
| 111 | distinct_counts | opt | opt | - | deprecated; **not written in v3** |
| 125 | lower_bounds | opt | opt | opt | `map<126: int, 127: binary>` (Appendix D) |
| 128 | upper_bounds | opt | opt | opt | `map<129: int, 130: binary>` |
| 131 | key_metadata | opt | opt | opt | binary, encryption |
| 132 | split_offsets | opt | opt | opt | `list<133: long>`, sorted ascending |
| 135 | equality_ids | - | opt | opt | `list<136: int>`; required when content=2, null otherwise |
| 140 | sort_order_id | opt | opt | opt | null for position deletes; readers ignore it for position deletes |
| 142 | first_row_id | - | - | opt | **v3**, row lineage |
| 143 | referenced_data_file | - | opt | opt | **v2+v3**; required for DVs |
| 144 | content_offset | - | - | opt | **v3**, DV blob offset |
| 145 | content_size_in_bytes | - | - | opt | **v3**, DV blob length; "required if `content_offset` is present" |
| 141 | (reserved) | | | | "The following field ids are reserved on `data_file`: 141." (v4 uses it for `spec_id`) |

- Bounds: "Each value must be less than or equal to all non-null, non-NaN values" (lower), mirror for upper.
- "In v2, the partition struct's field ids must match the ids from the partition spec."

### 1.6 Manifest list `manifest_file` (`#manifest-lists`)
| id | name | v1 | v2 | v3 | notes |
|---|---|---|---|---|---|
| 500 | manifest_path | req | req | req | |
| 501 | manifest_length | req | req | req | |
| 502 | partition_spec_id | req | req | req | |
| 517 | content | - | req | req | 0 data, 1 deletes |
| 515 | sequence_number | - | req | req | 0 for v1 |
| 516 | min_sequence_number | - | req | req | min data seq of live files; 0 for v1 |
| 503 | added_snapshot_id | req | req | req | |
| 504/505/506 | added/existing/deleted_files_count | opt | req | req | "when null this is assumed to be non-zero" |
| 512/513/514 | added/existing/deleted_rows_count | opt | req | req | same |
| 507 | partitions | opt | opt | opt | `list<508: field_summary>` |
| 519 | key_metadata | opt | opt | opt | |
| 520 | first_row_id | - | - | opt | **v3** |

`field_summary`: 509 contains_null (req), 518 contains_nan (opt), 510 lower_bound (opt, bytes, Appendix D of the *partition field's* type), 511 upper_bound. "If -0.0 is a value of the partition field, the `lower_bound` must not be +0.0, and if +0.0 is a value ... the `upper_bound` must not be -0.0."
- W: "A new manifest list is written for each attempt to commit a snapshot".

### 1.7 Snapshots (`#snapshots`)
| field | v1 | v2 | v3 |
|---|---|---|---|
| snapshot-id | req | req | req |
| parent-snapshot-id | opt | opt | opt (omit when no parent) |
| sequence-number | - | req | req |
| timestamp-ms | req | req | req |
| manifest-list | opt | req | req |
| manifests | opt | - | - (must be omitted if manifest-list present) |
| summary | opt | req | req (`operation` required: append / replace / overwrite / delete) |
| schema-id | opt | opt | opt |
| first-row-id | - | - | **req** |
| added-rows | - | - | **req** |
| key-id | - | - | **opt** |
- Optional summary metrics (Appendix F `#optional-snapshot-summary-fields`), string-valued: `added-data-files`, `deleted-data-files`, `total-data-files`, `added-delete-files`, `added-equality-delete-files`, `removed-equality-delete-files`, `added-position-delete-files`, `removed-position-delete-files`, **`added-dvs`, `removed-dvs`**, `removed-delete-files`, `total-delete-files` (DVs count as delete files), `added-records`, `deleted-records`, `total-records` (now clarified: "before applying position deletes, equality deletes, or deletion vectors"), `added-files-size`, `removed-files-size`, `total-files-size`, `added/removed/total-position-deletes`, `added/removed/total-equality-deletes`, `deleted-duplicate-files`, `changed-partition-count`, `manifests-created/kept/replaced`, `entries-processed`; other: `wap.id`, `published-wap-id`, `source-snapshot-id`, `engine-name`, `engine-version`.
- Snapshot ids: positive, collision-resistant; Java = UUIDv4 hi XOR lo, AND Long.MAX. "Java will no longer write `-1` ... and will use `null` for 'no current snapshot' for all tables with a version greater than or equal to V3"; readers should accept `-1` as null.

### 1.8 Table metadata (v1-v3) (`#table-metadata-fields`)
| field | v1 | v2 | v3 |
|---|---|---|---|
| format-version | req | req | req |
| table-uuid | opt | req | req ("must throw ... if UUID does not match the expected UUID after refreshing") |
| location | req | req | req |
| last-sequence-number | - | req | req |
| last-updated-ms | req | req | req |
| last-column-id | req | req | req |
| schema | req | - | - (deprecated) |
| schemas / current-schema-id | opt | req | req |
| partition-spec | req | - | - (deprecated) |
| partition-specs / default-spec-id / last-partition-id | opt | req | req |
| properties | opt | opt | opt |
| current-snapshot-id | opt | opt | opt (must equal `refs.main`) |
| snapshots | opt | opt | opt |
| snapshot-log | opt | opt | opt |
| metadata-log | opt | opt | opt |
| sort-orders / default-sort-order-id | opt | req | req |
| refs | - | opt | opt ("There is always a `main` branch reference pointing to the `current-snapshot-id` even if the `refs` map is null") |
| statistics | opt | opt | opt |
| partition-statistics | opt | opt | opt |
| next-row-id | - | - | **req** |
| encryption-keys | - | - | **opt** |
| stats-fields | - | - | **opt -- NEW on main (2026-11-02, #18405), not in 1.12.0** |
- snapshot-log: "Each time the current-snapshot-id is changed, a new entry should be added"; on expiry "all entries before a snapshot that has expired should be removed".
- metadata-log: add the previous metadata file location on every new metadata file; may be capped.
- Metadata JSON may be GZIP compressed; Java reads `.gz.metadata.json` and `metadata.json.gz`.
- Commit protocols (`#commit-conflict-resolution-and-retry`): metastore check-and-put with `<V>-<uuid>.metadata.json`; file-system rename scheme `v<V>.metadata.json` is "**deprecated** and will be removed in version 4 ... **unsafe** in object stores and local file systems." Retry rules: appends always re-applicable; replace/delete must verify files still present; schema/spec updates must verify the schema has not changed.

### 1.9 Refs, branches, tags, retention (`#snapshot-references`, `#snapshot-retention-policy`)
- ref = `snapshot-id` (req), `type` `tag|branch` (req), `min-snapshots-to-keep`, `max-snapshot-age-ms` (branch only), `max-ref-age-ms` (not for main; main never expires). Defaults from properties `history.expire.min-snapshots-to-keep`, `history.expire.max-snapshot-age-ms`, `history.expire.max-ref-age-ms`.
- Expiry algorithm: drop refs older than max-ref-age-ms (except main) -> retain each ref's snapshot -> for each branch retain ancestors until both older than max-snapshot-age-ms AND beyond the first min-snapshots-to-keep -> expire the rest.
- File deletion: a file may be physically deleted when the snapshot that deleted it is GC'd "assuming that the file is not live in any remaining snapshot". Adding a deleted file back is "not recommended".
- **v3 row lineage interaction**: `next-row-id` must advance on every snapshot commit "even if the new snapshot is not in the main branch".

### 1.10 Schemas, evolution, projection (`#schema-evolution`, `#column-projection`)
- Field ids unique across the whole schema; last-column-id ensures fresh ids.
- Valid promotions: v1/v2: int->long, float->double, decimal(P,S)->decimal(P',S) with P'>P. **v3 adds**: `unknown` -> any type; `date` -> `timestamp` or `timestamp_ns` (NOT to timestamptz/timestamptz_ns; "values outside the promoted type's range must result in a runtime failure").
- Bounds after promotion are not rewritten; R must infer the write-time type from bound length: `long` 4 bytes = int; `double` 4 bytes = float; `timestamp` 4 bytes = date; `timestamp_ns` 4 bytes = date; decimal any length.
- Promotion forbidden for a partition source when the transform result would change; explicitly "date to timestamp or timestamp_ns" (e.g., `identity`/`bucket` on a date column).
- Disallowed: regrouping struct fields, primitive<->struct.
- Field deletion "cannot be rolled back unless the field was nullable or if the current snapshot has not changed".
- R projection by **field id**, never name/position. For ids missing in a data file, in order:
  1. value from partition metadata if an identity transform exists and the value is present in `data_file.partition`;
  2. `schema.name-mapping.default` mapping (files without field ids);
  3. `initial-default` if defined (v3);
  4. null.
- Name mapping (`#name-mapping-serialization`): JSON list of `{ "names": [...] (required, may be empty), "field-id": int (optional), "fields": [...] (optional) }`. Dots are literal. Lists map `element`; maps map `key` and `value`.
- Identifier fields (`identifier-field-ids` in schema JSON, v2/v3 optional): primitive only, not inside map/list, not float/double, not optional, not under optional struct. (Removed in v4 in favour of primary-key constraints.)
- Schema JSON: `schema-id` required v2+.

### 1.11 Partitioning (`#partitioning`, `#partition-transforms`, `#partition-evolution`)
- Source columns "must be a primitive type and cannot be contained in a map or list, but may be nested in a struct" (variant is not primitive -> never a source).
- v2+: partition field ids tracked, unique across all specs (`last-partition-id`). v1: implicit ids from 1000.
- Equivalent specs: same field count and per-field same source ids, transform, name. "Writers must not create a new partition spec if there already exists a compatible partition spec". "Partition field IDs must be reused if an existing partition spec contains an equivalent field."
- Unknown transforms: v3 readers **must** read and ignore them for filtering (inclusive projection = true) but "the result of an unknown transform is still used when testing whether partition values are equal" (delete matching). Writers must not commit data with such a spec.
- All transforms return null for null input. `void` produces null; result type "Source type or `int`".
- `day` result type is `date`; "Readers must also accept `int` values for the `day` transform".
- Truncate: positive remainders (`v - (((v % W) + W) % W)`), strings by code points (valid UTF-8), binary by bytes, decimal width scaled to the column scale.
- Bucket: `(murmur3_x86_32(x) & Integer.MAX_VALUE) % N`, seed 0 (Appendix B, 1.15).
- v1 evolution advice: never reorder, never drop (use void), only append.

### 1.12 Sort orders (`#sorting`)
- order id 0 reserved for unsorted; fields: source-id(s), transform, `asc|desc`, `nulls-first|nulls-last`.
- Float ordering: `-NaN < -Inf < -v < -0 < 0 < v < Inf < NaN`.
- Writers should sort by the default order but are not required to.

### 1.13 Scan planning and deletes (`#scan-planning`)
- Only live entries (EXISTING/ADDED) are used. Manifests may be skipped via counts, partition summaries, or column stats.
- Inclusive projection of predicates to partition predicates using the manifest's own spec (not current spec).
- **Content file uniqueness** (`#content-file-uniqueness`, clarified Aug 2026): "each content file must be referenced by at most one live manifest entry across all manifests; otherwise, the snapshot has undefined behavior"; writers should not produce both ADDED and DELETED entries for the same file; readers may raise an error.
- Delete application (v3 rules):
  - DV in a delete manifest applies when data `file_path` == DV `referenced_data_file`, AND data seq <= DV data seq, AND same spec + partition values.
  - Position delete file applies when (referenced_data_file null or equal), data seq <= delete seq, same partition, AND **"There is no deletion vector that must be applied to the data file"**.
  - Equality delete file applies when data seq **strictly <** delete seq, and same partition OR the delete's spec is unpartitioned (global).
  - Position deletes and DVs apply to data files from the same commit (equal seq).
- Floating partition value equality: bit layout with canonical NaN.
- Equality deletes: null in a delete column matches `IS NULL`; dropped delete columns must still be used; columns added later read via normal projection (default null). Equality-column restrictions = identifier-field restrictions except optional columns allowed.
- Position delete files: struct `2147483546 file_path`, `2147483545 pos`, optional `2147483544 row` (required struct when present); sorted by file_path then pos. Row data is deprecated in Java 1.11.0.
- Delete files carry the same stats as data files but describe deleted rows; "metrics must store bounds and counts for all deleted rows, or must be omitted".

### 1.14 Statistics and partition statistics (`#table-statistics`, `#partition-statistics`)
- `statistics` entries: `snapshot-id`, `statistics-path`, `file-size-in-bytes`, `file-footer-size-in-bytes` (total footer incl. magics, not payload), optional `key-metadata` (base64), `blob-metadata` list of {`type`, `snapshot-id`, `sequence-number`, `fields`, optional `properties`}. Informational; readers may ignore.
- `partition-statistics` entries: `snapshot-id`, `statistics-path`, `file-size-in-bytes`; at most one per snapshot; must be registered in metadata to count.
- Partition statistics file schema (any data format, rows sorted by `partition` asc nulls first):

| id | name | v1 | v2 | **v3** |
|---|---|---|---|---|
| 1 | partition (unified partition type: union of all partition fields ever, sorted by field id) | req | req | req |
| 2 | spec_id | req | req | req |
| 3 | data_record_count | req | req | req |
| 4 | data_file_count | req | req | req |
| 5 | total_data_file_size_in_bytes | req | req | req |
| 6 | position_delete_record_count (position delete files + DVs) | opt | opt | **req** |
| 7 | position_delete_file_count (ignoring DVs) | opt | opt | **req** |
| 13 | dv_count | - | - | **req** |
| 8 | equality_delete_record_count | opt | opt | **req** |
| 9 | equality_delete_file_count | opt | opt | **req** |
| 10 | total_record_count | opt | opt | opt |
| 11 | last_updated_at (ms) | opt | opt | opt |
| 12 | last_updated_snapshot_id | opt | opt | opt |
- After a v2->v3 upgrade, `position_delete_record_count` must include remaining v2 position delete files plus DVs.
- **Changed on main**: `total_record_count` -- with equality deletes or v2 position delete files "implementations may omit this field and must write `NULL`"; with no deletes or only DVs, "encouraged to populate" from manifests. (1.12.0 text said "Accurate count of records ... after applying deletes".)

### 1.15 Appendix B hashing (`#appendix-b-32-bit-hash-requirements`)
| type | hash | test vector |
|---|---|---|
| int | hashLong(long(v)) | 34 -> 2017239379 |
| long | hashBytes(LE 8 bytes) | 34L -> 2017239379 |
| decimal | hashBytes(minBigEndian(unscaled)) | 14.20 -> -500754589 |
| date | hashInt(days) | 2017-11-16 -> -653330422 |
| time | hashLong(micros from midnight) | 22:31:08 -> -662762989 |
| timestamp / timestamptz | hashLong(micros) | 2017-11-16T22:31:08 -> -2047944441; ...08.000001 -> -1207196810 |
| **timestamp_ns / timestamptz_ns** | **hashLong(microsecsFromUnixEpoch(v))** -- "Nanosecond timestamps must be converted to microsecond precision before hashing" | 2017-11-16T22:31:08.000001001 -> -1207196810 |
| string | hashBytes(utf8) | iceberg -> 1210000089 |
| uuid | hashBytes(16 bytes big-endian) | f79c3e09-677c-4bbd-a479-3f349cb785e7 -> 1488055340 |
| fixed/binary | hashBytes(v) | 00 01 02 03 -> -188683207 |
Not bucketable (hash given for completeness): unknown -> null; boolean -> hashInt(0/1) (true -> 1392991556); float/double -> hashLong(doubleToLongBits(double(v))) with NaN canonicalised to 0x7ff8000000000000 and -0.0 -> 0.0 (1.0 -> -142385009, 0.0/-0.0 -> 1669671676). "A 32-bit hash is not defined for `variant`".

### 1.16 Avro requirements (`#avro`)
- Optional fields, array elements and map values: union with `null`; "This is the only union type allowed in Iceberg data files."
- "Optional fields without an Iceberg default must set the Avro field default value to null. Fields with a non-null Iceberg default must convert the default to an equivalent Avro default." (v3 defaults)
- Non-string-key maps: array of 2-field records with `logicalType: map`.
- Ids: `field-id` on record fields, `element-id` on arrays, `key-id`/`value-id` on Avro maps, `field-id` on key/value of array-maps.
- Timestamps: `adjust-to-utc` property (Iceberg convention, default false).

### 1.17 Parquet requirements (`#parquet`)
- "Column IDs are required to be stored as field IDs on the parquet schema." "Lists must use the 3-level representation."
- decimal: int32 (P<=9), int64 (P<=18), else fixed with minimum bytes.
- See section 3 for v3 types.

### 1.18 ORC (`#orc`) -- for completeness
- `iceberg.id`, `iceberg.required` attributes; timestamps epoch 2015-01-01 in ORC; `iceberg.timestamp-unit` = MICROS/NANOS; writers for micro types must truncate nanos.

---

## 2. v3 feature checklists

### 2.1 New primitive types: `timestamp_ns`, `timestamptz_ns`

(a) Spec: "Timestamp, nanosecond precision, without timezone" / "with timezone" (`#primitive-types`). Without-zone values are wall-clock; with-zone values are stored as UTC instants.

(b) Reader:
- Parquet: int64 `TIMESTAMP(NANOS, isAdjustedToUTC=false|true)`; Avro: long `logicalType: timestamp-nanos` + `adjust-to-utc` ("an Iceberg convention; the Avro specification does not define this type"); ORC `timestamp`/`timestamp_instant` with `iceberg.timestamp-unit=NANOS`.
- Bounds: 8-byte LE nanoseconds. But a `timestamp_ns` column promoted from `date` can have 4-byte bounds (= date days).
- Transforms: identity, bucket (hash on micros), year, month, day, hour.

(c) Writer: same mappings; JSON single value `"2017-11-16T22:31:08.123456789"` (no offset) / `"...123456789+00:00"` (offset must be `+00:00`).

(d) Schema JSON: `"timestamp_ns"`, `"timestamptz_ns"`.

(e) Edge cases:
- Nanosecond int64 range is roughly 1677-09-21 to 2262-04-11. Promotion `date -> timestamp_ns` must fail at runtime for out-of-range dates.
- Bucket: ns -> us conversion before hashing; the spec does not say floor vs truncate-toward-zero for negative (pre-1970) values. Java uses floor division (`Math.floorDiv`) (from memory; verify against `BucketUtil`/`Transforms`). Same question for `day`/`hour`/`month`/`year` of pre-epoch ns values: use floor semantics (consistent with micros transforms in Java).
- There is no nanosecond *time* type; `time` stays micros.
- "Nanosecond time travel": the spec has none. Snapshot `timestamp-ms`, `last-updated-ms`, snapshot-log, and the time-travel note (Appendix F `#point-in-time-reads-time-travel`) are all milliseconds. Time travel = pick the last snapshot-log entry at or before T; "If no snapshot exists prior to the timestamp given or 'snapshot-log' is not populated ... raise an informative error".

### 2.2 `unknown` type

(a) "Default / null column type used when a more specific type is not known" -- "Must be optional with `null` defaults; not stored in data files."

(b) Reader: "When reading an `unknown` column, any corresponding column must be ignored and replaced with `null` values" (Parquet section). Parquet/ORC: "Omit from data files"; Avro: "`null` or omitted". Binary single-value serialization: "Not supported" (so no bounds). Hash: always null.

(c) Writer: never write the column. Must be optional; `initial-default`/`write-default` must be null.

(d) JSON: `"unknown"`.

(e) Promotion `unknown -> any type` (v3+). After promotion, older files still lack the column -> projection rules yield `initial-default`... but because the field was added as `unknown` its initial-default is null, so old rows read null (the spec does not say whether an initial-default may be set during promotion; since `initial-default` "cannot change", it stays null). On main, `unknown` is now explicitly listed as a valid **identity** partition source (result always null). Not allowed in constraint keys (v4).

### 2.3 `variant`

(a) "The variant type and binary encoding are defined in the Parquet project (VariantEncoding.md), with support currently available for V1." Clarified on main (Jul 2026, #16836): "As a semi-structured type, `variant` is neither a primitive type nor a nested type." Therefore anything scoped to "primitive" (partition sources, identifier fields, constraint keys, identity/bucket) excludes it.

(b) Reader:
- Parquet: `group` annotated `VARIANT` with `metadata` and `value` fields; "`metadata` and `value` must not be assigned field IDs and the fields are accessed through names." Shredding (`typed_value`) is allowed in Parquet per VariantShredding.md, so a reader must reassemble shredded values (and may push down on shredded columns). The variant group itself carries the Iceberg field id.
- Avro: `record` with `metadata` and `value` (no field ids); "Shredding is not supported in Avro."
- ORC: `struct` with `iceberg.struct-type=VARIANT`; "Shredding is not supported in ORC."
- Null semantics (v4 content-stats text, but applies conceptually): "A `variant` value is null only when the variant itself is null; a non-null variant that encodes a variant null is not counted as a null value."
- Bounds: see 2.10.

(c) Writer: defaults must be null. Cannot be partition/sort source (not primitive; no transform accepts it; no hash defined). Equality deletes: identifier/equality restrictions are primitive-only, so a variant cannot be an equality column.

(d) JSON: `"variant"`. REST OpenAPI added a distinct `VariantType` (const `"variant"`) in 2026 (#17256).

(e) Binary single-value serialization: "Not supported" generally, but **bound serialization** is defined (2.10). JSON single-value: no entry (defaults are always null).

### 2.4 `geometry(C)` and `geography(C, A)`

(a) OGC Simple Features; geometry edges always linear/planar, default CRS `OGC:CRS84`; geography default CRS `OGC:CRS84`, default algorithm `spherical`; algorithms: `spherical`, `vincenty`, `thomas`, `andoyer`, `karney`.
- CRS strings: `<context>:<identifier>` (e.g. `OGC:CRS84`, `EPSG:4326`, `SRID:0`) or `projjson:<property-name>` referring to a table property holding PROJJSON. "CRS value must not contain inlined PROJJSON definitions and implementations must not parse the contents of the CRS as PROJJSON." Geography custom CRS "must be geographic" with lon [-180,180], lat [-90,90].
- Appendix G: WKB/WKT per OGC SFA 1.2.1 (ISO WKB supporting XY, XYZ, XYM, XYZM). Coordinate order X (lon/easting), Y (lat/northing), Z, M.

(b) Reader: Parquet `binary` with logical type `GEOMETRY` / `GEOGRAPHY` (Parquet's own GEOMETRY/GEOGRAPHY logical types carry crs/algorithm parameters; the Iceberg spec table does not say to populate them -- writer should set them consistent with the Iceberg type). Avro `bytes` (WKB). ORC `binary` with `iceberg.binary-type=GEOMETRY|GEOGRAPHY`.
- Bounds are bounding-box points (2.10). For geography, xmin > xmax means antimeridian wrap: match if x >= xmin OR x <= xmax.

(c) Writer: defaults must be null. Not valid sources for identity, bucket, truncate, or time transforms (main lists source types explicitly and excludes them) -- i.e. only `void` (or an unknown future transform) applies. Not identifier fields? The identifier-field rules don't list them explicitly in v3 (only v4 constraint rules exclude geo "because Iceberg does not define equality for them"); treat as not usable for identifier/equality-delete columns.

(d) JSON: `"geometry(<C>)"`, `"geography(<C>, <A>)"` -- canonical with a space after the comma (#16799); readers "should accept optional whitespace around parameters and separators". Examples in the table use `srid:4326` (lower case). Plain `"geometry"` / `"geography"` (no params) means defaults -- this is implied by "If not specified, C is OGC:CRS84"; the Java parser accepts the bare form (from memory).

(e) Binary single-value: WKB. JSON single value: WKT string (e.g. `POINT (30 10)`) -- only relevant to null defaults, so effectively unused.

### 2.5 Default values (`initial-default`, `write-default`) (`#default-values`)

(a) Spec:
- "`initial-default` is used to populate the field's value for all records that were written before the field was added to the schema"; "`write-default` is used ... for any records written after the field was added ... if the writer does not supply the field's value".
- "The `initial-default` is set only when a field is added to an existing schema. The `write-default` is initially set to the same value as `initial-default` and can be changed through schema evolution."
- "If either default is not set for an optional field, then the default value is null".
- "Omitting a known field when writing a data file is never allowed. The write default for a field must be written if a field is not supplied to a write. If the write default for a required field is not set or the expression evaluates to `null`, the writer must fail."
- "All columns of `unknown`, `variant`, `geometry`, `geography`, and `file` types must default to null. Non-null values for `initial-default` or `write-default` are invalid."
- Nested structs: the struct's own default may only be `null` or `{}` (empty struct); sub-field defaults live on the sub-fields. Effective default = new struct with each field's default. Table in spec: `point` default null + data missing -> null; `{}` + missing -> `{"x":0,"y":0}`; partial data `{"x":3}` -> `{"x":3,"y":0}`.
- Evolution rules: initial-default must be set when a field is added and **cannot change**; write-default must be set at add and may change; adding a required field requires both non-null; adding an optional field may use null defaults "and should be explicitly set"; missing struct field values in a struct default fall back to the field's own default.

(b) Reader: when a field id is absent from a data file (and no identity-partition value / name-mapping hit), return `initial-default`. Applies at any nesting depth inside structs (including inside list elements / map values -- the spec says "struct fields (both nested structs and the top-level schema's struct)").
- Note the projection precedence: identity partition value and name mapping come **before** initial-default.

(c) Writer: fill missing input columns with `write-default`; fail for required without default. Avro data files: convert non-null Iceberg defaults into Avro field defaults.

(d) Schema JSON per field: `"initial-default": <JSON single value>`, `"write-default": <JSON single value>` (v4: may be a value-expression object). JSON single-value encodings (Appendix D `#json-single-value-serialization`):
| type | JSON | example |
|---|---|---|
| boolean | bool | `true` |
| int / long | number | `34` |
| float / double | number | `1.0` |
| decimal(P,S) | string | `"14.20"`, `"2E+20"` -- digits right of the point = scale; negative scale uses scientific notation with exponent = -scale |
| date | string ISO-8601 | `"2017-11-16"` |
| time | string, micros | `"22:31:08.123456"` |
| timestamp | string, micros, no offset | `"2017-11-16T22:31:08.123456"` |
| timestamptz | string, micros, offset `+00:00` | `"2017-11-16T22:31:08.123456+00:00"` |
| timestamp_ns | string, nanos, no offset | `"2017-11-16T22:31:08.123456789"` |
| timestamptz_ns | string, nanos, `+00:00` | `"2017-11-16T22:31:08.123456789+00:00"` |
| string | string | `"iceberg"` |
| uuid | lowercase string | `"f79c3e09-677c-4bbd-a479-3f349cb785e7"` |
| fixed(L) / binary | hex string | `"000102ff"` |
| struct | object keyed by **field id** | `{"1": 1, "2": "bar"}` |
| list | array | `[1, 2, 3]` |
| map | `{"keys": [...], "values": [...]}` | |
| geometry / geography | WKT string | `POINT (30 10)` |
(no entries for unknown/variant: defaults must be null)

(e) Edge cases:
- Old (v2) readers ignore defaults: "Old readers will fail to read required fields which are populated by `initial-default`".
- Ambiguity: whether `time`/`timestamp` JSON must always carry all 6 (or 9) fractional digits. The examples show full precision; Java's parser accepts ISO variants (from memory). Write full precision; parse leniently.
- Ambiguity: decimal string for a value with fewer fractional digits than the scale (e.g. `"14.2"` for scale 2) -- spec says the digit count "is used to indicate scale", so a strict reader would reject it; emit exactly `scale` digits.
- A struct default stored as `{}` must contain **no** field values ("must not contain default values for the struct's fields").

### 2.6 Row lineage (`#row-lineage`)

(a) "In v3 and later, an Iceberg table must track row lineage fields for all newly created rows. Engines must maintain the `next-row-id` table field and the following row-level fields when writing data files": `_row_id` (id 2147483540, long) and `_last_updated_sequence_number` (id 2147483539, long). Row lineage is mandatory in v3 -- there is no table property to turn it off in the spec.
- Not tracked through equality-delete updates: "treated as if the existing row was completely removed and a unique new row was added."

(b) Reader (for a data file with entry `first_row_id` F, data sequence number S, row position P):
- `_row_id` = stored non-null value, else F + P (`_pos` = position in the physical data file, starting 0, **before** deletes are applied).
- `_last_updated_sequence_number` = stored non-null value, else S (the file's *data* sequence number, after inheritance).
- If the columns are absent from the file: "readers should treat both columns as if they exist and are set to null for all rows" (then inherit).
- If F is null (snapshot predates upgrade): both columns read null ("When `first_row_id` is null, inherited row ID values are also null"; Appendix E: "readers must produce null for `_row_id` and `_last_updated_sequence_number`").
- First Row ID inheritance for entries: "the `first_row_id` is assigned by replacing `null` with the manifest's `first_row_id` plus the sum of `record_count` for all data files that preceded the file in the manifest that also had a null `first_row_id`." Note: preceding files of *any* status (EXISTING, DELETED too, as written) with null first_row_id consume ID space -- the sum is over entries in file order, not only ADDED ones. Delete files always have null `first_row_id` and do not participate (they live in delete manifests anyway).
- If the manifest's `first_row_id` is null (pre-upgrade), entries' first_row_id stays null.

(c) Writer:
- New rows: write `_row_id` and `_last_updated_sequence_number` as null, or omit the columns entirely ("A data file with only new rows for the table may omit").
- Rows moved to a new file (compaction, copy-on-write, MERGE): "The row's existing non-null `_row_id` must be copied into the new data file"; if modified, `_last_updated_sequence_number` must be null; if not modified, copy the existing non-null value. "Engines may model operations as deleting/inserting rows or as modifications to rows that preserve row ids." -- this means a writer of rewrites must *read* the inherited values (materialise F+P and S) and write them physically.
- Manifests: new data files written with `first_row_id = null`. "The inherited value of `first_row_id` must be written into data file metadata for all entries with a status other than `ADDED`" (i.e., when re-writing an entry as EXISTING or DELETED, materialise it). "Any null (unassigned) `first_row_id` must be assigned via inheritance, even if the data file is existing."
- Manifest list (`#snapshot-root-file-first-row-id-assignment`): preserve `first_row_id` of existing manifests; delete manifests always null; assign only to data manifests lacking one: first such manifest gets >= snapshot `first-row-id`, each next >= previous + (sum of `record_count` of that manifest's data files with null `first_row_id`). Recommended: `prev.first_row_id + prev.added_rows_count + prev.existing_rows_count`.
- Snapshot: `first-row-id` = table `next-row-id` at each commit attempt ("If a commit is retried, the `first-row-id` must be reassigned"); "required even if a commit does not assign any ID space". `added-rows` = upper bound of IDs assigned (may exceed actually-added rows, e.g. includes EXISTING rows in new manifests).
- Table: "When a new snapshot is added, the table's `next-row-id` should be increased by the sum of `record_count` for all data files that will be assigned a `first_row_id` via inheritance"; "must always be higher than any assigned row ID"; must be updated "even if the new snapshot is not in the main branch". Appendix E: "must be incremented by at least the number of newly assigned row ids".
- Worked example (spec): next-row-id 1000; new manifests added1 (100 added + 25 existing) -> 1000, added2 (0 + 100) -> 1125, added3 (125 + 25) -> 1225; new next-row-id 1375; inside added1: EXISTING data1 keeps 800; ADDED data2 null(=1000), data3 null(=1050). Note the example's `added-rows` text says "100 (50 + 50)" while next-row-id advances by 375 -- the example is internally inconsistent about what `added-rows` holds vs the "upper bound" definition (Java sets added-rows = next-row-id delta, from memory). **Use added-rows = the ID space consumed (375 in the example) so that `next-row-id += added-rows` is safe**, matching "It can be used safely to increment the table's `next-row-id`".

(d) Fields: table `next-row-id` (long, required v3); snapshot `first-row-id`, `added-rows` (required v3); manifest_file `520 first_row_id` (long, opt); data_file `142 first_row_id` (long, opt); reserved data columns 2147483540 `_row_id`, 2147483539 `_last_updated_sequence_number`.

(e) Upgrade v2 -> v3 (`#row-lineage-for-upgraded-tables`):
- `next-row-id` initialised to 0; existing snapshots unchanged (no `first-row-id`), so v3 snapshot "required" fields are absent on historic snapshots -> readers must tolerate missing `first-row-id`/`added-rows` on snapshots of an upgraded table.
- The first post-upgrade snapshot "must ... assign row IDs to existing and added files"; "all data manifests must be assigned a `first_row_id`" -- i.e. every carried-over manifest without first_row_id gets one, which can make `added-rows` equal the whole table's row count.
- Branches: post-upgrade snapshots on different branches assign disjoint ranges; a file in multiple branches may keep another branch's first_row_id or get a new one.
- "Existing rows will inherit `_last_updated_sequence_number` from their containing data file."
- Interaction with DVs: `_row_id` uses physical position, so deleted rows' IDs are simply never observed; IDs are not compacted.
- Interaction with equality deletes: an upsert via equality delete yields a new `_row_id`.
- Commit retries: manifests can be reused because first_row_id lives in the manifest list; only the manifest list and snapshot are rewritten with the new `first-row-id`.
- CDC: reserved columns `_change_type` (2147483543), `_change_ordinal` (2147483542), `_commit_snapshot_id` (2147483541) exist for changelog views but are not specified further.

### 2.7 Deletion vectors (`#deletion-vectors`, Puffin `#deletion-vector-v1-blob-type`)

(a) Spec:
- "v3 forbids new position delete files and allows deletion vectors as a replacement" (main wording; Appendix E: "Writers are not allowed to add new position delete files to v3 tables").
- "there can be at most one deletion vector for a given data file in a snapshot. Writers must ensure that there is at most one deletion vector per data file and must merge new deletes with existing vectors or position delete files."
- "If a DV is written for a data file, it must replace all previously written position delete files so that when a DV is present, readers can safely ignore matching position delete files."
- "When removing a data file, writers must also remove any deletion vector that applies to that data file from delete manifests. Writers are not required to rewrite Puffin files that contain the removed deletion vectors."
- "There are no restrictions on the data files that can be referenced by deletion vectors in the same Puffin file." (one Puffin file may hold DVs for many data files, across partitions)
- Positions: positive 64-bit (MSB must be 0). Key = high 32 bits, sub-position = low 32 bits, one 32-bit Roaring bitmap per key.

(b) Reader:
- Locate DV via delete manifest entry: `content = 1`, `file_format = "puffin"`, `file_path` = Puffin file, `referenced_data_file` = data file path, `content_offset`/`content_size_in_bytes` = blob location. Read exactly that byte range (no need to parse the Puffin footer).
- Apply when referenced path equal, data seq <= DV seq, same spec and partition.
- If any DV applies to a data file, ignore position delete files for it.
- Still apply equality deletes (v3 allows them).
- Validate: length prefix, magic, CRC (recommended).

(c) Writer:
- Produce at most one DV per data file per snapshot; when deleting from a file with an existing DV or v2 position deletes, read them, union, write a new DV, and remove the old DV entry (DELETED status) in the same commit. Position delete files that cover *other* data files too must stay until all their deletes are superseded by DVs ("need to be kept in table metadata until all deletes are replaced by DVs").
- Conflict detection (practical consequence): two concurrent commits adding DVs for the same data file conflict -- a retry must re-merge (the spec states the invariant; the Java implementation validates "no new DVs for the same data file", from memory).
- Manifest entry fields for a DV: `content=1`, `file_path`=puffin path, `file_format="puffin"`, `partition`=the data file's partition (same spec), `record_count`=cardinality, `file_size_in_bytes`=Puffin file size, `referenced_data_file` (required), `content_offset` and `content_size_in_bytes` (required; "must exactly match the `offset` and `length` stored in the Puffin footer"), `sort_order_id` null.
- Puffin blob metadata for the DV: `type = "deletion-vector-v1"`, `snapshot-id = -1`, `sequence-number = -1` ("must be set to -1 in blob metadata for Puffin v1"), `fields` (required list; spec does not say what to put -- Java writes `[2147483645]` i.e. `_pos`, from memory; verify), `offset`, `length`, **no `compression-codec`**, `properties` MUST include `referenced-data-file` (equal to the data file location in metadata) and `cardinality` (string decimal).

(d) **Blob byte layout (deletion-vector-v1)**:
```
offset 0   : uint32 BIG-endian  L = 4 (magic) + len(vector)      // "Combined length of the vector and magic bytes"
offset 4   : magic D1 D3 39 64
offset 8   : vector = Roaring 64-bit "portable" serialization (LITTLE-endian):
               uint64 LE  number of 32-bit bitmaps (N)
               repeat N times, ordered by UNSIGNED key:
                 uint32 LE  key (high 32 bits of position)
                 32-bit Roaring bitmap, standard RoaringFormatSpec "general layout"
                   (cookie 12346 = no run containers, or 12347 = has run containers; array/bitmap/run containers)
offset 8+V : uint32 BIG-endian CRC-32 over (magic + vector)       // i.e. bytes [4, 8+V)
total blob length = 4 + 4 + V + 4 = content_size_in_bytes = Puffin BlobMetadata.length
content_offset = Puffin BlobMetadata.offset = position of the 4-byte length prefix
```
- "Note that the length and CRC fields are stored using big-endian, but the Roaring bitmap format uses little-endian values. Big endian values were chosen for compatibility with existing deletion vectors in Delta tables."
- CRC-32: the spec says only "CRC-32"; the intended polynomial is the standard IEEE/zlib CRC-32 (`java.util.zip.CRC32`, same as Delta). (Relevant for EW: the bitmap body matches Delta's 64-bit portable RoaringBitmapArray format; Delta's DV descriptor `sizeInBytes` counts magic+bitmap only and its `offset` points at the length prefix, whereas Iceberg's `content_size_in_bytes` covers the full blob incl. the length prefix and CRC -- from memory of the Delta protocol; verify before sharing code.)

Puffin file structure (`#file-structure`):
```
Magic(50 46 41 31 "PFA1") Blob1 ... BlobN Footer
Footer = Magic FooterPayload FooterPayloadSize(int32 LE) Flags(4 bytes) Magic
Flags byte0 bit0 = FooterPayload compressed (LZ4 single frame with content size); other bits/bytes 0
FooterPayload = UTF-8 JSON FileMetadata { "blobs": [BlobMetadata...], "properties": {...} }
BlobMetadata = { type, fields[int], snapshot-id, sequence-number, offset, length, compression-codec?, properties? }
```
- Blob compression codecs: `lz4` (single frame, content size present), `zstd` (single frame, content size present), or uncompressed. Recommended FileMetadata property `created-by`.
- Other blob type: `apache-datasketches-theta-v1` (compact Theta sketch, values fed via Appendix D single-value serialization; optional `ndv` property). Used by table `statistics`.

(e) Edge cases:
- v2 -> v3 upgrade: existing position delete files remain valid; first DV for a data file must absorb them (including "file-scoped" ones with `referenced_data_file` and multi-file ones).
- DV and a data file must be in the same partition (spec+values) for the DV to apply -- a writer that rewrites partition specs must keep DVs aligned.
- DV data sequence number vs. data file: DV added in same commit as the file applies (<=).
- Equality deletes are still allowed in v3 (forbidden for new writes only in v4).
- A DV with cardinality equal to record_count: spec doesn't require converting to a file removal; writers typically drop the data file instead.
- Delete-file metrics for a DV: spec's general rule (bounds for all deleted rows or none) applies; not specified which columns. Writers may omit metrics.

### 2.8 Multi-argument transforms and `source-ids`

(a) v3 adds `source-ids` (JSON list of ints) to partition fields and sort fields. "For partition fields with a transform with a single argument, only `source-id` is written. In case of a multi-argument transform, only `source-ids` is written." Appendix E: "`source-ids` was added and must be written in the case of a multi-argument transform. `source-id` must be written in the case of single-argument transforms." Requirement columns: `source-id` became *optional* in v3, `source-ids` optional.

(b) Reader: accept either form. A field with `source-ids` whose transform is unknown must be ignored for filtering (but kept for partition equality). Type-promotion restriction applies to `source-ids` too.

(c) Writer: emit `source-id` (not `source-ids`) for every currently-defined transform.

(d) No manifest change; partition tuple field id = partition field id as before.

(e) **No multi-argument transform is defined in v3** (all transforms in `#partition-transforms` are single-argument). The feature is purely forward-compatible syntax: an implementation must parse `source-ids` (possibly with a single element? spec doesn't forbid `source-ids: [x]` for single-arg transforms but says only `source-id` is written) and treat unknown multi-arg transforms as unknown.

### 2.9 New transforms / partition and sort on new types

Source-type table (main, `#partition-transforms`, made explicit 2026-11-21 by #18545):
| transform | sources |
|---|---|
| identity | unknown, boolean, int, long, float, double, decimal, date, time, timestamp, timestamptz, **timestamp_ns, timestamptz_ns**, string, uuid, fixed, binary |
| bucket[N] | int, long, decimal, date, time, timestamp, timestamptz, **timestamp_ns, timestamptz_ns**, string, uuid, fixed, binary |
| truncate[W] | int, long, decimal, string, binary |
| year / month / day | date, timestamp, timestamptz, timestamp_ns, timestamptz_ns |
| hour | timestamp, timestamptz, timestamp_ns, timestamptz_ns |
| void | any |
- variant, geometry, geography: no transform except `void` (1.12.0 said identity = "Any primitive except for `geometry` and `geography`").
- `unknown` is newly listed as an identity source on main (it was implicitly "any primitive" before).
- Sort orders use the same transforms, so the same source restrictions apply.
- No new transform *names* in v3. Time transforms on ns values produce the same int results as on micros (years/months/days/hours since epoch).
- Commit message for #18545: "New primitive types should not silently become identity sources" -- i.e. future types must be added explicitly.

### 2.10 Statistics / bounds for new types

v3 continues to use the metrics maps (125/128 etc.). Appendix D `#bound-serialization` overrides the binary single-value encoding for:

- **geometry / geography**: "A single point, encoded as a x:y:z:m concatenation of its 8-byte little-endian IEEE 754 coordinate values. x and y are mandatory. This becomes x:y if z and m are both unset, x:y:z if only m is unset, and x:y:NaN:m if only z is unset." So lengths are 16, 24, or 32 bytes; a 32-byte value with NaN z means "no z". Lower bound = (xmin, ymin[, zmin][, mmin]); upper = (xmax, ymax, ...).
  - Computing (`#bounds-for-geometry-and-geography`): "null or NaN values in a coordinate dimension are skipped"; a dimension with only null/NaN values is omitted; "If either the X or Y dimension is missing then the bounding box itself is not produced."
  - Geography: may wrap (xmin > xmax); X restricted to [-180,180], Y to [-90,90]. Bounds for geography must contain all *edges* as interpolated (a geodesic edge can bulge beyond its vertices' latitudes) -- the spec only says "a bounding box that contains all objects", so naive vertex min/max is not conservative for geography; Java writes no geography bounds by default? (unverified -- check). Geometry bounds: vertex min/max is exact because edges are planar.
  - Never "tight" (v4 wording).
- **variant**: "A serialized Variant of encoded v1 metadata concatenated with an encoded variant object. Object keys are normalized JSON paths to identify fields; values are lower or upper bound values."
  - Paths: normalized JSON path, e.g. `$`, `$['event_type']`, `$['user.name']`, `$['location']['latitude']`, `$['tags']` (bounds over all array elements), `$['addresses']['zip']` (field inside objects inside an array -- array levels are NOT indexed).
  - "Including bounds for any field is optional and upper and lower bounds must have the same Variant type." "Bounds must not be written to describe values with mixed Variant types (other than null)." Bounds "must be accurate for all non-null values"; inside arrays "accurate for all values in the array".
  - `$` = the whole variant is a uniform primitive.
  - Decoding: the Variant metadata is self-describing (header byte with offset_size, dictionary_size, offsets, then strings), so the reader computes its length and the object value starts immediately after it.
- **timestamp_ns / timestamptz_ns**: 8-byte LE nanos.
- **unknown**: no bounds ("Not supported").
- Float/double: -0.0 precedes +0.0 (totalOrder); NaN never a bound.
- String/binary bounds may be truncated (lower: prefix; upper: incremented prefix) -- implied, not spelled out in v3 text; v4 says string/binary bounds "are never tight because they may be truncated".
- `value_counts` for variant / geo columns are keyed by the column's field id (the variant group id); for shredded variants, Parquet column-chunk stats of `typed_value` leaves are not Iceberg field ids (they have no ids) and must not be emitted as Iceberg column stats under invented ids.
- New on main for v3 tables: **`stats-fields`** (stats-only fields) in table metadata (`#stats-only-fields`): `{ "field-id", "type": "partition-value"|"expr-value", "data-type"?, "partition-field-id"? | "expr"? }`. "Readers must not fail when an unsupported stats-only field `type` is found"; "Writers must preserve existing stats for stats-only fields listed in a table's `stats-fields` if the `data-type` is known". Field ids come from `last-column-id`. In v3 these stats would live in the same id-keyed metrics maps. Its v3 status is new (2026-11-02) and absent from 1.12.0.

### 2.11 Table encryption keys and key metadata (`#encryption-keys`)

(a) Table metadata `encryption-keys` (v3 optional) list of:
| field | type | req |
|---|---|---|
| key-id | string | required |
| encrypted-key-metadata | string (base64) | required |
| encrypted-by-id | string | optional ("ID of the key used to encrypt or wrap `key-metadata`") |
| properties | map<string,string> | optional |
- Snapshot `key-id` (v3 optional): "ID of the encryption key that encrypts the manifest list key metadata".
- Existing (since v1/v2) `key_metadata` fields: data_file 131, manifest_file 519, statistics `key-metadata`.
- "The format of encrypted key metadata is determined by the table's encryption scheme and can be a wrapped format specific to the table's KMS provider."

(b) Reader: to read an encrypted table, resolve snapshot `key-id` -> `encryption-keys` entry -> unwrap (possibly via `encrypted-by-id` chain, KMS) -> decrypt the manifest list's key metadata -> decrypt manifest list (AES GCM Stream for Avro) -> `manifest_file.key_metadata` -> manifests -> `data_file.key_metadata` -> data files (Parquet Modular Encryption for Parquet; AGS1 for Avro). None of this chain beyond the field definitions is normative in the table spec.

(c) Writer: unencrypted tables simply omit everything. A writer that does not support encryption must refuse to write to a table that has `encryption-keys`/`key-id` (or encrypted key_metadata) rather than write plaintext files -- spec doesn't say this; it's the safe choice.

(d) AES GCM Stream (format/gcm-stream-spec.md): `Magic "AGS1" (41 47 53 31) | BlockLength int32 LE | CipherBlock...`; block = 12-byte nonce + ciphertext + 16-byte tag; AAD = file AAD prefix + block ordinal (int32 LE); file length must come from trusted metadata (e.g., manifest_length / file_size_in_bytes), not the file system.

(e) Ambiguities: the Appendix C example writes `{"key-id": "5f819b", "key-metadata": "aWNlYmVyZwo="}` while the field table and REST OpenAPI use `encrypted-key-metadata`. **Use `encrypted-key-metadata`** (REST `EncryptedKey` schema requires it). Also the description of `encrypted-by-id` refers to "`key-metadata`". The REST `Snapshot` schema has no `key-id` property.

### 2.12 Other v3 metadata changes
- Table: `next-row-id` (req), `encryption-keys` (opt), `stats-fields` (opt, main only).
- Snapshot: `first-row-id` (req), `added-rows` (req), `key-id` (opt).
- Manifest list: `520 first_row_id`.
- Data file: `142 first_row_id`, `144 content_offset`, `145 content_size_in_bytes`; `111 distinct_counts` dropped; `file_format` may be `puffin`.
- Partition field / sort field JSON: `source-ids`; `source-id` optional.
- Partition stats file: fields 6-9 required, `13 dv_count` added (required).
- Schema evolution: unknown->any, date->timestamp/timestamp_ns.
- Unknown partition transforms: reading is required in v3.
- Manifest key-value metadata unchanged from v2 (`format-version` = "3").

---

## 3. Appendix-level mappings for v3 types

### 3.1 Parquet (`#parquet`)
| Iceberg | Parquet physical | logical | notes |
|---|---|---|---|
| unknown | none | | omit |
| timestamp_ns | int64 | TIMESTAMP_NANOS adjustToUtc=false | nanos since epoch |
| timestamptz_ns | int64 | TIMESTAMP_NANOS adjustToUtc=true | |
| variant | group{metadata, value[, typed_value]} | VARIANT | metadata/value **no field ids**, accessed by name; shredding per VariantShredding.md |
| geometry | binary | GEOMETRY | WKB |
| geography | binary | GEOGRAPHY | WKB |
| (v4) file | group of sub-fields with ids | FILE | v4 only |
- Field ids: required on all Iceberg fields (struct fields, list element, map key/value); `_row_id`/`_last_updated_sequence_number` written with their reserved ids (2147483540 / 2147483539) as optional int64 columns when materialised.
- Variant group must be annotated with the VARIANT logical type (parquet-format 2.11+ with specification_version 1).

### 3.2 Avro
| Iceberg | Avro |
|---|---|
| unknown | `null` or omitted |
| timestamp_ns | `{"type":"long","logicalType":"timestamp-nanos","adjust-to-utc":false}` |
| timestamptz_ns | `{"type":"long","logicalType":"timestamp-nanos","adjust-to-utc":true}` |
| variant | record with `metadata` and `value` (bytes), no field ids; no shredding |
| geometry / geography | bytes (WKB) |

### 3.3 ORC
| Iceberg | ORC |
|---|---|
| timestamp_ns | timestamp, `iceberg.timestamp-unit=NANOS` |
| timestamptz_ns | timestamp_instant, `iceberg.timestamp-unit=NANOS` |
| variant | struct{metadata,value}, `iceberg.struct-type=VARIANT`, no ids |
| geometry / geography | binary, `iceberg.binary-type=GEOMETRY/GEOGRAPHY` |

### 3.4 Binary single-value serialization (Appendix D)
| type | bytes |
|---|---|
| unknown | not supported |
| boolean | 0x00 false, non-zero true |
| int / date | 4-byte LE |
| long / time / timestamp(tz) | 8-byte LE (micros) |
| timestamp_ns / timestamptz_ns | 8-byte LE nanos |
| float / double | 4 / 8-byte LE IEEE |
| string | UTF-8, no length |
| uuid | 16 bytes big-endian |
| fixed / binary | raw |
| decimal | unscaled two's complement big-endian, minimum bytes |
| struct / list / map / variant | not supported (variant has a bound-specific encoding) |
| geometry / geography | WKB (bounds override: x:y[:z][:m] LE doubles) |

### 3.5 Schema JSON type strings (Appendix C) -- canonical forms
`"unknown"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"date"`, `"time"`, `"timestamp"`, `"timestamptz"`, `"timestamp_ns"`, `"timestamptz_ns"`, `"string"`, `"uuid"`, `"fixed[16]"`, `"binary"`, `"decimal(9, 2)"` (space after comma is canonical since Aug 2026 #18145; readers accept optional whitespace), struct/list/map objects, `"variant"`, `"geometry(srid:4326)"`, `"geography(srid:4326, spherical)"`. Struct field JSON: `id`, `name`, `required`, `type`, `doc`, `initial-default`, `write-default`.
Partition field JSON: `source-id` | `source-ids`, `field-id`, `name`, `transform`. Sort field JSON: `transform`, `source-id` | `source-ids`, `direction`, `null-order`. Transform strings: `identity`, `bucket[N]`, `truncate[W]`, `year`, `month`, `day`, `hour`, `void`.

### 3.6 Reserved field ids (`#reserved-field-ids`)
"Iceberg tables must not use field ids greater than 2147483447 (`Integer.MAX_VALUE - 200`)."
| id | name | type |
|---|---|---|
| 2147483646 | _file | string |
| 2147483645 | _pos | long |
| 2147483644 | _deleted | boolean |
| 2147483643 | _spec_id | int |
| 2147483642 | _partition | struct |
| 2147483546 | file_path | string (position delete files) |
| 2147483545 | pos | long (position delete files) |
| 2147483544 | row | struct (position delete files) |
| 2147483543 | _change_type | string (INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER) |
| 2147483542 | _change_ordinal | int |
| 2147483541 | _commit_snapshot_id | long |
| 2147483540 | _row_id | long (v3 row lineage) |
| 2147483539 | _last_updated_sequence_number | long (v3 row lineage) |
(v4 adds stats-id ranges: `_last_updated_sequence_number` base 9000-9199, `_row_id` 9200-9399, content-stats ids 10_000 .. 200_000_000 exclusive.)

---

## 4. REST catalog (OpenAPI) -- what matters for v3

From open-api/rest-catalog-open-api.yaml on main:
- `TableMetadata.format-version`: `minimum: 1, maximum: 3`. Properties include `next-row-id`, `encryption-keys` (items `EncryptedKey`), `statistics`, `partition-statistics`. **No `stats-fields`** yet.
- `EncryptedKey`: required `key-id`, `encrypted-key-metadata` (string, base64); optional `encrypted-by-id`, `properties`.
- `Snapshot`: required `snapshot-id`, `timestamp-ms`, `manifest-list`, `summary`; optional `parent-snapshot-id`, `sequence-number`, `first-row-id`, `added-rows`, `schema-id`. **No `key-id`** property.
- Table updates relevant to v3: `upgrade-format-version` {format-version}, `add-snapshot` {snapshot} (carries `first-row-id`/`added-rows`), `add-encryption-key` {encryption-key: EncryptedKey}, `remove-encryption-key` {key-id}, plus all existing ones (add-schema, set-current-schema, add-spec, set-default-spec, add-sort-order, set-default-sort-order, set-snapshot-ref, remove-snapshots, remove-snapshot-ref, set-location, set/remove-properties, set/remove-statistics, set/remove-partition-statistics, remove-partition-specs, remove-schemas, assign-uuid).
- Requirements: `assert-create`, `assert-table-uuid`, `assert-ref-snapshot-id`, `assert-last-assigned-field-id`, `assert-current-schema-id`, `assert-last-assigned-partition-id`, `assert-default-spec-id`, `assert-default-sort-order-id`. **There is no `assert-next-row-id` or row-lineage requirement.** The client sends a snapshot whose `first-row-id` it computed from the `next-row-id` it loaded; the server's metadata builder must validate/advance `next-row-id` (Java's `TableMetadata.Builder.addSnapshot` rejects a snapshot whose first-row-id is behind next-row-id and adds `added-rows`, from memory). Because a concurrent commit changes `next-row-id`, a client must pair row-lineage commits with `assert-ref-snapshot-id` (which it normally does) and recompute `first-row-id`/manifest-list `first_row_id`s on retry.
- "Server implementations are required to fail with a 400 status code if any unknown updates or requirements are received."
- Types: `VariantType` (const "variant") added separately from `PrimitiveType`; `StructField` has `initial-default` / `write-default`.
- Scan planning API content files: `DataFile` has `first-row-id`; `PositionDeleteFile` has `content-offset` / `content-size-in-bytes` but **no `referenced-data-file`** and no distinct DV discriminator (DVs are `position-deletes` with `file-format: puffin`; association to data files is via the file scan task's delete-file references). `ContentFile.content-stats` / `FieldStatistics.total-bytes` reflect the v4 stats work.
- View spec: view `format-version` "must be 1"; nothing changes for table v3. Views carry schemas, so v3 types/defaults may appear in view schemas.

---

## 5. v4 draft (brief) -- what is coming and what touches v3 readers

Status: "under active development and has not been formally adopted" (Appendix E `#version-4`). Changes on main:
- **Adaptive metadata tree**: manifest list replaced by a single Parquet **root manifest** per snapshot (`root-manifest` snapshot field); manifests are Parquet; `tracked_file` records with a `tracking` struct (status 0 EXISTING, 1 ADDED, 2 DELETED, 3 REPLACED, 4 MODIFIED; `modified_snapshot_id`, `deleted_positions`, `replaced_positions`), `manifest_info`, `spec_id` (141) per entry; a manifest may mix partition specs; root may reference v1-v3 manifests.
- **Colocated DVs**: `deletion_vector` struct (155 location, 144 offset, 145 size_in_bytes, 156 cardinality, 149 key_metadata) on the data file entry; no new delete manifests; "Row-level deletes may only be written in v4 as deletion vectors".
- **Manifest DVs** using the new "Mumbling" bitmap spec (format/mumbling-spec.md).
- **Column files** (`column_files`, 158-165) for column updates; data sequence number changes when a column file is added.
- **Content stats** (`content_stats` 146): typed per-field stats structs at ids `10_000 + 200*field-id` (+1 lower, +2 upper, +3 value_count, +4 null_value_count, +5 nan_value_count, +6 `total_bytes`; geo structs at +10..+17); file-level `tight_stats` (151) replaced per-column `tight_bounds` (Oct 2026); `avg_value_size_in_bytes` renamed to `total_bytes` (Sep 2026).
- **Relative paths** in metadata; `location` optional in table metadata.
- **Equality deletes forbidden** for new writes (existing ones still applied).
- **`file` type** (7 field ids; Parquet FILE logical type).
- **Constraints** (`constraints`, `last-constraint-id`, snapshot `constraint-statuses`); `identifier-field-ids` replaced by a non-enforced `primary-key` constraint on upgrade.
- **Default value expressions** for `write-default` (`current_timestamp()` etc.; no field references).
- **Monotonic snapshot timestamps**: child `timestamp-ms` strictly greater than parent on the same branch.
- File-system (rename) tables to be removed.
- Things added to the spec under v4 work that also appear in the v1-v3 tabs: `stats-fields` (v3 optional), reserved data_file id 141.

---

## 6. Upgrade v2 -> v3: consolidated checklist
1. Set `format-version: 3`; set `next-row-id: 0` (spec text: initialised to 0).
2. Do not touch existing snapshots/manifests; historic snapshots lack `first-row-id`/`added-rows` (readers tolerate).
3. First v3 snapshot: assign `first_row_id` to **every** data manifest in the new manifest list that lacks one (including carried-over manifests), set snapshot `first-row-id`, `added-rows`, and advance `next-row-id`.
4. From then on: no new position delete files; first delete touching a data file writes a DV that merges any existing position deletes for that file; multi-file position delete files stay until fully superseded.
5. Partition stats files written after upgrade use v3 requirements (6-9, 13 required) and count v2 position deletes + DVs in field 6.
6. Equality deletes remain legal.
7. Manifests written after upgrade: `format-version` key "3"; may include v3 fields 142/144/145; omit 111.
8. Snapshot-id `-1` sentinel no longer written by Java for v3 (`current-snapshot-id` null/omitted).
9. Readers must now (if not already) read tables with unknown transforms.

---

## 7. Ambiguities, inconsistencies and gaps found while reading

1. Encryption key JSON field name: `encrypted-key-metadata` (field table, REST) vs `key-metadata` (Appendix C example and `encrypted-by-id` description).
2. Row lineage example: `added-rows` described as 100 while `next-row-id` advances 375; definition says "upper bound of the number of rows with assigned row IDs" and "can be used safely to increment `next-row-id`". Implement as the ID span consumed.
3. First Row ID inheritance counts "all data files that preceded the file in the manifest that also had a null `first_row_id`" -- regardless of status. A DELETED entry with null first_row_id (possible only if written incorrectly, since non-ADDED entries must carry materialised values) would consume space. Readers should implement the literal rule.
4. Multi-argument transforms exist only as syntax; no v3 transform uses `source-ids`.
5. ns->us conversion for bucket hashing (and floor semantics for time transforms) unspecified for negative values.
6. Puffin DV `fields` content unspecified; CRC-32 polynomial unspecified (use IEEE).
7. Geography bounding boxes must cover interpolated edges, not just vertices -- not stated; naive min/max may be non-conservative.
8. Variant bound path normalisation: escaping of `'` or `\` inside names in `$['...']` is not specified (the spec references "normalized JSON path" -- RFC 9535 normalized paths escape `'` as `\'`).
9. Geometry type string without params (`"geometry"`) is not shown in Appendix C; defaults are described in the type table.
10. JSON single-value for time/timestamps: fractional digit count not stated as mandatory.
11. `stats-fields` added to v3's optional metadata after v3 was "complete" (2026-11-02); 1.12.0 readers don't know it -- the spec has no general "ignore unknown JSON fields" rule, so EW's metadata parser should ignore unknown fields to be forward compatible.
12. Identity source list changed on 2026-11-21: now explicit, includes `unknown`; excludes variant/geo.
13. REST: no `key-id` on Snapshot, no `referenced-data-file` on PositionDeleteFile, `format-version` max 3, no `stats-fields`, no row-lineage requirement.
14. Snapshot `first-row-id`/`added-rows` "required" in v3 yet absent on pre-upgrade snapshots of upgraded tables.
15. `unknown` promotion: whether an `initial-default` may be introduced at promotion time is unstated (initial-default "cannot change" suggests no).
16. Identifier-field restrictions in v3 do not mention geometry/geography (only v4 constraints exclude them "because Iceberg does not define equality for them").

## 8. Notable spec commits since 1.12.0 (2026-09-30) affecting v1-v3 text
- 2026-11-21 #18545 identity and bucket source types listed explicitly.
- 2026-11-06 #16777 default value expressions (v4 only; JSON `write-default` may be an expression object in v4).
- 2026-11-04 #18301 row-level delete sections reworded (DVs "current representation"; position deletes "prohibited" in v3; equality "prohibited" in v4).
- 2026-11-02 #18405 `stats-fields` in table metadata (listed for v3 as optional).
- 2026-10-12 #18441 relative links; ORC javadoc link.
- 2026-10-02 #18228 schema versioning includes v3; #17318 `total-records` clarified.
- 2026-09-29 #18308 `avg_value_size_in_bytes` -> `total_bytes` (v4 stats).
- Partition statistics `total_record_count` may be NULL (wording change on main).
- Earlier 2026: #18145/#16798/#16799 canonical decimal/geography type strings with spaces; #17198 content file uniqueness; #16836 variant classification.
