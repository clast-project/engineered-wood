# Known Issues and Feature Gaps

This document tracks feature gaps and known runtime issues, grouped by
feature area. Within each area, gaps are split into **Missing features**
(things the spec defines that we do not implement) and
**Correctness / interop issues** (behavior where our code diverges from
the spec in a way that affects round-tripping with other tools).

For implementation status of the expression library and predicate
pushdown, see [`predicate-pushdown-design.md`](predicate-pushdown-design.md).
For the forward-looking encryption design, see
[`encryption-design.md`](encryption-design.md).

---

## Parquet

### Missing features

**Modular encryption (PARQUET-1375).** Neither reading nor writing of
encrypted Parquet files is implemented. Encrypted test files are skipped
by the test sweeps. See [`encryption-design.md`](encryption-design.md).

**Column Index / Offset Index.** Not parsed on read and not produced on
write. Pushdown granularity is the row group; files we write do not carry
page indexes. Tracked as phases 11–13 in
[`predicate-pushdown-design.md`](predicate-pushdown-design.md).

**LZO compression.** `CompressionCodec.Lzo` is defined and decoded from
Thrift, but `Decompressor.Decompress` in
`src/EngineeredWood.Core/Compression/Decompressor.cs` has no case for it
and throws `NotSupportedException`. Any file with LZO-compressed pages
fails to read.

**Logical / Converted type handling on read:**

- `INTERVAL` (12-byte FLBA) is not mapped to an Arrow type. Files with
  `ConvertedType.Interval` fall through to raw `FixedSizeBinaryType(12)`.
- `BSON` logical type decodes but is not mapped in
  `ArrowSchemaConverter`; falls through to `BinaryType`.

**Logical type emission on write:**

- `UUID` is not emitted. Arrow `FixedSizeBinaryType(16)` is written as a
  plain FLBA with no annotation in `ArrowToSchemaConverter`.
- `JSON` and `ENUM` are never emitted (Arrow has no enum type and we
  always write `StringType` as `StringType`).

**Geospatial and Variant types.** The `GEOMETRY` / `GEOGRAPHY` types
added to the Parquet spec in late 2024 are not supported. The `VARIANT`
logical type is not supported. Neither read nor write paths recognize
these annotations.

**Encoding strategies on write.** The decoder supports
`BYTE_STREAM_SPLIT` for `INT32`/`INT64`/`FIXED_LEN_BYTE_ARRAY`, but
`EncodingStrategyResolver` never emits BSS for these types — only for
`FLOAT`/`DOUBLE`. V1 data pages always emit `PLAIN` regardless of type.

**Arrow types rejected on write.** `ArrowToSchemaConverter.MapArrowType`
throws `NotSupportedException` for `Date64Type`, `IntervalType` (any
unit), `DurationType`, `DictionaryType`, `UnionType`, `FixedSizeListType`,
and `ListViewType`.

**Metadata fields not produced on write:**

- `ColumnMetaData.encoding_stats` (field 13) is never populated.
- `ColumnMetaData.key_value_metadata` (field 8, per-column) is neither
  read nor written.
- `RowGroup.SortingColumns` can be encoded and decoded but
  `ParquetWriteOptions` exposes no API to set it, so callers cannot
  produce sorted row groups with a sort manifest.
- `Statistics.distinct_count` is never computed or written.

### Correctness / interop issues

**Nanosecond time on read.** `ArrowSchemaConverter` maps any non-millis
`TIME` logical type to `Time32Type(Microsecond)`; nanosecond values are
silently truncated to microseconds.

**Deprecated `min`/`max` restricted to signed-order types.** The
deprecated `Statistics.min`/`max` fields (defined with signed byte
ordering) are emitted only for types whose logical ordering IS the
signed ordering (booleans, signed ints incl. date/time/timestamp,
floats); UTF-8 / binary / unsigned / decimal-FLBA columns get only
`min_value`/`max_value` (parquet-mr behavior), so a legacy signed-order
reader can no longer mis-prune them.

### Known runtime issue: concatenated Gzip members on .NET Framework

**Status:** Open — workaround in place, fix requires third-party Gzip
library.
**Affected targets:** `netstandard2.0` when consumed by .NET Framework 4.x.
**Not affected:** .NET 8+, .NET Core 3.0+.

.NET Framework's `System.IO.Compression.GZipStream` does not correctly
handle Gzip streams containing multiple concatenated members as permitted
by RFC 1952. After decompressing the first member, it stops reading and
over-reads the underlying stream, making subsequent members inaccessible.

Some Parquet writers — notably Apache parquet-mr — produce Gzip-compressed
data pages with multiple concatenated Gzip members. Normal Gzip-compressed
Parquet files (single member per page) work correctly on all platforms;
only the concatenated-member edge case is affected.

The test file `parquet-testing/data/concatenated_gzip_members.parquet`
triggers this issue. `GzipCompressed_ReadsTestFile` is skipped on
netstandard2.0/net472 via `#if !NET8_0_OR_GREATER` in `ReadRowGroupTests.cs`.
The round-trip Gzip test (`GzipCompressed_RoundTrip`) passes on all
platforms because our own writer produces single-member pages.

Possible fixes: use SharpZipLib or DotNetZip on netstandard2.0 (same
pattern as BrotliSharpLib for Brotli), or accept the limitation since
concatenated Gzip in pages is rare.

---

## ORC

### Missing features

**Column-level encryption.** Not implemented for read or write. The only
references are the generated protobuf message definitions in
`src/EngineeredWood.Orc/Proto/orc_proto.proto`; nothing consumes them.
See [`encryption-design.md`](encryption-design.md).

**Public predicate-pushdown API.** Internal bloom-filter evaluation
exists (`OrcReader.cs`, `BloomFilter/`), but `OrcReader` exposes no
`Filter`/`Predicate` option comparable to `ParquetReadOptions.Filter`.
Row indexes are written on every stripe but the reader has no
skip-to-row-group or `SeekToRow` API, so the positions are unused. The
shared expression library has not been wired in. Tracked as Phase 14 in
[`predicate-pushdown-design.md`](predicate-pushdown-design.md).

**VARCHAR / CHAR on write.** Arrow has no varchar/char type distinction,
and `OrcWriter.AddType` has no case emitting `Varchar` or `Char` —
Arrow `StringType` always maps to ORC `String`, losing any max-length
information. Reads collapse both to `StringType` too, so max-length is
never surfaced.

**RLE v1 / DictionaryV1 on write.** `RleDecoderV1` is present for read,
but there is no `RleEncoderV1`; `OrcWriterOptions.EncodingFamily` only
exposes `V2` / `DictionaryV2`. Files Hive 0.11 or other V1-only readers
can consume cannot be produced.

**Compression codecs.** `LZO` and `Brotli` are spec-defined
(`orc_proto.proto` includes both) but `OrcCompression.ToCodec` throws on
both. `LZ4` is likewise unsupported.

**ACID / transactional ORC.** The Hive 3 ACID extensions (synthetic
`originalTransaction`/`bucket`/`rowId`/`currentTransaction`/`operation`
columns, base/delta file concepts) are not implemented.

**ColumnarStripeStatistics (ORC v2 layout).** Only the v1 per-stripe
`StripeStatistics` layout is emitted and read. The v2
`ColumnarStripeStatistics` message defined in `orc_proto.proto` is
neither written nor parsed.

**Missing footer fields.** `Footer.WriterTimezone` and `Footer.Calendar`
are never set. The spec recommends `PROLEPTIC_GREGORIAN` for new writers;
without `Calendar`, Date/Timestamp interpretation is ambiguous to
conformant readers. Without `WriterTimezone`, non-instant `Timestamp`
columns cannot be correctly localized.

**Writer identification.** Both `Footer.Writer` and
`PostScript.WriterVersion` are hard-coded to `6`, which is the registered
CUDF writer code. EngineeredWood-produced files misidentify as CUDF to
conformant readers.

### Correctness / interop issues

**Timestamps use the Unix epoch instead of the ORC 2015-01-01 epoch.**
The ORC spec requires timestamp seconds to be stored as the offset from
`2015-01-01T00:00:00 UTC`. `TimestampColumnWriter` in
`src/EngineeredWood.Orc/ColumnWriters/TimestampColumnWriter.cs` writes
raw Unix epoch seconds, and `TimestampColumnReader` reads them back
as-is. Files round-trip with ourselves but are off by 1,420,070,400
seconds (≈45 years) relative to any conformant reader. The same class
also never populates the post-ORC-135 `TimestampStatistics.MinimumUtc` /
`MaximumUtc` fields, emitting only the legacy `Minimum`/`Maximum`.

**Decimal columns are effectively Decimal64.**
`DecimalColumnWriter.ReadDecimal128AsLong` reads only the low 8 bytes of
each Arrow Decimal128 value. `DecimalColumnReader` decodes the varint
into a `long`. Values whose magnitude exceeds 63 bits are silently
corrupted in both directions. Precision > 18 is not round-trip safe.
Decimal min/max statistics share the same truncation.

**Sliced / gap-containing ListArrays are written wrong.**
`ListColumnWriter.Write` emits `listArray.Values` wholesale without
slicing or respecting the offsets buffer. When the input list array is
sliced or contains nulls that leave gaps in the offsets, the written
child data includes elements that don't belong, corrupting the list
count.

**Bloom filter v1 with `Bitset.Count == 1`.** The v1 BloomFilter reader
path accepts a bitset only when `Utf8Bitset` is present or
`Bitset.Count > 1`. A legitimate v1 file with a single-chunk bitset
is rejected.

**Arrow types rejected on write.** `OrcWriter.cs` and
`ColumnWriters/ColumnWriter.cs` throw `NotSupportedException` for some
Arrow type combinations (certain union shapes, certain dictionary
configurations). The common set of primitives, nullable,
struct/list/map, decimal, date, timestamp is covered.

---

## Avro

### Missing features

**OCF codecs `bzip2` and `xz`.** Both are listed in the Avro OCF spec
but are not in `AvroCompression` / `AvroCodec`. Files using either
cannot be read or written.

**`big-decimal` logical type (Avro 1.12).** Arbitrary-precision decimal
backed by `bytes` — not supported. Decimal with precision > 38 is
silently clamped to Decimal128 by `DecimalBytesBuilder`.

**Record / enum / fixed aliases in schema resolution.**
`AvroRecordSchema.Aliases`, `EnumSchema.Aliases`, and `FixedSchema.Aliases`
are parsed but `SchemaResolver` only consults field-level aliases.
A writer schema whose record name matches a reader alias will not be
matched.

**Field `order` attribute** (`ascending` / `descending` / `ignore`).
Not parsed on read and not preserved on write. Usually readers ignore
this, but the metadata round-trip is lossy.

**Union schema evolution beyond nullable.** `SchemaResolver` only
unwraps 2-branch nullable unions. The spec's full branch-matching rules
(writer union → reader non-union pick-first-matching, reader union →
writer non-union pick-compatible) are not implemented.

**Complex-type default values.** `DefaultValueApplicator` substitutes
`null` when a missing reader field has an `array`/`map`/`record`/`fixed`
default. The spec requires JSON arrays/objects to be applied element-by-
element, and fixed defaults to be ISO-8859-1-decoded bytes. Only
primitive and enum defaults are honored.

**Schema registry integration.** `SchemaStore` is a local ID→schema map;
there is no HTTP fetch of Confluent/Apicurio endpoints. Framing is
supported (SOE, Confluent, Apicurio) but live registry lookup is the
caller's responsibility.

**Avro IDL / Protocol / RPC.** No `.avdl` parser, no protocol handshake,
no RPC. Not in scope but worth noting for discoverability.

### Correctness / interop issues

**`lz4` codec is non-standard.** The OCF spec defines
`null` / `deflate` / `bzip2` / `snappy` / `xz` / `zstandard`. LZ4 is not
in the spec. EngineeredWood exposes it (`AvroCodec.Lz4`) with a 4-byte
LE size prefix + LZ4 block framing. Files written with this codec will
not be readable by spec-compliant Avro tools.

**Recursive schemas are depth-limited.** Cyclic schemas (e.g. a linked
list where a record references itself) are blocked at depth 64 in
`ArrowSchemaConverter`, `RecordBatchAssembler`, and `SchemaResolver`.
Genuinely cyclic data structures throw on Arrow conversion.

**Schema round-trip drops fields.** `AvroSchemaWriter` omits `aliases`
on records/fields/enums/fixed, `doc` on most nodes, and enum `default`.
Round-tripping a schema through parse → write loses these attributes.

**Arrow↔Avro type mapping gaps.** `Schema/ArrowSchemaConverter.cs` and
`Data/RecordBatchEncoder.cs` throw `NotSupportedException` for some
unusual Arrow shapes. Common types are covered.

---

## Delta Lake

### Missing features

**Unsupported named features (reject writes / reads).** The following
features appear in the Delta protocol but are absent from
`SupportedReaderFeatures` / `SupportedWriterFeatures` in
`ProtocolVersions.cs`, so tables requiring them will be rejected:

| Feature | Role | Impact |
|---|---|---|
| `checkConstraints` | Writer | Tables with CHECK constraints rejected on write. |
| `generatedColumns` | Writer | Tables with generated columns rejected on write. |
| `invariants` | Writer | Legacy invariant constraints not validated. |
| `appendOnly` | Writer | Append-only enforcement not applied. |
| `allowColumnDefaults` / `columnDefaults` | Writer | Column default values on write not supported. |
| `clustering` (liquid clustering) | Writer | Not supported. |
| `variantType` | Reader-Writer | Variant (semi-structured) column type not supported. |
| `collations` | Writer | String collations not supported. |
| `catalogOwned` / `catalogOwned-preview` | Reader-Writer | Catalog-owned contracts not supported on either side. |
| `coordinatedCommits` / `managedCommits` | Writer | No commit-coordinator client; plain rename-based commits only. |
| `checkpointProtection` | Reader-Writer | Vacuum-guarded checkpoints not honored. |

CHECK-constraint and generated-column support additionally depend on a
Spark SQL expression parser (Phase 9 of the predicate-pushdown design),
which is not started. Even with the feature gate lifted, the runtime
could not evaluate the expressions.

**`rowTracking` classification.** `rowTracking` is a reader-writer
feature in the spec but is only listed in `SupportedWriterFeatures`.
Tables with `rowTracking` in `readerFeatures` will be rejected by
`ValidateReadSupport`. The writer also does not emit the
`delta.rowTracking` domain metadata that the spec requires for the
row-ID high-water mark.

**Multi-part V1 checkpoints on write.** Read is supported
(`CheckpointReader.cs`); write always emits a single
`.checkpoint.parquet` (`CheckpointWriter.cs`), regardless of table size.

**Full `_last_checkpoint` parsing.** `CheckpointReader` reads only
`v2Checkpoint.path`; other fields (`sizeInBytes`, `numOfAddFiles`,
checksum, sidecar counts) are ignored. Missing validation.

**Absolute-path deletion vectors (storage type `p`).**
`DeletionVectorWriter` emits only inline (`i`) and UUID-relative (`u`)
DV references. Absolute-path DVs cannot be written. Additionally, each
delete produces a new DV file rather than packing multiple DVs into a
single file with distinct offsets, so `offset`/`sizeInBytes` are
effectively unused on write.

**Table-property honoring.** The following properties are accepted in
table metadata but not acted on by the runtime: `delta.logRetentionDuration`,
`delta.deletedFileRetentionDuration`, `delta.enableExpiredLogCleanup`,
`delta.randomizeFilePrefixes`, `delta.checkpointInterval` (as a table
property; the .NET option `DeltaTableOptions.CheckpointInterval` does
work), `delta.dataSkippingNumIndexedCols`, `delta.dataSkippingStatsColumns`.

**Stats collection gaps.**

- No string-stat truncation. `StatsCollector` writes unbounded min/max
  strings, bloating commits for wide-text columns.
- `tightBounds` is never written.
- Stats do not recurse into nested struct / list / map leaf fields; only
  top-level primitives get stats.
- `stats_parsed` is built by `StatsParsedBuilder` for checkpoint writes
  but `CheckpointReader.ExtractAdd` reads only the JSON `stats` string.

**CommitInfo.** `InCommitTimestamp.CreateCommitInfo` populates only
`timestamp`, `operation`, `inCommitTimestamp`. Spec-standard fields
never emitted: `operationParameters`, `readVersion`, `isolationLevel`,
`operationMetrics`, `engineInfo`, `userId`, `userName`, `txnId`,
`clusterId`, `notebook`.

**Post-creation protocol upgrades.** `DeltaTable.CreateAsync` bumps
reader/writer versions only at create time when column mapping is
requested. There is no public API for enabling `deletionVectors` /
`rowTracking` / `typeWidening` / `inCommitTimestamp` on an existing
table, or for adding entries to `protocol.readerFeatures` /
`writerFeatures` after create.

**Exactly-once transactional writes.** `SetTransaction` actions are
read, written, and reconciled in snapshots, but `DeltaTable` has no
`WriteWithTxnAsync(appId, version, ...)` overload — streaming writers
cannot use `(appId, version)` for exactly-once idempotency.

**High-level DML.** No MERGE, no UPDATE-by-predicate beyond the
existing `UpdateAsync`, no DELETE-by-predicate beyond `DeleteAsync`, no
RESTORE (committing a time-travel state as the current version), no
CLONE (shallow/deep). `ReadChangesAsync` exists for CDF but there is no
raw incremental-by-version-range read outside of CDF.

**Schema evolution API.** No public ALTER TABLE for
add/drop/rename/reorder columns, change nullability, or add a column to
a nested struct. Column mapping mode is fixed at `CreateAsync`.

**Checkpoint content gaps.** `CheckpointWriter` drops `tags` and
`deletionVector` from `AddFile` when building the checkpoint Parquet,
and never emits a `remove` struct for unexpired tombstones. The spec
requires tombstones within the retention window to be preserved in
checkpoints.

**Orphan deletion-vector files.** `VacuumExecutor` excludes
`_delta_log/` from deletion. Abandoned DV `.bin` files written into
`_delta_log/` are never cleaned up.

**Schema round-trip.** `SchemaConverter.FromArrowField` preserves
per-field metadata (comments, column-mapping IDs, invariants) when
converting Arrow → Delta, filtering out `PARQUET:*` transport keys
(e.g. `PARQUET:field_id`). `ToArrowField` (Delta → Arrow) still drops
metadata.

### Correctness / interop issues

**Partition value encoding.**
`Partitioning/PartitionUtils.GetStringValue` and
`FormatTimestampPartitionValue` use `yyyy-MM-dd HH:mm:ss.ffffff` for
`timestamp` partitions; the spec's reference behavior is `yyyy-MM-dd
HH:mm:ss` (without microseconds) for `timestamp` and microseconds only
for `timestamp_ntz`. Decimal and binary partition values are not
encoded on write and `BuildConstantArray` has no case for them on read.
The `__HIVE_DEFAULT_PARTITION__` sentinel is written but not
recognized on read for non-string types — parsing will throw.

**V2 sidecar discovery.** `CheckpointReader.ReadV2CheckpointAsync`
rebuilds sidecar paths as `_delta_log/_sidecars/{name}` by a slash
check, which is fragile for paths that contain slashes in unexpected
places.

---

## Iceberg

### Missing features

**Arrow / Parquet handoff.** There is no data-file writer.
`TableOperations.AppendFilesAsync` accepts pre-built `DataFile` records
from the caller; callers must write the Parquet file themselves and
compute the column stats externally. There is no equivalent of
`OutputFile.newAppender()` from Iceberg Java — EngineeredWood.Iceberg
is a metadata library that doesn't produce data files.

**Row-level operations.** No MERGE / UPDATE / DELETE API.
`AppendDeleteFilesAsync` accepts pre-built delete-file records; it does
not convert rows to position-deletes or equality-deletes.

**Partition transform functions.** The transform types (`Identity`,
`Void`, `Bucket`, `Truncate`, `Year`, `Month`, `Day`, `Hour`) are
declared in `Transform.cs` but none has an `Apply(value)` implementation.
The library cannot derive partition values from input data. Bucket hash
(Murmur3 with per-type canonical bytes), truncate, and temporal
extraction are not implemented.

**V1 manifest schema.** Only the V2 manifest Avro schema is hard-coded
in `ManifestAvroSchemas.cs`. V1 manifests (required `snapshot_id`, no
sequence numbers) cannot be read or written.

**Puffin files.** Statistics sidecars (V2+) and V3 deletion-vector
sidecars are not implemented. No `Puffin/` directory, no reader, no
writer.

**Catalogs.** Only `FileSystemCatalog` and `InMemoryCatalog`. Missing:
REST, Glue, Hive, JDBC, Nessie, Polaris, Snowflake Open Catalog.

**Delete-file application.** `ScanResult.DeleteFiles` surfaces delete
files but the reader does not apply them. No position-delete / equality-
delete row filtering, no sequence-number filtering (data file's
sequence < delete's sequence), no V3 deletion-vector reader.

**Scan planning surface.** `TableScan` lacks `AsOfTimestamp()`
(supported in `TimeTravel` but not wired to `TableScan`), incremental
scans (`(startSnapshot, endSnapshot)` appends-between), per-file
residual predicates, branch/tag ref selection, and split-by-size / task
planning.

**Metadata operations.** No `SortOrderUpdate`. `SchemaUpdate` has no
column-reorder, no add-to-nested-struct, no `ALTER COLUMN … FIRST/AFTER`.
No format-version upgrade API.

**Iceberg views and materialized views.** Not implemented.

**Manifest compaction / merging.** No API.

**Iceberg table encryption.** `key_metadata` on manifest-list entries is
always written null (`ManifestIO.cs`). No KMS / wrapped-key plumbing.

### Correctness / interop issues

**Manifest Avro codec is severely truncated.**
`ManifestIO.EncodeDataFile` and `DecodeDataFile` omit the following
fields (some declared on `DataFile` but `[JsonIgnore]`d and absent from
the Avro codec):

- `partition` (written as an empty record, 0 bytes — single-partition tuple)
- `lower_bounds`, `upper_bounds` — stats pruning in `TableScan` only
  works for manifests whose `DataFileStats` were built in-process;
  manifests read from Avro have null bounds
- `nan_value_counts`, `distinct_counts`
- `key_metadata`, `equality_ids`, `referenced_data_file`
- `content_offset`, `content_size_in_bytes` (V2+)
- `first_row_id`, `spec_id` (V3)

Manifests EngineeredWood writes are not consumable by other Iceberg
tools (the schema is truncated and does not match the declared V2
schema), and manifests written by other tools lose bounds and partition
tuples when decoded here.

**Manifest-list partition summaries written null.** `ManifestIO`
encodes manifest-list `partitions` as null, so manifest-list-level
partition pruning (skipping entire manifests) cannot be performed by
downstream readers.

**No byte encoders for lower/upper bounds.** Even if the manifest codec
were complete, the library has no canonical-byte encoder for Iceberg
types (date, timestamp, decimal, string, binary, uuid, etc.), so
callers cannot correctly construct bounds to hand in.

**V3 features are declarative only.** `NestedField`'s initial-default
and write-default fields are parsed but never applied on read for
missing columns. `TableMetadata.NextRowId` is stored but never
incremented on appends, so V3 row lineage is not enforceable.
`DataFile.FirstRowId` exists but is not in the Avro codec.
`last-updated-sequence-number` is not tracked per row.

**V3 type declarations without I/O support.** `IcebergType.cs` declares
all 22 primitive/nested types (including geometry, geography, variant,
timestamp_ns, timestamptz_ns), but without a data-file writer, there is
no write-side validation that any of these types are correctly encoded
in Parquet, and no Iceberg-side schema bridge to Arrow or Parquet.

---

## Expressions

### Missing features

**Spark SQL parser.** `EngineeredWood.SparkSql` (Phase 9 of the
predicate-pushdown design) is not implemented. `Expression` /
`Predicate` trees must be built in code via the `Expressions` static
factory. This blocks any feature that needs to parse SQL expression
strings from table metadata — notably Delta CHECK constraints and
generated columns.

**Built-in function registry.** `ArrowRowEvaluator` accepts an optional
`IFunctionRegistry`, but the library ships no implementations.
`FunctionCall` expressions throw at evaluation time unless the caller
supplies a registry. A Spark function registry is planned alongside the
SparkSql parser.
