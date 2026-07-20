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

> **Last verified against the code: 2026-07-19.** The Parquet and Delta
> sections were re-checked claim by claim on that date; roughly a dozen
> entries described gaps that had since been closed. If you are relying on
> an entry here, confirm it against the code before acting on it — this file
> records absences, and absences are exactly what nothing fails when they
> stop being true. The Delta entries additionally have external coverage now
> (delta-rs and PySpark) in
> `test/EngineeredWood.DeltaLake.Table.Tests/Interop/`; see
> [`upstream-landing-notes.md`](upstream-landing-notes.md).

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

- `JSON` and `ENUM` are never emitted (Arrow has no enum type and we
  always write `StringType` as `StringType`).
- `UUID` is emitted only via the opt-in extension path — an Arrow field
  carrying the `arrow.uuid` extension type maps to `LogicalType.UuidType`
  + FLBA(16) (`ArrowToSchemaConverter`). A bare
  `FixedSizeBinaryType(16)` with no extension is still written as an
  unannotated FLBA, which is the correct conservative behavior.

**Geospatial types.** The `GEOMETRY` / `GEOGRAPHY` types added to the
Parquet spec in late 2024 are not supported on either path.

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
| `allowColumnDefaults` / `columnDefaults` | Writer | Column default values on write not supported. |
| `variantType` | Reader-Writer | Variant column type not supported AT THE DELTA LAYER. Note the Parquet layer does support VARIANT via the opt-in `arrow.parquet.variant` extension; the gap is Delta-side plumbing, not the codec. |
| `collations` | Writer | String collations not supported. |
| `catalogOwned` / `catalogOwned-preview` | Reader-Writer | Catalog-owned contracts not supported on either side. |
| `coordinatedCommits` / `managedCommits` | Writer | No commit-coordinator client; plain rename-based commits only. |
| `checkpointProtection` | Reader-Writer | Vacuum-guarded checkpoints not honored. |

**Enforcement features: supported-as-listed, fail-closed when active.**
`appendOnly`, `invariants`, `checkConstraints`, `generatedColumns` and
`changeDataFeed` ARE in `SupportedWriterFeatures`, because a v7 protocol
enumerates the legacy writer-v2/v3 features explicitly and merely LISTING
them imposes no obligation. `DeltaTable.HonorWriterFeatures` distinguishes
listed from ACTIVE: `delta.appendOnly=true` blocks non-append data changes,
and an active `delta.constraints.*` / `delta.invariants` /
`delta.generationExpression` REJECTS the write rather than committing
possibly-violating data (Delta enforces these at write time only, so one
bad commit poisons the table for every later reader).

Evaluating those expressions still depends on a Spark SQL parser (Phase 9
of the predicate-pushdown design), which is not started — so a table that
actively uses them is refused, not silently mishandled. The key names this
guard keys on are pinned against real Spark output by
`SparkWritten_CheckConstraint_EwRefusesToWrite` and
`SparkWritten_GeneratedColumn_EwRefusesToWrite`.

**Liquid clustering is interop-only.** `clustering` IS in
`SupportedWriterFeatures`, and the spec permits a non-clustering writer to
append and run DML against a clustered table (a later OPTIMIZE reclusters).
EW meets those obligations — the `delta.clustering` domain survives commits
and checkpoints, and `add.clusteringProvider` round-trips — and
`CreateAsync(clusteringColumns:)` / `SetClusteringColumnsAsync` declare the
layout. But EW does NOT write clustered layouts: no Hilbert-curve ordering,
no provider tagging on written files. Rows land wherever they land until a
clustering engine reorganizes them.

Note the domain stores PHYSICAL column names, because OSS Delta
`None.get`-crashes on logical ones. Verified end to end by
`EwWritten_Clustered_SparkResolvesClusteringColumns`, which asserts Spark's
`DESCRIBE DETAIL` resolves the declaration back to the logical names.

**Row tracking (`delta.enableRowTracking=true`) is READ-ONLY.** A
data-changing write to a row-tracking table is refused with
`NotSupportedException` (`DeltaTable.RejectRowTrackingWrite`, gating
`ValidateWritable` and `CompactAsync`) rather than silently corrupting it.
The write path could assign a `baseRowId` on append but DROPS it on any
copy-on-write rewrite (UPDATE / compaction) and materializes a non-spec
internal `__delta_row_id` column, so a write would violate the stable-row-id
invariants a conformant engine (Spark, Databricks) relies on. Reading a
row-tracking table is fully supported — `baseRowId` is log metadata that does
not affect the data, and the `delta.rowTracking` high-water mark is
reconciled on read (`RowTracking/RowTrackingConfig.cs`). A spec-conformant
writer (materialized-column naming via metadata, id preservation through
rewrites, tier-3 Spark validation) is the deferred **Layer 3 (B)** work; it
is also the prerequisite for row-tracking optimistic-concurrency rebase (the
`rebaseSafe: false` limitation). Full future-implementation brief:
`doc/row-tracking-conformance-brief.md`. There is also no `CreateAsync` surface to
ENABLE row tracking — the only way to reach a row-tracking table is opening
one a foreign engine created.

**`rowTracking` read-side classification.** `rowTracking` is a
reader-writer feature in the spec but is listed only in
`SupportedWriterFeatures`, so a table carrying it in `readerFeatures` is
still rejected by `ValidateReadSupport`.

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
`delta.enableExpiredLogCleanup`,
`delta.randomizeFilePrefixes`, `delta.checkpointInterval` (as a table
property; the .NET option `DeltaTableOptions.CheckpointInterval` does
work), `delta.dataSkippingNumIndexedCols`, `delta.dataSkippingStatsColumns`.

**Stats collection gaps.**

- `tightBounds` is never written.
- `stats_parsed` is built by `StatsParsedBuilder` for checkpoint writes
  but `CheckpointReader.ExtractAdd` reads only the JSON `stats` string.
- `delta.dataSkippingNumIndexedCols` / `delta.dataSkippingStatsColumns`
  are ignored; every eligible column gets stats.

(String-stat truncation and nested-struct recursion are both implemented —
`StatsCollector.TruncateMaxString` and `CollectStruct`. Nested stats are
verified externally: `EwWritten_NestedStats_SparkSkipsOnNestedFieldWithoutLosingRows`
asserts Spark prunes on `payload.score` and still returns every matching row.)

**CommitInfo.** `InCommitTimestamp.CreateCommitInfo` emits `timestamp`,
`operation`, `inCommitTimestamp`, `engineInfo` and `operationParameters`
(at minimum an empty object, which strict readers require). Spec-standard
fields still never emitted: `readVersion`, `isolationLevel`,
`operationMetrics`, `userId`, `userName`, `txnId`, `clusterId`, `notebook`.

**Post-creation protocol upgrades — partial.** `AddColumnAsync` and
`SetClusteringColumnsAsync` upgrade the protocol as needed via
`UpgradeProtocolForFeatures` / `UpgradeProtocolForWriterFeatures`, and
`CreateAsync` declares schema-driven features (`timestampNtz`,
`identityColumns`, `columnMapping`) up front, and `enableDeletionVectors:
true` declares `deletionVectors` at creation. There is still no general
public API for enabling `deletionVectors` / `rowTracking` / `typeWidening` /
`inCommitTimestamp` on an EXISTING table (only `deletionVectors` can be
declared, and only at create time).

**Exactly-once transactional writes.** `SetTransaction` actions are
read, written, and reconciled in snapshots, but `DeltaTable` has no
`WriteWithTxnAsync(appId, version, ...)` overload — streaming writers
cannot use `(appId, version)` for exactly-once idempotency.

**High-level DML.** No MERGE, no UPDATE-by-predicate beyond the
existing `UpdateAsync`, no DELETE-by-predicate beyond `DeleteAsync`, no
RESTORE (committing a time-travel state as the current version), no
CLONE (shallow/deep). `ReadChangesAsync` exists for CDF but there is no
raw incremental-by-version-range read outside of CDF.

**Schema evolution API — partial.** `AddColumnAsync`, `RenameColumnAsync`
and `DropColumnAsync` exist as metadata-only commits. Still missing:
`SetSchemaAsync` (adopt a whole incoming schema), column reorder,
nullability change, and adding a column to a nested struct. Column mapping
mode is fixed at `CreateAsync`.

**Vacuum does not collect expired change-data-feed files.** `_change_data/`
is excluded from the sweep because CDF files are referenced by `cdc`
actions, which never appear in the snapshot's active files — a keep-set
built from `add` actions alone does not cover them, and sweeping the
directory would destroy readable history. Building a proper CDF keep-set
needs the snapshot to track `cdc` actions, which it does not yet. This
under-deletes; it cannot lose data.

**Vacuum refuses tables with absolute-path deletion vectors.** A
`storageType: "p"` vector cannot be resolved against the table root from
the action alone, so vacuum cannot prove it lies outside the directory it
is about to sweep. It throws `NotSupportedException` rather than guessing —
deleting a live deletion vector would silently resurrect every row it
masked. EngineeredWood never writes `p` vectors.

### Correctness / interop issues

**Deletion vectors are opt-in; DELETE fails on a partial match when they are
off.** `DeltaTable.CreateAsync(..., enableDeletionVectors: true)` sets the
`delta.enableDeletionVectors` property and declares the `deletionVectors`
reader+writer feature (reader 3 / writer 7). Only then does a partial DELETE
soft-delete rows with a deletion vector. With DVs disabled, a DELETE may only
remove WHOLE files (a clean file/partition boundary — a metadata-only remove
needing no DV); a predicate that would soft-delete part of a file throws
`InvalidOperationException` rather than write a vector a foreign reader would
not apply. There is still **no way to enable DVs on an EXISTING table** (no
`ALTER TABLE`-style property update / protocol upgrade), and **no
copy-on-write DELETE** to rewrite files when DVs are off — the two "for now"
gaps behind the fail-fast. Earlier EW always wrote a DV without declaring the
feature, so a conformant foreign reader silently returned the deleted rows;
that data-loss gap is closed.

_Reader-side reality (measured):_ delta-rs 1.6.2's reader does not support the
`deletionVectors` feature, so it REFUSES an EW DV table (`DeltaProtocolError:
... not yet supported`) rather than mis-reading it — the safe reaction, pinned
by `DeltaRsInteropTests.EwUnionedDeletionVector_EwApplies_DeltaRsSafelyRefusesUnsupportedFeature`.
Spark 4.0 does support DV reads; `SparkInteropTests.EwWritten_UnionedDeletionVector_SparkReadsSurvivingRow`
is where the read-back of an EW deletion vector (including a row-level union)
is actually validated.

**Binary partition values are unsupported.**
`Partitioning/PartitionUtils.GetStringValue` throws
`NotSupportedException` for `binary` (and nested) partition types rather
than falling back to `.ToString()`. Deliberate — the fallback silently
wrote the .NET type name as the partition value — but it is still a gap
against the spec, which defines a binary encoding.

(The rest of this entry previously claimed broken timestamp formatting,
missing decimal encoding, and an unrecognized `__HIVE_DEFAULT_PARTITION__`
on read. All three were stale: `FormatTimestampPartitionValue` emits
`yyyy-MM-dd HH:mm:ss` and only adds `.ffffff` when the fraction is
non-zero, matching Spark; `Decimal128Array` is encoded on write and has a
`BuildConstantArray` case on read; and the sentinel decodes as SQL NULL for
every type.)

**V2 sidecar discovery.** `CheckpointReader.ReadV2CheckpointAsync`
rebuilds sidecar paths as `_delta_log/_sidecars/{name}` by a slash
check, which is fragile for paths that contain slashes in unexpected
places.

**Non-ASCII characters left literal in `add.path`.** `DeltaPath.Encode`
escapes only `% space # ?` and control characters. Measured against
delta-rs 1.6.2, the reference encoding is TWO layers: the on-disk Hive
directory percent-encodes non-ASCII as UTF-8 bytes (`region=caf%C3%A9`),
and `add.path` then percent-encodes that again (`region=caf%25C3%25A9`).
EW's output diverges from both. Low severity in practice — delta-rs reads
EW's literal form fine (`EwWritten_NonAsciiPartition_DeltaRsReadsSameRows`
passes) — but it is a producer-side divergence from Spark. Ground truth is
pinned by `DeltaRs_NonAsciiPartition_PathEncodingGroundTruth`.

**Column-mapping protocol shape differs from Spark's.** EW emits the
legacy `minReader=2`/`minWriter=5` pair; Spark emits a hybrid
(`minReader=2`, `minWriter=7`, `writerFeatures: [columnMapping, invariants,
appendOnly]`). Both are spec-legal and Spark reads EW's form. Note that
delta-rs cannot read column-mapped tables in EITHER form — it is an
unimplemented feature there, not a declaration mismatch — so changing this
would not widen reader support. See
[`upstream-landing-notes.md`](upstream-landing-notes.md).

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
