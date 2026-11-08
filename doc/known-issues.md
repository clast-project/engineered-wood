# Known Issues and Feature Gaps

Feature gaps and known runtime issues, by feature area. Each area splits
into **Missing features** (the spec defines it; we do not implement it) and
**Correctness / interop issues** (we diverge from the spec or from other
tools in a way that affects round-tripping).

Expression-library and predicate-pushdown status:
[`predicate-pushdown-design.md`](predicate-pushdown-design.md). Encryption
design: [`encryption-design.md`](encryption-design.md).

> **Last verified against the code: 2026-11-08.** This file records
> absences, which nothing fails on when they stop being true: confirm an
> entry against the code before relying on it. Delta entries also have
> external coverage (delta-rs and PySpark) in
> `test/EngineeredWood.DeltaLake.Table.Tests/Interop/`.

---

## Parquet

### Missing features

**Modular encryption (PARQUET-1375).** Encrypted files can be neither read
nor written. See [`encryption-design.md`](encryption-design.md).

**Page-index pruning skips no pages in some row groups.** With
`ParquetReadOptions.FilterUsePageIndex` (default true) a filtered read reads
only the pages the ColumnIndex cannot rule out. Columns without a usable
index, and every column of a row group that has a nested column, are read
whole and then cut to the same rows: the result is right, but no I/O is
saved.

**Column chunks stored in another file (`file_path`).** EW does not open
other files. By default such a chunk (as in Hive/Spark/pyarrow `_metadata`
summary files) is refused with `NotSupportedException` wherever its offsets
would be read; the footer and other columns stay readable.
`ParquetReadOptions.ColumnChunkFilePath = Ignore` reads the chunk from the
file at hand, as pyarrow, arrow-rs and Spark do, which is right only when
the data really is local.

**Extended-precision timestamps: three limits.** The
`FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` carrier
([parquet-format#600](https://github.com/apache/parquet-format/issues/600),
gated behind `EWPARQUET0004`) is supported for top-level columns only; a
nested column named in `ExtendedTimestampColumns` is refused. There is no
Arrow extension type for it, so a `FixedSizeBinary` read does not
round-trip to the carrier unless the column is named again on write. And
this library cannot write the values that motivate the type: an Arrow
timestamp is `int64`, so anything Arrow can hold already fits `INT64`. See
[`parquet-extended-precision-timestamps.md`](parquet-extended-precision-timestamps.md).

**LZO compression.** `CompressionCodec.Lzo` decodes from Thrift, but
`Decompressor` (`src/EngineeredWood.Core/Compression/Decompressor.cs`)
throws `NotSupportedException` for it, so files with LZO pages fail to read.

**Logical / converted types on read:**

- `INTERVAL` (12-byte FLBA) is not mapped; it reads as
  `FixedSizeBinaryType(12)`.
- `BSON` is not mapped in `ArrowSchemaConverter`; it reads as `BinaryType`.

**Logical types on write:**

- `JSON` and `ENUM` are never emitted (Arrow has no enum type; `StringType`
  is always written as `STRING`).
- `UUID` is emitted only for a field with the `arrow.uuid` extension type. A
  bare `FixedSizeBinaryType(16)` is written as unannotated FLBA.

**Geospatial types.** `GEOMETRY` / `GEOGRAPHY` are not supported on either
path.

**`BYTE_STREAM_SPLIT` for integers and FLBA on write.** The decoder supports
BSS for `INT32`/`INT64`/`FIXED_LEN_BYTE_ARRAY`, but `EncodingStrategyResolver`
emits it only for `FLOAT`/`DOUBLE`, and only when asked for by name. The
default float encoding is `PLAIN`, because Spark's vectorized reader rejects
BSS.

**Arrow types rejected on write.** `ArrowToSchemaConverter.MapArrowType`
throws `NotSupportedException` for `Date64Type`, `IntervalType`,
`DurationType`, `DictionaryType`, `UnionType` and `ListViewType`, for a
run-end-encoded column over a nested type, and for a decimal whose scale is
negative or above its precision. (`FixedSizeListType` is written as an
ordinary `LIST`; its width is lost.)

**Metadata fields not produced on write:**

- `ColumnMetaData.key_value_metadata` (per-column) is neither read nor
  written.
- `RowGroup.SortingColumns` is encoded and decoded, but
  `ParquetWriteOptions` has no way to set it.
- `Statistics.distinct_count` is never computed, so it is always absent.

### Correctness / interop issues

**VARIANT caveats** (none affects the common annotated path):

- *The shredded layout is not preserved on read.* A shredded column is
  reassembled into an unshredded `VariantArray`; `typed_value` cannot be
  inspected afterwards.
- *An unannotated nested variant is not wrapped.* A variant inside a
  struct/list/map with no logical-type annotation (Spark 4.0.x output, or
  EW's with `EmitVariantLogicalType=false`) reads back as a bare struct. The
  Delta layer's repair is top-level only.
- *Upstream Arrow gaps are worked around in `VariantShredding`*:
  apache/arrow-dotnet#398 and #399 are fixed upstream but not released as
  of Apache.Arrow 23.0.0, and
  [apache/arrow-dotnet#499](https://github.com/apache/arrow-dotnet/issues/499)
  (nullable shredded field groups, which Spark 4.1 reads as
  `MALFORMED_VARIANT`) is open. EW's output is spec-conformant either way.
- *A shredded narrow DECIMAL loses its trailing zeros.* A `typed_value`
  decimal of precision 18 or less reads back with the right value but a
  normalised scale (DuckDB's `12.30::DECIMAL(9,2)` comes back as `12.3`).
  The scale is lost in Apache.Arrow 23's `Decimal32Array.GetValue` /
  `Decimal64Array.GetValue`, which reassembly calls. 16-byte decimals (on
  .NET 7+) and decimals in the residual `value` keep their scale. Pinned by
  `DuckDbWritten_NarrowDecimal_KeepsItsValueButLosesTrailingZeros`.

**Deprecated `min`/`max` only for signed-order types.** The deprecated
signed-order `Statistics.min`/`max` are written only for booleans, signed
integers (including date/time/timestamp) and floats. UTF-8, binary,
unsigned and FLBA-decimal columns get only `min_value`/`max_value`, as
parquet-mr does, so a legacy reader gets no bounds for them.

**Files written by EngineeredWood 0.3.0 and earlier can have inverted
decimal and unsigned-integer statistics.** Until #398 the writer compared
FLBA decimals (every `Decimal128`/`Decimal256` column) as unsigned bytes and
`UInt32`/`UInt64` as signed. A chunk with decimals of both signs, or
unsigned values on both sides of 2^31 (or 2^63), recorded a minimum above
its maximum. This affects every release to 0.3.0 and `main` builds up to
`edabc29`; `created_by` cannot tell them apart from fixed builds. The values
are correct; only the statistics are wrong.

- *EW's own pruning* is safe: `StatisticsEvaluator` treats min > max as
  unknown and reads the row group.
- *Other readers are not.* DataFusion, Spark and other engines that trust the
  bounds skip these row groups and silently return none of their rows.
  Rewrite affected files with a current build.

**`ARROW:schema` is not fully restored on read.** EW writes the
`ARROW:schema` footer entry and restores the time zone from it, but:

- A `timestamp[s]` column reads back as `timestamp[ms]`, as PyArrow does.
- A fixed-size list reads back as an ordinary list (as DuckDB returns it);
  the width cannot be verified against the data, and PyArrow's attempt to
  restore it fails on files whose lists are not uniform.

---

## ORC

### Missing features

**Column-level encryption.** Not implemented for read or write; only the
generated protobuf definitions in
`src/EngineeredWood.Orc/Proto/orc_proto.proto` exist. See
[`encryption-design.md`](encryption-design.md).

**Public predicate-pushdown API.** Bloom-filter evaluation exists
internally, but `OrcReaderOptions` has no filter option comparable to
`ParquetReadOptions.Filter`. Row indexes are written on every stripe, but
the reader has no seek or row-group-skip API, so they are unused. Tracked as
Phase 14 in [`predicate-pushdown-design.md`](predicate-pushdown-design.md).

**VARCHAR / CHAR.** Arrow `StringType` always writes as ORC `String`, and
`Varchar`/`Char` read as `StringType`, so max length is never written or
surfaced.

**RLE v1 / DictionaryV1 on write.** `RleDecoderV1` exists for read, but the
`EncodingFamily` enum (`OrcWriterOptions.DefaultIntegerEncoding`,
`DefaultStringEncoding`, `ColumnEncodings`) offers only `V2` /
`DictionaryV2`, so files for V1-only readers (Hive 0.11) cannot be produced.

**Compression codecs.** `OrcCompression.ToCodec` maps `NONE`, `ZLIB`,
`SNAPPY`, `LZ4` and `ZSTD`. `LZO` and `Brotli` throw
`NotSupportedException`.

**ACID / transactional ORC.** Hive 3 ACID columns and base/delta files are
not implemented.

**ColumnarStripeStatistics (ORC v2 layout).** Only the v1 per-stripe
`StripeStatistics` is written and read.

**Missing footer field.** `Footer.Calendar` is never set (the spec
recommends `PROLEPTIC_GREGORIAN`), so Date/Timestamp interpretation is
ambiguous to conformant readers. Every stripe names `GMT` as its writer
time zone.

**Writer identification.** `OrcWriter` sets both `Footer.Writer` and
`PostScript.WriterVersion` to `6`. `WriterVersion = 6` is correct (non-Java
writers start their own sequence at 6). `Footer.Writer = 6` is not a
registered producer id (`orc_proto.proto` documents 0-5), so readers cannot
identify an EW file's producer. Because `writer_version` is interpreted
relative to the writer id, a reader that falls back to Java semantics reads
6 as "ORC-135 fixed" (UTC timestamp statistics); EW meets that, but any
other version-gated behaviour is a guess.

### Correctness / interop issues

**Files EngineeredWood wrote before #584 / #583 / #585.** Earlier writers
stored timestamps against the Unix epoch with their own nanosecond scale,
zigzag-encoded signed RLEv2 Patched Base runs, and (until #585) wrote struct
fields for null struct rows. EW still reads files with `software_version`
`"EngineeredWood.Orc 0.1"` (and `"0.2"` for the struct layout) as they were
written; other readers misread them. Rewrite them with the current writer.

**TIMESTAMP in a writer time zone on .NET Framework.** A non-instant
`Timestamp` column from a stripe whose writer time zone is not UTC (Hive and
Java writers record the JVM's zone) needs IANA zone rules. On .NET
Framework, reading one throws `NotSupportedException` naming the zone.

**Bloom filter v1 with `Bitset.Count == 1`.** `OrcBloomFilter` accepts a v1
bitset only when `Utf8Bitset` is present or `Bitset.Count > 1`, so a valid
single-word v1 bitset is rejected.

**Arrow types rejected on write.** `OrcWriter.cs` and
`ColumnWriters/ColumnWriter.cs` throw `NotSupportedException` for some
union and dictionary shapes. Primitives, struct/list/map, decimal, date and
timestamp are covered.

---

## Avro

### Missing features

**OCF codecs `bzip2` and `xz`.** Not in `AvroCodec`; such files cannot be
read or written.

**`big-decimal` logical type (Avro 1.12).** Not supported. A decimal with
precision > 38 is silently clamped to Decimal128 by `DecimalBytesBuilder`.

**Record / enum / fixed aliases in schema resolution.** Parsed, but
`SchemaResolver` consults only field aliases, so a writer record whose name
matches a reader alias is not matched.

**Field `order` attribute.** Not parsed or written, so it is lost on round
trip.

**Union schema evolution beyond nullable.** `SchemaResolver` only unwraps
2-branch nullable unions; the spec's general branch-matching rules are not
implemented.

**Complex-type default values.** `DefaultValueApplicator` substitutes `null`
for a missing reader field whose default is an `array`/`map`/`record`/
`fixed`. Primitive, enum and logical-typed `fixed` (decimal) defaults are
honoured.

**Schema registry integration.** `SchemaStore` is a local ID-to-schema map.
SOE, Confluent and Apicurio framing is supported, but fetching from a live
registry is the caller's job.

**Avro IDL / Protocol / RPC.** Not implemented, and not in scope.

### Correctness / interop issues

**`lz4` codec is non-standard.** LZ4 is not in the OCF spec. `AvroCodec.Lz4`
writes a 4-byte LE size prefix plus LZ4 block; spec-compliant tools cannot
read such files.

**Recursive schemas are depth-limited.** `ArrowSchemaConverter`,
`RecordBatchAssembler` and `SchemaResolver` stop at depth 64, so genuinely
cyclic data throws on Arrow conversion.

**Schema round-trip drops attributes.** `AvroSchemaWriter` writes no
`aliases` and no `doc`, so parse-then-write loses them.

**Arrow-Avro type mapping gaps.** `Schema/ArrowSchemaConverter.cs` and
`Data/RecordBatchEncoder.cs` throw `NotSupportedException` for some unusual
Arrow shapes.

---

## Delta Lake

### Missing features

**Unsupported table features.** These are absent from
`SupportedReaderFeatures` / `SupportedWriterFeatures` in
`ProtocolVersions.cs`, so tables requiring them are refused:

| Feature | Role | Impact |
|---|---|---|
| `allowColumnDefaults` / `columnDefaults` | Writer | Column defaults on write not supported. |
| `collations` | Writer | String collations not supported. |
| `catalogOwned` / `catalogOwned-preview` | Reader-Writer | Catalog-owned tables not supported. |
| `coordinatedCommits` / `managedCommits` | Writer | No commit-coordinator client; commits are create-if-absent log writes only. |
| `checkpointProtection` | Reader-Writer | Not honoured. |

**CHECK constraints, invariants and generated columns are enforced only
where EW sees the rows.** `appendOnly`, `invariants`, `checkConstraints`,
`generatedColumns` and `changeDataFeed` are supported writer features;
listing them in a protocol imposes nothing. When a table actively uses
`delta.constraints.*`, `delta.invariants` or `delta.generationExpression`,
the paths that build or receive the rows (`WriteAsync`, create-with-data,
`UpdateAsync`, `UpdateRowsAsync`, `MergeAsync`, `OverwriteWhereAsync`, and
their transaction forms) evaluate them and refuse a violating commit; DELETE
writes no new values and is not refused. `CommitDataFilesAsync` and
`DeltaTransaction.StageDataFiles` / `StageDataFilesAsync` take finished
files, so they refuse such a table unless the caller passes
`constraintsEnforcedByCaller: true`, an assertion nothing verifies.
`SupportsExternalDataFileCommit` reports false for such a table.
`delta.appendOnly=true` blocks non-append data changes. See
[Expressions](#expressions) for evaluator limits.

**Liquid clustering is interop-only.** `clustering` is supported: the
`delta.clustering` domain survives commits and checkpoints,
`add.clusteringProvider` round-trips, and `CreateAsync(clusteringColumns:)`
/ `SetClusteringColumnsAsync` declare the layout (stored as physical column
names, as OSS Delta requires). But EW writes no clustered layout (no
Hilbert ordering, no provider tagging); rows stay where they land until a
clustering engine reorganizes them.

**Variant shredding and variant pruning.** `variantShredding` and
`variantShredding-preview` are supported (#671); `delta.enableVariantShredding
= true` at creation declares `variantShredding` (delta-spark 4.3.1 declares
the preview name instead). Shredded writes (`ParquetWriteOptions.ShredVariants`
/ `VariantShredSchemas`) to any other table with a top-level variant column
are refused with `DELTA_SHREDDING_TABLE_PROPERTY_DISABLED`. Checkpoints are
never shredded. EW derives per-path variant bounds from the footers it
writes, as delta-spark 4.3 does, and prunes files (#677) and, inside the
Parquet reader, row groups and pages (#682) on `variant_get`,
`try_variant_get` and `v:a::int` predicates. delta-spark 4.3.1 and DuckDB
do not prune on these bounds. Gaps:

- *Dictionaries and Bloom filters are not consulted on a variant path*
  ([#772](https://github.com/clast-project/engineered-wood/issues/772)).
- *Row-level `variant_get` (#681) refuses some casts.* DML predicates and
  CHECK constraints on a variant path are evaluated row by row, but an
  object or array cast to `string`, a `timestamp_ntz` or nested target, and
  a TIME or nanosecond-timestamp value are refused. A generated column on a
  variant path is refused (`DELTA_UNSUPPORTED_EXPRESSION_GENERATED_COLUMN`,
  or `DELTA_UNSUPPORTED_DATA_TYPE_IN_GENERATED_COLUMN` for a VARIANT column)
  at the first write, not when declared. A failed cast while checking a
  constraint reports `DELTA_UNEVALUABLE_TABLE_EXPRESSION` wrapping
  `INVALID_VARIANT_CAST`, where Spark reports `TASK_WRITE_FAILED`.
- *Floats prune one way at file level.* Delta keeps no NaN count, so a
  variant float bound skips a file for `<` and `=` but not `>`.
- *delta-spark writes no variant bounds by default.* It needs
  `spark.databricks.delta.variantShredding.parseFooterForStats=true` and
  `spark.databricks.delta.stats.limitPerVariant` above 0. EW has no path cap.
- *Only top-level variant columns are bounded.* A host `IDataFileWriter`'s
  file is bounded from its footer read back after the write (one extra
  footer read per file); a footer that cannot be read fails the write.
  Files committed through `CommitDataFilesAsync` or `StageDataFiles*` get
  only `nullCount` unless the host supplies bounds, and with
  `EmitVariantLogicalType=false` a host's file stays unbounded.
- *Host files are not inspected.* `CommitDataFilesAsync`,
  `StageDataFiles*` and an `IDataFileWriter` that ignores the Parquet
  options can commit a shredded file to a table that does not enable
  shredding; that is the host's obligation. DuckDB shreds by default.
- *No ALTER.* The property can be set only at creation or replace.

**Row tracking: rewrite of a table without materialized column names.**
`RejectRowTrackingWrite` refuses a copy-on-write rewrite (UPDATE, OVERWRITE,
DELETE, compaction) of a row-tracking table that does not declare its
materialized column-name properties, since EW cannot then preserve row ids.
EW never creates such a table; appending to and reading one is allowed.

**Multi-part V1 checkpoints on write.** Read is supported; `CheckpointWriter`
always writes a single `.checkpoint.parquet`.

**delta-rs cannot materialize rows from a `v2Checkpoint` table.** Not an EW
gap. deltalake 1.6.2 reconstructs such a table (`version()`,
`file_uris()`, `get_add_actions()` work from the checkpoint alone), but
`to_pyarrow_dataset` / `to_pandas` refuse it (deltalake's Python
`SUPPORTED_READER_FEATURES` is `{timestampNtz, variantType,
variantType-preview}`), and the DataFusion `QueryBuilder` fails with
"Unsupported table features required: [V2Checkpoint]". Pinned by
`DeltaRsInteropTests.EwWrittenV2Checkpoint_DeltaRsRebuildsStateFromTheCheckpointAlone`.

**delta-kernel-rs refuses to write V1 checkpoints on a `v2Checkpoint`
table**, which PROTOCOL.md allows. EW's default
(`CheckpointFormat.Automatic`) follows the spec and delta-spark: a classic V1
checkpoint unless `delta.checkpointPolicy=v2`. Kernel reads that fine.
`CheckpointFormat.V2WhenSupported` adopts kernel's rule; it is opt-in
because it disagrees with delta-spark and overrides an explicit
`delta.checkpointPolicy=classic`.

**`_last_checkpoint` `checksum` is not written.** A wrong checksum is worse
than none (a validating reader would reject a good hint), so it is omitted.
delta-spark writes it.

**Version-checksum optional fields.** EW writes `_delta_log/<version>.crc`
after every commit (`DeltaTableOptions.WriteVersionChecksums`, default true)
with the required fields plus `inCommitTimestampOpt`, `setTransactions` and
`domainMetadata`. Omitted, as delta-kernel-rs omits them: `txnId`,
`allFiles`, `numDeletedRecordsOpt`, `numDeletionVectorsOpt`,
`deletedRecordCountsHistogramOpt`. `fileSizeHistogram` is omitted too:
delta-spark writes it as `histogramOpt`, and kernel rejects a file carrying
both names.

**Version checksums are not read by default.** Snapshots are built by
replaying the log; `VersionChecksumValidator` compares a snapshot against
its `.crc` on request and returns (does not throw) the disagreements. The
one shortcut, `DeltaTableOptions.ChangeFeedSchemaFromVersionChecksum`
(default false), takes a change feed's end-version schema under column
mapping from that version's checksum; an absent or unusable checksum falls
back to replay, but a wrong one can rename or drop columns in the feed.

**`_last_checkpoint` parsing is partial.** `CheckpointReader` reads
`version`, `size`, `parts`, `sizeInBytes`, `numOfAddFiles` and
`v2Checkpoint.path`; `checkpointSchema`, `checksum`,
`v2Checkpoint.nonFileActions` and `v2Checkpoint.sidecarFiles` are ignored.
That costs performance (the checkpoint is always opened), not correctness.

**Absolute-path deletion vectors (storage type `p`).**
`DeletionVectorWriter` writes only inline (`i`) and UUID-relative (`u`)
vectors. Each delete writes a new DV file; vectors are not packed into
shared files.

**Table properties.** Audited 2026-10-05 against delta-spark 4.4.0 (every
key `DeltaConfigs` defines, plus `delta.constraints.*`, `delta.feature.*`,
the row-tracking column names and the symlink-manifest flag). To repeat it,
run `javap` over `DeltaConfigsBase` and collect the first argument of each
`buildConfig` call.

*Honoured:* `delta.appendOnly`; `delta.checkpointInterval` (wins unless
`DeltaTableOptions.CheckpointInterval` <= 0); `delta.checkpointPolicy`;
`delta.checkpoint.writeStatsAsJson` / `writeStatsAsStruct`;
`delta.columnMapping.mode` / `maxColumnId`; `delta.constraints.*`;
`delta.deletedFileRetentionDuration`; `delta.enableChangeDataFeed`;
`delta.enableDeletionVectors`; `delta.enableExpiredLogCleanup` and
`delta.logRetentionDuration` (by modification time, or by in-commit
timestamps where the file system cannot date files, #115; otherwise nothing
is deleted); `delta.enableIcebergCompatV1` / `V2` / `V3` (below);
`delta.enableInCommitTimestamps` and its enablement version/timestamp
(#598); `delta.enableRowTracking` and its column names;
`delta.enableTypeWidening`; `delta.isolationLevel` (#472);
`delta.dataSkippingStatsColumns`; `delta.dataSkippingNumIndexedCols`;
`delta.dataSkippingStringPrefixLength`. Two differences from Spark:

- An absent `delta.isolationLevel` reads as `WriteSerializable`
  (Databricks' default). OSS delta-spark uses `Serializable`, so on a table
  neither engine configured, EW lets a DML commit past a concurrent blind
  append that OSS Spark would abort. This is deliberate: `Serializable` would
  also turn off EW's row-level concurrency by default. Set
  `delta.isolationLevel=Serializable` for Spark's behaviour.
- String prefix bounds differ: EW counts UTF-16 code units where Spark counts
  code points, and bumps the last kept character of a max where Spark
  appends U+007F or U+10FFFF. Both are valid bounds.

*Refused unless the caller opts in:* `delta.universalFormat.enabledFormats`
(UniForm). EW generates no Iceberg or Hudi metadata
([#490](https://github.com/clast-project/engineered-wood/issues/490)), so
every commit to (or creation or REPLACE of) a table with non-empty
`enabledFormats` is refused with `DELTA_UNIVERSAL_FORMAT_NOT_MAINTAINED`.
Reads, checkpoints and VACUUM are unaffected.
`DeltaTableOptions.AllowWritesWithoutUniversalFormatConversion` allows the
writes. `delta.universalFormat.iceberg.atomicConversion.supported` and
`delta.universalformat.config.*` are not read.

*Column mapping under IcebergCompat* (#757): `CreateAsync` /
`CreateOrReplaceAsync` turn on `name` mode when an IcebergCompat property is
set and the mode is left out, as delta-spark does; explicit `none` is refused
with `DELTA_ICEBERG_COMPAT_VIOLATION`. Turning IcebergCompat on for an
existing unmapped table is refused, as Spark refuses it.

*`delta.enableIcebergCompatV3`* (#745) follows delta-spark 4.4's checks,
including Iceberg's reserved row-lineage field ids on materialized
row-tracking columns. Every IcebergCompat version stores partition values
in data files, on appends, compaction and DML rewrites (#756). Gaps:

- A V3 table created by delta-spark 4.3.1 lacks `delta.enableRowTracking`,
  and EW refuses to write it (delta-spark 4.4 adds the property on its next
  commit; EW does not change properties on a write).
- delta-spark 4.4 refuses V3 writes unless
  `spark.databricks.delta.uniform.iceberg.v3.enabled` is set; EW has no such
  switch.
- V3's geospatial types are not modelled.
- For V1/V2, EW checks only column mapping, types (`void`, and V1's arrays
  and maps) and deletion vectors. delta-spark's V2 also refuses `variant`, a
  decimal partition column, partition evolution, a type change Iceberg
  cannot follow, and V1 + V2 together.

*`delta.feature.<name>`* (#601) and *`delta.minReaderVersion` /
`delta.minWriterVersion`* (#639) are consumed at `CreateAsync` /
`CreateOrReplaceAsync`, not stored, as in Spark 4.0.0. A version floor
becomes the smallest legacy protocol implying exactly the requested
features, so it can be lower than asked (writer 7 alone is `(1, 1)`). One
difference: Spark refuses a REPLACE that brings a legacy writer floor to a
reader-3 table (`DELTA_READ_FEATURE_PROTOCOL_REQUIRES_WRITE`); EW merges it.
A caller-supplied `metaData` action introducing any of these keys is
refused; use a `protocol` action.

*Not read, with consequences for a table EW writes to:*

| Property | What EW does instead |
|---|---|
| `delta.compatibility.symlinkFormatManifest.enabled` | Symlink manifests are not regenerated. |
| `delta.setTransactionRetentionDuration` | Expired `txn` identifiers are never dropped from checkpoints. |
| `delta.rowTrackingSuspended` | Not read; Spark's writer expectations while suspended are unverified. |
| `delta.enableChangeDataCapture` | Not read; how Spark uses it is unverified. |
| `delta.randomizeFilePrefixes`, `delta.randomPrefixLength` | Data files get no random prefixes. |
| `delta.checkpointRetentionDuration` | Not read; log cleanup uses `delta.logRetentionDuration`. |

*Moot, because EW refuses the table's feature:*
`delta.coordinatedCommits.*-preview`, `delta.redirectReaderWriter-preview`,
`delta.redirectWriterOnly-preview`,
`delta.enableMaterializePartitionColumnsFeature` /
`delta.writePartitionColumnsToParquet`,
`delta.requireCheckpointProtectionBeforeVersion`.

*Not relevant to EW:* `delta.autoOptimize*`, `delta.castIcebergTimeType`,
`delta.ignoreIcebergBucketPartition`, `delta.ignoreProtocolDefaults`,
`delta.sampleRetentionDuration`,
`delta.dropFeatureTruncateHistory.retentionDuration`,
`delta.enableFullRetentionRollback` (EW has no DROP FEATURE),
`delta.parquet.format.version`, `delta.lastCommitTimestamp`,
`delta.lastUpdateVersion`.

**Second and nanosecond timestamps are refused, not converted.** Delta
writes (`SchemaConverter`, and the batch checks in
`ComputeWriteActionsAsync` / `WriteDataFilesAsync`) refuse
`TimeUnit.Nanosecond` (Delta is microsecond precision) and
`TimeUnit.Second`. Cast to `TimeUnit.Microsecond` first. Both could be
converted; that needs a value-converting pass and is deferred. The Parquet
writer itself accepts all four units.

**Stats collection: `tightBounds`.** EW sets `tightBounds=false` only when a
deletion vector is attached; fresh files leave it off (read as `true`).
`nullCount` is not rewritten for wide bounds, which is correct because EW
counts physical rows. EW does not answer `MIN`/`MAX`/`COUNT` from
statistics, so the flag matters only to other engines.

**`stats_parsed`.** `StatsParsedBuilder` writes typed per-file bounds as
`add.stats_parsed`, matching delta-spark 4.1.0's layout (it is not in
PROTOCOL.md). Boolean, binary, array and map columns have no
`minValues`/`maxValues` there, only `nullCount`. A bound that does not fit
the column's type is written as null. delta-spark 4.0.0 neither reads nor
writes the field; 4.1.0 writes it by default but reads it only when `stats`
is absent; delta-rs 1.6.2 writes JSON stats only. EW reads it for pruning
(`DeltaTableOptions.PreferTypedCheckpointStats`, default true, decides only
when both copies exist) and falls back to JSON for columns it does not bound.
Code that reads row counts or rewrites stats must use
`AddFile.GetNumRecords()` / `AddFile.GetStatsJson()`, not `Stats`: a
struct-only checkpoint has no JSON string.

**CommitInfo.** EW writes `timestamp`, `operation`, `inCommitTimestamp`,
`engineInfo`, `operationParameters`, `isBlindAppend` (when the caller says),
`txnId`, `userMetadata`, and caller `CommitProperties` keys (#564), which may
not be any field of Spark's `CommitInfo`. `operationMetrics` (Spark's names,
as strings) is written by DELETE, UPDATE, MERGE (#577) and VACUUM, not by
WRITE, OPTIMIZE or metadata commits, and never Spark's timing metrics.
`operationParameters` carries the condition of a DELETE, UPDATE, MERGE or
replaceWhere (#591) as Spark SQL from `SparkSqlRenderer` (`(id = 2L)`), not
Spark's own Catalyst debug text for DML. A transaction records metrics and
parameters only when everything it staged is one kind of DELETE, UPDATE or
MERGE. A delegate condition, a row selection, or a condition with no Spark
SQL form records no parameters. Never written: `readVersion`,
`isolationLevel`, `userId`, `userName`, `clusterId`, `notebook`.

**No feature or property changes on an existing table.** There is no
ALTER-style property or protocol API. `AddColumnAsync` and
`SetClusteringColumnsAsync` upgrade the protocol as needed, and `CreateAsync`
declares schema-driven features (`timestampNtz`, `identityColumns`,
`columnMapping`) plus `deletionVectors` / `rowTracking` on request, but
`deletionVectors`, `rowTracking`, `typeWidening`, `inCommitTimestamp` and
other table properties can only be set at create (or replace) time.

**High-level DML limits.** `DeleteAsync`, `UpdateAsync`, `DeleteRowsAsync`,
`UpdateRowsAsync`, `MergeAsync` (#563) and `RestoreAsync` (#567) exist, and
on a DV table UPDATE and MERGE mask rows with deletion vectors as Spark does
(#704, #574). `UpdateRowsAsync` always copy-on-writes. Limits:

- *MERGE* takes key-equality conditions only, not an arbitrary ON
  condition. An assignment names a top-level column, not a nested field. A
  `timestamp_ntz`, struct, array or map column takes only a value already of
  its type (the expression library cannot cast to those), and a column
  copied from the source (`*`, a column list) must already have the table's
  type, where Spark casts it. A source column differing from a table column
  only in case is refused (Spark matches it). Key-range file pruning (#578)
  covers `byte`, `short`, `integer`, `long`, `date`, `timestamp`,
  `timestamp_ntz`, `decimal` and `string` keys, not `float`, `double`,
  `boolean` or `binary`. `MergeOptions.MergeSchema` (#604) shares the
  `mergeSchema` limits below.
- *RESTORE* also refuses a missing deletion-vector file (Spark does not),
  resolves a timestamp by `commitInfo.timestamp` where Spark uses the commit
  file's modification time (they agree unless the log was copied), and
  cannot downgrade the protocol.
- *Missing:* CLONE (shallow/deep), and an incremental read by version range
  outside the Change Data Feed.

**Schema evolution API: partial.** Top-level and nested ADD / RENAME / DROP,
`SetSchemaAsync`, and `WriteAsync(..., mergeSchema: true)` (#568, #603)
exist. `mergeSchema` refuses a name differing from a column only in case
(Spark would write into the existing column), does not widen integer to
double or decimal, widens no partition column or column a CHECK constraint
or generation expression reads, and never casts a narrower batch type up to
the table's. Missing: column reorder and nullability changes. Column
mapping mode is fixed at `CreateAsync`.

**Vacuum does not collect expired change-data-feed files.** `_change_data/`
is excluded from the sweep because `cdc` files are not in the snapshot's
active files and the snapshot does not track `cdc` actions. This
under-deletes; it cannot lose data. Spark and delta-rs do sweep it; see
[`vacuum-hidden-directories.md`](vacuum-hidden-directories.md).

**Vacuum refuses tables with absolute-path deletion vectors.** A
`storageType: "p"` vector cannot be proven to lie outside the swept
directory, and deleting a live one would resurrect every row it masks, so
vacuum throws `NotSupportedException`. EW never writes `p` vectors.

### Correctness / interop issues

**Deletion vectors are opt-in; without them a DELETE rewrites.**
`CreateAsync(..., enableDeletionVectors: true)` sets
`delta.enableDeletionVectors` and declares the `deletionVectors` feature
(reader 3 / writer 7). Only then does a DELETE that matches part of a file
write a deletion vector; otherwise it rewrites the file (copy-on-write,
#562), as Spark and delta-rs do. `RowDeleteMode` on `DeleteAsync` /
`DeleteRowsAsync` / `StageRowDeletesAsync` chooses explicitly;
`DeletionVector` on a non-DV table refuses a partial-file delete with
`InvalidOperationException`. IcebergCompat V1/V2 tables cannot have DVs
(#756). DVs cannot be enabled on an existing table. delta-rs 1.6.2 refuses
to read DV tables (`DeltaProtocolError: ... not yet supported`); Spark 4.0
reads them.

**Binary partition values are unsupported.**
`PartitionUtils.GetStringValue` throws `NotSupportedException` for `binary`
(and nested) partition types; the spec defines a binary encoding.

**V2 sidecar paths.** `CheckpointReader` treats a sidecar path containing
`/` as table-relative as written, and a bare name as under
`_delta_log/_sidecars/`; other spellings (such as an absolute URI) are not
resolved.

**Non-ASCII partition paths: Spark and delta-rs disagree; EW follows
Spark.** Spark 4.0 leaves non-ASCII literal in both the directory and
`add.path`; delta-rs 1.6.2 percent-encodes it as UTF-8:

| value | Spark 4.0 / delta-spark 4.0.0 | delta-rs 1.6.2 |
| --- | --- | --- |
| directory | `region=café` | `region=caf%C3%A9` |
| `add.path` | `region=café` | `region=caf%25C3%25A9` |

EW is byte-identical to Spark at both layers (pinned by
`EwPartitionPaths_AreIdenticalToSparks`), including the characters Spark
does escape. A table written by both Spark (or EW) and delta-rs gets two
directories for one partition value. Reads are unaffected, since partition
values come from `add.partitionValues`.

**Column-mapping protocol shape differs from Spark's.** EW emits the legacy
`minReader=2` / `minWriter=5`; Spark emits `minReader=2`, `minWriter=7`,
`writerFeatures: [columnMapping, invariants, appendOnly]`. Both are
spec-legal and Spark reads EW's form. delta-rs reads neither (column mapping
is unimplemented there). Writer 5's implied `checkConstraints` /
`generatedColumns` / `changeDataFeed` impose obligations only on a table
that uses them, and EW enforces them (see above), so the difference is
cosmetic. Likewise, when another feature forces reader 3 / writer 7, EW
names every feature the legacy pair implied where Spark names only
`[appendOnly, columnMapping, invariants, ...]` (#605). Full v3/v7 would
only raise the reader bar; EW detects column mapping from
`delta.columnMapping.mode`, so it reads Spark's form.

---

## Iceberg

**EW can operate only on tables whose metadata it wrote itself.** Reading
any Avro manifest or manifest list whose writer schema differs from EW's
own (compared in Avro Parsing Canonical Form) throws
`NotSupportedException` (#775). That covers manifests from Iceberg Java,
Spark and pyiceberg, V1 manifests, and partitioned manifests. The refusal
also stops a commit from rewriting a foreign manifest and dropping its
metadata. See
[#586](https://github.com/clast-project/engineered-wood/issues/586).

### Missing features

**No data-file writer.** `TableOperations.AppendFilesAsync` takes pre-built
`DataFile` records; callers write Parquet and compute column stats
themselves. EngineeredWood.Iceberg is a metadata library.

**Row-level operations.** No MERGE / UPDATE / DELETE.
`AppendDeleteFilesAsync` takes pre-built delete-file records; it does not
produce position or equality deletes.

**Partition transforms.** `Transform.cs` declares `Identity`, `Void`,
`Bucket`, `Truncate`, `Year`, `Month`, `Day` and `Hour`, but none can be
applied to a value, so partition values cannot be derived from data.

**V1 manifests.** Only the V2 manifest schema exists
(`ManifestAvroSchemas.cs`); V1 manifests can be neither read nor written.

**Puffin files.** Statistics and V3 deletion-vector sidecars are not
implemented.

**Catalogs.** Only `FileSystemCatalog` and `InMemoryCatalog`. No REST, Glue,
Hive, JDBC, Nessie, Polaris or Snowflake Open Catalog.

**Delete files are not applied.** `ScanResult.DeleteFiles` lists them, but
nothing filters rows by position or equality deletes, sequence numbers, or
V3 deletion vectors.

**Scan planning.** `TableScan` has no `AsOfTimestamp()` (it exists in
`TimeTravel`), incremental scans, per-file residuals, branch/tag selection,
or split planning.

**Metadata operations.** No `SortOrderUpdate`. `SchemaUpdate` has no
reorder, nested add, or `FIRST`/`AFTER`. No format-version upgrade.

**Views, materialized views, manifest compaction.** Not implemented.

**Table encryption.** `key_metadata` is always written null; no KMS
plumbing.

### Correctness / interop issues

**The manifest codec is truncated.** `ManifestIO` omits `partition`
(written as an empty record, so partitioned tables get an empty partition
tuple), `lower_bounds` / `upper_bounds` (stats pruning in `TableScan` works
only with in-process `DataFileStats`), `nan_value_counts`,
`distinct_counts`, `key_metadata`, `equality_ids`, `referenced_data_file`,
`content_offset`, `content_size_in_bytes`, `first_row_id` and `spec_id`.
Manifests EW writes do not match the declared V2 schema, so other Iceberg
tools cannot consume them (#586).

**Manifest-list partition summaries are written null**, so readers cannot
skip whole manifests by partition.

**No byte encoders for bounds.** There is no Iceberg canonical-byte encoder
(date, timestamp, decimal, string, binary, uuid, ...), so callers cannot
build bounds to hand in.

**V3 features are declarative only.** `NestedField` initial/write defaults
are parsed but not applied on read. `TableMetadata.NextRowId` is never
incremented, so row lineage is not maintained. `DataFile.FirstRowId` is not
in the codec. `last-updated-sequence-number` is not tracked per row.

**V3 types without I/O.** `IcebergType.cs` declares all primitive and nested
types (including geometry, geography, variant, `timestamp_ns`,
`timestamptz_ns`), but there is no data-file writer and no Iceberg schema
bridge to Arrow or Parquet.

---

## Expressions

### Missing features

**Write-time expressions** (`delta.constraints.*`, `delta.invariants`,
`delta.generationExpression`) are evaluated by `DeltaConstraintEnforcer` and
`DeltaGeneratedColumns`; a violation, or an expression that cannot be parsed
or evaluated, refuses the commit. `AddConstraintAsync` /
`DropConstraintAsync` (#565) follow Spark 4.0. Which write paths evaluate
them is under
[Delta Lake](#delta-lake). Rows an UPDATE did not change are not
re-checked, so an UPDATE does not fail over existing rows another engine
wrote.

**NOT NULL is enforced against the table's schema** (`DeltaNotNullEnforcer`,
#514) on `WriteAsync`, the transactional append, create-with-data,
`WriteDataFilesAsync`, and `UpdateAsync` / `UpdateRowsAsync` post-images,
with `DELTA_NOT_NULL_CONSTRAINT_VIOLATED`. It also checks NOT NULL inside
arrays and maps, which Spark refuses to create
(`DELTA_NESTED_NOT_NULL_CONSTRAINT`), so EW can refuse a null element Spark
would write. Files a host commits through the commit seam are not checked
for nulls.

**Gaps inside the parser and registry** (each refused by name, never
answered wrongly):

- The timezone for temporal conversions is fixed at UTC
  ([#133](https://github.com/clast-project/engineered-wood/issues/133)).
- `INTERVAL` literals, the `Y` and `S` literal suffixes, subqueries, window
  functions and `*` are not parsed. `current_date` / `current_timestamp` are
  deliberately absent (Delta forbids non-deterministic expressions in
  constraints and generated columns).

**No row-level post-filter on the Parquet or Delta read paths.** Both
filters are superset-safe: surviving batches still contain non-matching
rows, which the caller must filter. `ArrowRowEvaluator` is used by Delta
DML and `LanceTable.ReadAsync`, not by either reader.

**Dictionary-page pruning needs `encoding_stats`.**
`ParquetReadOptions.FilterUseDictionaries` (default true, #57) rules out a
row group when an equality or IN value is absent from a chunk's dictionary,
but only for chunks whose `encoding_stats` prove every data page is
dictionary-encoded; files without them (e.g. arrow-rs output) are not
pruned. On object storage it adds GETs (sent concurrently); set it false
where GETs are rationed. Dictionaries never reach Arrow, so they do not
speed row-level filtering.

### Correctness / interop issues

**Spark refuses an `IN` list whose members share no type; EW answers.**
`a IN (bl)` is an analysis error in both Spark dialects, and `bl IN
('true')` is one under the legacy dialect (Spark's string promotion excludes
boolean and binary). EW has no analysis phase and evaluates both. Closed as
not adopted
([#261](https://github.com/clast-project/engineered-wood/issues/261));
pinned by the `string-coercion` group of `spark-expression-corpus.json`.
