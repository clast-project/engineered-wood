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

> **Last verified against the code: 2026-07-31**, when every section was
> re-checked claim by claim — the ORC, Avro, Iceberg and Expressions sections
> for the first time. They held up: three entries needed correcting (ORC's
> LZ4 support, ORC's writer identification, and the precision of Parquet's
> `distinct_count` claim) and the rest were accurate.
>
> If you are relying on an entry here, confirm it against the code before
> acting on it — this file records absences, and absences are exactly what
> nothing fails when they stop being true. The Delta entries additionally have
> external coverage (delta-rs and PySpark) in
> `test/EngineeredWood.DeltaLake.Table.Tests/Interop/`.

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
- `Statistics.distinct_count` is never computed. The field is plumbed —
  `Statistics.DistinctCount` is nullable and `ColumnChunkWriter` copies it
  to the Thrift struct — but `StatisticsCollector` never populates it, so
  it is always absent from written files.

### Correctness / interop issues

**VARIANT — three caveats, none affecting the common annotated path.**

- *The shredded layout is not preserved on read.* A shredded column is
  reassembled into an ordinary unshredded `VariantArray` — correct and
  uniform, but a caller cannot inspect `typed_value` afterwards. If that is
  ever needed (predicate pushdown into `typed_value`), it should become an
  explicit opt-in on `ParquetReadOptions` rather than a change of default.
- *An unannotated NESTED variant is not wrapped.* Nested wrapping keys off
  the parquet reader's variant-awareness, so a variant inside a
  struct/list/map that carries no logical-type annotation — Spark 4.0.x
  output, or EW's own with `EmitVariantLogicalType=false` — reads back as a
  bare struct. The Delta-layer coercion that repairs this is top-level only.
- *Two upstream Arrow gaps are worked around in `VariantShredding`.* The
  shred pipeline has no validity, so a SQL-null row cannot be expressed and
  `VariantShredding.WithValidity` exists to repair the result
  ([apache/arrow-dotnet#398](https://github.com/apache/arrow-dotnet/issues/398));
  and there are no array-level entry points, so `Reassemble` and the decode
  loops are ours to carry
  ([#399](https://github.com/apache/arrow-dotnet/issues/399)). If both land,
  `VariantShredding.cs` shrinks to a few calls.

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
(`orc_proto.proto` declares both) but `OrcCompression.ToCodec` maps
neither, so both fall through to its `NotSupportedException`. `NONE`,
`ZLIB`, `SNAPPY`, `LZ4` and `ZSTD` are all mapped.

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

**Writer identification.** `OrcWriter` sets both `Footer.Writer` and
`PostScript.WriterVersion` to the same constant `6`. The two fields have
different meanings, and only one of the values is right:

- `PostScript.WriterVersion = 6` is **correct**. The spec reserves values
  below 6 for the ORC Java writer and says every other writer should start
  its own sequence at 6, which is what "original" means for the C++,
  Presto, Scritchley Go, Trino and CUDF writers alike.
- `Footer.Writer = 6` is **not a registered producer id**. The ids
  `orc_proto.proto` documents stop at 5 (0 = ORC Java, 1 = ORC C++,
  2 = Presto, 3 = Scritchley Go, 4 = Trino, 5 = CUDF), so a conformant
  reader cannot identify the producer of an EngineeredWood file.

The consequence is worse than cosmetic, because `writer_version` is only
interpretable *relative to the writer id*. A reader that falls back to Java
semantics for an unknown producer reads version 6 as "ORC-135 fixed
(timestamp statistics use utc)" — a guarantee EngineeredWood does not meet,
since it never populates `TimestampStatistics.MinimumUtc` / `MaximumUtc`
(see the timestamp entry below).

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
default — "for complex types, null is the safest fallback", as the code
says. The spec requires JSON arrays/objects to be applied element-by-
element, and fixed defaults to be ISO-8859-1-decoded bytes. Honored today:
primitive defaults, enum defaults, and logical-typed `fixed` defaults
(`AppendFixedLogicalDefault` covers the decimal case).

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

**Row tracking (`delta.enableRowTracking=true`) — one residual limitation.**
Row tracking is otherwise fully supported: `CreateAsync(enableRowTracking: true)`
enables it (generating the materialized column-name properties + declaring the
`rowTracking`/`domainMetadata` writer features), appends are spec-conformant
(`baseRowId` + position), and a copy-on-write rewrite (UPDATE / OVERWRITE /
DELETE / compaction) PRESERVES each surviving row's stable id by materializing
its original id + commit version into the declared hidden columns. Optimistic
concurrency rebases under row tracking (append/update re-derive `baseRowId` from
the advanced high-water mark; a losing DELETE remaps by stable row id across a
concurrent rewrite). All of this is measured cross-engine on Spark 4.0
(`SparkInteropTests` — Spark reads the preserved / rebased ids via
`_metadata.row_id`), in BOTH directions: EW-written tables Spark reads, and
Spark-written tables (with Spark's own hyphenated materialized column names)
that EW reads and then rewrites without losing the ids Spark assigned.
The one residual: `DeltaTable.RejectRowTrackingWrite`
refuses a copy-on-write REWRITE of a row-tracking table that does NOT declare
its materialized column-name properties (a spec-invalid shape EW never creates,
but a foreign engine could) — without those names EW cannot preserve ids through
the rewrite. Appending to and reading such a table is still allowed.

Readers can now see the ids: `ReadAllWithRowTrackingAsync` /
`ReadAtVersionWithRowTrackingAsync` append `_metadata.row_id` and
`_metadata.row_commit_version`, measured equal to Spark's own resolution row by
row — requested as `DeltaRowMetadata.RowTracking` on `ReadAsync`. The Change
Data Feed carries them too, as of the CDF row-tracking work: the same metadata
flag on `ReadChangesAsync` emits the two columns on the feed, every
`_change_data` file EW writes on a row-tracking table materializes the hidden
id/commit-version columns (a `cdc` action has no `baseRowId`, so materializing
is the only way identity reaches a change row), and both directions are measured
against Spark 4.0.1. The last gap on that surface — fixed emitted column names,
with no option to rename them for a host that cannot use a dotted identifier —
is CLOSED: `DeltaReadOptions.MetadataPrefix` /
`DeltaChangeReadOptions.MetadataPrefix` rename them, and a collision with a
table's own column is refused rather than shadowed. Background:
`doc/row-tracking-conformance-brief.md`.

**Multi-part V1 checkpoints on write.** Read is supported
(`CheckpointReader.cs`); write always emits a single
`.checkpoint.parquet` (`CheckpointWriter.cs`), regardless of table size.

**V2 checkpoints are never written automatically.** `V2CheckpointWriter`
is public and works, but nothing in the library calls it: the
`delta.checkpointInterval` path in `DeltaTable` only ever runs
`CheckpointWriter`, so a table EW maintains gets V1 checkpoints
regardless of whether it has the `v2Checkpoint` feature. The writer also
does not verify that the feature is enabled before producing a V2
checkpoint, which the spec requires of a writer. A host that wants V2
must call `V2CheckpointWriter` itself and check the protocol itself.

**Parquet-bodied V2 checkpoints on read.** `n.checkpoint.{uuid}.parquet`
is not decoded — only the NDJSON body is. `v2Checkpoint` remains a
supported *reader* feature, which is correct: the JSON form is read, and
refusing the feature outright would reject tables that read perfectly.

A table carrying the Parquet body is still read normally whenever
anything else covers the range (its commits, or an older readable
checkpoint). Only when that checkpoint is the sole route to the requested
version does the read fail, and it now fails with
`DELTA_UNSUPPORTED_CHECKPOINT_FORMAT` naming the checkpoint — previously
it reported "Delta log is incomplete", sending the reader to look at
retention settings for what is a limitation of this decoder.

Measured 2026-08-06: delta-spark 4.0.0 with `delta.checkpointPolicy=v2`
writes the **JSON** body (`<n>.checkpoint.<uuid>.json` plus a Parquet
sidecar), which EW reads — see
`SparkInteropTests.SparkWrittenV2Checkpoint_EwReadsFromTheCheckpointAlone`.
So this is a compatibility ceiling for writers that choose the Parquet
body, not a barrier to reading Spark's output.

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

**Timestamp units are refused rather than converted.** `SchemaConverter`
rejects two Arrow timestamp units at write, and the incoming batches are
checked again in `ComputeWriteActionsAsync` / `WriteDataFilesAsync` because a
write into an existing table converts no schema:

- `Nanosecond` — Delta timestamps are microsecond precision, and the ISO-8601
  stats strings stop at microseconds, so the low digits would be lost.
- `Second` — Parquet's `TimestampType` has only MILLIS/MICROS/NANOS. There is
  no second unit to map to, and `ArrowToSchemaConverter.MapTimeUnit` falls
  through to MICROS, leaving the raw seconds under a microsecond annotation.
  Measured before the rejection landed: `1700000000` (2023-11-14) round-tripped
  as `1970-01-01T00:28:20Z`.

TODO: convert instead of refusing. Both are losslessly representable in
microseconds — seconds and milliseconds scale up exactly, and nanoseconds could
take an explicit rounding option — so the write path could narrow the array to
`TimeUnit.Microsecond` and accept all four units. That needs a value-converting
pass over the batch (and its partition values) rather than a schema check, so it
is deliberately deferred; refusing is the cheap, safe interim. `Millisecond`
already round-trips exactly and is unaffected.

The underlying cause was a **Parquet-writer** bug rather than a Delta one, and
is now fixed there too: `ArrowToSchemaConverter.MapTimeUnit` fell through to
MICROS for any unmapped unit, relabelling values instead of rescaling them. It
throws instead, which covers nested columns for free since `MapArrowType` is
reached per leaf while building the schema tree. That also closed a second,
worse case — `Time32(Second)` was written as INT32 annotated TIME(MICROS), an
illegal pairing (micros requires INT64) whose file could not be read back at
all. `Timestamp(Nanosecond)` stays valid at the Parquet level; NANOS is a real
Parquet unit, and only Delta cannot carry it.

**Stats collection gaps.**

- `tightBounds` is written only where it changes meaning: `StatsWithLooseBounds`
  marks a file wide wherever a deletion vector is ATTACHED, since the min/max then
  describe rows the vector removed. A freshly written file leaves the flag off,
  which the spec reads as `true`. Only the flag is rewritten, not `nullCount`:
  Delta's tight-state null counts are logical and have to be converted on the way
  to wide, whereas EW collects over the physical rows and never recomputes, so an
  all-null column's count already equals the physical `numRecords` the wide
  reading tests against.

  Worth knowing how little this buys on the read side: the spec says the bounds
  are "sufficient information for data skipping" either way, and delta-spark's
  `DataSkippingReader` never mentions the flag. It matters to a reader answering
  `MIN`/`MAX`/`COUNT` from statistics alone, which EW does not do — the flag is
  written for other engines' benefit, not our own. EW's pruner is safe against
  wide statistics regardless: it only skips on the two `nullCount` states (0 and
  `== numRecords`) that the spec preserves when bounds go wide.
- `delta.dataSkippingNumIndexedCols` / `delta.dataSkippingStatsColumns`
  are ignored; every eligible column gets stats.

(String-stat truncation and nested-struct recursion are both implemented —
`StatsCollector.TruncateMaxString` and `CollectStruct`. Nested stats are
verified externally: `EwWritten_NestedStats_SparkSkipsOnNestedFieldWithoutLosingRows`
asserts Spark prunes on `payload.score` and still returns every matching row.
The checkpoint's own copy of the JSON stats is verified by
`EwCheckpointed_MinMaxStats_SparkSkipsFilesReadingTheCheckpointAlone`, which hides
the earlier commits so only the checkpoint can answer the scan.)

**`stats_parsed`.** `StatsParsedBuilder` writes typed per-file bounds as
`add.stats_parsed` — inside the add struct, where delta-spark writes them and the
only place its readers look. It is an implementation extension, not a spec'd
field: `stats_parsed` appears nowhere in `PROTOCOL.md` (checked), so delta-spark's
layout is the only definition there is, and EW's is measured against it rather
than copied from prose.

Which delta-spark you run decides whether it reads or writes the column at all.
4.0.0 (the pairing tier 3 pins) does neither — its `buildCheckpoint` adds only
`partitionValues_parsed` and its `loadActions` maps `add.stats` and nothing else;
the `extractStats` call landed after that tag. **4.1.0 writes `add.stats_parsed`
by default** (`checkpoint.writeStatsAsStruct` defaults to `true`) and reads it
back, but only as a fallback: `Snapshot.loadActions` re-encodes the typed struct
to JSON **iff** the add struct has `stats_parsed` and lacks `stats`, then drops
the field. A checkpoint carrying both — Delta's default, and EW's — is always read
from the JSON. delta-rs 1.6.2 writes JSON stats only. All measured.

`delta.checkpoint.writeStatsAsJson` / `delta.checkpoint.writeStatsAsStruct` are
honoured (`CheckpointStatsMode`), both defaulting to true as in delta-spark, and
settable at create time through `DeltaTable.CreateAsync(configuration: ...)`.
Turning JSON off leaves the typed struct as the only statistics, which is the
shape `EwCheckpointed_StructStatsOnly_SparkPrunesFromTheTypedStats` uses to prove
Spark prunes from EW's typed values — it needs delta-spark 4.1+ and self-skips on
4.0.

Bounds carry each column's own Arrow type (`decimal(p,s)`, `timestamp`), decimal
digits decoded exactly rather than through `System.Decimal`; nested structs
recurse; and boolean/binary/array/map columns are absent from `minValues`/
`maxValues` while still counted in `nullCount` — matching delta-spark 4.1.0's own
checkpoint, measured for
`(id BIGINT, amount DECIMAL(9,2), d DATE, ts TIMESTAMP, s STRING, b BOOLEAN)`:

```
add.stats_parsed.numRecords          bigint
add.stats_parsed.minValues           struct<id:bigint,amount:decimal(9,2),d:date,ts:timestamp,s:string>
add.stats_parsed.maxValues           struct<id:bigint,amount:decimal(9,2),d:date,ts:timestamp,s:string>
add.stats_parsed.nullCount           struct<id:bigint,amount:bigint,d:bigint,ts:bigint,s:bigint,b:bigint>
```

A bound that will not fit the column's own type is written as null (no bound)
rather than rounded, since a wrong bound skips a file that matches.

**EW reads `stats_parsed` for pruning.** `CheckpointStatsView` maps a checkpoint's
typed columns once per batch and each `AddFile` carries its row, so a bound costs
one indexed read instead of parsing the file's whole statistics blob — which the
pruner otherwise does inside `ShouldInclude`, i.e. per file per query. Measured
over a 100,000-file checkpoint with a single-column predicate: 210 ms -> 15 ms and
413 MB -> 4 MB of allocation. `DeltaTableOptions.PreferTypedCheckpointStats`
(default true) decides only the tie; a checkpoint carrying one copy is read from
that one either way, and a column the typed struct does not bound falls back to the
JSON, which is parsed lazily so the fast path never pays for it. That fallback is
load-bearing: `stats_parsed` omits boolean bounds and EW's JSON statistics carry
them, so a typed-only lookup silently stops pruning on booleans.

`AddFile.GetNumRecords()` reads the row count from whichever copy has one. Callers
must not reach for `Stats` directly — a struct-only checkpoint has no JSON string,
and a row count silently read as zero would mis-assign row ids and mis-size
compaction groups.

**Statistics survive a move on a struct-only table.** `CheckpointStatsView.BuildStatsJson`
writes a file's typed statistics back out as a Delta `stats` string — the inverse
of what `StatsParsedBuilder` read in, and the same answer delta-spark reaches with
`to_json(stats_parsed)`. Anything that WRITES statistics back goes through
`AddFile.GetStatsJson()` rather than `Stats`: the serialiser, and the loose-bounds
rewrite a deletion vector triggers. Without it, a file read from a checkpoint with
`writeStatsAsJson=false` lost its statistics the moment a DELETE or compaction
re-committed it, and the table scanned every file from then on. Values go out in
the forms `StatsCollector` emits, so a synthesised string is interchangeable with
an original one — including decimals, whose exact digits never pass through
`System.Decimal`.

**CommitInfo.** `InCommitTimestamp.CreateCommitInfo` emits `timestamp`,
`operation`, `inCommitTimestamp`, `engineInfo` and `operationParameters`
(at minimum an empty object, which strict readers require). Spec-standard
fields still never emitted: `readVersion`, `isolationLevel`,
`operationMetrics`, `userId`, `userName`, `txnId`, `clusterId`, `notebook`.

**Post-creation protocol upgrades — partial.** `AddColumnAsync` and
`SetClusteringColumnsAsync` upgrade the protocol as needed via
`UpgradeProtocolForFeatures` / `UpgradeProtocolForWriterFeatures`, and
`CreateAsync` declares schema-driven features (`timestampNtz`,
`identityColumns`, `columnMapping`) up front, and its `enableDeletionVectors:
true` / `enableRowTracking: true` switches declare `deletionVectors` /
`rowTracking` at creation. There is still no general public API for enabling
`deletionVectors` / `rowTracking` / `typeWidening` / `inCommitTimestamp` on an
EXISTING table — these can only be declared at create time.

**Exactly-once transactional writes — no convenience overload.** The
primitives are all public: the `TransactionId` (`txn`) action is reconciled onto
`Snapshot.AppTransactions` (readable for the last committed version per `appId`),
and a `TransactionId` can be fused into a commit via `CommitDataFilesAsync`'
`extraActions`. What is missing is a dedicated `WriteWithTxnAsync(appId, version,
…)` overload that packages the idempotency check + `txn` action for a streaming
writer, so today the caller must wire those primitives together by hand.

**File pruning is not reachable by an embedding host.** `DeltaFilePruner` —
the unified partition + stats pruner — is `internal`. A host that owns its
own execution engine can get a pruned file list from `PlanFiles`, but
cannot apply EW's pruning to a candidate set it assembled itself. Making the
type public is the whole change; the constraint is API surface, not
capability.

**High-level DML.** `DeleteAsync` and `UpdateAsync` each have a functional
overload and an analyzable-`Expressions.Predicate` overload (the predicate form
feeds file pruning + concurrency read-set analysis); `DeleteRowsAsync` /
`UpdateRowsAsync` do row-level DML keyed by a path-keyed `RowSelection`. Still
missing: MERGE, RESTORE (committing a time-travel state as the current version),
and CLONE (shallow/deep). `ReadChangesAsync` exists for CDF but there is no raw
incremental-by-version-range read outside of CDF.

**Schema evolution API — partial.** Metadata-only commits exist for top-level
ADD / RENAME / DROP COLUMN (`AddColumnAsync` / `RenameColumnAsync` /
`DropColumnAsync`) and the nested-struct-field analogs (`AddFieldAsync` /
`RenameFieldAsync` / `DropFieldAsync`) — each of those six also has a
compute-only `Compute*` counterpart that a buffered transaction fuses with data
changes — plus whole-schema adoption (`SetSchemaAsync`, the CREATE-OR-REPLACE
building block, which commits directly). Still missing: column reorder and
nullability change (drop/add NOT NULL). Column mapping mode is fixed at
`CreateAsync`.

**Vacuum does not collect expired change-data-feed files.** `_change_data/`
is excluded from the sweep because CDF files are referenced by `cdc`
actions, which never appear in the snapshot's active files — a keep-set
built from `add` actions alone does not cover them, and sweeping the
directory would destroy readable history. Building a proper CDF keep-set
needs the snapshot to track `cdc` actions, which it does not yet. This
under-deletes; it cannot lose data. Note this is a deliberate divergence from
the reference implementations, which *do* sweep `_change_data/` — see
[`vacuum-hidden-directories.md`](vacuum-hidden-directories.md).

**Vacuum sweeps directories the reference implementations protect.**
`VacuumExecutor.IsExcludedDirectory` names only `_delta_log/` and
`_change_data/`; delta-io/delta and delta-rs instead protect every `.`- or
`_`-prefixed directory, plus `metadata/` (UniForm's Iceberg output), and sweep
three named exceptions. Any sidecar another engine writes under the table root
is therefore collected once past retention. Not data loss — the affected
artifacts are derived — but silent destruction of another engine's metadata.
Spec, our divergences, and the reason `_delta_index/` is *correctly* swept are
in [`vacuum-hidden-directories.md`](vacuum-hidden-directories.md); tracked as
[#54](https://github.com/clast-project/engineered-wood/issues/54).

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
`ALTER TABLE`-style property update / protocol upgrade), and the predicate
`DeleteAsync` path has **no copy-on-write fallback** when DVs are off — it
removes whole files or throws. (A separate copy-on-write DELETE/UPDATE does
exist — `DeleteRowsAsync(selection, RowDeleteMode.CopyOnWrite)` / `UpdateRowsAsync`
— which rewrites the affected files with no DV. It needs neither deletion
vectors nor row tracking, preserves row-tracking ids when the table has them,
and writes the Change Data Feed for exactly the rows it touched; only
IcebergCompat tables are still refused on that path.)
Earlier EW always wrote a DV without declaring the feature, so a conformant
foreign reader silently returned the deleted rows; that data-loss gap is closed.

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

**Non-ASCII partition paths: Spark and delta-rs disagree, and we follow
Spark.** Not an EW defect — recorded here because the ecosystem split is
real and this entry previously got it backwards.

Partition path encoding is TWO layers: the on-disk Hive directory, and
`add.path` as a URL-encoding of that directory-relative path (so a `%` the
first layer produced appears as `%25` in the log). The two reference
implementations differ at layer 1 for non-ASCII:

| value | Spark 4.0 / delta-spark 4.0.0 | delta-rs 1.6.2 |
| --- | --- | --- |
| directory | `region=café` | `region=caf%C3%A9` |
| `add.path` | `region=café` | `region=caf%25C3%25A9` |

Spark leaves non-ASCII literal at both layers, because
`ExternalCatalogUtils.escapePathName` bounds its escape table at `c < 128`
and so never escapes anything above ASCII. delta-rs percent-encodes it as
UTF-8 bytes. Both are self-consistent: each engine's reader decodes what
its writer produced.

**EW is byte-identical to Spark**, at both layers, including the cases
Spark does escape (`#`→`%23`, `?`→`%3F`, and space escaped at layer 2 only
— `region=a b%23c%3Fd` on disk, `region=a%20b%2523c%253Fd` in the log).
Measured, not assumed: `Spark_NonAsciiPartition_PathEncodingGroundTruth`
pins Spark's output and `EwPartitionPaths_AreIdenticalToSparks` asserts
equality against that same run rather than against literals.
`DeltaRs_NonAsciiPartition_PathEncodingGroundTruth` pins delta-rs's
different answer.

Consequence to be aware of: a table written by BOTH Spark (or EW) and
delta-rs gets two different directories for one logical partition value.
That is an ecosystem wart, not something either engine can fix
unilaterally. Reading is unaffected in every direction — partition values
come from `add.partitionValues`, not from parsing the path — and delta-rs
reads EW's form fine (`EwWritten_NonAsciiPartition_DeltaRsReadsSameRows`).

**Column-mapping protocol shape differs from Spark's.** EW emits the
legacy `minReader=2`/`minWriter=5` pair; Spark emits a hybrid
(`minReader=2`, `minWriter=7`, `writerFeatures: [columnMapping, invariants,
appendOnly]`). Both are spec-legal and Spark reads EW's form. Note that
delta-rs cannot read column-mapped tables in EITHER form — it is an
unimplemented feature there, not a declaration mismatch — so changing this
would not widen reader support.

Both halves are measured. Spark reads EW's form
(`EwWritten_ColumnMapping_SparkResolvesPhysicalNamesToLogical`), and delta-rs
1.6.2 rejects reader 2 (legacy) and reader 3 + a `columnMapping` reader
feature alike. Writer v5's extra implied features
(`checkConstraints` / `generatedColumns`) impose obligations only on tables
that actually declare a constraint or generated column, and
`HonorWriterFeatures` already fails closed on those — so the divergence is
cosmetic. **Recommendation: leave it alone.** Full v3/v7 is strictly worse,
raising the reader bar 2→3 for no gain. The read side needs no work either
way: EW detects column mapping from `delta.columnMapping.mode` in the
configuration rather than from the protocol version, so it already reads
Spark's hybrid form.

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

**No table layer can push a predicate into the Parquet reader.** Row-group
pruning and bloom probing are implemented and tested, but nothing in `src/` sets
`ParquetReadOptions.Filter`: it is fixed when the `ParquetFileReader` is
constructed, and Delta holds one options record shared by the scan, CDF, DML and
compaction paths, so setting it there would prune row groups during OPTIMIZE's
rewrite. `DeltaReadOptions.Filter` therefore prunes whole files and stops.
Tracked as [#55](https://github.com/clast-project/engineered-wood/issues/55);
see [`predicate-pushdown-design.md`](predicate-pushdown-design.md).

**No row-level post-filter on the Parquet or Delta read paths.** Both filters
are superset-safe by design and documented as such — surviving batches still
contain non-matching rows. `ArrowRowEvaluator` exists and is used by Delta
predicate DELETE/UPDATE and by `LanceTable.ReadAsync`, but not by either
reader.

**Dictionary pages are used only to decode.** No dictionary-based row-group
pruning, and the dictionary never reaches Arrow (dictionary-encoded columns are
always materialised), so it cannot accelerate row-level filtering either.
Tracked as [#57](https://github.com/clast-project/engineered-wood/issues/57).
