# EngineeredWood: reusable parts for an Iceberg v3 reader/writer

> **Correction (2026-11-25):** the magic does *not* differ. Delta's `1681511377` is `0x6439D3D1`, stored little-endian as `D1 D3 39 64`, which is Iceberg's magic. A Delta DV blob is byte-identical to an Iceberg `deletion-vector-v1` blob; only the container (Delta's leading version byte vs Puffin) differs. See `doc/iceberg-v3-plan.md` sections 1 and 6.


Snapshot: `main` @ ef5c8c7f (2026-11-25). Read-only survey.
All paths are relative to the repo root.

Baseline: `src/EngineeredWood.Iceberg` (~4.4k lines) references only Core, Avro and Expressions
(`EngineeredWood.Iceberg.csproj`). It has **no Parquet reference** and so no data path at all. It models
v3 types (`IcebergType.cs:9-141`: variant, unknown, geometry(crs), geography(crs, algo), timestamp_ns /
timestamptz_ns), metadata JSON, catalogs, and a manifest codec. Its tests are all unit tests; nothing
is validated against another engine.

---

## 1. Parquet (src/EngineeredWood.Parquet)

| Need | Status | Where | Notes / gap |
|---|---|---|---|
| Read `field_id` | **Yes**, surfaced | `Metadata/SchemaElement.cs:37` (`int? FieldId`), decoded in `Metadata/MetadataDecoder.cs:198`; exposed on Arrow fields as metadata `PARQUET:field_id` (`Data/ArrowSchemaConverter.cs:22,30`, applied to leaf, list, list element, map, struct nodes at lines 43-243 and in `NestedAssembler.cs:279,326,559`) | Tests: `test/EngineeredWood.Parquet.Tests/Parquet/FieldIdMetadataTests.cs`, `test/EngineeredWood.DeltaLake.Table.Tests/ParquetFieldIdReadTests.cs`. |
| **Project by field id** | **Partial** | `Schema/SchemaDescriptor.cs:185` `ResolveFieldIds(IReadOnlyList<int>) -> IReadOnlyList<string?>` | Maps an id to its **top-level** column name only. A nested id resolves to the enclosing top-level column. All read APIs take `IReadOnlyList<string>? columnNames` (`ParquetFileReader.cs:3104 ResolveColumns`: a top-level name, or a leaf's dotted path). **Gap:** an id-to-path resolver for nested fields (struct fields, list elements, map key/value), plus an Iceberg-style "project schema by ids, fill missing ids with null or initial-default, then rename to the current names" layer. Delta's id-mode column mapping already does the top-level version (`DeltaLake.Table/DeltaTable.cs:13529-13548`); reuse that pattern. |
| Write `field_id` on every node | **Yes**, except the synthetic repeated groups | `Data/ArrowToSchemaConverter.cs:99 GetFieldId` reads `PARQUET:field_id` or `field_id` from each Arrow `Field`'s metadata; list (`:187`), map (`:215`), struct and leaf nodes carry it; the list **element** and the map **key/value** fields get theirs from their own Arrow field metadata (`AddField` is called recursively) | The repeated `list` / `key_value` groups carry no id, which Iceberg does not need. The element name is whatever the Arrow value field is called (Arrow's default is `item`, while Iceberg Java and the spec write `element`). Readers accept either, but set it for parity. Delta writes nested ids (`delta.columnMapping.nested.ids`, `DeltaLake/Schema/ColumnMappingRecursive.cs:473-579`); that is the closest analog to Iceberg element-id/key-id/value-id, and it is Spark-interop-tested (`IcebergCompatNestedIdsInteropTests.cs`). |
| Column stats for manifests | **Raw material is there; no Iceberg mapping** | `ParquetFileWriter.WrittenMetadata` (`ParquetFileWriter.cs:1061`) returns the footer `FileMetaData` once the file is closed: per chunk `ColumnMetaData` (`Metadata/ColumnMetaData.cs`: `NumValues`, `TotalCompressedSize`, `Statistics`); `Statistics` (`Metadata/Statistics.cs`) has `MinValue/MaxValue` (plain-encoded bytes), `NullCount`, `NanCount` (PARQUET-2249, `:47`; the writer emits it for float columns, `Data/ColumnChunkWriter.cs:1265`), and `IsMin/MaxValueExact`. Delta already consumes the footer this way (`DeltaTable.cs:11588`, `RollingDataFileWriter.cs:365`). | Parquet PLAIN stat bytes **are almost exactly Iceberg's single-value binary form**: LE int32/int64/float/double, UTF-8 strings, raw binary, FLBA(16) UUID big-endian, date as int32, timestamps as int64. Differences to code: (a) a decimal stored as INT32/INT64 must become minimal big-endian two's-complement unscaled bytes (FLBA stats are already big-endian fixed width, so trim the sign extension); (b) Iceberg truncates string and binary bounds (default `truncate(16)`, upper bound rounded up), and the Parquet chunk stats are untruncated, or truncated by EW's own rule (`IsMaxValueExact`); (c) bool is one byte in both formats; (d) per-field-id keying: walk `SchemaDescriptor.Columns`, take `ColumnDescriptor.SchemaElement.FieldId`, and sum over row groups. Iceberg keeps no bounds for repeated-leaf (list/map) columns. Nothing in the repo produces Iceberg bounds today: the Iceberg `DataFile` marks `ColumnLower/UpperBounds` as `[JsonIgnore]` and never encodes them in Avro (`Iceberg/Manifest/ManifestTypes.cs:101-103`). Delta's own stats come from Arrow batches as JSON (`DeltaLake.Table/Stats/StatsCollector.cs:17`, internal), so Iceberg cannot use them. |
| TIMESTAMP(NANOS) | **Yes** | `ArrowToSchemaConverter.cs:315,423` (Arrow `Timestamp(ns)` -> INT64 TIMESTAMP(NANOS)); read side `ArrowSchemaConverter.cs:483`. There is also an FLBA(12) extended timestamp (`Data/ExtendedTimestamp.cs`), which Iceberg does not need. | Fine. The Expressions literal for a timestamp is `DateTimeOffset` with 100 ns ticks, so ns predicates lose precision (see section 4). |
| VARIANT incl. shredding | **Yes, mature** | Logical type `Metadata/LogicalType.cs:33`; write via `ParquetWriteOptions.ShredVariants` / `VariantShredSchemas` (`ParquetWriteOptions.cs:448,462`); `VariantShredding.cs` (shred/unshred over `VariantArray`, backed by `Apache.Arrow.Operations`); `ShreddedVariantPath.cs`; read reassembly when `ParquetReadOptions.ExtensionRegistry` is set (`ParquetFileReader.cs:171`, `Data/VariantNestedWrapper.cs`); variant path pruning and stats | Interop-tested: `test/EngineeredWood.Parquet.Tests/Interop/DuckDbShreddedVariantTests.cs`, plus the Delta `Variant*InteropTests` (Spark). Iceberg v3 variant uses the same Parquet encoding, so this is directly reusable. Iceberg variant bounds (lower/upper per shredded path, serialized as a variant) do not exist; Delta's variant stats code (`DeltaLake/VariantStatsEncoding.cs`, `DeltaLake.Table/Stats/VariantFooterStats.cs`) is the nearest template. |
| GEOMETRY / GEOGRAPHY + bbox stats | **Missing** | `LogicalType.cs` has no Geometry/Geography records. An unknown logical type decodes to `UnknownLogicalType(ThriftFieldId)` (`:55`) and re-encodes as an empty struct (`MetadataEncoder.cs:269`), so the CRS and algorithm are dropped. No `GeospatialStatistics` (ColumnMetaData field 17) and no WKB handling. No source file outside the Iceberg type model mentions geometry. | Needs new work: the logical types, WKB passthrough as binary, bbox and geospatial-types stats on write and read, and the Iceberg bound encoding for geo (x/y/z/m points). |
| UUID / FIXED / DECIMAL | **Yes** | UUID: `arrow.uuid` extension <-> `UuidType` + FLBA(16) (`ArrowToSchemaConverter.cs:363,413`); FIXED: `FixedSizeBinaryType` -> FLBA(n) (`:347`); decimal: Decimal32/64/128/256 -> INT32/INT64/FLBA by precision (`:334-345`, `DecimalNarrowing.PhysicalTypeFor`) | This matches Iceberg's required decimal physical mapping. Decimals read as `SqlDecimal` (see the project's decimal convention). |
| Row group / file metadata writers | **Yes** | `ParquetFileWriter` (`ParquetFileWriter.cs:15`; `DeclareSchema :90`, `WriteBatchAsync :193`, `WriteRowGroupAsync :120`, `FlushRowGroupAsync :354`, `OpenRowGroupBytes :370`, `CloseAsync :1067`), `BufferedParquetWriter` (`BufferedParquetWriter.cs:33`), `ParquetWriteOptions` (row group rows and bytes, page index, bloom filters, key-value metadata `:547`, `WriteArrowSchema :560`) | For Iceberg, set `WriteArrowSchema` deliberately and put `iceberg.schema` JSON in `KeyValueMetadata` if parity with Java is wanted. `split_offsets` can come from the `RowGroup` file offsets in `WrittenMetadata`. |
| Constant / metadata columns (`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`) | **Not in Parquet; Delta has the pattern** | Parquet gives positions only: `ReadWithPositionsAsync` -> `PositionedRecordBatch` (`ParquetFileReader.cs:261,295`); `ReadSelectedAsync` -> `SelectedRecordBatch` with `RowPositions` (`ParquetFileReader.Selected.cs:30`, `RowPositions.cs:21-30`, explicit `Int64Array` when non-contiguous) | Delta appends `_metadata.file_path`, `row_index`, `row_id` and `row_commit_version` above the codec (`DeltaLake.Table/DeltaMetadataColumns.cs:19`, schema built at `DeltaTable.cs:7532-7541`, `DeltaRowMetadata.cs:15`). Iceberg `_row_id = first_row_id + _pos` with a materialized override is the same algorithm as Delta's `baseRowId + position` with a materialized column (`RowTrackingWriter.cs:18`), so lift it. |
| Position filtering (DV / position deletes) | **Yes, good fit** | `IExcludedRows` (`IExcludedRows.cs`: `ExcludesAll(firstRow, rowCount)`, `ClearExcluded(firstRow, rowCount, Span<ulong>)`) on `ParquetScan.ExcludedRows` (`ParquetScan.cs:32`), applied before `RowFilter` predicates, with whole-span skipping; also `ReadRowRangesAsync(rowGroup, IReadOnlyList<RowRange>)` (`ParquetFileReader.cs:1357`) and `GetCandidateRowRangesAsync` (`:3662`) | Delta's `DeletionVectorBitmap` already implements `IExcludedRows` (`DeltaLake/DeletionVectors/DeletionVectorBitmap.cs:22`). Iceberg would add a puffin DV and a position-delete-file implementation of the same interface. Equality deletes need an anti-join over `RowFilter` / `RowPredicate` (`RowPredicate.cs`), which does not exist. |
| Streaming writer + target size + rollover | **Writer yes; rollover lives in Delta** | Streaming: `ParquetFileWriter.WriteBatchAsync` (bounded memory, segmented page store #942). Rollover: `DeltaLake.Table/RollingDataFileWriter.cs:92` (internal; partition grouping `PartitionGroups :17`, `AddAsync :162`, `CompleteAsync :315`, per-file field-id injection `:103-133`, `TargetFileSize` from `DeltaTableOptions.cs:64`) | Gap: Parquet has no generic rolling writer. Either lift `RollingDataFileWriter` to a shared assembly or reimplement it. It is entangled with Delta stats and partition-path spelling. |

Other read features to reuse as-is: stats, dictionary, bloom and page-index pruning driven by an
Expressions `Predicate` (`ParquetReadOptions.Filter :287`, `ParquetScan.Prune`), `MemoryBudget`
(`ParquetReadOptions.cs:521`), and row-group concurrency.

---

## 2. Delta Lake (src/EngineeredWood.DeltaLake, src/EngineeredWood.DeltaLake.Table)

### Roaring / deletion vectors: reusable for Puffin `deletion-vector-v1`

- `src/EngineeredWood.Core/Encodings/RoaringBitmap.cs:32` (internal): the **32-bit portable** Roaring parser
  (`DeserializePortable :66`, `ParseContainers :127`), with a `RoaringFormat` flag (`:41`) for the
  Java/spec vs CRoaring cookie ambiguity. It is shared with Lance DVs.
- `DeltaLake/DeletionVectors/DeletionVectorBuilder.cs:265-293 Serialize`: writes the magic (`MT1d`,
  1681511377 LE), an **int64 LE sub-bitmap count**, then per sub-bitmap a **uint32 LE high key** and a
  portable 32-bit bitmap, with array/bitmap/run containers.
  `RoaringBitmapReader.cs:33` reads the same layout and calls `DeletionVectorBitmap.Parse(data, pos, subBitmaps)` (`DeletionVectorBitmap.cs:62`).
- **Assessment:** the bytes after Delta's 4-byte magic are exactly the "portable 64-bit Roaring" layout
  that Iceberg's `deletion-vector-v1` uses (8-byte count, then per key a 4-byte key and a 32-bit portable
  bitmap). Iceberg's blob framing is: 4-byte **big-endian** length of (magic + vector), the magic bytes
  `D1 D3 39 64`, the vector, then a **big-endian CRC-32** of (magic + vector). That is structurally
  identical to Delta's on-disk DV framing (`DeletionVectorWriter.cs:91-119`: 4-byte BE size, blob,
  BE CRC-32 via `System.IO.Hashing.Crc32`), except that Delta has a leading version byte and uses a
  different magic. So the work is roughly to parameterize the magic in `DeletionVectorBuilder.Serialize`
  and `RoaringBitmapReader.Deserialize` (or split off a magic-less core), then add a Puffin framing. The
  `DeletionVectorBitmap` view (Contains, ranges, `ClearDeleted`, Union, Except, and `IExcludedRows`) is
  reusable as-is. Caveats: the classes are `internal` and live in the Delta assembly (move them to Core);
  check the Iceberg spec's run-container and key-ordering rules against Java `RoaringPositionBitmap`
  output with a cross-engine fixture.
- Interop: Spark and delta-rs DV tests (`test/EngineeredWood.DeltaLake.Table.Tests/Interop/*DeletionVector*`).

### Row tracking ≈ Iceberg row lineage
- `DeltaLake/RowTracking/RowTrackingConfig.cs`, `DeltaLake.Table/RowTracking/RowTrackingWriter.cs:18`
  (`AddRowIdAndCommitVersionColumns :63`, `StripMaterializedColumns :95`). The model (`baseRowId + pos`,
  default commit version, and a non-null materialized value that overrides it) matches Iceberg v3
  `first_row_id` / `_last_updated_sequence_number`.
- `DeltaLake/Schema/IcebergCompat.cs:67,73`: `RowIdFieldId = 2147483540`, `RowCommitVersionFieldId =
  2147483539`. These are **Iceberg's reserved ids** for `_row_id` and `_last_updated_sequence_number`, and
  IcebergCompatV3 already stamps them on the materialized columns (`RowTrackingFieldIds :170`,
  `RollingDataFileWriter.WithColumnFieldIds`, `DeltaTable.cs:11536-11558`).
  Interop: `IcebergCompatV3InteropTests.cs`.

### Column mapping by id, defaults, variant
- `DeltaLake/Schema/ColumnMapping.cs` (id/name modes, `GetFieldId`, `BuildLogicalToFieldIdMap`, `AssignNestedIds`),
  `ColumnMappingRecursive.cs:35-257` (`ToPhysical`, `ToLogical`, `DropStaleFields`, `StripParquetFieldIds`). The
  read by id is at `DeltaTable.cs:13529-13548`. It is name-keyed physical-to-logical, not a full Iceberg
  projection by id, but it shows where to hook in.
- Defaults: `ColumnDefaultLiteral.cs` (Spark `CURRENT_DEFAULT` literal rules), `DeltaLiteralDecoder.cs`. Iceberg's
  `initial-default` / `write-default` are JSON single-values, so the semantics differ. The schema-evolution
  backfill in the Delta reader is the hook for `initial-default`.
- Variant: `VariantShreddingConfig.cs`, `VariantStatsEncoding.cs`, `DeltaLake.Table/VariantColumnCoercion.cs`.

### IcebergCompat / UniForm
- `IcebergCompat.cs:43` validates IcebergCompatV1/V2/V3 constraints (`Validate :116`, partition
  materialization, numRecords, nested ids). `UniversalFormat.cs:16` only parses
  `delta.universalFormat.enabledFormats` and states "This library has no converter." **No Iceberg metadata is
  generated anywhere** (this agrees with #490).

### Table-layer architecture (to mirror)
- **Log layer** (`DeltaLake`): `Log/TransactionLog`, `LogCommitter` (OCC loop: attempt, read the winners,
  `ConflictChecker`, then rebase or abort), `Concurrency/ConflictChecker.cs:196,218` (`ConflictType`:
  MetadataChanged, ProtocolChanged, ConcurrentDeleteRead, ConcurrentDeleteDelete, ConcurrentAppend,
  DomainMetadataChanged; plus a `ReadSet`), `Snapshot/SnapshotBuilder`, `DeltaFilePruner` (stats pruning
  via `StatisticsEvaluator`), checkpoints and CRC.
- **Table layer** (`DeltaLake.Table`, `DeltaTable.cs` alone is 15k lines): `PlanFiles` -> `PlannedFile(FileOrdinal,
  AddFile)` (`PlannedFile.cs:30`); reads go through ParquetFileReader (or `IDataFileReader`), then column
  mapping, DV exclusion (`IExcludedRows`), schema-evolution backfill, partition re-add, type widening, and
  row-tracking/metadata columns. Writes: partition split, then `RollingDataFileWriter`, stats, `add`
  actions, then `DeltaTransaction` (`DeltaTransaction.cs`: Stage*, Declare*Read for the conflict read set,
  `CommitAsync`). DML: `DeleteRowsAsync` / `UpdateRowsAsync` with `RowDeleteMode.DeletionVector |
  CopyOnWrite` (`RowDeleteMode.cs:10`), MERGE (`DeltaTable.Merge.cs`), row-addressed DML via
  `RowSelection` (`RowSelection.cs:26`; `ByPath`, `FromRowAddresses`) and `TransientRowAddress`
  (file ordinal << bits | pos), row-level DV rebase (`RebaseDvDmlActionsAsync`, `DeltaTable.cs:12522`),
  compaction and vacuum.
- **Codec seam**: `IDataFileReader.cs:31` (`ReadAsync(relativePath, physicalColumns, ct)`: raw batches in
  file order, DV rows included) and `IDataFileWriter.cs:27` (`WriteAsync(IAsyncEnumerable<RecordBatch>,
  relativePath, ct) -> size`). Both are `[Experimental("EWDELTA0001")]` and have no in-tree implementation. The
  seam hides the footer, so no row-group pruning and no footer stats. Its purpose is to swap in DuckDB's
  Parquet codec. An Iceberg layer could take the same shape but must get stats back from the writer
  (manifests need them), so the writer contract would have to return the footer or the per-field stats.

---

## 3. Avro (src/EngineeredWood.Avro)

- **Public API:** `AvroReaderBuilder` (`WithReaderSchema :39`, `WithProjection`, `WithSkipFields`, `Build`/`BuildAsync`) ->
  Arrow `RecordBatch`; `AvroWriterBuilder(Arrow schema)` (`WithAvroSchema :30`, codecs Null, Deflate, Snappy, Zstandard).
  Internal and Iceberg-visible (`InternalsVisibleTo EngineeredWood.Iceberg`, csproj:37): `OcfReader`
  (`WriterSchema`, `Metadata`, `ReadBlock`), `OcfWriter`, `AvroBinaryReader` / `AvroBinaryWriter`.
- **Schema resolution:** `Schema/SchemaResolver.cs:17` handles record fields **by name and alias**: writer-only
  fields are skipped, reader-only fields take a default (it throws if there is none), and it supports
  int->long->float->double and string<->bytes promotion, nested records, array items, map values, and enum
  defaults. This is enough to read Java-written manifests with **extra fields** through the
  RecordBatch path. Matching by **field-id is not supported** (names only). Iceberg manifest field names
  are fixed by the spec, so names mostly suffice, but the partition struct (`r102`) is per-spec and must be
  built from the PartitionSpec.
- **Gaps:**
  1. **Field-id attributes are dropped.** `AvroSchemaParser` keeps only `logicalType` and decimal
     attributes (`AvroSchemaParser.cs:90-251`); `AvroSchemaNode` has no property bag. `OcfWriter.WriteHeader`
     (`Container/OcfWriter.cs:47-60`) re-serializes through `AvroSchemaWriter.ToJson`. The embedded
     `ManifestAvroSchemas.cs` JSON has `field-id`s, but the written `avro.schema` loses them, and
     element-id/key-id/value-id would be lost too. Iceberg Java resolves manifest columns by field id.
  2. **No custom OCF header metadata.** `WriteHeader` writes only `avro.schema` / `avro.codec`. Iceberg
     manifests need `schema`, `partition-spec`, `partition-spec-id`, `format-version` and `content` (and the
     manifest list needs `snapshot-id`, `parent-snapshot-id`, `sequence-number`, `first-row-id`). Reading
     them is possible via `OcfReader.Metadata`.
  3. The Iceberg codec bypasses the resolver: `Iceberg/Manifest/ManifestIO.cs:223-239` decodes positionally
     and **refuses any writer schema that is not EW's own** (#586), so Java, Spark and pyiceberg manifests,
     partitioned manifests and V1 manifests are all refused. The data file is written with an empty partition
     record and without bounds, nan counts, key_metadata, equality_ids or v3 fields beyond `FirstRowId`,
     `ReferencedDataFile` and `ContentOffset` (`ManifestIO.cs:239-260`). The manifest list is always written
     uncompressed (`AvroCodec.Null`).
  4. The array-with-`logicalType: map` encoding (Iceberg's int-keyed maps) parses as an array of records,
     which is fine for decoding but has no Map semantics.
- Unions: nullable `[null, T]` and general unions (dense union) are both supported. Logical types: date,
  time and timestamp millis/micros/nanos, local timestamps, decimal (bytes/fixed), uuid, and duration.
- Tests: cross-validated against fastavro in both directions (`test/EngineeredWood.Avro.Tests/CrossValidationTests.cs`),
  and there are recursive, default-value, union and projection tests. There is no Iceberg-Java manifest fixture.

---

## 4. Expressions (src/EngineeredWood.Expressions[.Arrow])

- Used by Iceberg: the factory helpers (`Iceberg/Expressions/Expressions.cs`), `ExpressionBinder`,
  `BoundReference(int FieldId, string Name)` (`Expression.cs:83`), `StatisticsEvaluator.Evaluate<TStats>`
  (`StatisticsEvaluator.cs:27`), and `IStatisticsAccessor<T>` (`IStatisticsAccessor.cs`; min, max, null count,
  value count by **column path**, with `INanCountAccessor` alongside).
- Binding by field id: `ExpressionBinder(Func<string,int?>)` (`ExpressionBinder.cs:37`) resolves **one-part
  (top-level) names only**, and nested refs stay unbound. Iceberg's `TableScan.BindFilter` and
  `IcebergStatisticsAccessor` likewise handle top-level fields only (#771), and the evaluator is still
  keyed by name, with the field id carried along.
- **Partition transforms: missing.** `Iceberg/Transform.cs` is data-only records (Identity, Bucket(n),
  Truncate(w), Year, Month, Day, Hour, Void). There is no transform application, no inclusive or strict
  projection of predicates through transforms, no partition-summary (`partitions` field-summary) pruning of
  manifests, and no residual evaluator. `TableScan` prunes on file stats only and includes every delete file.
- Literals (`LiteralValue.cs`): bool, int and uint 32/64, float, double, Half, decimal and high-precision
  (BigInteger, scale), string, binary, Guid, DateOnly, TimeOnly, and DateTimeOffset (ticks). **No ns
  timestamp literal** (100 ns ticks), and there is no Iceberg single-value binary codec anywhere.
- Row-level evaluation: `Expressions.Arrow/ArrowRowEvaluator*.cs` and `IRowEvaluator` (Spark-dialect
  semantics, with a differential fuzzer against Spark). This can evaluate residuals. Iceberg's
  null/NaN semantics are close to SQL, but check them.

---

## 5. Storage (src/EngineeredWood.Core/IO + Aws/Azure/Gcs)

- `ITableFileSystem` (`Core/IO/ITableFileSystem.cs:11`): List, OpenRead (`IRandomAccessFile`), Create
  (`ISequentialFile`, overwrite flag), Delete, Exists, ReadAllBytes, WriteAllBytes, and
  **`TryWriteAllBytesAsync` = atomic create-if-absent** (`:80-99`, documented as load-bearing for
  Delta and Iceberg commits). `TableFileInfo` carries an `ETag`.
- Backends: Local (`IO/Local/LocalTableFileSystem.cs`), S3 (`If-None-Match: *` PutObject,
  `Aws/S3TableFileSystem.cs:241-248`; endpoint must honor conditional writes), Azure Blob
  (`IfNoneMatch = ETag.All`, `Azure/AzureTableFileSystem.cs:205`; ADLS works via blob endpoint), GCS
  (`IfGenerationMatch = 0`, `Gcs/GcsTableFileSystem.cs:243`). Cloud emulator oracles exist.
- What catalogs can use: create-if-absent is enough for a file-system/Hadoop-style catalog that commits
  `vN.metadata.json` exclusively. **Gaps:** there is no compare-and-swap/`If-Match` overwrite (which a
  pointer-file catalog or `version-hint.text` would need) and no REST catalog client. **Bug:**
  `Iceberg/FileSystemCatalog.cs:310,315` writes `vN.metadata.json` and the version hint with **unconditional**
  `WriteAllBytesAsync`, so concurrent committers can clobber each other. It should use `TryWriteAllBytesAsync`.

---

## 6. Puffin and murmur3

- **Puffin: none.** No source or test file mentions it. The DV pieces above (roaring, CRC-32 via
  `System.IO.Hashing`, and the BE framing) cover the `deletion-vector-v1` blob. Still missing: the
  Puffin file container (magic `PFA1`, JSON footer with blob metadata, optional LZ4/zstd footer
  compression, flags), `apache-datasketches-theta-v1` (NDV), and the manifest `content_offset` and
  `content_size_in_bytes` plumbing.
- **murmur3_x86_32: none.** The only murmur is `src/EngineeredWood.Orc/BloomFilter/Murmur3.cs:28` (internal), which
  implements **x64 variants only** (`Hash64Cpp`, `Hash64Java` = murmur3_x64_128 h1). The Iceberg bucket
  transform needs a new murmur3_x86_32 (seed 0) with the spec's per-type input rules: int and long hashed as
  an 8-byte LE long, date as int->long, time and timestamp as micros long, decimal as minimal big-endian
  unscaled bytes, string as UTF-8, uuid as 16 big-endian bytes, fixed and binary as raw bytes. For v3
  timestamp_ns, the spec hashes after converting to micros (verify against the spec appendix). Test it
  against the spec's appendix B vectors.

---

## Biggest reuse wins

1. The Parquet reader and writer, with field ids, variant shredding, ns timestamps, UUID and decimal,
   `IExcludedRows`/`ReadSelectedAsync`, positions, and Expressions-driven pruning.
2. The Delta DV roaring code: same 64-bit portable layout and same CRC/BE framing; only the magic and the
   wrapper differ.
3. Delta row tracking, which uses the same lineage model and already stamps Iceberg's reserved field ids.
4. Footer stats, which are already almost in Iceberg's single-value binary form.
5. `TryWriteAllBytesAsync` on all four backends.
6. The Delta table layering as a template: log/OCC, then table/plan/read/write/DML, then the codec seam.

## Biggest gaps

Geometry and geography with geo stats; nested id projection; an Iceberg stats builder plus a single-value
codec plus truncation; a generic rolling writer; Avro field-id attribute preservation and custom OCF
metadata; general (resolving) manifest decoding and partition structs; transforms (apply, inclusive and
strict projection, murmur3_x86_32); Puffin; equality deletes; a ns literal; CAS and a REST catalog; and
fixing `FileSystemCatalog`'s non-atomic commit.
