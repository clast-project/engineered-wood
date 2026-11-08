# Architecture

EngineeredWood is a .NET monorepo (`net10.0`, `net8.0`, `netstandard2.0`) for reading and writing columnar
file formats — Apache Parquet, Apache ORC, Apache Avro, Lance, and Vortex — and table formats — Delta Lake,
Apache Iceberg, and Lance datasets — as Apache Arrow `RecordBatch` objects. It is built around cloud storage
access patterns (batched range reads, concurrent column chunk I/O, pooled buffers) rather than a
single-cursor `Stream`.

This document is a guide to the source. For usage and build instructions, see `README.md`.

> **Coverage note.** Lance has no section below. Its coverage is in [`doc/lance.md`](doc/lance.md), its gaps
> in [`doc/lance-future-work.md`](doc/lance-future-work.md), and its original design in
> [`doc/lance-reader-design.md`](doc/lance-reader-design.md) (historical). Vortex coverage tables are in
> [`doc/vortex.md`](doc/vortex.md).

## Project layout

```
src/
  EngineeredWood.Core/                  Shared abstractions
    IO/                                 Random-access, sequential and table-filesystem I/O; coalescing;
                                          background uploads
      Local/                            Local file backends
    Compression/                        Codec dispatch (compress + decompress)
    Arrow/                              ArrowCompute (take/concatenate), SelectionMask, bitmaps,
                                          NativeBuffer<T> (NativeAllocator.cs)
    Encodings/                          ByteStreamSplitDecoder, RoaringBitmap, Varint
    Memory/                             MemoryBudget
    Scans/                              OrderedScan, ScanMemoryBudget (parallel ordered scans)
  EngineeredWood.Expressions/           Format-agnostic expression library
    LiteralValue, Expression, Predicate, ExpressionBinder, StatisticsEvaluator, variant paths/bounds
    Sql/                                Spark SQL tokenizer, parser and renderer
  EngineeredWood.Expressions.Arrow/     Row-level evaluator over RecordBatch
    ArrowRowEvaluator, IRowEvaluator, IFunctionRegistry
    Spark/                              SparkFunctionRegistry and the Spark function implementations
  EngineeredWood.Parquet/Parquet/       Parquet format implementation
    Thrift/                             Thrift Compact Protocol codec
    Metadata/                           Footer, ColumnIndex/OffsetIndex model and (de)serialization
    Schema/                             Schema tree with definition/repetition levels
    Data/                               Column encoding/decoding, Arrow conversion, page index collection
    BloomFilter/                        Bloom filter read/write
    ParquetFileReader.cs, ParquetFileWriter.cs, BufferedParquetWriter.cs, ParquetFileAssembler.cs
    ParquetStatisticsAccessor.cs        Adapter for the shared StatisticsEvaluator
    MembershipPredicateEvaluator.cs     Dictionary- and Bloom-based predicate probing
    PageIndexPruner.cs                  Row ranges from the ColumnIndex
    VariantShredding.cs                 Shred / unshred VARIANT columns
  EngineeredWood.Orc/                   ORC format implementation
    Proto/                              Protobuf schema (orc_proto.proto)
    Encodings/                          RLE v1/v2, byte RLE, bit reader/writer
    ColumnReaders/, ColumnWriters/      Per-type column readers and writers
    BloomFilter/                        ORC Bloom filters
  EngineeredWood.Avro/                  Avro format implementation
    Schema/                             Schema parsing, resolution, fingerprinting
    Container/                          OCF read/write (sync + async)
    Data/                               RecordBatch assembly/encoding
    Encoding/                           AvroBinaryReader/Writer (ref struct)
  EngineeredWood.Lance/                 Lance file reader and writer (v2.0, v2.1, v2.2)
    Proto/, Format/, Schema/, Encodings/V20/, Encodings/V21/, LanceFileWriter.cs
  EngineeredWood.Lance.Table/           Lance dataset API (manifests, fragments, deletions,
                                          secondary indices, versioned commits)
  EngineeredWood.Vortex/                Vortex file reader and writer
    FlatBuffers/                        Hand-rolled FlatBuffer reader and builder
    Format/                             Typed accessors over the footer FlatBuffers
    Schema/                             DType → Arrow schema
    Layouts/                            Layout tree, LayoutPlanner, dict + zone-map layouts
    Encodings/, Writer/Encodings/       One decoder / encoder per array encoding
    Writer/                             VortexFileWriter and serializers
  EngineeredWood.DeltaLake/             Delta transaction log (low-level)
    Actions/                            AddFile, RemoveFile, MetadataAction, ProtocolAction, etc.
    Log/                                NDJSON commits, LogCommitter (OCC loop), log compaction and cleanup,
                                          in-commit timestamps, version checksums (.crc)
    Concurrency/                        ConflictChecker + ReadSet — the pure OCC verdict function
    Checkpoint/                         V1 (Parquet) and V2 (JSON + sidecar) checkpoint reader/writer
    Snapshot/                           Snapshot reconstruction
    Schema/                             Delta schema model, ColumnMapping, TypeWidening, IcebergCompat
    DeletionVectors/                    RoaringBitmap reader/writer, Base85 codec, DV filter
    RowTracking/, ChangeDataFeed/       Feature configuration
    DeltaFilePruner.cs                  Partition + stats predicate pushdown
    IsolationLevel.cs, IsolationLevelProperty.cs
  EngineeredWood.DeltaLake.Table/       Delta table API (high-level Arrow I/O)
    DeltaTable.cs (+ .Merge, .Restore, .ReplaceWhere, .MergeSchema, .Constraints)
    DeltaTransaction.cs                 Staged multi-operation transactions
    DeltaConstraintEnforcer.cs, DeltaGeneratedColumns.cs, DeltaNotNullEnforcer.cs
    RollingDataFileWriter.cs            Partition split + target-size file rolling
    Partitioning/, Compaction/, Vacuum/, ChangeDataFeed/, IdentityColumns/, RowTracking/, Stats/, TypeWidening/
  EngineeredWood.Iceberg/               Iceberg metadata + scan planning
    Manifest/                           Manifest file/list types and Avro-encoded I/O
    Expressions/                        Iceberg-flavored Expressions factory + TableScan
    Serialization/                      JSON serialization for table metadata
    Catalog interfaces and implementations (FileSystem, InMemory)
  EngineeredWood.Azure/                 Azure Blob backends (namespace EngineeredWood.IO.Azure)
  EngineeredWood.Gcs/                   Google Cloud Storage backends (namespace EngineeredWood.IO.Gcs)
  EngineeredWood.Aws/                   Amazon S3 backends (namespace EngineeredWood.IO.Aws)
  EngineeredWood.Parquet.TestTool/      Not packaged; encoding-settings analysis tool
test/
  EngineeredWood.<Project>.Tests/       xUnit tests, one project per library (plus Core.Tests)
  EngineeredWood.<Format>.Benchmarks/   BenchmarkDotNet suites (Parquet, ORC, Avro, Lance, DeltaLake)
  EngineeredWood.DeltaLake.DeltaDotnetBenchmarks/   Comparison against delta-dotnet
  EngineeredWood.Parquet.Compatibility/ Cross-tool validation over a downloaded file corpus
  EngineeredWood.Parquet.Bridge/, .SparkBridge/     Hooks for the external Parquity test matrix
  Shared/                               InteropDriver shared by the interop tiers
parquet-testing/                        Git submodule with Parquet sample files
```

### Project dependencies

Every library depends on `EngineeredWood.Core` except `EngineeredWood.Expressions`, which has no Arrow or
format dependencies.

```
EngineeredWood.Expressions
  ├── EngineeredWood.Expressions.Arrow                (+ Apache.Arrow)
  ├── EngineeredWood.Parquet
  │     └── EngineeredWood.DeltaLake                  (checkpoints are Parquet)
  │           └── EngineeredWood.DeltaLake.Table      (+ Expressions.Arrow)
  ├── EngineeredWood.Iceberg                          (+ Avro)
  ├── EngineeredWood.Vortex
  └── EngineeredWood.Lance.Table                      (+ Lance, Expressions.Arrow)
EngineeredWood.Orc, .Avro, .Lance, .Azure, .Gcs, .Aws  (Core only)
```

## Conventions

- **Async model.** `ValueTask<T>` everywhere, `.ConfigureAwait(false)` on every await.
- **Buffer ownership.** I/O methods return `IMemoryOwner<byte>`; the caller disposes.
- **Ref structs.** Hot-path decoders (`ThriftCompactReader`, `RleBitPackedDecoder`, `DeltaBinaryPackedDecoder`) are `ref struct` over `ReadOnlySpan<byte>`: zero allocation, but not usable across awaits or in lambdas.
- **Nullable and implicit usings enabled.**
- **`checked()` casts** for `long → int` conversions.
- **AOT gate.** The `net10.0` build of every `src/` project sets `IsAotCompatible` with warnings as errors (`src/Directory.Build.props`), so a trim/AOT incompatibility fails the build.

---

## Core library (`EngineeredWood.Core`)

### I/O layer (`EngineeredWood.IO`)

Replaces `Stream` with interfaces designed for columnar access.

**Reading: `IRandomAccessFile`** — no shared position cursor, so concurrent reads of disjoint ranges are safe.

| Type | Notes |
|---|---|
| `LocalRandomAccessFile` | `RandomAccess` API |
| `AzureBlobRandomAccessFile` | HTTP range requests; `SemaphoreSlim` throttle (default 16); multi-range reads go through `CoalescingFileReader` |
| `GcsRandomAccessFile` | Ranged `DownloadObjectAsync`; same throttle + coalescing |
| `S3RandomAccessFile` | `GetObject` + `ByteRange`; same throttle + coalescing |
| `CoalescingFileReader` | Merges nearby ranges (`CoalescingOptions`: gap ≤ 1 MiB, request ≤ 64 MiB), then slices results back to the original order |

The cloud files implement `IReadConcurrencyHint`, so readers size their parallelism to the store rather than
the core count.

**Writing: `ISequentialFile`** — append-only `WriteAsync` + `FlushAsync` + `Position`, and `CompleteAsync`,
which publishes the file. Nothing appears at the file's name until `CompleteAsync`; disposing without it aborts,
so a failed write leaves no truncated file. Format writers call it when they write their footer. The cloud
writers stream with bounded memory through each backend's chunked upload: Azure block blobs, GCS resumable
uploads (fed through a bounded channel), and S3 multipart uploads. Azure and S3 upload blocks/parts in the
background from a shared pool of spare buffers (`BackgroundUploads.cs`), overlapping uploads with encoding.

**Directory / table operations: `ITableFileSystem`** — list / open-read / create / delete / exists / read-all /
write-all / try-write-all over a root, for the table formats. `LocalTableFileSystem`, `AzureTableFileSystem`,
`GcsTableFileSystem` and `S3TableFileSystem` implement it. `TryWriteAllBytesAsync` is the commit primitive: it
publishes a file only if the path does not exist. Object stores do this in one conditional request
(`IfGenerationMatch = 0` on GCS, `If-None-Match: *` on Azure and S3); the local filesystem writes a temp file
and `File.Move`s it without overwrite. A lost race returns `false`, which `TransactionLog.WriteCommitAsync` turns
into `DeltaConflictException`.

**Buffer management** — `BufferAllocator` is an abstract factory for `IMemoryOwner<byte>`;
`PooledBufferAllocator` rents from `ArrayPool<byte>.Shared` and slices to exact size.

### Memory budget and parallel scans

`MemoryBudget` (`Memory/`) caps the Arrow buffer bytes its consumers may hold; budgets form a tree, and a child
charges itself and every ancestor. Pass one on `ParquetReadOptions` or `DeltaTableOptions` and every parallel
scan through those options draws from it, each through a child capped at the options' `MaxBufferedReadBytes`
(default 256 MiB). Today only parallel scans are counted (decoded batches not yet handed out, plus compressed
chunks in flight); writers and serial reads are not.

`OrderedScan` (`Scans/`) runs several units (files of a table scan, row groups of a Parquet file) at once and
yields their batches in serial order; `ScanMemoryBudget` reserves budget before each decode.

### Compression (`EngineeredWood.Compression`)

`Compressor` and `Decompressor` dispatch on the shared `CompressionCodec` enum:

| Codec | Library | Notes |
|---|---|---|
| Uncompressed | — | Direct copy |
| Snappy | Snappier | Pure managed |
| Gzip, Deflate | System.IO.Compression | Deflate is raw RFC 1951 (ORC's "Zlib") |
| Brotli | System.IO.Compression; BrotliSharpLib on netstandard2.0 | |
| Lz4 | K4os.Compression.LZ4 | Block codec |
| Lz4Hadoop | K4os.Compression.LZ4 | Hadoop framing, with frame and raw fallback |
| Zstd | ZstdSharp.Port | `[ThreadStatic]` compressor/decompressor reuse |
| Lzo | — | In the enum; not implemented |

Levels are codec-agnostic (`BlockCompressionLevel`) with an optional native override. Each format maps its wire
values to the enum (e.g. `MetadataDecoder.ParquetCodecFromThrift()`, ORC's `CompressionKind`).

### Arrow helpers (`EngineeredWood.Arrow`)

`ArrowCompute` gathers rows (`take`) and concatenates arrays by copying raw value slots, because Arrow's typed
builders round-trip through lossy .NET types (`DateTimeOffset`, `decimal`). `SelectionMask` is the per-batch row
selection where deletion vectors and predicate masks meet before one gather. `NativeBuffer<T>` is a growable
native-memory buffer handed to Arrow without copying.

---

## Parquet (`EngineeredWood.Parquet`)

### Thrift codec (`Parquet.Thrift`)

The footer and page headers use the Thrift Compact Protocol; EngineeredWood has its own codec rather than a Thrift
dependency. `ThriftCompactReader` is a `ref struct` over a span (varints, zigzag ints, delta field IDs, nested
structs on an inline 8-slot stack); `ThriftCompactWriter` mirrors it.

### Metadata and schema

`Parquet.Metadata` holds immutable records mirroring the Thrift spec (`FileMetaData`, `RowGroup`, `ColumnChunk` →
`ColumnMetaData`, `SchemaElement`, `LogicalType`, `ColumnIndex`, `OffsetIndex`); `MetadataDecoder`/`MetadataEncoder`
convert to and from Thrift bytes. `SchemaDescriptor` turns the flat `SchemaElement` list into a `SchemaNode` tree
and leaf `ColumnDescriptor`s with definition and repetition levels.

### Read pipeline

```
ParquetFileReader.ReadAllAsync() / ReadWithPositionsAsync()
  ├─ Plan row groups in order: statistics → dictionaries / Bloom filters → page index (row ranges)
  ├─ OrderedScan over kept row groups (MaxConcurrentRowGroups; serial when 1)
  │    ├─ ReadRangesAsync                   Batched I/O (coalescing applied)
  │    ├─ Parallel.For → ColumnChunkReader  per column
  │    │    ├─ PageReader / PageHeaderDecoder   Thrift → PageHeader (V1/V2/Dict)
  │    │    ├─ Decompressor
  │    │    ├─ LevelDecoder                     Def/rep levels
  │    │    ├─ Value decoder                    see table
  │    │    └─ ArrowArrayBuilder                Typed Arrow array, nulls from def levels
  │    └─ AssembleRecordBatch → NestedAssembler (nested columns → Struct/List/Map)
  └─ yield batches in file order
```

Single-row-group entry points are `ReadRowGroupAsync`, `ReadRowGroupBatchesAsync` and `ReadRowRangesAsync`.
`ReadWithPositionsAsync` pairs each batch with its first row's file position, for layers keyed by position
(deletion vectors, row ids), and can skip spans a `RowSkipPredicate` says are unneeded.

**Encodings:**

| Encoding | Decoder | Notes |
|---|---|---|
| PLAIN | `PlainDecoder` | Boolean bit-packed (LSB), fixed-width via `MemoryMarshal.Cast`, BYTE_ARRAY with 4-byte length prefix |
| PLAIN_DICTIONARY / RLE_DICTIONARY | `DictionaryDecoder` + `RleBitPackedDecoder` | Dictionary as typed arrays; indices RLE/bit-packed |
| RLE | `RleBitPackedDecoder` | Def/rep levels and booleans |
| BIT_PACKED (deprecated) | `RleBitPackedDecoder` | MSB-first packing |
| DELTA_BINARY_PACKED | `DeltaBinaryPackedDecoder` | 128-value blocks, 4 miniblocks |
| DELTA_LENGTH_BYTE_ARRAY | `DeltaLengthByteArrayDecoder` | Delta-encoded lengths + raw data |
| DELTA_BYTE_ARRAY | `DeltaByteArrayDecoder` | Prefix lengths + suffixes |
| BYTE_STREAM_SPLIT | `ByteStreamSplitDecoder` (Core) | AVX2 with scalar fallback |
| ALP (experimental, 10) | `AlpDecoder` | Unratified proposal; per 1024-value vector |
| PFOR (experimental, 11) | `PforDecoder` | Unratified proposal; FOR + bit-packing with patched outliers, optional per-vector delta |
| FSST (experimental, 12) | `FsstPageDecoder` + `FsstSymbolTable` | Unratified proposal; symbol table in a chunk-level `SYMBOL_TABLE_PAGE` |

**Arrow array construction** — for nullable flat columns `ArrowArrayBuilder` scatters non-null values
right-to-left in place to open gaps for nulls, avoiding a temporary buffer. Validity bitmaps use SIMD
(`BitmapHelper`). `NestedAssembler` rebuilds `StructArray`, `ListArray` and `MapArray` from leaves and their
levels.

### Predicate pushdown

`ParquetReadOptions.Filter` takes a shared `EngineeredWood.Expressions.Predicate`. Row groups are pruned in
three passes, each only over what the previous one left `Unknown`:

- **Statistics.** `ParquetStatisticsAccessor` implements `IStatisticsAccessor<RowGroup>` (plus NaN-count and
  variant accessors), decoding min/max per (physical, logical) type. It prefers `min_value`/`max_value` over the
  legacy `min`/`max` (used only for signed numerics); INT96 has no defined order and yields null.
- **Membership.** `MembershipPredicateEvaluator` probes `Equal`/`In` sub-predicates against the chunk's
  dictionary page (exact, when every data page is dictionary-encoded; `FilterUseDictionaries`, default on) or
  Bloom filter (`FilterUseBloomFilters`, default off). `MembershipPrefetch` reads these for a window of row groups
  per request, because one request per row group made pruning a net loss on object storage.
- **Page index.** `PageIndexPruner` cuts a kept row group into intervals from the referenced columns'
  `ColumnIndex` page bounds, judges each with the same `StatisticsEvaluator`, and returns row ranges
  (`FilterUsePageIndex`, default on). Design: [`doc/parquet-page-index.md`](doc/parquet-page-index.md).

Pruning returns a superset; rows are not filtered. `variant_get` predicates prune shredded VARIANT columns
through the `typed_value` statistics.

### Write pipeline

```
ParquetFileWriter.WriteRowGroupAsync(RecordBatch)
  ├─ Auto-split if batch > RowGroupMaxRows
  ├─ Schema from first batch (ArrowToSchemaConverter); later batches checked by RowGroupSchemaCheck
  ├─ Parallel.For → ColumnChunkWriter.WriteColumn()   per leaf column
  │    ├─ NestedLevelWriter.Decompose()     (nested) Arrow → leaves + def/rep levels
  │    ├─ DictionaryEncoder.TryEncode()     Dictionary path: PLAIN dict page + RLE index pages
  │    ├─ otherwise                          Value encoding from EncodingStrategyResolver
  │    ├─ Compressor
  │    ├─ StatisticsCollector               Min/max/null_count (O(unique) with a dictionary)
  │    └─ PageIndexCollector                ColumnIndex + OffsetIndex per page
  └─ CloseAsync → ParquetFileAssembler: chunks, Bloom filters, page index, footer, PAR1 → CompleteAsync
```

`BufferedParquetWriter` accumulates several batches into one row group, buffering dictionary indices and levels
rather than full value buffers. Both writers lay the file out through `ParquetFileAssembler`, so layout features
(page index, encryption) are added once.

**Encoding strategy** — `EncodingStrategyResolver` picks the non-dictionary encoding. V1 and V2 pages use the same
value encodings; only level framing and the compression boundary differ. Data page V2 is the default.

| Physical type | Default | Opt-in |
|---|---|---|
| Boolean | RLE | — |
| Int32, Int64 | DELTA_BINARY_PACKED | PLAIN, PFOR |
| Float, Double | PLAIN | BYTE_STREAM_SPLIT, ALP |
| ByteArray | DELTA_LENGTH_BYTE_ARRAY | PLAIN, DELTA_BYTE_ARRAY, FSST |
| FixedLenByteArray | PLAIN | DELTA_BYTE_ARRAY |

Dictionary encoding is tried first (unless disabled, or Boolean) and abandoned above 20% distinct non-null values.
ByteArray/FLBA deduplication uses an open-addressing FNV-1a hash table to avoid collision-chain garbage.

**FSST** compresses the whole chunk up front, because its symbol table is shared by the chunk: `FsstCompressedColumn.TryCompress` trains and compresses once, data pages are slices of that payload, and a null
result (FSST did not help) reverts to `DELTA_LENGTH_BYTE_ARRAY`. Codes are renumbered in ascending-length order
because the wire format stores a length histogram, not per-symbol lengths. The symbol table page is written first
and located by `ColumnMetaData.symbol_table_page_offset`. Details: [`doc/parquet-fsst.md`](doc/parquet-fsst.md).

**Write options** — `ParquetWriteOptions` covers compression (per-column overrides), page version and size,
dictionary limits, row group limits, encodings, statistics, page index, Bloom filters, variant shredding,
key-value metadata and `created_by`.

---

## Avro (`EngineeredWood.Avro`)

Reads and writes Avro Object Container Files (OCF) and framed messages as Arrow `RecordBatch`es.

### Schema

`AvroSchemaNode` trees cover all Avro types and the logical types date, time-millis/micros,
timestamp-millis/micros/nanos (UTC and local), decimal (bytes and fixed), and uuid.

- **`AvroSchemaParser` / `AvroSchemaWriter`** — JSON ↔ schema tree, with named and self-referencing types.
- **`ArrowSchemaConverter`** — Avro ↔ Arrow: `Decimal128Type`, `DictionaryType` for enums, nullable unions as
  nullable fields, other unions as `DenseUnion`.
- **`SchemaResolver`** — Avro schema evolution: match by name/alias, type promotion, defaults for missing reader
  fields, skipping writer-only fields.
- **`ParsingCanonicalForm`**, **`RabinFingerprint`** — PCF and CRC-64-AVRO fingerprints.

### Read and write pipelines

```
AvroReaderBuilder.Build / BuildAsync(Stream) → AvroReader / AvroAsyncReader
  └─ OcfReader blocks → RecordBatchAssembler.DecodeBlock()
       ├─ AvroBinaryReader (ref struct)
       ├─ Per-type builders; NullableBuilder for ["null", T]
       └─ PromotingBuilders for schema evolution

AvroWriterBuilder.Build / BuildAsync(Stream) → AvroWriter / AvroAsyncWriter
  └─ OcfWriter blocks ← RecordBatchEncoder.Encode() (AvroBinaryWriter, per-type dispatch)
```

Projection (`WithProjection(int[])`, `WithSkipFields(string[])`) builds a projected reader schema, so schema
resolution skips the unwanted fields.

### Streaming encode/decode

Framed (non-OCF) messages support Single Object Encoding (`C3 01` + 8-byte LE Rabin fingerprint), Confluent
(`00` + 4-byte BE schema ID), Apicurio (`00` + 8-byte BE global ID), and raw datums. `AvroEncoder` produces
`EncodedRows`; `AvroDecoder` is push-based, resolves schemas through a `SchemaStore`, and flushes on batch limit or
schema switch.

### Compression

Null, Deflate, Snappy (+ 4-byte BE CRC32C), Zstandard, and LZ4 (4-byte LE size prefix), through Core's codecs.

---

## ORC (`EngineeredWood.Orc`)

Metadata (postscript, footer, stripe information) is Protobuf, compiled from `Proto/orc_proto.proto` by Grpc.Tools
at build time. `OrcSchema` builds the type tree and maps it to Arrow (`ToArrowType()`).

### Read pipeline

```
OrcReader (postscript/footer/schema)
  └─ CreateRowReader() → OrcRowReader (IAsyncEnumerable<RecordBatch>)
      └─ ReadStripeSelectiveAsync / ReadStripeFullAsync
          └─ CreateColumnReaders → ColumnReader.ReadBatch(size) → IArrowArray
```

Column data lives in named streams (PRESENT, DATA, SECONDARY, LENGTH, DICTIONARY_DATA) decoded by RLE v1/v2;
PRESENT is a boolean-RLE null bitmap. With a projection only the requested columns' streams are read, coalesced
across gaps up to 1 MB. Compression is blockwise with 3-byte headers (`OrcCompression.DecompressBlock()`).

### Write pipeline

```
OrcWriter
  └─ WriteBatchAsync → ColumnWriter.Write(array)
  └─ FlushStripeAsync (stripe full or closing): streams, row index, stripe footer
  └─ CloseAsync → WriteFileTailAsync (metadata, footer, postscript) → CompleteAsync
```

Defaults (`OrcWriterOptions`): 64 MB stripes, ZSTD, row index stride 10,000, RLE v2 for integers, Dictionary v2
for strings falling back to Direct v2 above 40,000 distinct values. Bloom filters are opt-in per column.

### ORC types

All ORC types are read and written:

| Category | ORC Types | Arrow Mapping |
|---|---|---|
| Integer | Boolean, Byte, Short, Int, Long | BooleanArray, Int8-64Array |
| Float | Float, Double | FloatArray, DoubleArray |
| String | String, Varchar, Char | StringArray |
| Binary | Binary | BinaryArray |
| Temporal | Date, Timestamp, TimestampInstant | Date32Array, TimestampArray |
| Decimal | Decimal | Decimal128Array |
| Complex | Struct, List, Map, Union | StructArray, ListArray, etc. |

---

## Vortex (`EngineeredWood.Vortex`)

[Vortex](https://github.com/vortex-data/vortex) is a columnar format with FlatBuffers metadata and many array
encodings. The reader handles files from Vortex up to 0.86 (edition `core2026.08.1`); the writer produces files
any reader since 0.36 accepts. Both are cross-validated against the Rust `vortex-array` crate.

### File container

```
[VTXF magic][segment_0][segment_1]...
[DType FB][Layout FB][Statistics FB][Footer FB][Postscript FB]
[EndOfFile struct][VTXF magic]
```

`EndOfFile` is `version:u16 | postscript_len:u16 | "VTXF"`. The postscript holds offsets to the other
FlatBuffers; the footer carries registries that segments and array nodes reference by index: `array_specs`
(encoding ids), `layout_specs` (layout ids) and `segment_specs` (offset, length, alignment, codec).

Only uncompressed segments are read: no upstream writer sets segment compression and segments record no
uncompressed length. Real compression lives in array encodings such as `vortex.zstd`. Encryption is rejected.

### Components

- **FlatBuffers without codegen.** `FlatBufferTable` (a `ref struct` vtable walker), `FlatBufferVector` and
  `BackwardsFlatBufferBuilder` replace `Google.FlatBuffers` + `flatc`. `Format/` has typed accessors for
  Postscript, Footer, DType, Layout and Array.
- **Schema.** `VortexSchemaConverter` maps all `DTypeKind`s to Arrow, dispatches the `vortex.timestamp` /
  `date` / `time` / `uuid` extensions, falls back to storage type for unknown extensions, and widens decimal
  precision > 38 to `Decimal256Type`. Non-struct roots are rejected.
- **Layouts.** `LayoutPlanner.PlanField` walks the layout tree per Arrow field into a `ColumnPlan`
  (`vortex.struct`, `vortex.chunked`, `vortex.flat`, `vortex.dict` → `DictColumnPlan`, and the zone-map layouts
  `vortex.stats` (≤ 0.83) / `vortex.zoned` (0.84+), read by `ZoneStatsLayout` / `ZonedZoneMap`). A zone map with
  an unknown aggregate is ignored and the column read unpruned. The writer still emits `vortex.stats`, which every
  reader since 0.36 understands.
- **Editions.** Since 0.84 upstream freezes component ids into editions. The reader covers `core2026.08.1`
  (including `vortex.onpair`) and `vortex.map` from `core2026.08.2`; `vortex.variant` is not read. The writer emits only core-edition
  components (checked by `VortexWriterEditionTests`), except `fastlanes.delta`, used only with `preferDelta`.

### Read pipeline

```
VortexFileReader.OpenAsync()
  └─ magic + EndOfFile → Postscript → Footer registries → DType → Arrow schema
     → Layout tree → LayoutPlanner → ColumnPlan[]

VortexFileReader.ReadAllAsync(rowOffset, rowCount, columnIndices, predicate)
  ├─ EvaluatePredicateRangesAsync → accepted row ranges (zone z → rows [z·zone_len, (z+1)·zone_len))
  ├─ Clip ranges to the requested rows, sort, merge
  └─ Per range: batches end at the next chunk boundary of any requested column
       (columns are chunked independently); each chunk is fetched, parsed
       (SerializedArray) and decoded (ArrayDecoder) once, then sliced
```

Predicates are the shared `EngineeredWood.Expressions` types. `VortexZoneStatsAccessor` implements
`IStatisticsAccessor<VortexZoneCursor>` over the typed Arrow zone-stat arrays; any zone not proven `AlwaysFalse`
is kept.

### Array decoders (`Encodings/`)

Dispatched by encoding id in `ArrayDecoder`:

| Group | Encodings |
|---|---|
| Primitive | `vortex.primitive`, `vortex.constant`, `vortex.sequence`, `vortex.null` |
| Bool | `vortex.bool`, `vortex.bytebool` |
| String / Binary | `vortex.varbin`, `vortex.varbinview`, `vortex.fsst` (via `Clast.Fsst`), `vortex.onpair` |
| Compression | `vortex.runend`, `vortex.dict`, `vortex.sparse`, `vortex.masked`, `vortex.zigzag`, `vortex.zstd` |
| Float | `vortex.alp`, `vortex.alprd`, `vortex.pco` (via `Clast.Pcodec`) |
| FastLanes | `fastlanes.bitpacked` (with patches), `fastlanes.for`, `fastlanes.rle`, `fastlanes.delta` |
| Composite | `vortex.list`, `vortex.listview`, `vortex.fixed_size_list`, `vortex.struct`, `vortex.ext` |
| Decimal | `vortex.decimal`, `vortex.decimal_byte_parts` |
| Temporal | `vortex.datetimeparts` |

### Write pipeline

`VortexFileWriter(stream, schema, compress, preferVarBinView, preserveStats, preferPco, preferDateTimeParts,
preferDictLayout, preferDelta)` serializes the DType up front, then per batch and column lets
`ArrayEncoderDispatch` choose an encoding (roughly constant > dict > FSST > ALP > ALP-RD > RLE > sparse > runend >
delta > FoR > bitpacked > raw; Pco replaces the numeric chain when `preferPco`), each compressing encoder gated on
profitability. `ArrayStatsComputer` collects zone stats when `preserveStats`. On close, `LayoutSerializer` builds
the root layout (flat, chunked, with zoned stats and/or dict layout), then the footer, postscript and EndOfFile.
Encoders accept sliced inputs (`data.Offset != 0`) without copying.

### Multi-targeting and dependencies

Builds for `netstandard2.0`, `net8.0` and `net10.0` without source changes; tests also run on `net472`, with two
HalfFloat tests gated to .NET 6+. Dependencies beyond Arrow and Core: `EngineeredWood.Expressions`,
`Clast.FastLanes`, `Clast.Fsst`, `Clast.Pcodec`.

---

## Expressions (`EngineeredWood.Expressions`, `EngineeredWood.Expressions.Arrow`)

A format-agnostic expression library for predicate pushdown (Parquet, Delta Lake, Iceberg, Vortex), a Spark SQL
parser, and an Arrow row evaluator with Spark function semantics. Design:
[`doc/predicate-pushdown-design.md`](doc/predicate-pushdown-design.md).

### Expression tree

- **`LiteralValue`** — readonly struct with 17 kinds (booleans, ints, floats, strings, binary, decimal,
  `BigInteger` high-precision decimal, dates, times, GUIDs, Half), inline primitive storage, and cross-type numeric
  promotion in `CompareTo`.
- **`Expression`** — sealed records `UnboundReference`, `BoundReference`, `LiteralExpression`, `FunctionCall`.
  Arithmetic and casts are function calls.
- **`Predicate`** (extends `Expression`) — `TruePredicate`, `FalsePredicate`, n-ary `AndPredicate`/`OrPredicate`,
  `NotPredicate`, `ComparisonPredicate` (incl. `NullSafeEqual`, `StartsWith`), `UnaryPredicate` (null/NaN tests),
  `SetPredicate` (`In`/`NotIn`).
- **`Expressions`** — static factory with constant folding and flattening.
- **Variant paths** — `VariantGetCall`, `VariantPath` and `VariantBound` let `variant_get(col, path, type)`
  predicates prune against shredded statistics (`IVariantStatisticsAccessor`).

### Schema binding

`ExpressionBinder` resolves `UnboundReference` to `BoundReference` through a caller-supplied lookup, returning the
same instance when nothing changed. Iceberg needs bound references; Parquet and Delta use column names.

### Statistics evaluation (three-valued)

`StatisticsEvaluator.Evaluate<TStats>(predicate, stats, accessor)` returns `AlwaysTrue`, `AlwaysFalse` or
`Unknown`. Each format implements `IStatisticsAccessor<TStats>`:

| Format | TStats | Accessor |
|---|---|---|
| Iceberg | `DataFileStats` | `IcebergStatisticsAccessor` (name → field ID) |
| Parquet | `RowGroup` | `ParquetStatisticsAccessor`; page-level accessor in `PageIndexPruner` |
| Delta Lake | `DeltaFileStats` | `DeltaFileStatsAccessor` (partition values + JSON stats) |
| Vortex | `VortexZoneCursor` | `VortexZoneStatsAccessor` |

It handles all operators, AND/OR/NOT, IS NULL via null counts, IN/NOT IN, operand flipping and constant folding.
Truncated bounds (`IsMinExact`/`IsMaxExact`) can still prove `AlwaysFalse` but never `AlwaysTrue` on equality.

### Spark SQL parser (`Sql/`)

`SparkSqlParser.ParseExpression` / `ParsePredicate` turn Spark SQL text — a Delta CHECK constraint, invariant,
generation expression or predicate — into the expression tree; it is hand-written recursive descent with
precedence climbing. `SparkSqlRenderer` writes a tree back as Spark SQL.

### Row-level evaluation

`ArrowRowEvaluator` evaluates a tree against a `RecordBatch`, returning a `BooleanArray` for predicates and an
`IArrowArray` for values, with SQL three-valued logic (`NULL AND FALSE` → FALSE, `NULL <=> NULL` → TRUE,
`5 IN (1, NULL)` → NULL). Function calls go to an `IFunctionRegistry`; without one they throw.
`SparkFunctionRegistry` (`Spark/`) implements the Spark functions Delta expressions use — arithmetic, `cast` /
`try_cast`, string, date/time, decimal and `variant_get` — with Spark's semantics. These are checked by a
differential fuzzer against Spark (`fuzz_expressions.py` in the Delta interop tests).

---

## Delta Lake (`EngineeredWood.DeltaLake`, `EngineeredWood.DeltaLake.Table`)

Two layers:

- **`EngineeredWood.DeltaLake`** — the transaction log: actions, NDJSON commits (`TransactionLog`), V1/V2
  checkpoints, snapshot reconstruction, log compaction, in-commit timestamps, version checksums, schema model,
  column mapping, type widening, identity columns, IcebergCompat validation, deletion vectors, row tracking, and
  `DeltaFilePruner`. `LogCommitter` runs the full optimistic-concurrency commit loop, so a host with its own data
  plane can commit without the table layer.
- **`EngineeredWood.DeltaLake.Table`** — the Arrow table API on `DeltaTable`: create/open, read (incl. time
  travel and change data feed), write/overwrite, DML, schema changes, constraints, compaction, vacuum, restore,
  checkpoints and transactions. It commits through `LogCommitter`, supplying the data-plane rebases as an
  `ICommitRebaseHandler`.

### Read pipeline

```
DeltaTable.ReadAllAsync(columns, filter)
  ├─ DeltaFilePruner.ShouldInclude per active file (partition values + JSON stats)
  ├─ OrderedScan over kept files (MaxConcurrentReads, MaxBufferedReadBytes, MemoryBudget)
  │    └─ ReadFileAsync: Parquet read with RowGroupPushdown (filter → file's physical
  │       columns, for row-group/page pruning), DV filter, type widening, column mapping,
  │       schema reconcile, partition columns re-materialized
  └─ yield batches in serial order
```

The pruner judges partition and data columns in one evaluator pass: a partition column's value is both its min and
its max. `DeltaLiteralDecoder` turns stats JSON and partition strings into `LiteralValue`s, using `BigInteger`
decimals past `System.Decimal`'s range.

### Write pipeline

```
DeltaTable.WriteAsync(batches)
  ├─ ProtocolVersions.ValidateWriteSupport, IcebergCompat.Validate
  ├─ Per batch: generated columns, identity columns, CHECK constraints / invariants, NOT NULL,
  │    column mapping rename, optional mergeSchema
  ├─ RollingDataFileWriter: split by partition, roll files at TargetFileSize, collect stats
  ├─ Build AddFile actions (stats, row tracking ids)
  ├─ CommitOccAsync → LogCommitter.CommitAsync
  │    ├─ TransactionLog.WriteCommitAsync (create-if-absent)
  │    └─ on collision: read the intervening commits, ICommitRebaseHandler,
  │       ConflictChecker; rebase and retry, or abort
  └─ Post-commit: checkpoint at CheckpointInterval, version checksum
```

Row tracking needs nothing on an append (`baseRowId` + position suffices); a copy-on-write rewrite materializes each
moved row's id and commit version into the hidden columns.

### Constraints and generated columns

CHECK constraints and invariants (`DeltaConstraintEnforcer`) and generation expressions (`DeltaGeneratedColumns`)
are parsed by `SparkSqlParser` and evaluated per batch by `ArrowRowEvaluator` with `SparkFunctionRegistry`. An
expression that cannot be parsed or evaluated refuses the write rather than guess, because Delta checks only at
write time and one bad commit poisons the table. `DeltaNotNullEnforcer` checks nullability against the table's
schema at every nesting level. `AddConstraintAsync` / `DropConstraintAsync` manage `delta.constraints.*`.

### DML

- **DELETE / UPDATE** (`DeleteAsync`, `UpdateAsync`) take an analyzable `Predicate` (files pruned by stats; the
  predicate is the read set for conflict checks) or an opaque delegate. `RowDeleteMode` picks how a partly matched
  file changes: `DeletionVector` masks rows in place, `CopyOnWrite` rewrites the file; the default follows
  `delta.enableDeletionVectors`. Fully matched files are removed whole.
- **MERGE** (`DeltaTable.Merge.cs`, `MergeOptions`) matches on key equality with matched / not matched / not
  matched by source clauses. Files whose key stats cannot overlap the source's key range are not read. Each changed
  file changes once, in one commit.
- **Row-level entry points** (`DeleteRowsAsync`, `UpdateRowsAsync`, `ReadRowsAsync`) address rows by
  `RowSelection`: absolute in-file positions per `add.path`.
- **replaceWhere** (`OverwriteWhereAsync`), **RESTORE** (`RestoreAsync`, a new commit re-adding the old version's
  files without touching data) and **mergeSchema** writes live in the matching `DeltaTable.*.cs` partial files;
  column add / drop / rename (`AddColumnAsync`, `DropColumnAsync`, `RenameColumnAsync`) are on `DeltaTable` too.

All of these run the same constraint, identity, NOT NULL and change-data-feed paths as an append.

### Concurrency and the transaction surface

`LogCommitter` (log layer) owns the protocol gate, retry, conflict verdict, post-commit snapshot refresh and
checkpointing, and never inspects actions beyond handing them to the log. `DeltaTransaction` (`StartTransaction`)
records a read version, a `ReadSet` and staged actions; `ConflictChecker` is a pure verdict over the commits that
landed in between, precise to the isolation level. The table layer's `ICommitRebaseHandler` resolves conflicts at
row granularity: disjoint deletes in one file union their deletion vectors, a delete whose file was compacted is
remapped by stable row id, and `baseRowId`s are re-derived. `WrittenFileLedger` records the files a transaction
wrote so `AbortAsync` deletes only those. Details: [`doc/delta-concurrency.md`](doc/delta-concurrency.md).

### Embedding seam

A host with its own Parquet codec and engine can use EngineeredWood for the log alone. `IDataFileReader` /
`IDataFileWriter` (on `DeltaTableOptions`, `[Experimental("EWDELTA0001")]`) swap the codec, and `PlanFiles` /
`WriteDataFilesAsync` / `CommitDataFilesAsync` let the host drive read, write and commit against a pinned
snapshot. See [`doc/embedding-host-guide.md`](doc/embedding-host-guide.md) and
[`doc/codec-seam-investigation.md`](doc/codec-seam-investigation.md).

### Supported features

Reader v3 / Writer v7. The README lists the named features implemented; a table requiring any other is rejected
rather than misread. `appendOnly`, `invariants`, `checkConstraints` and `generatedColumns` are enforced on write.
IcebergCompat V1–V3 is a writer constraint keeping the table convertible to Iceberg by an external tool; no Iceberg
metadata is produced.

---

## Iceberg (`EngineeredWood.Iceberg`)

Iceberg metadata, manifest read/write and scan planning for format versions 1–3 (including geometry/geography,
variant, default values, row IDs).

> **Experimental, and read-oriented.** The manifest Avro codec is a subset of the V2 schema (no
> `lower_bounds` / `upper_bounds`, partition tuple, or several V2/V3 fields), so manifests it writes are not
> consumable by other engines. It decodes by position in its own layout, so `ManifestIO` refuses any manifest
> whose writer schema differs from EngineeredWood's — i.e. those written by other engines (#586). Partition
> transforms are declarations only: none can be applied to a value. `TableScan` therefore prunes on in-memory
> statistics, not partitions. See [`doc/known-issues.md`](doc/known-issues.md).

### Components

- **Table metadata** (`TableMetadata.cs`) — JSON table state: snapshots, schemas, partition specs, sort orders.
- **Manifests** (`Manifest/`) — `DataFile`, `ManifestEntry`, `ManifestListEntry`; `ManifestIO` reads/writes them
  with `EngineeredWood.Avro`. `DataFile` bounds use `LiteralValue`.
- **Partition transforms** (`Transform.cs`) — Identity, Void, Bucket, Truncate, Year, Month, Day, Hour as
  `Transform` records, used for naming only.
- **Catalog** (`ICatalog.cs`) — `FileSystemCatalog` and `InMemoryCatalog`.
- **Expressions** (`Expressions/`) — an Iceberg-flavored factory over the shared library, plus `TableScan`.

### TableScan

```
TableScan(metadata, fs).Filter(predicate).PlanFilesAsync()
  ├─ Bind filter (name → field ID) and build IcebergStatisticsAccessor
  ├─ Read manifest list, then each manifest
  ├─ Per entry: skip deleted; pass delete files through; evaluate data files' stats
  └─ ScanResult { DataFiles, DeleteFiles, TotalFilesScanned, FilesSkipped }
```

Iceberg is metadata-only; data files are read with this library's Parquet, ORC or Avro readers.

---

## Arrow ↔ format type mapping

- **Parquet read** (`ArrowSchemaConverter`) — LogicalType → ConvertedType → PhysicalType fallthrough; decimal
  routing (INT32 → Decimal32, INT64 → Decimal64, FLBA → Decimal128/256), timestamp units, and the BYTE_ARRAY output
  modes (`ParquetReadOptions.ByteArrayOutput`).
- **Parquet write** (`ArrowToSchemaConverter`) — Arrow fields → pre-order `SchemaElement` list with standard
  group annotations for struct, list and map.
- **ORC** (`OrcSchema`) — `ToArrowType()` one way, the writer's `AddType()` the other.

## Decimal handling

Parquet decimals are big-endian and Arrow's little-endian: the read path reverses FLBA bytes and sign-extends to the
target width, and the write path reverses back. `Decimal32/64/128/256Type` all derive from `FixedSizeBinaryType`,
so decimal cases must precede `FixedSizeBinaryType` in switch expressions.

## Performance techniques

- **Ref struct decoders** over spans, with no heap allocation.
- **SIMD** — AVX2 `ByteStreamSplitDecoder`; vectorized validity bitmaps.
- **Thread-static buffers** — `ColumnChunkWriter` reuses compression and encoding buffers across pages.
- **ArrayPool / NativeBuffer** — pooled arrays for levels; native memory for Arrow buffers, handed over without
  copying.
- **In-place null scatter** in `ArrowArrayBuilder`.
- **Dictionary stats shortcut** — statistics over dictionary entries (O(unique)).
- **Open-addressing hash** — `DictionaryEncoder.BytesHashTable` (FNV-1a, linear probing).
- **Batched metadata I/O** — `MembershipPrefetch` and `BatchPageWindow` fetch many row groups' dictionaries or
  many batches' pages in one request.

## Test infrastructure

- **Parquet:** `TestData.cs` finds the `parquet-testing/data/` submodule; sweep tests read every sample file. The
  `Compatibility` project downloads files from fastparquet, parquet-dotnet, parquet-tools, DuckDB and others and
  validates every row group. Encoder/decoder tests round-trip against ParquetSharp; an interop tier reads with
  DuckDB and arrow-rs.
- **ORC:** `.orc` files in the test project; `CrossValidationTests` checks against PyArrow when available.
- **Avro:** fixtures from `TestData/generate_test_data.py` (fastavro); cross-validation in both directions over
  all types, codecs, schema evolution, projection and unions.
- **Vortex:** a Rust crate in `test/EngineeredWood.Vortex.Tests/Rust/` (`vortex-array` / `vortex-file` 0.86)
  writes fixtures, each chosen to force a specific encoding, and a `vortex-validator` binary reads .NET-written
  files back for `VortexCrossValidationTests`. `TestData/legacy-0.70/` keeps `vortex.stats` fixtures for the
  pruning tests.
- **Delta Lake:** `DeltaLake.Tests` covers the log layer; `DeltaLake.Table.Tests` covers the table API on temp
  directories and emulated cloud stores. Its `Interop/` tier drives Spark, delta-rs and DuckDB (`*_driver.py`) to
  read what EngineeredWood writes and vice versa, and reports Skipped when a tool is absent.
- **Iceberg:** metadata updates, V3 features, catalogs, manifest serialization and `TableScan` pruning.
- **Expressions:** `LiteralValue`, factories, binder and every evaluator path including truncated bounds; the
  Arrow suite covers three-valued row evaluation and the Spark functions.

## Benchmarks

BenchmarkDotNet suites for Parquet, ORC, Avro, Lance and Delta Lake live in `test/*.Benchmarks`;
`EngineeredWood.DeltaLake.DeltaDotnetBenchmarks` compares Delta operations against delta-dotnet.
