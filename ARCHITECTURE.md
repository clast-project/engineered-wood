# Architecture

EngineeredWood is a .NET 10 monorepo for reading and writing columnar file
formats — Apache Parquet, Apache ORC, Apache Avro, Lance, and Vortex — and
table formats — Lance dataset, Delta Lake, and Apache Iceberg — as Apache
Arrow `RecordBatch` objects. It is designed around cloud storage access
patterns — batched range reads, concurrent column chunk I/O, and pooled
buffer management — rather than the traditional single-cursor `Stream`
abstraction.

This document describes the implementation. For usage and build instructions, see `README.md`.

> **Coverage note.** The per-format sections below go deep on Parquet, Avro, ORC
> and Vortex. Lance has no equivalent section yet — its projects appear in the
> layout below, and the design is written up in
> [`doc/lance-reader-design.md`](doc/lance-reader-design.md) (historical) and
> [`doc/lance-future-work.md`](doc/lance-future-work.md) (current gaps) until one
> is written.

## Project layout

```
src/
  EngineeredWood.Core/                  Shared abstractions
    IO/                                 Random-access and sequential I/O abstractions
      Local/                            Local file backends
    Compression/                        Codec dispatch (compress + decompress)
    Arrow/                              NativeBuffer<T>
  EngineeredWood.Expressions/           Format-agnostic expression library
    LiteralValue, Expression, Predicate, ExpressionBinder, StatisticsEvaluator
  EngineeredWood.Expressions.Arrow/     Row-level evaluator over RecordBatch
    ArrowRowEvaluator, IRowEvaluator, IFunctionRegistry
  EngineeredWood.Parquet/               Parquet format implementation
    Parquet/
      Thrift/                           Thrift Compact Protocol codec
      Metadata/                         Footer metadata model and (de)serialization
      Schema/                           Schema tree with definition/repetition levels
      Data/                             Column encoding/decoding, Arrow conversion
      ParquetStatisticsAccessor.cs      Adapter for shared StatisticsEvaluator
      BloomFilterPredicateEvaluator.cs  Bloom-based predicate probing
  EngineeredWood.Orc/                   ORC format implementation
    Proto/                              Protobuf schema (orc_proto.proto)
    Encoders/                           RLE v1/v2, boolean, dictionary encoders
    Readers/                            Per-type column readers
    Writers/                            Per-type column writers
  EngineeredWood.Avro/                  Avro format implementation
    Schema/                             Schema parsing, resolution, fingerprinting
    Container/                          OCF read/write (sync + async)
    Data/                               RecordBatch assembly/encoding
    Encoding/                           AvroBinaryReader/Writer (ref struct)
  EngineeredWood.Lance/                 Lance file reader and writer (v2.0 + v2.1 + v2.2)
    Proto/                              file.proto, file2.proto, encodings_v2_0.proto,
                                          encodings_v2_1.proto (verbatim from lance-format/lance)
    Format/                             LanceFooter, OffsetSizeEntry, LanceVersion,
                                          FieldColumnRange (field→column mapping)
    Schema/                             LanceSchemaConverter (logical_type → Arrow,
                                          including v2.2 "map" → Apache.Arrow.MapType)
    Encodings/V20/                      Per-encoding decoders for v2.0 ArrayEncoding
    Encodings/V21/                      MiniBlockLayout / FullZipLayout / ConstantLayout /
                                          PageLayout dispatcher and CompressiveEncoding
                                          decoders (Flat, Variable, Fsst, InlineBitpacking,
                                          OutOfLineBitpacking, Dictionary, FSL, General/ZSTD,
                                          plus bool bit-pack)
    LanceFileWriter.cs                  Single-file writer: leaf primitives, FSL, List/
                                          LargeList (incl. multi-chunk + repetition_index),
                                          Struct (recursive), Map, Bool, optional ZSTD
                                          wrap on Flat values
  EngineeredWood.Lance.Table/           Lance dataset / table API (manifests, fragments,
                                          versioned commits)
    LanceTable.cs                       Open at latest / version / asOf timestamp; Read
                                          with column projection + predicate pushdown +
                                          fragment pruning via secondary indices; deletion
                                          mask filtering during read
    LanceDatasetWriter.cs               Create / Append / Overwrite, multi-fragment per
                                          transaction, DeleteRowsAsync / DeleteAsync /
                                          UpdateAsync / CompactAsync / VacuumAsync; every
                                          commit stamps manifest.timestamp for time travel
    Manifest/                           ManifestReader, ManifestPathResolver
                                          (latest / by version / by timestamp)
    Deletions/                          DeletionFile reader/writer (Arrow IPC + Roaring),
                                          DeletionMask, RecordBatchRowFilter
    Indices/                            B-tree + bitmap secondary index reads, IndexPruner
    Proto/                              table.proto, transaction.proto (verbatim)
  EngineeredWood.Vortex/                Vortex file reader and writer
    FlatBuffers/                        Hand-rolled FlatBuffer reader (FlatBufferTable /
                                          Vector) + BackwardsFlatBufferBuilder (no flatc
                                          / Google.FlatBuffers dependency)
    Format/                             Postscript, Footer, DType, Layout, Array typed
                                          ref-struct accessors over FlatBufferTable;
                                          SegmentLocator (offset, length, alignment, codec)
    Schema/                             VortexSchemaConverter (DType → Apache.Arrow.Schema,
                                          including extension types vortex.timestamp /
                                          vortex.date / vortex.time / vortex.uuid)
    Layouts/                            VortexLayout tree, LayoutPlanner (per-Arrow-field
                                          ColumnPlan with chunked / dict / zoned variants),
                                          DictReconstructor, ZoneStatsLayout
    Encodings/                          Read-side decoders (one file per array encoding)
                                          and SerializedArray + ScalarValueProto helpers
    Writer/                             VortexFileWriter (multi-batch streaming),
                                          DTypeSerializer, LayoutSerializer,
                                          PostscriptSerializer, FooterSerializer,
                                          SegmentWriter
    Writer/Encodings/                   Write-side encoders (one file per array encoding,
                                          plus ArrayEncoderDispatch, ArrayStatsComputer,
                                          ArrayNodeEmitter, EncoderHelpers,
                                          ScalarValueSerializer, SegmentBuilder)
    VortexZoneStatsAccessor.cs          IStatisticsAccessor<TStats> adapter so
                                          shared EngineeredWood.Expressions
                                          predicates evaluate against per-zone
                                          stats (typed Arrow Min/Max arrays)
    Stat.cs / ZoneStats.cs              Per-zone stats enum + typed accessor surface
    VortexFileReader.cs                 Open + Schema + ReadAllAsync (with column
                                          projection, row-range slice, and predicate)
    VortexFileFormat.cs                 Magic / EndOfFile constants
  EngineeredWood.DeltaLake/             Delta transaction log (low-level)
    Actions/                            AddFile, RemoveFile, MetadataAction, Protocol, etc.
    Log/                                NDJSON commit read/write, log compaction, in-commit timestamps,
                                          LogCommitter (the optimistic-concurrency commit loop)
    Concurrency/                        ConflictChecker + ReadSet — the pure OCC verdict function
    Checkpoint/                         V1 (Parquet) and V2 (JSON + sidecar) checkpoint reader/writer
    Snapshot/                           Snapshot reconstruction
    Schema/                             Delta schema model, ColumnMapping, TypeWidening, IcebergCompat
    DeletionVectors/                    RoaringBitmap reader/writer, Base85 codec
    RowTracking/                        Row tracking config + writer
    ChangeDataFeed/                     CDF config
    DeltaFilePruner.cs                  Partition + stats predicate pushdown
    IsolationLevel.cs                   WriteSerializable / Serializable
  EngineeredWood.DeltaLake.Table/       Delta table API (high-level Arrow I/O)
    DeltaTable.cs                       Open / Read / Write / Update / Delete / Compact / Vacuum
    Partitioning/, Compaction/, Vacuum/, ChangeDataFeed/, IdentityColumns/, RowTracking/, Stats/, TypeWidening/
  EngineeredWood.Iceberg/               Iceberg metadata + scan planning
    Manifest/                           Manifest file/list types and Avro-encoded I/O
    Expressions/                        Iceberg-flavored Expressions factory + TableScan
    Serialization/                      JSON serialization for table metadata
    Catalog interfaces and implementations (FileSystem, InMemory)
  EngineeredWood.Azure/                 Azure Blob Storage backends (namespace EngineeredWood.IO.Azure)
    AzureBlobRandomAccessFile, AzureBlobSequentialFile, AzureTableFileSystem
  EngineeredWood.Gcs/                   Google Cloud Storage backends (namespace EngineeredWood.IO.Gcs)
    GcsRandomAccessFile, GcsSequentialFile, GcsTableFileSystem
  EngineeredWood.Aws/                   Amazon S3 backends (namespace EngineeredWood.IO.Aws)
    S3RandomAccessFile, S3SequentialFile, S3TableFileSystem
test/
  EngineeredWood.Parquet.Tests/             xUnit tests for Parquet
  EngineeredWood.Parquet.Benchmarks/        BenchmarkDotNet suites for Parquet
  EngineeredWood.Parquet.Compatibility/     92-file cross-tool validation
  EngineeredWood.Orc.Tests/                 xUnit tests for ORC
  EngineeredWood.Orc.Benchmarks/            BenchmarkDotNet suites for ORC
  EngineeredWood.Avro.Tests/                xUnit tests for Avro
  EngineeredWood.Avro.Benchmarks/           BenchmarkDotNet suites for Avro
  EngineeredWood.Lance.Tests/               xUnit tests for Lance file-level reader
                                              and writer, including a compatibility
                                              sweep over committed pylance-produced
                                              .lance files and writer→pylance
                                              cross-validation
  EngineeredWood.Lance.Table.Tests/         xUnit tests for the Lance dataset API
                                              (Create / Append / Overwrite / Delete /
                                              Update / Compact / Vacuum / time travel)
  EngineeredWood.Lance.Benchmarks/          BenchmarkDotNet suites for Lance
  EngineeredWood.Vortex.Tests/              xUnit tests for the Vortex reader and
                                              writer; cross-validates against the
                                              Rust vortex-array 0.70 implementation
                                              via a Rust binary (test/.../Rust/)
                                              that emits .vortex fixtures and a
                                              vortex-validator that opens
                                              .NET-written files. Also targets net472.
  EngineeredWood.DeltaLake.Tests/           xUnit tests for the Delta log layer
  EngineeredWood.DeltaLake.Table.Tests/     xUnit tests for the Delta table API
  EngineeredWood.DeltaLake.Benchmarks/      BenchmarkDotNet suites for Delta Lake
  EngineeredWood.Iceberg.Tests/             xUnit tests for Iceberg
  EngineeredWood.Expressions.Tests/         xUnit tests for the expression library
  EngineeredWood.Expressions.Arrow.Tests/   xUnit tests for the Arrow row evaluator
parquet-testing/                            Git submodule with 100+ Parquet sample files
```

### Project dependencies

```
EngineeredWood.Core
EngineeredWood.Expressions                            (no Arrow, no format deps)
  ↑
  ├── EngineeredWood.Expressions.Arrow                (depends on Apache.Arrow)
  ├── EngineeredWood.Parquet
  ├── EngineeredWood.Iceberg
  ├── EngineeredWood.Vortex
  └── EngineeredWood.DeltaLake
        ↑
        └── EngineeredWood.DeltaLake.Table
```

## Conventions

- **Async model.** `ValueTask<T>` everywhere, `.ConfigureAwait(false)` on every await.
- **Buffer ownership.** I/O methods return `IMemoryOwner<byte>`; the caller disposes.
- **Ref structs.** Hot-path decoders (`ThriftCompactReader`, `RleBitPackedDecoder`, `DeltaBinaryPackedDecoder`) are `ref struct` over `ReadOnlySpan<byte>` — zero allocation, but they cannot be captured in lambdas or used across awaits.
- **Nullable enabled, implicit usings enabled.**
- **`checked()` casts** for `long → int` conversions; `ObjectDisposedException.ThrowIf()` for disposed guards.

---

## Core library (`EngineeredWood.Core`)

Shared abstractions used by both Parquet and ORC.

### I/O layer (`EngineeredWood.IO`)

Replaces `Stream` with two interfaces designed for columnar access patterns.

**Reading: `IRandomAccessFile`** — No shared position cursor, so concurrent reads of disjoint ranges are safe.

| Type | Notes |
|---|---|
| `LocalRandomAccessFile` | `RandomAccess` API; sequential sync reads (OS page cache efficient) |
| `AzureBlobRandomAccessFile` | HTTP range requests; `SemaphoreSlim` throttle (default 16); delegates multi-range reads to `CoalescingFileReader` |
| `GcsRandomAccessFile` | Google Cloud Storage ranged `DownloadObjectAsync`; same throttle + coalescing |
| `S3RandomAccessFile` | Amazon S3 `GetObject` + `ByteRange`; same throttle + coalescing |
| `CoalescingFileReader` | Decorator that merges nearby ranges (gap ≤ 64 KB, merged ≤ 16 MB) to reduce round trips, then slices results back to original order |

**Writing: `ISequentialFile`** — Append-only: `WriteAsync(ReadOnlyMemory<byte>)` + `FlushAsync()` + `Position`. `LocalSequentialFile` writes to disk; the cloud writers stream with bounded memory using each backend's native chunked upload rather than buffering the whole object — Azure block blobs (`AzureBlobSequentialFile`), GCS resumable uploads (`GcsSequentialFile`, fed through a bounded `System.Threading.Channels` pipe by a background upload task), and S3 multipart uploads (`S3SequentialFile`, which drives the part loop directly).

**Directory / table operations: `ITableFileSystem`** — Table formats (Delta Lake, Iceberg, Lance datasets) manage many files under a root and need list / open-read / create / delete / exists / read-all / write-all / try-write-all. `LocalTableFileSystem` backs the local filesystem; `AzureTableFileSystem`, `GcsTableFileSystem`, and `S3TableFileSystem` back the three clouds. `TryWriteAllBytesAsync` is the load-bearing operation: it publishes a file's complete contents only if that path does not already exist, which is exactly the claim a commit protocol makes about version N. The object stores satisfy it in a single conditional request — `IfGenerationMatch = 0` (GCS), `If-None-Match: *` (Azure and S3) — so a whole commit is one round trip. A backend without a conditional single-shot upload emulates it: the local filesystem writes a sibling temp file and `File.Move`s it, which fails rather than replacing. A lost race returns `false` instead of clobbering the winner, and `TransactionLog.WriteCommitAsync` turns that into `DeltaConflictException`.

**Buffer management** — `BufferAllocator` is an abstract factory for `IMemoryOwner<byte>`. The concrete `PooledBufferAllocator` wraps `ArrayPool<byte>.Shared` and slices rented arrays to exact size, reducing GC pressure.

### Compression (`EngineeredWood.Compression`)

`Compressor` and `Decompressor` dispatch to codec-specific implementations:

| Codec | Library | Notes |
|---|---|---|
| Uncompressed | — | Direct copy |
| Snappy | Snappier | Pure managed, span-based |
| Gzip | System.IO.Compression | `CompressionLevel.Fastest` for write |
| Brotli | System.IO.Compression | Span-based `TryCompress` / `TryDecompress` |
| Lz4 | K4os.Compression.LZ4 | Block codec, span-based |
| Lz4Hadoop | K4os.Compression.LZ4 | Hadoop framing (big-endian length headers) with frame and raw fallback |
| Zstd | ZstdSharp.Port | Pure managed; `[ThreadStatic]` compressor/decompressor for reuse |
| Deflate | System.IO.Compression | Raw deflate (RFC 1951) |

The shared `CompressionCodec` enum uses semantic names. Format-specific mapping functions translate between wire values and the enum (e.g., `MetadataDecoder.ParquetCodecFromThrift()`, ORC's `CompressionKind` protobuf enum).

### Arrow helpers (`EngineeredWood.Arrow`)

`NativeBuffer<T>` is a growable buffer backed by native memory that can be transferred to Arrow without copying.

---

## Parquet (`EngineeredWood.Parquet`)

### Thrift codec (`Parquet.Thrift`)

Parquet's footer and page headers are encoded in the Thrift Compact Protocol. EngineeredWood implements its own codec rather than depending on an external Thrift library.

- **`ThriftCompactReader`** — `ref struct` over `ReadOnlySpan<byte>`. Reads varints (ULEB128), zigzag int32/int64, field headers with delta-encoded field IDs, binary, lists, and nested structs (inline 8-slot stack).
- **`ThriftCompactWriter`** — Mirror encoder with a growable `byte[]` buffer. Reusable via `Reset()`.

### Metadata (`Parquet.Metadata`)

Immutable record types mirroring the Parquet Thrift spec: `FileMetaData`, `RowGroup`, `ColumnChunk`→`ColumnMetaData`, `SchemaElement`, `LogicalType`. `MetadataDecoder`/`MetadataEncoder` convert between the model and Thrift bytes.

### Schema (`Parquet.Schema`)

`SchemaDescriptor` converts the flat `SchemaElement` list into a `SchemaNode` tree, then collects leaf columns into `ColumnDescriptor` objects with computed definition and repetition levels.

### Read pipeline

```
ParquetFileReader.ReadRowGroupAsync()
  ├─ PrepareRowGroupAsync()          Gather column ranges + metadata
  ├─ ReadRangesAsync()               Parallel I/O (coalescing applied)
  ├─ Parallel.For → ColumnChunkReader.ReadColumn()   per column
  │    ├─ PageHeaderDecoder          Thrift → PageHeader (V1/V2/Dict)
  │    ├─ Decompressor               Codec dispatch
  │    ├─ LevelDecoder               Def/rep levels (V1: 4-byte prefix; V2: raw)
  │    ├─ Value decoder              PLAIN / RLE_DICTIONARY / DELTA_* / BYTE_STREAM_SPLIT
  │    └─ ArrowArrayBuilder          Build typed Arrow array, insert nulls from def levels
  └─ AssembleRecordBatch()
       └─ NestedAssembler            (if nested columns) flat leaves → Struct/List/Map arrays
```

**Encodings:**

| Encoding | Decoder | Notes |
|---|---|---|
| PLAIN | `PlainDecoder` | Boolean bit-packed (LSB), fixed-width via `MemoryMarshal.Cast`, BYTE_ARRAY with 4-byte length prefix |
| PLAIN_DICTIONARY / RLE_DICTIONARY | `DictionaryDecoder` + `RleBitPackedDecoder` | Dictionary stored as typed arrays; indices RLE/bit-packed |
| RLE | `RleBitPackedDecoder` | For def/rep levels; LSB bit-packing, 8-value groups |
| BIT_PACKED (deprecated) | `RleBitPackedDecoder` | MSB-first packing, no length prefix |
| DELTA_BINARY_PACKED | `DeltaBinaryPackedDecoder` | 128-value blocks, 4 miniblocks; 8-byte aligned fast path for bit widths ≤ 56 |
| DELTA_LENGTH_BYTE_ARRAY | `DeltaLengthByteArrayDecoder` | Delta-encoded lengths + raw data |
| DELTA_BYTE_ARRAY | `DeltaByteArrayDecoder` | Delta-encoded prefix lengths + suffix lengths + suffix data |
| BYTE_STREAM_SPLIT | `ByteStreamSplitDecoder` | AVX2 SIMD with scalar fallback; interleaved byte streams for float/double |
| ALP (experimental, 10) | `AlpDecoder` | Unratified proposal; decimal-aware integer encoding + FOR + bit-packing, per 1024-value vector |
| FSST (experimental, 11) | `FsstPageDecoder` + `FsstSymbolTable` | Unratified proposal; symbol table lives in a chunk-level `SYMBOL_TABLE_PAGE`, data pages carry a 9-byte header + end-offset array + code stream |

**Arrow array construction** — `ArrowArrayBuilder` dispatches by Arrow type. For nullable flat columns, it scatters non-null values right-to-left in-place to open gaps for nulls, avoiding a temporary buffer. Validity bitmaps are built from definition levels with SIMD (`Vector256`/`Vector128`) when available.

**Nested types** — `NestedAssembler` reconstructs Arrow `StructArray`, `ListArray`, and `MapArray` from flat leaf columns plus their def/rep level arrays. Phantom entries (null/empty list markers) are filtered before recursing into inner element assembly.

**Concurrency strategies:**

| Method | I/O | Decode |
|---|---|---|
| `ReadRowGroupAsync` (default) | Parallel batch via `ReadRangesAsync` | `Parallel.For` |
| `ReadRowGroupIncrementalAsync` | Sequential per column | Sequential |
| `ReadRowGroupIncrementalParallelAsync` | Parallel with bounded concurrency | `Parallel.For` |
| `ReadAllAsync` | Streams row groups as `IAsyncEnumerable<RecordBatch>` | Per-group strategy above |

### Write pipeline

```
ParquetFileWriter.WriteRowGroupAsync(RecordBatch)
  ├─ Auto-split if batch > RowGroupMaxRows
  ├─ Schema inference from first batch (ArrowToSchemaConverter)
  ├─ Parallel.For → ColumnChunkWriter.WriteColumn()   per leaf column
  │    ├─ NestedLevelWriter.Decompose()     (if nested) Arrow → flat leaves + def/rep levels
  │    ├─ DictionaryEncoder.TryEncode()     Analyze cardinality; open-addressing hash for ByteArray
  │    ├─ Dictionary path:
  │    │    ├─ PlainEncoder (dictionary page)
  │    │    └─ RleBitPackedEncoder (index pages)
  │    ├─ Non-dictionary path:
  │    │    └─ Type-aware V2 encoder or PLAIN V1
  │    ├─ Compressor                        Codec dispatch
  │    └─ StatisticsCollector               Min/max/null_count (O(unique) when dict available)
  ├─ Sequential column writes to ISequentialFile
  └─ CloseAsync() → MetadataEncoder → footer + footer length + trailing PAR1
```

**Encoding strategy** — `EncodingStrategyResolver` selects the encoding per column:

| Physical type | V2 encoding | V1 encoding |
|---|---|---|
| Boolean | RLE (1-bit) | PLAIN |
| Int32, Int64 | DELTA_BINARY_PACKED | PLAIN |
| Float, Double | BYTE_STREAM_SPLIT | PLAIN |
| ByteArray | DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, or FSST | PLAIN |
| FixedLenByteArray | DELTA_BYTE_ARRAY | PLAIN |

Dictionary encoding is attempted first (unless disabled or Boolean). Cardinality threshold: 20% of non-null values. The `DictionaryEncoder` uses an open-addressing hash table with FNV-1a hashing for ByteArray/FLBA to avoid GC pressure from collision chains.

**FSST** inverts the usual page-at-a-time flow, because its symbol table is shared by the whole column chunk and so cannot be known until every value has been seen. `FsstCompressedColumn.TryCompress` trains the table and compresses the chunk's non-null values once, up front; the data pages that follow are slices of that one payload, with their end offsets rebased onto each page's own data section. Compressing eagerly is also what makes the proposal's "fall back if FSST did not help" rule answerable before any page is written — `TryCompress` returns null and the chunk reverts to `DELTA_LENGTH_BYTE_ARRAY`. Two details are load-bearing: codes are renumbered into ascending-length order (the wire format stores a length histogram, not per-symbol lengths, so code order *is* the length information), and the symbol table page is written before the data pages with its offset recorded in `ColumnMetaData.symbol_table_page_offset`, which the reader uses as the chunk's start the way it uses the dictionary page offset.

**Write options** — `ParquetWriteOptions` controls compression (per-column overrides), page version, page size, dictionary limits, row group splitting, byte array encoding strategy, key-value metadata, and application identifier.

### Predicate pushdown

`ParquetReadOptions.Filter` accepts a shared `EngineeredWood.Expressions.Predicate`. When set, `ReadAllAsync` evaluates each row group's column statistics against the predicate before reading any data pages, skipping row groups that prove `AlwaysFalse`.

- **`ParquetStatisticsAccessor`** implements `IStatisticsAccessor<RowGroup>` for the shared `StatisticsEvaluator`. Decodes raw min/max bytes per (PhysicalType, LogicalType) into `LiteralValue`. Prefers the typed `min_value`/`max_value` fields (correct logical sort) over the legacy `min`/`max` (only used as fallback for signed numeric types). INT96 returns null per the spec — sort order is undefined.
- **`BloomFilterPredicateEvaluator`** is a second pass enabled by `FilterUseBloomFilters`. Walks the predicate tree and probes Bloom filters for `Equal`/`In` sub-predicates whose values miss; returns `AlwaysFalse` only when every value misses, `Unknown` otherwise. Other predicate kinds (range, IS NULL, function calls) contribute nothing. Three-valued logic propagates through AND/OR/NOT identically to the statistics evaluator.
- The reader runs the statistics evaluator first; bloom probing only fires for row groups that statistics returned `Unknown` for, since bloom probing requires extra I/O.

---

## Avro (`EngineeredWood.Avro`)

EngineeredWood.Avro reads and writes Apache Avro Object Container Files (OCF) and framed streaming messages, producing Arrow `RecordBatch` objects.

### Schema

The schema layer parses Avro JSON schemas into an `AvroSchemaNode` tree, supporting all Avro types: primitives (null, boolean, int, long, float, double, bytes, string), named types (record, enum, fixed), and complex types (array, map, union). Logical types include date, time-millis/micros, timestamp-millis/micros/nanos (UTC and local variants), decimal (bytes and fixed, with precision/scale), and uuid.

- **`AvroSchemaParser`** — Recursive JSON parser with named type resolution and self-referencing schema support.
- **`AvroSchemaWriter`** — Serializes schema tree back to JSON, including logical type parameters (e.g. decimal precision/scale).
- **`ArrowSchemaConverter`** — Bidirectional Avro ↔ Arrow type mapping. Handles Decimal128Type, DictionaryType (for enums), unions (nullable → nullable field, general → DenseUnion), and all temporal types.
- **`SchemaResolver`** — Schema evolution per Avro spec: field matching by name/alias, type promotion (int→long/float/double, long→float/double, float→double, string↔bytes), default value insertion for missing reader fields, writer field skipping.
- **`ParsingCanonicalForm`** — Computes PCF (normalized JSON) for fingerprinting.
- **`RabinFingerprint`** — CRC-64-AVRO with precomputed 256-entry lookup table.

### Container layer

- **`OcfReader` / `OcfReaderAsync`** — Read OCF header (magic, metadata, sync marker), decompress blocks, yield raw data spans.
- **`OcfWriter` / `OcfWriterAsync`** — Write OCF header, compress and frame data blocks.

### Read pipeline

```
AvroReaderBuilder
  ├─ Build(Stream) → AvroReader (sync, IEnumerable<RecordBatch>)
  └─ BuildAsync(Stream) → AvroAsyncReader (async, IAsyncEnumerable<RecordBatch>)
       └─ OcfReader reads blocks
            └─ RecordBatchAssembler.DecodeBlock()
                 ├─ AvroBinaryReader (ref struct, zero-alloc varint/LE primitives)
                 ├─ Per-type builders: primitive, Date32, Timestamp, Enum, Fixed,
                 │   Decimal (fixed/bytes), Array, Map, Struct, DenseUnion
                 ├─ NullableBuilder wraps builders for ["null", T] unions
                 └─ PromotingBuilders for schema evolution (8 promotion types)
```

### Write pipeline

```
AvroWriterBuilder
  ├─ Build(Stream) → AvroWriter (sync)
  └─ BuildAsync(Stream) → AvroAsyncWriter (async)
       └─ OcfWriter writes blocks
            └─ RecordBatchEncoder.Encode()
                 ├─ AvroBinaryWriter (varint, LE float/double, length-prefixed bytes)
                 └─ Per-type dispatch: primitive, logical types, enum, fixed,
                     decimal (fixed/bytes), array, map, struct, DenseUnion
```

### Streaming encode/decode

For framed (non-OCF) messages, the library supports multiple wire formats:

- **Single Object Encoding (SOE)** — `[0xC3, 0x01]` + 8-byte LE Rabin fingerprint + datum
- **Confluent** — `[0x00]` + 4-byte BE schema ID + datum
- **Apicurio** — `[0x00]` + 8-byte BE global ID + datum
- **Raw binary** — Bare datum (requires pre-set fingerprint)

`AvroEncoder` encodes `RecordBatch` rows into `EncodedRows` with per-row message framing. `AvroDecoder` is a push-based streaming decoder that looks up schemas in a `SchemaStore` and auto-flushes on batch limit or schema switch.

### Compression

| Codec | Library | Notes |
|---|---|---|
| Null | — | Passthrough |
| Deflate | System.IO.Compression | Raw deflate (RFC 1951) |
| Snappy | Snappier (via Core) | Snappy block + 4-byte big-endian CRC32C |
| Zstandard | ZstdSharp.Port (via Core) | Frame format with embedded size |
| LZ4 | K4os.Compression.LZ4 (via Core) | 4-byte LE uncompressed size prefix + LZ4 block |

### Projection

Field projection is supported via `WithProjection(int[])` (select by index) or `WithSkipFields(string[])` (exclude by name). Both construct a projected reader schema and leverage the schema resolution mechanism to skip unwanted fields during decode.

---

## ORC (`EngineeredWood.Orc`)

### Metadata

ORC metadata (postscript, footer, stripe information) is serialized with Protobuf. The `.proto` file is compiled by Grpc.Tools at build time into `EngineeredWood.Orc.Proto`.

### Schema (`OrcSchema`)

`OrcSchema` builds a schema tree from the ORC footer's type list and converts between ORC types and Arrow types via `ToArrowType()`.

### Read pipeline

```
OrcReader (open file, parse postscript/footer/schema)
  └─ CreateRowReader() → OrcRowReader (async iterator)
      └─ ReadStripeSelectiveAsync / ReadStripeFullAsync
          └─ CreateColumnReaders (one per ORC type)
              └─ ColumnReader.ReadBatch(size) → IArrowArray
```

Each column type has a dedicated reader class. Column data is stored in named streams (PRESENT, DATA, SECONDARY, LENGTH, DICTIONARY_DATA) which are decoded via RLE v1/v2 decoders. The PRESENT stream carries null bitmaps as boolean RLE.

**Selective I/O** — When column projection is active, only the streams for requested columns are read. Nearby streams are coalesced (1 MB gap threshold) to reduce I/O round trips.

**Compression** — ORC uses blockwise compression with 3-byte headers (original-length, compressed/uncompressed flag). `OrcCompression.DecompressBlock()` handles the block format.

### Write pipeline

```
OrcWriter (init with Arrow schema, create column writers)
  └─ WriteBatchAsync (buffer rows, check stripe size)
      └─ ColumnWriter.Write(array) (encode data, track stats)
  └─ FlushStripeAsync (when stripe full or closing)
      └─ Collect streams (PRESENT + data streams per column)
      └─ Write row index (positions, per-row-group stats)
      └─ Write stripe footer, compress if needed
  └─ CloseAsync → WriteFileTailAsync (metadata, footer, postscript)
```

**Stripe management** — Auto-flush at configurable size (default 64 MB). Row indexing at configurable stride (default 10K rows) with byte-offset tracking for skip-scan.

**Encoding defaults** — RLE v2 for integers, Dictionary v2 for strings (fallback to Direct v2 above 40K unique values).

### ORC types

All 19 ORC types are supported for both reading and writing:

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

[Vortex](https://github.com/vortex-data/vortex) is a columnar file format
with FlatBuffers-based metadata and a rich array-encoding zoo. The
EngineeredWood implementation reads + writes Vortex 0.70-format files,
cross-validated against the Rust `vortex-array` crate.

### File container

```
[VTXF magic][segment_0][segment_1]...
[DType FB][Layout FB][Statistics FB][Footer FB][Postscript FB]
[EndOfFile struct][VTXF magic]
```

`EndOfFile` is `version:u16 | postscript_len:u16 | "VTXF"`. The
postscript (≤ 65 528 bytes) holds offsets to the DType / Layout /
Statistics / Footer FlatBuffers; the footer carries the file's
**registries** — the strings that segments and array nodes reference by
small integer index:

- `array_specs` — encoding ids the file actually uses
  (`vortex.primitive`, `fastlanes.bitpacked`, `vortex.fsst`, etc.).
- `layout_specs` — layout ids (`vortex.flat`, `vortex.struct`,
  `vortex.chunked`, `vortex.stats`, `vortex.dict`).
- `segment_specs` — `(offset, length, alignment_exponent,
  compression_codec)` per segment.

Segment compression is wired through to Core's `CompressionCodec` —
`None` is implemented; LZ4 / ZLib / ZStd are recognised in the
locator but rejected at decode time pending fixtures that exercise
them. Encryption is rejected outright.

### FlatBuffers without codegen

Rather than depend on `Google.FlatBuffers` + a `flatc` build step, the
project hand-rolls FlatBuffer access:

- **`FlatBuffers/FlatBufferTable.cs`** — `readonly ref struct` over
  `ReadOnlySpan<byte>` that walks vtables and exposes typed slot reads
  (scalars, sub-tables, vectors, strings, bytes).
- **`FlatBuffers/FlatBufferVector.cs`** — typed vector accessor.
- **`FlatBuffers/BackwardsFlatBufferBuilder.cs`** — high-to-low
  offset-order writer used by the Writer's `DTypeSerializer`,
  `LayoutSerializer`, `FooterSerializer`, and `PostscriptSerializer`.
- **`Format/{Postscript, Footer, DType, Layout, Array}.cs`** — typed
  ref-struct accessors over `FlatBufferTable`. Enum mappings for
  `PType`, `DTypeKind`, `CompressionScheme`, `BufferCompression`,
  `Precision`.

### Schema (`Schema/VortexSchemaConverter`)

Walks a Vortex `DType` tree and emits an `Apache.Arrow.Schema`. Covers
all 11 `DTypeKind` variants. Extension types are dispatched by id:
`vortex.timestamp` → `TimestampType(unit, tz)`, `vortex.date` →
`Date32Type` / `Date64Type`, `vortex.time` → `Time32` / `Time64`,
`vortex.uuid` → `FixedSizeBinaryType(16)`. Unknown extensions fall
through to the storage dtype. Decimal precision > 38 widens to
`Decimal256Type`. Non-Struct roots are rejected at open time — the
public API is RecordBatch-shaped.

### Layouts (`Layouts/`)

- **`VortexLayout`** — managed tree node carrying encoding id (string,
  resolved via the layout-spec registry), row count, metadata bytes,
  children, and segment refs.
- **`LayoutPlanner.PlanField`** — recursive walk over the layout tree
  per Arrow field, producing a `ColumnPlan` (a sequence of per-chunk
  `(SegmentRef, RowCount)`). Handles `vortex.struct` (descend by field
  index), `vortex.stats` (descend to child[0]; capture per-zone stats
  ref from child[1] when present), `vortex.chunked` (flatten children),
  `vortex.dict` (return a `DictColumnPlan` with separate values + codes
  references), `vortex.flat` (leaf with one segment ref).
- **`DictReconstructor`** — materialises `output[i] = values[codes[i]]`
  for the layout-level dict path; supports `StringType` + all integer
  / float Arrow types and propagates validity from the codes child.
- **`ZoneStatsLayout`** — reconstructs the zones-table struct dtype
  from the stats bitset so `GetZoneStatsAsync` can decode it.

The wire encoding id `vortex.stats` is also known upstream as
`ZonedLayout` — for legacy reasons the serialized id stayed `vortex.stats`.

### Read pipeline

```
VortexFileReader.OpenAsync()
  ├─ Validate leading + trailing 'VTXF' magic
  ├─ Parse EndOfFile (version, postscript_len, magic)
  ├─ Parse Postscript → segment offsets for DType / Layout / Footer
  ├─ Decompress + parse Footer (array_specs, layout_specs, segment_specs)
  ├─ Decompress + parse DType → VortexSchemaConverter → Apache.Arrow.Schema
  ├─ Decompress + parse Layout → VortexLayout tree
  └─ LayoutPlanner.Plan → ColumnPlan[] with optional ZoneInfo per column

VortexFileReader.ReadAllAsync(rowOffset, rowCount, columnIndices, predicate)
  ├─ predicate.EvaluateZonesAsync(this, totalZones) → HashSet<int> acceptedZones
  ├─ For each chunkIdx:
  │    ├─ Skip if chunkIdx ∉ acceptedZones
  │    ├─ Skip via row-range cursor if chunk wholly outside [rowOffset, rowOffset + rowCount)
  │    ├─ For each requested column:
  │    │    ├─ Fetch the chunk's segment(s) via IRandomAccessFile
  │    │    ├─ Decompress (None today)
  │    │    ├─ SerializedArray.Parse → Array FlatBuffer + raw buffer slices
  │    │    └─ ArrayDecoder.Decode → IArrowArray
  │    ├─ Assemble RecordBatch
  │    └─ Slice via RecordBatch.Slice if at the row-range boundary
  └─ yield batch
```

### Array decoders (`Encodings/`)

Per-encoding decoder classes, dispatched by encoding string in
`ArrayDecoder`. Coverage:

| Group | Encodings |
|---|---|
| Primitive | `vortex.primitive` (nullable + non-nullable), `vortex.constant`, `vortex.sequence`, `vortex.null` |
| Bool | `vortex.bool` (LSB-packed bitmap), `vortex.bytebool` |
| String / Binary | `vortex.varbin`, `vortex.varbinview`, `vortex.fsst` (via `Clast.Fsst`) |
| Compression | `vortex.runend`, `vortex.dict` (array-level), `vortex.sparse`, `vortex.masked` |
| Float | `vortex.alp`, `vortex.alprd` (f32 + f64), `vortex.pco` (via `Clast.Pcodec`) |
| FastLanes | `fastlanes.bitpacked` (with patches), `fastlanes.for`, `fastlanes.rle` (floats); `fastlanes.delta` wired but skipped pending an upstream Clast.FastLanes lane-major helper |
| Composite | `vortex.list`, `vortex.listview`, `vortex.fixed_size_list`, `vortex.struct`, `vortex.ext` |
| Decimal | `vortex.decimal` (i8..i256 → Decimal128/256), `vortex.decimal_byte_parts` |
| Temporal | `vortex.datetimeparts` (combined with `vortex.ext` → Timestamp) |

Helpers: `SerializedArray` parses a `vortex.flat` segment's trailing
`u32 fb_length LE` to find the Array FlatBuffer + buffer slices;
`ScalarValueProto` is a minimal vortex-proto `ScalarValue` parser used
by constant / sequence / sparse / FoR.

### Write pipeline

```
VortexFileWriter(stream, schema, compress?, preferVarBinView?, preserveStats?,
                 preferPco?, preferDateTimeParts?, preferDictLayout?)
  ├─ Reserve VTXF magic at file head
  ├─ DTypeSerializer.Emit → schema → DType FlatBuffer (held in memory)
  └─ Each WriteBatch(batch):
       ├─ Per column:
       │    ├─ ArrayEncoderDispatch.Emit (per encoding, recursive)
       │    │    │  Order: constant > dict > FSST > ALP > ALP-RD >
       │    │    │  RLE > sparse > runend > delta > FoR > bitpacked >
       │    │    │  raw primitive (or varbin / varbinview / list / FSL /
       │    │    │  struct / decimal / datetimeparts / ext)
       │    │    ├─ Pco supersedes ALP/ALP-RD/RLE/FoR/bitpacked when
       │    │    │  preferPco is set
       │    │    ├─ Compressing encoders gate on profitability
       │    │    │  (e.g. `(encoded × 1.5) < raw`)
       │    │    └─ Recurse into composite children through dispatch
       │    ├─ ArrayStatsComputer (if preserveStats) collects per-batch
       │    │  Min/Max/NullCount/NaNCount/IsConstant/IsSorted/Sum/
       │    │  UncompressedSizeInBytes per column type
       │    └─ Emit segment(s) via SegmentWriter
       └─ Track per-column segment indices

  Close():
    ├─ FinalizeDictLayoutColumns (if preferDictLayout): emit one shared
    │  values segment + per-batch codes segments per dict-eligible column
    ├─ LayoutSerializer builds the root layout per-shape:
    │    SerializeStructFlat                         single-batch
    │    SerializeStructChunked                      multi-batch
    │    SerializeStructStatsChunked                 + zoned stats
    │    SerializeStructDictMixed                    + dict layout
    │    SerializeStructDictMixedStats               + dict + zoned stats
    ├─ FooterSerializer emits array_specs / layout_specs / segment_specs
    ├─ PostscriptSerializer emits offsets to DType / Layout / Footer
    └─ Write EndOfFile struct + trailing VTXF magic
```

### Array encoders (`Writer/Encodings/`)

One file per encoding, plus shared infrastructure:

- **`ArrayEncoderDispatch`** — chooses an encoding per column based on
  the column's Arrow type, `compress` / `prefer*` flags, and per-encoder
  `IsApplicable` profitability gates.
- **`ArrayNodeEmitter`** — vtable shapes for the common Array FlatBuffer
  layouts (single-buffer, metadata + buffer + children, with-stats,
  multi-buffer).
- **`ArrayStatsComputer`** — collects per-batch stats for the
  `preserveStats` zones table.
- **`EncoderHelpers`** — shared validity-bitmap rebasing for sliced
  inputs (`ExtractValidityBitmap(srcBitOffset, rowCount)`).
- **`ScalarValueSerializer`** — vortex-proto `ScalarValue` writer
  (mirror of `ScalarValueProto` on the read side).
- **`SegmentBuilder` / `SegmentWriter`** — collect Array FlatBuffer +
  raw buffer slices, write them to the underlying `Stream`, and append
  to the segment-specs registry with the correct alignment exponent.

Compressing encoders honor `data.Offset != 0` so sliced Arrow inputs
round-trip without a copy. Nullable inputs are supported on every
encoder that encounters them in practice; `IsApplicable` rejects
all-null columns where the encoding would degenerate.

### Predicate API + zone pruning

Vortex consumes the shared `EngineeredWood.Expressions` library — the
same `Predicate` type that drives Parquet row-group pruning, Delta
file pruning, and Iceberg scan planning. There is no Vortex-specific
predicate hierarchy.

- Build predicates with the shared `EngineeredWood.Expressions.Expressions`
  factory: `Equal("col", LiteralValue.Of(...))`,
  `GreaterThanOrEqual(...)`, `IsNull("col")`, `In("col", ...)`,
  `And(...)`, `Or(...)`, `Not(...)`, etc.
- The `LiteralValue` struct carries 17 typed kinds (numeric, string,
  binary, decimal, `DateOnly`, `TimeOnly`, `DateTimeOffset`, GUID,
  Half) with cross-type numeric promotion in `CompareTo`.

The reader-side wiring lives in two places:

- **`VortexZoneStatsAccessor : IStatisticsAccessor<VortexZoneCursor>`**
  reads the typed Arrow zone-stats arrays at the cursor's `ZoneIndex`
  and produces `LiteralValue` matching the column's Arrow type. Handles
  Arrow numerics, Bool, String, Binary, Decimal128/256 (via raw
  little-endian bytes → `BigInteger` →
  `LiteralValue.HighPrecisionDecimalOf`), and the temporal types
  Date32/64 (→ `DateOnly`), Time32/64 (→ `TimeOnly`), Timestamp (→
  `DateTimeOffset`). On netstandard2.0 the date/time kinds fall back
  to long unit-ticks since `System.DateOnly` / `System.TimeOnly`
  require .NET 6+.
- **`VortexFileReader.EvaluatePredicateZonesAsync`** walks the
  predicate to collect referenced column names, calls
  `GetZoneStatsAsync` once per referenced column, then iterates each
  zone with a mutable `VortexZoneCursor` and calls
  `StatisticsEvaluator.Evaluate(predicate, cursor, accessor)`. Zones
  whose result is anything other than `FilterResult.AlwaysFalse` are
  kept (conservative — the row batch may still need a row-level
  filter). Unresolved column references resolve to a null stats entry
  → accessor returns null → evaluator returns Unknown → zone kept.

`ReadAllAsync(Predicate, ...)` is a thin wrapper that drives this
loop and then dispatches to `ReadAllAsync(ISet<int> acceptedZones,
...)`. The wider features of the shared evaluator (truncated-bound
handling, three-valued AND/OR/NOT, set predicates, all-null column
short-circuiting) come for free.

### Cross-validation

Test fixtures are built by a Rust crate at
`test/EngineeredWood.Vortex.Tests/Rust/` that uses `vortex-array` /
`vortex-file` 0.70 from crates.io. A second Rust binary (also under
`Rust/`) acts as a `vortex-validator` — it opens .NET-written files and
emits the row-by-row contents in JSON. The C# `VortexCrossValidationTests`
shells out to that validator to verify writer output.

Observed Vortex compressor heuristics (used to pick fixtures that hit
each decoder):

| Input shape | Encoding picked |
|---|---|
| Monotonic ints `[1,2,3]` | `vortex.sequence` |
| Random wide-range ints | `vortex.primitive` |
| All-equal | `vortex.constant` |
| Nullable ints | `vortex.primitive` + `vortex.bool` validity child |
| Strings (default) | `vortex.fsst` |
| Strings (no-compress) | `vortex.varbinview` |
| Low-cardinality strings, 64+ rows | `vortex.dict` LAYOUT |
| Long-run integers | `vortex.runend` |
| Mode-dominant integers | `vortex.sparse` |
| Decimal-shaped floats, ≥ 1024 rows | `vortex.alp` |
| Irrational floats, ≥ 1024 rows | `vortex.alprd` |
| Small-range integers, ≥ 1024 rows | `fastlanes.bitpacked` |

### Multi-targeting

The library compiles clean for `netstandard2.0`, `net8.0`, and `net10.0`
with no source changes — every encoder / decoder, the FlatBuffer
reader/writer, the predicate API, and the row-range slicer are
binary-compatible with .NET Framework 4.7.2. The test suite also runs
on `net472`; only the two HalfFloat-focused tests are gated behind
`#if NET6_0_OR_GREATER` because `System.Half` /
`Apache.Arrow.HalfFloatArray` require .NET 6+.

### Dependencies (above Apache.Arrow + Core)

- **`EngineeredWood.Expressions`** — shared predicate / `LiteralValue`
  / `StatisticsEvaluator`. Vortex doesn't depend on
  `EngineeredWood.Expressions.Arrow` since zone-stat arrays are read
  via the Arrow API directly inside `VortexZoneStatsAccessor`.
- **`Clast.FastLanes`** — bit-packing / FoR / delta / RLE primitives
  (same dependency Lance uses).
- **`Clast.Fsst`** — FSST symbol-table compression / decompression.
- **`Clast.Pcodec`** — Pco wrapped encoder / decoder.

---

## Expressions (`EngineeredWood.Expressions`, `EngineeredWood.Expressions.Arrow`)

A format-agnostic expression library used by Parquet, Delta Lake, Iceberg, and Vortex for predicate pushdown, plus a separate Arrow-based row evaluator. See [`doc/predicate-pushdown-design.md`](doc/predicate-pushdown-design.md) for the full architecture and remaining phases.

### Expression tree

- **`LiteralValue`** — readonly struct, 17 typed kinds (booleans, ints, floats, strings, binary, decimal, high-precision decimal via `BigInteger`, dates, times, GUIDs). Inline storage for primitives via `[StructLayout(Explicit)]`, object slot for reference types. Cross-type numeric promotion in `CompareTo` (int vs long, float vs double).
- **`Expression`** hierarchy — `UnboundReference(name)`, `BoundReference(fieldId, name)`, `LiteralExpression(value)`, `FunctionCall(name, args)`. All sealed records.
- **`Predicate`** hierarchy (extends `Expression` so predicates can be function arguments) — `TruePredicate`, `FalsePredicate`, `AndPredicate`, `OrPredicate`, `NotPredicate` (n-ary And/Or with constant folding and flattening), `ComparisonPredicate`, `UnaryPredicate`, `SetPredicate`. Operator enums: `ComparisonOperator` (Equal, NotEqual, LessThan/Equal, GreaterThan/Equal, NullSafeEqual, StartsWith/NotStartsWith), `UnaryOperator` (IsNull, IsNotNull, IsNaN, IsNotNaN), `SetOperator` (In, NotIn).
- **`Expressions`** static factory — convenience methods (`Equal("col", value)`, `And(...)`, etc.) that produce the records above with constant folding (e.g. `And(true, x) → x`).

### Schema binding

`ExpressionBinder` walks an expression tree and resolves `UnboundReference` to `BoundReference` against a caller-supplied `Func<string, int?>` or `IReadOnlyDictionary<string, int>`. Iceberg requires bound references for manifest evaluation; Parquet and Delta work directly with column names. Identity-preserving: returns the same instance when nothing changed.

### Statistics evaluation (three-valued)

`StatisticsEvaluator.Evaluate<TStats>(predicate, stats, accessor)` returns `FilterResult.AlwaysTrue`, `AlwaysFalse`, or `Unknown`. Each format implements `IStatisticsAccessor<TStats>` for its own carrier:

| Format | TStats | Accessor |
|---|---|---|
| Iceberg | `DataFileStats` | `IcebergStatisticsAccessor` (column name → field ID translation) |
| Parquet | `RowGroup` | `ParquetStatisticsAccessor` (raw bytes → `LiteralValue` per physical+logical type) |
| Delta Lake | `DeltaFileStats` | `DeltaFileStatsAccessor` (handles partition values + JSON stats in one pass) |
| Vortex | `VortexZoneCursor` | `VortexZoneStatsAccessor` (typed Arrow zone-stat arrays at a per-zone cursor; Date/Time/Timestamp unit conversion) |

The evaluator handles all comparison operators, AND/OR/NOT with short-circuiting, IS NULL via null counts, IN/NOT IN with empty-set folding, operator flipping when literal is on the left, and constant folding when both sides are literals. Truncated statistics (`IsMinExact`/`IsMaxExact`) are conservative: derives `AlwaysFalse` safely but holds back from `AlwaysTrue` on equality.

### Row-level evaluation

`EngineeredWood.Expressions.Arrow.ArrowRowEvaluator` walks an expression tree against a `RecordBatch`, producing a `BooleanArray` for predicates and `IArrowArray` for value expressions. Uses `LiteralValue?[]` and `bool?[]` internally for SQL three-valued semantics:

- `NULL = 5` → NULL; `NULL AND FALSE` → FALSE; `NULL OR TRUE` → TRUE
- `5 <=> NULL` → FALSE; `NULL <=> NULL` → TRUE
- `x IN (1, NULL)` where x = 5 → NULL (no match, but list contains null)

Function calls dispatch to an optional `IFunctionRegistry`. The library ships no built-in functions; the eventual `EngineeredWood.SparkSql` package will provide a Spark function registry. Until then, function-bearing expressions throw at evaluation time.

---

## Delta Lake (`EngineeredWood.DeltaLake`, `EngineeredWood.DeltaLake.Table`)

Two-layer API:

- **`EngineeredWood.DeltaLake`** — Transaction log: actions (`AddFile`, `RemoveFile`, `MetadataAction`, `ProtocolAction`, `CommitInfo`, `DomainMetadata`, `DeletionVector`, etc.), NDJSON commit reader/writer (`TransactionLog`), V1/V2 checkpoint reader/writer, snapshot reconstruction, log compaction, in-commit timestamps. **Reading and updating a log is complete here**: `LogCommitter` runs the optimistic-concurrency commit loop — attempt, conflict-check via `ConflictChecker`, rebase, retry, checkpoint on interval — so a host with its own data plane never needs the table layer to commit. Also: schema model (`StructType`/`StructField`/`PrimitiveType`), column mapping (id and name modes), type widening, identity columns, Iceberg compatibility validation, deletion vectors (RoaringBitmap reader/writer with Base85 codec), row tracking, change data feed config, and `DeltaFilePruner` (partition + statistics file skipping).

- **`EngineeredWood.DeltaLake.Table`** — Arrow-based table API: `DeltaTable.OpenAsync` / `CreateAsync`, `WriteAsync` / `ReadAllAsync` / `ReadAtVersionAsync` / `ReadAtTimestampAsync`, `UpdateAsync` / `DeleteAsync`, `CompactAsync`, `VacuumAsync`. Also: identity column generation, partition splitting/path encoding, change data feed reader/writer, type widening on read, stats collection, deletion vector filtering on read. It commits through the log layer's `LogCommitter`, supplying the two rebases that only a data plane can do (deletion-vector union / row-id remap, and row-tracking id re-derivation) as an `ICommitRebaseHandler`.

### Read pipeline

```
DeltaTable.ReadAllAsync(columns, filter)
  ├─ Build DeltaFilePruner (if filter is set) from snapshot.Schema + partitionColumns
  ├─ Iterate snapshot.ActiveFiles.Values
  │    ├─ pruner.ShouldInclude(addFile, filter)
  │    │    ├─ Parse addFile.Stats JSON to ColumnStats (lazy)
  │    │    ├─ Wrap in DeltaFileStats with parsed stats
  │    │    └─ StatisticsEvaluator.Evaluate(predicate, fileStats, accessor)
  │    ├─ ReadFileAsync (open Parquet, optional DV filter, type widening,
  │    │   column mapping rename, partition column re-materialization)
  │    └─ yield batches
```

The pruner unifies partition pruning and stats pruning into a single evaluator pass: for partition columns the accessor returns the constant partition value as both min and max; for data columns it decodes the JSON stats. A single `AlwaysFalse` from either source skips the file.

### Write pipeline

```
DeltaTable.WriteAsync(batches)
  ├─ ProtocolVersions.ValidateWriteSupport
  ├─ IcebergCompat.Validate (if active)
  ├─ For each batch:
  │    ├─ Identity column generation
  │    ├─ Partition split → one file per (partition values, batch slice)
  │    ├─ Column mapping rename (logical → physical)
  │    ├─ Optional partition materialization (IcebergCompat)
  │    ├─ Row tracking: nothing on an append (baseRowId + position suffices);
  │    │   a copy-on-write REWRITE materializes each moved row's original
  │    │   id + commit version into the declared hidden columns
  │    └─ ParquetFileWriter.WriteRowGroupAsync
  ├─ Build AddFile actions (Stats, BaseRowId, DefaultRowCommitVersion, etc.)
  ├─ CommitOccAsync → LogCommitter.CommitAsync — the optimistic-concurrency loop
  │    ├─ ProtocolVersions.ValidateWriteSupport (the gate travels with the commit)
  │    ├─ TransactionLog.WriteCommitAsync (NDJSON, atomic create-if-absent)
  │    └─ on collision: read readVersion+1..latest, run the table's
  │       ICommitRebaseHandler, run ConflictChecker, rebase and retry if
  │       nothing we read was invalidated, else abort
  └─ Auto-checkpoint at CheckpointInterval
```

### Concurrency and the transaction surface

Commits go through an optimistic-concurrency loop rather than failing on any
collision. The loop itself is `LogCommitter`, in the **log** layer: it owns the
protocol gate, the retry, the conflict verdict, the post-commit snapshot
refresh, and checkpoint-on-interval, and it never inspects the actions beyond
handing them to the log — so a host with its own Parquet, statistics and
deletion vectors can commit with real OCC without taking the Arrow data plane.

`DeltaTransaction` (public, via `StartTransaction`) records a read version plus
staged actions, and `ConflictChecker` is a pure verdict function over the
commits that landed in between. What the table layer adds is an
`ICommitRebaseHandler`, for the actions whose CONTENT is coupled to the version
they land at. That is where conflicts get resolved at row granularity: two
deletes of disjoint rows in one file union their deletion vectors, a delete
whose file was concurrently compacted is remapped by stable row id, and
row-tracking `baseRowId`s are re-derived against the advanced high-water mark.
Details in [`doc/delta-concurrency.md`](doc/delta-concurrency.md).

### Embedding seam

A host that owns its own Parquet codec and execution engine can use
EngineeredWood for the log alone: `IDataFileReader` / `IDataFileWriter` swap the
codec, and `PlanFiles` / `WriteDataFilesAsync` / `CommitDataFilesAsync` let the
host drive read, write and commit against a pinned snapshot. Both seams are
`[Experimental]`. See [`doc/embedding-host-guide.md`](doc/embedding-host-guide.md)
and [`doc/codec-seam-investigation.md`](doc/codec-seam-investigation.md).

### Decoding stats and partitions

`DeltaLiteralDecoder` (internal) converts `JsonElement` (from `AddFile.Stats`) and `string` (from `AddFile.PartitionValues`) to `LiteralValue` based on the Delta primitive type name. Falls back to high-precision decimal via `BigInteger` when values exceed `System.decimal`'s 28-29 digit range.

### Supported features

Reader v3 / Writer v7. The README lists the named features that are implemented; a table requiring one that is not is rejected rather than mis-read, and the enforcement features (`appendOnly`, `invariants`, `checkConstraints`, `generatedColumns`) fail closed when genuinely active. Iceberg compatibility (V1 and V2) is implemented as a writer constraint that ensures the Delta table's structure is convertible to Iceberg by an external tool — it does not produce Iceberg metadata directly.

---

## Iceberg (`EngineeredWood.Iceberg`)

Apache Iceberg metadata, manifest read/write, and scan planning. Format versions 1, 2, and 3 (including geometry/geography, variant, default values, row IDs).

> **Experimental, and read-oriented.** Two limits shape everything below. The
> manifest Avro codec is a subset of the declared V2 schema — it omits
> `lower_bounds` / `upper_bounds`, the partition tuple, and several V2/V3 fields
> — so manifests EngineeredWood writes are not consumable by other Iceberg
> engines, and manifests it reads lose their bounds. And the partition transforms
> below are **declarations only**: they parse, serialize and name, but none can
> be applied to a value, so partition values cannot be derived from data.
> Consequently `TableScan` prunes on statistics carried in memory, not on
> partition transforms. Full list in [`doc/known-issues.md`](doc/known-issues.md).

### Components

- **Table metadata** (`TableMetadata.cs`) — JSON representation of Iceberg table state including snapshots, schemas, partition specs, sort orders.
- **Manifest layer** (`Manifest/`) — `DataFile`, `ManifestEntry`, `ManifestListEntry`. `ManifestIO` reads/writes Avro-encoded manifest files using `EngineeredWood.Avro`. Statistics on `DataFile` (`ColumnLowerBounds`, `ColumnUpperBounds`, `NullValueCounts`) use the shared `LiteralValue` struct.
- **Partition transforms** (`Transform.cs`) — Identity, Void, Bucket, Truncate, Year, Month, Day, Hour, as `abstract record Transform` cases. Composed into `PartitionField` in `PartitionSpec`. They carry their parameters (`BucketTransform(int NumBuckets)`) and drive partition-field naming, but implement no `Apply(value)` — the bucket hash (Murmur3 over per-type canonical bytes), truncation, and temporal extraction are all unimplemented.
- **Catalog** (`ICatalog.cs`) — Abstract catalog with `FileSystemCatalog` and `InMemoryCatalog` implementations.
- **Expressions** (`Expressions/`) — Iceberg consumes the shared `EngineeredWood.Expressions` library. `Iceberg.Expressions.Expressions` is a thin Iceberg-flavored factory that preserves the historical API surface (`AlwaysTrue()`, `Apply()`, `NotNull()` aliases) while producing shared types underneath.

### TableScan

```
TableScan(metadata, fs).Filter(predicate).PlanFilesAsync()
  ├─ Bind filter against schema (column name → field ID via shared ExpressionBinder)
  ├─ Build IcebergStatisticsAccessor with the schema's name→id map
  ├─ Read manifest list, then each manifest file
  ├─ For each entry:
  │    ├─ Skip if marked deleted
  │    ├─ Pass through delete files unconditionally
  │    └─ For data files: wrap stats in DataFileStats, evaluate via shared
  │       StatisticsEvaluator. If AlwaysFalse, skip.
  └─ Return ScanResult { DataFiles, DeleteFiles, TotalFilesScanned, FilesSkipped }
```

Iceberg is metadata-only — data files referenced in manifests are read with the Parquet, ORC, or Avro readers in this same library.

---

## Arrow ↔ format type mapping

### Parquet — read direction (`ArrowSchemaConverter`)

Three-stage fallthrough: LogicalType → ConvertedType → PhysicalType. Handles decimal precision routing (INT32→Decimal32, INT64→Decimal64, FLBA→Decimal128/256), timestamp units, and the three BYTE_ARRAY output modes.

### Parquet — write direction (`ArrowToSchemaConverter`)

Converts Arrow `Schema` fields into a flat list of `SchemaElement` in pre-order traversal. Nested types (struct, list, map) produce the standard Parquet group annotations with correct `NumChildren` and repetition types.

### ORC — bidirectional (`OrcSchema`)

`ToArrowType()` maps ORC types to Arrow; the writer's `AddType()` maps Arrow schema fields back to the ORC type tree.

---

## Decimal handling

Parquet stores decimals in big-endian; Arrow uses little-endian. The read path reverses FLBA bytes and sign-extends to the target Arrow width (Decimal32/64/128/256). The write path reverses Arrow bytes back to big-endian before encoding.

Pattern match ordering matters: `Decimal32/64/128/256Type` all inherit from `FixedSizeBinaryType`, so decimal cases must come before `FixedSizeBinaryType` in switch expressions.

---

## Performance techniques

- **Ref struct decoders** — `ThriftCompactReader`, `RleBitPackedDecoder`, `DeltaBinaryPackedDecoder` operate over spans with no heap allocation.
- **SIMD** — `ByteStreamSplitDecoder` has AVX2 fast paths for 4-byte and 8-byte unsplit. `ArrowArrayBuilder` uses `Vector256`/`Vector128` for validity bitmap construction.
- **Thread-static buffers** — `ColumnChunkWriter` reuses compression and value encoding buffers across pages via `[ThreadStatic]` fields.
- **ArrayPool / NativeBuffer** — Pooled managed arrays for levels; native memory for Arrow buffer construction (transferred to Arrow without copying).
- **In-place null scatter** — `ArrowArrayBuilder` scatters non-null values right-to-left to open gaps, avoiding a temporary copy.
- **Dictionary stats shortcut** — When dictionary encoding succeeds, statistics are computed over dictionary entries (O(unique)) rather than all values.
- **Open-addressing hash** — `DictionaryEncoder.BytesHashTable` uses FNV-1a + linear probing for ByteArray deduplication, avoiding managed `Dictionary<>` overhead.

---

## Test infrastructure

- **Parquet:** `TestData.cs` locates the `parquet-testing/data/` submodule by walking up from `AppContext.BaseDirectory`. Sweep tests iterate all sample files, skip encrypted/malformed, collect failures, assert none. The `EngineeredWood.Parquet.Compatibility` project downloads 92 files from fastparquet, parquet-dotnet, parquet-tools, and HuggingFace and validates all row groups. Decoder/encoder tests use both unit-level checks and round-trip verification against ParquetSharp.
- **ORC:** Test data files (.orc) are included directly in the test project. `CrossValidationTests` validates round-trip correctness against PyArrow's ORC implementation when available.
- **Avro:** Test data generated by a Python script (`generate_test_data.py`) using fastavro. Cross-validation tests verify both directions: fastavro writes → EngineeredWood reads, and EngineeredWood writes → fastavro reads. Coverage includes all types (primitives, nullable, enum, array, map, fixed, struct, decimal, uuid), all codecs (null, deflate, snappy, zstandard, lz4), schema evolution, projection, and dense unions.
- **Vortex:** Test fixtures (`*.vortex`) are produced by a Rust crate at `test/EngineeredWood.Vortex.Tests/Rust/` that depends on `vortex-array` / `vortex-file` 0.70 from crates.io. A second Rust binary in the same crate (`vortex-validator`) opens .NET-written files and dumps the row-by-row contents in JSON; `VortexCrossValidationTests` shells out to it. Each Vortex array encoding has at least one fixture chosen to force vortex's Rust compressor to pick it (often via `with_strategy(...)` overrides when the default heuristics would pick something else). The test project targets `net8.0`, `net10.0`, and `net472`; only the two HalfFloat tests are gated to net6+.
- **Delta Lake:** Two suites. `EngineeredWood.DeltaLake.Tests` covers the log layer (action serialization, snapshot reconstruction, checkpoints, deletion vectors, in-commit timestamps, etc.). `EngineeredWood.DeltaLake.Table.Tests` covers the table API end-to-end with temp-directory tables, including write/read round-trips, partitioning, Iceberg compatibility, identity columns, row tracking, change data feed, deletion vectors, and predicate pushdown.
- **Iceberg:** `EngineeredWood.Iceberg.Tests` covers schema/partition/snapshot updates, V3 features (geometry, variant, default values, row IDs), catalog operations, manifest serialization, and `TableScan` with predicate pruning.
- **Expressions:** `EngineeredWood.Expressions.Tests` covers `LiteralValue` (cross-type comparison, high-precision decimal, hashing), expression factories (constant folding, flattening), the binder (identity preservation, lenient mode), and the statistics evaluator (every predicate variant with synthetic stats including truncation edge cases). `EngineeredWood.Expressions.Arrow.Tests` covers the row evaluator with full SQL three-valued logic.

## Benchmarks

`EngineeredWood.Parquet.Benchmarks`, `EngineeredWood.Orc.Benchmarks`, `EngineeredWood.Avro.Benchmarks`, and `EngineeredWood.DeltaLake.Benchmarks` contain BenchmarkDotNet suites for reads, writes, and encodings.
