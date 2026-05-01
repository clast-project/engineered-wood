# Architecture

EngineeredWood is a .NET 10 monorepo for reading and writing columnar file
formats — Apache Parquet, Apache ORC, Apache Avro — and table formats —
Delta Lake and Apache Iceberg — as Apache Arrow `RecordBatch` objects.
It is designed around cloud storage access patterns — batched range reads,
concurrent column chunk I/O, and pooled buffer management — rather than
the traditional single-cursor `Stream` abstraction.

This document describes the implementation. For usage and build instructions, see `README.md`.

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
  EngineeredWood.DeltaLake/             Delta transaction log (low-level)
    Actions/                            AddFile, RemoveFile, MetadataAction, Protocol, etc.
    Log/                                NDJSON commit read/write, log compaction, in-commit timestamps
    Checkpoint/                         V1 (Parquet) and V2 (JSON + sidecar) checkpoint reader/writer
    Snapshot/                           Snapshot reconstruction
    Schema/                             Delta schema model, ColumnMapping, TypeWidening, IcebergCompat
    DeletionVectors/                    RoaringBitmap reader/writer, Base85 codec
    RowTracking/                        Row tracking config + writer
    ChangeDataFeed/                     CDF config
  EngineeredWood.DeltaLake.Table/       Delta table API (high-level Arrow I/O)
    DeltaTable.cs                       Open / Read / Write / Update / Delete / Compact / Vacuum
    DeltaFilePruner.cs                  Partition + stats predicate pushdown
    Partitioning/, Compaction/, Vacuum/, ChangeDataFeed/, IdentityColumns/, RowTracking/, Stats/, TypeWidening/
  EngineeredWood.Iceberg/               Iceberg metadata + scan planning
    Manifest/                           Manifest file/list types and Avro-encoded I/O
    Expressions/                        Iceberg-flavored Expressions factory + TableScan
    Serialization/                      JSON serialization for table metadata
    Catalog interfaces and implementations (FileSystem, InMemory)
  EngineeredWood.Azure/                 Azure Blob Storage backends
    IO/Azure/                           AzureBlobRandomAccessFile, AzureBlobSequentialFile
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
| `CoalescingFileReader` | Decorator that merges nearby ranges (gap ≤ 64 KB, merged ≤ 16 MB) to reduce round trips, then slices results back to original order |

**Writing: `ISequentialFile`** — Append-only: `WriteAsync(ReadOnlyMemory<byte>)` + `FlushAsync()` + `Position`. Implementations for local files (`LocalSequentialFile`) and Azure (`AzureBlobSequentialFile`).

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
| ByteArray | DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY | PLAIN |
| FixedLenByteArray | DELTA_BYTE_ARRAY | PLAIN |

Dictionary encoding is attempted first (unless disabled or Boolean). Cardinality threshold: 20% of non-null values. The `DictionaryEncoder` uses an open-addressing hash table with FNV-1a hashing for ByteArray/FLBA to avoid GC pressure from collision chains.

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

## Expressions (`EngineeredWood.Expressions`, `EngineeredWood.Expressions.Arrow`)

A format-agnostic expression library used by Parquet, Delta Lake, and Iceberg for predicate pushdown, plus a separate Arrow-based row evaluator. See [`doc/predicate-pushdown-design.md`](doc/predicate-pushdown-design.md) for the full architecture and remaining phases.

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

- **`EngineeredWood.DeltaLake`** — Transaction log: actions (`AddFile`, `RemoveFile`, `MetadataAction`, `ProtocolAction`, `CommitInfo`, `DomainMetadata`, `DeletionVector`, etc.), NDJSON commit reader/writer (`TransactionLog`), V1/V2 checkpoint reader/writer, snapshot reconstruction, log compaction, in-commit timestamps. Also: schema model (`StructType`/`StructField`/`PrimitiveType`), column mapping (id and name modes), type widening, identity columns, Iceberg compatibility validation, deletion vectors (RoaringBitmap reader/writer with Base85 codec), row tracking, change data feed config.

- **`EngineeredWood.DeltaLake.Table`** — Arrow-based table API: `DeltaTable.OpenAsync` / `CreateAsync`, `WriteAsync` / `ReadAllAsync` / `ReadAtVersionAsync` / `ReadAtTimestampAsync`, `UpdateAsync` / `DeleteAsync`, `CompactAsync`, `VacuumAsync`. Also: identity column generation, partition splitting/path encoding, change data feed reader/writer, type widening on read, stats collection, deletion vector filtering on read.

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
  │    ├─ Row tracking column injection (if enabled)
  │    └─ ParquetFileWriter.WriteRowGroupAsync
  ├─ Build AddFile actions (Stats, BaseRowId, DefaultRowCommitVersion, etc.)
  ├─ TransactionLog.WriteCommitAsync (NDJSON, atomic temp + rename)
  └─ Auto-checkpoint at CheckpointInterval
```

### Decoding stats and partitions

`DeltaLiteralDecoder` (internal) converts `JsonElement` (from `AddFile.Stats`) and `string` (from `AddFile.PartitionValues`) to `LiteralValue` based on the Delta primitive type name. Falls back to high-precision decimal via `BigInteger` when values exceed `System.decimal`'s 28-29 digit range.

### Supported features

Reader v3 / Writer v7 with all named features supported (see README for the list). Iceberg compatibility (V1 and V2) is implemented as a writer constraint that ensures the Delta table's structure is convertible to Iceberg by an external tool — it does not produce Iceberg metadata directly.

---

## Iceberg (`EngineeredWood.Iceberg`)

Apache Iceberg metadata, manifest read/write, and scan planning. Format versions 1, 2, and 3 (including geometry/geography, variant, default values, row IDs).

### Components

- **Table metadata** (`TableMetadata.cs`) — JSON representation of Iceberg table state including snapshots, schemas, partition specs, sort orders.
- **Manifest layer** (`Manifest/`) — `DataFile`, `ManifestEntry`, `ManifestListEntry`. `ManifestIO` reads/writes Avro-encoded manifest files using `EngineeredWood.Avro`. Statistics on `DataFile` (`ColumnLowerBounds`, `ColumnUpperBounds`, `NullValueCounts`) use the shared `LiteralValue` struct.
- **Partition transforms** (`Transform.cs`) — Identity, Void, Bucket, Truncate, Year, Month, Day, Hour. Composed into `PartitionField` in `PartitionSpec`.
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
- **Delta Lake:** Two suites. `EngineeredWood.DeltaLake.Tests` covers the log layer (action serialization, snapshot reconstruction, checkpoints, deletion vectors, in-commit timestamps, etc.). `EngineeredWood.DeltaLake.Table.Tests` covers the table API end-to-end with temp-directory tables, including write/read round-trips, partitioning, Iceberg compatibility, identity columns, row tracking, change data feed, deletion vectors, and predicate pushdown.
- **Iceberg:** `EngineeredWood.Iceberg.Tests` covers schema/partition/snapshot updates, V3 features (geometry, variant, default values, row IDs), catalog operations, manifest serialization, and `TableScan` with predicate pruning.
- **Expressions:** `EngineeredWood.Expressions.Tests` covers `LiteralValue` (cross-type comparison, high-precision decimal, hashing), expression factories (constant folding, flattening), the binder (identity preservation, lenient mode), and the statistics evaluator (every predicate variant with synthetic stats including truncation edge cases). `EngineeredWood.Expressions.Arrow.Tests` covers the row evaluator with full SQL three-valued logic.

## Benchmarks

`EngineeredWood.Parquet.Benchmarks`, `EngineeredWood.Orc.Benchmarks`, `EngineeredWood.Avro.Benchmarks`, and `EngineeredWood.DeltaLake.Benchmarks` contain BenchmarkDotNet suites for reads, writes, and encodings.
