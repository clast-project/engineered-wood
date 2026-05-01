# EngineeredWood

A .NET library for reading and writing columnar file formats — **Apache Parquet**, **Apache ORC**, **Apache Avro**, and **Lance** — and table formats — **Lance dataset**, **Delta Lake**, and **Apache Iceberg** — as Apache Arrow `RecordBatch` objects.

## Highlights

- **Four formats, one Arrow surface.** Parquet, ORC, Avro, and Lance readers and writers all speak `Apache.Arrow.RecordBatch`; Delta Lake, Lance dataset, and Iceberg sit on top of them.
- **Predicate pushdown across formats.** A shared expression library (`EngineeredWood.Expressions`) drives row-group pruning in Parquet, file pruning in Delta Lake, scan planning in Iceberg, and predicate-based delete/update on Lance datasets from the same tree.
- **Table-format support.** Delta Lake Reader v3 / Writer v7 with deletion vectors, column mapping, type widening, change data feed, identity columns, row tracking, and V2 checkpoints. Lance datasets with Create / Append / Overwrite / Delete / Update / Compact / Vacuum and version + timestamp time travel. Iceberg v1/v2/v3 metadata with manifest read/write and partition-transform-aware scan planning.
- **Cloud-native I/O.** An offset-based I/O layer (instead of `Stream`) lets readers issue concurrent, coalesced range requests against local files or Azure Blob Storage.
- **Pure-managed compression.** Snappy, Zstd, and LZ4 via managed codecs; no native dependencies.
- **Multi-targeted.** Libraries build for `netstandard2.0`, `net8.0`, and `net10.0`.

## Why

There are two motivations for this project, and they're equally important:

**1. Better columnar file format libraries for .NET.** Existing .NET Parquet and ORC libraries were not designed around the access patterns that matter for cloud-native analytics — batched range reads, concurrent column chunk fetches, and zero-copy buffer management. EngineeredWood is built from the ground up with these patterns in mind, drawing inspiration from [arrow-rs](https://github.com/apache/arrow-rs).

**2. An experiment in agentic coding.** This project is being built collaboratively with an AI coding agent (Claude Code). Every file, test, and design decision has been produced through human-AI pair programming. It's a real-world test of how far agentic coding can go on a nontrivial systems library — not a toy demo, but a genuine attempt to build something useful while exploring a new way of writing software.

## Project structure

```
src/
  EngineeredWood.Core/                   Shared abstractions: I/O, compression, Arrow helpers
  EngineeredWood.Expressions/            Format-agnostic expression trees + statistics evaluator
  EngineeredWood.Expressions.Arrow/      Row-level expression evaluation against RecordBatch
  EngineeredWood.Parquet/                Parquet reader and writer
  EngineeredWood.Orc/                    ORC reader and writer
  EngineeredWood.Avro/                   Avro reader and writer
  EngineeredWood.Lance/                  Lance file reader and writer (v2.0 + v2.1 + v2.2)
  EngineeredWood.Lance.Table/            Lance dataset / table API (manifests, fragments, time travel)
  EngineeredWood.DeltaLake/              Delta Lake transaction log (low-level)
  EngineeredWood.DeltaLake.Table/        Delta Lake table API (high-level Arrow I/O)
  EngineeredWood.Iceberg/                Apache Iceberg metadata + scan planning
  EngineeredWood.Azure/                  Azure Blob Storage backends
test/                                    xUnit tests, BenchmarkDotNet suites, and a 92-file
                                         cross-tool Parquet compatibility harness
```

## Features — Parquet

### Reading

- Full footer and metadata parsing (custom Thrift Compact Protocol codec)
- Parallel column I/O with configurable concurrency strategies
- Column projection by name (including dotted paths for nested columns)
- Streaming via `IAsyncEnumerable<RecordBatch>` (`ReadAllAsync`)
- **Predicate pushdown**: row group pruning via column statistics, with optional Bloom filter probing for equality and `IN` predicates
- Three BYTE_ARRAY output modes: standard (32-bit offsets), view types (inline short strings), large offsets (64-bit, removes 2 GB limit)

### Writing

- Arrow `RecordBatch` → Parquet with parallel column encoding
- V2 data pages by default with type-aware encodings
- Analyze-before-write dictionary encoding (20% cardinality threshold)
- Auto-splitting of large batches into multiple row groups
- Per-column compression and encoding overrides
- Column statistics (min/max/null_count) with binary truncation

### Types

- **Flat columns**: all physical types (Boolean, Int32, Int64, Int96, Float, Double, ByteArray, FixedLenByteArray)
- **Nested columns**: Struct (optional/required), List (3-level standard, 2-level legacy, bare repeated), Map
- **Deeply nested**: list-of-list, map-of-map, list-of-map, etc.
- **Decimal**: INT32→Decimal32, INT64→Decimal64, FLBA→Decimal128/256 with big-endian↔little-endian conversion
- **Temporal**: Timestamp (millis/micros/nanos), Date, Time

### Encodings

| Encoding | Read | Write |
|---|---|---|
| PLAIN | yes | yes |
| RLE_DICTIONARY / PLAIN_DICTIONARY | yes | yes |
| DELTA_BINARY_PACKED | yes | yes |
| DELTA_LENGTH_BYTE_ARRAY | yes | yes |
| DELTA_BYTE_ARRAY | yes | yes |
| BYTE_STREAM_SPLIT | yes | yes |
| RLE (levels) | yes | yes |
| BIT_PACKED (deprecated, levels) | yes | — |

## Features — ORC

### Reading

- Full ORC file parsing (postscript, footer, stripe metadata via Protobuf)
- Selective column I/O with stream coalescing
- Column projection by name
- Row indexing support (10K row stride)
- Streaming via `IAsyncEnumerable<RecordBatch>`

### Writing

- Arrow `RecordBatch` → ORC with auto stripe management (64 MB default)
- RLE v2 encoding for integers, Dictionary v2 for strings
- Configurable string dictionary threshold (40K unique values default)
- Row index generation (10K row stride, configurable)
- Per-stripe and file-level column statistics

### Types

All 19 ORC types are supported for both reading and writing:
- **Integer**: Boolean, Byte, Short, Int, Long
- **Floating point**: Float, Double
- **String/Binary**: String, Varchar, Char, Binary
- **Temporal**: Date, Timestamp, TimestampInstant (UTC)
- **Decimal**: Decimal128 (configurable precision/scale)
- **Complex**: Struct, List, Map, Union

### Encodings

| Encoding | Read | Write |
|---|---|---|
| DIRECT | yes | yes |
| DIRECT_V2 (RLE v2) | yes | yes |
| DICTIONARY | yes | — |
| DICTIONARY_V2 | yes | yes |

## Features — Avro

### Reading

- Object Container File (OCF) reading with sync and async APIs
- Fluent builder API (`AvroReaderBuilder`)
- Schema evolution: field matching by name/alias, type promotion, default value insertion
- Field projection by index or skip-by-name
- Streaming via `IAsyncEnumerable<RecordBatch>`

### Writing

- Arrow `RecordBatch` → Avro OCF with codec selection
- Sync and async writers
- Explicit Avro schema or auto-inferred from Arrow schema

### Streaming (non-OCF)

- Push-based decoder for framed messages (`AvroDecoder`)
- Row-level encoder with per-message framing (`AvroEncoder`)
- Wire formats: Single Object Encoding (SOE), Confluent Schema Registry, Apicurio Registry, raw binary
- Schema fingerprinting: CRC-64-AVRO (Rabin), MD5, SHA-256
- `SchemaStore` for schema-by-fingerprint lookup

### Types

- **Primitives**: null, boolean, int, long, float, double, bytes, string
- **Named**: record (→ StructArray), enum (→ DictionaryArray), fixed (→ FixedSizeBinaryArray)
- **Complex**: array (→ ListArray), map (→ MapArray), union (nullable → nullable field, general → DenseUnionArray)
- **Logical types**: date, time-millis/micros, timestamp-millis/micros/nanos (UTC and local), decimal (bytes and fixed with precision/scale → Decimal128), uuid

### Compression

| Codec | Read | Write |
|---|---|---|
| null (uncompressed) | yes | yes |
| deflate | yes | yes |
| snappy (with CRC32C) | yes | yes |
| zstandard | yes | yes |
| lz4 | yes | yes |

## Features — Lance

EngineeredWood ships two Lance layers: a file-level reader/writer
(`EngineeredWood.Lance`) for individual `.lance` files and a dataset
API (`EngineeredWood.Lance.Table`) for manifests, fragments, and
transactional operations. Implementation is driven by the protobufs
and Rust source of [`lance-format/lance`](https://github.com/lance-format/lance)
plus cross-validation against [pylance](https://pypi.org/project/pylance/)-
produced files; many of the writer paths are tested via "we write,
pylance reads" round trips.

### Versions

- **v2.0** (footer bytes `(0, 3)` — pylance's pre-0.38 default — and `(2, 0)`)
- **v2.1** (footer bytes `(2, 1)` — pylance ≥ 0.38 default for new writes)
- **v2.2** (footer bytes `(2, 2)` — required for Map type; the file
  layout is otherwise identical to v2.1)

v0.1 and any future v2.3+ are rejected with explicit errors.

### File-level reading

- Public `LanceFileReader.OpenAsync(path)` parses the footer, GBO/CMO
  tables, `FileDescriptor`, and per-column metadata.
- `ReadColumnAsync(int fieldIndex)` returns the N-th top-level Arrow
  field as an `IArrowArray`. For nested fields it reads every physical
  column needed and assembles a single nested array.
- Optimistic 64 KiB tail read brings the footer + metadata + global
  buffer 0 in one I/O on cloud storage.

### File-level writing

- `LanceFileWriter.CreateAsync(path)` writes a v2.1 file containing one
  or more columns. Auto-bumps to v2.2 when a column requires it (Map).
- `WriteColumnAsync(name, IArrowArray)` for single-page columns;
  `WriteColumnAsync(name, IReadOnlyList<IArrowArray>)` for explicit
  multi-page leaf columns. Each call appends to the schema in order.
- Optional ZSTD compression on fixed-width primitive value buffers
  (`LanceCompressionScheme.Zstd` ctor parameter) wraps Flat in
  `General(ZSTD, Flat(N))` per chunk.
- Pylance cross-validation passes for every writer surface — see
  `LanceFileWriterTests.*_CrossValidatedAgainstPylance`.

### Dataset / table layer

`EngineeredWood.Lance.Table` wraps the file writer in the Lance dataset
directory layout (`data/`, `_versions/`, `_transactions/`, `_deletions/`)
and exposes the read side via `LanceTable`.

#### Reading

- `LanceTable.OpenAsync(path)` opens the latest manifest version;
  overloads accept `version: ulong` or `asOf: DateTimeOffset` for
  time travel.
- `IAsyncEnumerable<RecordBatch>` streaming, column projection,
  deletion-mask filtering applied during read.
- **Predicate pushdown**: filter passed to `ReadAsync` evaluates against
  each fragment via `ArrowRowEvaluator`. For indexed columns,
  fragment-level pruning skips fragments whose B-tree or bitmap
  index proves they can't match.
- **Index reads**: B-tree and bitmap secondary index files (read-only —
  we don't write indices yet).

#### Writing

- `LanceDatasetWriter.CreateAsync(path)` — fresh dataset; refuses to
  clobber an existing one.
- `LanceDatasetWriter.AppendAsync(path)` — adds new fragments alongside
  the existing ones; schema must match.
- `LanceDatasetWriter.OverwriteAsync(path)` — replaces dataset contents;
  schema can change.
- `NewFragmentAsync()` between batches splits a single transaction
  into multiple fragments.
- `DeleteRowsAsync(path, perFragmentRowOffsets)` — emits deletion files
  (Arrow-IPC `{ row_id: uint32 }`) and bumps the manifest.
- `DeleteAsync(path, predicate)` — predicate-based delete layered on
  top of `DeleteRowsAsync`.
- `UpdateAsync(path, predicate, assignments)` — delete-and-rewrite via
  `ArrowRowEvaluator.EvaluateExpression`; matching rows are tombstoned
  in their source fragments and reappear in a fresh appended fragment
  with the assigned columns replaced.
- `CompactAsync(path)` — repacks fragments carrying deletion files into
  a single fresh fragment of survivors.
- `VacuumAsync(path, options)` — removes data, manifest, transaction,
  and deletion files no longer referenced by any retained version
  (`RetainVersions`, `DryRun`).

Every commit path stamps `manifest.timestamp` for time travel.

### Type coverage

| Arrow type | Read | Write | Notes |
|---|---|---|---|
| Int8/16/32/64, UInt8/16/32/64, Float, Double | yes | yes | |
| Bool | yes | yes | bit-packed `Flat(1)`, single-chunk only on the writer |
| String, Binary | yes | yes | |
| FixedSizeBinary | yes | yes | |
| Decimal128, Decimal256 | yes | yes | |
| Date32, Date64, Time32, Time64, Timestamp, Duration | yes | yes | |
| FixedSizeList | yes | yes | writer: primitive inner only; inner-element nulls (`has_validity=true`) reader-only |
| List, LargeList | yes | yes | writer: primitive / string / binary inner; inner-element nulls supported |
| Struct (recursive) | yes | yes | writer: nested struct, FSL, list children all supported |
| Map (v2.2) | yes | yes | |
| HalfFloat (Float16), LargeString, LargeBinary, Union | not yet | not yet | |

### v2.0 encoding coverage

| Encoding | Status |
|---|---|
| `Flat` | yes |
| `Nullable` (NoNull / SomeNull / AllNull) | yes |
| `Binary` (variable strings/binary, with `null_adjustment` nulls) | yes |
| `Constant` | yes |
| `FixedSizeBinary` | yes |
| `Bitpacked` (simple LSB, signed/unsigned) | yes |
| `Dictionary` (1-based indices with 0 = null) | yes |
| `BitpackedForNonNeg` (Fastlanes, via `Clast.FastLanes`) | yes |
| `FixedSizeList` | yes |
| `SimpleStruct` (multi-column shred) | yes |
| `List` / `LargeList` (with `null_offset_adjustment`) | yes |
| `PackedStruct`, `Fsst`, miniblock-only encodings (`Rle`, `InlineBitpacking`, `OutOfLineBitpacking`, `GeneralMiniBlock`, `ByteStreamSplit`, `Block`) | reader: not yet |

### v2.1 / v2.2 encoding coverage

| Layout / Encoding | Read | Write |
|---|---|---|
| `MiniBlockLayout` with `Flat` values, primitive leaves, single layer (`ALL_VALID_ITEM` / `NULLABLE_ITEM`) | yes | yes |
| `MiniBlockLayout` with `Variable` (strings/binary, u32 offsets) | yes | yes |
| `MiniBlockLayout` with `Fsst` (FSST-compressed strings/binary) | yes | not yet (writer emits `Variable`) |
| `MiniBlockLayout` with `InlineBitpacking` / `OutOfLineBitpacking` (Fastlanes) | yes | not yet (writer emits `Flat`) |
| `MiniBlockLayout` with `Dictionary` (layout-level dictionary) | yes | not yet |
| `MiniBlockLayout` with `General(ZSTD, Flat(N))` (per-chunk ZSTD wrap) | yes | yes (fixed-width primitive scope) |
| `MiniBlockLayout` with `FixedSizeList` value-compression (FSL row encoding) | yes | yes (primitive inner; inner-null reader-only) |
| `MiniBlockLayout` with rep/def cascades (every `RepDefLayer` combo, multi-list, struct-of-list) | yes | yes |
| `MiniBlockLayout` with bool (`bits_per_value=1`) | yes | yes |
| Multi-chunk pages with repetition index (`repetition_index_depth=1`) | yes | yes (lists) |
| Multi-page columns | yes | yes (leaf shapes) |
| `FullZipLayout` fixed-width with `Flat` / `FixedSizeList(Flat)` | yes | not yet |
| `FullZipLayout` variable-width / nested-leaf cascades | yes | not yet |
| `ConstantLayout` (all-null pages) | yes | not yet |
| `BlobLayout` (two-level external blob storage) | not yet | not yet |
| `Rle`, `ByteStreamSplit`, `PackedStruct` | not yet | not yet |

### Storage abstraction

Reuses the existing offset-based `IRandomAccessFile` — Lance's reader
needs `size()` + `get_range()` + coalesced `get_ranges()`, all of which
the Core API already provides. Priority-aware scheduling and HTTP
multi-range requests are perf optimizations not currently implemented.

### Bit-packing dependency

Lance's v2.0 `BitpackedForNonNeg` and v2.1 `InlineBitpacking` use the
fastlanes 1024-element packing format. `EngineeredWood.Lance` consumes
the [`Clast.FastLanes`](https://www.nuget.org/packages/Clast.FastLanes/)
NuGet package (zero Lance/Arrow dependencies; bit-for-bit compatible
with the Rust `lance_bitpacking` crate).

### Future work

See [`doc/lance-future-work.md`](doc/lance-future-work.md) for the
prioritised list of remaining gaps (encoding-level features the writer
doesn't yet emit, Arrow types not yet covered, dataset-level features
like fragment-level concurrency control and multi-fragment compaction
targets).

## Features — Delta Lake

EngineeredWood ships two layers: a low-level transaction log API
(`EngineeredWood.DeltaLake`) for metadata/stats consumers and a high-level
table API (`EngineeredWood.DeltaLake.Table`) for Arrow-based read/write.
Reader v3 / Writer v7 with full named feature support.

### Reading

- Snapshot-based read at current version, a specific version, or a timestamp (time travel)
- `IAsyncEnumerable<RecordBatch>` streaming, column projection, partition column re-materialization
- **Predicate pushdown**: file-level pruning via partition values and `AddFile` statistics in a single evaluator pass
- Deletion vector filtering (RoaringBitmap, inline + file-based)
- Type widening on read (int/float/date/decimal widenings via `delta.typeChanges` metadata)
- Column mapping (id and name modes); row tracking; in-commit timestamps

### Writing

- Append and overwrite; partitioned writes; identity column generation
- Auto-checkpoint (V1 Parquet and V2 JSON+sidecar formats)
- Compaction and vacuum; log compaction
- Change data feed (insert / delete / update pre/post-image)
- Iceberg compatibility (V1 and V2): partition column materialization,
  schema validation, stats enforcement so an external converter can
  produce valid Iceberg metadata

### Supported reader features

`columnMapping`, `deletionVectors`, `timestampNtz`, `typeWidening`, `v2Checkpoint`, `vacuumProtocolCheck`

### Supported writer features

`changeDataFeed`, `columnMapping`, `deletionVectors`, `domainMetadata`,
`icebergCompatV1`, `icebergCompatV2`, `identityColumns`, `inCommitTimestamp`,
`rowTracking`, `timestampNtz`, `typeWidening`, `v2Checkpoint`, `vacuumProtocolCheck`

## Features — Iceberg

`EngineeredWood.Iceberg` provides Apache Iceberg metadata parsing,
manifest reading/writing, table metadata (v1, v2, v3), partition specs,
sort orders, snapshots, and scan planning. It is metadata-only — data files
are read with the Parquet/ORC/Avro readers in this same library.

- Table metadata in JSON (v1, v2, v3 including geometry/geography, variant, default values, row IDs)
- Manifest file/list read + write (Avro-encoded)
- Partition transforms (identity, void, bucket, truncate, year, month, day, hour)
- Catalog interfaces with file-system and in-memory implementations
- `TableScan` with predicate-based file pruning via the shared
  `EngineeredWood.Expressions` library

## Features — Expressions

A format-agnostic expression library used by Parquet, Delta Lake, and Iceberg
for predicate pushdown, and available to consumers for row-level evaluation.

- **`EngineeredWood.Expressions`** (no Arrow dependency):
  - Expression and predicate trees (`UnboundReference`, `BoundReference`,
    `LiteralExpression`, `FunctionCall`, comparison/unary/set predicates,
    boolean combinators)
  - `LiteralValue` struct with 17 typed kinds, cross-type numeric promotion,
    and high-precision decimal via `BigInteger`
  - `ExpressionBinder` for resolving column names to stable IDs against a schema
  - `StatisticsEvaluator` with three-valued logic (AlwaysTrue / AlwaysFalse / Unknown)
    over a generic `IStatisticsAccessor<TStats>` adapter
- **`EngineeredWood.Expressions.Arrow`** (depends on Apache.Arrow):
  - `ArrowRowEvaluator` walks an expression tree against a `RecordBatch`,
    producing a `BooleanArray` for predicates with full SQL three-valued
    null semantics
  - `IFunctionRegistry` for pluggable function dispatch (date/time, string,
    cast, etc. — registry implementations live with the parser that produces them)

See [`doc/predicate-pushdown-design.md`](doc/predicate-pushdown-design.md)
for the architecture and remaining phases.

## Shared infrastructure

### I/O

EngineeredWood uses an offset-based I/O layer instead of `Stream`. A single
`Stream` position is a poor fit for columnar layouts, where readers fetch many
disjoint byte ranges — each "seek + read" on cloud storage is a separate
HTTP request. The offset-based interfaces support concurrent reads with no
shared cursor, batched range requests, and pooled buffers.

| Backend | Read | Write |
|---|---|---|
| Local files | `LocalRandomAccessFile` | `LocalSequentialFile` |
| Azure Blob Storage | `AzureBlobRandomAccessFile` | `AzureBlobSequentialFile` |

`CoalescingFileReader` is a decorator that merges nearby byte ranges to reduce I/O round trips — particularly useful on cloud storage.

### Compression

| Codec | Library | Parquet | ORC | Avro |
|---|---|---|---|---|
| Snappy | [Snappier](https://github.com/brantburnett/Snappier) — pure managed | yes (default) | yes | yes |
| Zstd | [ZstdSharp](https://github.com/oleg-st/ZstdSharp) — pure managed | yes | yes (default) | yes |
| LZ4 / LZ4_RAW | [K4os.Compression.LZ4](https://github.com/MiloszKrajewski/K4os.Compression.LZ4) | yes | yes | yes |
| Gzip / Zlib | System.IO.Compression | yes | yes | — |
| Brotli | System.IO.Compression | yes | — | — |
| Deflate | System.IO.Compression | — | yes | yes |
| Uncompressed | — | yes | yes | yes |

## Usage

### Parquet — Reading

```csharp
await using var file = new LocalRandomAccessFile("data.parquet");
await using var reader = new ParquetFileReader(file);

// Read all row groups
await foreach (var batch in reader.ReadAllAsync())
{
    // batch is an Apache.Arrow.RecordBatch
}

// Or read a specific row group with column projection
var batch = await reader.ReadRowGroupAsync(0, columnNames: ["id", "name"]);
```

### Parquet — Writing

```csharp
await using var file = new LocalSequentialFile("output.parquet");
await using var writer = new ParquetFileWriter(file, options: new ParquetWriteOptions
{
    Compression = CompressionCodec.Zstd,
    DataPageVersion = DataPageVersion.V2,
});

await writer.WriteRowGroupAsync(recordBatch);
await writer.CloseAsync();
```

### Parquet — Predicate pushdown

```csharp
using Ex = EngineeredWood.Expressions.Expressions;

await using var file = new LocalRandomAccessFile("data.parquet");
await using var reader = new ParquetFileReader(file, ownsFile: false,
    new ParquetReadOptions
    {
        // Skip row groups whose statistics prove no rows match.
        Filter = Ex.And(
            Ex.GreaterThanOrEqual("event_count", 100L),
            Ex.Equal("region", "us")),

        // Optional: also probe Bloom filters for Equal/IN predicates
        // (extra I/O per candidate row group).
        FilterUseBloomFilters = true,
    });

await foreach (var batch in reader.ReadAllAsync())
{
    // Reader does file/row-group level pruning only — no row-level filtering.
}
```

### ORC — Reading

```csharp
await using var reader = await OrcReader.OpenAsync("data.orc");

var rowReader = reader.CreateRowReader();
await foreach (var batch in rowReader)
{
    // batch is an Apache.Arrow.RecordBatch
}
```

### ORC — Writing

```csharp
await using var writer = OrcWriter.Create("output.orc", arrowSchema, new OrcWriterOptions
{
    Compression = CompressionKind.Zstd,
});

await writer.WriteBatchAsync(recordBatch);
await writer.CloseAsync();
```

### Avro — Reading

```csharp
using var stream = File.OpenRead("data.avro");
using var reader = new AvroReaderBuilder()
    .WithBatchSize(4096)
    .Build(stream);

foreach (var batch in reader)
{
    // batch is an Apache.Arrow.RecordBatch
}
```

### Avro — Writing

```csharp
using var stream = File.Create("output.avro");
using var writer = new AvroWriterBuilder(arrowSchema)
    .WithCompression(AvroCodec.Snappy)
    .Build(stream);

writer.Write(recordBatch);
writer.Finish();
```

### Lance — File-level reading and writing

```csharp
using EngineeredWood.Lance;

// Write a single Lance file with optional ZSTD on fixed-width columns.
await using (var writer = await LanceFileWriter.CreateAsync(
    "data.lance", compression: LanceCompressionScheme.Zstd))
{
    await writer.WriteColumnAsync("id", new[] { 1, 2, 3, 4, 5 });
    await writer.WriteColumnAsync("name", stringArray);
    await writer.FinishAsync();
}

// Read it back.
await using var reader = await LanceFileReader.OpenAsync("data.lance");
var idArr = (Apache.Arrow.Int32Array)await reader.ReadColumnAsync(0);
```

### Lance — Dataset / table layer

```csharp
using EngineeredWood.Lance.Table;
using Ex = EngineeredWood.Expressions.Expressions;

// Create a fresh dataset with two fragments in one transaction.
await using (var ds = await LanceDatasetWriter.CreateAsync("/path/to/dataset"))
{
    await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
    await ds.NewFragmentAsync();
    await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 4, 5, 6 });
    await ds.FinishAsync();
}

// Append more data later.
await using (var ds = await LanceDatasetWriter.AppendAsync("/path/to/dataset"))
{
    await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 7, 8 });
    await ds.FinishAsync();
}

// Predicate-based delete + update.
await LanceDatasetWriter.DeleteAsync(
    "/path/to/dataset", Ex.LessThan("x", LiteralValue.Of(3)));

await LanceDatasetWriter.UpdateAsync(
    "/path/to/dataset",
    predicate: Ex.GreaterThanOrEqual("x", LiteralValue.Of(7)),
    assignments: new Dictionary<string, Expression>
    {
        ["x"] = new LiteralExpression(LiteralValue.Of(0)),
    });

// Maintenance.
await LanceDatasetWriter.CompactAsync("/path/to/dataset");
await LanceDatasetWriter.VacuumAsync(
    "/path/to/dataset", new LanceVacuumOptions { RetainVersions = 1 });

// Read latest, by version, or as-of a timestamp.
await using var latest = await LanceTable.OpenAsync("/path/to/dataset");
await using var v3     = await LanceTable.OpenAsync("/path/to/dataset", version: 3);
await using var asOf   = await LanceTable.OpenAsync(
    "/path/to/dataset", asOf: DateTimeOffset.UtcNow.AddHours(-1));

await foreach (var batch in latest.ReadAsync(
    columns: ["x"], filter: Ex.GreaterThan("x", LiteralValue.Of(0))))
{
    // ...
}
```

### Delta Lake — Reading and writing

```csharp
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

var fs = new LocalTableFileSystem("/path/to/table");
await using var table = await DeltaTable.OpenAsync(fs);

// Append data
await table.WriteAsync([recordBatch]);

// Read at the current version with column projection and predicate pushdown
await foreach (var batch in table.ReadAllAsync(
    columns: ["id", "value"],
    filter: Ex.And(
        Ex.Equal("region", "us"),       // partition prune
        Ex.GreaterThan("id", 1000L))))  // file-level stats prune
{
    // ...
}

// Time travel
await foreach (var batch in table.ReadAtVersionAsync(version: 5)) { /* ... */ }
```

### Iceberg — Scan planning

```csharp
using EngineeredWood.Iceberg.Expressions;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

var scan = new TableScan(metadata, fileSystem)
    .Filter(Ex.Equal("region", "us"))
    .Filter(Ex.GreaterThan("event_count", 100L));

var result = await scan.PlanFilesAsync();
// result.DataFiles contains files that may match — read them with the
// Parquet/ORC/Avro readers in this same library.
```

## Building

```
dotnet build
dotnet test
```

Libraries multi-target `netstandard2.0`, `net8.0`, and `net10.0`. Tests and
benchmarks require .NET 8 or .NET 10 (some also run on `net472`).

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed guide to the source code, implementation choices, and internal structure.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
