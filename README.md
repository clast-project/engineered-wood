# EngineeredWood

A pure-managed .NET library for columnar file formats (**Apache Parquet**, **Apache ORC**, **Apache Avro**, **Lance**, **Vortex**) and table formats (**Delta Lake**, **Apache Iceberg**, **Lance datasets**), all read and written as Apache Arrow `RecordBatch`es.

> **Status: preliminary (0.4.0).** EngineeredWood is pre-1.0. **Every public API may change without notice
> or a deprecation cycle until 1.0.0**, so pin an exact version. Feedback on the API is welcome.
>
> **The main breaking change in 0.4.0:** format writers publish a file only when it is closed (`CloseAsync` / `FinishAsync`),
> and a streamed write publishes only on `ISequentialFile.CompleteAsync`. Disposing a writer without closing it
> abandons the file. A custom `ISequentialFile` must implement `CompleteAsync`, and a custom
> `ITableFileSystem` must implement `PathConstraints`. The package release notes list the rest.

## Maturity

The packages are not equally mature. Parquet, Delta Lake and the packages they depend on get most of the
attention and testing. The others work, but are tested less and have known gaps.

| Maturity | Packages | What that means |
|---|---|---|
| **Most mature** | `Core`, `Expressions`, `Expressions.Arrow`, `Parquet`, `DeltaLake`, `DeltaLake.Table`, `Azure`, `Gcs`, `Aws` | Tested against other implementations in CI: pyarrow, arrow-rs and DuckDB for Parquet; PySpark and delta-rs for Delta; cloud emulators for the storage backends. Bugs found there get fixed first. |
| **Functional** | `Orc`, `Avro` | Read and write their formats and are cross-validated against pyarrow (ORC) and fastavro (Avro), with less coverage and some known gaps. |
| **Experimental** | `Lance`, `Lance.Table`, `Vortex`, `Iceberg` | Useful for reading and experimenting. Expect gaps and format-version churn. **Iceberg is not a conformant writer.** |

All package names begin with `EngineeredWood.`. [doc/known-issues.md](https://github.com/clast-project/engineered-wood/blob/main/doc/known-issues.md) lists known gaps per package.

## Highlights

- **One Arrow surface.** Every format reads and writes `Apache.Arrow.RecordBatch`; the table formats sit on top of the file formats.
- **Delta Lake with DML.** Reader v3 / Writer v7: DELETE, UPDATE, MERGE, RESTORE, replaceWhere, schema evolution, deletion vectors, column mapping, type widening, change data feed, row tracking, CHECK constraints and generated columns, V2 checkpoints, and optimistic concurrency.
- **Predicate pushdown.** One shared predicate type drives Parquet row-group, page and dictionary pruning, Delta file pruning, Iceberg scan planning, and Vortex zone pruning. A Spark SQL expression parser (`SparkSqlParser`) produces those predicates from text.
- **Cloud-native I/O.** An offset-based I/O layer (not `Stream`) issues concurrent, coalesced range reads against local files, Azure Blob Storage, Google Cloud Storage and Amazon S3. Commits are a single conditional create on every backend.
- **No native dependencies.** Snappy, Zstd and LZ4 use pure-managed codecs.
- **Multi-targeted and strongly named.** `netstandard2.0`, `net8.0` and `net10.0`; signed with the clast-project key (public key token `0b0eddb1936076d9`).

## Why

Two goals, equally important:

1. **Better columnar libraries for .NET**, designed around what cloud analytics needs: batched range reads, concurrent column-chunk fetches, and careful buffer management. The design draws on [arrow-rs](https://github.com/apache/arrow-rs).
2. **An experiment in agentic coding.** The library is built by a human working with an AI coding agent (Claude Code), to see how far that goes on a nontrivial systems library.

## Packages

| Package | Contents | Depends on |
|---|---|---|
| `EngineeredWood.Core` | Offset-based I/O, local file system, compression, memory budget, Arrow helpers | — |
| `EngineeredWood.Expressions` | Expression/predicate trees, statistics evaluator, Spark SQL parser | — |
| `EngineeredWood.Expressions.Arrow` | Row-level evaluation against a `RecordBatch` | Core, Expressions |
| `EngineeredWood.Parquet` | Parquet reader and writer | Core, Expressions |
| `EngineeredWood.DeltaLake` | Delta transaction log: actions, snapshots, checkpoints, commits | Core, Parquet, Expressions |
| `EngineeredWood.DeltaLake.Table` | Delta table API: Arrow read/write, DML, maintenance | DeltaLake, Parquet, Expressions.Arrow |
| `EngineeredWood.Azure` / `.Gcs` / `.Aws` | Azure Blob, GCS and S3 backends | Core |
| `EngineeredWood.Orc` | ORC reader and writer | Core |
| `EngineeredWood.Avro` | Avro OCF and streaming (SOE, Confluent, Apicurio) reader and writer | Core |
| `EngineeredWood.Lance` | Lance file reader and writer (v2.0–v2.2) | Core |
| `EngineeredWood.Lance.Table` | Lance datasets: manifests, fragments, time travel, delete/update | Lance, Expressions.Arrow |
| `EngineeredWood.Vortex` | Vortex reader, writer and zone pruning | Core, Expressions |
| `EngineeredWood.Iceberg` | Iceberg metadata (v1–v3), manifests, scan planning (metadata only) | Core, Avro, Expressions |

## Parquet

**Reading:** parallel column I/O; projection by name, including dotted nested paths; streaming via
`IAsyncEnumerable<RecordBatch>`; all physical types and nested Struct/List/Map at any depth; Decimal32–256;
INT96 as a timestamp; VARIANT, shredded or not. String columns can come back with 32-bit offsets, as view
types, or with 64-bit offsets.

**Pruning** (`ParquetReadOptions.Filter`): row groups by statistics; pages by the page index
(`FilterUsePageIndex`, on by default); row groups by dictionary pages (`FilterUseDictionaries`, on by
default); Bloom filters for `=` / `IN` (`FilterUseBloomFilters`, off by default because it costs extra I/O).
Pruning also works on VARIANT paths. The reader does not filter rows: it skips data that cannot match.

**Writing:** parallel column encoding; V2 data pages; dictionary encoding chosen per column after
inspecting the data; statistics, page indexes and the Arrow schema written by default; optional Bloom filters
(`BloomFilterColumns`); per-column compression and encoding overrides. Large batches are split into row
groups automatically (`RowGroupMaxRows`, default 1,000,000). `BufferedParquetWriter` accepts batches of any
size.

| Encoding | Read | Write |
|---|---|---|
| PLAIN, RLE_DICTIONARY / PLAIN_DICTIONARY, RLE | yes | yes |
| DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY | yes | yes |
| BYTE_STREAM_SPLIT | yes | opt-in |
| BIT_PACKED (deprecated) | yes | — |
| ALP (spec "Preview") | yes | opt-in |
| PFOR, FSST (unratified proposals) | yes | opt-in, experimental |

- **BYTE_STREAM_SPLIT** is opt-in because Spark's vectorized reader cannot decode it (SPARK-37975).
- **ALP** is in parquet-format 2.14.0, but few readers decode it yet (arrow-rs ≥ 60.0.0), and the spec
  asks writers to make it opt-in.
- Turn either on with `ParquetWriteOptions.FloatingPointEncoding`.

### Experimental features

Features whose wire format may still change are marked `[Experimental]`: using one is a compile error until
you suppress the diagnostic. Suppress it with a narrow `#pragma warning disable` at the use site.

| ID | Feature | Risk |
|---|---|---|
| `EWPARQUET0002` | `ParquetWriteOptions.OmitPathInSchema` | **No other implementation can read the file.** pyarrow, ParquetSharp and delta-kernel-rs report it as corrupt |
| `EWPARQUET0003` | FSST (`ByteArrayEncoding.Fsst` / `Fsst16`) | Unratified, and written as encoding **12** where the proposal says 10 ([doc/parquet-fsst.md](https://github.com/clast-project/engineered-wood/blob/main/doc/parquet-fsst.md)) |
| `EWPARQUET0004` | Extended-precision timestamps (`FIXED_LEN_BYTE_ARRAY(12)`) | Unratified, and the byte order is still undecided upstream ([doc/parquet-extended-precision-timestamps.md](https://github.com/clast-project/engineered-wood/blob/main/doc/parquet-extended-precision-timestamps.md)) |
| `EWPARQUET0005` | PFOR (`IntegerEncoding.Pfor`) | Unratified; the wire format may change ([doc/parquet-pfor.md](https://github.com/clast-project/engineered-wood/blob/main/doc/parquet-pfor.md)) |

PFOR and FSST fall back to the default encoding for any page or chunk they don't shrink.
`EWPARQUET0001` (ALP) is retired, and existing suppressions of it can be deleted.

## Delta Lake

Two layers: `EngineeredWood.DeltaLake` is the transaction log, for hosts that bring their own data plane
(see [doc/embedding-host-guide.md](https://github.com/clast-project/engineered-wood/blob/main/doc/embedding-host-guide.md)). `EngineeredWood.DeltaLake.Table` is the
Arrow table API. EngineeredWood implements Reader v3 / Writer v7. It refuses a table that requires a
feature it doesn't implement, rather than misreading it.

- **Read:** latest version, a given version, or a timestamp; projection; partition pruning and file
  skipping on statistics, pushed down to Parquet row groups and pages; deletion vectors; column mapping;
  type widening; VARIANT (shredded or not); row ids (`_metadata.row_id`); change data feed.
- **Write:** append, overwrite, `OverwritePartitionsAsync`, `DynamicOverwriteAsync`, `OverwriteWhereAsync`
  (replaceWhere); `CreateOrReplaceAsync` in one commit; partitioned writes; identity columns; generated
  columns computed; CHECK constraints and NOT NULL enforced; `mergeSchema`, including inside arrays and maps.
- **DML:** `DeleteAsync`, `UpdateAsync` and `MergeAsync` (several clauses of each kind, SET/VALUES
  expressions, schema evolution), using deletion vectors when the table enables them and copy-on-write
  otherwise. Commits record Spark-style `operationParameters` and `operationMetrics`.
- **Table management:** `RestoreAsync`; `AddConstraintAsync` / `DropConstraintAsync`; add, rename and
  drop columns and nested fields; compaction; vacuum; checkpoints (V1 and V2) and log compaction; version
  checksums.
- **Concurrency:** optimistic concurrency with conflict detection and automatic rebase where it is safe
  ([doc/delta-concurrency.md](https://github.com/clast-project/engineered-wood/blob/main/doc/delta-concurrency.md)); `DeltaTransaction` stages several operations
  into one commit.
- **IcebergCompat V1–V3:** enforces what an external converter needs to produce valid Iceberg metadata.
  EngineeredWood doesn't write the Iceberg metadata itself.

**Reader features:** `columnMapping`, `deletionVectors`, `timestampNtz`, `typeWidening`, `v2Checkpoint`,
`vacuumProtocolCheck`, `variantType`.

**Writer features:** `appendOnly`, `changeDataFeed`, `checkConstraints`, `clustering`, `columnMapping`,
`deletionVectors`, `domainMetadata`, `generatedColumns`, `icebergCompatV1`–`V3`, `identityColumns`,
`inCommitTimestamp`, `invariants`, `rowTracking`, `timestampNtz`, `typeWidening`, `v2Checkpoint`,
`vacuumProtocolCheck`, `variantType`.

`clustering` is supported for interop only: clustered tables can be appended to and modified, but
EngineeredWood doesn't cluster data. Constraint and generated-column expressions are evaluated with Spark's
semantics. A write to a table whose expression EngineeredWood cannot parse or evaluate is refused.

## ORC and Avro

**ORC:** all 19 ORC types, read and write; RLE v2 and dictionary v2 encodings; column projection; stripe
and file statistics; row indexes. Timestamps and decimals follow the spec, and files written by 0.1 are
still read as written. There is no RLE v1 writer and no public predicate pushdown.

**Avro:** OCF reading and writing (sync and async), with schema evolution by name, alias, type promotion
and defaults. Streaming encode and decode for Single Object Encoding, Confluent, Apicurio and raw binary,
with schema fingerprints and a `SchemaStore`. Codecs: null, deflate, snappy, zstandard, lz4.

## Lance, Vortex and Iceberg (experimental)

- **Lance:** reads and writes Lance files v2.0–v2.2. Datasets support create, append, overwrite,
  predicate delete and update, compact, vacuum, and time travel by version or timestamp. B-tree and
  bitmap indexes are read (not written) and used to prune fragments. Coverage is in
  [doc/lance.md](https://github.com/clast-project/engineered-wood/blob/main/doc/lance.md); gaps are in [doc/lance-future-work.md](https://github.com/clast-project/engineered-wood/blob/main/doc/lance-future-work.md).
- **Vortex:** reads and writes Vortex files, cross-validated against Rust `vortex-array` 0.86 in both
  directions. Supports projection, row-range slices and zone-stat pruning. Coverage is in
  [doc/vortex.md](https://github.com/clast-project/engineered-wood/blob/main/doc/vortex.md).
- **Iceberg:** parses table metadata (v1–v3), reads and writes manifests, and plans scans with predicate
  pruning. It refuses (`NotSupportedException`) any manifest it didn't write itself rather than misreading
  it, so for now scans work only on tables EngineeredWood wrote. The manifests it writes lack bounds and
  partition tuples, and partition transforms can't yet be applied to values. **Do not use it to write
  tables other engines will read.**

## Usage

### Parquet

```csharp
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

// Read, skipping row groups and pages that cannot match.
await using var reader = new ParquetFileReader(
    new LocalRandomAccessFile("data.parquet"),
    options: new ParquetReadOptions
    {
        Filter = Ex.And(Ex.GreaterThanOrEqual("event_count", 100L), Ex.Equal("region", "us")),
    });

await foreach (var batch in reader.ReadAllAsync())
{
    // batch is an Apache.Arrow.RecordBatch
}

// Write. CloseAsync writes the footer and publishes the file.
await using var writer = new ParquetFileWriter(
    new LocalSequentialFile("output.parquet"),
    options: new ParquetWriteOptions { Compression = CompressionCodec.Zstd });
await writer.WriteRowGroupAsync(recordBatch);
await writer.CloseAsync();
```

### Delta Lake

```csharp
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem("/path/to/table"));

await table.WriteAsync([recordBatch]);

await foreach (var batch in table.ReadAllAsync(
    columns: ["id", "value"],
    filter: SparkSqlParser.ParsePredicate("region = 'us' AND id > 1000")))
{
    // Files and row groups that cannot match are skipped; rows are not filtered.
}

await table.DeleteAsync(SparkSqlParser.ParsePredicate("id < 100"));

await table.MergeAsync([sourceBatch], new MergeOptions
{
    Keys = ["id"],
    WhenMatched = [MergeAction.Update],
    WhenNotMatched = [MergeAction.Insert],
});

await foreach (var batch in table.ReadAtVersionAsync(version: 5)) { /* time travel */ }
```

### Cloud storage

Pass a cloud file handle wherever a local one is accepted, or a cloud `ITableFileSystem` to a table format.
S3 is shown here; Azure (`AzureBlobRandomAccessFile`, `AzureTableFileSystem`) and GCS
(`GcsRandomAccessFile`, `GcsTableFileSystem`) work the same way.

```csharp
using Amazon.S3;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Aws;
using EngineeredWood.Parquet;

var s3 = new AmazonS3Client();

await using var reader = new ParquetFileReader(
    new S3RandomAccessFile(s3, "my-bucket", "data/events.parquet"));

await using var table = await DeltaTable.OpenAsync(
    new S3TableFileSystem(s3, "my-bucket", rootPath: "tables/events"));
```

### Other formats

```csharp
// ORC
await using var orc = await OrcReader.OpenAsync("data.orc");
await foreach (var batch in orc.CreateRowReader()) { }

await using var orcWriter = OrcWriter.Create("output.orc", schema);
await orcWriter.WriteBatchAsync(recordBatch);
await orcWriter.CloseAsync();

// Avro
using var avro = new AvroReaderBuilder().Build(File.OpenRead("data.avro"));
foreach (var batch in avro) { }

// Lance dataset
await using var lance = await LanceTable.OpenAsync("/path/to/dataset");
await foreach (var batch in lance.ReadAsync()) { }

// Vortex
await using var vortex = await VortexFileReader.OpenAsync("data.vortex");
await foreach (var batch in vortex.ReadAllAsync()) { }
```

## Building

```
dotnet build
dotnet test
```

Tests need .NET 8 or 10, and some also run on `net472`. [doc/running-tests.md](https://github.com/clast-project/engineered-wood/blob/main/doc/running-tests.md) covers
the interop tiers (PySpark, delta-rs, DuckDB, cloud emulators). [ARCHITECTURE.md](https://github.com/clast-project/engineered-wood/blob/main/ARCHITECTURE.md) is a
guide to the source. [doc/README.md](https://github.com/clast-project/engineered-wood/blob/main/doc/README.md) indexes the design notes.

## Acknowledgements

- [**arrow-rs**](https://github.com/apache/arrow-rs): the model for the offset-based, range-oriented I/O and
  columnar buffer management.
- [**hardwood**](https://github.com/hardwood-hq/hardwood): its "few dependencies, no native code" approach
  to Parquet inspired EngineeredWood's.

Each format is built from its public specification and cross-validated against that ecosystem's reference
tools: pyarrow, arrow-rs, DuckDB, delta-rs, PySpark, fastavro, pylance and `vortex-array`.

## License

[Apache License, Version 2.0](https://github.com/clast-project/engineered-wood/blob/main/LICENSE).
