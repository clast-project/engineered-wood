# Lance File Format Reader Design

**Status:** Reader landed end-to-end on branch `Lance` (Phases 1–11). The
companion `EngineeredWood.Lance.Table` package adds Lance dataset support
(manifest read, projection, row-level filter, deletion files, time travel)
on top of the file reader. Writer (Phase 13) is still out of scope; a
priority-aware I/O scheduler (Phase 12) is deferred pending a workload
that demands it.

## Overview

Lance is a columnar file format built by LanceDB, designed from the start
for object storage and for random-access workloads (feature stores,
embeddings, ML training reads). Like Parquet and ORC it is columnar with
a protobuf metadata footer, but its page layout, encoding model, and
rep/def level representation are meaningfully different from Parquet.

This document scopes the addition of Lance *reader* support to
EngineeredWood. A writer is explicitly out of scope for a first pass —
real writer support needs multipart-upload semantics that the current
`ISequentialFile` abstraction cannot express (see
[Storage abstraction fit](#storage-abstraction-fit)), and that is a
larger design change best deferred until a reader has proved the rest of
the surface.

Target versions:

- **v2.0** (stable since mid-2024) — still the dominant on-disk version.
- **v2.1** (stable since 2025-10-03) — the default for new writers.

Non-targets for a first pass:

- **v0.1** — legacy, a completely different layout; reject with a clear
  version error.
- **v2.2** — experimental at time of writing; reject for now.

## Where Lance sits

Lance spans four specification layers:

| Layer | What it is | This project |
|---|---|---|
| Object store | S3 / GCS / Azure / local, accessed via ranged reads | Existing `IRandomAccessFile` |
| File format (`.lance`) | Columnar file with protobuf footer | **In scope** |
| Table format | Dataset = manifest + fragments + deletion files | Out of scope (future, parallel to Delta/Iceberg) |
| Catalog (Lance Namespace) | Namespace spec | Out of scope |
| Compute | DataFusion-based query layer | Out of scope |

Our initial work is the single-file format reader. This mirrors how we
treat Parquet and ORC: the file format lives in its own package; table
formats that reference Lance files would later sit in a separate
package (e.g., `EngineeredWood.Lance.Table`) the same way Delta Lake
sits on top of Parquet today.

## File format layout

### Footer

The footer is a fixed 40-byte trailer at end-of-file, little-endian,
magic `"LANC"`:

```
u64 column_meta_start       // offset of first ColumnMetadata blob
u64 cmo_table_offset        // offset of Column Metadata Offset table
u64 gbo_table_offset        // offset of Global Buffer Offset table
u32 num_global_buffers
u32 num_columns
u16 major_version           // 0, 2
u16 minor_version           // 1, 0, 1, 2
"LANC"                      // 4-byte magic
```

Source: `rust/lance-file/src/format.rs` — `FOOTER_LEN = 40`,
`MAGIC = b"LANC"`.

A footer with `(major, minor) == (0, 2)` signals the legacy v0.1
format (whose layout is entirely different — `FileDescriptor` with a
page table). Our reader rejects it with a clear error.

### Physical layout

From file start to EOF:

```
[ Data Buffers       ]   page data, column buffers, global buffers — interleaved
[ Column Metadatas   ]   N protobuf blobs (pbfile::ColumnMetadata)
[ CMO Table          ]   N × { position: u64, size: u64 }  — 16 bytes/column
[ GBO Table          ]   G × { position: u64, size: u64 }  — 16 bytes/global buffer
[ Footer (40 bytes)  ]
```

Global buffer 0 is always the `FileDescriptor` (schema + total row
count). The reader aborts if it is missing.

### Bootstrap (what a reader actually does)

1. `GetLengthAsync()` — 1 IOP.
2. **Optimistic tail read**: fetch the last *block_size* bytes
   (typically 64 KiB for cloud, 4 KiB for local). This almost always
   brings the footer, CMO table, GBO table, column metadata, and the
   `FileDescriptor` along for free.
3. Parse the 40-byte footer. Validate magic + version.
4. Slice the GBO table from the tail buffer; read global buffer 0
   (`FileDescriptor`) — another IOP only if the tail read didn't cover
   it.
5. Slice the Column Metadata region + CMO table; parse per-column
   `ColumnMetadata` protobufs.
6. Per column: page descriptors carry `buffer_offsets` /
   `buffer_sizes` plus an `Encoding` blob. The Encoding is interpreted
   differently by version (see next section).

Protobuf source of truth (in the `lance-format/lance` repo, Apache-2.0):

- `protos/file2.proto` — v2.x envelope (`ColumnMetadata`, `Page`,
  `Encoding`, `DirectEncoding`, `DeferredEncoding`).
- `protos/encodings_v2_0.proto` — v2.0 `ArrayEncoding` / `ColumnEncoding`.
- `protos/encodings_v2_1.proto` — v2.1+ `PageLayout` /
  `CompressiveEncoding`.

The envelope (footer, `ColumnMetadata`, `Page`) is **identical** across
v2.0 and v2.1. The dispatch happens when decoding `page.encoding`.

## Encoding inventory

### v2.0: `ArrayEncoding` tree

Per-buffer encodings described recursively by `ArrayEncoding` (21
variants in `encodings_v2_0.proto`):

| Group | Encodings |
|---|---|
| Value layout | `Flat`, `FixedSizeBinary`, `Variable`, `Constant`, `Block` |
| Nullability | `Nullable` (NoNull / SomeNull / AllNull) |
| Nesting | `FixedSizeList`, `List`, `SimpleStruct`, `PackedStruct` |
| String/binary | `Binary`, `Fsst` |
| Integer | `Bitpacked`, `BitpackedForNonNeg`, `InlineBitpacking`, `OutOfLineBitpacking` |
| Run-length | `Rle`, `Dictionary` |
| FP | `ByteStreamSplit` |
| Composite | `PackedStructFixedWidthMiniBlock`, `GeneralMiniBlock` |

Column-level (`ColumnEncoding`): `values` (empty — "just data"),
`ZoneIndex` (zone maps for pushdown), `Blob` (large-binary position/size
pairs that point into a dedicated blob column).

### v2.1: structural redesign

Top-level `PageLayout` oneof selects the page's **structural** layout:

- `MiniBlockLayout` — chunks of ≤32 KiB (≥64 KiB in v2.2 when
  `has_large_chunk` is set). One I/O per page, many values per chunk.
  Intended for small values (scalars, short strings).
- `ConstantLayout` — the whole page is a single repeated value.
- `FullZipLayout` — one I/O *per value*. Intended for large values
  (embeddings, images, ≥32 KiB each). Compression must be "transparent"
  per-value.
- `BlobLayout` — page points into a blob buffer.

Inside these, value data uses a `CompressiveEncoding` tree (13
variants): `Flat`, `Variable`, `Constant`, `OutOfLineBitpacking`,
`InlineBitpacking`, `Fsst`, `Dictionary`, `Rle`, `ByteStreamSplit`,
`General` (wraps LZ4/ZSTD), `FixedSizeList`, `PackedStruct`,
`VariablePackedStruct` (v2.2+).

Rep/def levels are first-class, encoded via a Lance-specific
`RepDefLayer` enum (all-valid-item, nullable-item,
null-and-empty-list, etc.) and sliced into mini-blocks alongside value
data. This is different from Parquet's model and is the single
highest-risk area of the port — the prose spec is thin here; the Rust
comments in `encodings_v2_1.proto` and
`rust/lance-encoding/src/repdef.rs` are the normative reference.

### Non-trivial algorithms

- **FSST** (Boncz/Neumann/Leis, VLDB 2020) — no managed .NET port
  exists; must be ported (~500 LOC). Algorithm is public.
- **Bit-packing** — both out-of-line (fixed width per page) and inline
  (width per mini-block chunk).
- **Byte-stream-split** — trivial once layout is known; we already have
  it in Parquet (`ByteStreamSplit{Decoder,Encoder}`), it can be
  factored into Core if useful.
- **RLE** over 8/16/32/64/128-bit values.
- **LZ4 + ZSTD** — already in Core.

## Storage abstraction fit

Lance's Rust `Reader` trait reduces to: `size()`, `get_range(range)`,
coalesced `submit_request(ranges, priority)`, plus advisory
`block_size()` / `io_parallelism()` hints. Concretely against our
current API:

| What Lance needs | What we have |
|---|---|
| File length | `IRandomAccessFile.GetLengthAsync` |
| Single ranged read | `ReadAsync(FileRange)` |
| Multi-range coalesced reads | `ReadRangesAsync` + `CoalescingFileReader` |
| `block_size` hint (4 KiB local / 64 KiB cloud) to size the tail read | Missing — currently implicit |
| Priority-aware scheduling with in-flight-bytes backpressure | **Missing** — reads have no priority concept |
| Multi-range HTTP (`bytes=0-9,20-29`) | **Not required** — Lance issues separate requests and coalesces client-side |
| Streaming multipart upload for cloud writers | **Missing** — `ISequentialFile` is append-only buffered |
| Listing | `ITableFileSystem.ListAsync` — only needed at table-format layer |

For a **reader**, the existing API is a *superset* of what is strictly
required. Priority scheduling is a performance optimization, not a
correctness requirement. A naive implementation that issues ranged
reads and calls `CoalescingFileReader` will produce correct output; it
will just not match the Rust crate's scan throughput on high-latency
object stores.

**Proposed additions, reader-only, non-breaking:**

1. An optional `IoHints` property on `IRandomAccessFile` (or passed at
   open time) exposing `BlockSize` and `IoParallelism`. Default values
   chosen per storage backend.
2. A `PriorityCoalescingFileReader` decorator — takes a priority per
   range and fulfils higher-priority ranges first, with a caller-set
   cap on in-flight bytes. Composes over any `IRandomAccessFile`.
   Implement only when benchmarks show it's needed.

Both are decorators or optional extensions; nothing in the existing
format readers (Parquet, ORC, Avro) needs to change.

**Writer-level changes, deferred:** Lance writes use `object_store`'s
`MultipartUpload` trait (S3 multipart, Azure block-blob staging). Our
`ISequentialFile` cannot express this. When we eventually want a
writer we will need either:

- An `IMultipartWriter` interface (`StartAsync`, `UploadPartAsync`,
  `CompleteAsync`, `AbortAsync`), or
- A cloud-aware extension to `ISequentialFile` that buffers into parts
  internally.

This is a meaningful design decision and it should be made when we
actually need a writer, not speculatively now.

## Project layout

Mirroring the existing format projects (Parquet / ORC / Avro):

```
src/
  EngineeredWood.Lance/                 new
    Lance/
      Format/                           footer, CMO/GBO tables, version dispatch
      Metadata/                         ColumnMetadata, Page, FileDescriptor
      Schema/                           Arrow <-> Lance schema conversion
      Encodings/
        V20/                            ArrayEncoding decoders
        V21/                            PageLayout + CompressiveEncoding decoders
        Shared/                         FSST, bit-packing, RLE, byte-stream-split
      Data/                             RecordBatch assembly, page scheduling
      LanceFileReader.cs                public API
      LanceReaderBuilder.cs
      LanceReadOptions.cs

test/
  EngineeredWood.Lance.Tests/           new — xUnit 2.9.3
    TestData/                           pylance-produced .lance files
    GenerateTestData.py                 pylance generator (same pattern as Avro)
    FormatTests/                        envelope + version dispatch
    EncodingTests/                      per-encoding round-trip
    CrossValidationTests/               pylance ↔ EngineeredWood parity

  EngineeredWood.Lance.Benchmarks/      new — BenchmarkDotNet
    ReadBenchmarks.cs
    ScanBenchmarks.cs
    ProjectionBenchmarks.cs
```

`InternalsVisibleTo` for `EngineeredWood.Core` is already set; add
`EngineeredWood.Lance` to the list.

### Naming conflicts

- `EngineeredWood.Lance.Encodings.V20` has an `Encoding` enum-like
  hierarchy; as with Avro/Parquet, fully qualify `System.Text.Encoding`
  inside that namespace.
- The Rust protobuf `FileDescriptor` clashes with
  `System.ComponentModel.FileDescriptor` in some tooling contexts;
  prefer `LanceFileDescriptor` in public API.

## Implementation phases

| Phase | Scope | Depends on | Status |
|---|---|---|---|
| **Phase 1** | Envelope reader: footer, CMO/GBO tables, `FileDescriptor`, version dispatch, Arrow schema conversion. End-to-end "open a file, list columns and row count" with no decoded data. | nothing | Done |
| **Phase 2** | v2.0 basic encodings: `Flat`, `Nullable`, `Binary`, `FixedSizeBinary`, `Constant`. Decode primitive + variable-binary columns end-to-end. `Variable` dropped as a v2.1-only artifact. | Phase 1 | Done |
| **Phase 3** | v2.0 integer/dictionary encodings: `Bitpacked` (simple LSB) and `Dictionary`. `BitpackedForNonNeg` deferred to the dedicated Fastlanes phase; `InlineBitpacking`/`OutOfLineBitpacking`/`Rle`/`GeneralMiniBlock` are miniblock-scoped and belong to Phase 6. | Phase 2 | Done |
| **Phase 4** | v2.0 FP + advanced. Scope corrected mid-stream: `ByteStreamSplit` factored out of Parquet into `EngineeredWood.Encodings` (Core); `Block` / `GeneralMiniBlock` / `PackedStructFixedWidthMiniBlock` moved to Phase 6 (no buffer field, miniblock-only); `PackedStruct` moved to Phase 5; `Fsst` deferred to its own phase. | Phase 3 | Done |
| **Phase 5** | v2.0 nested: `SimpleStruct` (lossy — struct-level nulls aren't representable), `List` / `LargeList`, `FixedSizeList`. `PackedStruct` still throws (pylance never emits it). | Phase 2 | Done (first slice) |
| **Phase 6** | v2.1 structural layouts: `ConstantLayout`, `MiniBlockLayout`, and the `CompressiveEncoding` tree. First slice ships `Flat` for byte-aligned primitive leaves with `ALL_VALID_ITEM` / `NULLABLE_ITEM` layers. `InlineBitpacking` / `OutOfLineBitpacking` / `Rle` / `GeneralMiniBlock` extended in subsequent slices. | Phases 2–5 | Done (first slice) |
| **Phase 7** | v2.1 rep/def: `RepDefLayer` decoding, repetition index sidecar, nested nullable/list assembly. First slice: `Variable` strings + single-column lists. Phase 7b (2026-04-26) added v2.1 multi-leaf struct support with cross-column def coherence. Multi-chunk Variable + InlineBitpacking def levels followed; layout-level dictionary, FullZip+General(ZSTD)+Variable, and MiniBlock+General(ZSTD) wrapping all shipped 2026-04-26. | Phase 6 | Done (first slice + v2.1 struct) |
| **Phase 8** | v2.1 `FullZipLayout` (fixed-width values; variable-width via `bits_per_offset` deferred). `BlobLayout` deferred entirely. | Phase 7 | Done (first slice) |
| **Phase 9** | Wire `Clast.FastLanes` 0.1.0 (standalone bit-packing library, zero Lance/Arrow deps) into `BitpackedForNonNegDecoder` (v2.0) and v2.1 miniblock bitpacking. | Phase 6 | Done |
| **Phase 10** | Cross-validation sweep test against pylance-produced files; Lance added to README + ARCHITECTURE.md format coverage tables. | Phases 5, 8 | Done |
| **Phase 11** | Benchmarks (open, read, fastlanes-unpack) at parity with Parquet/ORC benchmark structure. Verdict: open + protobuf + Arrow construction dominate at 38–52 µs/op; raw I/O is not the bottleneck for local files. | Phase 10 | Done |
| **Phase 12** *(optional)* | `IoHints` extension + `PriorityCoalescingFileReader` decorator. Deferred — only moves the needle on cloud blob storage with priority-aware predicates, neither of which Lance currently has. | Phase 11 | Deferred |
| **FSST** | Lance-specific FSST symbol-table format reverse-engineered (8-byte file prefix + "TSSF" magic + 256 × 8-byte slots + 256-byte trailing metadata = 2312 bytes). Wired through `Clast.Fsst` 0.1.2's bring-your-own-framing API. Includes the OutOfLineBitpacking def-level path for nullable FSST. | Phase 7 | Done |
| **Lance.Table** | `EngineeredWood.Lance.Table/` — read-only Lance dataset support: `LanceTable.OpenAsync` (manifest read), `ReadAsync(columns, filter)` (projection + row-level filter via shared `EngineeredWood.Expressions`), deletion files (sparse Arrow-IPC + dense CRoaring portable variants), time travel (`OpenAsync(path, version)` + `ListVersionsAsync`). | Phase 10 | Done |
| **Phase 13** *(future)* | `IMultipartWriter` design + cloud multipart upload + Lance writer. Separate design doc at that time. | — | Not started |

### Ordering rationale

Phase 1 (envelope) is a weekend of work and unblocks everything else —
ship it first and iterate on encodings. Phases 2–5 deliver a complete
v2.0 reader; at that point we can read real-world files written by
older pylance versions and claim meaningful coverage. Phase 6 reuses
the Phase 2–5 decoders under a different wrapper and is mostly
structural plumbing. Phase 7 (rep/def) is the risk-concentrated phase;
attempting it before Phase 6's structural plumbing is wasted effort.
Phase 9 (Fastlanes) is independent work — it's a separate library with
its own release cadence and can proceed in parallel with anything
earlier. It just can't be *tested against real data* until Phase 6 has
at least exercised miniblock bitpacking, so it sits after Phase 8 in
the sequencing. Phases 10–11 ensure we ship tests and benchmarks
parallel to the existing formats rather than bolting them on.
Phases 12–13 are explicitly optional / deferred.

### Testing strategy

- **Envelope tests:** hand-crafted minimal .lance files covering
  footer parsing, version rejection (v0.1, v2.2), malformed magic,
  truncated tails, missing global buffer 0.
- **Encoding round-trip tests:** pylance writes files per encoding
  variant, EngineeredWood reads and asserts Arrow equality. Skip the
  reverse direction until we have a writer.
- **Cross-tool sweep:** point the test at a directory of
  pylance-produced files covering the matrix of
  (version, logical type, encoding); collect failures; assert none.
  Same pattern as
  `test/EngineeredWood.Parquet.Compatibility/`.
- **Projection + random access:** assert Lance's zero-copy random
  access — open a file, read a single row by index, verify no extra
  pages were fetched (instrument `IRandomAccessFile`).
- **Version edge cases:** mixed-case footer bytes, minor-version
  forward-compat (we should read unknown minor versions within a
  supported major, per Lance's forward-compat guarantees).
- **Nested nulls (Phase 7):** deeply-nested lists of nullable structs
  with every combination of `RepDefLayer` values; cross-validate
  against pylance.

## Risks and open questions

1. **Spec website is thin prose.** The `.proto` files and Rust
   comments are the normative spec. Maintenance model mirrors what we
   already do for ORC, but the commitment is real.
2. **No third-party reader to cross-reference.** Every downstream
   integration we surveyed (DuckDB's 2026 Lance extension, Python,
   Java JNI, Spark/Flink/Trino connectors) links the Rust crate rather
   than reimplementing. We would be the first clean-room non-Rust
   reader. Upside: ecosystem gap. Downside: no prior-art port to
   learn from.
3. **Rep/def in v2.1** is under-documented in prose. Budget time for
   reading `rust/lance-encoding/src/repdef.rs` and the comments in
   `encodings_v2_1.proto`.
4. **`DeferredEncoding`** (shared encodings across columns/pages) is
   permitted by `file2.proto` but the Rust reader has `todo!()` for
   the indirect case in `fetch_encoding`. Suggests it is not used in
   practice. Stub with a clear error; revisit when a real file needs
   it.
5. **v2.2 movement.** `VariablePackedStruct`, 64 KiB mini-blocks,
   generalized `ConstantLayout`. Explicitly out of scope; reject with
   a clear version error.
6. **Test data generation.** pylance requires Rust + Python;
   `GenerateTestData.py` should document its toolchain the same way
   `test/EngineeredWood.Avro.Tests/TestData/generate_test_data.py`
   documents fastavro.
7. **Schema conversion.** Lance schemas carry metadata and field IDs.
   Need to decide whether Arrow metadata keys round-trip 1:1 with
   Lance's field-level metadata — check against pylance output before
   freezing the public API.
8. **Priority scheduler necessity.** Unknown without measurement.
   Phase 11 is gated on Phase 10 benchmarks showing the naive reader
   leaves performance on the table.

## References

- Spec site: <https://lance.org/format/file/>,
  <https://lance.org/format/file/encoding/>,
  <https://lance.org/format/file/versioning/>
- Source repo: <https://github.com/lance-format/lance> (Apache-2.0).
  Relevant paths:
  - `protos/file2.proto`, `protos/encodings_v2_0.proto`,
    `protos/encodings_v2_1.proto`
  - `rust/lance-file/src/format.rs`, `rust/lance-file/src/reader.rs`
  - `rust/lance-encoding/src/decoder.rs`,
    `rust/lance-encoding/src/repdef.rs`
- Paper: "Lance: Efficient Random Access in Columnar Storage through
  Adaptive Structural Encodings"
  (<https://arxiv.org/html/2504.15247v1>).
- v2.1-stable announcement (2025-10-03):
  <https://www.lancedb.com/blog/lance-file-2-1-stable>.
- FSST paper: Boncz, Neumann, Leis, "FSST: Fast Random Access String
  Compression", VLDB 2020.
