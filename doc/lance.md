# Lance support

> **Experimental.** See the maturity table in the [README](../README.md#maturity). Gaps and plans are in
> [lance-future-work.md](lance-future-work.md).

`EngineeredWood.Lance` reads and writes individual `.lance` files; `EngineeredWood.Lance.Table` adds the dataset layer
(manifests, fragments, transactions). Both are built from the protobufs and Rust source of
[`lance-format/lance`](https://github.com/lance-format/lance) and cross-validated against
[pylance](https://pypi.org/project/pylance/): many writer paths are tested by having pylance read what we write.

## Versions

- **v2.0** (footer bytes `(0, 3)` — pylance's pre-0.38 default — and `(2, 0)`)
- **v2.1** (footer bytes `(2, 1)` — pylance ≥ 0.38 default for new writes)
- **v2.2** (footer bytes `(2, 2)` — required for Map type; the file
  layout is otherwise identical to v2.1)

v0.1 and any future v2.3+ are rejected with explicit errors.

## File-level reading

- Public `LanceFileReader.OpenAsync(path)` parses the footer, GBO/CMO
  tables, `FileDescriptor`, and per-column metadata.
- `ReadColumnAsync(int fieldIndex)` returns the N-th top-level Arrow
  field as an `IArrowArray`. For nested fields it reads every physical
  column needed and assembles a single nested array.
- Optimistic 64 KiB tail read brings the footer + metadata + global
  buffer 0 in one I/O on cloud storage.

## File-level writing

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

## Dataset / table layer

`EngineeredWood.Lance.Table` wraps the file writer in the Lance dataset
directory layout (`data/`, `_versions/`, `_transactions/`, `_deletions/`)
and exposes the read side via `LanceTable`.

### Reading

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

### Writing

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

## Type coverage

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

## v2.0 encoding coverage

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

## v2.1 / v2.2 encoding coverage

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

## Storage abstraction

Reuses the existing offset-based `IRandomAccessFile` — Lance's reader
needs `size()` + `get_range()` + coalesced `get_ranges()`, all of which
the Core API already provides. Priority-aware scheduling and HTTP
multi-range requests are perf optimizations not currently implemented.

## Bit-packing dependency

Lance's v2.0 `BitpackedForNonNeg` and v2.1 `InlineBitpacking` use the
fastlanes 1024-element packing format. `EngineeredWood.Lance` consumes
the [`Clast.FastLanes`](https://www.nuget.org/packages/Clast.FastLanes/)
NuGet package (zero Lance/Arrow dependencies; bit-for-bit compatible
with the Rust `lance_bitpacking` crate).

