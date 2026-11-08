# Vortex support

> **Experimental.** See the maturity table in the [README](../README.md#maturity).

[Vortex](https://github.com/vortex-data/vortex) is a columnar file format
with FlatBuffers-based metadata and a rich encoding zoo (ALP, FSST,
FastLanes bit-packing / FoR / delta / RLE, Pco, sparse, run-end, dict,
etc.). `EngineeredWood.Vortex` ships a reader, a writer, and a
predicate-based zone-pruning API — all driven by hand-rolled
FlatBuffers/protobuf parsing (no `Google.FlatBuffers` or `flatc`
dependency) and cross-validated against the Rust `vortex-array` 0.86
implementation in both directions: upstream reads what the writer emits,
and the reader decodes upstream's published compatibility fixtures — every
release since 0.64 — to the same values upstream does.

## Reading

- `VortexFileReader.OpenAsync(string)` / `OpenAsync(IRandomAccessFile)`
  validates the leading + trailing `'VTXF'` magic, parses the
  postscript / footer / DType / Layout FlatBuffer segments, and exposes
  `Schema` (`Apache.Arrow.Schema`) and `NumberOfRows`.
- `ReadAllAsync()` streams the file as `IAsyncEnumerable<RecordBatch>`,
  one batch per chunk for chunked layouts. Files whose root layout holds
  whole rows rather than a `vortex.struct` of columns (upstream's flat
  layout strategy) read the same way.
- **Column projection**: `ReadAllAsync(IReadOnlyList<int> columnIndices)`
  decodes only the requested columns. `ReadColumnAsync(int fieldIndex)`
  returns a single column as one Arrow array (concatenated across chunks).
- **Row-range slice**: `ReadAllAsync(long rowOffset, long rowCount, ...)`
  drops chunks fully outside the range with zero I/O; boundary chunks are
  decoded fully and sliced via `RecordBatch.Slice`.
- **Predicate-based zone pruning**: `ReadAllAsync(Predicate, ...)` takes
  a shared `EngineeredWood.Expressions.Predicate` and skips zones whose
  stored stats prove the predicate can't match. The same predicate types
  drive Parquet row-group pruning, Delta Lake file pruning, and Iceberg
  scan planning. Coverage spans numeric (`i8..i64`, `u8..u64`, `f32`,
  `f64`) + string + binary + bool + temporal (`Date32/64`, `Timestamp`,
  `Time32/64`) comparisons, `IS NULL` / `IS NOT NULL`, `IN` / `NOT IN`,
  and `AND` / `OR` / `NOT` composition with three-valued logic.
- `GetZoneStatsAsync(int fieldIndex)` exposes the per-zone stats table
  (Min, Max, Sum, NullCount, NaNCount, IsConstant, IsSorted,
  IsStrictSorted, UncompressedSizeInBytes, MinIsTruncated,
  MaxIsTruncated) as typed Arrow arrays.

## Writing

- `VortexFileWriter(Stream, Apache.Arrow.Schema, ...)` writes one or
  more `RecordBatch`es. Single-batch files use a
  `vortex.struct(vortex.flat × N)` layout; multi-batch files use
  `vortex.struct(vortex.chunked(vortex.flat × M) × N)`.
- Opt-in flags: `compress` (turn on the compressing-encoding chain),
  `preferVarBinView` (use `vortex.varbinview` instead of `vortex.varbin`
  for strings that fall through compression), `preferPco` (route eligible
  numeric columns through `vortex.pco`), `preferDateTimeParts` (split
  `TimestampArray` into days/seconds/subseconds), `preferDictLayout`
  (share one global string dict across all batches via a
  `vortex.dict` layout instead of per-batch array-level dicts),
  `preserveStats` (wrap each column in a `vortex.stats` layout that
  carries per-zone Min/Max/NullCount/etc., enabling reader-side zone
  pruning), and `preferDelta` (let the compressing chain use
  `fastlanes.delta`; see below).
- Everything the writer emits belongs to a frozen core Vortex edition
  (`core2025.05.0` through `core2025.10.0`: vortex 0.36 reads the base set,
  0.40 adds `vortex.pco`, 0.54 adds `fastlanes.rle` and
  `vortex.fixed_size_list`), except
  `fastlanes.delta`, which belongs to no edition and so carries no upstream
  promise that later readers accept it. The writer only uses it under
  `preferDelta`. A `FixedSizeBinary(16)` column is written as the
  `vortex.uuid` extension, which vortex has read since before 0.70 but
  only froze in `core2026.08.3` (0.85). A Map column is written as the Map
  dtype over `vortex.map`, both from `core2026.08.2`: a file containing
  one needs vortex 0.85 or later to read it, as it would from vortex's own
  writer. No earlier encoding can carry the Map dtype, and files without
  a map are unaffected.
- `preferDictLayout && preserveStats` emits
  `vortex.stats(vortex.dict(...), zones-flat)` so predicate pruning
  works against dict-layout files too.

## Type coverage

| Arrow type | Read | Write |
|---|---|---|
| Int8/16/32/64, UInt8/16/32/64, Float, Double | yes | yes |
| HalfFloat (Float16) | yes (net6+) | yes |
| Bool | yes | yes |
| String, Binary | yes | yes |
| FixedSizeBinary | yes | yes (via `vortex.uuid` for size 16) |
| Decimal128, Decimal256 | yes | yes |
| Date32, Date64, Time32, Time64, Timestamp | yes | yes |
| FixedSizeList | yes | yes |
| List | yes | yes (i32 offsets; LargeList deferred) |
| Struct (recursive, including nested struct/list/FSL) | yes | yes |
| Map (`keys_sorted`, duplicate keys, null maps) | yes | yes (vortex 0.85+ readers) |
| LargeString, LargeBinary, Union | not yet | not yet |

## Encoding coverage

**Layouts**: `vortex.flat`, `vortex.struct`, `vortex.chunked`,
`vortex.stats` and `vortex.zoned` (zone maps; the writer emits
`vortex.stats`), `vortex.dict` (layout-level, shared dict).

**Array encodings — read**: `vortex.primitive` (nullable + non-nullable),
`vortex.constant`, `vortex.chunked`, `vortex.sequence`, `vortex.bool`, `vortex.bytebool`,
`vortex.null`, `vortex.varbin`, `vortex.varbinview`, `vortex.fsst`,
`vortex.onpair`, `vortex.runend`, `vortex.dict`, `vortex.sparse`, `vortex.masked`,
`vortex.list`, `vortex.listview`, `vortex.map`, `vortex.fixed_size_list`,
`vortex.struct`, `vortex.decimal`, `vortex.decimal_byte_parts`,
`vortex.alp`, `vortex.alprd` (f32 + f64), `vortex.datetimeparts`,
`vortex.ext` (with extension types `vortex.timestamp` / `vortex.date` /
`vortex.time` / `vortex.uuid`), `fastlanes.bitpacked` (with patches),
`fastlanes.for`, `fastlanes.rle`, `fastlanes.delta` (signed and unsigned),
`vortex.zigzag`, `vortex.zstd`, `vortex.pco`.

**Array encodings — write**: `vortex.primitive`, `vortex.bool`,
`vortex.varbin`, `vortex.varbinview`, `vortex.constant`,
`vortex.dict` (string), `vortex.fsst` (string + binary, nullable),
`vortex.runend` (nullable), `vortex.sparse` (nullable),
`vortex.alp` (f32 + f64), `vortex.alprd` (f32 + f64, nullable),
`vortex.list`, `vortex.listview` and `vortex.map` (map columns),
`vortex.fixed_size_list`, `vortex.struct`,
`vortex.decimal`, `vortex.datetimeparts`, `vortex.ext` (Date / Time /
Timestamp / UUID), `fastlanes.bitpacked` (with best-bit-width selection
and patches), `fastlanes.for`, `fastlanes.delta` (under `preferDelta`), `fastlanes.rle`
(floats, nullable), `vortex.pco`. Compressing encoders honor
`data.Offset != 0` (sliced inputs).

## Storage and compression

- Uses the existing offset-based `IRandomAccessFile`. Per-segment
  compression: only `None` is read. The format reserves the
  `LZ4`/`ZLib`/`ZStd` segment codecs without defining them (no writer sets
  them, and a segment records no uncompressed length), so a segment that
  names one is refused. Compression that writers use lives in array
  encodings such as `vortex.zstd`. Encryption is rejected outright.

## Multi-targeting

- The library compiles clean for `netstandard2.0`, `net8.0`, and
  `net10.0`. The test suite also runs on `net472`; only the two
  HalfFloat-focused tests are gated behind `#if NET6_0_OR_GREATER`
  because `System.Half` / `Apache.Arrow.HalfFloatArray` require .NET 6+.

