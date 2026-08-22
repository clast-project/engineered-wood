# Extended-precision timestamps for Parquet

Implementation notes for the experimental `TIMESTAMP` on `FIXED_LEN_BYTE_ARRAY(12)` support in
`EngineeredWood.Parquet`, gated behind `EWPARQUET0004` — and, more importantly, a record of the
decisions that are ours rather than the spec's, since a reader of the code cannot tell those apart.

## Sources

| Source | What it is | State |
|---|---|---|
| [parquet-format#600](https://github.com/apache/parquet-format/issues/600) | Tracking issue | open |
| [parquet-format#601](https://github.com/apache/parquet-format/pull/601) | The normative spec change | **open**, two approvals |
| [proposal doc](https://docs.google.com/document/d/1H43RQZhWKcg9c4tJO5W87YxhbUxN7qGSFxJBrrwWOfY/edit) | Rationale, alternatives considered | — |
| [parquet-java#3680](https://github.com/apache/parquet-java/pull/3680) | Reference implementation | open |
| [parquet-testing#123](https://github.com/apache/parquet-testing/pull/123) | `flba12_timestamp.parquet` conformance fixture | open |

Nothing here is merged. That is the context for everything below.

## What the carrier is

`TIMESTAMP` may annotate `FIXED_LEN_BYTE_ARRAY` with `type_length = 12`. The value is a signed
96-bit two's-complement **little-endian** count of the column's declared `TimeUnit` since the Unix
epoch. All three units and both `isAdjustedToUTC` values are legal. Sort order is
`TypeDefinedOrder` with a signed comparison — *not* the lexicographic byte order every other
`FIXED_LEN_BYTE_ARRAY` column uses.

It exists because `INT64` nanoseconds spans only 1677-09-21 to 2262-04-11, which does not cover the
ANSI SQL `TIMESTAMP(9)` range of years 0001–9999. Ninety-six bits does, with room for picoseconds
and femtoseconds later at no further format change.

## The byte order is not settled, and that is the main risk

The spec text, the parquet-java reference implementation and the conformance fixture are all
little-endian, and the proposal defends the choice with a benchmark (0.572 vs 0.684 ns/value against
a big-endian reverse-and-pad). **But it is explicitly still open.** On parquet-format#601, a
proposal co-author argued for big-endian so readers could reuse the `DECIMAL` comparator; the author
replied that they had no strong preference and wanted to hear from others; and the approving
reviewer signed off with "LGTM, we can bikeshed more on little endian vs big-endian to finalize
this."

**Nothing on the wire distinguishes the two orders.** If the spec flips, files already written here
do not become unreadable — they become silently wrong-valued, which is worse. That is the risk the
experimental gate carries, and it is a stronger reason for the gate than ALP or FSST had.

Every entry point goes through `ExtendedTimestamp` so that a flip is a one-file change plus the
conformance vectors.

## Decisions that are ours, not the spec's

### Arrow has no type for this, and no plan for one

Arrow's timestamp is hard-coded to `int64`. `timestamp128`
([apache/arrow#47848](https://github.com/apache/arrow/issues/47848)) has been dormant since May 2026
— the author said they are not working on it — and no canonical extension type exists. The proposal
doc's own comment thread concludes that "a canonical extension type might be the only reasonable
mechanism here" and deliberately decouples the two proposals.

So the read mapping is this library's choice. `ParquetReadOptions.ExtendedTimestampOutput`:

| `ExtendedTimestampOutputKind` | Arrow type | Notes |
|---|---|---|
| `TimestampMicroseconds` (default) | `timestamp[us, tz?]` | Spans ±292,000 years, so a conforming file always reads |
| `Timestamp` | `timestamp[declared unit, tz?]` | Keeps every digit; reports a value `int64` cannot hold |
| `FixedSizeBinary` | `fixed_size_binary[12]` | The raw bytes, uninterpreted |

The enum is a deliberate **sibling** of `Int96OutputKind` rather than the same enum. INT96 carries no
logical annotation, so its unit is genuinely the reader's choice; here the file declares the unit and
reading at another one would be a rescale, not an output kind. The two also want opposite defaults —
INT96's default is the one that never throws, and `Timestamp` is not.

The default was `Timestamp` for one commit and was changed. The corpus sweep is what settled it:
adding the conformance fixture to `parquet-testing` broke `ReadRowGroupTests` outright, because a
plain `ParquetFileReader` could not read a valid, spec-conforming file. Reading a valid file should
not require knowing in advance what is in it.

`TimestampMicroseconds` is not unconditionally safe, only practically so: the carrier holds ±2^95
units, which in microseconds is far past `int64`. Nothing representing a date can reach that.

### No extension type, for now

`ExtensionType`, `ExtensionDefinition` and `ExtensionTypeRegistry` are public in Arrow 23.0.0, so we
*could* define one — but `GuidExtensionDefinition`, `Bool8ExtensionDefinition`,
`VariantExtensionDefinition` and `TimestampWithOffsetExtensionDefinition` all ship **upstream**. This
would be the first extension name EngineeredWood invents, and inventing `ew.parquet.timestamp96` for
a type Arrow may later standardise differently is a compatibility trap of our own making. If Arrow
picks a name, we adopt it.

### `converted_type` is omitted

`TIMESTAMP_MILLIS` and `TIMESTAMP_MICROS` are defined for `INT64` only. A reader that understands
converted types but not the new logical-type carrier would decode twelve bytes as eight, so the field
is absent entirely for this carrier.

**This is not in the spec PR's text.** parquet-java found it in review and suppresses it the same
way. It has its own test here, because a future refactor that "helpfully" restored the converted type
would produce files that silently misread on older engines.

## What is implemented

**Read.** All three units, both `isAdjustedToUTC` values, every encoding legal for
`FIXED_LEN_BYTE_ARRAY` (PLAIN, RLE_DICTIONARY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT). Narrowing to
64-bit happens once at array-build time, sharing the machinery INT96 already used — both are twelve
opaque bytes in the value buffer that become eight in place.

**Write.** Opt-in per column via `ParquetWriteOptions.ExtendedTimestampColumns` (dotted paths).
Encoding to the carrier happens in the same place as the `DECIMAL` big-endian reversal and for the
same reason: everything downstream — dictionary, page encoders, statistics, bloom filter — must see
the bytes that reach the file.

**Statistics.** `min_value`/`max_value` computed with the signed 96-bit comparator. The deprecated
`min`/`max` are dropped, because they promise signed ordering *over the bytes as compared* and no
reader could reproduce that from little-endian bytes. Bounds decode back through `BigInteger`'s
`byte[]` constructor, which reads little-endian two's complement and takes the sign from the last
byte — exactly this layout, and exactly why the `DECIMAL` case beside it has to reverse first.

**Bloom filters.** A timestamp literal is converted to the same twelve bytes the file holds, and only
when the conversion is exact.

## The promotion can never be automatic

An Arrow timestamp is `int64`, so **any value Arrow can hold already fits `INT64`** with room to
spare. Nothing this library can be handed needs the wider carrier. `ExtendedTimestampColumns` exists
to produce files in that shape — interop fixtures, and readers being tested against the proposal —
not to rescue values that would otherwise overflow.

It follows that **this library cannot write the values that motivate the carrier.** The fixture's
`timestamp_nanos` column is interesting precisely because two of its rows need more than 64 bits, and
those rows cannot be expressed in Arrow at all. The `MILLIS` and `MICROS` columns are fully
reproducible and are checked byte for byte.

## Not implemented

- **Nested columns.** A path inside a struct, list or map is refused rather than ignored. The schema
  is built by `ArrowToSchemaConverter` while the physical type the data is written with is decided by
  `NestedLevelWriter`, which does not see these options; honouring a nested request would put
  `FIXED_LEN_BYTE_ARRAY(12)` in the footer over pages holding `INT64`.
- **ColumnIndex / page-level bounds.** Not implemented in this library at all, so the truncation trap
  parquet-java had to handle (`BinaryTruncator` must return these values unchanged) does not arise.
  Worth remembering if page indexes are ever added: these bounds must never be truncated.
- **An Arrow extension type**, as above.

## Validation, and its limits

**There is no external oracle.** PyArrow, DuckDB and delta-rs cannot read this combination;
parquet-java's implementation is unmerged; the parquity bridge cannot check it either. This is
weaker verification than the ALP and FSST work had, and it compounds the endianness risk.

What we do have:

- **The conformance fixture, byte-verified.** All eighteen encodings (six timestamps × three units)
  were confirmed to appear verbatim in `flba12_timestamp.parquet` before any of this was written, and
  the read tests take their expectations from that file's own documented table rather than from our
  decoder. The raw-bytes test rebuilds the encoding from the documented epoch seconds, so it compares
  the file to the spec.
- **A test that guards the fixture's premise** — that its two extreme rows really do exceed `int64` —
  so the range-refusal case cannot go quiet if the file is ever regenerated.
- **Both writers compared byte for byte**, because `BufferedParquetWriter` is an independent
  implementation that has drifted before, and a carrier encoded two ways would drift *silently*: both
  files would be well-formed.

The fixture tests no-op until parquet-testing#123 merges and the submodule pin moves.

## If the spec flips to big-endian

1. `ExtendedTimestamp.Read`/`Write`/`Compare` — the three that touch byte order.
2. The conformance vectors in `ExtendedTimestampTests` and `ExtendedTimestampReadTests`.
3. `ParquetStatisticsAccessor`'s bound decode, which currently exploits `BigInteger`'s little-endian
   constructor and would need the reversal the `DECIMAL` path does.
4. `StatisticsCollector.CompareFlba` could then defer to `SequenceCompareTo` after the same
   big-endian rewrite `DECIMAL` gets — which is exactly the argument made for big-endian upstream.

Files written before the flip would need rewriting; there is no way to detect them.

## Bugs this work uncovered

All pre-existing, all fixed on their own commits, none caused by the carrier:

- **`TIMESTAMP` was mapped to an Arrow timestamp without checking the physical type**, so a
  twelve-byte column was reinterpreted eight bytes at a time and produced plausible-looking wrong
  dates.
- **`FIXED_LEN_BYTE_ARRAY` written as `DELTA_BYTE_ARRAY` could not be read back** — a
  `NullReferenceException` for every such column, including `DECIMAL` above precision 18, `UUID` and
  `FLOAT16`. This library wrote files it could not itself read.
- **Sub-millisecond statistics bounds were truncated toward zero**, so a max bound of 1500 µs decoded
  as 0 ms and a predicate could prune rows that genuinely matched.
- **No temporal predicate could probe a bloom filter**, for any `DATE`, `TIME` or `TIMESTAMP` column.
