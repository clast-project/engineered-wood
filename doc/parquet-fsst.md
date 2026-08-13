# FSST encoding for Parquet

Implementation notes for the experimental FSST support in `EngineeredWood.Parquet`,
and — more importantly — a record of where the three upstream artifacts disagree, since
that is not obvious from any one of them.

## Sources

| Source | What it is |
|---|---|
| [Specification](https://docs.google.com/document/d/1Xg2b8HR19QnI3nhtQUDWZJhCLwJzW6y9tU1ziiLFZrM/edit) | The normative proposal, tracked as [parquet-format#531](https://github.com/apache/parquet-format/issues/531). Section numbers below refer to it. |
| [arrow-rs#10153](https://github.com/apache/arrow-rs/pull/10153) | Rust proof-of-concept, draft, explicitly "not ready for review" |
| [apache/arrow#48232](https://github.com/apache/arrow/pull/48232) | C++ proof-of-concept, vendoring cwida's libfsst |

**This implementation follows the specification.** Both proofs-of-concept predate the
current spec text and neither writes the format the spec describes:

- **arrow-rs** writes a page of three ULEB128 header values (a length-array encoding id,
  the length-array size, and the symbol-table size), then a DELTA_BINARY_PACKED array of
  per-value *lengths*, then the symbol table inline, then the payload. The spec has a
  fixed 9-byte header, an *end-offset* array, and no symbol table in the data page at all.
- **C++** `memcpy`s a raw `fsst_decoder_t` struct into the page and length-prefixes each
  value with a `uint32`. Dumping a C struct is not a portable wire format, and §10.3 says
  the fixed symbol-table body layout exists precisely to avoid "dependence on a specific
  FSST library internal serialization format".

Consequence: **there is nothing to interoperate with yet.** Building the Rust branch would
produce files this reader cannot read and vice versa — not because of a bug on either side,
but because the two encode different formats. Interop testing becomes worthwhile once one
of the upstream implementations is rewritten against the ratified spec.

## The encoding number is 11 here, not 10

The spec assigns `FSST = 10`. So does the ALP proposal, for itself. Both are unratified and
only one can win. ALP shipped in this library first and keeps 10, so FSST is written as
**11** — which is also what the arrow-rs author expects ("may become 11 once ALP encoding
lands"). Reversing this later is a one-line change to `Encoding.Fsst`, but it would
silently misread already-written files, which is why the choice was made deliberately
rather than by following the spec text literally.

## What is implemented

Phase 1 only (§1.4): one symbol table per column chunk, in its own page.

- `PageType.SymbolTablePage = 4`, `SymbolTablePageHeader { type, is_compressed }` at
  `PageHeader` field 9 (the spec does not assign a field id; 9 is the next free one).
- `ColumnMetaData.symbol_table_page_offset` (18) and `symbol_table_page_length` (19).
- Symbol table type `FSST` (8-bit codes) only — `SymbolTableType.Fsst` in the C# enum.
  `FSST_16` is recognized and rejected with a clear error rather than misread.
- BYTE_ARRAY only (§1.3), V2 data pages only.
- Both offset encodings, PLAIN and DELTA_BINARY_PACKED. The writer encodes the offset array
  both ways per page and keeps the smaller, recording the choice in the header byte — which
  is what the byte is for. Delta wins on essentially any page of short values, where four
  bytes of offset per value is a real cost.

## Two things that are easy to get wrong

**Code order is the length information.** The symbol table body stores a `length_histogram`,
not a per-symbol length: codes are assigned in ascending length order and the histogram is
what lets a reader cut `symbol_data` back into symbols. Clast.Fsst (like cwida's original)
assigns codes by training gain, in no particular length order — measured directly, a table
trained on URLs came back with lengths `2, 2, 1, 3, 3, ...`. `FsstSymbolTable.TryTrain`
therefore counting-sorts the trained codes by length and carries a 256-entry remap that
`Compress` applies to every emitted code. Escape markers (255) and the literal byte that
follows them pass through untouched — the literal is a raw byte, not a code.

**Compression happens per chunk, not per page.** The symbol table cannot be trained until
every value has been seen, so `FsstCompressedColumn.TryCompress` compresses the whole
chunk once, up front, and each data page is a slice of that payload with its end offsets
rebased onto its own data section. This also makes §7.5's "fall back if FSST did not help"
rule answerable *before* any page is written: `TryCompress` returns null — counting the
symbol table page against the win — and the chunk reverts to DELTA_LENGTH_BYTE_ARRAY. So
enabling FSST cannot make a file bigger.

## Validation

§3.7 and §8.1 require the reader to fail on corruption rather than produce garbage, so
`FsstSymbolTable.Parse` checks that the histogram sums to `symbol_count` and that
`symbol_data` is exactly `sum(histogram[i] * (i+1))` bytes, using widened arithmetic so a
hostile histogram cannot wrap past the check. Code streams are validated **per value**,
not over the concatenated payload: a value ending in an escape marker must not be able to
borrow the next value's first byte as its literal (§5.2). That costs one extra pass over
the compressed bytes, which is the price of the guarantee.

The one test that pins this implementation to the *specification* rather than to its own
encoder is `DataPage_SpecExample_DecodesToTheDocumentedValues`, which decodes the worked
example from §6.1/§6.2 byte for byte.

## Not implemented

- **`FSST_16`.** Note this is *not* deferred by the spec: §1.4's phase split is about
  per-chunk versus per-page symbol tables, and says nothing about code width. FSST_16 is
  fully specified in this document — §2.3 (enum), §3.3 (body layout), §3.4 (invariants),
  §3.5 (max sizes), §3.6 (lookup construction), §4.7 (code stream), §5.1, §5.3 and §8.3
  (decode pseudocode) — and Appendix C measures it favourably against OnPair16. Two
  concrete things stopped it here, neither of them the spec's scope:

  1. **No 16-bit trainer available** — tracked as
     [clast-project/fsst#1](https://github.com/clast-project/fsst/issues/1).
     Clast.Fsst ships an 8-bit table (`SymbolTable`) and
     a 12-bit one (`SymbolMap` / `Fsst12Encoder`), but nothing 16-bit — and 12-bit has no
     slot in `SymbolTableType`, which is `FSST = 0, FSST_16 = 1` only (comment [ae]
     proposed FSST_8/12/16; only 16 was added). cwida's construction is 8-bit-specific, so
     FSST_16 means writing symbol-table training, compression and decompression from
     scratch rather than plumbing an existing codec. That is the bulk of the work.
  2. **The spec contradicts itself on FSST_16 symbol length.** §1.2 says "Symbols may be
     between 1 and 8 bytes inclusive", but §3.3 gives FSST_16 a `uint16[16]` histogram
     covering lengths 1–16, §3.5 computes max `symbol_data` as 65,535 × **16**, and §3.6
     sets `max_symbol_length = 16`. Three sections say 16 and the intro says 8. A reader
     can be liberal and just honour the 16 histogram slots; a **writer cannot guess**,
     because the answer decides what it is allowed to emit. §1.2 reads like stale prose
     from the FSST8 description, but it is worth resolving with the author before writing
     bytes no one else can check — nothing else writes FSST_16 today, so a clean-room
     implementation could be perfectly self-consistent and still wrong.
- FIXED_LEN_BYTE_ARRAY. §1.3 is BYTE_ARRAY only; the doc's comment thread floats FLBA for
  UUIDs as possible future work.
- Composite dictionary + FSST (§9.2), which the spec itself says needs a further spec change.

## Design sketch: adding FSST_16

Written while the FSST8 work was fresh, so that picking this up later starts from a plan
rather than from research. Blocked on [clast-project/fsst#1](https://github.com/clast-project/fsst/issues/1).

### The §1.2 ambiguity does not reach the wire format

This is the finding that de-risks the whole thing, and it is worth checking before treating
the spec contradiction as a blocker on *implementation* rather than on *emission policy*.

§3.3 gives FSST_16 a `uint16[16]` histogram at offset 2 and `symbol_data` at offset 34,
**unconditionally**. It does not shrink when symbols happen to be short. So if §1.2 wins and
a writer may only emit symbols of 1–8 bytes, the result is simply a table whose histogram
entries for lengths 9–16 are zero — a legal, ordinary FSST_16 table. §3.6's reconstruction
loop runs `L in 1..16` and does nothing for the empty slots.

Consequence: **`Serialize`, `Parse` and every validation rule are identical under either
reading.** The only thing that changes is the cap handed to the trainer. So the Parquet side
can be built in full before the spec answers, and the answer later moves one argument.

The reader should be liberal regardless — honour all 16 histogram slots, whatever we write.

### What is shared, and what forks

The page body is **not** parameterised by symbol table type. §4.3's section list, §4.4's
9-byte header and §4.5's end-offset array are the same bytes for FSST and FSST_16; only the
interpretation of the data section changes. So:

| Component | FSST_16 impact |
|---|---|
| `FsstPageEncoder` header + offset array | **unchanged** — shared verbatim |
| `FsstPageDecoder` header + offset parsing + monotonicity checks | **unchanged** |
| Symbol table body serialize/parse | forks on field widths (u16 count, `uint16[16]` histogram, offset 34) |
| Code stream compress/decompress/validate | forks (LE u16 codes, escape 65,535 + one u16 literal in 0–255) |
| `ColumnChunkWriter` / `ColumnChunkReader` plumbing | **unchanged**, if the table type is abstracted — see below |

Suggested shape: make `FsstSymbolTable` an abstract base with `FsstSymbolTable8` and
`FsstSymbolTable16` subclasses, rather than a type-tagged class with internal branching.
The reader already threads one nullable `FsstSymbolTable?` through seven signatures
(`ReadDataPageV1/V2`, the two `*FromEntry` variants, the two `TryReadFixedListPage*`
probes, and `DecodeValues`) plus `ColumnPageMap`; a base class keeps every one of those
untouched. The virtual call lands once per page, not per value, so it is not on a hot path.

Numbers that differ: worst-case *compression* expansion is 4x rather than 2x (§5.3 — a byte
that escapes costs a 2-byte marker plus a 2-byte literal), so the `MaxCompressedLength`
bound and the `MaxArrayLength` guard in `FsstCompressedColumn.TryCompress` need the 4x
figure. The *decompression* bound stays 8x either way: FSST8 is one byte per code expanding
to at most 8, FSST_16 is two bytes per code expanding to at most 16.

### How does a writer choose 8-bit or 16-bit?

A question FSST8 did not raise, and the one real design decision here. Appendix C says
FSST16 wins on compression ratio, while note 1 says FSST8 may still be preferred for encode
time on low-cardinality data — so there is no universally right answer.

Recommendation: **ship an explicit `ByteArrayEncoding.Fsst16` member first, and no policy.**
That mirrors how `Fsst` shipped, and it inherits the per-column override machinery
(`GetByteArrayEncoding`) for free. Deciding automatically before there is data on this
codebase's own workloads would be guessing in the writer.

Two options considered and not recommended yet:

- *Train both and keep the smaller.* This is what the per-page offset-encoding choice does,
  but the cost is not comparable: offsets are re-encoded, whereas this means training a
  second symbol table and compressing the entire chunk again. Training is the expensive
  half of the writer's byte-array work.
- *Escape density heuristic.* Cheaper and worth revisiting: the chunk is already fully
  compressed with the 8-bit table before any page is written, so the fraction of emitted
  codes that are escapes is free to measure. A high escape rate with a saturated 255-symbol
  table is exactly the signal that a wider code space would pay, and only then is a retrain
  at 16 worth attempting. Needs a threshold picked from measurement, not from intuition.

### What Clast.Fsst has to provide

Per the issue: `Fsst16Encoder.BuildSymbolTable(rows, zeroTerminated, maxSymbolLength)` →
`SymbolTable16`, plus `TryCompress` / `CompressBatch` / `MaxCompressedLength`, and
`Fsst16Decoder.FromSymbols(lengths, symbols)` / `MaxDecompressedLength` /
`TryDecompressBatch`. `ExportRaw` needs **16-byte slots**, not the 8-byte slots the
existing `SymbolTable.ExportRaw` uses.

The code-renumbering trap applies unchanged and is the thing most likely to be forgotten:
whatever order the 16-bit trainer assigns codes in, they must be counting-sorted into
ascending length order and every emitted code remapped, because the histogram *is* the
length information. The remap table becomes `ushort[65536]`.

### Tests

Mirror the FSST8 set, with one honest gap to call out: **§6's worked examples are FSST8
only.** There is no FSST_16 example to decode, so the test that currently pins the
implementation to the specification rather than to its own encoder has no counterpart. A
hand-built FSST_16 page derived from §3.3 and §4.7 is still worth having, but it must be
commented as *derived from the rules*, not quoted from the document — otherwise it reads
like external corroboration when it is not.

Also worth an explicit test: an FSST_16 table containing only short symbols, serialized and
reparsed, to pin the "histogram entries 9–16 are zero and that is legal" property above.
