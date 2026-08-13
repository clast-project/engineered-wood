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
- `SymbolTableType.FSST` (8-bit codes) only. `FSST_16` is recognized and rejected with a
  clear error rather than misread.
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
