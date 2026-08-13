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
- Both symbol table types: `FSST` (8-bit codes) and `FSST_16` (16-bit), selected by
  `ByteArrayEncoding.Fsst` and `ByteArrayEncoding.Fsst16`. Both are written as encoding 11 —
  the symbol table page's type field is what tells them apart (§2.3), so the data pages are
  byte-identical in structure either way.
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
trained on URLs came back with lengths `2, 2, 1, 3, 3, ...`. `FsstSymbolTable8.TryTrain`
therefore counting-sorts the trained codes by length and carries a 256-entry remap that
`Compress` applies to every emitted code. Escape markers (255) and the literal byte that
follows them pass through untouched — the literal is a raw byte, not a code.

This applies to the **8-bit** trainer only. The 16-bit one already emits codes in ascending
length order, so `FsstSymbolTable16.TryTrain` verifies that and skips the renumbering.

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

- FIXED_LEN_BYTE_ARRAY. §1.3 is BYTE_ARRAY only; the doc's comment thread floats FLBA for
  UUIDs as possible future work.
- Composite dictionary + FSST (§9.2), which the spec itself says needs a further spec change.
- **Automatic choice between the two widths.** `ByteArrayEncoding.Fsst16` is an explicit
  opt-in, as `Fsst` is; the writer never picks for you. See below.

## FSST_16

Sketched while the FSST8 work was fresh and built from that sketch once
[clast-project/fsst#1](https://github.com/clast-project/fsst/issues/1) shipped a 16-bit
trainer in Clast.Fsst 0.3.x. The reasoning below is kept because it is what the code encodes;
where the plan and the result differ, the difference is called out.

### The §1.2 ambiguity — resolved, and it never reached the wire format

**Resolved 2026-08-13: the spec author clarified §1.2 to say that when the type is FSST_16,
codes are `uint16` and symbols may be 1–16 bytes, and §3.3's table now spells out
`length_histogram` as a `uint16[16]` of size 32.** So §1.2's "1 to 8 bytes" was FSST8 prose,
as suspected, and 16 wins. The clarification confirms this implementation's *reader* exactly —
the 34-byte body header and the 16 honoured histogram slots were already written that way — and
required no change to it.

What follows is why that resolution moved nothing, and why the writer still caps at 8.

§3.3 gives FSST_16 a `uint16[16]` histogram at offset 2 and `symbol_data` at offset 34,
**unconditionally**. It does not shrink when symbols happen to be short. So if §1.2 wins and
a writer may only emit symbols of 1–8 bytes, the result is simply a table whose histogram
entries for lengths 9–16 are zero — a legal, ordinary FSST_16 table. §3.6's reconstruction
loop runs `L in 1..16` and does nothing for the empty slots.

Consequence: **`Serialize`, `Parse` and every validation rule are identical under either
reading.** The only thing that changes is the cap handed to the trainer. So the Parquet side
can be built in full before the spec answers, and the answer later moves one argument.

That is what happened, and it is why the resolution cost nothing: no serialized byte, no
validation rule, and no already-written file changed meaning when 16 won.

### Why the writer still caps at 8

`FsstSymbolTable16.TrainedMaxSymbolLength` is **8**. That was originally the cautious choice —
the length legal under either reading of the contradiction — but the spec has since resolved to
16, so caution is no longer the reason. It stays at 8 on measurement.

Crucially, the clarified §1.2 says symbols *may* be 1–16, not that they must be: a table whose
histogram entries for lengths 9–16 are zero is an ordinary, conformant FSST_16 table (which is
what `SymbolTable_ShortSymbolsOnly_LeavesUpperHistogramSlotsZero` pins). So the cap is a tuning
knob, not a conformance question.

And raising it is **not** a free win. Compression ratio against the cap, measured over 4,000
values per corpus with the symbol table body counted in:

| corpus | cap 7 | cap 8 | cap 12 | cap 14 | cap 15 | cap 16 |
|---|---|---|---|---|---|---|
| URLs | 2.70x | 1.88x | 2.91x | **3.54x** | 2.01x | 1.41x |
| CDN URLs | 2.02x | 2.02x | 2.02x | 1.72x | 1.96x | **2.28x** |
| file paths | 2.05x | 2.47x | 2.19x | 2.02x | 2.18x | **2.54x** |
| UUIDs | — | 2.46x | 2.46x | — | — | **3.26x** |
| log lines | — | 2.09x | 2.09x | — | — | **2.35x** |

Two things to take from this. First, the best cap is corpus-dependent, so no single value is
right — 16 would cost the flagship URL case 60% against its best, and land *below* cap 8.
Second, and more importantly, **the response to the cap is erratic rather than monotone**: on
the URL corpus it swings 3.54x → 2.01x → 1.41x across caps 14, 15 and 16 on identical data, at
an essentially unchanged symbol count. A greedy trainer given a larger search space should not
lose 60% by being allowed *longer* symbols — it can always pick shorter ones — so this looks
like cap sensitivity in Clast.Fsst's FSST16 trainer rather than a property of FSST itself.

Until that is understood, 8 is the stable, conservative choice, and tuning the cap here would
be overfitting to whichever corpus was measured last. **Do not raise this constant on the
strength of the spec resolution alone** — the spec permits 16, it does not recommend it.

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

Shape as built: `FsstSymbolTable` is an abstract base with `FsstSymbolTable8` and
`FsstSymbolTable16` subclasses, rather than a type-tagged class with internal branching.
The reader already threaded one nullable `FsstSymbolTable?` through seven signatures
(`ReadDataPageV1/V2`, the two `*FromEntry` variants, the two `TryReadFixedListPage*`
probes, and `DecodeValues`) plus `ColumnPageMap`; the base class kept every one of those
untouched, as predicted. The virtual call lands once per page, not per value.

Two members had to move onto the base that the sketch did not anticipate: the decoder itself
is a different type per width (`FsstDecoder` versus `Fsst16Decoder`), so `Decoder` became
`TryDecompressBatch` plus `MaxDecompressedLength`, and `MaxCompressedLength` went from static
to instance for the 2x/4x split below.

Numbers that differ: worst-case *compression* expansion is 4x rather than 2x (§5.3 — a byte
that escapes costs a 2-byte marker plus a 2-byte literal), so the `MaxCompressedLength`
bound and the `MaxArrayLength` guard in `FsstCompressedColumn.TryCompress` need the 4x
figure. The *decompression* bound stays 8x either way: FSST8 is one byte per code expanding
to at most 8, FSST_16 is two bytes per code expanding to at most 16.

### How does a writer choose 8-bit or 16-bit?

A question FSST8 did not raise, and the one real design decision here. Appendix C says
FSST16 wins on compression ratio, while note 1 says FSST8 may still be preferred for encode
time on low-cardinality data — so there is no universally right answer.

Shipped as recommended: **an explicit `ByteArrayEncoding.Fsst16` member, and no policy.**
That mirrors how `Fsst` shipped, and it inherits the per-column override machinery
(`GetByteArrayEncoding`) for free. Deciding automatically before there is data on this
codebase's own workloads would be guessing in the writer.

Clast.Fsst's own measurements point the same way and are worth repeating here, because they
cut against the assumption that a wider code space is simply better: on a 1.4 MB synthetic URL
corpus **FSST8 reaches 3.11x against FSST16's 2.27x**, and the gap widens on short name-like
values. Two bytes per code has to be paid for by symbols longer than two bytes. FSST_16 earns
its place where the vocabulary genuinely exceeds 255 symbols *and* the repeated substrings are
long — and, separately, because it is what the proposal specifies.

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

### What Clast.Fsst provides

**Clast.Fsst 0.3.1 is the minimum.** 0.3.0 introduced the 16-bit trainer but shipped a broken
8-bit compressor with it — its own documented roundtrip returned wrong bytes for most values,
which cost 9 tests here and 2 in Vortex until 0.3.1 fixed it. FSST_16 itself was never
affected. Note that 0.3.1's `AssemblyVersion` still reads `0.3.0.0`, so identify the binary by
package version rather than by assembly metadata.

Clast.Fsst supplies `Fsst16Encoder.BuildSymbolTable(rows, maxSymbolLength)` →
`SymbolTable16`, `TryCompress` / `CompressBatch` / `MaxCompressedLength`, and
`Fsst16Decoder.FromSymbols(lengths, symbols)` / `MaxDecompressedLength` /
`TryDecompressBatch`. `ExportRaw` uses **16-byte slots**, not the 8-byte slots
`SymbolTable.ExportRaw` uses.

**The code-renumbering trap turned out not to apply.** The sketch assumed the 16-bit trainer
would assign codes by gain, as the 8-bit one does, and that every emitted code would therefore
need remapping through a `ushort[65536]`. It does not: Clast.Fsst assigns FSST16 codes in ascending
symbol-length order already, which is exactly what §3.3 needs. `FsstSymbolTable16.TryTrain`
checks that the exported lengths are non-decreasing and, when they are, keeps the raw table
untouched — no remap allocated, no code rewritten. The counting sort is still there for when
they are not, because the histogram *is* the length information and a trainer that quietly
stopped promising that order would otherwise produce tables no reader could cut apart.

Two other properties of these tables matter to a writer: they always contain all 256
single-byte symbols, so they **never escape** and never expand beyond 2x in practice (the 4x
bound is still what `MaxCompressedLength` must use, since a foreign table may escape); and the
escape encoding is code 65,535 followed by the literal byte widened to a little-endian `u16`,
which `ValidateCodeStream` rejects if the literal exceeds 255.

### Tests

`Fsst16EncodingTests` mirrors the FSST8 set — 26 tests over the table body, the page body,
dispatch, and end-to-end files — with the one honest gap the sketch predicted: **§6's worked
examples are FSST8 only.** There is no FSST_16 example to decode, so the test pinning the
implementation to the specification rather than to its own encoder has no counterpart. The
hand-built table and page (`HandBuiltSymbolTableBody`, `DataPage_HandBuiltFromTheRules_Decodes`)
are commented as *derived from* §3.3, §4.4 and §4.7 — not quoted — so they are not mistaken for
external corroboration.

`SymbolTable_ShortSymbolsOnly_LeavesUpperHistogramSlotsZero` pins the "histogram entries 9–16
are zero and that is legal" property the §1.2 argument rests on, and
`SymbolTablePage_TypeFieldSelectsTheCodeWidth` pins the dispatch — that the type field, and
nothing in the data page, is what selects the code width.
