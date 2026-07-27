# What a declined dictionary costs, and the two ways left to stop paying it

The per-column switch has landed. This note records the measurements behind it and the two larger changes
that were deliberately deferred, so neither is rediscovered from scratch.

## The shape of the problem

`DictionaryEncoder.TryEncode` analyzes a whole column before anything is written. It gives up on either of
two conditions — distinct values reaching `CardinalityThreshold` (20% of the non-null row count), or the
dictionary page passing `DictionaryPageSizeLimit` (1 MB) — and returns null. `WriteNonDictionaryColumn`
then reads the column again from the top and writes it PLAIN. **Everything the analysis pass built is
discarded.**

For a column that was never going to dictionary-encode, that is a full 20%-of-the-rows hashing pass, an
8.4 MB hash table, a 4 MB index array and ~200,000 `byte[]` copies, all thrown away.

## Measured

1M-row string columns, default write options, minimum of 5 after 2 warm-ups.

| | dictionary tried | dictionary disabled | cost of the attempt |
| --- | --- | --- | --- |
| high-cardinality (all distinct) | 47,652,256 B / 78.23 ms | 30,179,000 B / 65.02 ms | **+17,473,256 B / +13.20 ms** |
| low-cardinality (12 distinct) | 15,587,056 B / 35.40 ms | 19,140,648 B / 18.09 ms | −3,553,592 B / +17.31 ms |

The high-cardinality file is byte-identical either way (10,306,078 B), so the whole attempt buys nothing.
The low-cardinality row is the counterweight and the reason none of this is a case for disabling
dictionaries generally: there the dictionary saves 3.5 MB of allocation and shrinks the file 8×
(239,231 → 29,736 B), which is worth its 17 ms.

On a realistic two-column table (one low-cardinality, one all-distinct), naming the distinct column in
`ColumnDictionaryEnabled` saves **17,450,424 B and 20.52 ms** per 1M rows and produces an identical file.

## Done — A. Grow the hash table instead of pre-sizing it

`TryEncodeByteArray` and `TryEncodeFixedLenByteArray` used to open with:

```csharp
var table = new BytesHashTable(Math.Max(16, maxCardinality * 2));
```

`maxCardinality` is 20% of the row count, so a 1M-row column sized the table to 400,000 slots, which
rounds to 524,288 and cost **8,388,608 bytes at 16 bytes a slot** — allocated before the first row was
hashed, and identical whether the column held twelve distinct values or a million. `TryEncodeFixed` never
had the problem, because `Dictionary<T,int>` grows geometrically; the penalty fell only on string, binary
and FLBA columns.

`BytesHashTable` now starts at 16 slots and doubles, holding the same load-factor ceiling of one half that
the old sizing guaranteed — so probe lengths are unchanged and only the memory moves. The run-end encoded
arm still passes a hint, because the run count is an exact upper bound on distinct values and an exact
bound never rehashes.

### Measured, A/B in one process

Same data, same build, only the sizing differs. Minimum of 5 after 2 warm-ups.

| shape | pre-sized | grown | delta |
| --- | --- | --- | --- |
| 1M rows, 12 distinct | 15,579,256 B / 40.14 ms | 7,191,488 B / 39.38 ms | **−8,387,768 B (−54%)** |
| 1M rows, 50,000 distinct | 28,124,320 B / 48.35 ms | 23,930,696 B / 49.29 ms | −4,193,624 B (−15%) |
| 1M rows, all distinct | 36,588,312 B / 48.17 ms | 32,396,488 B / 47.29 ms | −4,191,824 B (−11%) |
| 20 columns × 200k rows, 8 distinct | 67,343,496 B / 19.45 ms | 25,418,952 B / 21.37 ms | **−41,924,544 B (−62%)** |
| FSB(4), 1M rows, 1,000 distinct | 15,666,488 B / 22.12 ms | 7,343,664 B / 20.06 ms | −8,322,824 B (−53%) |
| FSB(4), 1M rows, 150,000 distinct | 33,773,248 B / 47.24 ms | 42,162,680 B / 51.11 ms | **+8,389,432 B (+25%)** |
| FSB(4), 1M rows, 250,000 distinct | 42,477,008 B / 43.61 ms | 50,867,352 B / 46.27 ms | **+8,390,344 B (+20%)** |

### The last two rows are a real regression, and it is bounded

A doubling series sums to about one extra copy of the final table, so a column that grows all the way to
the size it would have been pre-sized at pays roughly twice. That only happens when the **cardinality cap
is the binding constraint** rather than `DictionaryPageSizeLimit`.

For BYTE_ARRAY it never is. Fitting 200,000 entries inside a 1 MB dictionary page needs ~5 bytes each, and
a BYTE_ARRAY entry carries a 4-byte length prefix — so the values would have to be ~1 byte, and there are
not 200,000 distinct 1-byte values. The page limit always trips first, at a table far below the cap, which
is why the all-distinct string row above still *improves* by 4.2 MB.

It is reachable for narrow FIXED_LEN_BYTE_ARRAY columns, which have no length prefix: FSB(4) fits 262,144
entries in the page limit, so the 200,000 cap binds. That is the shape in the last two rows. Accepted
rather than mitigated, because:

- those columns are being **discarded** anyway — the dictionary is declined and the column written PLAIN,
  so it makes an already-wasteful path ~20% more wasteful, in exchange for halving the productive one;
- `ColumnDictionaryEnabled` is the escape hatch for a producer that has such a column;
- the alternatives all cost elsewhere. Growing by 4× would cut the series to ~⅓ of final but overshoot
  every mid-sized column; jumping straight to the cap once the table gets large would re-introduce the
  8.4 MB for every column that stops below it.

Timings moved by ±4 ms with no consistent direction, on a shared machine — treat them as flat.

## Deferred — B. Incremental fallback, the parquet-cpp model

parquet-cpp accumulates its dictionary as values arrive and, when it outgrows
`dictionary_pagesize_limit`, **falls back to PLAIN for the remainder of the column chunk**, keeping the
dictionary-encoded pages it already produced. Nothing is discarded, because falling back is the design
rather than a failure. The switch happens at a page boundary (a page header declares one encoding), and
the dictionary page has to be the chunk's first page while its contents are not final until the fallback
— which is why that writer buffers its data pages.

**EW does not have that constraint.** `ColumnChunkWriter` receives the whole column and assembles the
chunk in a `MemoryStream` regardless, so a mixed chunk would be cheap to build: the analysis pass already
walks rows in order and knows the row R where it gave up, and today it discards that. Dictionary pages for
`[0, R)` and PLAIN pages for `[R, N)` is nearly the whole change; the encodings list already carries both
`Plain` and `RleDictionary` (the dictionary page is itself PLAIN-encoded).

Two reasons it was NOT done as part of this work:

- **On a uniformly high-cardinality column the mixed chunk makes the file worse.** Dictionary-encoding the
  first 200,000 all-distinct rows writes every one of those values into the dictionary page — the bytes
  PLAIN would have written — and then adds an index per row on top. EW's all-or-nothing decision produces
  the better file on exactly the column measured above. parquet-cpp accepts the overhead because streaming
  leaves it no way to reconsider the prefix; EW has the whole column and does not have to.
- **The memory rationale does not transfer.** In a streaming writer the dictionary is the one unbounded
  structure, so capping it caps the writer. EW has already materialized the entire column in Arrow before
  `WriteColumn` is called, and the dictionary is a fraction of that total.

Where it genuinely wins is data whose cardinality changes partway through — a compacted file concatenating
partitions, a categorical that shifts regime. There the dictionary prefix is real value that
all-or-nothing throws away. That is the case to justify it on, not waste.

Before borrowing the design, read parquet-cpp's `ColumnWriter::FallbackToPlainEncoding` and its
page-buffering: the sequence above is recalled, not verified against the source.

## Also noted, not addressed

- **A per-column encoding request is silently overridden by the dictionary.** Setting
  `ColumnEncodings["c"] = DeltaByteArray` does not skip the analysis, and if the dictionary succeeds the
  requested encoding is never used — `ByteArrayEncoding` is only consulted in `WriteNonDictionaryColumn`.
  This matches parquet-mr's precedence, so it is conventional rather than wrong, but it does mean there is
  no way to force PLAIN or DELTA_BYTE_ARRAY on a column that would dictionary-encode.
- **`BufferedParquetWriter` builds its dictionary unconditionally** and only consults the enabled flag at
  flush time, so turning dictionaries off does not save it the accumulation — it saves only the use. The
  per-column switch inherits that behaviour.
- **`EncodingStrategyResolver.ShouldAttemptDictionary` has no callers.** It duplicates the check in
  `DictionaryEncoder.TryEncode` and would need the per-column resolution if it were ever wired up.
- **Every distinct value is copied twice.** `BytesHashTable.GetOrAdd` stores its own `key.ToArray()` for
  comparison, and the caller then does `uniqueEntries.Add(valueBytes.ToArray())` for the dictionary page —
  two allocations per distinct value, in all three arms. Collapsing them needs `GetOrAdd` to be handed the
  array the caller is going to keep, which is a call-site change in each arm rather than a change to the
  table.
