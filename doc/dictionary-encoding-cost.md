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

## Deferred — A. Grow the hash table instead of pre-sizing it

`TryEncodeByteArray` and `TryEncodeFixedLenByteArray` open with:

```csharp
var table = new BytesHashTable(Math.Max(16, maxCardinality * 2));
```

`maxCardinality` is 20% of the row count, so a 1M-row column sizes the table to 400,000 slots, which
rounds to 524,288 and costs **8,388,608 bytes at 16 bytes a slot** — allocated before the first row is
hashed, and identical whether the column holds twelve distinct values or a million. `TryEncodeFixed` does
not have this problem, because `Dictionary<T,int>` grows geometrically; the penalty falls only on string
and binary columns, which are the ones dictionary encoding usually serves best.

`WriteSingleRowGroupAsync` runs columns under `Parallel.For`, so twenty string columns is ~168 MB of hash
table live simultaneously, independent of the data.

The fix is geometric growth, and the catch is that it is a contract change rather than a tuning one:
`BytesHashTable` has no resize path and no load-factor guard, so `GetOrAdd` spins forever if the table
fills. The generous pre-sizing is currently load-bearing. Anything that shrinks it has to add growth and
rehashing first. (The run-end-encoded arm sizes from the run count instead, which is an exact upper bound
on distinct values — see `doc/ree-constant-column-encoding.md`. That trick does not generalize, because
the per-row arms have no such bound before they start.)

This is the change that needs no API and no producer knowledge, and it helps the low-cardinality columns
that currently over-pay. It is the one to do next.

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
