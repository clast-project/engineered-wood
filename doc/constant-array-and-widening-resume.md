# Arrow-builder audit — outcome

**Status: complete.** This file was the handoff for tiers 2 and 3 of the Arrow-builder audit (tier 1 landed
as `ArrowCompute.Take` in `04eaac4` / `9fd512b`). Everything it planned has landed; what follows is the
record, including the one item deliberately not done and why. Kept under its original name so the references
to it from `cb919a8` and from the session memory still resolve.

## What landed

| Commit | Tier | Change |
| --- | --- | --- |
| `0d7149c` | step 1 | Cross-validated tier 1's timestamp fix against delta-rs |
| `6409d2d` | 2b + 2c | `ArrowCompute.MakeNullArray` — the all-null factory |
| `6407c20` | 2a | `ArrowCompute.Repeat` — the constant factory, plus two decimal fixes |
| `a55e652` | 3 | `ArrowCompute.Widen` — widening by value-slot copy |

`ArrowCompute` now holds four kernels — `Take`, `MakeNullArray`, `Repeat`, `Widen` — sharing one
`FixedWidthBytes` width table, all producing offset-0 arrays and reusing the caller's `IArrowType` verbatim.

## Defects found

The audit's premise was that a builder round-trip hides correctness bugs, not just allocations. It held:

1. **`ValueWidener.BuildNullArray` typed columns wrong** (2c). Seven per-type arms behind
   `_ => BuildNullString(length)`, so a missing Timestamp, Decimal, Date, Binary or struct column was
   backfilled as a `StringArray` — an array contradicting the schema of the batch it was placed into, with
   nothing raised. Same shape as the `DeletionVectorFilter.CreateEmptyArray` fallback tier 1 fixed.

2. **Partition decimals were silently rounded** (2a). `decimal.Parse` does not fail on excess precision — it
   rounds and reports success. Measured: a `decimal(38,10)` partition value of
   `1234567890123456789012345678.1234567890` materialised as `…678.1000000000` for every row of the
   partition.

3. **Partition decimals wider than `System.Decimal` failed the read** (2a). A `decimal(38,0)` value above
   `decimal.MaxValue` fits Decimal128's 128 bits and is legal Delta, but `decimal.Parse` threw
   `OverflowException`. Measured on `99999999999999999999999999999999999999`.

Tier 3 was, as the handoff predicted, performance-only — `Date32 → Timestamp` starts from a day count, which
carries no unit to misread and has no sub-day precision to lose.

## The interop gap step 1 actually found

Both tiers ran green (delta-rs 13, Spark 25) but **could not have caught tier 1's bug**: there was no
timestamp coverage anywhere under `test/…/Interop`, and every partition column in the suite was the string
`region`. `0d7149c` closes that from both sides — a partitioned table with a timestamp *data* column (the
gather tier 1 fixed) and a timestamp *partition* column (`FormatTimestampPartitionValue`, which had no test
of any kind). New driver command `read_epoch_micros` reports exact microseconds rather than a rendered
datetime, because a truncating format hides precisely the digits at issue.

## Not done: tier 2d (`ArrowRowEvaluator`), and why

The handoff listed this as "if it still seems worth it". It is not:

- `MaterializeAsArray`'s seven per-type loops are **not** the constant-array pattern. They materialise
  per-row *varying* values out of a `LiteralValue?[]`, and the data is already in .NET surface form by the
  time it arrives, so there is no fidelity to recover — only a rewrite with real risk. The cases where
  metadata *would* be lost (decimal, temporal) already build buffers directly in `BuildDecimalArray` /
  `BuildTimestampArray`.
- `BuildAllNullStrings` is a genuine `AppendNull` loop and would be a one-line call to the new factory — but
  `EngineeredWood.Expressions.Arrow` does not reference `EngineeredWood.Core`, and `Core` is the base
  library with no project references of its own. Adding that edge makes Snappier / ZstdSharp / K4os.LZ4
  transitive dependencies of a lightweight expression-evaluation package, for four lines. Wrong trade.
- `Constant` and `Repeat` fill `bool?[]` / `LiteralValue?[]` — managed arrays, not Arrow buffers.
  `Array.Fill` is marginally tidier and is unavailable on netstandard2.0, which this project targets.

Revisit only if `Expressions.Arrow` gains a `Core` dependency for some other reason.

## Measurements

Per-array construction, versus the builder loops replaced (`ConstantArrayBenchmarks`,
`WideningBenchmarks`; ratios at a 1024-row batch, then 65536):

| Operation | 1024 | 65536 |
| --- | --- | --- |
| Int64 constant | 20× | 3.4× |
| String constant | 24× | 4.0× |
| Timestamp constant | 30× | 4.0× |
| All-null Int64 | 21× | 27× |
| Int32 → Int64 widen | 20× | 5× |
| Date32 → Timestamp widen | 31× | 8× |

Allocation roughly halves throughout, and the builders' repeated internal growth no longer provokes
Gen1/Gen2 collections at 1024 rows. The fixed-width ratios compress at 65536 as the work becomes
bandwidth-bound; all-null does not, because it never writes a value buffer.

## Facts still worth not re-deriving

Carried forward from the original handoff, plus what this work added:

- **`ArrowBuffer.Empty` + `nullCount: 0` is the no-null shape**, and an *allocated* all-zero bitmap with
  `nullCount: length` is its inverse. An absent bitmap means all-valid, so it is not optional in the
  all-null direction.
- **A validity bitmap is indexed by PHYSICAL slot.** `Widen` shares the source's bitmap by reference when
  `Data.Offset == 0` (`ArrowBuffer` is a readonly struct over `ReadOnlyMemory`) and re-derives it otherwise.
  Verified load-bearing: removing the `Offset == 0` condition fails exactly the two offset tests and leaves
  the other 23 passing — the shape of a bug that reaches production.
- **A declared null count can be −1** (unknown) and must be counted, not trusted.
- **netstandard2.0 cannot construct a `HalfFloatArray`.** For `Take` that is a non-issue (no input can
  exist); for `MakeNullArray` it is reached from a *schema*, so it throws with an explanation instead.
- **`Apache.Arrow.MapType` does not derive from `ListType`**, so arm order between them is free.
- **`IntervalType` stays absent from `FixedWidthBytes`.** No caller produces interval columns, and an
  unverified width truncates silently.
- **`DeltaLiteralDecoder` and the partition materialiser now share `DecimalText`** for digit-exact decimal
  parsing. They differ only in what they do with an unparseable value: pruning treats it as unknown,
  materialising a column has to fail.
