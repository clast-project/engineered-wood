# Resuming the constant-array and value-widening builder cleanups (tiers 2 and 3)

Handoff for the two remaining items from the Arrow-builder audit whose first item landed as
`ArrowCompute` (commits `04eaac4` and `9fd512b` on master). Read this first: it records what already
landed and why, the line numbers as they stand *after* that landing, and several facts that were
established by measurement or by reading Apache.Arrow's actual behaviour rather than reasoned from
first principles.

**These are two independently landable commits, deliberately written up together.** Both edit
`src/EngineeredWood.DeltaLake.Table/TypeWidening/ValueWidener.cs` — tier 2 deletes its null-array
helpers, tier 3 rewrites its widening methods, and `WidenBatch` calls into both. Doing them in
separate sessions means reading and re-reasoning about that file twice, with adjacent edits.

## What already landed, and the pattern to follow

`src/EngineeredWood.Core/Arrow/ArrowCompute.cs` is a **public** static class in the
`EngineeredWood.Arrow` namespace holding `Take` — a row gather built from raw Arrow buffers rather
than through the typed `XArray.Builder` classes. It replaced four hand-rolled per-type gathers
(752 lines deleted for 15) in `PartitionUtils`, `DeletionVectorFilter`, `RecordBatchRowFilter` and
`NestedAssembler`.

Both remaining tiers are the same move applied to a different operation, and should extend the same
class. The argument for it is not primarily speed:

> A builder round-trips every value through its .NET surface type, which is lossy for timestamps and
> decimals, and the builder for a narrower type silently discards the source's type parameters (unit,
> timezone, precision, scale).

Tier 1 found four real defects hiding behind that, including silent data corruption on partitioned
writes. Expect the same class of finding here rather than only allocation wins.

`test/EngineeredWood.Core.Tests` exists now (287 tests, `net10.0;net8.0;net472`) and is the right home
for tests of anything added to `ArrowCompute`. Its `Arrow/RawArrays.cs` helper builds arrays straight
from buffers and is what you need to construct inputs the builders cannot express — a non-zero logical
offset, an unknown null count, a value too wide for the .NET surface type.

### Facts worth not re-deriving

- **`ArrowBuffer.Empty` + `nullCount: 0` is the right shape for a column with no nulls.** It lets
  Arrow skip the per-element validity check downstream. `ArrowCompute`'s private `ValidityWriter`
  does this by allocating the bitmap only once a null actually appears.
- **`ArrowArrayFactory.BuildArray` reconstructs extension-typed `ArrayData` correctly**, including as
  a struct child. This was an open question in tier 1 and is now covered by a test. It matters here
  because both `MakeNullArray` and `ValueWidener.BuildNullArray` special-case extension types.
- **netstandard2.0 cannot construct a `HalfFloatArray` at all** — `ArrowArrayFactory.BuildArray`
  throws `"Half-float arrays are not supported by this target framework"`. A type-complete null-array
  factory has to account for that. `ArrowCompute` needs no `#if` because its width table keys off
  `HalfFloatType` (present on every target); matching on the *array class* is what forces a guard.
- **`Apache.Arrow.ExtensionArray` does not derive from `Apache.Arrow.Array`** and has no `Slice`.
  `ExtensionType.CreateArray` is abstract and returns `ExtensionArray`, so every conforming extension
  array *is* one.
- **`DurationType` exposes only static per-unit instances**, no public constructor.
- **`IntervalType` is deliberately absent from `ArrowCompute.FixedWidthBytes`.** No caller produces
  interval columns and an unverified width truncates silently. `TakeTypeMatrixTests` pins the throw.

## Tier 2 — row-at-a-time loops that build a constant

Four sites loop `length` times appending a value that is known before the loop starts. All produce one
of two buffer shapes, which are inverses of each other and want **one helper each** rather than one
flag-driven helper:

| Column | Validity buffer | `nullCount` |
| --- | --- | --- |
| constant, non-null | none (`ArrowBuffer.Empty`) | `0` |
| all-null | all-zero bitmap | `length` |

### 2a. `PartitionUtils.BuildConstantArray` — line 270, 11 `case` arms

`src/EngineeredWood.DeltaLake.Table/Partitioning/PartitionUtils.cs`, called from lines **131**
(`AddPartitionColumns`) and **189** (`AppendPartitionColumns`).

> Line numbers moved: this was at 461 before tier 1 deleted 192 lines from the file.

**This is the hot one.** It runs on every batch of every file in a partitioned scan, once per
partition column, appending the same parsed value `length` times through a builder. Replacement: parse
once, then one `byte[length * width]` filled via `Span.Fill` for fixed-width types; for strings the
value bytes are the pattern repeated and the offsets are just `i * len`, both computable without a
loop over values.

Because it is a read hot path, this is the item with the largest behavioural surface of the three, and
worth benchmarking rather than assuming — `test/EngineeredWood.DeltaLake.Benchmarks` exists.

### 2b. `SchemaEvolution.MakeNullArray` — line 116, 20 `AppendNull`-loop arms

`src/EngineeredWood.DeltaLake.Table/SchemaEvolution.cs`. Called from lines **50** and **97**.

**This is the richest of the four and the consolidation should move toward its behaviour, not away
from it.** It already handles extension types (line 124), structs (169) and lists (177) recursively,
which `ValueWidener`'s copies do not. Two internal details:

- Line 175-176 builds list offsets with `new ArrowBuffer.Builder<int>(length + 1)` then appends `0`
  in a loop — a zeroed `byte[]` wrapped in an `ArrowBuffer` needs neither.
- Line 190-191 hand-rolls the all-zero validity bitmap but still loops `bitmap.Append(0)` per byte.

**`MakeNullArrayPublic` at line 113 is the same smell as the `TakeRowsPublic` that tier 1 retired** —
an `internal` one-line delegate to the `private MakeNullArray`, named after its accessibility,
existing only so `ValueWidener.cs:438` can reach it across a class boundary. A shared factory on
`ArrowCompute` retires it the same way. Note it is `internal`, not `public`, so unlike `TakeRowsPublic`
there is no downstream `fabricator-extension` breakage to weigh.

### 2c. `ValueWidener.BuildNullArray` — line 422, plus 8 `BuildNull*` at 443-458

`src/EngineeredWood.DeltaLake.Table/TypeWidening/ValueWidener.cs`. Called from `WidenBatch` line
**56** for a column missing from the source file.

These 8 are strictly redundant with 2b and should simply be **deleted**, not rewritten — the method
already delegates to `SchemaEvolution.MakeNullArrayPublic` for the extension case (line 438), so it is
half-migrated already. Its `_ => BuildNullString(length)` fallback (line 439) has the same
wrong-type-for-the-schema problem tier 1 fixed in `DeletionVectorFilter.CreateEmptyArray`: a null
Timestamp or Decimal column comes back typed as String.

### 2d. `ArrowRowEvaluator` — `src/EngineeredWood.Expressions.Arrow/ArrowRowEvaluator.cs`

Lowest priority; smaller and on a different path. `BuildAllNullStrings` (588) is an
`AppendNull` loop; `Constant` (608) and `Repeat` (615) are `for` loops that are `Array.Fill`;
`MaterializeAsArray` (472) dispatches to 7 per-type builder loops in the 405-470 region.

## Tier 3 — `ValueWidener` integer and float widening

`src/EngineeredWood.DeltaLake.Table/TypeWidening/ValueWidener.cs`, three regions:

- **Integer Widening**, lines **103-171** — 6 methods: int8→16, int8→32, int8→64, int16→32, int16→64,
  int32→64.
- **Float Widening**, lines **173-219** — 4 methods: float→double, int8→double, int16→double,
  int32→double.
- **Date → Timestamp**, lines **221-241** — `WidenDate32ToTimestamp`.

All ten integer/float methods are the same shape: `for` over `source.Length`, `IsNull` probe,
`b.Append(source.GetValue(i)!.Value)`. Each is a widening element copy: read the source value buffer
as a span of the narrow type, write a span of the wide type.

**Scope note: the Decimal Widening region (lines 243-398) is already buffer-based** —
`WidenDecimal128`, `WidenIntToDecimal` and `WidenLongToDecimal` build `resultBytes` with an
`ArrowBuffer.BitmapBuilder`. Leave it alone. Tier 3 is only the three regions above.

`WidenDate32ToTimestamp` (223) allocates an epoch `DateTime` and round-trips each value through
`DateTimeOffset` per row for what is arithmetic on the stored `int` days. Deriving the target unit's
multiplier once and multiplying is faster and avoids the round-trip.

**This one is a performance item only — it is *not* the unit bug tier 1 found in
`PartitionUtils.TakeRows`, and it was checked rather than assumed.** The difference is where the value
starts: `TakeRows` read a raw stored `long` and hardcoded an interpretation of its unit, whereas this
method starts from `days`, which carries no unit, and hands a `DateTimeOffset` (an absolute instant) to
a builder that knows its own target unit and converts correctly. A date also has no sub-day precision
to lose. Do not go looking for corruption here.

### The validity-buffer subtlety

Widening never changes nullness, so the source's validity buffer can be **shared by reference** —
`ArrowBuffer` is a readonly struct over `ReadOnlyMemory`, so this is safe and means no bitmap rebuild
at all.

**But it is only valid when `source.Data.Offset == 0`.** The bitmap is indexed by *physical* slot, so
reusing it requires the new array to carry the same offset — which in turn requires allocating
`(offset + length)` wide slots rather than `length`. For a non-zero offset, either re-derive the
bitmap for offset 0, or allocate the larger buffer and preserve the offset. Getting this wrong reads
the wrong rows' null flags, silently. Tier 1's `Offset_ReadsCorrectValidityBits` in
`TakeDimensionTests` is the template for testing it.

## Suggested order

1. **Run the interop tiers against what already landed, first.** Tier 1's timestamp fix changed the
   bytes written for millisecond and microsecond timestamp columns in partitioned tables, and the
   Spark / delta-rs tiers have *not* been run against it. They need `EW_REQUIRE_*` plus the local
   `JAVA_HOME`/`HADOOP_HOME` toolchain — see `reference_spark_interop_toolchain`. If those surface
   anything, better to know before layering more onto the same files.
2. Tier 2b + 2c together — they are one consolidation, and 2c is a deletion.
3. Tier 2a — separate commit, benchmark it, largest behavioural surface.
4. Tier 3.
5. Tier 2d if it still seems worth it.

## Definition of done

- New helpers live on `ArrowCompute` with XML docs (they are public API — Core is packaged, currently
  `0.1.0`, and `src/Directory.Build.props` notes the public API is preliminary pre-1.0).
- Direct tests in `test/EngineeredWood.Core.Tests`, using `RawArrays` for inputs, covering the type
  matrix *and* the offset / all-null / no-null / unknown-null-count shapes.
- Any behaviour change proven against the old code before deleting it. Tier 1's timestamp corruption
  was confirmed by replicating the old arm and measuring it (microsecond `1700000000000123` →
  `1700000000000000`; millisecond `1700000000123` → `1700000000`), not by reading it. A test that
  passes against both implementations is worthless.
- Full matrix green: Parquet 715, DeltaLake.Table 441, DeltaLake 210, Core.Tests 287/287/285, Lance
  Table 94 — plus `net472` for anything touching a `#if NET6_0_OR_GREATER` path, since net472
  consumes Core's netstandard2.0 build.
