# Expression and Predicate Pushdown Design

## Overview

Several format readers and writers in EngineeredWood have an overlapping need for
expressions: trees of typed values, column references, comparisons, and boolean
combinators. The current and planned uses are:

| Use case | Format | Purpose |
|---|---|---|
| Manifest evaluation | Iceberg | Skip data files using min/max stats per field ID |
| Row group pruning | Parquet | Skip row groups using min/max stats and bloom filters |
| Stripe pruning | ORC | Skip stripes using stripe statistics (future) |
| Partition pruning | Delta Lake | Skip files whose partition values can't satisfy a filter |
| Stats-based file pruning | Delta Lake | Skip files using `AddFile.stats` min/max |
| CHECK constraints | Delta Lake | Validate every row satisfies a boolean expression on write |
| Generated columns | Delta Lake | Compute a column's value from an expression on write |
| Row-level delete / update | Lance | Select the rows a predicate-based DML statement touches |
| Post-filter | Any reader | Drop rows that survived coarse pruning but don't match |

This document covers the architecture for unifying these needs across formats and
the implementation phases to get there.

**Implementation status.** Phases 1–8 are implemented and shipping: the shared
`EngineeredWood.Expressions` library (tree, `LiteralValue`, `ExpressionBinder`,
`StatisticsEvaluator`), statistics-based pruning for Parquet (row groups + bloom
filters) and Delta Lake (partition + stats in one unified pass), Iceberg
migrated onto the shared library, and the Arrow-based row evaluator in
`EngineeredWood.Expressions.Arrow`.

Phase 9 shipped 2026-08-11: a hand-written Spark SQL tokenizer and parser in
`EngineeredWood.Expressions/Sql`, and a `SparkFunctionRegistry` in
`EngineeredWood.Expressions.Arrow/Spark` covering arithmetic, casts including
temporal ones, and the named string, pattern, date-part and conditional
functions. A constraint or generation expression now goes from text in table
metadata to a per-row Arrow result.

Phases 10–14 (Delta CHECK/generated-column wiring, Parquet column/offset index,
ORC stripe pruning) are not built. See the phase table at the bottom for scope;
the issue tracker, not this document, is authoritative for what is in flight.

**The wiring gap, which matters more than any unbuilt phase.** Nothing in `src/`
sets `ParquetReadOptions.Filter`. Row-group pruning and bloom probing are built
and tested, and unreachable from any table layer: `DeltaReadOptions.Filter`
prunes files and stops, Iceberg has no data-file read path at all. The cause is
that `Filter` is fixed when the `ParquetFileReader` is constructed, and Delta
holds one options record shared by the scan, CDF, DML and compaction paths — so
setting it there would prune row groups during OPTIMIZE's rewrite, which is data
loss. Tracked as
[#55](https://github.com/clast-project/engineered-wood/issues/55), and it blocks
the payoff of most of the rest of this document.

**Reading this document.** It was written before any of it existed, and the body
still describes the destination rather than the current tree — the code samples
below are design sketches, not extracts. Where a section describes something
that has not shipped, it now says so inline. The two places where the built
result diverges from the sketch are the row evaluator's consumers (see
"Used by") and its bounded column-type coverage.

When this document was first written, only Iceberg had an expression tree —
Parquet had a design but no implementation, and ORC, Delta, and the row-level
evaluator had nothing. The sections that follow describe the architecture that
was proposed then and has since been built out through Phase 8.

## Architecture

Two evaluation models share a single expression tree:

- **Statistics-based pruning** is coarse and three-valued (`MightMatch` /
  `CannotMatch` / `Unknown`). It runs against aggregated stats — min/max, null
  count, distinct count — for a file, row group, or stripe. No row data is read.
- **Row-level evaluation** is exact and two-valued. It runs against an Arrow
  `RecordBatch` and produces a `BooleanArray`, one bit per row. Used by CHECK
  constraints, generated columns, and post-filtering.

Both consume the same `Expression` tree. The split falls on a clean dependency
boundary: stats evaluation only needs typed value comparison; row evaluation
needs Arrow.

### Project layout

```
EngineeredWood.Expressions              (new — no Arrow, no format deps)
    │
    ├── Expression tree types
    ├── LiteralValue (typed scalar with cross-type promotion)
    ├── IStatisticsEvaluator<TStats> + generic three-valued logic
    └── ExpressionBinder (resolve unbound name refs against a schema)
        ↑
        ├── EngineeredWood.Iceberg            (already has this; migrates to consume)
        ├── EngineeredWood.Parquet            (predicate pushdown — this document's primary deliverable)
        ├── EngineeredWood.Orc                (stripe pruning — future)
        └── EngineeredWood.DeltaLake          (partition + stats pruning)

EngineeredWood.Expressions.Arrow        (new — depends on Expressions + Apache.Arrow)
    │
    └── IRowEvaluator + ArrowRowEvaluator
        ↑
        ├── EngineeredWood.DeltaLake.Table    (CHECK constraints, generated columns, post-filter)
        └── EngineeredWood.Parquet            (post-pruning row filtering — future)

EngineeredWood.Expressions/Sql          (hand-written, inside Expressions)
    │
    └── Parses Spark SQL expression strings into Expression trees
        ↑
        └── used by Delta Lake for CHECK constraints / invariants / generated columns
```

The parser has no dependencies of its own — tokenizer and recursive-descent
parser over `std`-equivalent BCL surface only — so it lives inside
`EngineeredWood.Expressions` rather than in a separate package. Format libraries
that never parse anything pay for a few types they don't reference, and nothing
else. See [the decision record](#the-spark-sql-parser) below for why this is not
the optional ANTLR package the earlier revision of this document specified.

### Why not one library per concern?

- **Stats evaluation without Arrow** is genuinely useful (metadata-only tools,
  query planners deciding which files to distribute to workers).
- **Row evaluation without stats** is the natural Delta Lake CHECK path.
- Forcing an Arrow dependency on stats consumers, or a stats dependency on row
  consumers, creates needless coupling.
- Two small libraries with a clear upstream/downstream relationship is simpler
  than one library with conditional Arrow references or feature flags.

## `EngineeredWood.Expressions` — Core Library

### Expression tree

Modeled on Iceberg's existing implementation (which is already proven and shaped
correctly). The tree distinguishes value-producing expressions from
boolean-producing predicates, mirroring delta-kernel-rs:

> **How far the kernel resemblance actually goes**, checked against kernel
> `d1f7f52a` while sizing phase 9, because the answer bears on the parser and
> the function registry both.
>
> The `Expression` / `Predicate` split is genuinely kernel's. Nothing below it
> is. `FunctionCall` is Iceberg's `ApplyExpression` renamed — its predecessor's
> doc comment reads "replaces the old Transform-in-expression pattern", because
> the only computation Iceberg pruning ever needs is a partition transform
> (`bucket(x, 16)`, `truncate`, `year`), and an n-ary apply node covers that
> whole requirement. There was never a decision to route arithmetic through
> function application; there was no arithmetic requirement, and the shape came
> across wholesale.
>
> Kernel took the opposite approach. Its `Expression` is a closed set —
> `Literal`, `Column`, `Predicate`, `Struct`, `StructPatch`, `Unary`, `Binary`,
> `Variadic`, `Cast`, `Opaque`, `Unknown` — with **no generic function-call node
> at all**. Arithmetic is a first-class node
> (`BinaryExpressionOp { Plus, Minus, Multiply, Divide }`), `CAST` is its own
> node carrying a `CastExpression`, `COALESCE` and `ARRAY` are variadic
> operators, and engine-defined operations go through `Opaque` plus an
> `OpaqueExpressionOp` trait rather than a string-keyed registry.
>
> So this tree has no arithmetic node, and `a + b` lowers to `FunctionCall("+")`
> with every coercion rule living in registry code behind an open-ended string
> contract.
>
> **Decided 2026-08-11: it stays that way.** Arithmetic continues to route
> through `FunctionCall` until there is strong evidence the shape is wrong —
> which there is not today. The relevant evidence points the other way: the only
> normative statement anyone makes about the expression language is Databricks'
> — a CHECK constraint "can use any SQL functions in Spark that always return the
> same result when given the same argument values", excluding UDFs, aggregates,
> window functions and table functions. That is a large open set, and an
> open-ended registry models it better than a closed operator enum would. Adding
> a closed arithmetic node later remains possible; nothing here forecloses it.
>
> Also weighing in favour of leaving it alone: **`FunctionCall` has no producers
> today.** It is built only by the two `Expressions.Call` factories and Iceberg's
> compatibility shim, and every consumer treats it as pass-through-or-throw.
> Iceberg partition transforms do not lower into it — they stay in the closed
> `Transform` hierarchy. The Spark parser will be its first real producer, so the
> shape can be judged against real use before it is changed on speculation.

```csharp
public abstract record Expression;

// Leaf expressions
public sealed record UnboundReference(string Name) : Expression;
public sealed record BoundReference(int FieldId, string Name) : Expression;
public sealed record LiteralExpression(LiteralValue Value) : Expression;
public sealed record FunctionCall(string Name, IReadOnlyList<Expression> Arguments) : Expression;

// Boolean predicates
public abstract record Predicate : Expression;
public sealed record TruePredicate : Predicate;
public sealed record FalsePredicate : Predicate;
public sealed record AndPredicate(IReadOnlyList<Predicate> Children) : Predicate;
public sealed record OrPredicate(IReadOnlyList<Predicate> Children) : Predicate;
public sealed record NotPredicate(Predicate Child) : Predicate;
public sealed record ComparisonPredicate(Expression Left, ComparisonOperator Op, Expression Right) : Predicate;
public sealed record UnaryPredicate(Expression Operand, UnaryOperator Op) : Predicate;
public sealed record SetPredicate(Expression Operand, IReadOnlyList<LiteralValue> Values, SetOperator Op) : Predicate;
```

Operators are enums:
- `ComparisonOperator`: `Equal`, `NotEqual`, `LessThan`, `LessThanOrEqual`,
  `GreaterThan`, `GreaterThanOrEqual`, `StartsWith`, `NotStartsWith`,
  `NullSafeEqual` (Spark's `<=>`, used by generated column validation)
- `UnaryOperator`: `IsNull`, `IsNotNull`, `IsNaN`, `IsNotNaN`
- `SetOperator`: `In`, `NotIn`

### The tree has one semantics, and front ends normalise into it

Stated here 2026-08-11 because it had only ever been implicit, and a format
turned up whose native meaning differs.

**A node means SQL three-valued logic.** `ComparisonOperator.Equal` propagates
null — `null = null` is null, not true. `AndPredicate` and `OrPredicate` are
Kleene. `NullSafeEqual` is the only operator that treats null as an ordinary
value. Every evaluator in the tree, and every consumer that pattern-matches on a
node, is entitled to assume this.

Not to be confused with the other three-valued thing in this document.
[Three-valued evaluation rules](#three-valued-evaluation-rules) is about
*pruning* — `AlwaysTrue` / `AlwaysFalse` / `Unknown`, describing how much a
statistic lets you conclude. This paragraph is about *nulls*, and the two are
independent: a predicate with fully known statistics can still evaluate to null
on a row.

**A front end owes a normalisation into that semantics.** This is already how the
Spark parser behaves and was not presented as a general rule: `BETWEEN` becomes
two comparisons, `IN` over expressions becomes a disjunction, `IS TRUE` becomes
`<=> TRUE`, and a bare boolean becomes `= TRUE`. The rule generalises — a format
whose operators mean something else lowers them at decode time rather than
asking the evaluator to behave differently.

**Why not carry the dialect on the node instead.** The alternative considered was
routing comparisons through `FunctionCall`, so that function identity carries the
semantics and the registry resolves it — attractive because it needs no policy
anywhere and matches Iceberg's own "complexity is pushed to the functions"
philosophy. Rejected for two reasons:

- It cannot be carried through. A `FunctionCall` is an `Expression`, not a
  `Predicate`, so junction children lose their type and each comparison needs
  wrapping in `… = TRUE` — which is itself a comparison. Terminating the
  regress requires a `BooleanExpressionPredicate(Expression)` node the tree does
  not have, so it trades comparison nodes for a different node rather than
  removing a concept. And it does not stop at comparisons: dialects differ on
  `AND`/`OR`/`NOT` too, so `Predicate` ends up with no members at all.
- It would cost this document's primary deliverable. `StatisticsEvaluator` prunes
  by matching `ComparisonPredicate(BoundReference, Op, Literal)` against min/max,
  `SetPredicate` drives bloom probing and dictionary pruning, and Lance's
  `IndexPruner` and Vortex's `VortexZoneStatsAccessor` match the same shapes.
  Opaque calls make all of them return `Unknown`. Recovering pruning would mean
  special-casing function names in the stats evaluator — the node set again, as
  strings, with a runtime lookup and no type safety.

#### Normalising Iceberg expressions

Not built — there is no Iceberg data-file read path yet, so nothing evaluates an
Iceberg expression row-wise. Recorded because
[the derived-column RFC](https://github.com/apache/iceberg/issues/15923) would
create one, and because the mapping is the specification for whoever writes the
decoder.

Iceberg's [expressions spec](https://github.com/apache/iceberg/blob/main/format/expressions-spec.md)
is explicit that predicates are **two-valued** — "Evaluation of all predicates
must produce `true` or `false`" — and that comparisons are **null-safe**:
`null = null` is true, `34 = null` is false, `null <= null` is true. So Iceberg's
`=` is our `NullSafeEqual`, not our `Equal`:

| Iceberg | lowered to |
|---|---|
| `=` | `NullSafeEqual(a, b)` |
| `!=` | `Not(NullSafeEqual(a, b))` |
| `<`, `>` | `LessThan`, `GreaterThan` — these already agree |
| `<=` | `Or(NullSafeEqual(a, b), And(IsNotNull(a), IsNotNull(b), LessThan(a, b)))` |
| `>=` | as `<=`, with `GreaterThan` |

The explicit null guards on `<=` are not defensiveness. Iceberg's `<=` is true
when both operands are null and false when only one is, and without the guards a
Kleene `Or` would yield null for the one-null case. Iceberg's own spec prescribes
this shape of translation in the opposite direction: "Engines that implement SQL
3-valued boolean logic must add `IS NULL` and `IS NOT NULL` to produce the
2-valued equivalent."

Three further differences a decoder has to handle, none of which are
representational:

- **Function identity is richer.** Iceberg names functions by catalog, namespace
  and name, with reserved `sql_functions` and `iceberg_functions` sets and engine
  sets such as `spark_builtin_functions.to_utc_timestamp`. `FunctionCall` carries
  a bare string. Notably the pre-migration Iceberg tree had exactly this shape —
  `ApplyExpression(FunctionIdentifier, …)`, dropped in `23cfc4e` — so this is a
  question of whether to reinstate it, and it bears on
  [#119](https://github.com/clast-project/engineered-wood/issues/119).
- **Iceberg rejects `x = null`** outright, requiring `IS NULL`, and forbids null
  or NaN constants as the direct child of a comparison. Our Spark parser builds
  that node happily, because Spark accepts it. That is a validation difference
  per front end, not a difference in what the tree can hold.
- **Iceberg writes null-tolerance into the constraint** (`x < 10 OR x IS NULL`)
  where Delta puts it in the evaluation rule (a null result violates). The same
  tree means different things to the two writers, which is a Phase 10 concern
  rather than a Phase 9 one.

Convenience factories live in a static `Expressions` class:

```csharp
public static class Expressions
{
    public static Predicate Equal(string column, LiteralValue value);
    public static Predicate GreaterThan(string column, LiteralValue value);
    public static Predicate IsNull(string column);
    public static Predicate And(params Predicate[] children);
    // etc.
}
```

### `LiteralValue` — typed scalar

A value type that wraps any comparable scalar without boxing. Supports cross-type
numeric promotion (int vs long, float vs double) via `IComparable<LiteralValue>`.

**`CompareTo` and `Equals` are two different relations, on purpose.** `CompareTo`
is SQL's, measured against Spark, and promotes across kinds: `1` compares equal
to `1.0d`, and a `decimal(20,0)` holding 2^53+1 compares equal to the double
2^53 because both widen to double. That relation is *pairwise* — which pairs
match depends on the types involved — so it is neither transitive nor consistent
with any hash, and cannot also be `Equals`. `Equals`/`GetHashCode` therefore
compare representation: two kinds are never equal, the hash agrees, and `Equals`
never throws. Evaluate predicates with `CompareTo(x) == 0`; use `Equals` only for
keys and for comparing expression trees. See issue #206.

```csharp
public readonly struct LiteralValue : IComparable<LiteralValue>, IEquatable<LiteralValue>
{
    internal enum Kind : byte
    {
        Null,
        Boolean, Int32, Int64, UInt32, UInt64,
        Float, Double, Half,
        String, Binary,
        Decimal, HighPrecisionDecimal,
        DateOnly, TimeOnly, DateTimeOffset,
        Guid,
    }

    public Kind Type { get; }

    public static implicit operator LiteralValue(bool value);
    public static implicit operator LiteralValue(int value);
    public static implicit operator LiteralValue(long value);
    public static implicit operator LiteralValue(uint value);
    public static implicit operator LiteralValue(ulong value);
    public static implicit operator LiteralValue(float value);
    public static implicit operator LiteralValue(double value);
    public static implicit operator LiteralValue(decimal value);
    public static implicit operator LiteralValue(string value);
    public static implicit operator LiteralValue(byte[] value);
    public static implicit operator LiteralValue(DateTimeOffset value);
    public static implicit operator LiteralValue(Guid value);
#if NET6_0_OR_GREATER
    public static implicit operator LiteralValue(Half value);
    public static implicit operator LiteralValue(DateOnly value);
    public static implicit operator LiteralValue(TimeOnly value);
#endif

    public static LiteralValue Null { get; }

    /// High-precision decimal for Decimal128/256 columns whose precision exceeds
    /// System.decimal's 28-29 digit limit.
    public static LiteralValue Decimal(BigInteger unscaledValue, int scale);

    public int CompareTo(LiteralValue other);
}
```

The implicit conversions mean call sites look like
`Expressions.Equal("name", "Alice")` — the `LiteralValue` type is invisible in
common cases.

**Why not Iceberg's existing `LiteralValue`?** Iceberg's version is a class
(boxes), doesn't support high-precision decimal, and lives in the wrong
assembly. The new struct subsumes it; Iceberg migrates to use the shared one.

### Schema binding

`UnboundReference("name")` carries a column name; `BoundReference(fieldId, name)`
carries an Iceberg-style field ID after binding. `ExpressionBinder` walks an
expression tree against a schema and replaces unbound references with bound
ones, also validating that referenced columns exist and that types make sense.

For non-Iceberg formats (Parquet, Delta, ORC), binding is optional — name-based
references work directly. Iceberg requires binding before evaluation.

### Statistics evaluator

Three-valued logic generic over the statistics carrier:

```csharp
public enum FilterResult
{
    /// All rows in this unit satisfy the predicate.
    AlwaysTrue,
    /// No rows in this unit can satisfy the predicate.
    AlwaysFalse,
    /// Some rows may satisfy the predicate; must read to determine.
    Unknown,
}

public interface IStatisticsAccessor<TStats>
{
    LiteralValue? GetMinValue(TStats stats, string column);
    LiteralValue? GetMaxValue(TStats stats, string column);
    long? GetNullCount(TStats stats, string column);
    long? GetValueCount(TStats stats, string column);
    bool IsMinExact(TStats stats, string column);
    bool IsMaxExact(TStats stats, string column);
}

public static class StatisticsEvaluator
{
    public static FilterResult Evaluate<TStats>(
        Predicate predicate,
        TStats stats,
        IStatisticsAccessor<TStats> accessor);
}
```

Each format implements `IStatisticsAccessor<TStats>` for its own stats carrier:

- Iceberg: `IStatisticsAccessor<DataFileStats>`
- Parquet: `IStatisticsAccessor<RowGroup>` (decodes physical bytes into `LiteralValue` on demand)
- Delta: `IStatisticsAccessor<ColumnStats>` (parses `JsonElement` into `LiteralValue`)
- ORC: `IStatisticsAccessor<StripeFooter>` (future)

### Three-valued evaluation rules

| Predicate | AlwaysTrue when | AlwaysFalse when |
|-----------|----------------|-----------------|
| `Equal(col, v)` | min == max == v | v < min or v > max |
| `NotEqual(col, v)` | v < min or v > max | min == max == v |
| `GreaterThan(col, v)` | min > v | max <= v |
| `GreaterThanOrEqual(col, v)` | min >= v | max < v |
| `LessThan(col, v)` | max < v | min >= v |
| `LessThanOrEqual(col, v)` | max <= v | min > v |
| `NullSafeEqual(col, v)` | min == max == v | v < min or v > max |
| `IsNull(col)` | NullCount == ValueCount | NullCount == 0 |
| `IsNotNull(col)` | NullCount == 0 | NullCount == ValueCount |
| `In(col, vs)` | (defers to bloom filter) | all values outside [min, max] |
| `And(a, b, ...)` | all AlwaysTrue | any AlwaysFalse |
| `Or(a, b, ...)` | any AlwaysTrue | all AlwaysFalse |
| `Not(a)` | a is AlwaysFalse | a is AlwaysTrue |

When statistics are missing or the accessor returns null, the result is
`Unknown`. When `IsMinExact` or `IsMaxExact` is false (truncated statistics),
range comparisons at the boundary conservatively return `Unknown`.

## `EngineeredWood.Expressions.Arrow` — Row Evaluator

Walks an `Expression` tree against a `RecordBatch`, producing typed Arrow arrays
for value expressions and `BooleanArray` for predicates.

```csharp
public interface IRowEvaluator
{
    BooleanArray EvaluatePredicate(Predicate predicate, RecordBatch batch);
    IArrowArray EvaluateExpression(Expression expression, RecordBatch batch);
}

public sealed class ArrowRowEvaluator : IRowEvaluator
{
    public ArrowRowEvaluator(IFunctionRegistry? functions = null);
}

public interface IFunctionRegistry
{
    IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount);
    bool IsRegistered(string name);
}
```

The default `ArrowRowEvaluator` handles every built-in predicate: comparisons,
AND/OR/NOT, IS NULL, and IN. Everything else — `CAST`, arithmetic, and named
functions alike — is a `FunctionCall`, and the library ships no
`IFunctionRegistry` implementation, so any of them throws unless the caller
supplies one. Spark SQL functions like `YEAR`, `SUBSTRING` and `DATE_FORMAT`
would come from the `SparkFunctionRegistry` of
[phase 9](#the-spark-sql-parser).

> Earlier revisions of this paragraph listed "arithmetic, CAST" among the
> evaluator's built-in operators. It never implemented either, and with no
> arithmetic node in the tree there is nothing for it to dispatch on — see the
> note under [Expression tree](#expression-tree).

**Column-type coverage is bounded**, which is a real constraint rather than a
detail: `Boolean`, `Int8/16/32/64`, `UInt8/16/32/64`, `Float`, `Double`,
`String`, `Binary`, `Date32/64`, `Timestamp`, and `Decimal32/64/128/256`. A
predicate over any other column type — `Time32`/`Time64`, `HalfFloat`, or a
nested column — throws `NotSupportedException` at evaluation time. Statistics
pruning is independent and broader, so pruning can succeed on a column the row
evaluator cannot evaluate.

Note that the Delta and Parquet readers narrow a decimal column to the smallest
`Decimal{32,64,128,256}Array` that fits its precision, so all four widths are
reachable; a `decimal(12,2)` arrives as `Decimal64Array`.

### Used by

Shipping today:

- **Delta Lake predicate DELETE / UPDATE**: the `Expressions.Predicate`
  overloads evaluate the predicate to a per-row mask, and the same predicate
  becomes the transaction's `ReadSet.Predicates` so concurrency conflict
  detection is precise rather than conservative.
- **Lance predicate delete / update**: `LanceTable` and `LanceDatasetWriter`
  construct an `ArrowRowEvaluator` per operation and keep only the rows the
  predicate selects.

Designed but not implemented (both blocked on the Spark SQL parser, phases 9–10):

- **Delta Lake CHECK constraints**: evaluate the constraint predicate against
  every batch being written; if any row evaluates to false (or null), abort the
  write. Today `HonorWriterFeatures` detects an active constraint and **refuses
  the write** rather than evaluating it.
- **Delta Lake generated columns**: evaluate the generation expression to
  produce the column's values. Same refusal applies today.
- **Parquet post-filter**: after row group pruning identifies candidate groups,
  filter the materialized batches to drop non-matching rows.

## Parquet Predicate Pushdown

This is the original scope of this document. Parquet integration becomes
straightforward once the shared library exists.

### `ParquetReadOptions.Filter`

```csharp
public sealed class ParquetReadOptions
{
    // ... existing fields ...

    /// Filter predicate for row group pruning. Row groups whose statistics
    /// prove no rows can match are skipped entirely.
    public Predicate? Filter { get; init; }

    /// When true and a Filter is set, bloom filters are probed for equality
    /// predicates before falling back to statistics. Requires additional I/O
    /// per candidate row group. Default: false.
    public bool FilterUseBloomFilters { get; init; }
}
```

### `ParquetStatisticsAccessor`

Implements `IStatisticsAccessor<RowGroup>` for Parquet's row group metadata.
Decodes raw `byte[]` min/max from the column's physical type into `LiteralValue`
on demand using the column descriptor.

This is where Parquet's binary statistics encoding (signed vs unsigned int,
big-endian decimal, lexicographic byte arrays, NaN handling on float/double)
lives. The shared `StatisticsEvaluator` consumes typed `LiteralValue` and
doesn't need to know about physical encoding.

**Sort orders** (from the Parquet spec):

| Physical Type | Logical Type | Sort Order |
|---|---|---|
| BOOLEAN | — | Unsigned (false < true) |
| INT32 | — | Signed |
| INT32 | INT(unsigned) | Unsigned |
| INT32 | DATE | Signed |
| INT64 | — | Signed |
| INT64 | INT(unsigned) | Unsigned |
| INT64 | TIMESTAMP | Signed |
| FLOAT | — | Signed (with NaN handling) |
| DOUBLE | — | Signed (with NaN handling) |
| BYTE_ARRAY | STRING | Unsigned lexicographic |
| BYTE_ARRAY | — | Unsigned lexicographic |
| FIXED_LEN_BYTE_ARRAY | — | Unsigned lexicographic |
| FIXED_LEN_BYTE_ARRAY | DECIMAL | Big-endian signed |
| INT96 | — | Undefined (statistics unreliable) |

**Float/Double NaN handling:** Per the Parquet spec, NaN is not a valid
statistics value. If min or max is NaN, statistics are treated as absent.

### Reader integration

```csharp
public async IAsyncEnumerable<RecordBatch> ReadAllAsync(
    IReadOnlyList<string>? columnNames = null,
    [EnumeratorCancellation] CancellationToken ct = default)
{
    var metadata = await ReadMetadataAsync(ct).ConfigureAwait(false);
    var accessor = new ParquetStatisticsAccessor(_schema);

    for (int i = 0; i < metadata.RowGroups.Count; i++)
    {
        if (_options.Filter != null)
        {
            var result = StatisticsEvaluator.Evaluate(
                _options.Filter, metadata.RowGroups[i], accessor);

            if (result == FilterResult.AlwaysFalse)
                continue;

            if (_options.FilterUseBloomFilters
                && result == FilterResult.Unknown)
            {
                var bloomResult = await BloomFilterPredicateEvaluator.EvaluateAsync(
                    _options.Filter, _file, metadata.RowGroups[i], _schema, ct)
                    .ConfigureAwait(false);
                if (bloomResult == FilterResult.AlwaysFalse)
                    continue;
            }
        }

        yield return await ReadRowGroupAsync(i, columnNames, ct)
            .ConfigureAwait(false);
    }
}
```

### Bloom filter integration

`ParquetFileReader.GetCandidateRowGroupsAsync` already probes bloom filters for
a column + value list. The new `BloomFilterPredicateEvaluator` walks an
`Expression` tree to find `Equal`/`In` predicates and probes them per row
group:

1. Walk the predicate to find equality-shaped sub-predicates
2. For each, probe the bloom filter (if present) for the row group/column
3. If the bloom filter says "definitely not present" → `AlwaysFalse`
4. Otherwise, fall through to statistics

Bloom filter probing is only useful for equality and set membership. Range
predicates (`GreaterThan`, `LessThan`, etc.) cannot use bloom filters.

## Delta Lake Integration

### Stats-based file pruning

> **Implemented, but not in the shape sketched here.** Partition and statistics
> pruning were unified into a single pass in `DeltaFilePruner` rather than the
> two separate steps below, and the public entry point is
> `ReadAllAsync(columns, filter, ct)`. Two things learned after this section was
> written and worth knowing before touching it:
>
> - **Decimal bounds must not go through `System.Decimal`.**
>   `JsonElement.TryGetDecimal` and `decimal.TryParse` silently *round* a value
>   with more than 28–29 significant digits and return `true`. A rounded bound is
>   a wrong bound, a wrong bound skips a file that matches, and the query
>   silently returns fewer rows. `DeltaLiteralDecoder` parses the raw digits into
>   an unscaled `BigInteger` and materializes `System.Decimal` only when lossless.
> - **A checkpoint's typed statistics are read in preference to the JSON.**
>   `CheckpointStatsView` maps `stats_parsed` columns once per batch so a bound
>   costs one indexed read instead of parsing each file's whole statistics blob
>   inside the pruning loop. The JSON fallback is load-bearing, not vestigial:
>   `stats_parsed` omits boolean bounds.

The original design: implement `IStatisticsAccessor<ColumnStats>` for Delta's
`ColumnStats` type (parsed from `AddFile.stats` JSON), and filter
`Snapshot.ActiveFiles` before reading:

```csharp
public IAsyncEnumerable<RecordBatch> ReadAllAsync(
    Predicate? filter = null,
    IReadOnlyList<string>? columns = null,
    CancellationToken ct = default)
{
    var accessor = new DeltaStatisticsAccessor();

    foreach (var addFile in CurrentSnapshot.ActiveFiles.Values)
    {
        if (filter != null && addFile.Stats != null)
        {
            var stats = ColumnStats.Parse(addFile.Stats);
            var result = StatisticsEvaluator.Evaluate(filter, stats, accessor);
            if (result == FilterResult.AlwaysFalse)
                continue;
        }

        // Also: partition pruning based on addFile.PartitionValues
        // ...

        await foreach (var batch in ReadFileAsync(addFile, columns, ...))
            yield return batch;
    }
}
```

### CHECK constraints

> **Implemented** for CHECK constraints and invariants, on the write paths that
> hold the batches. `DeltaConstraintEnforcer` parses `delta.constraints.*` and
> `delta.invariants` and evaluates them per batch before anything is written;
> a violation refuses the commit with `DELTA_VIOLATE_CONSTRAINT`.
>
> The fail-closed guard did not go away, it narrowed. An expression that cannot
> be parsed or cannot be evaluated still refuses, and so does any path that never
> sees the rows — `HonorWriterFeatures` takes a `rowsWillBeValidated` flag that
> only the validating paths pass, defaulting to the refusing answer so a path
> added later inherits it.
>
> `DeltaTableOptions.ExpressionParser` still does not exist: the built-in parser
> is used directly, and an override has had no caller to justify it.

On write, parse `delta.constraints.{name}` into a `Predicate`, evaluate against
each batch, and abort if any row fails:

```csharp
foreach (var batch in batches)
{
    foreach (var (name, expr) in constraints)
    {
        var result = _rowEvaluator.Evaluate(expr, batch);
        if (HasFalseOrNull(result))
            throw new DeltaConstraintViolationException(name);
    }
    // ... write batch ...
}
```

Rejecting on null as well as false is confirmed against delta-spark 4.0.0, not
assumed — see
[What Delta actually does with a constraint](#what-delta-actually-does-with-a-constraint),
which also records that the *evaluation* semantics behind this pseudocode are a
choice EngineeredWood has to make rather than inherit.

The constraint expressions are stored as Spark SQL strings in
`Metadata.Configuration`. Since [the parser decision](#the-spark-sql-parser),
parsing no longer needs a separate package: the parser lives in
`EngineeredWood.Expressions`, which `DeltaLake.Table` already references through
`Expressions.Arrow`, so the default parser is always present.

`DeltaTableOptions.ExpressionParser` therefore becomes an optional *override*
rather than the mandatory injection seam the earlier design needed — useful for
tests and for a host that wants its own dialect, and null means "use the
built-in parser" rather than "refuse the write":

```csharp
public sealed record DeltaTableOptions
{
    // ... existing ...

    /// Overrides the built-in Spark SQL parser used for CHECK constraints,
    /// invariants, and generated columns. Null uses the built-in parser.
    public IExpressionParser? ExpressionParser { get; init; }
}
```

### Generated columns

> **Implemented**, on the write paths that hold the batches.
> `DeltaGeneratedColumns` computes a column the caller omitted and checks one it
> supplied, refusing with `DELTA_GENERATED_COLUMN_MISMATCH` when the two
> disagree. It runs before the CHECK constraints, since a constraint may
> reference a generated column and would otherwise read a null the table never
> stores.

On write, parse `delta.generationExpression` from each generated column's
metadata. For each batch:

1. If the user provided a value for the generated column: validate
   `(materialized <=> generated) IS TRUE`
2. If the user did not provide a value: compute it via the row evaluator and
   substitute it into the batch

Step 1 is the protocol's wording, not a paraphrase — [Generated
Columns](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#generated-columns)
requires that writers "MUST enforce that any data writing to the table satisfy
the condition `(<value> <=> <generation expression>) IS TRUE`". Unlike the
constraint semantics discussed
[below](#what-delta-actually-does-with-a-constraint), this one is specified.

Same parser injection as CHECK constraints.

## The Spark SQL parser

> **Decided 2026-08-09** ([#101](https://github.com/clast-project/engineered-wood/issues/101)):
> a hand-written tokenizer and recursive-descent parser inside
> `EngineeredWood.Expressions`. No ANTLR, no grammar-subsetting tool, no new
> package, no new dependency. The ANTLR design this section used to carry is
> preserved under [rejected approaches](#rejected-approaches) below.

### Why hand-written

**The subsetting tool did not buy what it was meant to buy.** The ANTLR design
rejected hand-maintaining a grammar fork on drift grounds, and answered it with a
program that walks `SqlBaseParser.g4` from the `expression` rule, emits a subset
grammar, and is rerun against each Spark release. That program is itself a
bespoke grammar-analysis tool that has to be maintained, and rerunning it per
release *is* the drift-management work a fork requires — diff the output, repair
the visitor where a rule changed shape. The real comparison was never "fork
versus no fork"; it was "a parser" versus "a subsetting tool plus a generated
grammar plus a CST visitor". The second stack is strictly larger.

**The toolchain cost lands at the bottom of the build.** The ANTLR tool is a Java
jar; the MSBuild integrations either require a JDK on the build machine or fetch
one. `EngineeredWood.Expressions` targets `netstandard2.0` alongside net8.0 and
net10.0, the repository must build `net472`, and `src/Directory.Build.props`
applies `IsAotCompatible` and `TreatWarningsAsErrors` to every net10.0 library
under `src/`. A hand-written parser has no exposure to any of it. Whether the
ANTLR runtime trims and AOT-compiles cleanly under those settings is a question
that would have to be answered empirically before committing — and that it is a
question at all is most of the argument.

**Almost all the risk lives behind the parser.** Recognising
`a > 0 AND b IS NOT NULL` is not the hard part of CHECK constraints and
generated columns. Spark's semantics are: implicit type coercion, decimal
precision promotion under arithmetic, ANSI versus legacy cast behaviour,
timezone handling in `CAST(ts AS DATE)`, and the exact `<=>` behaviour that
generated-column validation depends on. A generated grammar contributes nothing
to any of it. This also weakens the original rejection of third-party parsers on
"approximate Spark-dialect coverage" grounds: dialect gaps in the *syntax* are
the cheap half.

### Scope: larger than delta-kernel-rs, not equal to it

delta-kernel-rs is the precedent for *how* — hand-written, no `sqlparser`
dependency, in-tree — and deliberately not the precedent for *how much*.

Its `kernel/src/expressions/sql/` parser sits behind the
`check-constraints-in-dev` feature flag, which `kernel/Cargo.toml` describes as
experimental and temporary: "the SQL predicate parser and, later,
`checkConstraints` write enforcement. Remove once full check-constraints support
ships."

Read against kernel at `d1f7f52a`, that parser is earlier-stage than its token
set suggests, and the two layers must not be conflated:

- `token.rs` (449 lines) tokenizes comparison operators including `<=>`, `+` and
  `-`, the keywords `AND` / `OR` / `NOT` / `IS`, quoted strings, numbers with
  exponents, `X'…'` binary, booleans and null, typed `DATE` / `TIMESTAMP`
  literals, and backtick-quoted or dotted identifiers.
- `parser.rs` (291 lines) accepts **exactly one `operand <op> operand`
  comparison**. Its own header records that "junctions (`AND`/`OR`/`NOT`),
  parentheses, and `IS [NOT] NULL` are not yet supported and surface as errors",
  and `parse_operand` treats `+` / `-` as literal signs only — "unary minus on a
  column and binary arithmetic are out of grammar". Trailing tokens after the
  single comparison are an error.

So the keywords in the tokenizer are groundwork, not capability. This is a
snapshot of unfinished work rather than a finding about what real constraints
contain, and shipping its scope would reject nearly everything Spark writes.

It is also check-constraints-only. Generated columns and invariants sit outside
kernel's remit by design, because kernel's contract is that the engine supplies
expressions. Phase 10 here covers constraints *and* invariants *and* generated
columns, and generated columns are exactly where parentheses and function calls
concentrate — `CAST(x AS DATE)`, `date_format`, `year`, `substring`.

So the target is the full expression grammar from the start: parentheses,
arithmetic, `CASE`, `CAST`, `IN` / `BETWEEN` / `LIKE`, function calls,
comparison including `<=>`, `AND` / `OR` / `NOT`, and the `IS` predicates.
Sizing that against kernel is the honest anchor available: 740 lines buys it a
tokenizer plus a single-comparison parser, so the full grammar should be
budgeted well above that — on the order of 1,200–1,800 lines — and still below
what a subsetting tool plus a generated grammar plus a CST visitor would have
cost.

### How coverage gaps get reported

Unsupported syntax throws a distinct, quotable error naming the construct that
stopped the parse. `HonorWriterFeatures`' fail-closed refusal stays underneath
it, so an unparseable constraint degrades to today's behaviour — the write is
refused — rather than to a write committed without validation.

That error *is* the tracking mechanism. It reports coverage gaps from real
tables, with no watch on Spark's grammar and no watch on kernel's repository,
neither of which would signal reliably: Spark's release notes conflate syntax
additions with function additions, and kernel will never signal on generated
columns at all. The one kernel event worth reading is the removal of the
`check-constraints-in-dev` flag, at which point its shipped scope becomes a
statement about the floor the ecosystem expects.

### Translation layer and function registry

The parser produces `EngineeredWood.Expressions` `Expression` nodes directly —
there is no intermediate concrete syntax tree to visit. Function calls become
`FunctionCall(name, args)`, and so does arithmetic, because the tree has no
arithmetic node: `Expression` is `UnboundReference`, `BoundReference`,
`LiteralExpression` and `FunctionCall`, plus the `Predicate` subtypes.

That made the function registry the larger and riskier half of this phase rather
than a footnote to it, since every coercion and promotion rule named above is
registry work. `SparkFunctionRegistry` now ships it: arithmetic, casts, and the
named functions, with the promotion rules checked against the corpus rather than
derived. `ArrowRowEvaluator` still accepts any `IFunctionRegistry`, so a host can
substitute its own.

#### Where the dialect configuration lives

Decided 2026-08-11. Spark's evaluation semantics — ANSI above all, since
[Delta pins nothing](#what-delta-actually-does-with-a-constraint) and the same
constraint can accept or reject the same row depending on it — are bound **when
the registry is constructed**, not chosen by the parser and not threaded through
evaluation:

```csharp
var registry = new SparkFunctionRegistry(new SparkDialectOptions { Ansi = true });
```

The registry is already the dialect boundary: `IFunctionRegistry` exists so that
format-specific semantics live somewhere pluggable, and it is the object that was
always going to hold Spark's coercion rules. Giving it the configuration too
keeps three other things clean:

- **The parser stays syntax-only.** Text to tree, no semantics — which is also
  what lets it be tested in isolation against the precedence oracle.
- **The tree stays dialect-neutral.** `+` means `+`, so a `FunctionCall` built by
  the `Expressions` factories means the same thing as one produced by the parser.
  That matters because the parser is not the tree's only producer — predicate
  `DeleteAsync`/`UpdateAsync`, Lance's `ReadAsync`, Iceberg and
  `DeltaReadOptions.Filter` all build trees with no parser involved. It also
  keeps the tree round-trippable back to SQL text, which an eventual
  `AddConstraintsAsync` needs.
- **The evaluator stays policy-free.** No policy object threaded through
  evaluation and no per-row cost.

The alternative considered was for the parser to resolve the dialect at lowering
time, emitting distinct names such as `+_ansi` versus `+_legacy`. That is cheap
— the registry dispatches on strings already — and would be sound if the parser
were the only producer and parse-time config were always evaluation-time config.
Neither holds: parsing happens once when a table is opened while evaluation
happens per write, and Delta's own model attaches the configuration to the
writing session rather than to the table or the expression text. Binding at
registry construction keeps those two lifetimes separate without giving up the
simplicity.

### Function set

Minimum viable set for CHECK constraints and generated columns. The syntactic
rows are the parser's target scope; the rest is the registry's:

- Type casting: `CAST`, `TRY_CAST`
- Date/time: `YEAR`, `MONTH`, `DAY`, `HOUR`, `DATE_FORMAT`, `CURRENT_DATE`,
  `CURRENT_TIMESTAMP`
- String: `SUBSTRING`, `CONCAT`, `LENGTH`, `TRIM`, `UPPER`, `LOWER`
- Null handling: `COALESCE`, `IFNULL`, `NULLIF`
- Conditional: `CASE/WHEN/THEN/ELSE/END`, `IF`
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`, `<=>` (null-safe), `BETWEEN`,
  `IN`, `LIKE`
- Logical: `AND`, `OR`, `NOT`
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- IS predicates: `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE`

### Rejected approaches

- **ANTLR over a subset of Spark's `SqlBaseParser.g4`**, generated by a
  subsetting tool that walks the grammar from the `expression` rule and is rerun
  per Spark release (~200 lines of parser rules, ~100 lines of lexer tokens, plus
  a CST visitor). Rejected for the three reasons above: the tool does not
  eliminate the fork's maintenance, it puts a Java toolchain at the bottom of the
  build, and it addresses the part of the problem that was never the risk.
- **Hand-maintaining a grammar fork.** Drifts, and carries the toolchain cost
  without the automation.
- **A third-party SQL parser** (SqlParser-cs and similar). A dependency at the
  bottom of the stack whose Spark-dialect fidelity has to be verified anyway,
  bought for the cheap half of the problem.

## What the ecosystem actually does

Measured 2026-08-02, because the prioritisation above depends on which auxiliary
structures real files actually carry, and the intuitive answers are wrong in
several places.

### Page index: common in files, because parquet-mr writes it by default

Scanning `parquet-testing/data/` (63 readable files), 21 carry a page index and
the split is almost perfectly by writer and era:

| Writer | Page index |
|---|---|
| parquet-mr ≥ 1.11.1 (1.11, 1.12, 1.13, 1.14) | yes, every file |
| parquet-mr ≤ 1.10.1, 1.8.x | no |
| arrow-rs / parquet-rs 49, 53, 55 | yes |
| parquet-rs 0.3, 9.0 | no |
| parquet-cpp-arrow 11 → 20 | **no, every file** |

parquet-mr has written ColumnIndex/OffsetIndex by default since 1.11.0 (2019), so
essentially all Spark-, Hive- and Trino-written Parquet from the last six years
has one. Arrow C++/PyArrow is the holdout: verified directly on pyarrow 24.0.0,
`write_page_index` defaults to false.

Read-side exploitation is patchier: Impala drove the feature, Trino defaults
`parquet.use-column-index` to true, parquet-mr applies column-index filtering
when a predicate is pushed down, and DataFusion supports it behind
`with_page_index`.

**The caveat that decides whether it is worth building.** Page skipping only pays
when the data is clustered on the predicate column. Row-group statistics already
catch the coarse case; the page index buys granularity *inside* a surviving
row group. On a randomly distributed column every page's min/max spans the domain,
nothing is skipped, and the extra metadata read is pure cost.

Note that writing the index is much cheaper work than using it — it is mostly
serialising per-page statistics the `StatisticsCollector` already computes,
whereas reading requires a row-range-aware decode path in `ColumnChunkReader`.
The dependency in the phase table (13 after 11) is not a real one.

### Bloom filters: opt-in everywhere except DuckDB

| Writer | Default | Knob |
|---|---|---|
| parquet-mr (Spark, Hive, Trino) | off | `parquet.bloom.filter.enabled`, per column |
| Spark SQL | off | `spark.sql.parquet.bloomFilter.enabled` |
| arrow-rs | off | `set_bloom_filter_enabled` |
| Arrow C++ / PyArrow | off (verified, pyarrow 24.0.0: `bloom_filter_options` defaults to `None`) | `bloom_filter_options` |
| **DuckDB** | **on** | automatic, see below |
| EngineeredWood | off | `BloomFilterColumns` |

Consistent with that, only 2 of the 63 corpus files carry bloom filters, both
parquet-mr files written to exercise the feature.

DuckDB is the interesting case, because it answers "is there a heuristic good
enough to default to?" with a shipped yes. From
`extension/parquet/include/writer/templated_column_writer.hpp`, inside
`FlushDictionary` — called only for dictionary-encoded columns:

```cpp
if (writer.EnableBloomFilters()) {
  auto bloom_filter_entries = state.dictionary.GetSize() *
      OP::template BloomFilterEntriesPerValue<SRC, TGT>();
  state.bloom_filter = make_uniq<ParquetBloomFilter>(
      bloom_filter_entries, writer.BloomFilterFalsePositiveRatio());
}
```

A filter is written exactly when the column dictionary-encodes, sized from the
*exact* distinct count, populated by iterating the dictionary rather than the
rows, at a default FPP of 0.01. `dictionary_size_limit` bounds the dictionary and
therefore the filter.

**Why dictionary-encoded is the right trigger, when the opposite is intuitive.**
The tempting rule is to bloom the columns where the dictionary *failed*: high
cardinality is where min/max statistics are useless and point lookups hurt most.
It is wrong on three counts — on fallback the distinct count is unknown (we
substitute the row count, which overshoots and pins at `BloomFilterMaxBytes`),
the build is O(rows), and the filter is at its largest. The dictionary case is the
one that can be sized exactly, built cheaply, and bounded. The counter-argument
that a dictionary already provides exact membership so a bloom is redundant does
not hold either: probing a bloom is a small read at a known offset plus one hash,
where using the dictionary means locating and decompressing that chunk's
dictionary page.

Tracked as [#56](https://github.com/clast-project/engineered-wood/issues/56).

### Dictionary pruning: free, exact, and unused by us

parquet-mr has pruned row groups from dictionary contents for years behind
`parquet.filter.dictionary.enabled` (default on). For a *fully* dictionary-encoded
chunk the dictionary is the exact set of values present, so `col = v` is decidable
with no false positives.

This is plausibly better value than either of the above: it needs no cooperation
from the writer (dictionary pages are in nearly every real file), it is exact
rather than probabilistic, and `DictionaryPageOffset` is already parsed onto
`ColumnMetaData`. Our reader uses the dictionary only to decode. The trap is that
a chunk which fell back to PLAIN holds values absent from its dictionary, so the
chunk's encodings must be checked before trusting absence.

Tracked as [#57](https://github.com/clast-project/engineered-wood/issues/57).

## Future: Page-Level Pushdown via Column/Offset Index

| Index | Content | Enables |
|---|---|---|
| **Column Index** | Min/max values and null counts per page within a column chunk | Page-level statistics evaluation — skip pages that can't match |
| **Offset Index** | Byte offset and row range of each page within a column chunk | Seeking directly to matching pages without scanning headers |

Composes naturally with row group pruning: pruning eliminates row groups, the
column index identifies pages within surviving groups, the offset index
provides byte ranges to read only matching pages. The same `Predicate` tree is
evaluated per-page with the same `StatisticsEvaluator`.

This is a separate body of work — requires parsing column/offset index Thrift
structures, a page-skipping read path in `ColumnChunkReader`, and writing the
indexes in `ColumnChunkWriter`.

## Implementation Phases

### Shipped

History, not status — these are done and stay done.

| Phase | Scope | Project |
|---|---|---|
| **Phase 1** | `EngineeredWood.Expressions` core: tree, `LiteralValue`, factories | Expressions |
| **Phase 2** | `IStatisticsAccessor<TStats>`, `StatisticsEvaluator` | Expressions |
| **Phase 3** | `ExpressionBinder`, schema binding | Expressions |
| **Phase 4** | Migrate Iceberg expressions to consume the shared library | Iceberg |
| **Phase 5** | `ParquetStatisticsAccessor`; `ParquetReadOptions.Filter`; row group pruning | Parquet |
| **Phase 6** | Bloom filter probing in Parquet | Parquet |
| **Phase 7** | Delta Lake stats-based file pruning + partition pruning | DeltaLake.Table |
| **Phase 8** | `EngineeredWood.Expressions.Arrow`: `ArrowRowEvaluator`, `IFunctionRegistry` | new project |
| **Phase 9** | Spark SQL tokenizer and parser; `SparkFunctionRegistry` (arithmetic, casts, named functions) | Expressions + Expressions.Arrow |

### Designed, not built

**No status column on purpose.** This table describes scope; the issue tracker is
the only place that knows what is in flight. Rows marked *unfiled* have no issue
because nobody has committed to them — that is a deliberate state, not an
oversight.

| Phase | Scope | Project | Tracked as |
|---|---|---|---|
| — | Per-read filter on the Parquet reader; Delta forwarding, with logical→physical rewriting under column mapping | Parquet + DeltaLake.Table | [#55](https://github.com/clast-project/engineered-wood/issues/55) |
| — | Bloom auto-mode keyed on dictionary encoding; dictionary-sourced population; FPP default | Parquet | [#56](https://github.com/clast-project/engineered-wood/issues/56) |
| — | Dictionary-page row-group pruning | Parquet | [#57](https://github.com/clast-project/engineered-wood/issues/57) |
| **Phase 10** | Wire CHECK constraints and generated columns into Delta Lake writes. No longer blocked — Phase 9 shipped | DeltaLake.Table | [#102](https://github.com/clast-project/engineered-wood/issues/102) |
| **Phase 11** | Column/offset index parsing (Parquet read) | Parquet | unfiled |
| **Phase 12** | Page-level pushdown using column index (needs Phase 11 and a row-range read path) | Parquet | unfiled |
| **Phase 13** | Column/offset index writing — independent of Phase 11, despite the original ordering | Parquet | unfiled |
| **Phase 14** | ORC stripe pruning | Orc | unfiled |

### Ordering rationale

Phases 1-7 were the natural progression of "build the core, prove it on the two
formats with concrete needs (Parquet pruning, Delta pruning), and migrate
Iceberg to consume it." Each phase was independently testable and shippable.

Phases 8-10 extend the architecture to row-level evaluation, unblocking CHECK
constraints and generated columns. That reading held: the parser depended on
nothing but the expression tree, while the registry was where the risk actually
was, because Spark's coercion semantics land there. Phase 9 is done and Phase 10
is the remaining piece.

**What phase 9 cost, for whoever sizes the next one.** The parser was the
predictable part. The registry was not: nine separate behaviours contradicted a
reasonable-looking implementation and were only caught by asking Spark through
the `expr_oracle` rig — ANSI divide-by-zero applying to floating point, two
distinct overflow error classes, casts having their own overflow class, a string
cast to an integer refusing `'12.5'` where a number truncates, four separate
`substring` position rules, `concat` propagating null, `LIKE` honouring
backslash escapes, `nullif` comparing values rather than renderings, and a
non-boolean condition failing rather than reading as false. Review caught four
more, three of them raw BCL exceptions escaping the fail-closed path at
type-conversion boundaries. Budget measurement time for anything that claims to
reproduce another engine's semantics.

**What the original ordering got wrong.** It assumed the next win was more
pruning machinery. It is not: every mechanism phases 5-7 built is already
unreachable from the table layer, so #55 comes before all of it — more pruning
that nothing can invoke adds nothing. After that, #57 (dictionary pruning) is
plausibly the best value of the remaining work, because unlike bloom filters and
page indexes it needs no cooperation from whoever wrote the file, and it is exact
rather than probabilistic. Page-level pushdown (11-12) is the largest of these by
some margin, since it needs a row-range-aware decode path and not just metadata
parsing; index *writing* (13) is much cheaper and has interop value on its own.

Row-level filtering — the "post-filter" listed as future in the row evaluator
section — has working prior art in-repo: `LanceTable.ReadAsync(columns, filter, ct)`
evaluates per row and, in `ExtendProjectionForFilter`, reads the columns a
predicate references even when the caller did not project them, then drops them
before yielding. That is the non-obvious half, and it is already written. Delta
would additionally have to apply the same mask to the row-metadata columns
(`_ew_row_address`, row tracking), which `ReadCoreAsync` builds positionally per
batch — mask them out of step and row identities silently detach from their rows.

### Testing strategy

- **Unit tests:** `LiteralValue` comparison across all type pairs and edge
  cases (NaN, decimal precision, string ordering)
- **Unit tests:** `StatisticsEvaluator` against synthetic stats with known
  min/max/null_count for each predicate variant
- **Unit tests:** `ParquetStatisticsAccessor` decoding of physical bytes for
  every (physical_type, logical_type, sort_order) combination
- **Round-trip tests:** Write Parquet files with known data distributions, read
  with filters, assert correct row group pruning
- **Cross-validation:** Compare pruning decisions against PyArrow's
  `read_table(filters=...)` or DuckDB's predicate pushdown on the same files
- **Edge cases:** Empty statistics, all-null columns, single-row row groups,
  NaN in float columns, truncated min/max, Decimal256 statistics
- **Iceberg parity:** After migration, all existing Iceberg expression tests
  must still pass

### Differential testing against Spark (phase 9)

A hand-written parser buys back the correctness a generated grammar would have
supplied only if something independent checks it. PySpark can be that something,
and the Delta interop rig already hosts it — `spark_driver.py` runs a long-lived
`SparkSession` over a stdin command loop with a `COMMANDS` dispatch table, so
this is a new command plus one dispatch line. Session startup is the only
expensive part; batch every expression through one session.

Probed 2026-08-10 against `ew-spark40` (pyspark 4.0.1, JDK 17). Three oracles
work, and they are worth separating because they cost different things and
answer different questions:

**1. Spark's own parser, for precedence.**
`spark._jsparkSession.sessionState().sqlParser().parseExpression(s)` is the same
entry point Delta uses on `delta.constraints.*` and `delta.generationExpression`.
It returns the Catalyst tree; `.sql()` renders it fully parenthesised, so
`(a + b) * 2 <= 10` comes back as `(((a + b) * 2) <= 10)` and
`a > 0 AND b IS NOT NULL` as `((a > 0) AND (b IS NOT NULL))`. Diffing that string
against our own tree's rendering tests associativity and precedence with no data
and no evaluation. For a precedence-climbing parser this is the highest-value
test that exists.

**2. Resolved type without data, for coercion.** An empty DataFrame carrying the
target schema still resolves and type-checks an expression, so
`empty.selectExpr("(expr) AS r").schema[0].dataType` is a coercion oracle that
evaluates nothing. Measured, with `d1 decimal(10,2)`, `d2 decimal(6,4)`,
`a int`, `b bigint`:

| expression | Spark's resolved type |
|---|---|
| `a + b` | `bigint` |
| `d1 + d2` | `decimal(13,4)` |
| `d1 * d2` | `decimal(17,6)` |
| `d1 / d2` | `decimal(21,9)` |
| `a / b` | `double` — `/` is not integer division |
| `s = a` | `boolean`, implicit cross-type comparison |

These are the rules the `SparkFunctionRegistry` has to reproduce, they are
mechanical to get wrong, and hundreds of cases can be harvested for the cost of
one Spark startup.

**3. Per-row evaluation, for three-valued logic.** `a > 0` over `[1, null, -2]`
gives `[true, null, false]`; `a > 0 AND b > 5` gives `[true, false, false]`, the
null absorbed rather than propagated; `NOT (a > 0)` gives `[false, null, true]`;
`a <=> NULL` gives `[false, true, false]`.

**Pin the session config, and record it with the expectations.** "What does
Spark do" is not well-formed on its own. Flipping `spark.sql.ansi.enabled`
changes answers on the same data, including turning results into errors:

| expression | `ansi=false` | `ansi=true` |
|---|---|---|
| `a / 0` | `[null, null, null]` | `DIVIDE_BY_ZERO` |
| `CAST('abc' AS INT)` | `[null, null, null]` | `CAST_INVALID_INPUT` |

`spark.sql.session.timeZone` and `spark.sql.storeAssignmentPolicy` matter the
same way. The value to match is whatever Delta uses when it validates a
constraint, which is a separate question from the session default and should be
established before the corpus is harvested.

**Sequencing.** Oracles 1 and 2 need no EngineeredWood code at all. The
precedence and coercion corpora can be harvested as static fixtures before the
parser is written and developed against offline, which moves differential
testing from something that follows phase 10 to something that precedes phase 9.

All three are available as the `expr_oracle` command in
`test/EngineeredWood.DeltaLake.Table.Tests/Interop/spark_driver.py`, which takes
`{expressions, schema?, rows?, conf?}` and echoes the config in force alongside
the results.

### The harvested corpus

Done, 2026-08-11. `harvest_expression_corpus.py` (beside the driver) drives
`expr_oracle` over 198 expressions grouped by feature and writes
`test/EngineeredWood.Expressions.Tests/Fixtures/spark-expression-corpus.json`,
pinned at `ansi.enabled=true`, `session.timeZone=UTC`,
`storeAssignmentPolicy=ANSI`. 194 parse and 192 type-resolve; the failures are
the deliberately malformed entries.

Because it is checked in, the parser can be developed and tested offline — the
Expressions test project needs no Spark, no JVM and no network.
`SparkExpressionCorpusTests` guards it, and asserts the recorded configuration
rather than merely storing it, since re-harvesting under different settings
would silently invalidate every expectation derived from the file.

Three things it turned up that are worth knowing before writing the parser.

**`NOT` binds looser than comparison but tighter than `AND`.**
`NOT a > 0 AND b > 0` renders as `((NOT (a > 0)) AND (b > 0))`. Easy to get
wrong in a precedence table, and silently wrong if you do.

**Spark's numeric promotion has sharp edges no one would guess.** From the
coercion group:

| expression | resolved type |
|---|---|
| `sh * sh` (smallint) | `smallint` — no widening, so it can overflow |
| `a % b` (int, bigint) | `bigint` |
| `d1 % d2` | `decimal(6,4)` — the *narrower* operand's scale |
| `d1 / d2` | `decimal(21,9)` |
| `d3 * d3` (both `decimal(38,10)`) | `decimal(38,6)` — clamped at precision 38, scale sacrificed |
| `coalesce(d1, a)` | `decimal(12,2)` — the int widens into the decimal |
| `s = a` (string, int) | `boolean` — permitted even under ANSI |

**Rejecting aggregates and subqueries is validation, not parsing.** Spark's
expression parser accepts `count(a)`, `sum(a) > 0`, `rank() OVER (ORDER BY a)`,
`a > (SELECT 1)` and `a IN (SELECT 1)` — they parse, and most type-resolve.
What rejects them is Delta, separately and later, via
`DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT`. So a parser that refuses them
at the grammar level would be diverging from Spark rather than matching it;
the refusal belongs in a post-parse validation step, as it does upstream.

### What Delta actually does with a constraint

Measured 2026-08-10 against delta-spark 4.0.0, because the corpus above is
worthless if it is gathered under semantics Delta does not use. Cross-checked
2026-08-10 against the protocol and delta-spark's own test suites; where upstream
states or asserts a rule, it is cited below rather than resting on measurement.

**Delta parses with Spark's session parser.** Verified in the 4.0.0 jar rather
than inferred: `Constraints$.getCheckConstraints(Metadata, SparkSession)`
delegates to a lambda whose bytecode calls
`SparkSession.sessionState().sqlParser().parseExpression(String)` and wraps the
result in `Constraints$Check`. That is the same entry point oracle 1 uses, so
oracle 1 is measuring the real thing and not an approximation of it.

**Delta pins no configuration, so "what Spark does" depends on who writes the
row.** `CheckDeltaInvariant` carries no `SQLConf` reference at all; the
constraint is evaluated under whatever session performs the write. This is not
theoretical — the same table, the same constraint `a + b < 0`, and the same row
`(2147483647, 1)`:

| `spark.sql.ansi.enabled` | outcome |
|---|---|
| `false` | **ACCEPTED** — the int overflow wraps to `-2147483648`, which satisfies `< 0` |
| `true` | **REJECTED** — `ARITHMETIC_OVERFLOW` |

So a constraint does not have one behaviour to match. EngineeredWood has no
session config to inherit, which means it must *choose* a policy and document it
rather than claim Spark parity — bound at `SparkFunctionRegistry` construction,
per [Where the dialect configuration lives](#where-the-dialect-configuration-lives).
The defensible choice is ANSI: Spark 4.0
defaults `spark.sql.ansi.enabled=true` and `spark.sql.storeAssignmentPolicy=ANSI`
(both confirmed as the running defaults in the interop venv), so ANSI is what a
current-generation writer produces. Any harvested corpus must record the config
it was gathered under, and two corpora gathered under different settings must
never be compared.

**That configuration dependence is unspecified by omission, not by design**,
which is what makes choosing a policy legitimate rather than a deviation. The
[protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#check-constraints)
requires only that "evaluating the SQL expressions of CHECK constraints must
return `true` for each row in a table". It never names a dialect — the value is
"a SQL expression string", and for generated columns the metadata "SHOULD be
parsed as a SQL expression" — and it says nothing about evaluation semantics.
Neither does the [Delta Lake constraints
documentation](https://docs.delta.io/latest/delta-constraints.html).

delta-spark's tests confirm the silence is a blind spot. `CheckConstraintsSuite`
and `InvariantEnforcementSuite` contain no reference to ANSI at all, and
`GeneratedColumnSuite` — 2,235 lines — contains none to ANSI or to session
timezone. Repo-wide only two files under `spark/src/test` mention
`ansi.enabled`, and neither covers write-time constraint evaluation.

The contrast makes it conclusive. Where *Delta itself* introduces a cast, it is
meticulous: `ImplicitDMLCastingSuite` sweeps a three-dimensional matrix of
`followAnsiEnabled × ansiEnabled × storeAssignmentPolicy`, backed by a dedicated
`DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG` and the
`DELTA_CAST_OVERFLOW_IN_TABLE_WRITE` error class. Where the *user* supplies the
expression, none of that machinery applies. So there is no upstream contract to
match here, and none to violate.

**A NULL constraint result rejects the write**, confirming the `HasFalseOrNull`
shape in the CHECK-constraints pseudocode above. With constraint `a > 0`:
`a = 1` is accepted, `a = -1` is rejected, and `a = NULL` is rejected with
`DELTA_VIOLATE_CONSTRAINT`.

This one is load-bearing upstream, so it can be relied on rather than merely
observed. The protocol's "must return `true`" already excludes null by
construction, and `CheckConstraintsSuite`'s "constraints with nulls" test asserts
it directly: under `CHECK (nested.arr[0] < 100)` it expects an
`InvariantViolationException` for three separate null origins — a null element, a
null array, and a null parent struct — while the sibling constraint
`CHECK (nested.arr[1] IS NULL)` admits those same rows. It is the *result* being
null that violates, not the input.

**Not every constraint on a table would pass Delta's own validator.**
`DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT` rejects scalar subqueries and
`DELTA_UDF_IN_CHECK_CONSTRAINT` rejects UDFs, but the UDF check only fires when
`DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS` is `ASSERT`; `CheckConstraintsSuite`
asserts that under `OFF` the same `ALTER TABLE ... CHECK (external_udf(value))`
succeeds. A table can therefore carry a constraint referencing a function no
other engine can resolve, which is an argument for the parser's
unsupported-syntax error being a clean, quotable refusal rather than a crash.

**The stored constraint text is token-spaced, not canonicalised.** Delta does
not persist the expression verbatim, and it does not persist Catalyst's `.sql()`
rendering either — it re-joins the parsed token stream with single spaces,
preserving case, operator spelling and redundant parentheses:

| written | stored as |
|---|---|
| `a>0   and   b IS NOT NULL` | `a > 0 and b IS NOT NULL` |
| `SUBSTRING(s, 1, 2) = 'ab'` | `SUBSTRING ( s , 1 , 2 ) = 'ab'` |
| `((a) > (0))` | `( ( a ) > ( 0 ) )` |
| `a <> 5` | `a <> 5` |

delta-spark's own tests expect this form, so it is a real convention rather than
an artefact of how these cases were probed: `CheckConstraintsSuite` writes
`CHECK (nested.arr[1] < 5)` and asserts the resulting error quotes
`(nested . arr [ 1 ] < 5)`. Note that the *runtime* violation message renders
compactly instead (`(nested.arr[0] < 100)`) — the two error paths differ, and
only the `ALTER`-time one shows the stored text.

Three consequences for the parser. Keywords must match case-insensitively —
`and` survives as written, so a keyword table keyed on uppercase will miss it.
`<>` must be accepted rather than assumed normalised to `!=`. And parentheses
appear routinely in stored text, which is independent confirmation that kernel's
paren-free scope is unusable here. Note also that other writers (delta-rs,
EngineeredWood itself) need not follow this convention, so the parser cannot
rely on the spacing.

One surprise worth recording for whoever writes the grammar: in Spark 4.0
`a between 1 and 10` parses to an `UnresolvedFunction` rendering as
`between(a, 1, 10)`, not to a desugared pair of comparisons.

## Open Questions

1. **Where does `EngineeredWood.Expressions` live in the dependency graph?** It
   has no Arrow dependency, so it could sit alongside `EngineeredWood.Core`. It
   does not need to depend on Core itself unless we want to share types like
   `IMemoryOwner<byte>` (we don't, for the expression layer). Recommendation:
   sibling of Core, no mutual dependency.

2. **Should `Predicate` and `Expression` be separate hierarchies, or should
   `Predicate` be a tag interface on `Expression`?** delta-kernel-rs separates
   them; Iceberg merges them (`AndExpression` extends `Expression`). Separation
   is more type-safe (you can't accidentally use a value expression where a
   predicate is required) but creates two parallel hierarchies. Recommendation:
   separate, with `Predicate` as a subclass of `Expression` for cases that
   need to mix them (function arguments, CASE branches).

3. **Should the Iceberg migration happen before or after Parquet pushdown?**
   Resolved: Iceberg migrated first (Phase 4 before Phase 5), as
   recommended. The API shape held up against the existing Iceberg tests
   before Parquet and Delta consumers were added.

4. **Bloom filter probing automatic or opt-in?** Requires I/O even when stats
   alone prove `AlwaysFalse`. Recommendation: opt-in via
   `FilterUseBloomFilters`, as proposed.

5. **Should `ReadRowGroupAsync` also accept a filter?** Currently only
   `ReadAllAsync` would use it. Defer until page-level pushdown exists.

6. **Where do per-format expression semantics live — one shared tree, or a
   dialect AST per format lowered into one?** Resolved: one shared tree, dialect
   bound at registry construction. Raised while sizing phase 9 because the parser
   is the first front end that would exercise the answer. Sharing nothing was
   ruled out at the outset. The two live options
   are (a) one shared tree both front ends emit into, dialect differences carried
   by an evaluation policy, which is the status quo — `23cfc4e` migrated Iceberg
   off its parallel expression types onto this library — and (b) a dialect AST
   per format, lowered into a shared tree.

   What the code says, as of this writing:

   - **The symmetry option (b) assumes is not there.** Delta stores Spark SQL
     strings in table metadata and needs a real parser. Iceberg has no expression
     grammar to parse: its partition transforms are a closed record hierarchy in
     `EngineeredWood.Iceberg/Transform.cs` (`Identity`, `Bucket(int)`,
     `Truncate(int)`, `Year`/`Month`/`Day`/`Hour`, `Void`) decoded from JSON by
     `TransformConverter`, and its predicates arrive programmatically from the
     host. So "a parser per format emitting into a tree" is one parser plus a
     JSON decoder that already exists.
   - **`FunctionCall` has no producers.** It is constructed only by the two
     `Expressions.Call` factories and Iceberg's compatibility shim, and consumed
     by `ExpressionBinder` and the four evaluators as pass-through-or-throw.
     Iceberg partition transforms never become `FunctionCall`s. The open-ended,
     string-keyed node that exists *because of* Iceberg's transform pattern is
     not used for Iceberg transforms or for anything else, and the Spark parser
     would be its first real producer.
   - **The formats agree where the tree is most used.** `StringOrdering` records
     that Parquet, Delta, Iceberg and Vortex all specify string min/max ordering
     over UTF-8 bytes. Divergence is concentrated in row-level evaluation, which
     is Delta-only today.

     That last clause has since expired, and it was load-bearing. Iceberg's
     [expressions spec](https://github.com/apache/iceberg/blob/main/format/expressions-spec.md)
     specifies row-level semantics that differ from Spark's at the operator
     level: predicates are two-valued and comparisons are null-safe, so
     Iceberg's `=` means our `NullSafeEqual`. It stays latent only because
     Iceberg has no data-file read path, and the
     [derived-column RFC](https://github.com/apache/iceberg/issues/15923) would
     end that. See
     [Normalising Iceberg expressions](#normalising-iceberg-expressions).

   A useful way to sort the question: for each expected semantic difference, ask
   whether it resolves at lowering time (Spark `LIKE 'x%'` to `StartsWith`, `<>`
   to `Not(Equal)`), is pervasive and live (decimal promotion, ANSI cast and
   overflow, division by zero), or is localized and live (a named function one
   format has and the other does not). Only a fourth kind — structural, with no
   representation in the shared tree and no lowering into one — would actually
   require (b), and none has been found for Iceberg.

   **Resolved 2026-08-11: (a), one shared tree.** No structural difference was
   found, the symmetry (b) assumes is absent, and (a) is the status quo already
   paid for. Dialect semantics are bound at `SparkFunctionRegistry` construction
   — see [Where the dialect configuration lives](#where-the-dialect-configuration-lives),
   which also records why that beats resolving the dialect in the parser.

   **Amended later the same day**, after reading Iceberg's expressions spec. The
   answer holds and the premise it rested on holds — Iceberg expressions are
   JSON, so a derived-column feature needs a decoder rather than a second parser
   — but the account of *where dialect lives* was incomplete. Registry-bound
   configuration covers function semantics and nothing else; it cannot express
   "`=` is null-safe here and null-propagating there", because comparison and
   boolean semantics are evaluated from the nodes themselves.

   The missing piece is not a policy at the evaluator. It is that the tree has a
   canonical semantics and each front end normalises into it — stated now under
   [The tree has one semantics](#the-tree-has-one-semantics-and-front-ends-normalise-into-it),
   where it had previously only been implicit in what the Spark parser happened
   to do.

   One correction to the reasoning above, since it was used to reach this
   answer and was partly wrong. The six-consumers argument was aimed at
   dialect-split *node types* (`SparkAdd` versus `IcebergAdd`), which would
   indeed force every consumer to handle both arms. It does not apply to
   dialect-split *function names* (`+_ansi` versus `+_legacy`), which cost
   nothing structurally because the registry dispatches on strings already. That
   option was rejected on lifetime grounds instead — parse happens once per table
   open, evaluation happens per write — not on consumer count.

   Related and resolved the same day, from
   [#101](https://github.com/clast-project/engineered-wood/issues/101):
   arithmetic stays in `FunctionCall` rather than gaining a closed node — see the
   note under [Expression tree](#expression-tree).
