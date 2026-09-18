// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Expressions.Arrow;

/// <summary>
/// Evaluates expressions and predicates against an Arrow
/// <see cref="RecordBatch"/>, producing typed Arrow arrays.
/// </summary>
public interface IRowEvaluator
{
    /// <summary>
    /// Evaluates a predicate against every row in the batch. Returns a
    /// <see cref="BooleanArray"/> of the same length: each element is
    /// <c>true</c>/<c>false</c> per SQL semantics, or <c>null</c> when the
    /// predicate produced an unknown result for that row (e.g. comparison
    /// with a NULL operand).
    /// </summary>
    BooleanArray EvaluatePredicate(Predicate predicate, RecordBatch batch);

    /// <summary>
    /// Evaluates a value expression against every row. The returned array's
    /// type is inferred from the values: column references return the
    /// underlying column, literals return a constant array, function calls
    /// return whatever the registered function produces. Value kinds whose
    /// Arrow type cannot be reconstructed from the value alone (decimal,
    /// timestamp, date, unsigned integers) are not supported by this overload —
    /// use <see cref="EvaluateExpression(Expression, RecordBatch, IArrowType)"/>.
    /// </summary>
    IArrowArray EvaluateExpression(Expression expression, RecordBatch batch);

    /// <summary>
    /// Evaluates a value expression against every row and materializes it as
    /// <paramref name="targetType"/>. Supplying the target Arrow type resolves
    /// the metadata a bare value cannot carry — a decimal's precision/scale and
    /// physical width, a timestamp's unit and timezone, date vs timestamp — so
    /// decimal, timestamp and date results (in addition to the inferrable
    /// primitives) can be produced faithfully.
    /// </summary>
    IArrowArray EvaluateExpression(Expression expression, RecordBatch batch, IArrowType targetType);
}

/// <summary>
/// Pluggable registry for function calls invoked during row evaluation.
/// Format-specific function libraries (e.g. Spark SQL functions) implement
/// this interface to provide their own functions to the evaluator.
/// </summary>
public interface IFunctionRegistry
{
    /// <summary>Returns true if a function with the given name is registered.</summary>
    bool IsRegistered(string name);

    /// <summary>
    /// Invokes a function. Implementations must return an array of length
    /// <paramref name="rowCount"/>.
    /// </summary>
    IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount);
}

/// <summary>
/// A function registry whose functions do not all evaluate every argument.
/// </summary>
/// <remarks>
/// <para>
/// Optional; the evaluator asks for it with an <c>as</c> cast. Which arguments a call evaluates
/// is the function's own business: Spark's conditional family evaluates a branch only over the
/// rows that select it, so a failing cast, an overflow or a division by zero in a branch no row
/// reaches never happens. <c>coalesce(a, CAST(s AS DOUBLE))</c> answers over a batch where
/// <c>a</c> has no nulls and raises over one where it has one — the same expression, decided per
/// row.
/// </para>
/// <para>
/// A branch no row selects is still evaluated, over no rows. Spark types a conditional from every
/// branch, reached or not — <c>coalesce(f, 'x')</c> is a <c>double</c> even where the string is
/// never chosen, and <c>if(a &gt; 0, a, bin)</c> is refused outright — and an evaluation over an
/// empty selection yields the branch's type without reading a value that could raise.
/// </para>
/// </remarks>
public interface IShortCircuitingFunctions
{
    /// <summary>
    /// Whether <paramref name="name"/> decides for itself which of its arguments to evaluate,
    /// and over which rows.
    /// </summary>
    /// <remarks>
    /// A name this answers false for — most of them, including <c>nullif</c>, <c>greatest</c> and
    /// <c>least</c>, which Spark evaluates eagerly — goes through
    /// <see cref="IFunctionRegistry.Invoke"/> with every argument already evaluated.
    /// </remarks>
    bool ShortCircuits(string name);

    /// <summary>
    /// Invokes a short-circuiting function. Arguments are evaluated through
    /// <paramref name="arguments"/> rather than supplied, so the implementation chooses what to
    /// evaluate and over which rows.
    /// </summary>
    IArrowArray Invoke(string name, IConditionalArguments arguments, int rowCount);
}

/// <summary>
/// The arguments of a short-circuiting call, each evaluated only over the rows asked for.
/// </summary>
public interface IConditionalArguments
{
    /// <summary>How many arguments the call was written with.</summary>
    int Count { get; }

    /// <summary>
    /// Whether argument <paramref name="index"/> is a bare <c>NULL</c> literal.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark types one as <c>void</c>, which constrains nothing: <c>coalesce(a, NULL)</c> is an
    /// <c>int</c>. A conditional leaves such a branch out of its type fold, and has to recognise
    /// one structurally rather than by noticing that a branch came back all null. Under
    /// short-circuiting a branch no row selects is all null by construction, so a content test
    /// would retype the result — a zero-row <c>coalesce(a, s)</c> would come back <c>int</c>
    /// where Spark says <c>bigint</c> — and it would also misread a string column that merely
    /// held nothing in this batch.
    /// </para>
    /// <para>
    /// An implementation may answer from the expression or from the type. An evaluator-driven
    /// call reads the tree, where a bare <c>NULL</c> is a literal and <c>CAST(NULL AS INT)</c> —
    /// whose type Spark does let constrain the result — is not; an eager one reads the column,
    /// which is a <c>void</c> column for a bare NULL and the cast's own type for a cast.
    /// </para>
    /// </remarks>
    bool IsNullLiteral(int index);

    /// <summary>
    /// Evaluates argument <paramref name="index"/> over the rows <paramref name="rows"/> selects,
    /// and returns an array of the call's full row count whose other rows are NULL.
    /// </summary>
    /// <remarks>
    /// Full length, so the caller indexes it by row number like any other argument; NULL
    /// elsewhere, so an unrequested row reads as "not known" rather than as another row's answer.
    /// An empty selection evaluates nothing and yields an all-null array of the type the argument
    /// would have produced, which is what a conditional needs for a branch nothing reached. A
    /// full selection is passed straight through with no copying.
    /// </remarks>
    IArrowArray Evaluate(int index, ReadOnlySpan<bool> rows);
}

/// <summary>
/// A function registry that also knows which operand a comparison casts, and to what.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and separate from <see cref="IFunctionRegistry"/>: a registry says what <c>cast</c>
/// does; this says when a comparison inserts one. <see cref="ArrowRowEvaluator"/> asks for it
/// with an <c>as</c> cast, and without it inserts no comparison casts.
/// </para>
/// <para>
/// The rule is dialect-dependent and the evaluator has no dialect. ANSI compares <c>'0.1'</c>
/// against a <c>float</c> column through <c>double</c> and the legacy dialect compares it as a
/// <c>float</c>; see the <c>string-coercion</c> group of
/// <c>Fixtures/spark-expression-corpus.json</c>.
/// </para>
/// <para>
/// Asked per operand rather than per pair, because which side moves is part of the answer: a
/// string against a number is cast to the number, while a string against a binary stays put and
/// the binary is rendered as text. Splitting the target from the cast also keeps the evaluator
/// lazy — it materialises an operand as an Arrow array only once a target says one of them moves.
/// </para>
/// </remarks>
public interface IComparisonCoercion
{
    /// <summary>
    /// The type <paramref name="operand"/> must be cast to before it can be compared against a
    /// value of <paramref name="other"/>, or null when this operand needs no cast.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Null is an answer, not a failure: it is what both operands of an ordinary comparison get,
    /// and what a pair with no rule at all gets. A caller asks about each operand in turn and
    /// casts at most the one that comes back with a target.
    /// </para>
    /// <para>
    /// Asked with the operator because one rule is equality's alone: under the legacy dialect
    /// Spark's <c>BooleanEquality</c> coercion casts a BOOLEAN operand to the numeric it is
    /// compared against, for <c>=</c>, <c>&lt;&gt;</c> and <c>&lt;=&gt;</c> only. <c>a = bl</c>
    /// answers while <c>a &lt; bl</c> is <c>DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES</c> in both
    /// dialects.
    /// </para>
    /// </remarks>
    IArrowType? ComparisonTarget(ComparisonOperator op, IArrowType operand, IArrowType other);

    /// <summary>
    /// The one type every member of a set membership test must be cast to, or null when the set
    /// needs no coercion.
    /// </summary>
    /// <remarks>
    /// Separate from <see cref="ComparisonTarget"/> because <c>IN</c> is not the disjunction of
    /// equalities it looks like: Spark resolves one type over the operand and the whole list,
    /// where a comparison resolves each pair on its own. Under the legacy dialect
    /// <c>a IN ('01')</c> is false, since the list resolves to text and <c>'1'</c> is not
    /// <c>'01'</c>, while <c>a = '01'</c> is true. Every member is cast to the target, including
    /// those that already match.
    /// </remarks>
    IArrowType? SetComparisonTarget(IReadOnlyList<IArrowType> memberTypes);

    /// <summary>
    /// Casts an operand to a target <see cref="ComparisonTarget"/> or
    /// <see cref="SetComparisonTarget"/> returned for it.
    /// </summary>
    /// <remarks>
    /// Under a raising dialect this may throw rather than return, because a comparison against a
    /// value the cast refuses is a refused comparison.
    /// </remarks>
    IArrowArray CastForComparison(IArrowArray operand, IArrowType target, int rowCount);
}

/// <summary>
/// A function registry that reads an integral literal against a decimal as a narrower decimal
/// than its type implies.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and asked for with an <c>as</c> cast: a registry that does not implement it types
/// every literal from its own kind.
/// </para>
/// <para>
/// Spark reads a literal <c>2</c> met with a decimal as <c>decimal(1,0)</c> rather than as the
/// <c>decimal(10,0)</c> an <c>int</c> occupies, and those nine integer digits are nine the result
/// keeps as scale: <c>1.5BD / 2</c> is <c>decimal(7,6)</c> and <c>d1 + 2</c> over a
/// decimal(10,2) is <c>decimal(11,2)</c>.
/// </para>
/// <para>
/// The registry owns this because where it applies is dialect knowledge, and it is narrow. Spark
/// inserts the cast at a binary operator and nowhere else, so arithmetic and comparison take it
/// while <c>greatest</c>, <c>coalesce</c> and an <c>IN</c> list do not:
/// <c>CAST(4E-32 AS DECIMAL(38,38)) = 0</c> is FALSE and the same value <c>IN (0)</c> is TRUE.
/// Asking per site keeps that boundary where Spark puts it.
/// </para>
/// </remarks>
public interface ILiteralPrecisionRules
{
    /// <summary>
    /// The type an integral literal takes as argument <paramref name="argumentIndex"/> of
    /// <paramref name="function"/>, against an argument of <paramref name="other"/>; null to
    /// leave the literal as it is.
    /// </summary>
    /// <remarks>
    /// Asked per argument position because <c>nullif</c> takes the rule on its second argument
    /// and not on its first. From the optimized plans, <c>nullif(d5, 0)</c> compares at
    /// decimal(38,37) — the literal narrowed — while <c>nullif(0, d5)</c> compares at
    /// decimal(38,28), the pair's common type with the literal read as an <c>int</c>, so the two
    /// answer differently over the same values.
    /// </remarks>
    IArrowType? LiteralArgumentType(string function, int argumentIndex, long value, IArrowType other);

    /// <summary>
    /// The type an integral literal operand of a comparison takes against an operand of
    /// <paramref name="other"/>; null to leave it as it is.
    /// </summary>
    /// <remarks>
    /// Separate from <see cref="LiteralArgumentType"/> because a comparison is not a call: it
    /// resolves both operands' types to pick the one they compare through — see
    /// <see cref="IComparisonCoercion.ComparisonTarget"/> — rather than invoking a function over
    /// them. Every comparison operator takes the rule, in both operand orders.
    /// </remarks>
    IArrowType? LiteralComparisonType(long value, IArrowType other);
}

/// <summary>
/// A function registry with calls that mean something different when every argument is a
/// constant.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and asked for with an <c>as</c> cast: a registry that does not implement it
/// evaluates every call the same way whatever its arguments are.
/// </para>
/// <para>
/// Spark reads <c>epoch</c>, <c>today</c>, <c>yesterday</c>, <c>tomorrow</c> and <c>now</c> as
/// dates and timestamps in a cast over a constant, and refuses the same word arriving in a row.
/// That is <c>SpecialDatetimeValues</c>, an optimizer rule over <c>Cast(e, DateType)</c> with
/// <c>e.foldable</c>: <c>CAST('epoch' AS DATE)</c> is 1970-01-01 while
/// <c>CAST(CASE WHEN a &gt; 0 THEN 'epoch' ELSE 'epoch' END AS DATE)</c> — a string that is
/// <c>'epoch'</c> in every row — is <c>CAST_INVALID_INPUT</c>.
/// </para>
/// <para>
/// Whether an argument is constant is a property of the tree, which only
/// <see cref="ArrowRowEvaluator"/> can see; which calls care and what they answer is dialect
/// knowledge, which only the registry has. So the evaluator decides when to ask and the registry
/// decides what to answer. It is asked with the arguments already evaluated: Spark's rule calls
/// <c>e.eval()</c> itself, so an error inside the operand happens either way, and handing over
/// arrays keeps this interface free of the expression tree.
/// </para>
/// </remarks>
public interface IConstantFoldedFunctions
{
    /// <summary>
    /// The value a call to <paramref name="name"/> takes when every one of its arguments is
    /// constant, or null to invoke it as usual.
    /// </summary>
    /// <remarks>
    /// Null is the ordinary answer: it is what every name without a rule gets, and what a name
    /// with one gets whenever the constant is not a value the rule recognises — a cast to a type
    /// the rule does not cover, or a string outside the vocabulary. Both must fall through to the
    /// cast so that the dialect decides whether they refuse or read null.
    /// </remarks>
    IArrowArray? InvokeOverConstants(string name, IReadOnlyList<IArrowArray> args, int rowCount);
}

/// <summary>
/// A function registry that knows which of its functions can never produce a null.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and asked for with an <c>as</c> cast. It supplies the function-call half of the
/// judgement only: a registry that does not implement it makes every call nullable, so nothing
/// containing one is folded, while the shapes <see cref="ArrowRowEvaluator"/> judges for itself —
/// a non-null literal, a nested <c>IS NULL</c>, <c>&lt;=&gt;</c>, and the connectives over them —
/// still fold.
/// </para>
/// <para>
/// Spark discards the operand of <c>IS NULL</c> / <c>IS NOT NULL</c> when the operand is provably
/// non-nullable, so an error inside it never happens. Under ANSI,
/// <c>CASE WHEN (CAST('abc' AS INT) &gt; 1) THEN 1 ELSE 2 END IS NULL</c> answers <c>false</c>
/// while the same CASE without the <c>IS NULL</c> raises CAST_INVALID_INPUT. That is Spark's
/// <c>NullPropagation</c>, which fires on the whole predicate before a row is read, not per row.
/// </para>
/// <para>
/// This is an allow-list, and the default must be "nullable". Wrongly claiming a function is
/// never null turns <c>x IS NOT NULL</c> into a constant <c>true</c> — which, inside a Delta
/// CHECK constraint, admits a row Spark rejects — while under-claiming costs only the fold. A
/// name this does not recognise, and every name added to a registry later, must answer false
/// until it has been measured.
/// </para>
/// </remarks>
public interface INullabilityRules
{
    /// <summary>
    /// Whether a call to <paramref name="name"/> can never produce a null, given which of its
    /// arguments can never produce one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Asked with the arguments' own answers rather than the argument expressions, so every rule
    /// stated here is positional: <c>coalesce</c> is never null if any argument is, <c>if</c> and
    /// <c>nvl2</c> if their two result arguments are, <c>case</c> if it has an ELSE and every
    /// result, ELSE included, is.
    /// </para>
    /// <para>
    /// A function whose nullability depends on its argument types must answer false, which is
    /// why arithmetic is absent from <c>SparkFunctionRegistry</c>'s list. Under ANSI,
    /// <c>CAST(1 AS DECIMAL(10,2)) + CAST(1 AS DECIMAL(10,2))</c> is nullable (its sum wants
    /// decimal(11,2)) while <c>CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))</c> is not,
    /// though both add two non-null literals. Answering true for arithmetic would fold
    /// <c>(d + d) IS NOT NULL</c> to a constant and suppress a real overflow.
    /// </para>
    /// </remarks>
    bool NeverNull(string name, ReadOnlySpan<bool> argumentsNeverNull);
}

/// <summary>
/// A function registry that knows which expressions its dialect's analyzer refuses on type
/// grounds, before a row is read.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and asked for with an <c>as</c> cast. A registry that does not implement it refuses
/// nothing: a comparison between operands with no rule between them answers null per row rather
/// than being rejected. Silence has to be the default because the evaluator is shared —
/// <c>DeltaTable</c>, <c>LanceTable</c> and <c>LanceDatasetWriter</c> each construct one with no
/// registry at all, and none of them wants Spark's analyzer.
/// </para>
/// <para>
/// Spark refuses these at analysis, and answering them instead is not harmless: a Delta table
/// carrying such a constraint would be readable here while every Spark write against it fails,
/// including a write that satisfies the constraint. The refusals not yet covered are #286.
/// </para>
/// <para>
/// Every method answers rather than throws, and null means accepted, so one set of rules serves
/// both callers. Evaluation has a row to refuse against and abandons the batch at the first
/// failure: <see cref="ArrowRowEvaluator"/> turns a diagnostic into an
/// <see cref="ExpressionAnalysisException"/> at the site that asked. A definition-time pass —
/// validating a <c>CHECK</c> constraint or generation expression against a schema and no data —
/// would instead collect every diagnostic into a report.
/// </para>
/// <para>
/// The dialect belongs to the implementation, not to the caller. Spark's analyzer refuses
/// different sets under the two dialects — boolean joins the numeric family for equality only,
/// and only under the legacy dialect — so the evaluator, which has no dialect, must not try to
/// state the rule. This is the step before <see cref="IComparisonCoercion"/>: that one says which
/// operand a comparison casts, this one says whether there is a comparison to make at all.
/// </para>
/// </remarks>
public interface IAnalysisRules
{
    /// <summary>
    /// Why a comparison of <paramref name="left"/> against <paramref name="right"/> under
    /// <paramref name="op"/> is refused at analysis, or null when it is accepted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Asked with the operator because equality and ordering do not answer alike: under the
    /// legacy dialect <c>a = bl</c> compares an int against a boolean by casting the boolean to
    /// the number, while <c>a &lt; bl</c> is refused as
    /// <c>DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES</c> in both dialects.
    /// </para>
    /// <para>
    /// This also covers boolean context, which is why there is no method for it. The parser
    /// lowers a non-boolean in predicate position to <c>expr = TRUE</c> and <c>IS TRUE</c> to
    /// <c>expr &lt;=&gt; TRUE</c>, so <c>0.5 IS TRUE</c> and <c>0.5 AND x</c> arrive here as an
    /// ordinary comparison against a boolean.
    /// </para>
    /// <para>
    /// A set test does not come here, and asking per member with <c>Equal</c> would be wrong:
    /// <c>a = bl</c> answers under the legacy dialect while <c>a IN (bl)</c> is refused in both,
    /// because a set resolves one type over the operand and every member. Use
    /// <see cref="CheckSetComparison"/>.
    /// </para>
    /// <para>
    /// Not asked about an operand whose type is unknown — a bare <c>NULL</c> literal. Spark types
    /// one <c>void</c>, which is comparable with everything, and the caller cannot supply a type
    /// it does not have without inventing one.
    /// </para>
    /// </remarks>
    AnalysisDiagnostic? CheckComparison(ComparisonOperator op, IArrowType left, IArrowType right);

    /// <summary>
    /// Why a cast from <paramref name="source"/> to <paramref name="target"/> is refused at
    /// analysis, or null when it is allowed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A property of the two types, asked once, not of a value read inside a row loop: a refusal
    /// thrown after reading a value makes the same expression refuse or answer depending on the
    /// batch.
    /// </para>
    /// <para>
    /// <paramref name="tryCast"/> selects the table, not the outcome of a failure.
    /// <c>try_cast</c> uses the ANSI cast table under both dialects, so every ANSI-only refusal is
    /// also a legacy <c>try_cast</c> refusal — and it is a refusal, not the null that
    /// <c>try_cast</c> gives a value it cannot convert.
    /// </para>
    /// <para>
    /// <see cref="ArrowRowEvaluator"/> never asks this: a cast is a function call, and only the
    /// registry that implements it can read its target type. It is here so that the cast table is
    /// stated once, where a definition-time pass can reach it too.
    /// </para>
    /// </remarks>
    AnalysisDiagnostic? CheckCast(IArrowType source, IArrowType target, bool tryCast);

    /// <summary>
    /// Why a set membership test over <paramref name="memberTypes"/> — the operand's type first,
    /// then each member's — is refused at analysis, or null when it is accepted.
    /// </summary>
    /// <remarks>
    /// Separate from <see cref="CheckComparison"/> for the reason
    /// <see cref="IComparisonCoercion.SetComparisonTarget"/> is separate from
    /// <see cref="IComparisonCoercion.ComparisonTarget"/>: <c>IN</c> resolves one type over the
    /// operand and the whole list, so it can refuse a pair an equality accepts. One list rather
    /// than an operand and a list, because the resolution does not privilege the operand. A bare
    /// <c>NULL</c> member is left out by the caller, since <c>void</c> constrains nothing.
    /// </remarks>
    AnalysisDiagnostic? CheckSetComparison(IReadOnlyList<IArrowType> memberTypes);
}

/// <summary>
/// Why an expression is refused at analysis, in the terms the engine whose analyzer refused it
/// would have used.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="ErrorClass"/> carries that engine's own name for the condition —
/// <c>DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES</c>,
/// <c>DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION</c> — so a caller reporting a refused write can
/// name the condition the user would have seen from Spark, and a recorded refusal can be matched
/// by class rather than by message text.
/// </para>
/// <para>
/// It carries no location: a rule is a function of types and knows nothing of the tree it was
/// asked about, so the caller attaches the node. That keeps one rule usable from a row evaluator,
/// which raises immediately, and from a pass over a whole constraint, which reports several at
/// once.
/// </para>
/// </remarks>
/// <param name="ErrorClass">The engine's name for the condition, without surrounding brackets.</param>
/// <param name="Message">The condition, spelled for a human.</param>
public sealed record AnalysisDiagnostic(string ErrorClass, string Message);
