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
/// Optional, and asked for with an <c>as</c> cast, so a registry that does not implement it is
/// invoked exactly as before. It exists because <b>which</b> arguments a call evaluates is the
/// function's own business and the evaluator has no way to guess it: Spark's conditional family
/// evaluates a branch only over the rows that select it, so a cast that would fail, an overflow
/// or a division by zero in a branch no row reaches never happens at all. Measured on 4.0.1,
/// <c>coalesce(a, CAST(s AS DOUBLE))</c> answers over a batch where <c>a</c> has no nulls and
/// raises over one where it has one — the same expression, decided per ROW. #279.
/// </para>
/// <para>
/// <b>A branch no row selects is still evaluated, over no rows at all.</b> That is not a
/// wasted call: Spark types a conditional from every branch, reached or not —
/// <c>coalesce(f, 'x')</c> is a <c>double</c> even where the string is never chosen, and
/// <c>if(a &gt; 0, a, bin)</c> is refused outright — and an evaluation over an empty row
/// selection produces the branch's type without reading a value that could raise.
/// </para>
/// </remarks>
public interface IShortCircuitingFunctions
{
    /// <summary>
    /// Whether <paramref name="name"/> decides for itself which of its arguments to evaluate,
    /// and over which rows.
    /// </summary>
    /// <remarks>
    /// A name this answers false for — which is most of them, <c>nullif</c>, <c>greatest</c> and
    /// <c>least</c> included, all three measured eager in Spark — goes through
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
    /// <c>int</c>. A conditional therefore has to leave such a branch out of its type fold, and
    /// this is how it tells one apart — <b>structurally</b>, rather than by noticing that a branch
    /// came back all null.
    /// </para>
    /// <para>
    /// The difference is not academic. Under short-circuiting a branch no row selects is all null
    /// BY CONSTRUCTION, so a content test would swallow every unreached branch and retype the
    /// result: a zero-row <c>coalesce(a, s)</c> would come back <c>int</c> where Spark says
    /// <c>bigint</c>. It was also wrong for a string column that merely held nothing in this
    /// batch, which is #293.
    /// </para>
    /// <para>
    /// <b>An implementation may answer from the expression or from the type</b>, and both are in
    /// use: an evaluator-driven call reads the tree, where a bare <c>NULL</c> is a literal and
    /// <c>CAST(NULL AS INT)</c> — which carries a type Spark DOES let constrain the result — is
    /// not; an eager one reads the column, which since #293 is a <c>void</c> column for a bare
    /// NULL and the cast's own type for a cast. They agree because the materialisation makes them
    /// agree, not because either is a fallback for the other.
    /// </para>
    /// </remarks>
    bool IsNullLiteral(int index);

    /// <summary>
    /// Evaluates argument <paramref name="index"/> over the rows <paramref name="rows"/> selects,
    /// and returns an array of the call's full row count whose other rows are NULL.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Full length, so the caller indexes it by row number like any other argument. NULL
    /// elsewhere, so a row the caller did not ask for reads as "not known" rather than as another
    /// row's answer.
    /// </para>
    /// <para>
    /// An empty selection evaluates nothing and yields an all-null array of the type the argument
    /// WOULD have produced — which is the answer a conditional needs for a branch nothing reached.
    /// A full selection is passed straight through with no copying.
    /// </para>
    /// </remarks>
    IArrowArray Evaluate(int index, ReadOnlySpan<bool> rows);
}

/// <summary>
/// A function registry that also knows which operand a comparison casts, and to what.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and separate from <see cref="IFunctionRegistry"/> because it answers a different
/// question. A registry says what <c>cast</c> does; this says when a comparison inserts one.
/// <see cref="ArrowRowEvaluator"/> asks for it with an <c>as</c> cast, so a registry that does
/// not implement it leaves comparison exactly as it was.
/// </para>
/// <para>
/// It exists because the rule is dialect-dependent and the evaluator has no dialect. Measured
/// against Spark 4.0, ANSI compares <c>'0.1'</c> against a <c>float</c> column through
/// <c>double</c> and the legacy dialect compares it as a <c>float</c> — the same expression,
/// two answers, and only the registry knows which. See the <c>string-coercion</c> group of
/// <c>Fixtures/spark-expression-corpus.json</c>.
/// </para>
/// <para>
/// Asked per OPERAND rather than per pair, because which side moves is part of the answer:
/// a string against a number is cast to the number, while a string against a binary stays put
/// and the BINARY is rendered as text. Splitting the target from the cast also keeps the
/// evaluator lazy — it materialises an operand as an Arrow array only once a target says one
/// of them moves.
/// </para>
/// </remarks>
public interface IComparisonCoercion
{
    /// <summary>
    /// The type <paramref name="operand"/> must be cast to before it can be compared against a
    /// value of <paramref name="other"/>, or null when this operand needs no cast.
    /// </summary>
    /// <remarks>
    /// Null is an answer, not a failure: it is what both operands of an ordinary comparison get,
    /// and what a pair with no rule at all gets. A caller asks about each operand in turn and
    /// casts at most the one that comes back with a target.
    /// </remarks>
    IArrowType? ComparisonTarget(IArrowType operand, IArrowType other);

    /// <summary>
    /// The one type every member of a set membership test must be cast to, or null when the set
    /// needs no coercion.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Separate from <see cref="ComparisonTarget"/> because <c>IN</c> is not the disjunction of
    /// equalities it looks like: Spark resolves ONE type over the operand and the whole list,
    /// where a comparison resolves each pair on its own. Measured, the two disagree — under the
    /// legacy dialect <c>a IN ('01')</c> is false, since the list resolves to text and
    /// <c>'1'</c> is not <c>'01'</c>, while <c>a = '01'</c> is true.
    /// </para>
    /// <para>
    /// One target for everything, rather than a moving side: every member is cast to it,
    /// including those that already match.
    /// </para>
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
