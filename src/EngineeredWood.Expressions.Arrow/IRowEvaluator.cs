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

/// <summary>
/// A function registry that reads an integral LITERAL against a decimal as a narrower decimal
/// than its type implies.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and asked for with an <c>as</c> cast: a registry that does not implement it types
/// every literal from its own kind, exactly as before.
/// </para>
/// <para>
/// It exists because Spark reads a literal <c>2</c> met with a decimal as <c>decimal(1,0)</c>
/// rather than as the <c>decimal(10,0)</c> an <c>int</c> occupies, and those nine integer digits
/// are nine the result keeps as scale: measured on 4.0.3, <c>1.5BD / 2</c> is
/// <c>decimal(7,6)</c> and <c>d1 + 2</c> over a decimal(10,2) is <c>decimal(11,2)</c>. #281.
/// </para>
/// <para>
/// <b>The evaluator cannot own this rule, because where it applies is dialect knowledge and it
/// is narrow.</b> Spark inserts the cast at a binary operator and nowhere else, so arithmetic
/// and comparison take it while <c>greatest</c>, <c>coalesce</c> and an <c>IN</c> list do not —
/// measured one expression apart, <c>CAST(4E-32 AS DECIMAL(38,38)) = 0</c> is FALSE and the same
/// value <c>IN (0)</c> is TRUE. Asking the registry per SITE is what keeps that boundary in the
/// place Spark puts it.
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
    /// Asked per ARGUMENT POSITION, because one measured rule needs it: <c>nullif</c> takes the
    /// rule on its second argument and not on its first. From the optimized plans,
    /// <c>nullif(d5, 0)</c> compares at decimal(38,37) — the literal narrowed — while
    /// <c>nullif(0, d5)</c> compares at decimal(38,28), the pair's common type with the literal
    /// read as an <c>int</c>. The two therefore answer differently over the same values.
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
/// A function registry that knows which of its functions can never produce a null.
/// </summary>
/// <remarks>
/// <para>
/// Optional, and asked for with an <c>as</c> cast. It supplies the FUNCTION-CALL half of the
/// judgement only: a registry that does not implement it makes every call nullable, so nothing
/// containing one is folded, while the shapes <see cref="ArrowRowEvaluator"/> judges for itself —
/// a non-null literal, a nested <c>IS NULL</c>, <c>&lt;=&gt;</c>, and the connectives over them —
/// still fold.
/// </para>
/// <para>
/// It exists because Spark DISCARDS the operand of <c>IS NULL</c> / <c>IS NOT NULL</c> when the
/// operand is provably non-nullable, so an error inside it never happens. Measured on 4.0.3 under
/// ANSI, <c>CASE WHEN (CAST('abc' AS INT) &gt; 1) THEN 1 ELSE 2 END IS NULL</c> answers
/// <c>false</c> while the same CASE without the <c>IS NULL</c> raises CAST_INVALID_INPUT, and
/// <c>(2147483647 + 1) IS NULL</c> answers <c>false</c> where the same sum compared against zero
/// raises ARITHMETIC_OVERFLOW. That is Spark's <c>NullPropagation</c>, and it fires on the whole
/// predicate before a row is read rather than per row, so it is not the laziness of #279/#306.
/// #319.
/// </para>
/// <para>
/// <b>An ALLOW-LIST, and the default must be "nullable".</b> The judgement only ever makes a
/// difference when it says a function can never be null, and saying so wrongly turns
/// <c>x IS NOT NULL</c> into a constant <c>true</c> — which, inside a Delta CHECK constraint,
/// admits a row Spark rejects. Under-claiming costs nothing but the fold. So a name this does not
/// recognise, and every name added to a registry later, must answer false until it has been
/// measured.
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
    /// Asked with the arguments' own answers rather than the argument expressions, because every
    /// rule an implementation may state here has to be POSITIONAL: <c>coalesce</c> is never null
    /// if ANY argument is, <c>if</c> and <c>nvl2</c> if their two RESULT arguments are (the first
    /// argument's nullability does not reach the answer), <c>case</c> if every result and a
    /// present ELSE are.
    /// </para>
    /// <para>
    /// <b>A function whose nullability depends on its argument TYPES must answer false</b>, and
    /// that is not a corner case — it is why arithmetic is absent from
    /// <c>SparkFunctionRegistry</c>'s own list. Measured under ANSI,
    /// <c>CAST(1 AS DECIMAL(10,2)) + CAST(1 AS DECIMAL(10,2))</c> is NULLABLE (its sum wants
    /// decimal(11,2)) while <c>CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0))</c> is not,
    /// and both are two non-null literals added together. An implementation that answered true
    /// for arithmetic because "all arguments are non-null" would fold
    /// <c>(d + d) IS NOT NULL</c> to a constant and suppress a real overflow.
    /// </para>
    /// </remarks>
    bool NeverNull(string name, ReadOnlySpan<bool> argumentsNeverNull);
}
