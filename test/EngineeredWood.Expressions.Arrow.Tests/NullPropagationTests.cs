// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The machinery behind #319 — not evaluating the operand of <c>IS NULL</c> / <c>IS NOT NULL</c>
/// when it can never be null.
/// </summary>
/// <remarks>
/// What Spark ANSWERS is pinned by the corpus's <c>null-propagation</c> group, which is the gate.
/// This covers the three things that group cannot reach from its fixed schema: a column declared
/// NON-NULLABLE (every column of the corpus frame is nullable, because it is built with
/// <c>createDataFrame([], ddl)</c>), a batch with no rows in it, and a registry that does not
/// implement the seam at all.
/// </remarks>
public sealed class NullPropagationTests
{
    private static readonly SparkFunctionRegistry Ansi = new();

    private static IArrowArray Strings(params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (var value in values)
        {
            if (value is null) builder.AppendNull(); else builder.Append(value);
        }

        return builder.Build();
    }

    private static IArrowArray Ints(params int?[] values)
    {
        var builder = new Int32Array.Builder();
        foreach (var value in values)
        {
            if (value is { } present) builder.Append(present); else builder.AppendNull();
        }

        return builder.Build();
    }

    private static RecordBatch Batch(bool nullable, params (string Name, IArrowArray Array)[] columns)
    {
        var schema = new Schema.Builder();
        foreach (var (name, array) in columns)
            schema.Field(new Field(name, array.Data.DataType, nullable));

        return new RecordBatch(schema.Build(), columns.Select(c => c.Array), columns[0].Array.Length);
    }

    private static BooleanArray Eval(string sql, RecordBatch batch, IFunctionRegistry? registry = null) =>
        new ArrowRowEvaluator(registry ?? Ansi)
            .EvaluatePredicate(SparkSqlParser.ParsePredicate(sql), batch);

    /// <summary>
    /// The fold, through each wrapper that makes an operand non-nullable. Every one of these
    /// raises without the <c>IS NULL</c> on top, which is what makes it a test rather than a
    /// tautology.
    /// </summary>
    [Theory]
    [InlineData("CASE WHEN CAST(s AS INT) > 1 THEN 1 ELSE 2 END IS NULL", false)]
    [InlineData("CASE WHEN CAST(s AS INT) > 1 THEN 1 ELSE 2 END IS NOT NULL", true)]
    [InlineData("if(CAST(s AS INT) > 1, 1, 2) IS NULL", false)]
    [InlineData("coalesce(CAST(s AS INT), 0) IS NOT NULL", true)]
    [InlineData("nvl2(CAST(s AS INT), 1, 2) IS NULL", false)]
    [InlineData("greatest(CAST(s AS INT), 1) IS NULL", false)]
    [InlineData("least(CAST(s AS INT), 1) IS NULL", false)]
    [InlineData("(CAST(s AS INT) IS NULL) IS NULL", false)]
    [InlineData("(CAST(s AS INT) <=> 1) IS NULL", false)]
    [InlineData("NOT (coalesce(CAST(s AS INT), 0) IS NULL)", true)]
    public void AnOperandThatCanNeverBeNullIsNotEvaluated(string sql, bool expected)
    {
        var batch = Batch(true, ("s", Strings("abc")));

        var result = Eval(sql, batch);

        Assert.Equal(expected, result.GetValue(0));
    }

    /// <summary>The other direction: Spark calls these nullable, so the operand is still read.</summary>
    /// <remarks>
    /// A set of cases that only asserted the folds would be satisfied by an implementation that
    /// folded everything — which answers <c>x IS NOT NULL</c> true for a genuine null and, in a
    /// CHECK constraint, admits a row Spark rejects. Same rule as the <c>short-circuit</c> group.
    /// </remarks>
    [Theory]
    [InlineData("CAST(s AS INT) IS NULL")]
    [InlineData("nullif(CAST(s AS INT), 0) IS NULL")]
    [InlineData("CASE WHEN CAST(s AS INT) > 1 THEN 1 END IS NULL")]
    [InlineData("if(CAST(s AS INT) > 1, 1, CAST(NULL AS INT)) IS NULL")]
    [InlineData("greatest(CAST(s AS INT), CAST(NULL AS INT)) IS NULL")]
    [InlineData("(CAST(s AS INT) = 1) IS NULL")]
    [InlineData("round(CAST(s AS DOUBLE), 0) IS NULL")]
    public void ANullableOperandIsStillEvaluatedAndStillRaises(string sql)
    {
        var batch = Batch(true, ("s", Strings("abc")));

        Assert.Throws<SparkEvaluationException>(() => Eval(sql, batch));
    }

    /// <summary>
    /// A COLUMN IS NULLABLE HERE WHATEVER ITS FIELD SAYS, and that is the load-bearing part.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark reads nullability from the TABLE's schema and folds <c>(a + b) IS NOT NULL</c> over
    /// two NOT NULL columns to a constant true, overflow and all. The only schema the evaluator
    /// can see is the batch the caller built — <c>DeltaConstraintEnforcer</c> is handed the
    /// caller's rows, not the snapshot's — so believing that flag would let a caller who marks a
    /// field non-nullable that the table declares nullable turn <c>x IS NOT NULL</c> into a
    /// constant true and write a null row past a constraint that forbids it.
    /// </para>
    /// <para>
    /// So the flag is not read at all. Both halves are asserted here: the overflow still raises
    /// (fail-closed, the divergence #319 declares), and an actually-null value under a
    /// non-nullable field still answers true for IS NULL rather than being folded away. The
    /// second is the one that would be a data defect.
    /// </para>
    /// </remarks>
    [Fact]
    public void ANonNullableColumnIsNotTrusted()
    {
        // `Ints(new int?[] { null })`, not `Ints(null)`: the latter binds null to the params
        // ARRAY rather than to its one element, and hands over no column at all.
        var claimsNotNull = Batch(
            false, ("a", Ints(int.MaxValue)), ("b", Ints(1)), ("n", Ints(new int?[] { null })));

        // Fail-closed: Spark answers true here by folding; we add, and the add overflows.
        Assert.Throws<SparkEvaluationException>(() => Eval("(a + b) IS NOT NULL", claimsNotNull));

        // Fail-open is what the flag would have cost: the column is declared non-nullable and
        // holds a null anyway, and the answer has to come from the value.
        Assert.True(Eval("n IS NULL", claimsNotNull).GetValue(0));
        Assert.False(Eval("n IS NOT NULL", claimsNotNull).GetValue(0));
        Assert.True(Eval("coalesce(n, 0) IS NOT NULL", claimsNotNull).GetValue(0));
    }

    /// <summary>
    /// The operand is still TYPE-checked, because Spark analyses it before the optimizer folds it.
    /// </summary>
    /// <remarks>
    /// Measured: <c>if(a &gt; 0, a, bin)</c> is refused outright by Spark for having no common
    /// type, and wrapping it in <c>IS NOT NULL</c> does not rescue it — the fold happens after
    /// analysis. "Do not evaluate it" must not become "do not look at it", which is the same rule
    /// #307 needed for an unreached branch.
    /// </remarks>
    [Theory]
    // Both results are non-null literals, so `NeverNull` says true and the fold IS taken -- which
    // is what makes this a test of the folded path. `X'00'` against an int has no common type.
    [InlineData("if(a > 0, 1, X'00') IS NOT NULL")]
    [InlineData("CASE WHEN a > 0 THEN 1 ELSE X'00' END IS NOT NULL")]
    // A non-boolean CONDITION, which Spark refuses at analysis under both dialects
    // (DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE). The type check has to reach it over no rows,
    // which is why the guard is on the condition ARRAY and not inside the per-row loop.
    [InlineData("if(1, 1, 2) IS NOT NULL")]
    [InlineData("CASE WHEN 1 THEN 1 ELSE 2 END IS NOT NULL")]
    public void AFoldedOperandIsStillTypeChecked(string sql)
    {
        var batch = Batch(true, ("a", Ints(1)));

        Assert.ThrowsAny<Exception>(() => Eval(sql, batch));
    }

    /// <summary>
    /// The fold is taken for these, so the refusals above are refusals of the FOLDED path.
    /// </summary>
    /// <remarks>
    /// Without this the theory above would pass just as well if the fold never fired and ordinary
    /// evaluation raised instead — which is exactly how its first version was wrong.
    /// </remarks>
    [Fact]
    public void TheTypeCheckedShapesReallyDoFold()
    {
        var batch = Batch(true, ("a", Ints(1)));

        Assert.True(Eval("if(a > 0, 1, CAST(s AS INT)) IS NOT NULL",
            Batch(true, ("a", Ints(1)), ("s", Strings("abc")))).GetValue(0));
        Assert.True(Eval("CASE WHEN a > 0 THEN 1 ELSE 2 END IS NOT NULL", batch).GetValue(0));
    }

    /// <summary>
    /// The fold answers per row, over batches of every size, and does not disturb a value that
    /// was already right.
    /// </summary>
    /// <remarks>
    /// Zero rows is the case that broke the conditional family in #307 — a literal materialised at
    /// the batch's length collapses there — and the fold types its operand through the same path.
    /// </remarks>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    public void TheFoldHoldsAtEveryBatchLength(int rows)
    {
        var values = new string?[rows];
        for (var i = 0; i < rows; i++) values[i] = i == 1 ? null : "abc";
        var batch = Batch(true, ("s", Strings(values)));

        var folded = Eval("coalesce(CAST(s AS INT), 0) IS NOT NULL", batch);
        var evaluated = Eval("s IS NOT NULL", batch);

        Assert.Equal(rows, folded.Length);
        for (var i = 0; i < rows; i++)
        {
            Assert.True(folded.GetValue(i));
            Assert.Equal(i != 1, evaluated.GetValue(i));
        }
    }

    /// <summary>
    /// The seam is optional: a registry that does not implement <see cref="INullabilityRules"/>
    /// folds nothing beyond what the evaluator can decide structurally.
    /// </summary>
    /// <remarks>
    /// A function's nullability is the registry's to answer, so without one every call is
    /// nullable — while a literal and a nested predicate, which the evaluator judges itself, still
    /// fold. That split is what keeps the rule out of the evaluator's own dialect-free core.
    /// </remarks>
    [Fact]
    public void ARegistryWithoutTheSeamFoldsOnlyWhatTheEvaluatorKnows()
    {
        var batch = Batch(true, ("s", Strings("abc")));
        var bare = new BareRegistry(Ansi);

        Assert.Throws<SparkEvaluationException>(
            () => Eval("coalesce(CAST(s AS INT), 0) IS NOT NULL", batch, bare));

        // Statically non-nullable operands, so these fail if structural folding is removed --
        // `s = 'abc'` would NOT, because `s` is a nullable column and the comparison is evaluated
        // either way.
        Assert.False(Eval("1 IS NULL", batch, bare).GetValue(0));
        Assert.True(Eval("(1 IS NULL) IS NOT NULL", batch, bare).GetValue(0));
        Assert.False(Eval("(CAST(s AS INT) <=> 1) IS NULL", batch, bare).GetValue(0));
    }

    /// <summary>A registry with the functions but none of the optional seams past invocation.</summary>
    private sealed class BareRegistry : IFunctionRegistry, IComparisonCoercion, IShortCircuitingFunctions
    {
        private readonly SparkFunctionRegistry _inner;

        public BareRegistry(SparkFunctionRegistry inner) => _inner = inner;

        public bool IsRegistered(string name) => _inner.IsRegistered(name);

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount) =>
            _inner.Invoke(name, args, rowCount);

        public bool ShortCircuits(string name) => _inner.ShortCircuits(name);

        public IArrowArray Invoke(string name, IConditionalArguments arguments, int rowCount) =>
            _inner.Invoke(name, arguments, rowCount);

        public IArrowType? ComparisonTarget(IArrowType operand, IArrowType other) =>
            _inner.ComparisonTarget(operand, other);

        public IArrowType? SetComparisonTarget(IReadOnlyList<IArrowType> memberTypes) =>
            _inner.SetComparisonTarget(memberTypes);

        public IArrowArray CastForComparison(IArrowArray operand, IArrowType target, int rowCount) =>
            _inner.CastForComparison(operand, target, rowCount);
    }
}
