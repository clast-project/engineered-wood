// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The seam behind #286 — a registry that knows which expressions its dialect's analyzer refuses
/// on type grounds.
/// </summary>
/// <remarks>
/// <para>
/// WHAT Spark refuses is not here and cannot be: no registry implements
/// <see cref="IAnalysisRules"/> yet, and the rule tables land with the slices that adopt them
/// (the comparison families with #333, the cast table with #332). These tests cover the seam
/// itself — that a registry without it is unaffected, that a diagnostic reaches the caller as a
/// refusal, and that the questions asked are the ones the interface says are asked.
/// </para>
/// <para>
/// The fake registry refuses by TYPE rather than by family, deliberately: a double that
/// reimplemented Spark's rule would pass whether or not the seam carried the answer through.
/// </para>
/// </remarks>
public sealed class AnalysisRulesTests
{
    private static readonly SparkFunctionRegistry Spark = new();

    /// <summary>A registry that answers function calls as Spark's does and analyses as told.</summary>
    private sealed class Analyzing : IFunctionRegistry, IAnalysisRules
    {
        private readonly Func<ComparisonOperator, IArrowType, IArrowType, AnalysisDiagnostic?> _rule;

        public Analyzing(Func<ComparisonOperator, IArrowType, IArrowType, AnalysisDiagnostic?> rule)
            => _rule = rule;

        /// <summary>Every pair the evaluator asked about, in the order it asked.</summary>
        public List<(ComparisonOperator Op, string Left, string Right)> Asked { get; } = new();

        public bool IsRegistered(string name) => Spark.IsRegistered(name);

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount)
            => Spark.Invoke(name, args, rowCount);

        public AnalysisDiagnostic? CheckComparison(
            ComparisonOperator op, IArrowType left, IArrowType right)
        {
            Asked.Add((op, left.Name, right.Name));
            return _rule(op, left, right);
        }

        public AnalysisDiagnostic? CheckCast(IArrowType source, IArrowType target, bool tryCast)
            => null;

        /// <summary>Every list the evaluator asked about, in the order it asked.</summary>
        public List<string> AskedSets { get; } = new();

        public AnalysisDiagnostic? CheckSetComparison(IReadOnlyList<IArrowType> memberTypes)
        {
            AskedSets.Add(string.Join(",", memberTypes.Select(t => t.Name)));

            // The same shape the pair rule uses, applied to a list: an int and a boolean in one
            // set is the refusal, whatever else is in it.
            return memberTypes.Any(t => t is Int32Type) && memberTypes.Any(t => t is BooleanType)
                ? DiffTypes
                : null;
        }
    }

    private static readonly AnalysisDiagnostic DiffTypes =
        new("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES", "the two operands differ in type");

    /// <summary>Refuses an int against a boolean, whichever way round and whatever the operator.</summary>
    private static Analyzing RefusingIntAgainstBoolean() =>
        new((_, left, right) =>
            (left is Int32Type && right is BooleanType) || (left is BooleanType && right is Int32Type)
                ? DiffTypes
                : null);

    private static IArrowArray Ints(params int?[] values)
    {
        var builder = new Int32Array.Builder();
        foreach (var value in values)
        {
            if (value is { } present) builder.Append(present); else builder.AppendNull();
        }

        return builder.Build();
    }

    private static IArrowArray Booleans(params bool?[] values)
    {
        var builder = new BooleanArray.Builder();
        foreach (var value in values)
        {
            if (value is { } present) builder.Append(present); else builder.AppendNull();
        }

        return builder.Build();
    }

    private static IArrowArray Strings(params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (var value in values)
        {
            if (value is null) builder.AppendNull(); else builder.Append(value);
        }

        return builder.Build();
    }

    private static RecordBatch Batch(params (string Name, IArrowArray Array)[] columns)
    {
        var schema = new Schema.Builder();
        foreach (var (name, array) in columns)
            schema.Field(new Field(name, array.Data.DataType, nullable: true));

        return new RecordBatch(schema.Build(), columns.Select(c => c.Array), columns[0].Array.Length);
    }

    /// <summary>The corpus frame, narrowed to the columns these tests compare.</summary>
    private static RecordBatch Frame() => Batch(
        ("a", Ints(1, null, -2147483648)),
        ("bl", Booleans(true, null, false)));

    private static BooleanArray Eval(string sql, IFunctionRegistry? registry, RecordBatch? batch = null) =>
        new ArrowRowEvaluator(registry)
            .EvaluatePredicate(SparkSqlParser.ParsePredicate(sql), batch ?? Frame());

    private static bool?[] Rows(BooleanArray array) =>
        Enumerable.Range(0, array.Length).Select(array.GetValue).ToArray();

    /// <summary>
    /// Without a registry that implements the seam, a cross-family comparison answers null per row
    /// exactly as it did before the seam existed.
    /// </summary>
    /// <remarks>
    /// The default has to be silence: <c>DeltaTable</c>, <c>LanceTable</c> and
    /// <c>LanceDatasetWriter</c> all construct an evaluator with no registry at all, and none of
    /// them wants Spark's analyzer.
    /// </remarks>
    [Fact]
    public void ARegistryWithoutTheSeamRefusesNothing() =>
        Assert.Equal(new bool?[] { null, null, null }, Rows(Eval("a = bl", registry: null)));

    /// <summary>
    /// <see cref="SparkFunctionRegistry"/> implements it, and the two dialects answer DIFFERENTLY
    /// — which is the whole shape of #286 and #333 in one expression.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ANSI refuses an int against a boolean at analysis, so we refuse it too rather than handing
    /// back a column of nulls. The legacy dialect coerces the boolean to the number and answers,
    /// which is #333 — and the two rules have to agree, since a check that refused what the
    /// coercion goes on to cast would turn those answers straight back into refusals.
    /// </para>
    /// <para>
    /// This replaces the placeholder that pinned "the seam changes nothing yet". That it changed
    /// is the point.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheSparkRegistryRefusesUnderAnsiAndAnswersUnderLegacy()
    {
        var refusal = Assert.Throws<ExpressionAnalysisException>(() => Eval("a = bl", Spark));
        Assert.Equal("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES", refusal.ErrorClass);
        Assert.Contains("\"INT\" and \"BOOLEAN\"", refusal.Message);

        var legacy = new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false });
        Assert.Equal(new bool?[] { true, null, false }, Rows(Eval("a = bl", legacy)));

        // ...and ordering stays refused in BOTH dialects, which is what says the legacy exception
        // is equality's alone.
        Assert.Throws<ExpressionAnalysisException>(() => Eval("a < bl", legacy));
    }

    /// <summary>A diagnostic reaches the caller as a refusal carrying the class it named.</summary>
    [Fact]
    public void ADiagnosticBecomesARefusal()
    {
        var registry = RefusingIntAgainstBoolean();

        var refusal = Assert.Throws<ExpressionAnalysisException>(() => Eval("a = bl", registry));

        Assert.Equal("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES", refusal.ErrorClass);
        Assert.Contains("DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES", refusal.Message);
    }

    /// <summary>
    /// The refusal is a property of the TYPES, so it happens over a batch with no rows in it.
    /// </summary>
    /// <remarks>
    /// The definition-time premise in miniature: nothing was read to reach the answer, so the same
    /// rule can answer for an expression no data has been seen for at all. It is also what
    /// separates this from a value refusal — <c>CAST('abc' AS INT)</c> over an empty batch raises
    /// nothing, because there is no 'abc' to read.
    /// </remarks>
    [Fact]
    public void ARefusalNeedsNoRows()
    {
        var empty = Batch(("a", Ints()), ("bl", Booleans()));

        Assert.Throws<ExpressionAnalysisException>(
            () => Eval("a = bl", RefusingIntAgainstBoolean(), empty));
    }

    /// <summary>
    /// The operator reaches the rule, so a pair can be refused for ordering and accepted for
    /// equality.
    /// </summary>
    /// <remarks>
    /// Spark's shape, and the reason <see cref="IAnalysisRules.CheckComparison"/> is asked with
    /// the operator at all: under the legacy dialect <c>a = bl</c> answers through
    /// <c>BooleanEquality</c> while <c>a &lt; bl</c> is refused in both dialects. #333.
    /// </remarks>
    [Fact]
    public void TheOperatorReachesTheRule()
    {
        var registry = new Analyzing((op, left, right) =>
            left is Int32Type && right is BooleanType && op != ComparisonOperator.Equal
                ? DiffTypes
                : null);

        Assert.Throws<ExpressionAnalysisException>(() => Eval("a < bl", registry));
        Assert.Equal(new bool?[] { null, null, null }, Rows(Eval("a = bl", registry)));
    }

    /// <summary>
    /// A non-boolean in predicate position and <c>IS TRUE</c> arrive as comparisons against a
    /// boolean, so the comparison rule covers boolean context with no rule of its own.
    /// </summary>
    /// <remarks>
    /// <c>SparkSqlParser.AsPredicate</c> lowers the first to <c>expr = TRUE</c> and the parser
    /// lowers the second to <c>expr &lt;=&gt; TRUE</c>. The measurement for #286 says the same
    /// thing from the other end: a family check at the comparison site dropped the ANSI gap from
    /// 171 rows to the 17 of the cast table, taking the 28 boolean-context rows with it.
    /// </remarks>
    [Theory]
    [InlineData("a IS TRUE")]
    [InlineData("a IS NOT TRUE")]
    [InlineData("a AND bl")]
    [InlineData("NOT a")]
    public void BooleanContextIsAComparisonAgainstABoolean(string sql)
    {
        Assert.Throws<ExpressionAnalysisException>(() => Eval(sql, RefusingIntAgainstBoolean()));
    }

    /// <summary>
    /// A set test asks ONE question over the whole list, and never the pair question.
    /// </summary>
    /// <remarks>
    /// <para>
    /// An earlier version of this seam asked per member with <c>Equal</c>, which looked right and
    /// is not: <c>IN</c> resolves a single type over the operand and every member, so it can
    /// refuse a pair an equality accepts. Measured on 4.0.3, <c>a = bl</c> ANSWERS under the
    /// legacy dialect — Spark's <c>BooleanEquality</c>, #333 — while <c>a IN (bl)</c> is refused
    /// under BOTH dialects. Asking the pair question here would have made the legacy set answer.
    /// </para>
    /// <para>
    /// The operand comes first and the members follow, which is the order Spark prints them in.
    /// </para>
    /// </remarks>
    [Fact]
    public void ASetTestAsksOneQuestionOverTheWholeList()
    {
        Assert.Throws<ExpressionAnalysisException>(
            () => Eval("a IN (bl)", RefusingIntAgainstBoolean()));

        var accepting = RefusingIntAgainstBoolean();
        Assert.Equal(new bool?[] { true, null, false }, Rows(Eval("a IN (1, 2)", accepting)));

        Assert.Equal(new[] { "int32,int32,int32" }, accepting.AskedSets);
        Assert.Empty(accepting.Asked);
    }

    /// <summary>A bare <c>NULL</c> member is left out of the list rather than typed.</summary>
    /// <remarks>
    /// Spark types one <c>void</c>, which constrains the resolution no more than it constrains a
    /// comparison — the same rule <c>CoerceSet</c> already applies when it resolves the set's
    /// cast target, and the same structural test: read from the TREE, not from a column that came
    /// back all null.
    /// </remarks>
    [Fact]
    public void ABareNullMemberIsLeftOutOfTheList()
    {
        var accepting = RefusingIntAgainstBoolean();

        Eval("a IN (1, NULL)", accepting);

        Assert.Equal(new[] { "int32,int32" }, accepting.AskedSets);
    }

    /// <summary>
    /// An operand with no type to read — a bare <c>NULL</c> — is not asked about at all.
    /// </summary>
    /// <remarks>
    /// Spark types one <c>void</c> and compares it with anything, so asking would refuse what
    /// Spark accepts. The trap this pins is <see cref="ArrowRowEvaluator"/>'s other type resolver,
    /// which substitutes a string for an operand it cannot read: through that one this would ask
    /// about an int against a string and, under a real family rule, refuse <c>a = NULL</c>.
    /// </remarks>
    [Fact]
    public void AnUntypedNullOperandIsNotAsked()
    {
        var registry = new Analyzing((_, _, _) => DiffTypes);   // refuses everything it is asked

        Assert.Equal(new bool?[] { null, null, null }, Rows(Eval("a = NULL", registry)));
        Assert.Empty(registry.Asked);
    }

    /// <summary>
    /// A pair of literals is refused over an empty batch, where neither operand has a value to
    /// read a type from.
    /// </summary>
    /// <remarks>
    /// The companion to <see cref="ARefusalNeedsNoRows"/>, which uses columns and so reads both
    /// types from the schema. A literal carries its type in the TREE, and reading it from a value
    /// instead leaves an expression made only of literals unanalysed exactly when there is no
    /// data — which is the case a definition-time pass consists of.
    /// </remarks>
    [Theory]
    [InlineData("1 = TRUE")]
    [InlineData("1 IN (TRUE)")]
    public void ALiteralPairIsRefusedOverAnEmptyBatch(string sql)
    {
        Assert.Throws<ExpressionAnalysisException>(
            () => Eval(sql, RefusingIntAgainstBoolean(), Batch(("a", Ints()))));
    }

    /// <summary>
    /// An operand that RAISES does not mask the refusal: the types are read without reading a
    /// value, so the analysis answer is reached first.
    /// </summary>
    /// <remarks>
    /// The soundness property the whole seam exists for, and #286's own complaint about the cast
    /// path: an answer that depends on whether a value happened to raise first is an analysis
    /// decision made by the data. Spark analyses the tree before any of it runs, and reports the
    /// type refusal for both of these.
    /// </remarks>
    [Theory]
    [InlineData("CAST(s AS INT) = bl")]
    [InlineData("a IN (bl, CAST(s AS INT))")]
    public void AValueFailureDoesNotMaskTheRefusal(string sql)
    {
        var batch = Batch(
            ("a", Ints(1, 2)),
            ("bl", Booleans(true, false)),
            ("s", Strings("abc", "def")));

        Assert.Throws<ExpressionAnalysisException>(
            () => Eval(sql, RefusingIntAgainstBoolean(), batch));
    }

    /// <summary>
    /// A refusal reached by a zero-row type probe is not retried over the batch, where a value
    /// would raise in its place.
    /// </summary>
    /// <remarks>
    /// <c>TypeOver</c> answers "what type would this have produced" by evaluating over no rows and
    /// falling back to the whole batch when that cannot answer. The fallback is right for a
    /// registry that cannot type something over an empty selection, and wrong for an analysis
    /// refusal: the refusal is the answer, it will not change over more rows, and retrying turns
    /// it into whatever the first value raises. Measured here as CAST_INVALID_INPUT displacing
    /// the type refusal.
    /// </remarks>
    [Fact]
    public void AZeroRowProbeKeepsItsRefusal()
    {
        var batch = Batch(
            ("bl", Booleans(true, false)),
            ("s", Strings("abc", "def")));

        Assert.Throws<ExpressionAnalysisException>(
            () => Eval("false AND CAST(s AS INT) = bl", RefusingIntAgainstBoolean(), batch));
    }

    /// <summary>The rules are asked once per comparison, not once per row.</summary>
    /// <remarks>
    /// Cheap to assert and worth asserting: an analyzer question inside the row loop would be both
    /// slower and wrong in the way #332 describes, where a refusal that depends on a value makes
    /// the same expression refuse or answer depending on the batch.
    /// </remarks>
    [Fact]
    public void TheRuleIsAskedOncePerComparisonRegardlessOfRowCount()
    {
        var registry = new Analyzing((_, _, _) => null);

        Eval("a = 1 AND a = 2", registry, Batch(("a", Ints(1, 2, 3, 4, 5, 6, 7, 8))));

        Assert.Equal(2, registry.Asked.Count);
    }
}
