// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The machinery behind #279 and #306 — evaluating a branch over only the rows that select it.
/// </summary>
/// <remarks>
/// What Spark ANSWERS is pinned by the corpus's <c>short-circuit</c> group, which is the gate;
/// this covers the seams that group cannot reach from a three-row batch of its fixed schema. The
/// gather path in particular is invisible from the outside: an expression evaluated over a subset
/// of rows has to come back indexed by the original row numbers, with the types and the exact bits
/// a whole-batch evaluation would have produced.
/// </remarks>
public sealed class ShortCircuitEvaluationTests
{
    private static readonly SparkFunctionRegistry Ansi = new();

    private static IArrowArray Ints(params int?[] values)
    {
        var builder = new Int32Array.Builder();
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
            schema.Field(new Field(name, array.Data.DataType, true));

        return new RecordBatch(schema.Build(), columns.Select(c => c.Array), columns[0].Array.Length);
    }

    private static IArrowArray Eval(string sql, RecordBatch batch) =>
        new ArrowRowEvaluator(Ansi).EvaluateExpression(SparkSqlParser.ParseExpression(sql), batch);

    /// <summary>
    /// The gather path: some rows take the branch and some do not, so it is evaluated over a
    /// SHORT batch and its answers have to land back on the right rows.
    /// </summary>
    /// <remarks>
    /// The corpus reaches this too, but only ever with one row selected. Here the selection is
    /// interleaved — rows 1 and 3 of four — which is what would break an implementation that
    /// scattered by position rather than by row number, and which still reads as a pass if the
    /// answers happen to be symmetric.
    /// </remarks>
    [Fact]
    public void AMixedBatchEvaluatesTheBranchOverItsOwnRowsAndPutsThemBack()
    {
        var batch = Batch(
            ("a", Ints(10, null, 30, null)),
            ("s", Strings("bad", "7", "bad", "9")));

        var result = (Int64Array)Eval("coalesce(a, CAST(s AS BIGINT))", batch);

        // Rows 0 and 2 never reach the cast, so 'bad' is never converted; rows 1 and 3 do.
        Assert.Equal(10, result.GetValue(0));
        Assert.Equal(7, result.GetValue(1));
        Assert.Equal(30, result.GetValue(2));
        Assert.Equal(9, result.GetValue(3));
    }

    /// <summary>A branch gathered and put back keeps its exact type, width and value.</summary>
    /// <remarks>
    /// A decimal is the case that matters: it is the type the evaluator elsewhere cannot carry
    /// through its own <c>LiteralValue</c> round trip, and one wider than <see cref="decimal"/>
    /// is the one a rebuild through a typed builder would silently narrow.
    /// </remarks>
    [Fact]
    public void AGatheredBranchKeepsItsDecimalTypeAndValue()
    {
        var wide = new Decimal128Type(38, 10);
        var decimals = new Decimal128Array.Builder(wide);
        decimals.Append(1.5m).Append(2.5m).Append(3.5m);

        var batch = Batch(("a", Ints(null, 20, null)), ("d", decimals.Build()));

        var result = Eval("coalesce(CAST(a AS DECIMAL(38,10)), d)", batch);

        var type = Assert.IsType<Decimal128Type>(result.Data.DataType);
        Assert.Equal(wide.Precision, type.Precision);
        Assert.Equal(wide.Scale, type.Scale);
        var values = (Decimal128Array)result;
        Assert.Equal(1.5m, values.GetValue(0));
        Assert.Equal(20m, values.GetValue(1));
        Assert.Equal(3.5m, values.GetValue(2));
    }

    /// <summary>
    /// A branch no row selects is still evaluated — over no rows — so it still types the result.
    /// </summary>
    /// <remarks>
    /// Measured: <c>coalesce(f, 'x')</c> is a <c>double</c> in Spark. Dropping an unreached branch
    /// would leave this a <c>float</c>, and a float widened later is a different VALUE, not just a
    /// different label — which is why this asserts the type rather than only the answer.
    /// </remarks>
    [Fact]
    public void AnUnreachedBranchStillTypesTheResult()
    {
        var floats = new FloatArray.Builder();
        floats.Append(1.1f).Append(2.2f);

        var result = Eval("coalesce(f, 'x')", Batch(("f", floats.Build())));

        Assert.Equal(DoubleType.Default, result.Data.DataType);
    }

    /// <summary>...and is still type-CHECKED, so a pair with no common type refuses.</summary>
    [Fact]
    public void AnUnreachedBranchIsStillTypeChecked()
    {
        var binary = new BinaryArray.Builder();
        binary.Append(new byte[] { 1 });

        var batch = Batch(("a", Ints(1)), ("bin", binary.Build()));

        Assert.Throws<NotSupportedException>(() => Eval("if(1 = 1, a, bin)", batch));
    }

    /// <summary>
    /// A bare NULL branch is <c>void</c> and constrains nothing; a TYPED null still constrains.
    /// </summary>
    /// <remarks>
    /// The pair is the discriminator #293 is about, and the reason the answer is read off the
    /// expression rather than off the column: by the time they are arrays both are all-null.
    /// Measured — <c>coalesce(a, NULL)</c> is an <c>int</c> and
    /// <c>if(1 = 1, a, CAST(NULL AS STRING))</c> a <c>bigint</c>.
    /// </remarks>
    [Fact]
    public void ABareNullDoesNotConstrainTheTypeAndATypedOneDoes()
    {
        var batch = Batch(("a", Ints(1, 2)));

        Assert.Equal(Int32Type.Default, Eval("coalesce(a, NULL)", batch).Data.DataType);
        Assert.Equal(Int64Type.Default, Eval("if(1 = 1, a, CAST(NULL AS STRING))", batch).Data.DataType);
    }

    /// <summary>
    /// A string column holding nothing but nulls in this batch is not a bare NULL, however alike
    /// they look.
    /// </summary>
    /// <remarks>
    /// The batch-dependent half of #293, and the one a content test gets wrong: Spark types this
    /// from the SCHEMA, so it is a <c>bigint</c> whatever the column happens to hold.
    /// </remarks>
    [Fact]
    public void AnAllNullStringColumnStillConstrainsTheType()
    {
        var batch = Batch(("a", Ints(1, 2)), ("s", Strings(null, null)));

        Assert.Equal(Int64Type.Default, Eval("coalesce(a, s)", batch).Data.DataType);
    }

    /// <summary>
    /// Restricting a batch to a row selection keeps BOTH halves of an ambiguous name, so the
    /// reference is still refused.
    /// </summary>
    /// <remarks>
    /// The restricted batch carries only the columns the branch names, and resolving a name to
    /// pick them would have quietly answered what <see cref="ArrowRowEvaluator"/> refuses
    /// everywhere else (#181) — and only on the batches that happened to take the gather path,
    /// which is the worst shape a bug can have.
    /// </remarks>
    [Fact]
    public void AnAmbiguousReferenceInsideAGatheredBranchIsStillAmbiguous()
    {
        var batch = Batch(
            ("a", Ints(1, null, 3)),
            ("dup", Ints(7, 7, 7)),
            ("DUP", Ints(8, 8, 8)));

        var error = Assert.Throws<ArgumentException>(() => Eval("coalesce(a, dup)", batch));
        Assert.Contains("dup", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A column the branch does not name is neither gathered nor required to be gatherable.
    /// </summary>
    /// <remarks>
    /// Restricting the whole batch would have made an untouched column decide whether a branch can
    /// be evaluated at all. A union is the shape that makes the point: <c>ArrowCompute</c> declines
    /// to gather one, and no expression here reads it.
    /// </remarks>
    [Fact]
    public void AColumnTheBranchDoesNotNameIsNotGathered()
    {
        var children = new IArrowArray[] { Ints(1, 2, 3) };
        var typeIds = new ArrowBuffer.Builder<byte>();
        typeIds.Append(0).Append(0).Append(0);
        var union = new SparseUnionArray(
            new UnionType([new Field("i", Int32Type.Default, true)], [0], UnionMode.Sparse),
            length: 3, children, typeIds.Build(), nullCount: 0);

        var batch = Batch(("a", Ints(10, null, 30)), ("s", Strings("bad", "7", "bad")), ("u", union));

        var result = (Int64Array)Eval("coalesce(a, CAST(s AS BIGINT))", batch);

        Assert.Equal(10, result.GetValue(0));
        Assert.Equal(7, result.GetValue(1));
        Assert.Equal(30, result.GetValue(2));
    }

    /// <summary>
    /// An empty batch still answers with an empty column, despite literals being built one row
    /// long inside the evaluator.
    /// </summary>
    /// <remarks>
    /// The extra row exists so that a zero-row evaluation can still read a cast's target type out
    /// of row 0 — see <c>ConstantArray</c>. It must not escape: a caller is owed an answer of the
    /// batch's length, and a generated column one row longer than the batch it belongs to would be
    /// a silent corruption rather than a failure.
    /// </remarks>
    [Fact]
    public void AnEmptyBatchAnswersWithAnEmptyColumn()
    {
        var batch = new RecordBatch(
            new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Build(),
            new[] { Ints() },
            0);

        Assert.Equal(0, Eval("1", batch).Length);
        Assert.Equal(0, Eval("CAST('0x10' AS DOUBLE)", batch).Length);
        Assert.Equal(0, Eval("coalesce(a, 1)", batch).Length);
        Assert.Equal(DoubleType.Default, Eval("CAST('0x10' AS DOUBLE)", batch).Data.DataType);
    }

    /// <summary>
    /// A branch whose result type depends on an argument's VALUE is still typed, even though no
    /// row selects it.
    /// </summary>
    /// <remarks>
    /// <c>round</c>'s scale decides the result's precision and scale and is read out of row 0, so
    /// this branch cannot be typed over zero rows the way every other one can — the scale comes
    /// back as an empty array and `round` throws instead of answering. A literal scale hides it,
    /// because a literal is built one row long on purpose; <c>1 + 1</c> is not a literal by the
    /// time the registry sees it. Measured: Spark types this <c>decimal(12,2)</c>.
    /// </remarks>
    [Fact]
    public void ABranchTypedFromAnArgumentsValueIsStillTyped()
    {
        var decimals = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        decimals.Append(1.25m).Append(2.25m);

        var batch = Batch(("a", Ints(1, 2)), ("d1", decimals.Build()));

        foreach (var sql in new[] { "coalesce(a, round(d1, 1 + 1))", "if(1 = 1, a, round(d1, 1 + 1))" })
        {
            var type = Assert.IsType<Decimal128Type>(Eval(sql, batch).Data.DataType);
            Assert.Equal(12, type.Precision);
            Assert.Equal(2, type.Scale);
        }
    }

    /// <summary>
    /// AND stops at a FALSE and OR at a TRUE, per row — and neither stops at a null.
    /// </summary>
    /// <remarks>
    /// The corpus pins this against Spark; asserted here per row because the corpus's null row
    /// makes the two halves of the rule hard to read side by side. <c>z</c> is zero on the row
    /// where the guard decides, so an operand evaluated there raises.
    /// </remarks>
    [Fact]
    public void AndStopsAtFalseAndOrAtTrue()
    {
        var batch = Batch(("z", Ints(0, 1, 2)), ("n", Ints(null, null, null)));

        var and = (BooleanArray)Eval("z <> 0 AND 1/z > 0", batch);
        Assert.False(and.GetValue(0));
        Assert.True(and.GetValue(1));
        Assert.True(and.GetValue(2));

        var or = (BooleanArray)Eval("z = 0 OR 1/z > 0", batch);
        Assert.True(or.GetValue(0));
        Assert.True(or.GetValue(1));
        Assert.True(or.GetValue(2));

        // A null left operand does NOT decide the row, so the right one is evaluated there and
        // raises on the zero. Measured against Spark, and the half an "unknown counts as decided"
        // mask would get wrong.
        Assert.Throws<SparkEvaluationException>(() => Eval("n > 0 AND 1/z > 0", batch));
        Assert.Throws<SparkEvaluationException>(() => Eval("n > 0 OR 1/z > 0", batch));
    }

    /// <summary>
    /// An operand every row has already decided is still evaluated, over no rows, so one that
    /// cannot be evaluated at all still says so.
    /// </summary>
    [Fact]
    public void AnOperandNoRowNeedsIsStillRejectedIfItCannotBeEvaluated()
    {
        var batch = Batch(("z", Ints(1, 2, 3)));

        Assert.Throws<ArgumentException>(() => Eval("z = 0 AND nosuchcolumn > 0", batch));
    }

    /// <summary>
    /// Invoking the registry directly, with the arguments already evaluated, still answers.
    /// </summary>
    /// <remarks>
    /// One implementation of the conditional family serves both entry points. This is the eager
    /// one, which a caller holding columns rather than expressions reaches — it cannot
    /// short-circuit, and it must still choose and unify the same way.
    /// </remarks>
    [Fact]
    public void TheEagerRegistryEntryPointStillWorks()
    {
        var args = new[] { Ints(1, null, 3), Ints(9, 9, 9) };

        var result = (Int32Array)Ansi.Invoke("coalesce", args, 3);

        Assert.Equal(1, result.GetValue(0));
        Assert.Equal(9, result.GetValue(1));
        Assert.Equal(3, result.GetValue(2));
    }

    /// <summary>Only the conditional family evaluates its own arguments.</summary>
    [Fact]
    public void TheShortCircuitingFamilyIsTheConditionalOne()
    {
        foreach (var name in new[] { "coalesce", "nvl", "ifnull", "if", "case" })
            Assert.True(Ansi.ShortCircuits(name), name);

        // Measured eager in Spark, all of them.
        foreach (var name in new[] { "nullif", "greatest", "least", "concat", "cast", "round" })
            Assert.False(Ansi.ShortCircuits(name), name);
    }
}
