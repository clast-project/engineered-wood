// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// What type an expression holding a bare <c>NULL</c> resolves to, against Spark's own answer.
/// </summary>
/// <remarks>
/// <para>
/// #293. A bare <c>NULL</c> is <c>void</c> in Spark, and EngineeredWood used to materialise one as
/// an all-null <see cref="StringArray"/> — a type that was a lie, and one nothing downstream could
/// see through, because a real string column holding nothing in this batch was the same array.
/// </para>
/// <para>
/// <b>This is a TYPE test because the corpus's evaluation test could not have caught it.</b>
/// <c>SparkEvaluationCorpusTests</c> compares VALUES, and every defect here answered null — the
/// right value at the wrong type. <c>coalesce(a, s)</c> came back <c>int</c> where Spark says
/// <c>bigint</c> holding the same 1, and that comparison passes. The one shape it did catch was
/// the arithmetic, and only because throwing is not an answer at all.
/// </para>
/// <para>
/// The corpus's own type checks (<c>SparkNumericTypesTests</c>) walk two groups and reach the
/// two rules directly, without evaluating. This walks the <c>null-literal</c> group through the
/// whole evaluator, which is where the placeholder lived.
/// </para>
/// </remarks>
public sealed class NullLiteralTypeTests
{
    private static readonly JsonDocument Corpus = JsonDocument.Parse(
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "spark-expression-corpus.json")));

    [Fact]
    public void EveryNullLiteralExpressionResolvesToSparksType() =>
        AssertGroupTypes(Corpus.RootElement.GetProperty("groups"), new SparkFunctionRegistry());

    /// <summary>
    /// The same group under the legacy dialect, which it was harvested twice for.
    /// </summary>
    /// <remarks>
    /// <c>void</c> ought to be dialect-independent — it carries no value to overflow and no text
    /// to parse — but #278 measured the conditional family coercing in OPPOSITE directions per
    /// dialect, so "ought to" is not a measurement. Both sections are asserted, and they agree.
    /// </remarks>
    [Fact]
    public void EveryNullLiteralExpressionResolvesToSparksTypeUnderTheLegacyDialect() =>
        AssertGroupTypes(
            Corpus.RootElement.GetProperty("legacy").GetProperty("groups"),
            new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false }));

    private static void AssertGroupTypes(JsonElement groups, SparkFunctionRegistry registry)
    {
        var batch = CorpusEvaluation.BuildBatch(
            Corpus.RootElement.GetProperty("schema"), Corpus.RootElement.GetProperty("rows"));
        var evaluator = new ArrowRowEvaluator(registry);

        var mismatches = new List<string>();
        var compared = 0;

        foreach (var entry in groups.GetProperty("null-literal").EnumerateArray())
        {
            var sql = entry.GetProperty("expression").GetString()!;
            var recorded = entry.GetProperty("type");

            // A row Spark REFUSES is checked by the evaluation test, which counts a throw as
            // agreement. There is no type to compare here.
            if (!recorded.GetProperty("ok").GetBoolean())
                continue;

            compared++;
            var expected = recorded.GetProperty("type").GetString();
            string actual;
            try
            {
                actual = SparkName(
                    evaluator.EvaluateExpression(SparkSqlParser.ParseExpression(sql), batch)
                        .Data.DataType);
            }
            catch (Exception ex)
            {
                actual = $"{ex.GetType().Name}: {ex.Message}";
            }

            if (actual != expected)
                mismatches.Add($"{sql}: spark says {expected}, we say {actual}");
        }

        Assert.Empty(mismatches);

        // The vacuity guard the other corpus tests carry: a group that vanished from the fixture
        // would otherwise make this pass by comparing nothing.
        Assert.True(compared > 45, $"only {compared} expressions were compared");
    }

    /// <summary>
    /// A frame whose string column holds nothing at all, which no corpus row can express.
    /// </summary>
    /// <remarks>
    /// The corpus is ONE batch, and every string column in it holds values. The half of #293 that
    /// mattered is what happens when one does not — so the batch is built here instead.
    /// </remarks>
    private static RecordBatch EmptyStringColumn()
    {
        var ints = new Int32Array.Builder();
        ints.Append(1);
        ints.AppendNull();

        var strings = new StringArray.Builder();
        strings.AppendNull();
        strings.AppendNull();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        return new RecordBatch(schema, new IArrowArray[] { ints.Build(), strings.Build() }, 2);
    }

    /// <summary>
    /// A type is a property of the SCHEMA, not of what the batch happens to hold.
    /// </summary>
    /// <remarks>
    /// The whole of #293 in one assertion. <c>coalesce(a, s)</c> is a <c>bigint</c> in Spark
    /// because <c>s</c> is a string column — ANSI widens the integral to bigint and moves the
    /// string into it — and it stays a bigint over a batch where <c>s</c> is null in every row.
    /// It used to come back an <c>int</c> there, because an all-null string column was exactly
    /// the array a bare <c>NULL</c> was materialised as, and a bare NULL is dropped from the fold.
    /// The corpus row above pins the bigint; this pins that the batch cannot move it.
    /// </remarks>
    [Fact]
    public void AStringColumnHoldingNothingStillTypesAConditional()
    {
        var batch = EmptyStringColumn();
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());

        Assert.Equal(
            "bigint",
            SparkName(evaluator.EvaluateExpression(
                SparkSqlParser.ParseExpression("coalesce(a, s)"), batch).Data.DataType));

        // ...where a bare NULL really does drop out, over the very same batch. The two used to be
        // the same array; nothing but the type tells them apart, and the type now does.
        Assert.Equal(
            "int",
            SparkName(evaluator.EvaluateExpression(
                SparkSqlParser.ParseExpression("coalesce(a, NULL)"), batch).Data.DataType));
    }

    /// <summary>
    /// <c>greatest</c> refuses a string against a number however empty the string column is.
    /// </summary>
    /// <remarks>
    /// The direction that matters most, because it is a SILENT wrong answer rather than a loud
    /// one. <c>greatest</c> and <c>least</c> refuse a string against a number in both dialects —
    /// DATATYPE_MISMATCH.DATA_DIFF_TYPES, and the corpus rows pin it — but they used to drop any
    /// argument that held no value in this batch, so over this frame they ANSWERED an int. A
    /// write Spark rejects outright was accepted, and which it did depended on the batch.
    /// </remarks>
    [Theory]
    [InlineData("greatest(a, s)")]
    [InlineData("least(a, s)")]
    [InlineData("greatest(s, a)")]
    public void GreatestRefusesAStringAgainstANumberEvenWhenTheColumnHoldsNothing(string sql)
    {
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());

        Assert.Throws<NotSupportedException>(
            () => evaluator.EvaluateExpression(SparkSqlParser.ParseExpression(sql), EmptyStringColumn()));
    }

    /// <summary>
    /// ...and it still drops a bare NULL inside a branch no row selects.
    /// </summary>
    /// <remarks>
    /// The opposite failure, and the one the old content test had to suppress to avoid: over NO
    /// rows every argument is all-null because none of them has a row to be anything else, so the
    /// test decided nothing and was skipped entirely — which left a bare NULL constraining the
    /// type it should not, and <c>greatest(a, NULL)</c> inside an unreached branch REFUSED.
    /// A <c>void</c> column is void at every row count, so one rule covers both.
    /// </remarks>
    [Fact]
    public void ABareNullDropsOutOfGreatestInsideABranchNoRowSelects()
    {
        var batch = EmptyStringColumn();
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());

        // Every row takes the then-branch, so the else-branch is typed over ZERO rows -- the
        // range the old content test was vacuous over and had to be suppressed for.
        var result = evaluator.EvaluateExpression(
            SparkSqlParser.ParseExpression("if(true, a, greatest(a, NULL))"), batch);

        Assert.Equal("int", SparkName(result.Data.DataType));
    }

    /// <summary>
    /// The eager entry point answers the same way, with no expression to consult.
    /// </summary>
    /// <remarks>
    /// <c>IFunctionRegistry.Invoke</c> is handed arrays that are already evaluated, so #279's
    /// structural test — ask the EXPRESSION which branch is a bare NULL — has nothing to ask.
    /// That is why it kept the content test after #279, and why it was still wrong: the same
    /// all-null string column, the same <c>int</c>. A <c>void</c> column carries the answer WITH
    /// it, so both entry points agree now.
    /// </remarks>
    [Fact]
    public void TheEagerRegistryEntryPointTellsAVoidColumnFromAnEmptyStringOne()
    {
        var registry = new SparkFunctionRegistry();
        var batch = EmptyStringColumn();
        var ints = batch.Column(0);
        var strings = batch.Column(1);

        Assert.Equal(
            "bigint",
            SparkName(registry.Invoke("coalesce", new[] { ints, strings }, 2).Data.DataType));

        Assert.Equal(
            "int",
            SparkName(registry.Invoke(
                "coalesce", new IArrowArray[] { ints, new NullArray(2) }, 2).Data.DataType));

        Assert.Throws<NotSupportedException>(
            () => registry.Invoke("greatest", new[] { ints, strings }, 2));

        Assert.Equal(
            "int",
            SparkName(registry.Invoke(
                "greatest", new IArrowArray[] { ints, new NullArray(2) }, 2).Data.DataType));
    }

    /// <summary>
    /// <c>round</c> reads the column's type rather than its contents, for the same reason.
    /// </summary>
    /// <remarks>
    /// <c>round(NULL, 2)</c> is a <c>double</c> in Spark, and <c>round</c> used to reach that by
    /// asking whether every row was null. That test was DEAD — the placeholder was spelled as a
    /// string, so the string branch cast it to a double first and nothing ever reached it — and
    /// it would have been wrong the moment it was not: a real string column holding nothing takes
    /// the string cast, which is a different path with a dialect rule of its own.
    /// </remarks>
    [Fact]
    public void RoundOverAnEmptyStringColumnTakesTheStringCastAndNotTheVoidShortcut()
    {
        var batch = EmptyStringColumn();

        Assert.Equal(
            "double",
            SparkName(new ArrowRowEvaluator(new SparkFunctionRegistry())
                .EvaluateExpression(SparkSqlParser.ParseExpression("round(s, 2)"), batch)
                .Data.DataType));

        // The legacy dialect answers null where ANSI raises on a malformed string, and only the
        // cast knows that. Reaching the void shortcut instead would answer under both.
        Assert.Throws<SparkEvaluationException>(
            () => new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluateExpression(
                SparkSqlParser.ParseExpression("round('abc', 2)"), batch));
    }

    /// <summary>An Arrow type spelled the way Spark's <c>dataType.simpleString</c> spells it.</summary>
    private static string SparkName(IArrowType type) => type switch
    {
        // The answer the whole issue is about: Arrow's NullType IS Spark's void.
        NullType => "void",
        Int8Type => "tinyint",
        Int16Type => "smallint",
        Int32Type => "int",
        Int64Type => "bigint",
        FloatType => "float",
        DoubleType => "double",
        StringType => "string",
        BinaryType => "binary",
        BooleanType => "boolean",
        Date32Type or Date64Type => "date",
        TimestampType => "timestamp",
        Decimal128Type d => $"decimal({d.Precision},{d.Scale})",
        Decimal256Type d => $"decimal({d.Precision},{d.Scale})",
        _ => type.Name,
    };
}
