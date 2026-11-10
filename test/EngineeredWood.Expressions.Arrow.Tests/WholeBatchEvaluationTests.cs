// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// <see cref="ArrowRowEvaluator.TryEvaluateWithoutRaising"/> (#502 phase 5a) answers exactly what
/// <see cref="ArrowRowEvaluator.EvaluatePredicate"/> answers, or declines. It never raises an error a value causes,
/// since its answers are asked for rows the expression itself would never have evaluated.
/// </summary>
public class WholeBatchEvaluationTests
{
    private static string Show(BooleanArray mask) =>
        string.Join(",", Enumerable.Range(0, mask.Length)
            .Select(i => mask.GetValue(i) is { } v ? (v ? "T" : "F") : "n")) + $" ({mask.NullCount} null)";

    // The general path's outcome: its answers, or the exception it raised.
    private static (string Text, Exception? Error) General(ArrowRowEvaluator evaluator, Predicate predicate, RecordBatch batch)
    {
        try
        {
            return (Show(evaluator.EvaluatePredicate(predicate, batch)), null);
        }
        catch (Exception e)
        {
            return ($"{e.GetType().Name}: {e.Message}", e);
        }
    }

    /// <summary>
    /// Compares one case: an answer must equal the general path's; an analysis error may surface either way, as it
    /// does over any rows; any other error must not surface at all. Returns whether the whole-batch path answered.
    /// </summary>
    private static bool Check(
        Func<ArrowRowEvaluator> make, string name, Predicate predicate, RecordBatch batch, List<string> mismatches)
    {
        var (expected, error) = General(make(), predicate, batch);
        BooleanArray? whole;
        try
        {
            whole = make().TryEvaluateWithoutRaising(predicate, batch);
        }
        catch (ExpressionAnalysisException e) when (error is ExpressionAnalysisException)
        {
            if (e.Message != error.Message)
                mismatches.Add($"{name}: raised '{e.Message}', the general path '{error.Message}'");
            return false;
        }
        catch (Exception e)
        {
            mismatches.Add($"{name}: raised {e.GetType().Name}: {e.Message} (general: {expected})");
            return false;
        }

        if (whole is null)
            return false;
        if (error is not null)
            mismatches.Add($"{name}: answered {Show(whole)} where the general path raised {expected}");
        else if (Show(whole) != expected)
            mismatches.Add($"{name}\n  general: {expected}\n  whole:   {Show(whole)}");
        return true;
    }

    [Fact]
    public void ComparisonsAnswerAsTheGeneralPathOrDecline()
    {
        var batch = DirectColumnComparisonTests.Batch();
        var mismatches = new List<string>();
        int cases = 0, answered = 0;
        foreach (var (name, predicate) in DirectColumnComparisonTests.Cases(batch))
        {
            for (int e = 0; e < DirectColumnComparisonTests.Evaluators.Length; e++)
            {
                cases++;
                if (Check(DirectColumnComparisonTests.Evaluators[e],
                        $"[{DirectColumnComparisonTests.EvaluatorNames[e]}] {name}", predicate, batch, mismatches))
                {
                    answered++;
                }
            }
        }

        Assert.True(mismatches.Count == 0,
            $"{mismatches.Count} of {cases} differ:\n" + string.Join("\n", mismatches.Take(25)));
        Assert.True(answered > cases / 6, $"answered {answered} of {cases}");
    }

    [Theory]
    [InlineData("i32 > 0 AND i32 < 5", true)]
    [InlineData("i32 > 0 AND s = 'abc'", true)]
    [InlineData("i32 > 0 OR s = 'abc'", true)]
    [InlineData("NOT (i64 < 0) AND u8 <= 127 AND f32 = 1.5", true)]
    [InlineData("s IS NULL OR i32 > 0", true)]
    [InlineData("NOT (s IS NOT NULL AND i32 <> 1)", true)]
    [InlineData("b = true OR b IS NULL", true)]
    // Each of these holds a form that can raise on a value: declined, whatever the rest.
    [InlineData("i32 <> 0 AND 1 / i32 > 0", false)]
    [InlineData("s = 'abc' AND CAST(s AS INT) > 0", false)]
    [InlineData("i32 > 0 AND s IN ('a', 'b')", false)]
    [InlineData("lower(s) = 'ab'", false)]
    [InlineData("i32 + 1 > 0", false)]
    [InlineData("i32 > i64", false)]
    [InlineData("(i32 + 1) IS NULL", false)]
    public void CompoundsAnswerAsTheGeneralPathOrDecline(string sql, bool answers)
    {
        var batch = DirectColumnComparisonTests.Batch();
        var predicate = SparkSqlParser.ParsePredicate(sql);
        var mismatches = new List<string>();
        foreach (var make in DirectColumnComparisonTests.Evaluators)
        {
            bool answered = Check(make, sql, predicate, batch, mismatches);
            Assert.True(answered == answers, $"{sql}: answered {answered}");
        }

        Assert.True(mismatches.Count == 0, string.Join("\n", mismatches));
    }

    [Fact]
    public void AStringColumnAgainstANumberUnderAnsiDeclinesWithoutRaising()
    {
        // Under ANSI the column is cast to the number, which raises on 'abc': the direct path declines, and so must
        // this, without reading a value.
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("s", Apache.Arrow.Types.StringType.Default, true)], null),
            [new StringArray.Builder().AppendRange(["1", "abc", "7"]).Build()], 3);
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = true }));
        var predicate = SparkSqlParser.ParsePredicate("s = 5");
        Assert.ThrowsAny<Exception>(() => evaluator.EvaluatePredicate(predicate, batch));
        Assert.Null(evaluator.TryEvaluateWithoutRaising(predicate, batch));
    }

    /// <summary>
    /// Review round 1: a form the direct path always declines is not one <see cref="ArrowRowEvaluator.MayEvaluateWithoutRaising"/>
    /// admits, so a caller never prepares a batch only to be declined.
    /// </summary>
    [Theory]
    [InlineData("i32 > 0", true)]
    [InlineData("0 < i32", true)]
    [InlineData("d32 >= DATE'2026-10-01'", true)]
    [InlineData("s = CAST('5' AS STRING)", true)]
    [InlineData("i32 > i64", false)]
    [InlineData("i32 > CAST(s AS INT)", false)]
    [InlineData("i32 + 1 > 0", false)]
    [InlineData("1 > 0", false)]
    [InlineData("i32 > 0 AND i32 > i64", false)]
    [InlineData("s IS NULL OR i32 = 1", true)]
    public void OnlyFormsTheDirectPathCanAnswerAreAdmitted(string sql, bool admitted) =>
        Assert.Equal(admitted, ArrowRowEvaluator.MayEvaluateWithoutRaising(SparkSqlParser.ParsePredicate(sql)));

    /// <summary>The direct path reads the column as the value, never the prefix: <c>'ab' STARTSWITH s</c> declines.</summary>
    [Fact]
    public void AColumnAsThePrefixIsNotAdmitted()
    {
        Assert.False(ArrowRowEvaluator.MayEvaluateWithoutRaising(new ComparisonPredicate(
            Expressions.Lit(LiteralValue.Of("ab")), ComparisonOperator.StartsWith, Expressions.Ref("s"))));
        Assert.True(ArrowRowEvaluator.MayEvaluateWithoutRaising(new ComparisonPredicate(
            Expressions.Ref("s"), ComparisonOperator.StartsWith, Expressions.Lit(LiteralValue.Of("ab")))));
    }

    /// <summary>
    /// Review round 2: over an empty batch, an error no value causes (an unresolvable column, a pair of types with no
    /// comparison) still raises exactly as the general path raises it; it is not hidden behind a decline.
    /// </summary>
    [Theory]
    [InlineData("i32 > 0")]                 // no such column
    [InlineData("i32 IS NULL")]             // no such column
    [InlineData("j > 1 AND i32 > 0")]       // no such column, in a later operand
    [InlineData("j > X'61'")]               // no comparison between INT and BINARY
    [InlineData("j > 'abc'")]               // valid: answered, with no rows
    public void AnEmptyBatchRaisesWhatTheGeneralPathRaises(string sql)
    {
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("j", Apache.Arrow.Types.Int32Type.Default, true)], null),
            [new Int32Array.Builder().Build()], 0);
        var predicate = SparkSqlParser.ParsePredicate(sql);
        foreach (var make in DirectColumnComparisonTests.Evaluators)
        {
            var (expected, error) = General(make(), predicate, batch);
            if (error is not null)
            {
                var raised = Assert.ThrowsAny<Exception>(() => make().TryEvaluateWithoutRaising(predicate, batch));
                Assert.Equal(expected, $"{raised.GetType().Name}: {raised.Message}");
            }
            else
            {
                var whole = make().TryEvaluateWithoutRaising(predicate, batch);
                Assert.NotNull(whole);
                Assert.Equal(expected, Show(whole!));
            }
        }
    }

    /// <summary>
    /// Review round 3: a decline is not a verdict, so an invalid predicate may be declined without being checked. What
    /// the contract does promise is that a caller doing what the reader does (asking <see cref="ArrowRowEvaluator.EvaluatePredicate"/>
    /// on a decline) raises exactly what <see cref="ArrowRowEvaluator.EvaluatePredicate"/> alone raises. Copilot's two
    /// examples are here: an IN with no common type (a form this does not cover) and a missing column in a covered
    /// comparison, which the direct kernel declines.
    /// </summary>
    [Theory]
    [InlineData("missing > 0")]
    [InlineData("i32 > 0 AND missing IS NULL")]
    [InlineData("i32 IN (X'61')")]
    [InlineData("1 IN (TRUE)")]
    [InlineData("i32 > X'61'")]
    [InlineData("b = 'abc'")]
    public void ADeclineThenEvaluatePredicateRaisesWhatEvaluatePredicateRaises(string sql)
    {
        var batch = DirectColumnComparisonTests.Batch();
        var predicate = SparkSqlParser.ParsePredicate(sql);
        int raised = 0;
        for (int e = 0; e < DirectColumnComparisonTests.Evaluators.Length; e++)
        {
            var make = DirectColumnComparisonTests.Evaluators[e];
            string name = $"[{DirectColumnComparisonTests.EvaluatorNames[e]}] {sql}";
            var (expected, error) = General(make(), predicate, batch);
            string actual;
            try
            {
                var evaluator = make();
                actual = Show(evaluator.TryEvaluateWithoutRaising(predicate, batch) ?? evaluator.EvaluatePredicate(predicate, batch));
            }
            catch (Exception ex)
            {
                actual = $"{ex.GetType().Name}: {ex.Message}";
            }

            Assert.True(expected == actual, $"{name}\n  alone:    {expected}\n  declined: {actual}");
            if (error is not null)
                raised++;
        }

        // Not vacuous: the general path refuses each of these under at least one evaluator.
        Assert.True(raised > 0, $"{sql}: no evaluator refused it");
    }

    [Fact]
    public void AnEmptyBatchIsAnsweredEmpty()
    {
        var batch = DirectColumnComparisonTests.Batch();
        var empty = new RecordBatch(batch.Schema, batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, 0)), 0);
        var answer = new ArrowRowEvaluator().TryEvaluateWithoutRaising(SparkSqlParser.ParsePredicate("i32 > 0"), empty);
        Assert.NotNull(answer);
        Assert.Equal(0, answer!.Length);
    }
}
