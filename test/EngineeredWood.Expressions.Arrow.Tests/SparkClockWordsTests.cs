// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// #801: <see cref="SparkClockWords.Resolve(Expression, DateTimeOffset)"/> turns each clock word of a cast into the
/// value it names at one instant, which every later evaluation then reads, however many batches there are.
/// </summary>
public class SparkClockWordsTests
{
    private static readonly RecordBatch OneRow = new(new Apache.Arrow.Schema.Builder().Build(), [], 1);

    private static readonly Func<ArrowRowEvaluator>[] Evaluators =
    [
        () => new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = true })),
        () => new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false })),
    ];

    // Instants with something in every field, including the sub-microsecond tick a TIMESTAMP drops, either side
    // of midnight UTC, and a leap day.
    public static IEnumerable<object[]> Instants() =>
    [
        [new DateTimeOffset(2026, 11, 11, 19, 31, 12, TimeSpan.Zero).AddTicks(1_234_567)],
        [new DateTimeOffset(2026, 12, 31, 23, 59, 59, TimeSpan.Zero).AddTicks(9_999_999)],
        [new DateTimeOffset(2028, 2, 29, 0, 0, 0, TimeSpan.Zero)],
        [new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(5)],

        // Before the epoch, with sub-microsecond ticks: a TIMESTAMP truncates toward the epoch there, not toward the
        // past (#828 review).
        [new DateTimeOffset(1969, 12, 31, 23, 59, 59, TimeSpan.Zero).AddTicks(9_999_999)],
        [new DateTimeOffset(1900, 1, 1, 12, 0, 0, TimeSpan.Zero).AddTicks(5)],
    ];

    /// <summary>
    /// Each word, in each spelling of the cast, to each target, resolves to text the cast reads as exactly the value
    /// the fold names at that instant (the fold's own conversion, SparkSpecialDatetimeValues, at the same instant).
    /// </summary>
    [Theory]
    [MemberData(nameof(Instants))]
    public void AResolvedCastNamesTheValueTheFoldNamesAtThatInstant(DateTimeOffset now)
    {
        foreach (var word in new[] { "now", "today", "yesterday", "tomorrow", " Today ", "TOMORROW" })
        {
            foreach (var cast in new[] { "CAST", "TRY_CAST" })
            {
                foreach (var target in new[] { "DATE", "TIMESTAMP" })
                {
                    var original = SparkSqlParser.ParseExpression($"{cast}('{word}' AS {target})");
                    var resolved = SparkClockWords.Resolve(original, now);
                    Assert.NotSame(original, resolved);

                    bool read = target == "DATE"
                        ? SparkSpecialDatetimeValues.TryReadDate(word.AsSpan(), now, out var expected)
                        : SparkSpecialDatetimeValues.TryReadTimestamp(word.AsSpan(), now, out expected);
                    Assert.True(read, word);

                    foreach (var make in Evaluators)
                    {
                        var value = make().EvaluateExpression(resolved, OneRow);
                        if (target == "DATE")
                        {
                            var date = Assert.IsType<Date32Array>(value);
                            Assert.Equal(expected.Date, date.GetDateTimeOffset(0)!.Value.Date);
                        }
                        else
                        {
                            var stamp = Assert.IsType<TimestampArray>(value);
                            // A TIMESTAMP holds microseconds: the instant truncated to one.
                            long micros = (expected.UtcTicks - new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks) / 10;
                            Assert.Equal(micros, stamp.GetValue(0));
                        }
                    }
                }
            }
        }
    }

    /// <summary>Every word of one expression is read at the one instant, wherever it sits.</summary>
    [Fact]
    public void EveryWordOfAnExpressionIsResolvedAtTheOneInstant()
    {
        var now = new DateTimeOffset(2026, 11, 11, 19, 31, 12, TimeSpan.Zero);
        var predicate = SparkSqlParser.ParsePredicate(
            "(ts < CAST('now' AS TIMESTAMP) AND NOT (d = CAST('today' AS DATE))) "
            + "OR d IN (CAST('yesterday' AS DATE), TRY_CAST('tomorrow' AS DATE)) OR CAST('now' AS TIMESTAMP) IS NULL");

        var resolved = SparkClockWords.Resolve(predicate, now);
        string text = resolved.ToString()!;

        // A literal renders unquoted: cast(now, TIMESTAMP) before, cast(2026-11-11 ..., TIMESTAMP) after.
        foreach (var word in new[] { "(now,", "(today,", "(yesterday,", "(tomorrow," })
            Assert.DoesNotContain(word, text, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, CountOf(text, "cast(2026-11-11 19:31:12.000000Z, TIMESTAMP)"));
        Assert.Equal(1, CountOf(text, "cast(2026-11-11, DATE)"));
        Assert.Equal(1, CountOf(text, "cast(2026-11-10, DATE)"));
        Assert.Equal(1, CountOf(text, "try_cast(2026-11-12, DATE)"));
    }

    private static int CountOf(string text, string part)
    {
        int count = 0;
        for (int at = text.IndexOf(part, StringComparison.Ordinal); at >= 0; at = text.IndexOf(part, at + 1, StringComparison.Ordinal))
            count++;
        return count;
    }

    /// <summary>
    /// What resolves is a clock word in a DATE or TIMESTAMP cast of a literal. Everything else is left as it is, and an
    /// expression with nothing to resolve comes back as the same instance.
    /// </summary>
    [Theory]
    [InlineData("ts < TIMESTAMP'2026-01-01 00:00:00'")]
    [InlineData("d = CAST('epoch' AS DATE)")]                     // no clock in it
    [InlineData("d = CAST('2026-11-11' AS DATE)")]                // not a word
    [InlineData("s = CAST('now' AS STRING)")]                     // not a date or timestamp
    [InlineData("i = CAST('now' AS INT)")]
    [InlineData("ts = CAST(concat('n', 'ow') AS TIMESTAMP)")]     // foldable, not a literal: the fold's, as before
    [InlineData("ts = CAST(s AS TIMESTAMP)")]
    [InlineData("d = CAST('someday' AS DATE)")]                   // not in the vocabulary: the cast's to refuse
    public void AnythingElseIsLeftAsItIs(string sql)
    {
        var predicate = SparkSqlParser.ParsePredicate(sql);
        Assert.Same(predicate, SparkClockWords.Resolve(predicate, DateTimeOffset.UtcNow));
    }

    /// <summary>
    /// The point of #801: a resolved predicate answers the same however long after, and over however many batches, it
    /// is evaluated -- the clock is not read again.
    /// </summary>
    [Fact]
    public void AResolvedPredicateIsTheSameAtEveryEvaluation()
    {
        var now = new DateTimeOffset(2026, 11, 11, 12, 0, 0, TimeSpan.Zero);
        var resolved = SparkClockWords.Resolve(
            SparkSqlParser.ParsePredicate("ts < CAST('now' AS TIMESTAMP)"), now);
        var stamps = new TimestampArray.Builder(new Apache.Arrow.Types.TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, "UTC"))
            .Append(now.AddSeconds(-1)).Append(now).Append(now.AddSeconds(1)).Build();
        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("ts", stamps.Data.DataType, true)).Build(),
            [stamps], 3);

        foreach (var make in Evaluators)
        {
            var mask = make().EvaluatePredicate(resolved, batch);
            Assert.Equal([true, false, false], Enumerable.Range(0, 3).Select(i => mask.GetValue(i)!.Value));
        }
    }
}
