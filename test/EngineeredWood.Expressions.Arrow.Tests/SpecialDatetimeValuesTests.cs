// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// The five words Spark reads as a date or a timestamp, and the constant-only rule that reads
/// them.
/// </summary>
/// <remarks>
/// <para>
/// #342. Spark's <c>SpecialDatetimeValues</c> rewrites <c>Cast(e, DateType)</c> into a literal
/// when <c>e</c> is <c>foldable</c> and holds one of <c>epoch</c>, <c>today</c>,
/// <c>yesterday</c>, <c>tomorrow</c>, <c>now</c>. The grammar underneath it knows none of them,
/// so the same word arriving in a ROW is refused — and that is the whole shape of the issue, so
/// every discriminator below is asserted in both directions rather than only on the accept side.
/// </para>
/// <para>
/// <b>Only <c>epoch</c> reaches the corpus.</b> Four of the five are a function of the clock, so
/// the fixture can pin the word <c>epoch</c> and the SHAPES around the others
/// (<c>yesterday &lt; today</c>) and nothing else; what today's date actually is belongs here,
/// where it can be recomputed rather than recorded. Every expectation in this file was measured
/// against Spark 4.0.3 / JDK 17.0.20 on 2026-09-17, session zone UTC, in both dialects.
/// </para>
/// </remarks>
public class SpecialDatetimeValuesTests
{
    private static readonly SparkFunctionRegistry Ansi = new();

    private static readonly SparkFunctionRegistry Legacy =
        new(new SparkDialectOptions { Ansi = false });

    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>UTC midnight of the day <paramref name="instant"/> falls in, shifted by days.</summary>
    private static DateTimeOffset Day(DateTimeOffset instant, int offsetDays = 0) =>
        new(instant.UtcDateTime.Date.AddDays(offsetDays), TimeSpan.Zero);

    /// <summary>
    /// Asserts that <paramref name="read"/> answers today shifted by
    /// <paramref name="offsetDays"/>, plus <paramref name="timeOfDay"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The clock is sampled ACROSS the call, not beside it.</b> Four of the five words are a
    /// function of the clock, so an expected value read from a second `UtcNow` is a different
    /// reading from the one the call made — and once a day, for however long the call takes,
    /// those two readings name different days. Bracketing the call and accepting either day is
    /// exact: the call happened between the two samples, so whichever day it saw is one of them.
    /// </para>
    /// <para>
    /// <b>Bracketing rather than a frozen clock</b>, which is what a review asked for. Freezing
    /// means a clock seam, and it would have to reach <c>SparkTemporalText</c> — which reads
    /// <c>UtcNow</c> for the time-alone form and has no options object to carry one, that being
    /// the constraint #341 recorded. It also would not help the one assertion that compares two
    /// INDEPENDENT clock readings against each other, which is what
    /// <see cref="TheLiteralAndTheCastAgree"/> is for. Two samples cost nothing and are sound
    /// for both.
    /// </para>
    /// </remarks>
    private static void AssertNamesDay(
        Func<DateTimeOffset> read, int offsetDays = 0, TimeSpan timeOfDay = default)
    {
        var before = DateTimeOffset.UtcNow;
        var answer = read();
        var after = DateTimeOffset.UtcNow;

        Assert.Contains(
            answer,
            new[] { Day(before, offsetDays) + timeOfDay, Day(after, offsetDays) + timeOfDay });
    }

    /// <summary>One row carrying an int to branch on and a string column holding <c>'epoch'</c>.</summary>
    /// <remarks>
    /// The string column is the discriminator: it holds the very word the fold reads, so a rule
    /// that looked at the VALUE rather than at the tree would pass every accept-side case here
    /// and still be wrong.
    /// </remarks>
    private static RecordBatch Row()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int32Type.Default, true))
            .Field(new Field("s", StringType.Default, true))
            .Build();

        return new RecordBatch(schema, new IArrowArray[]
        {
            new Int32Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("epoch").Build(),
        }, 1);
    }

    private static IArrowArray Evaluate(SparkFunctionRegistry registry, string expression) =>
        new ArrowRowEvaluator(registry)
            .EvaluateExpression(SparkSqlParser.ParseExpression(expression), Row());

    private static DateTimeOffset Date(SparkFunctionRegistry registry, string expression)
    {
        var dates = Assert.IsType<Date32Array>(Evaluate(registry, expression));
        return Assert.NotNull(dates.GetDateTimeOffset(0));
    }

    private static DateTimeOffset Instant(SparkFunctionRegistry registry, string expression)
    {
        var timestamps = Assert.IsType<TimestampArray>(Evaluate(registry, expression));
        return timestamps.GetTimestamp(0)!.Value;
    }

    // ── The vocabulary ──

    /// <summary>The five words as DATES, where <c>now</c> is today rather than an instant.</summary>
    /// <remarks>
    /// <c>convertSpecialDate</c> maps <c>now</c> and <c>today</c> to the same day — a date has no
    /// time to carry, so the current instant has nowhere to go. Measured: <c>CAST('now' AS
    /// DATE)</c> and <c>CAST('today' AS DATE)</c> were the same day, and so was
    /// <c>CAST(CAST('now' AS TIMESTAMP) AS DATE)</c>.
    /// </remarks>
    [Fact]
    public void EveryWordReadsAsADate()
    {
        foreach (var registry in new[] { Ansi, Legacy })
        {
            var r = registry;

            Assert.Equal(Epoch, Date(r, "CAST('epoch' AS DATE)"));
            AssertNamesDay(() => Date(r, "CAST('today' AS DATE)"));
            AssertNamesDay(() => Date(r, "CAST('now' AS DATE)"));
            AssertNamesDay(() => Date(r, "CAST('yesterday' AS DATE)"), -1);
            AssertNamesDay(() => Date(r, "CAST('tomorrow' AS DATE)"), 1);
        }
    }

    /// <summary>...and as TIMESTAMPS, where <c>now</c> IS the instant.</summary>
    /// <remarks>
    /// The one word the two conversions disagree about. <c>now</c> is bounded rather than
    /// compared, because it is read from the clock inside the call.
    /// </remarks>
    [Fact]
    public void EveryWordReadsAsATimestamp()
    {
        foreach (var registry in new[] { Ansi, Legacy })
        {
            var r = registry;

            Assert.Equal(Epoch, Instant(r, "CAST('epoch' AS TIMESTAMP)"));
            AssertNamesDay(() => Instant(r, "CAST('today' AS TIMESTAMP)"));
            AssertNamesDay(() => Instant(r, "CAST('yesterday' AS TIMESTAMP)"), -1);
            AssertNamesDay(() => Instant(r, "CAST('tomorrow' AS TIMESTAMP)"), 1);

            // Bounded rather than compared, because the clock is read inside the call. The low
            // bound gives back one MICROSECOND: the folded instant is materialised into a
            // microsecond timestamp -- Spark's unit, and this library's -- so the sub-microsecond
            // ticks .NET hands out are truncated away, and a `before` taken at full resolution
            // can sit a few ticks above the answer. Caught on net472, where the coarser
            // UtcNow granularity made `before` and `after` the same instant.
            var before = DateTimeOffset.UtcNow.AddTicks(-TimeSpan.TicksPerMillisecond / 1000);
            var now = Instant(registry, "CAST('now' AS TIMESTAMP)");
            Assert.InRange(now, before, DateTimeOffset.UtcNow);
        }
    }

    /// <summary>The words are matched case-insensitively, over text trimmed as a cast trims it.</summary>
    /// <remarks>
    /// The trim is Java's <c>String.trim</c> — every character at or below U+0020 — which is the
    /// set <c>SparkText.TrimBounds</c> already removes and not <c>string.Trim</c>'s. So a leading
    /// U+00A0 is not trimmed and the word is not found, which is the same asymmetry #316 records
    /// for every other cast.
    /// </remarks>
    [Theory]
    [InlineData("epoch")]
    [InlineData("EPOCH")]
    [InlineData("Epoch")]
    [InlineData("  epoch  ")]
    [InlineData("\tepoch\t")]
    [InlineData("\nepoch")]
    [InlineData("\u001fepoch")]
    public void TheVocabularyIsCaseInsensitiveAndTrimmed(string text)
    {
        Assert.Equal(Epoch, Date(Ansi, $"CAST('{text}' AS DATE)"));
    }

    /// <summary>A leading U+00A0 is not trimmed, so the word is not there to find.</summary>
    [Fact]
    public void ANonBreakingSpaceIsNotTrimmedAndHidesTheWord()
    {
        Assert.Throws<SparkEvaluationException>(
            () => Evaluate(Ansi, "CAST('\u00a0epoch' AS DATE)"));
    }

    // ── Constant, not literal: the rule's actual shape ──

    /// <summary>
    /// Any CONSTANT string folds, not only a literal one.
    /// </summary>
    /// <remarks>
    /// Spark's rule is <c>e.foldable</c> and it calls <c>e.eval()</c> itself, so it does not wait
    /// for constant folding to have run — measured, <c>CAST(concat('epo','ch') AS DATE)</c>,
    /// <c>CAST(upper('EPOCH') AS DATE)</c> and <c>CAST(substring('epochal',1,5) AS DATE)</c> all
    /// answer 1970-01-01. A rule that matched a string LITERAL as written would miss all three.
    /// </remarks>
    [Theory]
    [InlineData("CAST(concat('epo','ch') AS DATE)")]
    [InlineData("CAST(upper('epoch') AS DATE)")]
    [InlineData("CAST(lower('EPOCH') AS DATE)")]
    [InlineData("CAST(substring('epochal', 1, 5) AS DATE)")]
    [InlineData("CAST(trim(' epoch ') AS DATE)")]
    [InlineData("CAST(coalesce(NULL, 'epoch') AS DATE)")]
    [InlineData("CAST(CASE WHEN 1 > 0 THEN 'epoch' ELSE 'x' END AS DATE)")]
    public void AConstantThatIsNotALiteralFoldsToo(string sql)
    {
        foreach (var registry in new[] { Ansi, Legacy })
            Assert.Equal(Epoch, Date(registry, sql));
    }

    /// <summary>
    /// THE DISCRIMINATOR: a string that is <c>'epoch'</c> in every row does NOT fold, because it
    /// reads a column.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the half of the issue that says the rule is about the TREE. Measured on 4.0.3,
    /// with <c>s</c> holding <c>'epoch'</c>: <c>CAST(s AS DATE)</c>,
    /// <c>CAST(concat(s,'') AS DATE)</c> and
    /// <c>CAST(CASE WHEN a &gt; 0 THEN 'epoch' ELSE 'epoch' END AS DATE)</c> are all
    /// <c>CAST_INVALID_INPUT</c> under ANSI and null without it — the last of them a constant in
    /// every row but one branch of an expression that reads <c>a</c>.
    /// </para>
    /// <para>
    /// A fold keyed on the VALUES would answer 1970-01-01 for all three, and would then answer
    /// differently for the same CHECK constraint over the next batch, which is worse than being
    /// wrong once.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("CAST(s AS DATE)")]
    [InlineData("CAST(concat(s, '') AS DATE)")]
    [InlineData("CAST(upper(s) AS DATE)")]
    [InlineData("CAST(CASE WHEN a > 0 THEN 'epoch' ELSE 'epoch' END AS DATE)")]
    [InlineData("CAST(coalesce(s, 'epoch') AS DATE)")]
    public void AColumnHoldingTheWordDoesNotFold(string sql)
    {
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, sql));

        var dates = Assert.IsType<Date32Array>(Evaluate(Legacy, sql));
        Assert.True(dates.IsNull(0));
    }

    /// <summary>
    /// <c>try_cast</c> folds as well, which the name argues against.
    /// </summary>
    /// <remarks>
    /// Spark's <c>try_cast</c> is a <c>Cast</c> with a different eval mode rather than a node of
    /// its own, so the optimizer rule matches it — measured, <c>try_cast('epoch' AS DATE)</c> is
    /// 1970-01-01 and only <c>try_cast(s AS DATE)</c> is the null it would otherwise always be.
    /// </remarks>
    [Fact]
    public void TryCastFoldsTheWordAndStillNullsAColumn()
    {
        foreach (var registry in new[] { Ansi, Legacy })
        {
            Assert.Equal(Epoch, Date(registry, "try_cast('epoch' AS DATE)"));
            Assert.True(Assert.IsType<Date32Array>(
                Evaluate(registry, "try_cast('someday' AS DATE)")).IsNull(0));
            Assert.True(Assert.IsType<Date32Array>(
                Evaluate(registry, "try_cast(s AS DATE)")).IsNull(0));
        }
    }

    // ── extractSpecialValue's own edges ──

    /// <summary>
    /// The tail after the word is a TIMEZONE, and it has to resolve for the word to count.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Spark reads <c>(\p{Alpha}+)\p{Blank}*(.*)</c> and hands the tail to <c>getZoneId</c>. So
    /// <c>'epoch UTC'</c> is a date and <c>'epoch extra'</c> is not — the difference is whether
    /// the trailing word names a zone, which nothing about the shape suggests. The zone is never
    /// USED: the value is still the epoch, and <c>'today +05:00'</c> is still today in the
    /// session zone.
    /// </para>
    /// <para>
    /// <c>'epochUTC'</c> is refused because the alpha run is greedy and the match must cover the
    /// string: the word becomes <c>epochutc</c> rather than backtracking to <c>epoch</c>.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData("epoch UTC", true)]
    [InlineData("epoch\tUTC", true)]
    [InlineData("epoch  UTC", true)]
    [InlineData("epoch GMT+02:00", true)]
    [InlineData("epoch +02:00", true)]
    [InlineData("epoch Z", true)]
    [InlineData("epoch extra", false)]
    [InlineData("epochUTC", false)]
    [InlineData("epoch UTC extra", false)]
    public void TheTailAfterTheWordMustResolveAsATimezone(string text, bool folds)
    {
        if (folds)
        {
            Assert.Equal(Epoch, Date(Ansi, $"CAST('{text}' AS DATE)"));
            return;
        }

        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, $"CAST('{text}' AS DATE)"));
    }

    /// <summary>
    /// <c>now</c> alone refuses a timezone, even one that resolves.
    /// </summary>
    /// <remarks>
    /// <c>isValid</c> has a case for it by name, and nothing else in the vocabulary carries it.
    /// Measured: <c>CAST('now UTC' AS DATE)</c> is <c>CAST_INVALID_INPUT</c> while
    /// <c>CAST('today UTC' AS DATE)</c> is today.
    /// </remarks>
    [Fact]
    public void NowRefusesATimezoneWhereTheOtherWordsAcceptOne()
    {
        AssertNamesDay(() => Date(Ansi, "CAST('today UTC' AS DATE)"));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "CAST('now UTC' AS DATE)"));
    }

    /// <summary>Text too short, or not led by a letter, never reaches the vocabulary.</summary>
    /// <remarks>
    /// <c>input.length &lt; 3 || !input(0).isLetter</c> is a guard above the regex. The bound
    /// matters because <c>now</c> is exactly three characters, so two is the shortest refusal.
    /// </remarks>
    [Theory]
    [InlineData("ep")]
    [InlineData("no")]
    [InlineData("epochs")]
    [InlineData("someday")]
    [InlineData("tod")]
    public void AWordOutsideTheVocabularyIsStillRefused(string text)
    {
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, $"CAST('{text}' AS DATE)"));
    }

    /// <summary>
    /// A line terminator past the word fails the match, although the zone would have resolved.
    /// </summary>
    /// <remarks>
    /// Java's <c>.</c> does not match one without <c>DOTALL</c>, so the regex never matches and
    /// <c>isValid</c> is never asked. A newline at either END is a different matter — the text is
    /// trimmed first, so <c>'\nepoch'</c> is the epoch.
    /// </remarks>
    [Fact]
    public void ALineTerminatorPastTheWordFailsTheMatch()
    {
        // Asked of the rule directly. A newline inside a SQL string literal would have to
        // survive the tokenizer to get here, and it is the READER that is being pinned.
        Assert.True(SparkSpecialDatetimeValues.TryReadDate("today UTC".AsSpan(), out _));
        Assert.False(SparkSpecialDatetimeValues.TryReadDate("today\nUTC".AsSpan(), out _));
        Assert.False(SparkSpecialDatetimeValues.TryReadDate("today\rUTC".AsSpan(), out _));
        Assert.False(SparkSpecialDatetimeValues.TryReadDate("today\u2028UTC".AsSpan(), out _));

        // ...while a line terminator at either END is trimmed away before the match runs.
        Assert.True(SparkSpecialDatetimeValues.TryReadDate("\nepoch\r\n".AsSpan(), out _));
    }

    // ── The typed literal, which is the parser's half ──

    /// <summary>
    /// A typed literal reads the words too, and refuses at PARSE time when it cannot.
    /// </summary>
    /// <remarks>
    /// Spark's <c>AstBuilder.visitTypeConstructor</c> tries <c>convertSpecialDate</c> before
    /// <c>stringToDate</c>, so <c>DATE'epoch'</c> is a literal rather than a cast — and a word
    /// the conversion refuses falls through to the grammar, which refuses it as
    /// <c>INVALID_TYPED_LITERAL</c>. Measured: <c>DATE'now UTC'</c> is a parse error in both
    /// dialects, where <c>CAST('now UTC' AS DATE)</c> is an evaluation-time refusal.
    /// </remarks>
    [Fact]
    public void ATypedLiteralReadsTheWordsAndRefusesAtParseTime()
    {
        Assert.Equal(Epoch, Date(Ansi, "DATE'epoch'"));
        Assert.Equal(Epoch, Date(Ansi, "DATE'  EPOCH  '"));
        AssertNamesDay(() => Date(Ansi, "DATE'today'"));
        AssertNamesDay(() => Date(Ansi, "DATE'today UTC'"));
        Assert.Equal(Epoch, Instant(Ansi, "TIMESTAMP'epoch'"));

        Assert.Throws<SparkSqlParseException>(
            () => SparkSqlParser.ParseExpression("DATE'now UTC'"));
        Assert.Throws<SparkSqlParseException>(
            () => SparkSqlParser.ParseExpression("DATE'someday'"));
    }

    /// <summary>
    /// The literal and the cast name the same day, which is what makes them one rule.
    /// </summary>
    /// <remarks>
    /// Both read the clock, so this can only be asked as an equality between them — and that is
    /// exactly the shape the corpus pins for <c>today</c>, since neither side can be written down.
    /// </remarks>
    [Fact]
    public void TheLiteralAndTheCastAgree()
    {
        AssertAgree("DATE'today'", "CAST('today' AS DATE)", 0);
        AssertAgree("DATE'yesterday'", "CAST('yesterday' AS DATE)", -1);
    }

    /// <summary>
    /// Two expressions that each read the clock name the same day.
    /// </summary>
    /// <remarks>
    /// The pair is bracketed rather than each half, because the claim is about the two AGREEING
    /// and equality is what a midnight between them would break. Both halves are held to the
    /// bracket unconditionally -- neither may name a day the clock did not show -- and equality
    /// is asserted on the runs where the bracket did not cross a day, which is every run but at
    /// most one a year. The other arm is not a let-off: across midnight the two readings must be
    /// the two ADJACENT days, in that order, which is a statement about them the loose form
    /// would not make.
    /// </remarks>
    private static void AssertAgree(string left, string right, int offsetDays)
    {
        var before = DateTimeOffset.UtcNow;
        var first = Date(Ansi, left);
        var second = Date(Ansi, right);
        var after = DateTimeOffset.UtcNow;

        var opened = Day(before, offsetDays);
        var closed = Day(after, offsetDays);

        Assert.Contains(first, new[] { opened, closed });
        Assert.Contains(second, new[] { opened, closed });

        if (opened == closed)
            Assert.Equal(first, second);
        else
            Assert.True(first <= second, $"{left} read {first} after {right} read {second}");
    }

    // ── What the rule must NOT reach ──

    /// <summary>
    /// A cast to anything but DATE or TIMESTAMP is left exactly as it was.
    /// </summary>
    /// <remarks>
    /// Spark's rule names three target types and this library models two of them, so a cast to
    /// STRING, INT or BOOLEAN over the same word must still take the ordinary path: measured,
    /// <c>CAST('epoch' AS STRING)</c> is the text <c>'epoch'</c> and <c>CAST('epoch' AS INT)</c>
    /// is refused.
    /// </remarks>
    [Fact]
    public void OnlyADateOrTimestampTargetFolds()
    {
        var text = Assert.IsType<StringArray>(Evaluate(Ansi, "CAST('epoch' AS STRING)"));
        Assert.Equal("epoch", text.GetString(0));

        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "CAST('epoch' AS INT)"));
        Assert.Throws<SparkEvaluationException>(() => Evaluate(Ansi, "CAST('today' AS BOOLEAN)"));
    }

    /// <summary>
    /// A constant that is a DATE the grammar reads is unaffected, in either spelling.
    /// </summary>
    /// <remarks>
    /// The fold sits in front of every constant cast to a date, so the control is worth asking:
    /// an ordinary date string must still go through <c>SparkTemporalText</c> and keep #318's
    /// answers, including the ones a special-value reader could plausibly have swallowed —
    /// <c>'2026-08-11 extra'</c> is a date and <c>'T12:30:00'</c> is a time alone.
    /// </remarks>
    [Fact]
    public void AnOrdinaryDateStringIsUntouched()
    {
        Assert.Equal(
            new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero),
            Date(Ansi, "CAST('2026-08-11 extra' AS DATE)"));

        Assert.Equal(
            new DateTimeOffset(2026, 8, 11, 12, 30, 0, TimeSpan.Zero),
            Instant(Ansi, "CAST('2026-08-11 12:30:00' AS TIMESTAMP)"));

        // A time alone starts with a letter and is three characters or more, so it reaches
        // extractSpecialValue and must come back out of it: 'T' is not the vocabulary, and the
        // tail '12:30:00' is not a zone.
        AssertNamesDay(
            () => Instant(Ansi, "CAST('T12:30:00' AS TIMESTAMP)"),
            timeOfDay: new TimeSpan(12, 30, 0));
    }

    /// <summary>
    /// A registry with no rule folds nothing, which is every other caller of the evaluator.
    /// </summary>
    /// <remarks>
    /// <c>DeltaTable</c>, <c>LanceTable</c> and <c>LanceDatasetWriter</c> each build an
    /// <see cref="ArrowRowEvaluator"/> with no registry at all, and the seam is asked for with an
    /// <c>as</c> cast so that they are unchanged. Asked through a registry that implements
    /// <see cref="IFunctionRegistry"/> alone, so the answer is about the seam and not about the
    /// evaluator having no functions.
    /// </remarks>
    [Fact]
    public void ARegistryWithoutTheRuleFoldsNothing()
    {
        var evaluator = new ArrowRowEvaluator(new CastOnly());
        var result = evaluator.EvaluateExpression(
            SparkSqlParser.ParseExpression("CAST('epoch' AS DATE)"), Row());

        var text = Assert.IsType<StringArray>(result);
        Assert.Equal("epoch", text.GetString(0));
    }

    /// <summary>A registry that answers a cast by handing its operand back, and nothing else.</summary>
    private sealed class CastOnly : IFunctionRegistry
    {
        public bool IsRegistered(string name) => name == "cast";

        public IArrowArray Invoke(string name, IReadOnlyList<IArrowArray> args, int rowCount) =>
            args[0];
    }
}
