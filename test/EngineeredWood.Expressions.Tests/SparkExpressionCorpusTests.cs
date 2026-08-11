// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// Guards <c>Fixtures/spark-expression-corpus.json</c>, the checked-in record of what Spark
/// says about the expressions the phase 9 parser will have to handle.
/// </summary>
/// <remarks>
/// The corpus is harvested by
/// <c>test/EngineeredWood.DeltaLake.Table.Tests/Interop/harvest_expression_corpus.py</c> and
/// checked in so the parser can be developed offline — nothing here needs Spark, a JVM, or a
/// network.
///
/// These tests do not exercise a parser; there isn't one yet. They exist so the fixture cannot
/// rot silently before there is: that it ships to the output directory, that it is structurally
/// intact, and above all that it still carries the configuration it was gathered under.
///
/// That last one is the point. Delta pins no configuration when it evaluates a constraint, so a
/// recorded answer only means something next to the settings that produced it — the same
/// constraint accepts or rejects the same row depending on <c>spark.sql.ansi.enabled</c>.
/// Re-harvesting under different settings would silently invalidate every expectation derived
/// from this file, so the settings are asserted rather than merely stored.
/// </remarks>
public sealed class SparkExpressionCorpusTests
{
    [Theory]
    [InlineData("spark.sql.ansi.enabled", "true")]
    [InlineData("spark.sql.session.timeZone", "UTC")]
    [InlineData("spark.sql.storeAssignmentPolicy", "ANSI")]
    public void CorpusRecordsTheConfigurationItWasGatheredUnder(string key, string expected)
    {
        var conf = SparkCorpus.Root.GetProperty("conf");
        Assert.True(conf.TryGetProperty(key, out var actual), $"corpus does not record {key}");
        Assert.Equal(expected, actual.GetString());
    }

    [Fact]
    public void EveryGroupIsPopulated()
    {
        var groups = SparkCorpus.Root.GetProperty("groups");
        Assert.NotEmpty(groups.EnumerateObject());

        foreach (var group in groups.EnumerateObject())
            Assert.True(group.Value.GetArrayLength() > 0, $"group '{group.Name}' is empty");
    }

    [Fact]
    public void EveryEntryCarriesAnExpressionAndAParseVerdict()
    {
        foreach (var entry in SparkCorpus.Entries())
        {
            Assert.True(entry.TryGetProperty("expression", out _), "entry has no expression");
            var expression = entry.GetProperty("expression").GetString();

            Assert.True(entry.TryGetProperty("parse", out var parse),
                $"'{expression}' has no parse verdict");
            Assert.True(parse.TryGetProperty("ok", out var ok),
                $"'{expression}' parse verdict has no ok flag");

            // A successful parse must say how Spark re-rendered it — that rendering is the
            // precedence oracle, and an entry without it contributes nothing.
            if (ok.GetBoolean())
                Assert.False(string.IsNullOrWhiteSpace(parse.GetProperty("sql").GetString()),
                    $"'{expression}' parsed but recorded no sql rendering");
        }
    }

    [Fact]
    public void MalformedExpressionsAreRecordedAsParseFailures()
    {
        // If Spark ever starts accepting these, the corpus is describing a different language
        // than the one we are building a parser for, and we should find out here.
        foreach (var entry in SparkCorpus.Group("malformed").EnumerateArray())
            Assert.False(entry.GetProperty("parse").GetProperty("ok").GetBoolean(),
                $"'{entry.GetProperty("expression").GetString()}' is in the malformed group but parsed");
    }

    [Fact]
    public void ExpressionsBeyondConstraintScopeStillParse()
    {
        // Aggregates, windows and subqueries are Delta's to reject after parsing
        // (DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT), not the grammar's to refuse. This
        // pins that distinction: a parser that rejects them at the syntax level would be
        // diverging from Spark, not matching it.
        var group = SparkCorpus.Group("beyond-constraint-scope");

        foreach (var entry in group.EnumerateArray())
        {
            var expression = entry.GetProperty("expression").GetString();
            if (expression == "*")
                continue; // parses, but resolution legitimately fails outside a projection

            Assert.True(entry.GetProperty("parse").GetProperty("ok").GetBoolean(),
                $"'{expression}' was expected to parse, since Spark's parser accepts it");
        }
    }

}
