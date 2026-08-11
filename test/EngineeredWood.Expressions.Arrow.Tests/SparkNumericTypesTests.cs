// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Checks the arithmetic promotion rules against Spark's own answers.
/// </summary>
/// <remarks>
/// The rules are not derivable from first principles — <c>smallint * smallint</c> stays
/// <c>smallint</c>, <c>int + float</c> is <c>double</c>, and <c>decimal(38,10)</c> squared clamps
/// to <c>decimal(38,6)</c> — so they are checked against what Spark actually resolved rather than
/// against a second reading of the same reasoning. Answers come from the harvested corpus, so
/// this needs no Spark, JVM or network.
/// </remarks>
public sealed class SparkNumericTypesTests
{
    private static readonly JsonDocument Corpus = JsonDocument.Parse(
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "spark-expression-corpus.json")));

    private static readonly Dictionary<string, IArrowType> Schema = LoadSchema();

    private static Dictionary<string, IArrowType> LoadSchema()
    {
        var schema = new Dictionary<string, IArrowType>(StringComparer.Ordinal);
        foreach (var field in Corpus.RootElement.GetProperty("schema").EnumerateArray())
        {
            var type = ParseSparkType(field.GetProperty("type").GetString()!);
            if (type is not null)
                schema[field.GetProperty("name").GetString()!] = type;
        }

        return schema;
    }

    /// <summary>A Spark type name as written in the corpus, or null if it is not numeric.</summary>
    private static IArrowType? ParseSparkType(string spark)
    {
        if (spark.StartsWith("decimal(", StringComparison.Ordinal))
        {
            // Substring rather than a range expression: net472 has no System.Index/System.Range.
            var inner = spark.Substring("decimal(".Length, spark.Length - "decimal(".Length - 1);
            var parts = inner.Split(',');
            return new Decimal128Type(int.Parse(parts[0].Trim()), int.Parse(parts[1].Trim()));
        }

        return spark switch
        {
            "tinyint" => Int8Type.Default,
            "smallint" => Int16Type.Default,
            "int" => Int32Type.Default,
            "bigint" => Int64Type.Default,
            "float" => FloatType.Default,
            "double" => DoubleType.Default,
            _ => null,
        };
    }

    /// <summary>Renders an Arrow type the way the corpus spells it.</summary>
    private static string SparkName(IArrowType type) => type switch
    {
        Int8Type => "tinyint",
        Int16Type => "smallint",
        Int32Type => "int",
        Int64Type => "bigint",
        FloatType => "float",
        DoubleType => "double",
        Decimal128Type d => $"decimal({d.Precision},{d.Scale})",
        Decimal256Type d => $"decimal({d.Precision},{d.Scale})",
        _ => type.Name,
    };

    /// <summary>
    /// The type our rules give an expression, or null if it is not pure numeric arithmetic.
    /// </summary>
    private static IArrowType? TypeOf(Expression expression)
    {
        switch (expression)
        {
            case UnboundReference reference:
                return Schema.TryGetValue(reference.Name, out var type) ? type : null;

            case LiteralExpression:
                return null; // no literal-typed cases in this group

            case FunctionCall { Name: "+" or "-" or "*" or "/" or "%" } call when call.Arguments.Count == 2:
                var left = TypeOf(call.Arguments[0]);
                var right = TypeOf(call.Arguments[1]);
                return left is null || right is null
                    ? null
                    : SparkNumericTypes.ArithmeticResult(call.Name, left, right);

            default:
                return null;
        }
    }

    [Fact]
    public void ArithmeticPromotionMatchesSpark()
    {
        var checked_ = 0;
        var mismatches = new List<string>();

        foreach (var entry in Corpus.RootElement.GetProperty("groups").GetProperty("coercion").EnumerateArray())
        {
            var sql = entry.GetProperty("expression").GetString()!;
            var recorded = entry.GetProperty("type");
            if (!recorded.GetProperty("ok").GetBoolean())
                continue;

            var ours = TypeOf(SparkSqlParser.ParseExpression(sql));
            if (ours is null)
                continue; // not arithmetic over two numeric columns

            checked_++;
            var expected = recorded.GetProperty("type").GetString();
            if (SparkName(ours) != expected)
                mismatches.Add($"{sql}: spark says {expected}, we say {SparkName(ours)}");
        }

        Assert.Empty(mismatches);

        // Guard against the filter silently excluding everything and the test passing vacuously.
        Assert.True(checked_ >= 15, $"only {checked_} arithmetic expressions were checked");
    }

    [Theory]
    // The five results nobody would guess, called out so a regression names itself.
    [InlineData("smallint", "*", "smallint", "smallint")]   // no widening: can overflow at its own width
    [InlineData("int", "+", "float", "double")]             // not float
    [InlineData("int", "/", "bigint", "double")]            // never integer division
    [InlineData("decimal(10,2)", "%", "decimal(6,4)", "decimal(6,4)")] // narrower operand wins
    [InlineData("decimal(38,10)", "*", "decimal(38,10)", "decimal(38,6)")] // clamped, scale sacrificed
    public void TheSurprisingRulesAreThePinnedOnes(string left, string op, string right, string expected)
    {
        var result = SparkNumericTypes.ArithmeticResult(op, ParseSparkType(left)!, ParseSparkType(right)!);
        Assert.Equal(expected, SparkName(result));
    }

    [Fact]
    public void AnIntegerMixedWithADecimalConvertsToTheDecimalThatHoldsItExactly()
    {
        Assert.Equal((10, 0), SparkNumericTypes.AsDecimal(Int32Type.Default));
        Assert.Equal((20, 0), SparkNumericTypes.AsDecimal(Int64Type.Default));

        // Which is what makes int + decimal(10,2) come out as decimal(13,2).
        Assert.Equal("decimal(13,2)", SparkName(
            SparkNumericTypes.ArithmeticResult("+", Int32Type.Default, new Decimal128Type(10, 2))));
    }

    [Fact]
    public void ClampingGivesUpScaleRatherThanIntegerDigits()
    {
        // Losing integer digits would change a value's magnitude; losing scale only its
        // exactness. The floor of 6 is why a very wide product keeps any fraction at all.
        Assert.Equal("decimal(38,6)", SparkName(SparkNumericTypes.Clamp(77, 20)));
        Assert.Equal("decimal(38,9)", SparkName(SparkNumericTypes.Clamp(39, 10)));
        Assert.Equal("decimal(20,4)", SparkName(SparkNumericTypes.Clamp(20, 4)));
    }
}
