// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions.Sql;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// Variant path extraction (#677): Spark's path grammar, the <c>variant_get</c> forms a pruner recognizes, the
/// <c>v:a</c> shorthand, and the statistics evaluator's hook for a per-path accessor. Path and shorthand cases are
/// measured against Spark 4.1.2.
/// </summary>
public sealed class VariantGetTests
{
    private static string[] Keys(VariantPath path) =>
        path.Segments.Select(s => s.Key ?? $"#{s.Index}").ToArray();

    [Theory]
    [InlineData("$", new string[0])]
    [InlineData("$.a", new[] { "a" })]
    [InlineData("$. a", new[] { " a" })] // nothing is trimmed
    [InlineData("$.a ", new[] { "a " })]
    [InlineData("$.a b", new[] { "a b" })]
    [InlineData("$['a']", new[] { "a" })]
    [InlineData("$[' a']", new[] { " a" })]
    [InlineData("$[\"a\"]", new[] { "a" })]
    [InlineData("$['a.b']", new[] { "a.b" })]
    [InlineData("$.a.b", new[] { "a", "b" })]
    [InlineData("$.b['c']", new[] { "b", "c" })]
    [InlineData("$['b'].c", new[] { "b", "c" })]
    [InlineData("$.b .c", new[] { "b ", "c" })]
    [InlineData("$.a]", new[] { "a]" })]
    [InlineData("$.a\t", new[] { "a\t" })]
    [InlineData("$.*", new[] { "*" })]
    [InlineData("$['?']", new[] { "?" })]
    [InlineData("$.A", new[] { "A" })]
    [InlineData("$.a[0]", new[] { "a", "#0" })]
    [InlineData("$[12]", new[] { "#12" })]
    public void SparksPaths_Parse(string text, string[] keys)
    {
        Assert.True(VariantPath.TryParse(text, out var path), text);
        Assert.Equal(keys, Keys(path));
    }

    [Theory]
    [InlineData("")]
    [InlineData("a")]
    [InlineData("$ .a")]
    [InlineData(" $.a")]
    [InlineData("$.")]
    [InlineData("$..a")]
    [InlineData("$['a'")]
    [InlineData("$['q'x']")]
    [InlineData("$[ 'a' ]")]
    [InlineData("$[]")]
    [InlineData("$[-1]")]
    [InlineData("$[99999999999]")]
    public void TextsSparkRefuses_DoNotParse(string text) =>
        Assert.False(VariantPath.TryParse(text, out _), text);

    [Theory]
    [InlineData("a")]
    [InlineData("a b")]
    [InlineData("a.b")]
    [InlineData("a[b")]
    [InlineData("it's")]
    [InlineData("it's.x")]
    [InlineData("say \"hi\". ok")]
    public void PathText_ReadsBackAsThePath(string key)
    {
        var path = VariantPath.OfFields(key, "z");
        Assert.True(VariantPath.TryParse(path.ToPathText(), out var back), path.ToPathText());
        Assert.Equal(path, back);
    }

    [Fact]
    public void AKeyNoPathTextCanSpell_HasNoText() =>
        Assert.Null(VariantPath.OfFields("it's \"x\".y").ToPathText());

    // ── Recognizing the call ──

    private static VariantGetCall Match(string sql)
    {
        Assert.True(VariantGetCall.TryMatch(SparkSqlParser.ParseExpression(sql), out var call), sql);
        return call;
    }

    [Theory]
    [InlineData("variant_get(v, '$.a', 'int')", "int", true)]
    [InlineData("VARIANT_GET(v, '$.a', ' INT ')", "int", true)]
    [InlineData("try_variant_get(v, '$.a', 'integer')", "int", false)]
    [InlineData("variant_get(v, '$.a', 'long')", "bigint", true)]
    [InlineData("variant_get(v, '$.a')", "variant", true)]
    [InlineData("v:a", "variant", true)]
    [InlineData("v:a::int", "int", true)]
    [InlineData("cast(v:a AS decimal)", "decimal(10,0)", true)]
    [InlineData("try_cast(v:a AS DECIMAL(5, 2))", "decimal(5,2)", false)]
    [InlineData("cast(try_variant_get(v, '$.a') AS timestamp_ltz)", "timestamp", true)]
    public void TheForms_ReadAsOneCall(string sql, string type, bool failOnError)
    {
        var call = Match(sql);
        Assert.Equal("v", call.Column.Name);
        Assert.Equal(["a"], Keys(call.Path));
        Assert.Equal(type, call.TargetType);
        Assert.Equal(failOnError, call.FailOnError);
    }

    [Theory]
    [InlineData("v:a.b::int", new[] { "a", "b" })]
    [InlineData("v : a . b::int", new[] { "a", "b" })]
    [InlineData("v:a:b::int", new[] { "a", "b" })]
    [InlineData("v:['a b']['c']::int", new[] { "a b", "c" })]
    [InlineData("v:`a b`.c::int", new[] { "a b", "c" })]
    [InlineData("v:`it's`::int", new[] { "it's" })]
    [InlineData("v:A::int", new[] { "A" })]
    [InlineData("v:a[0]::int", new[] { "a", "#0" })]
    [InlineData("v:[0].a::int", new[] { "#0", "a" })]
    [InlineData("v:b.`c`::int", new[] { "b", "c" })]
    [InlineData("variant_get(variant_get(v, '$.a'), '$.b', 'int')", new[] { "a", "b" })]
    [InlineData("s.v:a::int", new[] { "a" })]
    public void TheShorthand_ExtendsThePath(string sql, string[] keys)
    {
        var call = Match(sql);
        Assert.Equal(keys, Keys(call.Path));
        Assert.Equal("int", call.TargetType);
    }

    [Fact]
    public void ANestedColumn_IsTheReference() =>
        Assert.Equal(["s", "v"], Match("s.v:a::int").Column.NameParts);

    [Theory]
    [InlineData("v:[\"a.b\"]")]
    [InlineData("v:['q''x']")]
    [InlineData("v:1")]
    [InlineData("v:*")]
    [InlineData("v:")]
    public void ShorthandSparkRefuses_DoesNotParse(string sql) =>
        Assert.Throws<SparkSqlParseException>(() => SparkSqlParser.ParseExpression(sql));

    [Fact]
    public void TheShorthand_BindsAsAPostfix()
    {
        // -v:a::int is -(cast(v:a AS int)); v:a::int + 1 adds to the cast.
        var negated = Assert.IsType<FunctionCall>(SparkSqlParser.ParseExpression("-v:a::int"));
        Assert.Equal("negative", negated.Name);
        Assert.True(VariantGetCall.TryMatch(negated.Arguments[0], out _));
        var sum = Assert.IsType<FunctionCall>(SparkSqlParser.ParseExpression("v:a::int + 1"));
        Assert.True(VariantGetCall.TryMatch(sum.Arguments[0], out _));
    }

    [Theory]
    [InlineData("variant_get(1, '$.a', 'int')")]
    [InlineData("variant_get(v, path, 'int')")]
    [InlineData("variant_get(v, '$.a', t)")]
    [InlineData("variant_get(v, 'a', 'int')")]
    [InlineData("variant_get(v, '$.a', 'array<int>')")]
    [InlineData("variant_get(v, '$.a', 'decimal(39,0)')")]
    [InlineData("variant_get(v, '$.a', 'decimal(5,6)')")]
    [InlineData("cast(v AS int)")]
    [InlineData("cast(variant_get(v, '$.a', 'int') AS bigint)")]
    [InlineData("get_json_object(v, '$.a')")]
    public void OtherCalls_AreNotOne(string sql) =>
        Assert.False(VariantGetCall.TryMatch(SparkSqlParser.ParseExpression(sql), out _), sql);

    [Fact]
    public void TheFactories_BuildWhatTheParserDoes()
    {
        Assert.Equal(SparkSqlParser.ParseExpression("variant_get(v, '$.a', 'int')"), Ex.VariantGet("v", "$.a", "int"));
        Assert.Equal(SparkSqlParser.ParseExpression("try_variant_get(v, '$.a', 'int')"), Ex.TryVariantGet("v", "$.a", "int"));
        Assert.Equal(Ex.VariantGet("v", "$.a", "int"), Match("v:a::int").ToFunctionCall());
    }

    // ── The evaluator's hook ──

    /// <summary>`v` is a plain column [0, 9]; its path `$.a` as int is bounded [10, 20].</summary>
    private sealed class Accessor : IStatisticsAccessor<int>, IVariantStatisticsAccessor<int>
    {
        public List<VariantGetCall> Asked { get; } = [];

        public LiteralValue? GetMinValue(int stats, IReadOnlyList<string> column) => column is ["v"] ? LiteralValue.Of(0) : (LiteralValue?)null;
        public LiteralValue? GetMaxValue(int stats, IReadOnlyList<string> column) => column is ["v"] ? LiteralValue.Of(9) : (LiteralValue?)null;
        public long? GetNullCount(int stats, IReadOnlyList<string> column) => null;
        public long? GetValueCount(int stats, IReadOnlyList<string> column) => 100;
        public bool IsMinExact(int stats, IReadOnlyList<string> column) => true;
        public bool IsMaxExact(int stats, IReadOnlyList<string> column) => true;

        public IStatisticsAccessor<int>? ForVariantGet(VariantGetCall call)
        {
            Asked.Add(call);
            return call.TargetType == "int" && Keys(call.Path).SequenceEqual(["a"]) ? new Path() : null;
        }

        private sealed class Path : IStatisticsAccessor<int>
        {
            public LiteralValue? GetMinValue(int stats, IReadOnlyList<string> column) => LiteralValue.Of(10);
            public LiteralValue? GetMaxValue(int stats, IReadOnlyList<string> column) => LiteralValue.Of(20);
            public long? GetNullCount(int stats, IReadOnlyList<string> column) => null;
            public long? GetValueCount(int stats, IReadOnlyList<string> column) => 100;
            public bool IsMinExact(int stats, IReadOnlyList<string> column) => true;
            public bool IsMaxExact(int stats, IReadOnlyList<string> column) => true;
        }
    }

    private static FilterResult Evaluate(string sql, Accessor? accessor = null) =>
        StatisticsEvaluator.Evaluate(SparkSqlParser.ParsePredicate(sql), 0, accessor ?? new Accessor());

    [Fact]
    public void TheEvaluator_ReadsAPathFromThePathsAccessor()
    {
        Assert.Equal(FilterResult.AlwaysFalse, Evaluate("v:a::int > 20"));
        Assert.Equal(FilterResult.AlwaysFalse, Evaluate("5 > v:a::int"));
        Assert.Equal(FilterResult.AlwaysFalse, Evaluate("v:a::int IN (1, 2)"));
        Assert.Equal(FilterResult.Unknown, Evaluate("v:a::int > 15"));
        // The column's own bounds are not the path's.
        Assert.Equal(FilterResult.Unknown, Evaluate("v:a::int > 5"));
        Assert.Equal(FilterResult.AlwaysFalse, Evaluate("v > 9"));
    }

    [Fact]
    public void APathTheAccessorDoesNotBound_IsUnknown()
    {
        var accessor = new Accessor();
        Assert.Equal(FilterResult.Unknown, Evaluate("v:b::int > 20", accessor));
        Assert.Equal(FilterResult.Unknown, Evaluate("v:a::bigint > 20", accessor));
        Assert.Equal(2, accessor.Asked.Count);
    }

    [Fact]
    public void AnAccessorWithoutTheHook_AnswersUnknown()
    {
        var plain = new PlainAccessor();
        Assert.Equal(FilterResult.Unknown,
            StatisticsEvaluator.Evaluate(SparkSqlParser.ParsePredicate("v:a::int > 20"), 0, plain));
    }

    private sealed class PlainAccessor : IStatisticsAccessor<int>
    {
        public LiteralValue? GetMinValue(int stats, IReadOnlyList<string> column) => LiteralValue.Of(0);
        public LiteralValue? GetMaxValue(int stats, IReadOnlyList<string> column) => LiteralValue.Of(9);
        public long? GetNullCount(int stats, IReadOnlyList<string> column) => 0;
        public long? GetValueCount(int stats, IReadOnlyList<string> column) => 100;
        public bool IsMinExact(int stats, IReadOnlyList<string> column) => true;
        public bool IsMaxExact(int stats, IReadOnlyList<string> column) => true;
    }
}
