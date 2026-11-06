// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Data.SqlTypes;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;

namespace EngineeredWood.Expressions.Arrow.Tests;

/// <summary>
/// Row-level <c>variant_get</c> / <c>try_variant_get</c> and the cast of a variant (#681).
/// </summary>
/// <remarks>
/// Every expected value was measured against pyspark 4.1.2, as
/// <c>variant_get(parse_json('…'), path, type)</c> and its <c>try_</c> form. <c>parse_json</c> reads a JSON
/// number with a fraction as a DECIMAL and one with an exponent as a DOUBLE, so the values here are built with
/// those types rather than parsed.
/// </remarks>
public sealed class SparkVariantGetTests
{
    private const string Error = "INVALID_VARIANT_CAST";

    private static VariantValue Obj(string key, VariantValue value) =>
        VariantValue.FromObject(new Dictionary<string, VariantValue> { [key] = value });

    private static VariantValue A(VariantValue value) => Obj("a", value);

    private static VariantValue Dec(decimal d) => VariantValue.FromDecimal(d);

    /// <summary>(variant, path, type, variant_get's answer, try_variant_get's answer).</summary>
    public static TheoryData<string, string, string, object?, object?> Measured()
    {
        var data = new TheoryData<string, string, string, object?, object?>();
        foreach (var (name, _, path, type, get, tryGet) in Cases())
            data.Add(name, path, type, get, tryGet);
        return data;
    }

    private static IEnumerable<(string Name, VariantValue Value, string Path, string Type, object? Get, object? Try)> Cases()
    {
        yield return ("string 7 as int", A(VariantValue.FromString("7")), "$.a", "int", 7, 7);
        yield return ("padded string as int", A(VariantValue.FromString(" 7 ")), "$.a", "int", 7, 7);
        yield return ("string 1.5 as int", A(VariantValue.FromString("1.5")), "$.a", "int", Error, null);
        yield return ("string x as int", A(VariantValue.FromString("x")), "$.a", "int", Error, null);
        yield return ("decimal 1.25 as int truncates", A(Dec(1.25m)), "$.a", "int", 1, 1);
        yield return ("decimal 1.25 as decimal(2,1) rounds", A(Dec(1.25m)), "$.a", "decimal(2,1)", 1.3m, 1.3m);
        yield return ("decimal -2.5 as decimal(1,0)", A(Dec(-2.5m)), "$.a", "decimal(1,0)", -3m, -3m);
        yield return ("decimal 2.5 as bigint", A(Dec(2.5m)), "$.a", "bigint", 2L, 2L);
        yield return ("decimal 1.50 as string strips", A(Dec(1.50m)), "$.a", "string", "1.5", "1.5");
        yield return ("decimal 1.50 as decimal(10,2)", A(Dec(1.50m)), "$.a", "decimal(10,2)", 1.50m, 1.50m);
        yield return ("decimal 0.05 as string", A(Dec(0.05m)), "$.a", "string", "0.05", "0.05");
        yield return ("decimal 1.5 as double", A(Dec(1.5m)), "$.a", "double", 1.5, 1.5);
        yield return ("int 100 as string", A(VariantValue.FromInt8(100)), "$.a", "string", "100", "100");
        yield return ("double 1e2 as string", A(VariantValue.FromDouble(100.0)), "$.a", "string", "100.0", "100.0");
        yield return ("double 1.5e300 as int", A(VariantValue.FromDouble(1.5e300)), "$.a", "int", Error, null);
        yield return ("int 300 as tinyint", A(VariantValue.FromInt16(300)), "$.a", "tinyint", Error, null);
        yield return ("long as int overflows", A(VariantValue.FromInt64(99999999999L)), "$.a", "int", Error, null);
        yield return ("true as int", A(VariantValue.True), "$.a", "int", 1, 1);
        yield return ("true as string", A(VariantValue.True), "$.a", "string", "true", "true");
        yield return ("true as date", A(VariantValue.True), "$.a", "date", Error, null);
        yield return ("true as binary", A(VariantValue.True), "$.a", "binary", Error, null);
        yield return ("1 as boolean", A(VariantValue.FromInt8(1)), "$.a", "boolean", true, true);
        yield return ("1 as date", A(VariantValue.FromInt8(1)), "$.a", "date", Error, null);
        yield return ("1 as binary", A(VariantValue.FromInt8(1)), "$.a", "binary", Error, null);
        yield return ("1 as timestamp", A(VariantValue.FromInt8(1)), "$.a", "timestamp",
            DateTimeOffset.FromUnixTimeSeconds(1), DateTimeOffset.FromUnixTimeSeconds(1));
        yield return ("string as binary", A(VariantValue.FromString("abc")), "$.a", "binary", System.Text.Encoding.UTF8.GetBytes("abc"), System.Text.Encoding.UTF8.GetBytes("abc"));
        yield return ("string as date", A(VariantValue.FromString("2026-01-02")), "$.a", "date",
            new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero), new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero));
        yield return ("object as int", A(Obj("b", VariantValue.FromInt8(1))), "$.a", "int", Error, null);
        yield return ("array element", A(VariantValue.FromArray(VariantValue.FromInt8(1), VariantValue.FromInt8(2))), "$.a[1]", "int", 2, 2);
        yield return ("array element missing", A(VariantValue.FromArray(VariantValue.FromInt8(1), VariantValue.FromInt8(2))), "$.a[5]", "int", null, null);
        yield return ("variant null", A(VariantValue.Null), "$.a", "int", null, null);
        yield return ("missing key", A(VariantValue.FromInt8(1)), "$.b", "int", null, null);
        yield return ("path through a scalar", A(VariantValue.FromInt8(1)), "$.a.b", "int", null, null);
        yield return ("key into an array", VariantValue.FromArray(VariantValue.FromInt8(1), VariantValue.FromInt8(2)), "$.a", "int", null, null);
        yield return ("element of an object", A(VariantValue.FromInt8(1)), "$[0]", "int", null, null);
        yield return ("root", VariantValue.FromInt8(3), "$", "int", 3, 3);
        // PR #721 review, measured: a timestamp keeps its fraction, an integral cast floors it, TIMESTAMP_NTZ has no
        // numeric reading, and timestamp_ltz is timestamp.
        yield return ("timestamp 1.25 s as double", VariantValue.FromTimestamp(1_250_000L), "$", "double", 1.25, 1.25);
        yield return ("timestamp 1.25 s as float", VariantValue.FromTimestamp(1_250_000L), "$", "float", 1.25f, 1.25f);
        yield return ("timestamp 1.25 s as decimal(10,2)", VariantValue.FromTimestamp(1_250_000L), "$", "decimal(10,2)", 1.25m, 1.25m);
        yield return ("timestamp -1.25 s as decimal(10,1)", VariantValue.FromTimestamp(-1_250_000L), "$", "decimal(10,1)", -1.3m, -1.3m);
        yield return ("timestamp -1.25 s as bigint floors", VariantValue.FromTimestamp(-1_250_000L), "$", "bigint", -2L, -2L);
        yield return ("timestamp 1.25 s as int", VariantValue.FromTimestamp(1_250_000L), "$", "int", 1, 1);
        yield return ("timestamp as boolean", VariantValue.FromTimestamp(1_250_000L), "$", "boolean", Error, null);
        yield return ("timestamp_ntz as double", VariantValue.FromTimestampNtz(1_250_000L), "$", "double", Error, null);
        yield return ("timestamp_ntz as bigint", VariantValue.FromTimestampNtz(1_250_000L), "$", "bigint", Error, null);
        yield return ("timestamp_ntz as date", VariantValue.FromTimestampNtz(1_250_000L), "$", "date",
            DateTimeOffset.FromUnixTimeSeconds(0), DateTimeOffset.FromUnixTimeSeconds(0));
        yield return ("1 as timestamp_ltz", A(VariantValue.FromInt8(1)), "$.a", "timestamp_ltz",
            DateTimeOffset.FromUnixTimeSeconds(1), DateTimeOffset.FromUnixTimeSeconds(1));
    }

    private static readonly IReadOnlyDictionary<string, VariantValue> ByName =
        Cases().ToDictionary(c => c.Name, c => c.Value);

    [Theory]
    [MemberData(nameof(Measured))]
    public void UnshreddedMatchesSpark(string name, string path, string type, object? get, object? tryGet)
    {
        var batch = Batch(Unshredded(ByName[name]));
        AssertAnswer($"variant_get(v, '{path}', '{type}')", batch, get);
        AssertAnswer($"try_variant_get(v, '{path}', '{type}')", batch, tryGet);
    }

    [Theory]
    [MemberData(nameof(Measured))]
    public void ShreddedMatchesSpark(string name, string path, string type, object? get, object? tryGet)
    {
        var value = ByName[name];
        var batch = Batch(Shredded(value));
        Assert.True(((VariantArray)batch.Column(0)).IsShredded);
        AssertAnswer($"variant_get(v, '{path}', '{type}')", batch, get);
        AssertAnswer($"try_variant_get(v, '{path}', '{type}')", batch, tryGet);
    }

    [Fact]
    public void ASqlNullVariantIsNull()
    {
        var batch = Batch(Unshredded(null, A(VariantValue.FromInt8(4))));
        var result = (Int32Array)Eval("variant_get(v, '$.a', 'int')", batch);
        Assert.Null(result.GetValue(0));
        Assert.Equal(4, result.GetValue(1));
    }

    [Fact]
    public void TheShorthandCastsTheExtractedVariant()
    {
        var batch = Batch(Unshredded(Obj("a", Obj("b", VariantValue.FromInt8(5))), A(VariantValue.FromString("x"))));

        var tried = (Int32Array)Eval("try_cast(v:a.b AS INT)", batch);
        Assert.Equal(5, tried.GetValue(0));
        Assert.Null(tried.GetValue(1));

        Assert.Equal(5, ((Int32Array)Eval("v:a.b::int", Batch(Unshredded(Obj("a", Obj("b", VariantValue.FromInt8(5))))))).GetValue(0));

        // `v:a::int` over the string "x" raises, as variant_get does.
        var ex = Assert.Throws<SparkEvaluationException>(() => Eval("v:a::int", Batch(Unshredded(A(VariantValue.FromString("x"))))));
        Assert.Equal(Error, ex.ErrorClass);
        Assert.Contains("cannot be cast into `\"INT\"`", ex.Message);
    }

    [Fact]
    public void AVariantCastRaisesUnderTheLegacyDialectToo()
    {
        var legacy = new ArrowRowEvaluator(new SparkFunctionRegistry(new SparkDialectOptions { Ansi = false }));
        var batch = Batch(Unshredded(A(VariantValue.FromString("x"))));
        var cast = SparkSqlParser.ParseExpression("cast(v:a AS INT)");
        var ex = Assert.Throws<SparkEvaluationException>(() => legacy.EvaluateExpression(cast, batch));
        Assert.Equal(Error, ex.ErrorClass);

        var get = SparkSqlParser.ParseExpression("variant_get(v, '$.a', 'int')");
        Assert.Equal(Error, Assert.Throws<SparkEvaluationException>(() => legacy.EvaluateExpression(get, batch)).ErrorClass);
    }

    [Fact]
    public void AVariantResultKeepsAVariantNull()
    {
        // `variant_get(v, '$.a') IS NULL` is false over {"a":null} and true over a missing key.
        var batch = Batch(Unshredded(A(VariantValue.Null), A(VariantValue.FromInt8(1))));
        var isNull = new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluatePredicate(
            SparkSqlParser.ParsePredicate("variant_get(v, '$.a') IS NULL"), batch);
        Assert.False(isNull.GetValue(0));
        Assert.False(isNull.GetValue(1));

        var missing = new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluatePredicate(
            SparkSqlParser.ParsePredicate("variant_get(v, '$.b') IS NULL"), batch);
        Assert.True(missing.GetValue(0));

        var result = (VariantArray)Eval("variant_get(v, '$.a', 'variant')", batch);
        Assert.True(result.GetVariantValue(0).IsNull);
        Assert.False(result.IsNull(0));
        Assert.Equal(1, result.GetVariantValue(1).AsInt8());

        Assert.Null(((Int32Array)Eval("cast(variant_get(v, '$.a') AS INT)", batch)).GetValue(0));
    }

    [Fact]
    public void RowsOfDifferentTypesCastTogether()
    {
        var batch = Batch(Unshredded(
            A(VariantValue.FromString("7")),
            A(Dec(1.25m)),
            null,
            A(VariantValue.FromDouble(2.75)),
            A(VariantValue.FromString("x")),
            A(Dec(-3.5m)),
            A(VariantValue.FromInt64(12))));

        var result = (Int32Array)Eval("try_variant_get(v, '$.a', 'int')", batch);
        Assert.Equal(new int?[] { 7, 1, null, 2, null, -3, 12 }, Enumerable.Range(0, result.Length).Select(i => result.GetValue(i)));
    }

    [Fact]
    public void TheFirstFailingRowIsReported()
    {
        var batch = Batch(Unshredded(
            A(VariantValue.FromInt8(1)), A(VariantValue.FromString("first")), A(VariantValue.True), A(VariantValue.FromString("second"))));
        var ex = Assert.Throws<SparkEvaluationException>(() => Eval("variant_get(v, '$.a', 'date')", batch));
        Assert.Contains("`1`", ex.Message);
    }

    [Fact]
    public void AnInvalidPathIsRefused()
    {
        var batch = Batch(Unshredded(A(VariantValue.FromInt8(1))));
        var ex = Assert.Throws<SparkEvaluationException>(() => Eval("variant_get(v, 'a', 'int')", batch));
        Assert.Equal("INVALID_VARIANT_GET_PATH", ex.ErrorClass);
    }

    [Fact]
    public void ANonVariantInputIsRefused()
    {
        var b = new StringArray.Builder();
        b.Append("{\"a\":1}");
        var batch = Batch(b.Build());
        var ex = Assert.Throws<ExpressionAnalysisException>(() => Eval("variant_get(v, '$.a', 'int')", batch));
        Assert.Equal("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE", ex.ErrorClass);
    }

    [Fact]
    public void AnObjectToStringIsNotImplemented()
    {
        // Spark answers its JSON text, '{"b":1}'. Refused here rather than rendered approximately.
        var batch = Batch(Unshredded(A(Obj("b", VariantValue.FromInt8(1)))));
        Assert.Throws<NotSupportedException>(() => Eval("variant_get(v, '$.a', 'string')", batch));
    }

    [Fact]
    public void AnEmptyBatchEvaluates()
    {
        // The CHECK-constraint analysis evaluates against no rows.
        var batch = Batch(new VariantArray.Builder().Build());
        var result = new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluatePredicate(
            SparkSqlParser.ParsePredicate("variant_get(v, '$.a', 'int') > 0"), batch);
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public void AConstraintIsAViolationWhereThePathIsMissing()
    {
        var batch = Batch(Unshredded(A(VariantValue.FromInt8(3)), A(VariantValue.FromInt8(-1)), Obj("b", VariantValue.FromInt8(1)), null));
        var result = new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluatePredicate(
            SparkSqlParser.ParsePredicate("variant_get(v, '$.a', 'int') > 0"), batch);
        Assert.True(result.GetValue(0));
        Assert.False(result.GetValue(1));
        Assert.Null(result.GetValue(2));
        Assert.Null(result.GetValue(3));
    }

    [Fact]
    public void AFoldablePathAndTypeAreRead()
    {
        // Spark takes any foldable path and type (measured: concat('$.', 'a') and concat('in', 't') both answer 3).
        var batch = Batch(Unshredded(A(VariantValue.FromInt8(3))));
        Assert.Equal(3, ((Int32Array)Eval("variant_get(v, concat('$.', 'a'), concat('in', 't'))", batch)).GetValue(0));

        // Over no rows too, which is how a CHECK constraint is analysed (PR #721 review).
        var none = Batch(new VariantArray.Builder().Build());
        var result = new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluatePredicate(
            SparkSqlParser.ParsePredicate("variant_get(v, concat('$.', 'a'), concat('in', 't')) > 0"), none);
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public void ATimestampColumnCastsToANumberWithItsFraction()
    {
        // Measured, Spark 4.1: CAST(TIMESTAMP '1970-01-01 00:00:01.25' AS DOUBLE) is 1.25, and
        // CAST(TIMESTAMP '1969-12-31 23:59:58.75' AS BIGINT) is -2. The fraction used to be dropped (PR #721 review).
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        b.Append(DateTimeOffset.FromUnixTimeMilliseconds(1250));
        b.Append(DateTimeOffset.FromUnixTimeMilliseconds(-1250));
        var ts = b.Build();
        var batch = new RecordBatch(new Schema.Builder().Field(new Field("ts", ts.Data.DataType, true)).Build(), new[] { ts }, 2);

        var doubles = (DoubleArray)Eval("CAST(ts AS DOUBLE)", batch);
        Assert.Equal(1.25, doubles.GetValue(0));
        Assert.Equal(-1.25, doubles.GetValue(1));

        var longs = (Int64Array)Eval("CAST(ts AS BIGINT)", batch);
        Assert.Equal(1L, longs.GetValue(0));
        Assert.Equal(-2L, longs.GetValue(1));

        var decimals = (Decimal128Array)Eval("CAST(ts AS DECIMAL(10,1))", batch);
        Assert.Equal(1.3m, decimals.GetSqlDecimal(0)!.Value.Value);
        Assert.Equal(-1.3m, decimals.GetSqlDecimal(1)!.Value.Value);
    }

    // ── helpers ──

    private static void AssertAnswer(string sql, RecordBatch batch, object? expected)
    {
        if (expected is Error)
        {
            var ex = Assert.Throws<SparkEvaluationException>(() => Eval(sql, batch));
            Assert.Equal(Error, ex.ErrorClass);
            return;
        }

        var actual = ValueAt(Eval(sql, batch), 0);
        Assert.Equal(expected, actual);
    }

    private static object? ValueAt(IArrowArray array, int index)
    {
        if (array.IsNull(index))
            return null;
        return array switch
        {
            Int8Array a => a.GetValue(index),
            Int16Array a => a.GetValue(index),
            Int32Array a => a.GetValue(index),
            Int64Array a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            FloatArray a => a.GetValue(index),
            BooleanArray a => a.GetValue(index),
            StringArray a => a.GetString(index),
            BinaryArray a => a.GetBytes(index).ToArray(),
            Decimal128Array a => a.GetSqlDecimal(index)!.Value.Value,
            Date32Array a => new DateTimeOffset(a.GetDateTime(index)!.Value, TimeSpan.Zero),
            TimestampArray a => a.GetTimestamp(index),
            _ => throw new InvalidOperationException(array.GetType().Name),
        };
    }

    private static IArrowArray Eval(string sql, RecordBatch batch) =>
        new ArrowRowEvaluator(new SparkFunctionRegistry()).EvaluateExpression(SparkSqlParser.ParseExpression(sql), batch);

    private static RecordBatch Batch(IArrowArray v) =>
        new(new Schema.Builder().Field(new Field("v", v.Data.DataType, true)).Build(), new[] { v }, v.Length);

    private static VariantArray Unshredded(params VariantValue?[] values)
    {
        var builder = new VariantArray.Builder();
        foreach (var value in values)
            builder.Append(value);
        return builder.Build();
    }

    /// <summary>The value shredded as deep as its shape allows: objects to fields, arrays to elements.</summary>
    private static VariantArray Shredded(VariantValue value)
    {
        var schema = SchemaFor(value);
        var (metadata, rows) = VariantShredder.Shred(new[] { value }, schema);
        return ShreddedVariantArrayBuilder.Build(schema, metadata, rows);
    }

    private static ShredSchema SchemaFor(VariantValue value)
    {
        if (value.IsObject)
            return ShredSchema.ForObject(value.AsObject().ToDictionary(kv => kv.Key, kv => SchemaFor(kv.Value)));
        if (value.IsArray)
            return ShredSchema.ForArray(value.AsArray().Count > 0 ? SchemaFor(value.AsArray()[0]) : ShredSchema.Unshredded());
        if (value.IsNull)
            return ShredSchema.Unshredded();
        return ShredSchema.Primitive(ShredSchema.ShredTypeFromPrimitive(value.PrimitiveType));
    }
}
