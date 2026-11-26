// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Values;
using static EngineeredWood.Iceberg.Tests.Values.JavaSingleValueVectorTests;

namespace EngineeredWood.Iceberg.Tests.Values;

public class SingleValueJsonTests
{
    private static LiteralValue Parse(string typeName, string json)
    {
        using var doc = JsonDocument.Parse(json);
        return SingleValueJson.Parse(ParseType(typeName), doc.RootElement);
    }

    private static void Validate(IcebergType type, string json)
    {
        using var doc = JsonDocument.Parse(json);
        SingleValueJson.Validate(type, doc.RootElement);
    }

    [Fact]
    public void Null_IsNullForEveryType()
    {
        foreach (var name in new[] { "int", "decimal(9, 2)", "timestamp_ns", "variant", "unknown", "binary" })
            Assert.True(Parse(name, "null").IsNull);
        Assert.True(SingleValueJson.Parse(IcebergType.Geometry("srid:4326"), JsonDocument.Parse("null").RootElement).IsNull);
        Validate(new StructType([new NestedField(1, "a", IcebergType.Int, true)]), "null");
    }

    [Fact]
    public void Write_Spellings()
    {
        Assert.Equal("\"000102ff\"", SingleValueJson.ToJsonString(IcebergType.Binary, LiteralValue.Of(new byte[] { 0, 1, 2, 255 })));
        Assert.Equal("\"0.05\"", SingleValueJson.ToJsonString(IcebergType.Decimal(9, 2), LiteralValue.HighPrecisionDecimalOf(5, 2)));
        Assert.Equal("\"-0.05\"", SingleValueJson.ToJsonString(IcebergType.Decimal(9, 2), LiteralValue.HighPrecisionDecimalOf(-5, 2)));
        Assert.Equal("\"7\"", SingleValueJson.ToJsonString(IcebergType.Decimal(9, 0), LiteralValue.Of(7)));
        Assert.Equal("\"0.0000001\"", SingleValueJson.ToJsonString(IcebergType.Decimal(9, 7), LiteralValue.HighPrecisionDecimalOf(1, 7)));
        Assert.Equal("1.0", SingleValueJson.ToJsonString(IcebergType.Double, LiteralValue.Of(1.0)));
        Assert.Equal("100.0", SingleValueJson.ToJsonString(IcebergType.Float, LiteralValue.Of(100f)));
        Assert.Equal("null", SingleValueJson.ToJsonString(IcebergType.Int, LiteralValue.Null));
        Assert.Equal("\"café\"", SingleValueJson.ToJsonString(IcebergType.String, LiteralValue.Of("café")));
        Assert.Equal("\"1677-09-21T00:12:43.145224192+00:00\"",
            SingleValueJson.ToJsonString(IcebergType.TimestamptzNs, LiteralValue.TimestampNanosOf(long.MinValue)));
        Assert.Equal("\"0001-01-01T00:00:00\"",
            SingleValueJson.ToJsonString(IcebergType.Timestamp, LiteralValue.Of(DateTimeOffset.MinValue)));
    }

    /// <summary>A float is read as a float, not as a double rounded twice.</summary>
    [Fact]
    public void Float_IsReadAsAFloat()
    {
        // 1.00000005960464477539062 is half-way between two floats only after rounding to double first.
        Assert.Equal(LiteralValue.Of(1.0000001f), Parse("float", "1.0000001"));
        Assert.Equal(LiteralValue.Of(float.MaxValue), Parse("float", "3.4028235E38"));
        Assert.Equal(LiteralValue.Of(float.Epsilon), Parse("float", "1.4E-45"));
    }

    [Theory]
    [InlineData("double", "1e400")]
    [InlineData("float", "3.5e38")]
    [InlineData("double", "\"1.0\"")]
    [InlineData("int", "true")]
    [InlineData("string", "null1")]
    public void RefusesNonValues(string typeName, string json)
    {
        if (json == "null1")
        {
            Assert.ThrowsAny<JsonException>(() => Parse(typeName, json));
            return;
        }

        Assert.Throws<FormatException>(() => Parse(typeName, json));
    }

    [Fact]
    public void Write_RefusesNonFiniteAndForeignKinds()
    {
        Assert.Throws<ArgumentException>(() => SingleValueJson.ToJsonString(IcebergType.Double, LiteralValue.Of(double.NaN)));
        Assert.Throws<ArgumentException>(() => SingleValueJson.ToJsonString(IcebergType.Float, LiteralValue.Of(float.NegativeInfinity)));
        Assert.Throws<ArgumentException>(() => SingleValueJson.ToJsonString(IcebergType.String, LiteralValue.Of(1)));
        // variant and unknown have no value but null: a non-null one is not one of the type's.
        Assert.Throws<ArgumentException>(() => SingleValueJson.ToJsonString(IcebergType.Variant, LiteralValue.Of(1)));
        Assert.Throws<ArgumentException>(() => SingleValueJson.ToJsonString(IcebergType.Unknown, LiteralValue.Of("x")));
        // geometry is an EngineeredWood gap, not a malformed value.
        Assert.Throws<NotSupportedException>(() => SingleValueJson.ToJsonString(IcebergType.Geometry("srid:4326"), LiteralValue.Of(new byte[] { 1 })));
    }

    /// <summary>
    /// A non-null default for these types is malformed metadata (FormatException), the same answer
    /// <see cref="SingleValueJson.Validate"/> gives, and distinct from an EngineeredWood gap (NotSupportedException).
    /// </summary>
    [Fact]
    public void VariantAndUnknown_HaveOnlyNull()
    {
        Assert.Throws<FormatException>(() => Parse("variant", "{}"));
        Assert.Throws<FormatException>(() => Parse("unknown", "1"));
        Assert.Throws<FormatException>(() => Validate(IcebergType.Variant, "\"x\""));
    }

    [Fact]
    public void Geometry_IsAWktStringThatIsNotParsed()
    {
        Validate(IcebergType.Geometry("srid:4326"), "\"POINT (30 10)\"");
        Assert.Throws<FormatException>(() => Validate(IcebergType.Geometry("srid:4326"), "30"));
        Assert.Throws<NotSupportedException>(() =>
            SingleValueJson.Parse(IcebergType.Geometry("srid:4326"), JsonDocument.Parse("\"POINT (30 10)\"").RootElement));
    }

    [Fact]
    public void Nested_ValidatesAgainstTheType()
    {
        var point = new StructType([
            new NestedField(1, "x", IcebergType.Double, true),
            new NestedField(2, "label", IcebergType.String, false),
        ]);
        Validate(point, "{\"1\": 1.5, \"2\": \"a\"}");
        Validate(point, "{\"1\": 1.5}");
        Validate(point, "{}");
        Assert.Throws<FormatException>(() => Validate(point, "{\"3\": 1}"));
        Assert.Throws<FormatException>(() => Validate(point, "{\"x\": 1.5}"));
        var e = Assert.Throws<FormatException>(() => Validate(point, "{\"1\": 1}"));
        Assert.Contains("$['1']", e.Message);
        Assert.Throws<FormatException>(() => Validate(point, "{\"1\": null}"));

        var list = new ListType(3, IcebergType.Int, ElementRequired: true);
        Validate(list, "[1, 2, 3]");
        Assert.Throws<FormatException>(() => Validate(list, "[1, null]"));
        Validate(new ListType(3, IcebergType.Int, ElementRequired: false), "[1, null]");

        var map = new MapType(4, IcebergType.String, 5, IcebergType.Long, ValueRequired: false);
        Validate(map, "{\"keys\": [\"a\", \"b\"], \"values\": [1, null]}");
        Assert.Throws<FormatException>(() => Validate(map, "{\"keys\": [\"a\"], \"values\": [1, 2]}"));
        Assert.Throws<FormatException>(() => Validate(map, "{\"keys\": [null], \"values\": [1]}"));
        Assert.Throws<FormatException>(() => Validate(map, "{\"a\": 1}"));

        Assert.Throws<NotSupportedException>(() => SingleValueJson.Parse(point, JsonDocument.Parse("{}").RootElement));
    }

    [Fact]
    public void ToJsonElement_RoundTrips()
    {
        var v = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("-12345678901234567890123456789012345678"), 0);
        var element = SingleValueJson.ToJsonElement(IcebergType.Decimal(38, 0), v);
        Assert.Equal(JsonValueKind.String, element.ValueKind);
        Assert.Equal(v, SingleValueJson.Parse(IcebergType.Decimal(38, 0), element));
    }

    /// <summary>Every primitive value written as JSON reads back as itself.</summary>
    [Theory]
    [MemberData(nameof(SingleValueBinaryTests.RoundTripValues), MemberType = typeof(SingleValueBinaryTests))]
    public void RoundTrips(string typeName, object value)
    {
        var type = ParseType(typeName);
        var literal = (LiteralValue)value;
        string json = SingleValueJson.ToJsonString(type, literal);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(IcebergLiterals.Coerce(type, literal), SingleValueJson.Parse(type, doc.RootElement));
    }

    [Fact]
    public void RandomDoublesAndFloats_RoundTrip()
    {
        var random = new Random(953);
        var bytes = new byte[8];
        for (int i = 0; i < 20_000; i++)
        {
            random.NextBytes(bytes);
            double d = BitConverter.ToDouble(bytes, 0);
            float f = BitConverter.ToSingle(bytes, 0);
            if (!double.IsNaN(d) && !double.IsInfinity(d))
            {
                using var doc = JsonDocument.Parse(SingleValueJson.ToJsonString(IcebergType.Double, LiteralValue.Of(d)));
                Assert.Equal(BitConverter.DoubleToInt64Bits(d), BitConverter.DoubleToInt64Bits(SingleValueJson.Parse(IcebergType.Double, doc.RootElement).AsDouble));
            }

            if (!float.IsNaN(f) && !float.IsInfinity(f))
            {
                using var doc = JsonDocument.Parse(SingleValueJson.ToJsonString(IcebergType.Float, LiteralValue.Of(f)));
                Assert.Equal(f, SingleValueJson.Parse(IcebergType.Float, doc.RootElement).AsFloat);
            }
        }
    }
}
