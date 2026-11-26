// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Numerics;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Values;
using static EngineeredWood.Iceberg.Tests.Values.JavaSingleValueVectorTests;

namespace EngineeredWood.Iceberg.Tests.Values;

public class SingleValueBinaryTests
{
    private static readonly DateTimeOffset Epoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // ── Bounds after a type promotion, told apart by length ──

    [Fact]
    public void LongBound_OfFourBytes_IsAnIntBoundWidened()
    {
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Long, FromHex("ffffffff"), out var b));
        Assert.Equal(LiteralValue.Of(-1L), b);
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Long, FromHex("ffffffffffffffff"), out b));
        Assert.Equal(LiteralValue.Of(-1L), b);
    }

    [Fact]
    public void DoubleBound_OfFourBytes_IsAFloatBoundWidened()
    {
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Double, FromHex("cdcccc3d"), out var b));
        Assert.Equal(LiteralValue.Of((double)0.1f), b); // exactly the float, not 0.1d
    }

    [Fact]
    public void TimestampBound_OfFourBytes_IsADateBoundAtMidnight()
    {
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Timestamp, FromHex("4e440000"), out var b));
        Assert.Equal(LiteralValue.Of(new DateTimeOffset(2017, 11, 16, 0, 0, 0, TimeSpan.Zero)), b);
    }

    [Fact]
    public void TimestampNsBound_OfFourBytes_IsADateBoundAtMidnight()
    {
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.TimestampNs, FromHex("4e440000"), out var b));
        Assert.Equal(LiteralValue.TimestampNanosOf(17_486L * 86_400_000_000_000L), b);
    }

    /// <summary>The spec promotes a date to timestamp and timestamp_ns only, never to a zoned type.</summary>
    [Fact]
    public void ZonedTimestampBounds_OfFourBytes_AreDeclined()
    {
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Timestamptz, FromHex("4e440000"), out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.TimestamptzNs, FromHex("4e440000"), out _));
    }

    [Fact]
    public void TimestampNsBound_FromADateOutsideItsRange_IsDeclined()
    {
        // 2263-01-01 is past 2262-04-11, the last nanosecond timestamp.
        var days = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(days, 106_752);
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.TimestampNs, days, out _));
        BinaryPrimitives.WriteInt32LittleEndian(days, int.MaxValue);
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.TimestampNs, days, out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Timestamp, days, out _));
    }

    /// <summary>Promoting decimal(P, S) to decimal(P', S) keeps the unscaled bytes, at any length.</summary>
    [Fact]
    public void DecimalBound_ReadsAnyLength()
    {
        var wide = IcebergType.Decimal(38, 2);
        Assert.True(SingleValueBinary.TryDecodeBound(wide, FromHex("058c"), out var b));
        Assert.Equal(LiteralValue.HighPrecisionDecimalOf(1420, 2), b);
        Assert.True(SingleValueBinary.TryDecodeBound(wide, FromHex("00000000000000000000058c"), out b));
        Assert.Equal(LiteralValue.HighPrecisionDecimalOf(1420, 2), b);
    }

    [Fact]
    public void DecimalBound_WiderThanItsPrecision_IsDeclined()
    {
        // 12345678.90 has ten digits.
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Decimal(9, 2), FromHex("499602d2"), out _));
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Decimal(10, 2), FromHex("499602d2"), out _));
    }

    [Theory]
    [InlineData("0000c07f")] // float NaN
    [InlineData("0000c0ff")] // a negative NaN
    public void FloatBound_NaN_IsDeclined(string hex)
    {
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Float, FromHex(hex), out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Double, FromHex(hex), out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Double, FromHex("000000000000f87f"), out _));
    }

    [Fact]
    public void Bounds_ForTypesWithoutAValueEncoding_AreDeclined()
    {
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Variant, FromHex("0100"), out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Unknown, FromHex("00"), out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Geometry("srid:4326"), new byte[16], out _));
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Geography("srid:4326", "spherical"), new byte[16], out _));
        Assert.False(SingleValueBinary.TryDecodeBound(new StructType([]), new byte[1], out _));
    }

    [Fact]
    public void StringBound_NotUtf8_IsDeclined()
    {
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.String, FromHex("61ff"), out _));
        // A truncated bound is cut at a character, so it stays UTF-8.
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.String, FromHex("636166c3a9"), out var b));
        Assert.Equal(LiteralValue.Of("café"), b);
    }

    [Fact]
    public void FixedBound_MayBeTruncated()
    {
        Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Fixed(4), FromHex("0001"), out var b));
        Assert.Equal(new byte[] { 0, 1 }, b.AsBinary);
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Fixed(4), FromHex("0001020304"), out _));
        Assert.Throws<FormatException>(() => SingleValueBinary.Decode(IcebergType.Fixed(4), FromHex("0001")));
    }

    [Theory]
    [InlineData("int", "0000000000000000")]
    [InlineData("long", "000000")]
    [InlineData("boolean", "")]
    [InlineData("uuid", "00")]
    [InlineData("time", "00000000")]
    [InlineData("date", "0000000000000000")]
    public void Bounds_OfAnUnexpectedLength_AreDeclined(string typeName, string hex) =>
        Assert.False(SingleValueBinary.TryDecodeBound(ParseType(typeName), FromHex(hex), out _));

    [Fact]
    public void TimeBound_OutsideADay_IsDeclined()
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, 86_400_000_000L);
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Time, bytes, out _));
        Assert.Throws<FormatException>(() => SingleValueBinary.Decode(IcebergType.Time, bytes));
    }

    [Fact]
    public void MicrosTimestampBound_OutsideDateTimeOffset_IsDeclined()
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, long.MaxValue);
        Assert.False(SingleValueBinary.TryDecodeBound(IcebergType.Timestamp, bytes, out _));
        Assert.Throws<NotSupportedException>(() => SingleValueBinary.Decode(IcebergType.Timestamp, bytes));
    }

    // ── Strict decoding ──

    [Theory]
    [InlineData("long", "ffffffff")]       // a promoted bound is not a long value
    [InlineData("double", "cdcccc3d")]
    [InlineData("timestamp", "4e440000")]
    [InlineData("decimal(9, 2)", "")]
    [InlineData("string", "c3")]           // cut inside a character
    public void Decode_IsStrict(string typeName, string hex) =>
        Assert.Throws<FormatException>(() => SingleValueBinary.Decode(ParseType(typeName), FromHex(hex)));

    [Fact]
    public void Decode_RefusesTypesWithoutABinaryValue()
    {
        Assert.Throws<NotSupportedException>(() => SingleValueBinary.Decode(IcebergType.Variant, [1]));
        Assert.Throws<NotSupportedException>(() => SingleValueBinary.Decode(IcebergType.Unknown, [1]));
        Assert.Throws<NotSupportedException>(() => SingleValueBinary.Decode(new ListType(2, IcebergType.Int, true), [1]));
    }

    [Fact]
    public void Decode_GeometryIsItsWkb()
    {
        byte[] wkb = FromHex("0101000000000000000000f03f0000000000000040"); // POINT (1 2)
        Assert.Equal(wkb, SingleValueBinary.Decode(IcebergType.Geometry("srid:4326"), wkb).AsBinary);
    }

    [Fact]
    public void Uuid_IsBigEndian()
    {
        var g = new Guid("f79c3e09-677c-4bbd-a479-3f349cb785e7");
        Assert.Equal("f79c3e09677c4bbda4793f349cb785e7", ToHex(SingleValueBinary.Encode(IcebergType.Uuid, LiteralValue.Of(g))));
        Assert.Equal(g, SingleValueBinary.Decode(IcebergType.Uuid, FromHex("f79c3e09677c4bbda4793f349cb785e7")).AsGuid);
    }

    /// <summary>Unsigned byte order of the RFC 4122 bytes, which is the order Guid compares in.</summary>
    [Fact]
    public void Uuid_ComparesInByteOrder()
    {
        var low = SingleValueBinary.Decode(IcebergType.Uuid, FromHex("7fffffffffffffffffffffffffffffff"));
        var high = SingleValueBinary.Decode(IcebergType.Uuid, FromHex("80000000000000000000000000000000"));
        Assert.True(low < high);
        var a = SingleValueBinary.Decode(IcebergType.Uuid, FromHex("00000000000000007fffffffffffffff"));
        var b = SingleValueBinary.Decode(IcebergType.Uuid, FromHex("00000000000000008000000000000000"));
        Assert.True(a < b);
    }

    // ── Encoding: accepted kinds ──

    [Fact]
    public void Encode_WidensAndRescalesExactly()
    {
        Assert.Equal("2200000000000000", ToHex(SingleValueBinary.Encode(IcebergType.Long, LiteralValue.Of(34))));
        Assert.Equal("058c", ToHex(SingleValueBinary.Encode(IcebergType.Decimal(9, 2), LiteralValue.Of(14.2m))));
        Assert.Equal("058c", ToHex(SingleValueBinary.Encode(IcebergType.Decimal(9, 2), LiteralValue.HighPrecisionDecimalOf(14200, 3))));
        Assert.Equal("64", ToHex(SingleValueBinary.Encode(IcebergType.Decimal(9, 2), LiteralValue.Of(1))));
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Decimal(9, 2), LiteralValue.Of(14.205m)));
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Decimal(3, 2), LiteralValue.Of(14.2m)));
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Int, LiteralValue.Of(34L)));
    }

    [Fact]
    public void Encode_Timestamps()
    {
        var instant = Epoch.AddTicks(15_108_714_681_234_560L);
        Assert.Equal("40a5282d215e0500", ToHex(SingleValueBinary.Encode(IcebergType.Timestamp, LiteralValue.Of(instant))));
        Assert.Equal("40a5282d215e0500", ToHex(SingleValueBinary.Encode(IcebergType.Timestamptz,
            LiteralValue.TimestampNanosOf(1_510_871_468_123_456_000L))));
        Assert.Equal("00b8695f98b1f714", ToHex(SingleValueBinary.Encode(IcebergType.TimestampNs,
            LiteralValue.Of(Epoch.AddTicks(15_108_714_680_000_000L)))));

        // A sub-microsecond instant is not a microsecond timestamp.
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Timestamp, LiteralValue.Of(Epoch.AddTicks(1))));
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Timestamp, LiteralValue.TimestampNanosOf(1)));
        // Outside a nanosecond timestamp's years.
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.TimestampNs,
            LiteralValue.Of(new DateTimeOffset(2300, 1, 1, 0, 0, 0, TimeSpan.Zero))));
    }

    [Fact]
    public void Encode_RefusesNullAndTheWrongFixedLength()
    {
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Int, LiteralValue.Null));
        Assert.Throws<ArgumentException>(() => SingleValueBinary.Encode(IcebergType.Fixed(4), LiteralValue.Of(new byte[3])));
    }

    /// <summary>Encoded and decoded, a value of every type comes back as itself.</summary>
    [Theory]
    [MemberData(nameof(RoundTripValues))]
    public void RoundTrips(string typeName, object value)
    {
        var type = ParseType(typeName);
        var literal = (LiteralValue)value;
        var expected = IcebergLiterals.Coerce(type, literal);
        Assert.Equal(expected, SingleValueBinary.Decode(type, SingleValueBinary.Encode(type, literal)));
        Assert.True(SingleValueBinary.TryDecodeBound(type, SingleValueBinary.Encode(type, literal), out var bound)
            || (type is FloatType or DoubleType));
        if (type is not (FloatType or DoubleType))
            Assert.Equal(expected, bound);
    }

    public static TheoryData<string, object> RoundTripValues()
    {
        var data = new TheoryData<string, object>
        {
            { "boolean", LiteralValue.Of(true) },
            { "int", LiteralValue.Of(int.MinValue) },
            { "long", LiteralValue.Of(long.MaxValue) },
            { "float", LiteralValue.Of(-0.0f) },
            { "double", LiteralValue.Of(double.Epsilon) },
            { "decimal(38, 10)", LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("-99999999999999999999999999999999999999"), 10) },
            { "decimal(1, 0)", LiteralValue.HighPrecisionDecimalOf(-9, 0) },
            { "timestamp", LiteralValue.Of(DateTimeOffset.MinValue) },
            { "timestamptz", LiteralValue.Of(new DateTimeOffset(9999, 12, 31, 23, 59, 59, TimeSpan.Zero).AddTicks(9_999_990)) },
            { "timestamp_ns", LiteralValue.TimestampNanosOf(long.MinValue) },
            { "timestamptz_ns", LiteralValue.TimestampNanosOf(-1) },
            { "string", LiteralValue.Of("🧊") },
            { "uuid", LiteralValue.Of(Guid.NewGuid()) },
            { "fixed[3]", LiteralValue.Of(new byte[] { 1, 2, 3 }) },
            { "binary", LiteralValue.Of(Array.Empty<byte>()) },
        };
#if NET6_0_OR_GREATER
        data.Add("date", LiteralValue.Of(new DateOnly(1, 1, 1)));
        data.Add("time", LiteralValue.Of(new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(9_999_990))));
#else
        data.Add("date", LiteralValue.Of(new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        data.Add("time", LiteralValue.Of(86_399_999_999L));
#endif
        return data;
    }
}
