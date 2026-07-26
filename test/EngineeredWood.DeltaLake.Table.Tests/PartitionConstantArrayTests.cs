// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Partitioning;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Materialising a partition column on read: the value lives only as a string in
/// <c>add.partitionValues</c>, so it is decoded once and repeated across the batch. These cover the decode,
/// which used to route every value through a typed array builder — and therefore through the builder's .NET
/// surface type.
///
/// <para>For decimals that was lossy in both directions, and silently so in one. Both are pinned below with
/// the values measured against the old code.</para>
/// </summary>
public class PartitionConstantArrayTests
{
    /// <summary>
    /// Drives the private materialiser through <c>AddPartitionColumns</c>, the way the read path reaches it:
    /// a one-column data batch plus a partition column the file does not contain.
    /// </summary>
    private static IArrowArray Materialize(IArrowType partitionType, string? value, int rows)
    {
        var dataSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) ids.Append(i);
        var dataBatch = new RecordBatch(dataSchema, [ids.Build()], rows);

        var fullSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("p", partitionType, true))
            .Build();

        var result = PartitionUtils.AddPartitionColumns(
            dataBatch, fullSchema,
            new Dictionary<string, string?> { ["p"] = value }!,
            ["p"]);

        Assert.Equal(2, result.ColumnCount);
        Assert.Equal(rows, result.Length);
        return result.Column(1);
    }

    private static BigInteger Unscaled(Decimal128Array array, int row) =>
        new BigInteger(array.GetBytes(row).ToArray());

    // ── Decimals: the two defects the builder round-trip caused. ──

    /// <summary>
    /// A <c>decimal(38,10)</c> partition value carrying all 38 of its significant digits. The old code parsed
    /// it with <c>decimal.Parse</c>, which does not fail on excess precision — it silently ROUNDS and reports
    /// success. Measured against the old code: this value materialised as
    /// <c>1234567890123456789012345678.1000000000</c>, i.e. unscaled <c>…781000000000</c>, for every row of
    /// the partition. Wrong data, no error, on a read.
    /// </summary>
    [Fact]
    public void HighPrecisionDecimal_KeepsEveryDigit()
    {
        var type = new Decimal128Type(precision: 38, scale: 10);
        const string value = "1234567890123456789012345678.1234567890";

        var column = Assert.IsType<Decimal128Array>(Materialize(type, value, 3));

        var expected = BigInteger.Parse("12345678901234567890123456781234567890");
        var rounded = BigInteger.Parse("12345678901234567890123456781000000000");

        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(expected, Unscaled(column, i));
            Assert.NotEqual(rounded, Unscaled(column, i));
        }

        // The column must also still declare the type it was asked for.
        var actual = Assert.IsType<Decimal128Type>(column.Data.DataType);
        Assert.Equal(38, actual.Precision);
        Assert.Equal(10, actual.Scale);
    }

    /// <summary>
    /// A <c>decimal(38,0)</c> value larger than <see cref="decimal.MaxValue"/> but entirely within
    /// Decimal128's 128-bit storage — a legal Delta value. The old code threw <see cref="OverflowException"/>
    /// out of <c>decimal.Parse</c>, so the table could not be read at all.
    /// </summary>
    [Theory]
    [InlineData("12345678901234567890123456789012345678")]
    [InlineData("99999999999999999999999999999999999999")]
    [InlineData("-99999999999999999999999999999999999999")]
    public void DecimalWiderThanSystemDecimal_ReadsRatherThanOverflowing(string value)
    {
        var type = new Decimal128Type(precision: 38, scale: 0);

        var column = Assert.IsType<Decimal128Array>(Materialize(type, value, 2));

        var expected = BigInteger.Parse(value);
        for (int i = 0; i < 2; i++)
            Assert.Equal(expected, Unscaled(column, i));
    }

    [Fact]
    public void Decimal_ScalesAValueGivenWithFewerFractionalDigits()
    {
        // "123.45" into a scale-10 column is unscaled 123.45 * 10^10.
        var column = Assert.IsType<Decimal128Array>(
            Materialize(new Decimal128Type(38, 10), "123.45", 2));

        Assert.Equal(BigInteger.Parse("1234500000000"), Unscaled(column, 0));
    }

    [Fact]
    public void Decimal_AcceptsTrailingZerosPastTheColumnScale()
    {
        // A writer may pad past the declared scale. The surplus digits are zeros, so the value IS
        // representable and narrowing it is exact — this must not be treated as a loss.
        var column = Assert.IsType<Decimal128Array>(
            Materialize(new Decimal128Type(38, 2), "123.4500", 2));

        Assert.Equal(new BigInteger(12345), Unscaled(column, 0));
    }

    [Fact]
    public void Decimal_RefusesToRoundAwayRealDigits()
    {
        // Here the digits past the column's scale are NOT zeros, so restating the value would change it.
        // Failing is the point: a partition value is an exact identity, not a bound.
        var ex = Assert.Throws<FormatException>(
            () => Materialize(new Decimal128Type(38, 2), "123.456", 2));

        Assert.Contains("fractional digits", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Decimal_NegativeValueSignExtends()
    {
        var column = Assert.IsType<Decimal128Array>(
            Materialize(new Decimal128Type(38, 2), "-1.25", 2));

        Assert.Equal(new BigInteger(-125), Unscaled(column, 0));

        // Sign extension has to fill the high bytes; a zero-filled buffer would read as a huge positive.
        Assert.Equal(0xFF, column.GetBytes(0)[15]);
    }

    // ── Every other partition type still decodes correctly. ──

    [Fact]
    public void Timestamp_KeepsMicrosecondPrecisionAndItsType()
    {
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");

        var column = Assert.IsType<TimestampArray>(
            Materialize(type, "2023-11-14 22:13:20.000123", 3));

        var actual = Assert.IsType<TimestampType>(column.Data.DataType);
        Assert.Equal(TimeUnit.Microsecond, actual.Unit);
        Assert.Equal("UTC", actual.Timezone);

        // The exact instant FormatTimestampPartitionValue encodes from this stored value.
        for (int i = 0; i < 3; i++)
            Assert.Equal(1_700_000_000_000_123L, column.GetValue(i));
    }

    [Fact]
    public void Timestamp_WithoutAFractionDecodesOnTheSecond()
    {
        var column = Assert.IsType<TimestampArray>(
            Materialize(new TimestampType(TimeUnit.Microsecond, (string?)null), "2023-11-14 22:13:20", 2));

        Assert.Equal(1_700_000_000_000_000L, column.GetValue(0));
        Assert.Null(((TimestampType)column.Data.DataType).Timezone); // timestamp_ntz stays ntz
    }

    [Fact]
    public void Date32_DecodesToADayCount()
    {
        var column = Assert.IsType<Date32Array>(Materialize(Date32Type.Default, "2023-11-14", 2));

        int expected = (int)(new DateTime(2023, 11, 14) - new DateTime(1970, 1, 1)).TotalDays;
        Assert.Equal(expected, column.GetValue(0));
        Assert.Equal(new DateTime(2023, 11, 14), column.GetDateTime(0));
    }

    [Theory]
    [InlineData("42", 42L)]
    [InlineData("-42", -42L)]
    [InlineData("9223372036854775807", long.MaxValue)]
    [InlineData("-9223372036854775808", long.MinValue)]
    public void Int64_DecodesIncludingTheExtremes(string value, long expected)
    {
        var column = Assert.IsType<Int64Array>(Materialize(Int64Type.Default, value, 3));
        for (int i = 0; i < 3; i++)
            Assert.Equal(expected, column.GetValue(i));
    }

    [Fact]
    public void NarrowIntegers_DecodeWithTheirOwnWidths()
    {
        Assert.Equal(-2_000_000_000, Assert.IsType<Int32Array>(
            Materialize(Int32Type.Default, "-2000000000", 2)).GetValue(0));
        Assert.Equal((short)-32_768, Assert.IsType<Int16Array>(
            Materialize(Int16Type.Default, "-32768", 2)).GetValue(0));
        Assert.Equal((sbyte)-128, Assert.IsType<Int8Array>(
            Materialize(Int8Type.Default, "-128", 2)).GetValue(0));
    }

    [Fact]
    public void Floats_DecodeExactly()
    {
        Assert.Equal(1.5, Assert.IsType<DoubleArray>(
            Materialize(DoubleType.Default, "1.5", 2)).GetValue(0));
        Assert.Equal(1.5f, Assert.IsType<FloatArray>(
            Materialize(FloatType.Default, "1.5", 2)).GetValue(0));

        // A double that needs all 17 significant digits to round-trip.
        Assert.Equal(0.1234567890123456789, Assert.IsType<DoubleArray>(
            Materialize(DoubleType.Default, "0.1234567890123456789", 2)).GetValue(0));
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Boolean_DecodesAndIsNotNull(string value, bool expected)
    {
        var column = Assert.IsType<BooleanArray>(Materialize(BooleanType.Default, value, 4));

        for (int i = 0; i < 4; i++)
        {
            Assert.False(column.IsNull(i));
            Assert.Equal(expected, column.GetValue(i));
        }
    }

    [Fact]
    public void String_DecodesIncludingNonAscii()
    {
        var column = Assert.IsType<StringArray>(Materialize(StringType.Default, "日本", 3));

        for (int i = 0; i < 3; i++)
            Assert.Equal("日本", column.GetString(i));
    }

    [Fact]
    public void String_EmptyValueStaysAnEmptyString()
    {
        // An empty string is NULL for every other type, but a legitimate value for a string column.
        var column = Assert.IsType<StringArray>(Materialize(StringType.Default, "", 3));

        for (int i = 0; i < 3; i++)
        {
            Assert.False(column.IsNull(i));
            Assert.Equal("", column.GetString(i));
        }
    }

    // ── The NULL shapes, which take the other helper. ──

    public static TheoryData<string, IArrowType> NullableTypes() => new()
    {
        { "int64", Int64Type.Default },
        { "string", StringType.Default },
        { "boolean", BooleanType.Default },
        { "date32", Date32Type.Default },
        { "timestamp", new TimestampType(TimeUnit.Microsecond, "UTC") },
        { "decimal", new Decimal128Type(38, 10) },
    };

    [Theory]
    [MemberData(nameof(NullableTypes))]
    public void NullPartitionValue_MaterializesAllNullAtTheColumnType(string label, IArrowType type)
    {
        Assert.NotNull(label);

        var column = Materialize(type, null, 3);

        Assert.Equal(3, column.Length);
        Assert.Equal(3, column.Data.NullCount);
        Assert.Equal(type.TypeId, column.Data.DataType.TypeId);

        var arr = Assert.IsAssignableFrom<Apache.Arrow.Array>(column);
        for (int i = 0; i < 3; i++)
            Assert.True(arr.IsNull(i), $"row {i} was not null");
    }

    [Theory]
    [MemberData(nameof(NullableTypes))]
    public void HiveDefaultPartitionSentinel_IsNull(string label, IArrowType type)
    {
        Assert.NotNull(label);

        // Some writers put the DIRECTORY sentinel into partitionValues rather than a JSON null.
        var column = Materialize(type, "__HIVE_DEFAULT_PARTITION__", 2);

        Assert.Equal(2, column.Data.NullCount);
        Assert.Equal(type.TypeId, column.Data.DataType.TypeId);
    }

    [Fact]
    public void EmptyStringForANonStringType_IsNull()
    {
        var column = Materialize(Int64Type.Default, "", 2);

        Assert.Equal(2, column.Data.NullCount);
        Assert.IsType<Int64Array>(column);
    }

    [Fact]
    public void ZeroRowBatch_MaterializesAnEmptyColumn()
    {
        var column = Materialize(new Decimal128Type(38, 10), "1.5", 0);

        Assert.Equal(0, column.Length);
        Assert.IsType<Decimal128Array>(column);
    }
}
