// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using ParquetTimeUnit = EngineeredWood.Parquet.Metadata.TimeUnit;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The TIMESTAMP annotation was mapped to an Arrow <see cref="TimestampType"/> without ever looking at
/// the column's physical type. That is fine while INT64 is the only carrier the spec allows, and stops
/// being fine the moment a file arrives carrying TIMESTAMP on something else: the read path maps
/// <c>Int64Type or TimestampType or Time64Type</c> onto a <c>long</c> value buffer, so a 12-byte column
/// was reinterpreted eight bytes at a time and decoded plausible-looking wrong dates instead of failing.
///
/// parquet-format is in the middle of allowing exactly that (apache/parquet-format#601 puts TIMESTAMP on
/// FIXED_LEN_BYTE_ARRAY(12)), so files in this shape are about to exist. These tests pin the gate: an
/// unrecognised carrier falls through to the physical type, which is lossless, rather than being decoded
/// as a timestamp. FLBA(12) is now a real carrier and decodes (see ExtendedTimestampReadTests); every
/// other width, and every other physical type, still falls through.
/// </summary>
public class TimestampCarrierGateTests
{
    private static ColumnDescriptor Describe(
        PhysicalType physicalType,
        LogicalType? logicalType = null,
        ConvertedType? convertedType = null,
        int? typeLength = null)
    {
        var element = new SchemaElement
        {
            Name = "ts",
            Type = physicalType,
            TypeLength = typeLength,
            RepetitionType = FieldRepetitionType.Optional,
            LogicalType = logicalType,
            ConvertedType = convertedType,
        };

        return new ColumnDescriptor
        {
            Path = ["ts"],
            PhysicalType = physicalType,
            TypeLength = typeLength,
            MaxDefinitionLevel = 1,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = new SchemaNode { Element = element, Children = [] },
        };
    }

    public static TheoryData<ParquetTimeUnit, TimeUnit> TimestampUnits => new()
    {
        { ParquetTimeUnit.Millis, TimeUnit.Millisecond },
        { ParquetTimeUnit.Micros, TimeUnit.Microsecond },
        { ParquetTimeUnit.Nanos, TimeUnit.Nanosecond },
    };

    [Theory]
    [MemberData(nameof(TimestampUnits))]
    public void Int64IsStillDecodedAsATimestamp(ParquetTimeUnit parquetUnit, TimeUnit arrowUnit)
    {
        var column = Describe(PhysicalType.Int64, new LogicalType.TimestampType(true, parquetUnit));

        var type = Assert.IsType<TimestampType>(ArrowSchemaConverter.ToArrowType(column));
        Assert.Equal(arrowUnit, type.Unit);
        Assert.Equal("UTC", type.Timezone);
    }

    [Fact]
    public void ANonUtcInt64TimestampIsStillNaive()
    {
        var column = Describe(
            PhysicalType.Int64,
            new LogicalType.TimestampType(false, ParquetTimeUnit.Micros));

        var type = Assert.IsType<TimestampType>(ArrowSchemaConverter.ToArrowType(column));
        Assert.Null(type.Timezone);
    }

    [Theory]
    [MemberData(nameof(TimestampUnits))]
    public void FixedLenByteArrayAtTwelveBytesIsTheExtendedCarrier(ParquetTimeUnit parquetUnit, TimeUnit arrowUnit)
    {
        // The one width the annotation is legal at. Decoding lives in ExtendedTimestampReadTests; what
        // matters here is that the gate lets exactly this shape past and nothing else.
        var column = Describe(
            PhysicalType.FixedLenByteArray,
            new LogicalType.TimestampType(true, parquetUnit),
            typeLength: 12);

        var type = Assert.IsType<TimestampType>(ArrowSchemaConverter.ToArrowType(column));
        Assert.Equal(arrowUnit, type.Unit);
        Assert.Equal("UTC", type.Timezone);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(11)]
    [InlineData(13)]
    [InlineData(16)]
    public void FixedLenByteArrayAtAnyOtherWidthFallsThroughToItsPhysicalType(int typeLength)
    {
        // TIMESTAMP is a carrier at twelve bytes and malformed at every other width -- including 8, where
        // the width is coincidentally right for an int64 and must not be what saves us.
        var column = Describe(
            PhysicalType.FixedLenByteArray,
            new LogicalType.TimestampType(true, ParquetTimeUnit.Micros),
            typeLength: typeLength);

        var type = Assert.IsType<FixedSizeBinaryType>(ArrowSchemaConverter.ToArrowType(column));
        Assert.Equal(typeLength, type.ByteWidth);
    }

    [Fact]
    public void ByteArrayCarryingTimestampFallsThroughToBinary()
    {
        var column = Describe(
            PhysicalType.ByteArray,
            new LogicalType.TimestampType(true, ParquetTimeUnit.Micros));

        Assert.IsType<BinaryType>(ArrowSchemaConverter.ToArrowType(column));
    }

    [Theory]
    [InlineData(ConvertedType.TimestampMillis, TimeUnit.Millisecond)]
    [InlineData(ConvertedType.TimestampMicros, TimeUnit.Microsecond)]
    public void Int64ConvertedTimestampsAreStillDecoded(ConvertedType converted, TimeUnit arrowUnit)
    {
        var column = Describe(PhysicalType.Int64, convertedType: converted);

        var type = Assert.IsType<TimestampType>(ArrowSchemaConverter.ToArrowType(column));
        Assert.Equal(arrowUnit, type.Unit);
        Assert.Equal("UTC", type.Timezone);
    }

    [Theory]
    [InlineData(ConvertedType.TimestampMillis)]
    [InlineData(ConvertedType.TimestampMicros)]
    public void ConvertedTimestampsOnTheWrongCarrierFallThrough(ConvertedType converted)
    {
        // TIMESTAMP_MILLIS / TIMESTAMP_MICROS are INT64-only converted types, and the FLBA(12) proposal
        // deliberately does NOT give the new carrier one -- parquet-java suppresses converted_type for it
        // precisely so a converted-type-only reader cannot misparse the column. A file carrying one
        // anyway is malformed, and the same reinterpretation bug applies.
        var column = Describe(PhysicalType.FixedLenByteArray, convertedType: converted, typeLength: 12);

        var type = Assert.IsType<FixedSizeBinaryType>(ArrowSchemaConverter.ToArrowType(column));
        Assert.Equal(12, type.ByteWidth);
    }

    [Fact]
    public void AnInt64TimestampStillEmitsTheDeprecatedBounds()
    {
        // The deprecated Statistics.min/max may only carry values whose SIGNED ordering is their logical
        // ordering. StatisticsCollector compares INT64 columns with a typed comparator, so it does.
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(
            new TimestampType(TimeUnit.Nanosecond, "UTC"), PhysicalType.Int64));
    }

    [Fact]
    public void AFixedLenByteArrayTimestampDoesNotEmitTheDeprecatedBounds()
    {
        // Latent until an Arrow TimestampType can map to FLBA, which is what the FLBA(12) writer will do.
        // StatisticsCollector compares every FLBA column with SequenceCompareTo -- unsigned lexicographic
        // -- which is not the signed order these fields promise. A wrong bound in the footer is a wrong
        // prune, so the deprecated pair has to be dropped rather than filled in from the wrong comparator.
        Assert.False(ColumnChunkWriter.SignedOrderMatchesLogical(
            new TimestampType(TimeUnit.Nanosecond, "UTC"), PhysicalType.FixedLenByteArray));
    }

    [Fact]
    public void TheOtherSignedTemporalTypesAreUnaffected()
    {
        // Date/Time/Duration only ever arrive on INT32/INT64, so narrowing the timestamp answer must not
        // have narrowed theirs.
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(Date32Type.Default, PhysicalType.Int32));
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(Date64Type.Default, PhysicalType.Int64));
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(
            new Time32Type(TimeUnit.Millisecond), PhysicalType.Int32));
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(
            new Time64Type(TimeUnit.Microsecond), PhysicalType.Int64));
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(Int64Type.Default, PhysicalType.Int64));
        Assert.True(ColumnChunkWriter.SignedOrderMatchesLogical(DoubleType.Default, PhysicalType.Double));
        Assert.False(ColumnChunkWriter.SignedOrderMatchesLogical(
            StringType.Default, PhysicalType.ByteArray));
    }
}
