// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Tests.Parquet.Schema;

/// <summary>
/// Which annotations the reader honours (parquet-format 2.14, GH-606): an unrecognized logical type,
/// or one the spec does not allow on the column's physical type, is dropped and the column read as its
/// physical type.
/// </summary>
public class SchemaAnnotationsTests
{
    private static SchemaElement Leaf(PhysicalType type, LogicalType? logical = null,
        ConvertedType? converted = null, int? typeLength = null, int? precision = null, int? scale = null) => new()
    {
        Name = "c",
        Type = type,
        TypeLength = typeLength,
        RepetitionType = FieldRepetitionType.Optional,
        LogicalType = logical,
        ConvertedType = converted,
        Precision = precision,
        Scale = scale,
    };

    public static TheoryData<string, SchemaElement, bool> LogicalCases() => new()
    {
        { "STRING on BYTE_ARRAY", Leaf(PhysicalType.ByteArray, new LogicalType.StringType()), true },
        { "STRING on INT32", Leaf(PhysicalType.Int32, new LogicalType.StringType()), false },
        { "JSON on FLBA", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.JsonType(), typeLength: 4), false },
        { "UUID on FLBA(16)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.UuidType(), typeLength: 16), true },
        { "UUID on FLBA(8)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.UuidType(), typeLength: 8), false },
        { "UUID on INT32", Leaf(PhysicalType.Int32, new LogicalType.UuidType()), false },
        { "FLOAT16 on FLBA(2)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.Float16Type(), typeLength: 2), true },
        { "FLOAT16 on FLOAT", Leaf(PhysicalType.Float, new LogicalType.Float16Type()), false },
        { "DATE on INT32", Leaf(PhysicalType.Int32, new LogicalType.DateType()), true },
        { "DATE on INT64", Leaf(PhysicalType.Int64, new LogicalType.DateType()), false },
        { "INT(16) on INT32", Leaf(PhysicalType.Int32, new LogicalType.IntType(16, true)), true },
        { "INT(64) on INT32", Leaf(PhysicalType.Int32, new LogicalType.IntType(64, true)), false },
        { "INT(32) on INT64", Leaf(PhysicalType.Int64, new LogicalType.IntType(32, false)), false },
        { "INT(12) on INT32", Leaf(PhysicalType.Int32, new LogicalType.IntType(12, true)), false },
        { "TIME(MILLIS) on INT32", Leaf(PhysicalType.Int32, new LogicalType.TimeType(true, TimeUnit.Millis)), true },
        { "TIME(MICROS) on INT32", Leaf(PhysicalType.Int32, new LogicalType.TimeType(true, TimeUnit.Micros)), false },
        { "TIME(NANOS) on INT64", Leaf(PhysicalType.Int64, new LogicalType.TimeType(false, TimeUnit.Nanos)), true },
        { "TIMESTAMP on INT64", Leaf(PhysicalType.Int64, new LogicalType.TimestampType(true, TimeUnit.Micros)), true },
        { "TIMESTAMP on INT32", Leaf(PhysicalType.Int32, new LogicalType.TimestampType(true, TimeUnit.Millis)), false },
        { "TIMESTAMP on FLBA(12), the extended carrier", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.TimestampType(false, TimeUnit.Nanos), typeLength: 12), true },
        { "TIMESTAMP on INT96", Leaf(PhysicalType.Int96, new LogicalType.TimestampType(false, TimeUnit.Nanos)), false },
        { "DECIMAL(9,2) on INT32", Leaf(PhysicalType.Int32, new LogicalType.DecimalType(2, 9)), true },
        { "DECIMAL(10,2) on INT32", Leaf(PhysicalType.Int32, new LogicalType.DecimalType(2, 10)), false },
        { "DECIMAL(18,0) on INT64", Leaf(PhysicalType.Int64, new LogicalType.DecimalType(0, 18)), true },
        { "DECIMAL(19,0) on INT64", Leaf(PhysicalType.Int64, new LogicalType.DecimalType(0, 19)), false },
        { "DECIMAL(38,10) on FLBA(16)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.DecimalType(10, 38), typeLength: 16), true },
        { "DECIMAL(39,10) on FLBA(16)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.DecimalType(10, 39), typeLength: 16), false },
        { "DECIMAL(9,0) on FLBA(4)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.DecimalType(0, 9), typeLength: 4), true },
        { "DECIMAL(10,0) on FLBA(4)", Leaf(PhysicalType.FixedLenByteArray, new LogicalType.DecimalType(0, 10), typeLength: 4), false },
        { "DECIMAL(100,5) on BYTE_ARRAY", Leaf(PhysicalType.ByteArray, new LogicalType.DecimalType(5, 100)), true },
        { "DECIMAL with scale > precision", Leaf(PhysicalType.Int32, new LogicalType.DecimalType(2, 1)), false },
        { "DECIMAL with precision 0", Leaf(PhysicalType.Int32, new LogicalType.DecimalType(0, 0)), false },
        { "DECIMAL on DOUBLE", Leaf(PhysicalType.Double, new LogicalType.DecimalType(2, 9)), false },
        { "UNKNOWN (null) on INT32", Leaf(PhysicalType.Int32, new LogicalType.NullType()), true },
        { "LIST on a leaf", Leaf(PhysicalType.Int32, new LogicalType.ListType()), false },
        { "VARIANT on a leaf", Leaf(PhysicalType.ByteArray, new LogicalType.VariantType()), false },
        { "unrecognized", Leaf(PhysicalType.ByteArray, new LogicalType.UnknownLogicalType(99)), false },
    };

    [Theory]
    [MemberData(nameof(LogicalCases))]
    public void LogicalType_IsHonouredOnlyWhereTheSpecAllowsIt(string name, SchemaElement element, bool valid)
    {
        _ = name;
        var honoured = SchemaAnnotations.Honoured(element, out bool ignored);

        Assert.Equal(valid, !ignored);
        Assert.Equal(valid ? element.LogicalType : null, honoured.LogicalType);
        Assert.Equal(element.Type, honoured.Type);
        Assert.Equal(element.TypeLength, honoured.TypeLength);
    }

    public static TheoryData<string, SchemaElement, bool> ConvertedCases() => new()
    {
        { "UTF8 on BYTE_ARRAY", Leaf(PhysicalType.ByteArray, converted: ConvertedType.Utf8), true },
        { "UTF8 on INT32", Leaf(PhysicalType.Int32, converted: ConvertedType.Utf8), false },
        { "UINT_64 on INT64", Leaf(PhysicalType.Int64, converted: ConvertedType.Uint64), true },
        { "UINT_64 on INT32", Leaf(PhysicalType.Int32, converted: ConvertedType.Uint64), false },
        { "TIMESTAMP_MILLIS on INT64", Leaf(PhysicalType.Int64, converted: ConvertedType.TimestampMillis), true },
        { "TIME_MICROS on INT32", Leaf(PhysicalType.Int32, converted: ConvertedType.TimeMicros), false },
        { "INTERVAL on FLBA(12)", Leaf(PhysicalType.FixedLenByteArray, converted: ConvertedType.Interval, typeLength: 12), true },
        { "INTERVAL on FLBA(16)", Leaf(PhysicalType.FixedLenByteArray, converted: ConvertedType.Interval, typeLength: 16), false },
        { "DECIMAL(9,2) on INT32", Leaf(PhysicalType.Int32, converted: ConvertedType.Decimal, precision: 9, scale: 2), true },
        { "DECIMAL without precision", Leaf(PhysicalType.Int32, converted: ConvertedType.Decimal, scale: 2), false },
        { "LIST on a leaf", Leaf(PhysicalType.Int32, converted: ConvertedType.List), false },
    };

    /// <summary>A file with only the legacy converted_type is held to the same rules.</summary>
    [Theory]
    [MemberData(nameof(ConvertedCases))]
    public void ConvertedType_AloneIsHeldToTheSameRules(string name, SchemaElement element, bool valid)
    {
        _ = name;
        var honoured = SchemaAnnotations.Honoured(element, out bool ignored);

        Assert.Equal(valid, !ignored);
        Assert.Equal(valid ? element.ConvertedType : null, honoured.ConvertedType);
    }

    /// <summary>
    /// When the logical type is dropped the converted type goes with it, even a valid one: a file that
    /// sets logical_type is read by it alone, as in arrow C++.
    /// </summary>
    [Fact]
    public void DroppedLogicalType_TakesTheConvertedTypeWithIt()
    {
        var element = Leaf(PhysicalType.ByteArray, new LogicalType.UnknownLogicalType(99), ConvertedType.Utf8);

        var honoured = SchemaAnnotations.Honoured(element, out bool ignored);

        Assert.True(ignored);
        Assert.Null(honoured.LogicalType);
        Assert.Null(honoured.ConvertedType);
    }

    /// <summary>
    /// An invalid converted type beside a valid logical type is dropped, but the column's order still
    /// comes from the logical type, so its statistics stay usable.
    /// </summary>
    [Fact]
    public void InvalidConvertedType_BesideAValidLogicalType_KeepsTheStatistics()
    {
        var element = Leaf(PhysicalType.Int32, new LogicalType.DateType(), ConvertedType.Utf8);

        var honoured = SchemaAnnotations.Honoured(element, out bool ignored);

        Assert.False(ignored);
        Assert.IsType<LogicalType.DateType>(honoured.LogicalType);
        Assert.Null(honoured.ConvertedType);
    }

    [Fact]
    public void ValidElement_IsReturnedAsIs()
    {
        var element = Leaf(PhysicalType.ByteArray, new LogicalType.StringType(), ConvertedType.Utf8);

        Assert.Same(element, SchemaAnnotations.Honoured(element, out bool ignored));
        Assert.False(ignored);
    }

    /// <summary>
    /// A TIME or TIMESTAMP whose unit is one this reader does not know (a unit added after it) is an
    /// unrecognized annotation: it used to decode as MILLIS, reading the values in the wrong unit and
    /// keeping their statistics. Decoded here from footer bytes, since no writer of ours can produce it.
    /// The converted type beside it goes too.
    /// </summary>
    [Theory]
    [InlineData(8, (short)4, PhysicalType.Int64)]   // TIMESTAMP, unit member 4
    [InlineData(7, (short)4, PhysicalType.Int64)]   // TIME, unit member 4
    [InlineData(8, (short)0, PhysicalType.Int64)]   // TIMESTAMP, no unit at all (unit is required)
    public void TimeUnitThisReaderDoesNotKnow_DropsTheAnnotation(short logicalField, short unitField, PhysicalType physical)
    {
        var metadata = MetadataDecoder.DecodeFileMetaData(FooterWithTimeUnit(logicalField, unitField, physical));
        var raw = metadata.Schema[1];
        Assert.Equal(new LogicalType.UnknownLogicalType(logicalField), raw.LogicalType);

        var column = new SchemaDescriptor(metadata.Schema).Columns[0];
        Assert.Null(column.SchemaElement.LogicalType);
        Assert.Null(column.SchemaElement.ConvertedType);
        Assert.True(column.SchemaNode.AnnotationIgnored);
    }

    [Theory]
    [InlineData((short)1, TimeUnit.Millis)]
    [InlineData((short)2, TimeUnit.Micros)]
    [InlineData((short)3, TimeUnit.Nanos)]
    public void TimeUnitThisReaderKnows_DecodesAsBefore(short unitField, TimeUnit unit)
    {
        var metadata = MetadataDecoder.DecodeFileMetaData(FooterWithTimeUnit(8, unitField, PhysicalType.Int64));

        Assert.Equal(new LogicalType.TimestampType(true, unit), metadata.Schema[1].LogicalType);
        Assert.False(new SchemaDescriptor(metadata.Schema).Columns[0].SchemaNode.AnnotationIgnored);
    }

    /// <summary>
    /// A footer with one optional leaf of <paramref name="physical"/> type, converted type
    /// TIMESTAMP_MICROS, and logical type <paramref name="logicalField"/> (7 TIME, 8 TIMESTAMP) whose
    /// TimeUnit union sets member <paramref name="unitField"/>, or nothing when it is 0.
    /// </summary>
    private static byte[] FooterWithTimeUnit(short logicalField, short unitField, PhysicalType physical)
    {
        var w = new ThriftCompactWriter();
        w.PushStruct();
        w.WriteFieldHeader(ThriftType.I32, 1); // version
        w.WriteZigZagInt32(1);
        w.WriteFieldHeader(ThriftType.List, 2); // schema
        w.WriteListHeader(ThriftType.Struct, 2);

        w.PushStruct(); // root
        w.WriteFieldHeader(ThriftType.Binary, 4);
        w.WriteString("schema");
        w.WriteFieldHeader(ThriftType.I32, 5); // num_children
        w.WriteZigZagInt32(1);
        w.WriteStructStop();
        w.PopStruct();

        w.PushStruct(); // the leaf
        w.WriteFieldHeader(ThriftType.I32, 1);
        w.WriteZigZagInt32((int)physical);
        w.WriteFieldHeader(ThriftType.I32, 3);
        w.WriteZigZagInt32((int)FieldRepetitionType.Optional);
        w.WriteFieldHeader(ThriftType.Binary, 4);
        w.WriteString("t");
        w.WriteFieldHeader(ThriftType.I32, 6);
        w.WriteZigZagInt32((int)ConvertedType.TimestampMicros);
        w.WriteFieldHeader(ThriftType.Struct, 10); // logicalType union
        w.PushStruct();
        w.WriteFieldHeader(ThriftType.Struct, logicalField);
        w.PushStruct();
        w.WriteBoolField(1, true); // isAdjustedToUTC
        w.WriteFieldHeader(ThriftType.Struct, 2); // unit: TimeUnit union
        w.PushStruct();
        if (unitField != 0)
        {
            w.WriteFieldHeader(ThriftType.Struct, unitField);
            w.PushStruct();
            w.WriteStructStop();
            w.PopStruct();
        }
        w.WriteStructStop();
        w.PopStruct();
        w.WriteStructStop();
        w.PopStruct();
        w.WriteStructStop();
        w.PopStruct();
        w.WriteStructStop();
        w.PopStruct();

        w.WriteFieldHeader(ThriftType.I64, 3); // num_rows
        w.WriteZigZagInt64(0);
        w.WriteFieldHeader(ThriftType.List, 4); // row_groups
        w.WriteListHeader(ThriftType.Struct, 0);
        w.WriteStructStop();
        w.PopStruct();
        return w.ToArray();
    }

    [Fact]
    public void Groups_TakeOnlyGroupAnnotations()
    {
        SchemaElement Group(LogicalType? logical, ConvertedType? converted = null) => new()
        {
            Name = "g", NumChildren = 1, RepetitionType = FieldRepetitionType.Optional,
            LogicalType = logical, ConvertedType = converted,
        };

        Assert.NotNull(SchemaAnnotations.Honoured(Group(new LogicalType.ListType(), ConvertedType.List), out _).LogicalType);
        Assert.NotNull(SchemaAnnotations.Honoured(Group(new LogicalType.MapType()), out _).LogicalType);
        Assert.NotNull(SchemaAnnotations.Honoured(Group(new LogicalType.VariantType()), out _).LogicalType);
        Assert.Null(SchemaAnnotations.Honoured(Group(new LogicalType.StringType()), out _).LogicalType);
        Assert.Null(SchemaAnnotations.Honoured(Group(null, ConvertedType.Utf8), out _).ConvertedType);
    }
}
