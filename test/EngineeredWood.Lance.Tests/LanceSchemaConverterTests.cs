// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Schema;

namespace EngineeredWood.Lance.Tests;

public class LanceSchemaConverterTests
{
    [Theory]
    [InlineData("bool", typeof(BooleanType))]
    [InlineData("int8", typeof(Int8Type))]
    [InlineData("uint8", typeof(UInt8Type))]
    [InlineData("int16", typeof(Int16Type))]
    [InlineData("uint16", typeof(UInt16Type))]
    [InlineData("int32", typeof(Int32Type))]
    [InlineData("uint32", typeof(UInt32Type))]
    [InlineData("int64", typeof(Int64Type))]
    [InlineData("uint64", typeof(UInt64Type))]
    [InlineData("halffloat", typeof(HalfFloatType))]
    [InlineData("float", typeof(FloatType))]
    [InlineData("double", typeof(DoubleType))]
    [InlineData("string", typeof(StringType))]
    [InlineData("large_string", typeof(LargeStringType))]
    [InlineData("binary", typeof(BinaryType))]
    [InlineData("large_binary", typeof(LargeBinaryType))]
    [InlineData("date32:day", typeof(Date32Type))]
    [InlineData("date64:ms", typeof(Date64Type))]
    [InlineData("null", typeof(NullType))]
    public void ParsesPrimitiveLogicalTypes(string logical, Type expectedArrowType)
    {
        var schema = BuildLeafSchema(logical);
        var arrow = LanceSchemaConverter.ToArrowSchema(schema);

        Assert.Single(arrow.FieldsList);
        Assert.IsType(expectedArrowType, arrow.FieldsList[0].DataType);
    }

    [Theory]
    [InlineData("decimal:128:12:4", 128, 12, 4)]
    [InlineData("decimal:256:38:10", 256, 38, 10)]
    public void ParsesDecimal(string logical, int width, int precision, int scale)
    {
        var schema = BuildLeafSchema(logical);
        var arrow = LanceSchemaConverter.ToArrowSchema(schema);

        var type = arrow.FieldsList[0].DataType;
        if (width == 128)
        {
            var d = Assert.IsType<Decimal128Type>(type);
            Assert.Equal(precision, d.Precision);
            Assert.Equal(scale, d.Scale);
        }
        else
        {
            var d = Assert.IsType<Decimal256Type>(type);
            Assert.Equal(precision, d.Precision);
            Assert.Equal(scale, d.Scale);
        }
    }

    [Theory]
    [InlineData("time:s", TimeUnit.Second, typeof(Time32Type))]
    [InlineData("time:ms", TimeUnit.Millisecond, typeof(Time32Type))]
    [InlineData("time:us", TimeUnit.Microsecond, typeof(Time64Type))]
    [InlineData("time:ns", TimeUnit.Nanosecond, typeof(Time64Type))]
    public void ParsesTime(string logical, TimeUnit expectedUnit, Type expectedArrowType)
    {
        var schema = BuildLeafSchema(logical);
        var arrow = LanceSchemaConverter.ToArrowSchema(schema);

        var type = arrow.FieldsList[0].DataType;
        Assert.IsType(expectedArrowType, type);

        TimeUnit actualUnit = type switch
        {
            Time32Type t32 => t32.Unit,
            Time64Type t64 => t64.Unit,
            _ => throw new InvalidOperationException(),
        };
        Assert.Equal(expectedUnit, actualUnit);
    }

    [Theory]
    [InlineData("timestamp:s", TimeUnit.Second)]
    [InlineData("timestamp:ms", TimeUnit.Millisecond)]
    [InlineData("timestamp:us", TimeUnit.Microsecond)]
    [InlineData("timestamp:ns", TimeUnit.Nanosecond)]
    public void ParsesTimestamp(string logical, TimeUnit expectedUnit)
    {
        var schema = BuildLeafSchema(logical);
        var arrow = LanceSchemaConverter.ToArrowSchema(schema);

        var type = Assert.IsType<TimestampType>(arrow.FieldsList[0].DataType);
        Assert.Equal(expectedUnit, type.Unit);
        Assert.Null(type.Timezone);
    }

    [Theory]
    [InlineData("duration:ns", TimeUnit.Nanosecond)]
    [InlineData("duration:us", TimeUnit.Microsecond)]
    public void ParsesDuration(string logical, TimeUnit expectedUnit)
    {
        var schema = BuildLeafSchema(logical);
        var arrow = LanceSchemaConverter.ToArrowSchema(schema);
        var type = Assert.IsType<DurationType>(arrow.FieldsList[0].DataType);
        Assert.Equal(expectedUnit, type.Unit);
    }

    [Fact]
    public void BuildsStructFromParentAndChildren()
    {
        // parent field "s" (id=1) with two int32 children (id=2, id=3)
        var schema = new Proto.Schema();
        schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Parent,
            Name = "s",
            Id = 1,
            ParentId = 0,
            LogicalType = "struct",
            Nullable = true,
        });
        schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
            Name = "a",
            Id = 2,
            ParentId = 1,
            LogicalType = "int32",
            Nullable = false,
        });
        schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
            Name = "b",
            Id = 3,
            ParentId = 1,
            LogicalType = "int64",
            Nullable = true,
        });

        var arrow = LanceSchemaConverter.ToArrowSchema(schema);
        Assert.Single(arrow.FieldsList);

        var s = arrow.FieldsList[0];
        Assert.Equal("s", s.Name);
        var structType = Assert.IsType<StructType>(s.DataType);
        Assert.Equal(2, structType.Fields.Count);
        Assert.IsType<Int32Type>(structType.Fields[0].DataType);
        Assert.False(structType.Fields[0].IsNullable);
        Assert.IsType<Int64Type>(structType.Fields[1].DataType);
        Assert.True(structType.Fields[1].IsNullable);
    }

    [Fact]
    public void BuildsListFromRepeated()
    {
        var schema = new Proto.Schema();
        schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Repeated,
            Name = "xs",
            Id = 1,
            ParentId = 0,
            LogicalType = "list",
            Nullable = true,
        });
        schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
            Name = "item",
            Id = 2,
            ParentId = 1,
            LogicalType = "int32",
            Nullable = true,
        });

        var arrow = LanceSchemaConverter.ToArrowSchema(schema);
        var listType = Assert.IsType<ListType>(arrow.FieldsList[0].DataType);
        Assert.IsType<Int32Type>(listType.ValueDataType);
    }

    [Fact]
    public void UnsupportedLogicalType_Throws()
    {
        var schema = BuildLeafSchema("ipaddr");
        Assert.Throws<LanceFormatException>(() =>
            LanceSchemaConverter.ToArrowSchema(schema));
    }

    private static Proto.Schema BuildLeafSchema(string logicalType)
    {
        var schema = new Proto.Schema();
        schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
            Name = "f",
            Id = 1,
            ParentId = 0,
            LogicalType = logicalType,
            Nullable = true,
        });
        return schema;
    }
}
