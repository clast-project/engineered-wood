// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests.Schema;

public class AvroSchemaParserTests
{
    [Fact]
    public void Parse_PrimitiveString_ReturnsCorrectType()
    {
        var schema = AvroSchemaParser.Parse("\"int\"");
        Assert.Equal(AvroType.Int, schema.Type);
    }

    [Theory]
    [InlineData("\"null\"", "Null")]
    [InlineData("\"boolean\"", "Boolean")]
    [InlineData("\"int\"", "Int")]
    [InlineData("\"long\"", "Long")]
    [InlineData("\"float\"", "Float")]
    [InlineData("\"double\"", "Double")]
    [InlineData("\"bytes\"", "Bytes")]
    [InlineData("\"string\"", "String")]
    public void Parse_AllPrimitives(string json, string expectedType)
    {
        var expected = (AvroType)Enum.Parse(typeof(AvroType), expectedType);
        var schema = AvroSchemaParser.Parse(json);
        Assert.Equal(expected, schema.Type);
    }

    [Fact]
    public void Parse_SimpleRecord()
    {
        var json = """
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
        """;

        var schema = AvroSchemaParser.Parse(json);
        var record = Assert.IsType<AvroRecordSchema>(schema);
        Assert.Equal("User", record.Name);
        Assert.Equal(2, record.Fields.Count);
        Assert.Equal("id", record.Fields[0].Name);
        Assert.Equal(AvroType.Long, record.Fields[0].Schema.Type);
        Assert.Equal("name", record.Fields[1].Name);
        Assert.Equal(AvroType.String, record.Fields[1].Schema.Type);
    }

    [Fact]
    public void Parse_RecordWithNamespace()
    {
        var json = """
        {
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "int"}
            ]
        }
        """;

        var record = Assert.IsType<AvroRecordSchema>(AvroSchemaParser.Parse(json));
        Assert.Equal("com.example", record.Namespace);
        Assert.Equal("com.example.User", record.FullName);
    }

    [Fact]
    public void Parse_NullableUnion()
    {
        var json = """["null", "string"]""";
        var schema = AvroSchemaParser.Parse(json);
        var union = Assert.IsType<AvroUnionSchema>(schema);
        Assert.True(union.IsNullable(out var inner, out int nullIndex));
        Assert.Equal(0, nullIndex);
        Assert.Equal(AvroType.String, inner.Type);
    }

    [Fact]
    public void Parse_RecordWithNullableField()
    {
        var json = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["null", "int"], "default": null}
            ]
        }
        """;

        var record = Assert.IsType<AvroRecordSchema>(AvroSchemaParser.Parse(json));
        var field = record.Fields[0];
        var union = Assert.IsType<AvroUnionSchema>(field.Schema);
        Assert.True(union.IsNullable(out var inner, out _));
        Assert.Equal(AvroType.Int, inner.Type);
    }

    [Fact]
    public void Parse_LogicalType_Date()
    {
        var json = """{"type": "int", "logicalType": "date"}""";
        var schema = AvroSchemaParser.Parse(json);
        Assert.Equal(AvroType.Int, schema.Type);
        Assert.Equal("date", schema.LogicalType);
    }

    [Fact]
    public void Parse_Enum()
    {
        var json = """
        {
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"]
        }
        """;

        var schema = AvroSchemaParser.Parse(json);
        var enumSchema = Assert.IsType<AvroEnumSchema>(schema);
        Assert.Equal("Color", enumSchema.Name);
        Assert.Equal(3, enumSchema.Symbols.Count);
        Assert.Equal("GREEN", enumSchema.Symbols[1]);
    }

    [Fact]
    public void Parse_Array()
    {
        var json = """{"type": "array", "items": "int"}""";
        var schema = AvroSchemaParser.Parse(json);
        var array = Assert.IsType<AvroArraySchema>(schema);
        Assert.Equal(AvroType.Int, array.Items.Type);
    }

    [Fact]
    public void Parse_Map()
    {
        var json = """{"type": "map", "values": "long"}""";
        var schema = AvroSchemaParser.Parse(json);
        var map = Assert.IsType<AvroMapSchema>(schema);
        Assert.Equal(AvroType.Long, map.Values.Type);
    }

    [Fact]
    public void Parse_Fixed()
    {
        var json = """{"type": "fixed", "name": "Hash", "size": 16}""";
        var schema = AvroSchemaParser.Parse(json);
        var fixedSchema = Assert.IsType<AvroFixedSchema>(schema);
        Assert.Equal("Hash", fixedSchema.Name);
        Assert.Equal(16, fixedSchema.Size);
    }

    [Fact]
    public void Parse_NestedRecord()
    {
        var json = """
        {
            "type": "record",
            "name": "Outer",
            "fields": [
                {
                    "name": "inner",
                    "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [
                            {"name": "value", "type": "int"}
                        ]
                    }
                }
            ]
        }
        """;

        var record = Assert.IsType<AvroRecordSchema>(AvroSchemaParser.Parse(json));
        var innerRecord = Assert.IsType<AvroRecordSchema>(record.Fields[0].Schema);
        Assert.Equal("Inner", innerRecord.Name);
        Assert.Single(innerRecord.Fields);
    }

    [Fact]
    public void Parse_FieldAliases()
    {
        var json = """
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "new_name", "type": "string", "aliases": ["old_name"]}
            ]
        }
        """;

        var record = Assert.IsType<AvroRecordSchema>(AvroSchemaParser.Parse(json));
        Assert.Equal(["old_name"], record.Fields[0].Aliases);
    }
}
