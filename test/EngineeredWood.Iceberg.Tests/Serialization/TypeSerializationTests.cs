// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Serialization;

public class TypeSerializationTests
{
    private static string Serialize<T>(T value) => IcebergJsonSerializer.Serialize(value);
    private static T Deserialize<T>(string json) => IcebergJsonSerializer.Deserialize<T>(json);

    [Theory]
    [InlineData("boolean")]
    [InlineData("int")]
    [InlineData("long")]
    [InlineData("float")]
    [InlineData("double")]
    [InlineData("date")]
    [InlineData("time")]
    [InlineData("timestamp")]
    [InlineData("timestamptz")]
    [InlineData("timestamp_ns")]
    [InlineData("timestamptz_ns")]
    [InlineData("string")]
    [InlineData("uuid")]
    [InlineData("binary")]
    public void PrimitiveType_RoundTrips(string typeString)
    {
        var json = $"\"{typeString}\"";
        var type = Deserialize<IcebergType>(json);
        var serialized = Serialize(type);
        Assert.Equal(json, serialized);
    }

    [Fact]
    public void DecimalType_RoundTrips()
    {
        var type = IcebergType.Decimal(10, 2);
        var json = Serialize(type);
        Assert.Equal("\"decimal(10,2)\"", json);

        var deserialized = Deserialize<IcebergType>(json);
        Assert.Equal(type, deserialized);
    }

    [Fact]
    public void FixedType_RoundTrips()
    {
        var type = IcebergType.Fixed(16);
        var json = Serialize(type);
        Assert.Equal("\"fixed[16]\"", json);

        var deserialized = Deserialize<IcebergType>(json);
        Assert.Equal(type, deserialized);
    }

    [Fact]
    public void StructType_RoundTrips()
    {
        var structType = new StructType([
            new NestedField(1, "id", IcebergType.Long, true),
            new NestedField(2, "name", IcebergType.String, false, "The name"),
        ]);

        var json = Serialize<IcebergType>(structType);
        var deserialized = Deserialize<IcebergType>(json);

        Assert.IsType<StructType>(deserialized);
        var result = (StructType)deserialized;
        Assert.Equal(2, result.Fields.Count);
        Assert.Equal("id", result.Fields[0].Name);
        Assert.True(result.Fields[0].IsRequired);
        Assert.Equal("The name", result.Fields[1].Doc);
    }

    [Fact]
    public void StructType_MatchesSpecFormat()
    {
        var json = """
            {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "data", "required": false, "type": "string", "doc": "user data"}
                ]
            }
            """;

        var type = Deserialize<IcebergType>(json);
        Assert.IsType<StructType>(type);
        var structType = (StructType)type;
        Assert.Equal(2, structType.Fields.Count);
        Assert.Equal(1, structType.Fields[0].Id);
        Assert.Equal(IcebergType.Long, structType.Fields[0].Type);
        Assert.True(structType.Fields[0].IsRequired);
        Assert.Equal("user data", structType.Fields[1].Doc);
    }

    [Fact]
    public void ListType_RoundTrips()
    {
        var json = """{"type":"list","element-id":3,"element":"string","element-required":true}""";
        var type = Deserialize<IcebergType>(json);

        Assert.IsType<ListType>(type);
        var listType = (ListType)type;
        Assert.Equal(3, listType.ElementId);
        Assert.Equal(IcebergType.String, listType.ElementType);
        Assert.True(listType.ElementRequired);

        var serialized = Serialize<IcebergType>(type);
        var roundTripped = Deserialize<IcebergType>(serialized);
        Assert.IsType<ListType>(roundTripped);
        var rt = (ListType)roundTripped;
        Assert.Equal(3, rt.ElementId);
    }

    [Fact]
    public void MapType_RoundTrips()
    {
        var json = """{"type":"map","key-id":4,"key":"string","value-id":5,"value":"long","value-required":false}""";
        var type = Deserialize<IcebergType>(json);

        Assert.IsType<MapType>(type);
        var mapType = (MapType)type;
        Assert.Equal(4, mapType.KeyId);
        Assert.Equal(IcebergType.String, mapType.KeyType);
        Assert.Equal(5, mapType.ValueId);
        Assert.Equal(IcebergType.Long, mapType.ValueType);
        Assert.False(mapType.ValueRequired);
    }

    [Fact]
    public void NestedComplexType_RoundTrips()
    {
        // A struct containing a list of maps
        var json = """
            {
                "type": "struct",
                "fields": [{
                    "id": 1,
                    "name": "tags",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 2,
                        "element": {
                            "type": "map",
                            "key-id": 3,
                            "key": "string",
                            "value-id": 4,
                            "value": "string",
                            "value-required": true
                        },
                        "element-required": true
                    }
                }]
            }
            """;

        var type = Deserialize<IcebergType>(json);
        Assert.IsType<StructType>(type);

        var structType = (StructType)type;
        var field = structType.Fields[0];
        Assert.IsType<ListType>(field.Type);

        var listType = (ListType)field.Type;
        Assert.IsType<MapType>(listType.ElementType);

        var mapType = (MapType)listType.ElementType;
        Assert.Equal(IcebergType.String, mapType.KeyType);
        Assert.Equal(IcebergType.String, mapType.ValueType);
    }
}
