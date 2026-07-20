// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Serialization;

public class SchemaSerializationTests
{
    [Fact]
    public void Schema_RoundTrips()
    {
        var schema = new Schema(0, [
            new NestedField(1, "id", IcebergType.Long, true),
            new NestedField(2, "data", IcebergType.String, false),
        ]);

        var json = IcebergJsonSerializer.Serialize(schema);
        var deserialized = IcebergJsonSerializer.Deserialize<Schema>(json);

        Assert.Equal(0, deserialized.SchemaId);
        Assert.Equal(2, deserialized.Fields.Count);
        Assert.Equal("id", deserialized.Fields[0].Name);
        Assert.Null(deserialized.IdentifierFieldIds);
    }

    [Fact]
    public void Schema_WithIdentifierFieldIds_RoundTrips()
    {
        var schema = new Schema(1,
        [
            new NestedField(1, "id", IcebergType.Long, true),
            new NestedField(2, "name", IcebergType.String, true),
        ],
        IdentifierFieldIds: [1]);

        var json = IcebergJsonSerializer.Serialize(schema);
        var deserialized = IcebergJsonSerializer.Deserialize<Schema>(json);

        Assert.NotNull(deserialized.IdentifierFieldIds);
        Assert.Single(deserialized.IdentifierFieldIds);
        Assert.Equal(1, deserialized.IdentifierFieldIds[0]);
    }

    [Fact]
    public void Schema_MatchesSpecFormat()
    {
        var json = """
            {
                "schema-id": 1,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "data", "required": false, "type": "string"}
                ],
                "identifier-field-ids": [1]
            }
            """;

        var schema = IcebergJsonSerializer.Deserialize<Schema>(json);
        Assert.Equal(1, schema.SchemaId);
        Assert.Equal(2, schema.Fields.Count);
        Assert.NotNull(schema.IdentifierFieldIds);
        Assert.Equal(1, schema.IdentifierFieldIds[0]);
    }

    [Fact]
    public void Schema_WritesTypeStruct()
    {
        var schema = new Schema(0, [
            new NestedField(1, "id", IcebergType.Int, true),
        ]);

        var json = IcebergJsonSerializer.Serialize(schema);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("struct", doc.RootElement.GetProperty("type").GetString());
    }
}
