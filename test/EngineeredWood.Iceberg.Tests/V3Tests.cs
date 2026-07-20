// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests;

public class V3TypeTests
{
    [Theory]
    [InlineData("unknown")]
    [InlineData("variant")]
    public void V3PrimitiveType_RoundTrips(string typeString)
    {
        var json = $"\"{typeString}\"";
        var type = IcebergJsonSerializer.Deserialize<IcebergType>(json);
        var serialized = IcebergJsonSerializer.Serialize(type);
        Assert.Equal(json, serialized);
    }

    [Fact]
    public void GeometryType_RoundTrips()
    {
        var type = IcebergType.Geometry("OGC:CRS84");
        var json = IcebergJsonSerializer.Serialize(type);
        Assert.Equal("\"geometry(OGC:CRS84)\"", json);

        var deserialized = IcebergJsonSerializer.Deserialize<IcebergType>(json);
        Assert.IsType<GeometryType>(deserialized);
        Assert.Equal("OGC:CRS84", ((GeometryType)deserialized).Crs);
    }

    [Fact]
    public void GeographyType_RoundTrips()
    {
        var type = IcebergType.Geography("OGC:CRS84", "spherical");
        var json = IcebergJsonSerializer.Serialize(type);
        Assert.Equal("\"geography(OGC:CRS84,spherical)\"", json);

        var deserialized = IcebergJsonSerializer.Deserialize<IcebergType>(json);
        Assert.IsType<GeographyType>(deserialized);
        var geo = (GeographyType)deserialized;
        Assert.Equal("OGC:CRS84", geo.Crs);
        Assert.Equal("spherical", geo.Algorithm);
    }

    [Fact]
    public void UnknownType_Singleton()
    {
        Assert.IsType<UnknownType>(IcebergType.Unknown);
        var a = IcebergJsonSerializer.Deserialize<IcebergType>("\"unknown\"");
        var b = IcebergJsonSerializer.Deserialize<IcebergType>("\"unknown\"");
        Assert.Same(a, b);
    }

    [Fact]
    public void VariantType_Singleton()
    {
        Assert.IsType<VariantType>(IcebergType.Variant);
    }
}

public class V3DefaultValueTests
{
    [Fact]
    public void NestedField_WithDefaults_RoundTrips()
    {
        var json = """
            {
                "id": 1,
                "name": "status",
                "required": false,
                "type": "string",
                "initial-default": "active",
                "write-default": "active"
            }
            """;

        var field = IcebergJsonSerializer.Deserialize<NestedField>(json);
        Assert.Equal("active", field.InitialDefault?.GetString());
        Assert.Equal("active", field.WriteDefault?.GetString());

        var serialized = IcebergJsonSerializer.Serialize(field);
        var roundTripped = IcebergJsonSerializer.Deserialize<NestedField>(serialized);
        Assert.Equal("active", roundTripped.InitialDefault?.GetString());
    }

    [Fact]
    public void NestedField_WithNumericDefault_RoundTrips()
    {
        var json = """
            {
                "id": 1,
                "name": "count",
                "required": false,
                "type": "int",
                "initial-default": 0,
                "write-default": 0
            }
            """;

        var field = IcebergJsonSerializer.Deserialize<NestedField>(json);
        Assert.Equal(0, field.InitialDefault?.GetInt32());
    }

    [Fact]
    public void NestedField_WithoutDefaults_OmitsInJson()
    {
        var field = new NestedField(1, "id", IcebergType.Long, true);
        var json = IcebergJsonSerializer.Serialize(field);

        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("initial-default", out _));
        Assert.False(doc.RootElement.TryGetProperty("write-default", out _));
    }
}

public class V3SchemaEvolutionTests
{
    [Fact]
    public void UnknownType_PromotesToAnyType()
    {
        var schema = new Schema(0, [
            new NestedField(1, "data", IcebergType.Unknown, false),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test", formatVersion: 3);

        var updated = new SchemaUpdate(metadata)
            .UpdateColumnType("data", IcebergType.String)
            .Apply();

        var newSchema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(IcebergType.String, newSchema.Fields[0].Type);
    }

    [Fact]
    public void UnknownType_PromotesToComplexType()
    {
        var schema = new Schema(0, [
            new NestedField(1, "payload", IcebergType.Unknown, false),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test", formatVersion: 3);

        var updated = new SchemaUpdate(metadata)
            .UpdateColumnType("payload", IcebergType.Variant)
            .Apply();

        var newSchema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.IsType<VariantType>(newSchema.Fields[0].Type);
    }

    [Fact]
    public void DateType_PromotesToTimestamp()
    {
        var schema = new Schema(0, [
            new NestedField(1, "event_date", IcebergType.Date, true),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test", formatVersion: 3);

        var updated = new SchemaUpdate(metadata)
            .UpdateColumnType("event_date", IcebergType.Timestamp)
            .Apply();

        var newSchema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(IcebergType.Timestamp, newSchema.Fields[0].Type);
    }

    [Fact]
    public void DateType_PromotesToTimestampNs()
    {
        var schema = new Schema(0, [
            new NestedField(1, "event_date", IcebergType.Date, true),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test", formatVersion: 3);

        var updated = new SchemaUpdate(metadata)
            .UpdateColumnType("event_date", IcebergType.TimestampNs)
            .Apply();

        var newSchema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(IcebergType.TimestampNs, newSchema.Fields[0].Type);
    }
}

public class V3TableMetadataTests
{
    private readonly Schema _schema = new(0, [
        new NestedField(1, "id", IcebergType.Long, true),
    ]);

    [Fact]
    public void Create_V3_SetsFormatVersion3()
    {
        var metadata = TableMetadata.Create(_schema, location: "/test", formatVersion: 3);
        Assert.Equal(3, metadata.FormatVersion);
    }

    [Fact]
    public void Create_V3_InitializesNextRowId()
    {
        var metadata = TableMetadata.Create(_schema, location: "/test", formatVersion: 3);
        Assert.Equal(0, metadata.NextRowId);
    }

    [Fact]
    public void Create_V2_HasNullNextRowId()
    {
        var metadata = TableMetadata.Create(_schema, location: "/test", formatVersion: 2);
        Assert.Null(metadata.NextRowId);
    }

    [Fact]
    public void V3Metadata_JsonRoundTrips()
    {
        var metadata = TableMetadata.Create(_schema, location: "/test", formatVersion: 3);
        var json = IcebergJsonSerializer.Serialize(metadata);
        var deserialized = IcebergJsonSerializer.Deserialize<TableMetadata>(json);

        Assert.Equal(3, deserialized.FormatVersion);
        Assert.Equal(0, deserialized.NextRowId);
    }

    [Fact]
    public void V3Metadata_NextRowId_InJson()
    {
        var metadata = TableMetadata.Create(_schema, location: "/test", formatVersion: 3);
        var json = IcebergJsonSerializer.Serialize(metadata);

        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("next-row-id", out var val));
        Assert.Equal(0, val.GetInt64());
    }

    [Fact]
    public void V3Metadata_WithV3Types_RoundTrips()
    {
        var schema = new Schema(0, [
            new NestedField(1, "id", IcebergType.Long, true),
            new NestedField(2, "payload", IcebergType.Variant, false),
            new NestedField(3, "location", IcebergType.Geometry("OGC:CRS84"), false),
            new NestedField(4, "region", IcebergType.Geography("OGC:CRS84", "spherical"), false),
            new NestedField(5, "placeholder", IcebergType.Unknown, false),
        ]);

        var metadata = TableMetadata.Create(schema, location: "/test", formatVersion: 3);
        var json = IcebergJsonSerializer.Serialize(metadata);
        var deserialized = IcebergJsonSerializer.Deserialize<TableMetadata>(json);

        var fields = deserialized.Schemas[0].Fields;
        Assert.IsType<VariantType>(fields[1].Type);
        Assert.IsType<GeometryType>(fields[2].Type);
        Assert.Equal("OGC:CRS84", ((GeometryType)fields[2].Type).Crs);
        Assert.IsType<GeographyType>(fields[3].Type);
        Assert.Equal("spherical", ((GeographyType)fields[3].Type).Algorithm);
        Assert.IsType<UnknownType>(fields[4].Type);
    }

    [Fact]
    public void Create_InvalidFormatVersion_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            TableMetadata.Create(_schema, location: "/test", formatVersion: 4));
    }
}

public class V3SnapshotTests
{
    [Fact]
    public void Snapshot_WithRowLineage_RoundTrips()
    {
        var snapshot = new Snapshot(
            SnapshotId: 100,
            ParentSnapshotId: null,
            SequenceNumber: 1,
            TimestampMs: 1700000000000,
            ManifestList: "s3://bucket/metadata/snap-100.avro",
            Summary: new Dictionary<string, string> { ["operation"] = "append" },
            SchemaId: 0,
            FirstRowId: 0,
            AddedRows: 5000);

        var json = IcebergJsonSerializer.Serialize(snapshot);
        var deserialized = IcebergJsonSerializer.Deserialize<Snapshot>(json);

        Assert.Equal(0, deserialized.FirstRowId);
        Assert.Equal(5000, deserialized.AddedRows);
    }

    [Fact]
    public void Snapshot_WithoutRowLineage_OmitsFields()
    {
        var snapshot = new Snapshot(
            SnapshotId: 100,
            ParentSnapshotId: null,
            SequenceNumber: 1,
            TimestampMs: 1700000000000,
            ManifestList: "path",
            Summary: new Dictionary<string, string> { ["operation"] = "append" });

        var json = IcebergJsonSerializer.Serialize(snapshot);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("first-row-id", out _));
        Assert.False(doc.RootElement.TryGetProperty("added-rows", out _));
    }
}

public class V3DeletionVectorTests
{
    [Fact]
    public void DataFile_WithDeletionVector_RoundTrips()
    {
        var dataFile = new DataFile
        {
            FilePath = "data/file1.parquet",
            RecordCount = 1000,
            FileSizeInBytes = 50000,
            ReferencedDataFile = "data/original.parquet",
            ContentOffset = 1024,
            ContentSizeInBytes = 256,
        };

        var json = IcebergJsonSerializer.Serialize(dataFile);
        var deserialized = IcebergJsonSerializer.Deserialize<DataFile>(json);

        Assert.Equal("data/original.parquet", deserialized.ReferencedDataFile);
        Assert.Equal(1024, deserialized.ContentOffset);
        Assert.Equal(256, deserialized.ContentSizeInBytes);
    }

    [Fact]
    public void DataFile_WithRowLineage_RoundTrips()
    {
        var dataFile = new DataFile
        {
            FilePath = "data/file1.parquet",
            RecordCount = 1000,
            FileSizeInBytes = 50000,
            FirstRowId = 0,
        };

        var json = IcebergJsonSerializer.Serialize(dataFile);
        var deserialized = IcebergJsonSerializer.Deserialize<DataFile>(json);

        Assert.Equal(0, deserialized.FirstRowId);
    }

    [Fact]
    public void ManifestListEntry_WithRowLineage_RoundTrips()
    {
        var entry = new ManifestListEntry
        {
            ManifestPath = "metadata/manifest.json",
            ManifestLength = 1234,
            AddedSnapshotId = 100,
            FirstRowId = 5000,
        };

        var json = IcebergJsonSerializer.Serialize(entry);
        var deserialized = IcebergJsonSerializer.Deserialize<ManifestListEntry>(json);

        Assert.Equal(5000, deserialized.FirstRowId);
    }
}
