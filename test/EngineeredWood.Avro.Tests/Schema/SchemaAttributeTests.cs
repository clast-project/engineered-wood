// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests.Schema;

/// <summary>
/// Avro lets a schema carry attributes the specification does not define. Iceberg depends on them: its
/// manifests identify every column by <c>field-id</c>, <c>element-id</c> and <c>key-id</c>/<c>value-id</c>,
/// so a schema must keep them through a parse and a write.
/// </summary>
public class SchemaAttributeTests
{
    // The shape of a manifest schema as Iceberg Java writes it: ids on fields, arrays and maps, docs, a map
    // with non-string keys written as an array of key/value records with logicalType "map", and a timestamp
    // with adjust-to-utc.
    private const string IcebergManifestSchema = """
        {
          "type": "record",
          "name": "manifest_entry",
          "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
            {"name": "data_file", "field-id": 2, "type": {
              "type": "record",
              "name": "r2",
              "fields": [
                {"name": "content", "type": "int", "doc": "Contents of the file", "field-id": 134},
                {"name": "partition", "doc": "Partition data tuple", "field-id": 102, "type": {
                  "type": "record",
                  "name": "r102",
                  "fields": [
                    {"name": "ts_day", "type": ["null", {"type": "int", "logicalType": "date"}],
                     "default": null, "field-id": 1000}
                  ]
                }},
                {"name": "column_sizes", "default": null, "field-id": 108, "type": ["null", {
                  "type": "array",
                  "logicalType": "map",
                  "items": {
                    "type": "record",
                    "name": "k117_v118",
                    "fields": [
                      {"name": "key", "type": "int", "field-id": 117},
                      {"name": "value", "type": "long", "field-id": 118}
                    ]
                  }
                }]},
                {"name": "split_offsets", "default": null, "field-id": 132,
                 "type": ["null", {"type": "array", "items": "long", "element-id": 133}]},
                {"name": "props", "field-id": 140,
                 "type": {"type": "map", "values": "string", "key-id": 141, "value-id": 142}},
                {"name": "ts", "field-id": 143,
                 "type": {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true}},
                {"name": "plain", "field-id": 144, "type": {"type": "string", "x-custom": [1, {"a": null}]}}
              ]
            }}
          ]
        }
        """;

    [Fact]
    public void IcebergIds_AreParsed()
    {
        var root = (AvroRecordSchema)AvroSchemaParser.Parse(IcebergManifestSchema);
        Assert.Equal(0, root.Fields[0].FieldId);
        Assert.Equal(1, root.Fields[1].FieldId);

        var dataFile = (AvroRecordSchema)root.Fields[2].Schema;
        Assert.Equal(2, root.Fields[2].FieldId);
        Assert.Equal(134, dataFile.Fields[0].FieldId);

        var partition = (AvroRecordSchema)dataFile.Fields[1].Schema;
        Assert.Equal(1000, partition.Fields[0].FieldId);

        var columnSizes = (AvroArraySchema)((AvroUnionSchema)dataFile.Fields[2].Schema).Branches[1];
        Assert.Equal("map", columnSizes.LogicalType);
        var kv = (AvroRecordSchema)columnSizes.Items;
        Assert.Equal(117, kv.Fields[0].FieldId);
        Assert.Equal(118, kv.Fields[1].FieldId);

        var splitOffsets = (AvroArraySchema)((AvroUnionSchema)dataFile.Fields[3].Schema).Branches[1];
        Assert.Equal(133, splitOffsets.ElementId);

        var props = (AvroMapSchema)dataFile.Fields[4].Schema;
        Assert.Equal(141, props.KeyId);
        Assert.Equal(142, props.ValueId);

        var ts = (AvroPrimitiveSchema)dataFile.Fields[5].Schema;
        Assert.Equal("timestamp-micros", ts.LogicalType);
        Assert.True(ts.Properties["adjust-to-utc"].GetBoolean());

        var plain = (AvroPrimitiveSchema)dataFile.Fields[6].Schema;
        Assert.Equal(AvroType.String, plain.Type);
        Assert.True(plain.Properties.ContainsKey("x-custom"));
    }

    [Fact]
    public void IcebergSchema_RoundTripsThroughTheWriter()
    {
        var written = AvroSchemaWriter.ToJson(AvroSchemaParser.Parse(IcebergManifestSchema));
        Assert.Equal(Canonical(IcebergManifestSchema), Canonical(written));
    }

    [Fact]
    public void DocsAliasesAndOrder_RoundTrip()
    {
        const string json = """
            {"type": "record", "name": "R", "namespace": "n", "doc": "a record", "aliases": ["Old"],
             "x-record": 1,
             "fields": [
               {"name": "a", "type": "int", "doc": "field a", "aliases": ["b"], "order": "descending"},
               {"name": "e", "type": {"type": "enum", "name": "E", "namespace": "n", "symbols": ["X", "Y"], "doc": "an enum",
                                      "aliases": ["OldE"], "x-enum": true}},
               {"name": "f", "type": {"type": "fixed", "name": "F", "namespace": "n", "size": 4, "aliases": ["OldF"],
                                      "x-fixed": "y"}}
             ]}
            """;
        var written = AvroSchemaWriter.ToJson(AvroSchemaParser.Parse(json));
        Assert.Equal(Canonical(json), Canonical(written));
    }

    [Fact]
    public void Fingerprint_IgnoresExtraAttributes()
    {
        // Parsing Canonical Form strips every attribute but the defined ones, so ids do not change a
        // schema's identity for schema registries.
        const string withIds = """
            {"type": "record", "name": "R", "fields": [
              {"name": "a", "type": {"type": "array", "items": "int", "element-id": 2}, "field-id": 1}
            ]}
            """;
        const string withoutIds = """
            {"type": "record", "name": "R", "fields": [
              {"name": "a", "type": {"type": "array", "items": "int"}}
            ]}
            """;
        Assert.Equal(
            new AvroSchema(withoutIds).ComputeFingerprint(),
            new AvroSchema(withIds).ComputeFingerprint());
    }

    [Fact]
    public void Project_KeepsIds()
    {
        var projected = new AvroSchema(IcebergManifestSchema).Project([2, 0]);
        var root = (AvroRecordSchema)AvroSchemaParser.Parse(projected.Json);
        Assert.Equal(2, root.Fields[0].FieldId);
        Assert.Equal(0, root.Fields[1].FieldId);
        Assert.Equal(133,
            ((AvroArraySchema)((AvroUnionSchema)((AvroRecordSchema)root.Fields[0].Schema).Fields[3].Schema)
                .Branches[1]).ElementId);
    }

    [Fact]
    public void NonIntegerId_ReadsAsAbsent()
    {
        var root = (AvroRecordSchema)AvroSchemaParser.Parse(
            """{"type": "record", "name": "R", "fields": [{"name": "a", "type": "int", "field-id": "7"}]}""");
        Assert.Null(root.Fields[0].FieldId);
        Assert.Equal("7", root.Fields[0].Properties["field-id"].GetString());
    }

    /// <summary>JSON with object properties sorted by name, so property order does not matter.</summary>
    internal static string Canonical(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var sb = new StringBuilder();
        Write(doc.RootElement, sb);
        return sb.ToString();

        static void Write(JsonElement e, StringBuilder sb)
        {
            switch (e.ValueKind)
            {
                case JsonValueKind.Object:
                    sb.Append('{');
                    foreach (var p in e.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal))
                    {
                        sb.Append(JsonSerializer.Serialize(p.Name)).Append(':');
                        Write(p.Value, sb);
                        sb.Append(',');
                    }
                    sb.Append('}');
                    break;
                case JsonValueKind.Array:
                    sb.Append('[');
                    foreach (var item in e.EnumerateArray())
                    {
                        Write(item, sb);
                        sb.Append(',');
                    }
                    sb.Append(']');
                    break;
                default:
                    sb.Append(e.GetRawText());
                    break;
            }
        }
    }
}
