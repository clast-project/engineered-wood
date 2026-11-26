// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using System.Text.Json;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Serialization;

/// <summary>
/// Metadata another engine wrote must survive a read and rewrite: every field the spec defines is modelled,
/// and anything else is carried through.
/// </summary>
public class LosslessMetadataTests
{
    // A v3 table as Java writes it, plus properties no engine defines yet (the "x-…" names) at every level
    // that must pass through. Every field EW always writes is present, so the rewrite can be compared whole.
    private const string V3Json = """
        {
          "format-version": 3,
          "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
          "location": "s3://bucket/warehouse/ns/table",
          "last-sequence-number": 2,
          "last-updated-ms": 1602638573590,
          "last-column-id": 4,
          "current-schema-id": 0,
          "schemas": [
            {
              "schema-id": 0,
              "type": "struct",
              "x-schema-extra": {"a": [1, 2]},
              "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long", "x-field-extra": "kept"},
                {"id": 2, "name": "amount", "required": false, "type": "decimal(9, 2)"},
                {"id": 3, "name": "place", "required": false, "type": "geography(srid:4326, planar)"},
                {"id": 4, "name": "ts", "required": false, "type": "timestamp"}
              ]
            }
          ],
          "default-spec-id": 0,
          "partition-specs": [
            {
              "spec-id": 0,
              "fields": [
                {"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"},
                {"source-ids": [1, 4], "field-id": 1001, "name": "multi", "transform": "zorder[8]",
                 "x-partition-field-extra": true}
              ]
            }
          ],
          "last-partition-id": 1001,
          "default-sort-order-id": 1,
          "sort-orders": [
            {"order-id": 0, "fields": []},
            {"order-id": 1, "fields": [
              {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"},
              {"transform": "hilbert", "source-ids": [1, 4], "direction": "desc", "null-order": "nulls-last"}
            ]}
          ],
          "properties": {"owner": "hank"},
          "current-snapshot-id": 3055729675574597004,
          "snapshots": [
            {
              "snapshot-id": 3055729675574597004,
              "sequence-number": 2,
              "timestamp-ms": 1555100955770,
              "manifest-list": "s3://b/wh/.../s2.avro",
              "summary": {"operation": "append"},
              "schema-id": 0,
              "first-row-id": 10,
              "added-rows": 5,
              "key-id": "snapshot-key",
              "x-snapshot-extra": 42
            }
          ],
          "snapshot-log": [{"timestamp-ms": 1555100955770, "snapshot-id": 3055729675574597004}],
          "metadata-log": [{"timestamp-ms": 1515100955770, "metadata-file": "s3://bucket/.../v1.json"}],
          "refs": {"main": {"snapshot-id": 3055729675574597004, "type": "branch"}},
          "next-row-id": 15,
          "statistics": [
            {
              "snapshot-id": 3055729675574597004,
              "statistics-path": "s3://bucket/warehouse/stats.puffin",
              "file-size-in-bytes": 413,
              "file-footer-size-in-bytes": 42,
              "key-metadata": "aWNlYmVyZwo=",
              "blob-metadata": [
                {
                  "type": "apache-datasketches-theta-v1",
                  "snapshot-id": 3055729675574597004,
                  "sequence-number": 2,
                  "fields": [1],
                  "properties": {"ndv": "3"}
                }
              ]
            }
          ],
          "partition-statistics": [
            {
              "snapshot-id": 3055729675574597004,
              "statistics-path": "s3://bucket/warehouse/partition-stats.parquet",
              "file-size-in-bytes": 1024
            }
          ],
          "encryption-keys": [
            {"key-id": "table-key", "encrypted-key-metadata": "AAEC", "encrypted-by-id": "kms",
             "properties": {"p": "v"}}
          ],
          "stats-fields": [
            {"field-id": 5, "type": "partition-value", "data-type": "int", "partition-field-id": 1000},
            {"field-id": 6, "type": "expr-value", "data-type": "long", "expr": {"type": "ref", "term": "id"}},
            {"field-id": 7, "type": "some-future-kind", "future-setting": [1]}
          ],
          "x-top-level-extra": {"nested": {"deep": null}}
        }
        """;

    [Fact]
    public void V3Metadata_IsModelled()
    {
        var md = IcebergJsonSerializer.Deserialize<TableMetadata>(V3Json);

        var stats = Assert.Single(md.Statistics!);
        Assert.Equal("s3://bucket/warehouse/stats.puffin", stats.StatisticsPath);
        Assert.Equal(413, stats.FileSizeInBytes);
        Assert.Equal(42, stats.FileFooterSizeInBytes);
        Assert.Equal("aWNlYmVyZwo=", stats.KeyMetadata);
        var blob = Assert.Single(stats.BlobMetadata);
        Assert.Equal("apache-datasketches-theta-v1", blob.Type);
        Assert.Equal(2, blob.SequenceNumber);
        Assert.Equal([1], blob.Fields);
        Assert.Equal("3", blob.Properties!["ndv"]);

        var partitionStats = Assert.Single(md.PartitionStatistics!);
        Assert.Equal(1024, partitionStats.FileSizeInBytes);

        var key = Assert.Single(md.EncryptionKeys!);
        Assert.Equal(new EncryptedKey("table-key", "AAEC", "kms", key.Properties), key);
        Assert.Equal("v", key.Properties!["p"]);
        Assert.True(md.IsEncrypted);

        Assert.Equal("snapshot-key", md.Snapshots[0].KeyId);

        Assert.Equal(3, md.StatsFields!.Count);
        Assert.Equal(1000, md.StatsFields[0].PartitionFieldId);
        Assert.Equal(IcebergType.Int, md.StatsFields[0].DataType);
        Assert.Equal("ref", md.StatsFields[1].Expr!.Value.GetProperty("type").GetString());
        Assert.Equal("some-future-kind", md.StatsFields[2].Type);
        Assert.True(md.StatsFields[2].AdditionalProperties!.ContainsKey("future-setting"));

        var fields = md.Schemas[0].Fields;
        Assert.Equal(IcebergType.Decimal(9, 2), fields[1].Type);
        Assert.Equal(IcebergType.Geography("srid:4326", "planar"), fields[2].Type);

        var multi = md.PartitionSpecs[0].Fields[1];
        Assert.Equal([1, 4], multi.SourceIds!);
        Assert.Equal(1, multi.SourceId);
        Assert.Equal(new UnknownTransform("zorder[8]"), multi.Transform);
        Assert.Null(md.PartitionSpecs[0].Fields[0].SourceIds);

        Assert.Equal([1, 4], md.SortOrders[1].Fields[1].SourceIds!);
        Assert.Equal(new UnknownTransform("hilbert"), md.SortOrders[1].Fields[1].Transform);
    }

    [Fact]
    public void V3Metadata_RoundTripsLosslessly()
    {
        var md = IcebergJsonSerializer.Deserialize<TableMetadata>(V3Json);
        var written = IcebergJsonSerializer.Serialize(md);
        Assert.Equal(Canonical(V3Json), Canonical(written));
    }

    [Fact]
    public void UnknownProperties_SurviveAModelChange()
    {
        // What a commit does: change a modelled field with `with`, then write.
        var md = IcebergJsonSerializer.Deserialize<TableMetadata>(V3Json);
        var changed = md with { LastUpdatedMs = 1, Properties = new Dictionary<string, string> { ["a"] = "b" } };
        using var doc = JsonDocument.Parse(IcebergJsonSerializer.Serialize(changed));
        var root = doc.RootElement;

        Assert.Equal(1, root.GetProperty("last-updated-ms").GetInt64());
        Assert.True(root.TryGetProperty("x-top-level-extra", out _));
        Assert.Equal(42, root.GetProperty("snapshots")[0].GetProperty("x-snapshot-extra").GetInt32());
        var schema = root.GetProperty("schemas")[0];
        Assert.True(schema.TryGetProperty("x-schema-extra", out _));
        Assert.Equal("kept", schema.GetProperty("fields")[0].GetProperty("x-field-extra").GetString());
        Assert.True(root.GetProperty("partition-specs")[0].GetProperty("fields")[1]
            .GetProperty("x-partition-field-extra").GetBoolean());
    }

    [Fact]
    public void MetadataWithoutTheNewFields_WritesNoneOfThem()
    {
        var md = TableMetadata.Create(new Schema(0, [new NestedField(1, "id", IcebergType.Long, true)]));
        using var doc = JsonDocument.Parse(IcebergJsonSerializer.Serialize(md));
        foreach (var name in new[] { "statistics", "partition-statistics", "encryption-keys", "stats-fields" })
            Assert.False(doc.RootElement.TryGetProperty(name, out _), name);
        Assert.False(md.IsEncrypted);
    }

    [Fact]
    public void SingleArgumentSourceIds_WriteSourceIds()
    {
        // A reader must accept source-ids even with one element; the field keeps the form it was read in.
        const string json = """{"source-ids": [3], "field-id": 1000, "name": "p", "transform": "identity"}""";
        var field = IcebergJsonSerializer.Deserialize<PartitionField>(json);
        Assert.Equal(3, field.SourceId);
        Assert.Equal(Canonical(json), Canonical(IcebergJsonSerializer.Serialize(field)));
    }

    [Fact]
    public void FieldWithNoSource_IsRefused()
    {
        Assert.Throws<JsonException>(() => IcebergJsonSerializer.Deserialize<PartitionField>(
            """{"field-id": 1000, "name": "p", "transform": "identity"}"""));
    }

    [Theory]
    [InlineData("decimal(9,2)")]
    [InlineData("decimal(9, 2)")]
    [InlineData("decimal( 9 , 2 )")]
    [InlineData("decimal(9,\t2)")]
    public void Decimal_AcceptsOptionalWhitespace(string spelling)
    {
        var type = IcebergJsonSerializer.Deserialize<IcebergType>(JsonSerializer.Serialize(spelling));
        Assert.Equal(IcebergType.Decimal(9, 2), type);
        Assert.Equal("\"decimal(9, 2)\"", IcebergJsonSerializer.Serialize(type));
    }

    [Theory]
    [InlineData("geography(srid:4326,planar)", "srid:4326", "planar")]
    [InlineData("geography(srid:4326, planar)", "srid:4326", "planar")]
    [InlineData("geography( srid:4326 , planar )", "srid:4326", "planar")]
    [InlineData("geography(srid:4326)", "srid:4326", "spherical")]
    [InlineData("geography", "OGC:CRS84", "spherical")]
    [InlineData("GEOGRAPHY(srid:4326, planar)", "srid:4326", "planar")]
    public void Geography_AcceptsOptionalWhitespaceAndDefaults(string spelling, string crs, string algorithm)
    {
        var type = IcebergJsonSerializer.Deserialize<IcebergType>(JsonSerializer.Serialize(spelling));
        Assert.Equal(IcebergType.Geography(crs, algorithm), type);
        Assert.Equal(JsonSerializer.Serialize($"geography({crs}, {algorithm})"), IcebergJsonSerializer.Serialize(type));
    }

    [Theory]
    [InlineData("geometry(srid:4326)", "srid:4326")]
    [InlineData("geometry( srid:4326 )", "srid:4326")]
    [InlineData("geometry", "OGC:CRS84")]
    public void Geometry_AcceptsOptionalWhitespaceAndDefaults(string spelling, string crs)
    {
        var type = IcebergJsonSerializer.Deserialize<IcebergType>(JsonSerializer.Serialize(spelling));
        Assert.Equal(IcebergType.Geometry(crs), type);
    }

    [Theory]
    [InlineData("fixed[16]")]
    [InlineData("fixed[ 16 ]")]
    public void Fixed_AcceptsOptionalWhitespace(string spelling)
    {
        var type = IcebergJsonSerializer.Deserialize<IcebergType>(JsonSerializer.Serialize(spelling));
        Assert.Equal(new FixedType(16), type);
    }

    [Theory]
    [InlineData("decimal(9)")]
    [InlineData("decimal(9,2")]
    [InlineData("decimal(a, 2)")]
    [InlineData("decimal(99999999999, 2)")]
    [InlineData("fixed[]")]
    public void MalformedParameterizedTypes_AreRefused(string spelling)
    {
        Assert.Throws<JsonException>(() =>
            IcebergJsonSerializer.Deserialize<IcebergType>(JsonSerializer.Serialize(spelling)));
    }

    [Fact]
    public void V1SpecWithoutFieldIds_NumbersThemFrom1000()
    {
        var spec = IcebergJsonSerializer.Deserialize<PartitionSpec>("""
            {"spec-id": 0, "fields": [
              {"source-id": 1, "name": "a", "transform": "identity"},
              {"source-id": 2, "name": "b", "transform": "bucket[4]"}
            ]}
            """);
        Assert.Equal([1000, 1001], spec.Fields.Select(f => f.FieldId));

        Assert.Throws<JsonException>(() => IcebergJsonSerializer.Deserialize<PartitionSpec>("""
            {"spec-id": 0, "fields": [
              {"source-id": 1, "field-id": 1000, "name": "a", "transform": "identity"},
              {"source-id": 2, "name": "b", "transform": "identity"}
            ]}
            """));
    }

    [Theory]
    [InlineData("bucket[16,2]")]
    [InlineData("bucket[x]")]
    [InlineData("truncate[ 4 ]")]
    [InlineData("bucket[99999999999]")]
    [InlineData("bucketed[4]")]
    public void TransformsThatOnlyLookKnown_AreUnknown(string name)
    {
        var transform = IcebergJsonSerializer.Deserialize<Transform>(JsonSerializer.Serialize(name));
        Assert.Equal(new UnknownTransform(name), transform);
        Assert.Equal(JsonSerializer.Serialize(name), IcebergJsonSerializer.Serialize(transform));
    }

    [Fact]
    public void DecimalPrecision_IsAtMost38()
    {
        Assert.Equal(IcebergType.Decimal(38, 10),
            IcebergJsonSerializer.Deserialize<IcebergType>("\"decimal(38, 10)\""));
        Assert.Throws<JsonException>(() => IcebergJsonSerializer.Deserialize<IcebergType>("\"decimal(39, 2)\""));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(0)]
    public void UnsupportedFormatVersion_IsRefused(int version)
    {
        var json = V3Json.Replace("\"format-version\": 3", $"\"format-version\": {version}");
        var ex = Assert.Throws<UnsupportedTableFeatureException>(() =>
            IcebergJsonSerializer.Deserialize<TableMetadata>(json));
        Assert.Contains($"format-version {version}", ex.Message);
    }

    /// <summary>
    /// A JSON value with object properties sorted by name, so two documents compare equal when they hold
    /// the same data in a different property order.
    /// </summary>
    private static string Canonical(string json)
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
