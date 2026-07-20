// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Serialization;

public class TableMetadataSerializationTests
{
    [Fact]
    public void TableMetadata_RoundTrips()
    {
        var metadata = new TableMetadata
        {
            FormatVersion = 2,
            TableUuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            Location = "s3://bucket/warehouse/ns/table",
            LastSequenceNumber = 34,
            LastUpdatedMs = 1602638573590,
            LastColumnId = 3,
            CurrentSchemaId = 1,
            Schemas =
            [
                new Schema(0, [
                    new NestedField(1, "x", IcebergType.Long, true),
                ]),
                new Schema(1, [
                    new NestedField(1, "x", IcebergType.Long, true),
                    new NestedField(2, "y", IcebergType.Long, true, "comment"),
                    new NestedField(3, "z", IcebergType.Long, true),
                ]),
            ],
            DefaultSpecId = 0,
            PartitionSpecs =
            [
                new PartitionSpec(0, [
                    new PartitionField(1, 1000, "x_bucket", Transform.Bucket(16)),
                ]),
            ],
            LastPartitionId = 1000,
            DefaultSortOrderId = 3,
            SortOrders =
            [
                SortOrder.Unsorted,
                new SortOrder(3, [
                    new SortField(2, Transform.Identity, SortDirection.Asc, NullOrder.NullsFirst),
                    new SortField(3, Transform.Bucket(4), SortDirection.Desc, NullOrder.NullsLast),
                ]),
            ],
            Properties = new Dictionary<string, string>
            {
                ["owner"] = "hank",
                ["commit.retry.num-retries"] = "1",
            },
            CurrentSnapshotId = 3055729675574597004,
            Snapshots =
            [
                new Snapshot(
                    SnapshotId: 3051729675574597004,
                    ParentSnapshotId: null,
                    SequenceNumber: 0,
                    TimestampMs: 1515100955770,
                    ManifestList: "s3://a/b/1.avro",
                    Summary: new Dictionary<string, string> { ["operation"] = "append" }),
                new Snapshot(
                    SnapshotId: 3055729675574597004,
                    ParentSnapshotId: 3051729675574597004,
                    SequenceNumber: 1,
                    TimestampMs: 1555100955770,
                    ManifestList: "s3://a/b/2.avro",
                    Summary: new Dictionary<string, string> { ["operation"] = "append" },
                    SchemaId: 1),
            ],
            SnapshotLog =
            [
                new SnapshotLogEntry(1515100955770, 3051729675574597004),
                new SnapshotLogEntry(1555100955770, 3055729675574597004),
            ],
            MetadataLog =
            [
                new MetadataLogEntry(1515100955770, "s3://bucket/warehouse/ns/table/metadata/v1.metadata.json"),
            ],
        };

        var json = IcebergJsonSerializer.Serialize(metadata);
        var deserialized = IcebergJsonSerializer.Deserialize<TableMetadata>(json);

        Assert.Equal(2, deserialized.FormatVersion);
        Assert.Equal("9c12d441-03fe-4693-9a96-a0705ddf69c1", deserialized.TableUuid);
        Assert.Equal("s3://bucket/warehouse/ns/table", deserialized.Location);
        Assert.Equal(34, deserialized.LastSequenceNumber);
        Assert.Equal(2, deserialized.Schemas.Count);
        Assert.Equal(3, deserialized.Schemas[1].Fields.Count);
        Assert.Single(deserialized.PartitionSpecs);
        Assert.Equal(2, deserialized.SortOrders.Count);
        Assert.Equal(2, deserialized.Snapshots.Count);
        Assert.Equal(3055729675574597004, deserialized.CurrentSnapshotId);
        Assert.Null(deserialized.Snapshots[0].ParentSnapshotId);
        Assert.Equal(3051729675574597004, deserialized.Snapshots[1].ParentSnapshotId);
        Assert.Equal(1, deserialized.Snapshots[1].SchemaId);
        Assert.Equal("hank", deserialized.Properties["owner"]);
        Assert.Equal(2, deserialized.SnapshotLog.Count);
        Assert.Single(deserialized.MetadataLog);
    }

    [Fact]
    public void TableMetadata_MatchesSpecJsonPropertyNames()
    {
        var metadata = new TableMetadata
        {
            FormatVersion = 2,
            TableUuid = "test-uuid",
            Location = "s3://test",
            LastUpdatedMs = 1000,
            LastColumnId = 1,
            CurrentSchemaId = 0,
            Schemas = [new Schema(0, [new NestedField(1, "id", IcebergType.Int, true)])],
            DefaultSpecId = 0,
            PartitionSpecs = [PartitionSpec.Unpartitioned],
            LastPartitionId = 0,
        };

        var json = IcebergJsonSerializer.Serialize(metadata);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("format-version", out _));
        Assert.True(root.TryGetProperty("table-uuid", out _));
        Assert.True(root.TryGetProperty("last-updated-ms", out _));
        Assert.True(root.TryGetProperty("last-column-id", out _));
        Assert.True(root.TryGetProperty("current-schema-id", out _));
        Assert.True(root.TryGetProperty("default-spec-id", out _));
        Assert.True(root.TryGetProperty("partition-specs", out _));
        Assert.True(root.TryGetProperty("last-partition-id", out _));
        Assert.True(root.TryGetProperty("default-sort-order-id", out _));
        Assert.True(root.TryGetProperty("sort-orders", out _));
        Assert.True(root.TryGetProperty("snapshot-log", out _));
        Assert.True(root.TryGetProperty("metadata-log", out _));
    }

    [Fact]
    public void TableMetadata_DeserializesFromSpecJson()
    {
        var json = """
            {
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
                "location": "s3://bucket/warehouse/ns/table",
                "last-sequence-number": 34,
                "last-updated-ms": 1602638573590,
                "last-column-id": 3,
                "current-schema-id": 1,
                "schemas": [
                    {
                        "schema-id": 0,
                        "type": "struct",
                        "fields": [
                            {"id": 1, "name": "x", "required": true, "type": "long"}
                        ]
                    },
                    {
                        "schema-id": 1,
                        "type": "struct",
                        "fields": [
                            {"id": 1, "name": "x", "required": true, "type": "long"},
                            {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                            {"id": 3, "name": "z", "required": true, "type": "long"}
                        ]
                    }
                ],
                "default-spec-id": 0,
                "partition-specs": [
                    {
                        "spec-id": 0,
                        "fields": [
                            {"source-id": 1, "field-id": 1000, "name": "x_bucket", "transform": "bucket[16]"}
                        ]
                    }
                ],
                "last-partition-id": 1000,
                "default-sort-order-id": 3,
                "sort-orders": [
                    {"order-id": 0, "fields": []},
                    {
                        "order-id": 3,
                        "fields": [
                            {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                            {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}
                        ]
                    }
                ],
                "properties": {
                    "owner": "hank",
                    "commit.retry.num-retries": "1"
                },
                "current-snapshot-id": 3055729675574597004,
                "snapshots": [
                    {
                        "snapshot-id": 3051729675574597004,
                        "sequence-number": 0,
                        "timestamp-ms": 1515100955770,
                        "manifest-list": "s3://a/b/1.avro",
                        "summary": {"operation": "append"}
                    },
                    {
                        "snapshot-id": 3055729675574597004,
                        "parent-snapshot-id": 3051729675574597004,
                        "sequence-number": 1,
                        "timestamp-ms": 1555100955770,
                        "manifest-list": "s3://a/b/2.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 1
                    }
                ],
                "snapshot-log": [
                    {"timestamp-ms": 1515100955770, "snapshot-id": 3051729675574597004},
                    {"timestamp-ms": 1555100955770, "snapshot-id": 3055729675574597004}
                ],
                "metadata-log": [
                    {"timestamp-ms": 1515100955770, "metadata-file": "s3://bucket/warehouse/ns/table/metadata/v1.metadata.json"}
                ]
            }
            """;

        var metadata = IcebergJsonSerializer.Deserialize<TableMetadata>(json);

        Assert.Equal(2, metadata.FormatVersion);
        Assert.Equal("9c12d441-03fe-4693-9a96-a0705ddf69c1", metadata.TableUuid);
        Assert.Equal(34, metadata.LastSequenceNumber);
        Assert.Equal(2, metadata.Schemas.Count);
        Assert.Equal(3, metadata.Schemas[1].Fields.Count);
        Assert.Equal("comment", metadata.Schemas[1].Fields[1].Doc);
        Assert.IsType<BucketTransform>(metadata.PartitionSpecs[0].Fields[0].Transform);
        Assert.Equal(16, ((BucketTransform)metadata.PartitionSpecs[0].Fields[0].Transform).NumBuckets);
        Assert.Equal(2, metadata.SortOrders.Count);
        Assert.Equal(SortDirection.Asc, metadata.SortOrders[1].Fields[0].Direction);
        Assert.Equal(NullOrder.NullsLast, metadata.SortOrders[1].Fields[1].NullOrder);
        Assert.Equal(3055729675574597004, metadata.CurrentSnapshotId);
        Assert.Equal(2, metadata.Snapshots.Count);
        Assert.Equal("hank", metadata.Properties["owner"]);
    }
}
