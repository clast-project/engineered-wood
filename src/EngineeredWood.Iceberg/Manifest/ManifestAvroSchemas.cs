// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// Avro schema definitions for Iceberg manifest files and manifest lists.
/// These schemas match the Iceberg spec for format version 2.
/// </summary>
internal static class ManifestAvroSchemas
{
    internal static readonly AvroRecordSchema ManifestListSchema =
        (AvroRecordSchema)AvroSchemaParser.Parse(ManifestListV2);

    internal static readonly AvroRecordSchema ManifestEntrySchema =
        (AvroRecordSchema)AvroSchemaParser.Parse(ManifestEntryV2);

    public const string ManifestListV2 = """
        {
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "field-id": 517},
            {"name": "sequence_number", "type": "long", "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {"name": "added_data_files_count", "type": "int", "field-id": 504},
            {"name": "existing_data_files_count", "type": "int", "field-id": 505},
            {"name": "deleted_data_files_count", "type": "int", "field-id": 506},
            {"name": "added_rows_count", "type": "long", "field-id": 512},
            {"name": "existing_rows_count", "type": "long", "field-id": 513},
            {"name": "deleted_rows_count", "type": "long", "field-id": 514},
            {"name": "partitions", "type": ["null", {"type": "array", "items": {
              "type": "record", "name": "field_summary", "fields": [
                {"name": "contains_null", "type": "boolean", "field-id": 509},
                {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 518},
                {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 510},
                {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 511}
              ]
            }}], "field-id": 507},
            {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519}
          ]
        }
        """;

    public const string ManifestEntryV2 = """
        {
          "type": "record",
          "name": "manifest_entry",
          "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1},
            {"name": "sequence_number", "type": ["null", "long"], "field-id": 3},
            {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4},
            {"name": "data_file", "type": {
              "type": "record",
              "name": "r2",
              "fields": [
                {"name": "content", "type": "int", "field-id": 134},
                {"name": "file_path", "type": "string", "field-id": 100},
                {"name": "file_format", "type": "string", "field-id": 101},
                {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}, "field-id": 102},
                {"name": "record_count", "type": "long", "field-id": 103},
                {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                {"name": "column_sizes", "type": ["null", {"type": "array", "items": {
                  "type": "record", "name": "k117_v118", "fields": [
                    {"name": "key", "type": "int", "field-id": 117},
                    {"name": "value", "type": "long", "field-id": 118}
                  ]
                }, "logicalType": "map"}], "field-id": 116},
                {"name": "value_counts", "type": ["null", {"type": "array", "items": {
                  "type": "record", "name": "k119_v120", "fields": [
                    {"name": "key", "type": "int", "field-id": 119},
                    {"name": "value", "type": "long", "field-id": 120}
                  ]
                }, "logicalType": "map"}], "field-id": 121},
                {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {
                  "type": "record", "name": "k122_v123", "fields": [
                    {"name": "key", "type": "int", "field-id": 122},
                    {"name": "value", "type": "long", "field-id": 123}
                  ]
                }, "logicalType": "map"}], "field-id": 124},
                {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "field-id": 132},
                {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140}
              ]
            }, "field-id": 2}
          ]
        }
        """;
}
