// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// The Avro schemas of manifests and manifest lists, per format version, as the spec defines them: field
/// names, field ids, optionality and (in a manifest) a <c>partition</c> record typed by the partition spec.
/// Field order follows Iceberg Java's writers.
/// </summary>
internal static class ManifestAvroSchemas
{
    // --- manifest_entry / data_file field ids (spec "Manifests") ---
    internal const int Status = 0, SnapshotId = 1, DataFileId = 2, SequenceNumber = 3, FileSequenceNumber = 4;
    internal const int FilePath = 100, FileFormat = 101, Partition = 102, RecordCount = 103, FileSizeInBytes = 104,
        BlockSizeInBytes = 105, FileOrdinal = 106, SortColumns = 107, ColumnSizes = 108, ValueCounts = 109,
        NullValueCounts = 110, DistinctCounts = 111, LowerBounds = 125, UpperBounds = 128, KeyMetadata = 131,
        SplitOffsets = 132, Content = 134, EqualityIds = 135, NanValueCounts = 137, SortOrderId = 140,
        FirstRowId = 142, ReferencedDataFile = 143, ContentOffset = 144, ContentSizeInBytes = 145;

    // --- manifest_file (manifest list) field ids ---
    internal const int ManifestPath = 500, ManifestLength = 501, PartitionSpecId = 502, AddedSnapshotId = 503,
        AddedFilesCount = 504, ExistingFilesCount = 505, DeletedFilesCount = 506, Partitions = 507,
        FieldSummary = 508, ContainsNull = 509, LowerBound = 510, UpperBound = 511, AddedRowsCount = 512,
        ExistingRowsCount = 513, DeletedRowsCount = 514, ListSequenceNumber = 515, MinSequenceNumber = 516,
        ManifestContent = 517, ContainsNan = 518, ManifestKeyMetadata = 519, ManifestFirstRowId = 520;

    /// <summary>
    /// Field ids by name, used when a writer schema carries no <c>field-id</c> (EngineeredWood's own manifests
    /// before #586 had none). Every spelling a writer is known to use is listed.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, int> EntryFieldsByName = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        ["status"] = Status, ["snapshot_id"] = SnapshotId, ["data_file"] = DataFileId,
        ["sequence_number"] = SequenceNumber, ["file_sequence_number"] = FileSequenceNumber,
    };

    internal static readonly IReadOnlyDictionary<string, int> DataFileFieldsByName = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        ["content"] = Content, ["file_path"] = FilePath, ["file_format"] = FileFormat, ["partition"] = Partition,
        ["record_count"] = RecordCount, ["file_size_in_bytes"] = FileSizeInBytes,
        ["block_size_in_bytes"] = BlockSizeInBytes, ["file_ordinal"] = FileOrdinal, ["sort_columns"] = SortColumns,
        ["column_sizes"] = ColumnSizes, ["value_counts"] = ValueCounts, ["null_value_counts"] = NullValueCounts,
        ["nan_value_counts"] = NanValueCounts, ["distinct_counts"] = DistinctCounts,
        ["lower_bounds"] = LowerBounds, ["upper_bounds"] = UpperBounds, ["key_metadata"] = KeyMetadata,
        ["split_offsets"] = SplitOffsets, ["equality_ids"] = EqualityIds, ["sort_order_id"] = SortOrderId,
        ["first_row_id"] = FirstRowId, ["referenced_data_file"] = ReferencedDataFile,
        ["content_offset"] = ContentOffset, ["content_size_in_bytes"] = ContentSizeInBytes,
    };

    internal static readonly IReadOnlyDictionary<string, int> ManifestFileFieldsByName = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        ["manifest_path"] = ManifestPath, ["manifest_length"] = ManifestLength,
        ["partition_spec_id"] = PartitionSpecId, ["content"] = ManifestContent,
        ["sequence_number"] = ListSequenceNumber, ["min_sequence_number"] = MinSequenceNumber,
        ["added_snapshot_id"] = AddedSnapshotId,
        ["added_files_count"] = AddedFilesCount, ["added_data_files_count"] = AddedFilesCount,
        ["existing_files_count"] = ExistingFilesCount, ["existing_data_files_count"] = ExistingFilesCount,
        ["deleted_files_count"] = DeletedFilesCount, ["deleted_data_files_count"] = DeletedFilesCount,
        ["added_rows_count"] = AddedRowsCount, ["existing_rows_count"] = ExistingRowsCount,
        ["deleted_rows_count"] = DeletedRowsCount, ["partitions"] = Partitions,
        ["key_metadata"] = ManifestKeyMetadata, ["first_row_id"] = ManifestFirstRowId,
    };

    internal static readonly IReadOnlyDictionary<string, int> FieldSummaryFieldsByName = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        ["contains_null"] = ContainsNull, ["contains_nan"] = ContainsNan,
        ["lower_bound"] = LowerBound, ["upper_bound"] = UpperBound,
    };

    /// <summary>
    /// The manifest_entry schema for <paramref name="formatVersion"/>, with <paramref name="partitionType"/>
    /// as the partition record. <paramref name="withDistinctCounts"/> adds the deprecated
    /// <c>distinct_counts</c> (v1 and v2 only), so a manifest that carried it keeps it through a rewrite.
    /// </summary>
    public static AvroRecordSchema ManifestEntry(
        int formatVersion, IReadOnlyList<PartitionValueType> partitionType, bool withDistinctCounts = false)
    {
        CheckVersion(formatVersion);
        return (AvroRecordSchema)AvroSchemaParser.Parse(Json(w =>
        {
            Record(w, "manifest_entry", () =>
            {
                Field(w, "status", Status, () => w.WriteStringValue("int"));
                if (formatVersion == 1)
                    Field(w, "snapshot_id", SnapshotId, () => w.WriteStringValue("long"));
                else
                {
                    Optional(w, "snapshot_id", SnapshotId, () => w.WriteStringValue("long"));
                    Optional(w, "sequence_number", SequenceNumber, () => w.WriteStringValue("long"));
                    Optional(w, "file_sequence_number", FileSequenceNumber, () => w.WriteStringValue("long"));
                }
                Field(w, "data_file", DataFileId, () => DataFileRecord(w, formatVersion, partitionType, withDistinctCounts));
            });
        }));
    }

    private static void DataFileRecord(
        Utf8JsonWriter w, int formatVersion, IReadOnlyList<PartitionValueType> partitionType, bool withDistinctCounts)
    {
        Record(w, "r2", () =>
        {
            if (formatVersion >= 2)
                Field(w, "content", Content, () => w.WriteStringValue("int"));
            Field(w, "file_path", FilePath, () => w.WriteStringValue("string"));
            Field(w, "file_format", FileFormat, () => w.WriteStringValue("string"));
            Field(w, "partition", Partition, () => PartitionRecord(w, partitionType));
            Field(w, "record_count", RecordCount, () => w.WriteStringValue("long"));
            Field(w, "file_size_in_bytes", FileSizeInBytes, () => w.WriteStringValue("long"));
            if (formatVersion == 1)
                Field(w, "block_size_in_bytes", BlockSizeInBytes, () => w.WriteStringValue("long"));
            IntMap(w, "column_sizes", ColumnSizes, 117, 118, "long");
            IntMap(w, "value_counts", ValueCounts, 119, 120, "long");
            IntMap(w, "null_value_counts", NullValueCounts, 121, 122, "long");
            IntMap(w, "nan_value_counts", NanValueCounts, 138, 139, "long");
            if (withDistinctCounts && formatVersion <= 2)
                IntMap(w, "distinct_counts", DistinctCounts, 123, 124, "long");
            IntMap(w, "lower_bounds", LowerBounds, 126, 127, "bytes");
            IntMap(w, "upper_bounds", UpperBounds, 129, 130, "bytes");
            Optional(w, "key_metadata", KeyMetadata, () => w.WriteStringValue("bytes"));
            Optional(w, "split_offsets", SplitOffsets, () => Array(w, "long", 133));
            if (formatVersion >= 2)
                Optional(w, "equality_ids", EqualityIds, () => Array(w, "int", 136));
            Optional(w, "sort_order_id", SortOrderId, () => w.WriteStringValue("int"));
            if (formatVersion >= 3)
                Optional(w, "first_row_id", FirstRowId, () => w.WriteStringValue("long"));
            if (formatVersion >= 2)
                Optional(w, "referenced_data_file", ReferencedDataFile, () => w.WriteStringValue("string"));
            if (formatVersion >= 3)
            {
                Optional(w, "content_offset", ContentOffset, () => w.WriteStringValue("long"));
                Optional(w, "content_size_in_bytes", ContentSizeInBytes, () => w.WriteStringValue("long"));
            }
        });
    }

    private static void PartitionRecord(Utf8JsonWriter w, IReadOnlyList<PartitionValueType> partitionType)
    {
        Record(w, "r102", () =>
        {
            foreach (var field in partitionType)
                Optional(w, field.Name, field.FieldId, () => IcebergAvroTypes.Write(w, field.Type, field.FieldId));
        });
    }

    /// <summary>The manifest_file (manifest list) schema for <paramref name="formatVersion"/>.</summary>
    public static AvroRecordSchema ManifestFile(int formatVersion)
    {
        CheckVersion(formatVersion);
        return (AvroRecordSchema)AvroSchemaParser.Parse(Json(w =>
        {
            Record(w, "manifest_file", () =>
            {
                Field(w, "manifest_path", ManifestPath, () => w.WriteStringValue("string"));
                Field(w, "manifest_length", ManifestLength, () => w.WriteStringValue("long"));
                Field(w, "partition_spec_id", PartitionSpecId, () => w.WriteStringValue("int"));
                if (formatVersion == 1)
                {
                    Field(w, "added_snapshot_id", AddedSnapshotId, () => w.WriteStringValue("long"));
                    Optional(w, "added_data_files_count", AddedFilesCount, () => w.WriteStringValue("int"));
                    Optional(w, "existing_data_files_count", ExistingFilesCount, () => w.WriteStringValue("int"));
                    Optional(w, "deleted_data_files_count", DeletedFilesCount, () => w.WriteStringValue("int"));
                    Optional(w, "partitions", Partitions, () => SummaryArray(w));
                    Optional(w, "added_rows_count", AddedRowsCount, () => w.WriteStringValue("long"));
                    Optional(w, "existing_rows_count", ExistingRowsCount, () => w.WriteStringValue("long"));
                    Optional(w, "deleted_rows_count", DeletedRowsCount, () => w.WriteStringValue("long"));
                    Optional(w, "key_metadata", ManifestKeyMetadata, () => w.WriteStringValue("bytes"));
                    return;
                }

                Field(w, "content", ManifestContent, () => w.WriteStringValue("int"));
                Field(w, "sequence_number", ListSequenceNumber, () => w.WriteStringValue("long"));
                Field(w, "min_sequence_number", MinSequenceNumber, () => w.WriteStringValue("long"));
                Field(w, "added_snapshot_id", AddedSnapshotId, () => w.WriteStringValue("long"));
                Field(w, "added_files_count", AddedFilesCount, () => w.WriteStringValue("int"));
                Field(w, "existing_files_count", ExistingFilesCount, () => w.WriteStringValue("int"));
                Field(w, "deleted_files_count", DeletedFilesCount, () => w.WriteStringValue("int"));
                Field(w, "added_rows_count", AddedRowsCount, () => w.WriteStringValue("long"));
                Field(w, "existing_rows_count", ExistingRowsCount, () => w.WriteStringValue("long"));
                Field(w, "deleted_rows_count", DeletedRowsCount, () => w.WriteStringValue("long"));
                Optional(w, "partitions", Partitions, () => SummaryArray(w));
                Optional(w, "key_metadata", ManifestKeyMetadata, () => w.WriteStringValue("bytes"));
                if (formatVersion >= 3)
                    Optional(w, "first_row_id", ManifestFirstRowId, () => w.WriteStringValue("long"));
            });
        }));
    }

    private static void SummaryArray(Utf8JsonWriter w)
    {
        w.WriteStartObject();
        w.WriteString("type", "array");
        w.WritePropertyName("items");
        Record(w, "r508", () =>
        {
            Field(w, "contains_null", ContainsNull, () => w.WriteStringValue("boolean"));
            Optional(w, "contains_nan", ContainsNan, () => w.WriteStringValue("boolean"));
            Optional(w, "lower_bound", LowerBound, () => w.WriteStringValue("bytes"));
            Optional(w, "upper_bound", UpperBound, () => w.WriteStringValue("bytes"));
        });
        w.WriteNumber("element-id", FieldSummary);
        w.WriteEndObject();
    }

    private static void CheckVersion(int formatVersion)
    {
        if (formatVersion is < 1 or > TableMetadata.MaxSupportedFormatVersion)
            throw new ArgumentOutOfRangeException(nameof(formatVersion), formatVersion, "Format version must be 1, 2, or 3.");
    }

    // --- JSON building blocks ---

    private static string Json(Action<Utf8JsonWriter> write)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
            write(w);
        return System.Text.Encoding.UTF8.GetString(ms.ToArray());
    }

    private static void Record(Utf8JsonWriter w, string name, Action fields)
    {
        w.WriteStartObject();
        w.WriteString("type", "record");
        w.WriteString("name", name);
        w.WriteStartArray("fields");
        fields();
        w.WriteEndArray();
        w.WriteEndObject();
    }

    private static void Field(Utf8JsonWriter w, string name, int id, Action type)
    {
        w.WriteStartObject();
        w.WriteString("name", name);
        w.WritePropertyName("type");
        type();
        w.WriteNumber("field-id", id);
        w.WriteEndObject();
    }

    private static void Optional(Utf8JsonWriter w, string name, int id, Action type)
    {
        w.WriteStartObject();
        w.WriteString("name", name);
        w.WritePropertyName("type");
        w.WriteStartArray();
        w.WriteStringValue("null");
        type();
        w.WriteEndArray();
        w.WriteNull("default");
        w.WriteNumber("field-id", id);
        w.WriteEndObject();
    }

    private static void Array(Utf8JsonWriter w, string itemType, int elementId)
    {
        w.WriteStartObject();
        w.WriteString("type", "array");
        w.WriteString("items", itemType);
        w.WriteNumber("element-id", elementId);
        w.WriteEndObject();
    }

    // Iceberg writes a map with non-string keys as an array of key/value records, logicalType "map".
    private static void IntMap(Utf8JsonWriter w, string name, int id, int keyId, int valueId, string valueType)
    {
        Optional(w, name, id, () =>
        {
            w.WriteStartObject();
            w.WriteString("type", "array");
            w.WriteString("logicalType", "map");
            w.WritePropertyName("items");
            w.WriteStartObject();
            w.WriteString("type", "record");
            w.WriteString("name", $"k{keyId}_v{valueId}");
            w.WriteStartArray("fields");
            Field(w, "key", keyId, () => w.WriteStringValue("int"));
            Field(w, "value", valueId, () => w.WriteStringValue(valueType));
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndObject();
        });
    }
}

/// <summary>A partition field's id, name and result type: one field of a manifest's partition record.</summary>
internal readonly record struct PartitionValueType(int FieldId, string Name, IcebergType Type);
