// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;
using EngineeredWood.Expressions;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>Indicates the kind of content stored in a data file.</summary>
public enum FileContent
{
    /// <summary>Regular data rows.</summary>
    Data = 0,
    /// <summary>Position-based delete entries.</summary>
    PositionDeletes = 1,
    /// <summary>Equality-based delete entries.</summary>
    EqualityDeletes = 2,
}

/// <summary>The columnar file format used to store data.</summary>
[JsonConverter(typeof(FileFormatConverter))]
public enum FileFormat
{
    /// <summary>Apache Avro format.</summary>
    Avro,
    /// <summary>Apache Parquet format.</summary>
    Parquet,
    /// <summary>Apache ORC format.</summary>
    Orc,
}

internal sealed class FileFormatConverter : JsonConverter<FileFormat>
{
    public override FileFormat Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            "AVRO" => FileFormat.Avro,
            "PARQUET" => FileFormat.Parquet,
            "ORC" => FileFormat.Orc,
            var s => throw new JsonException($"Unknown file format: {s}")
        };

    public override void Write(Utf8JsonWriter writer, FileFormat value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value switch
        {
            FileFormat.Avro => "AVRO",
            FileFormat.Parquet => "PARQUET",
            FileFormat.Orc => "ORC",
            _ => throw new JsonException($"Unknown file format: {value}")
        });
}

/// <summary>Status of a manifest entry relative to the snapshot that contains it.</summary>
public enum ManifestEntryStatus
{
    /// <summary>The file existed in a prior snapshot and is carried forward.</summary>
    Existing = 0,
    /// <summary>The file was added in the containing snapshot.</summary>
    Added = 1,
    /// <summary>The file was deleted in the containing snapshot.</summary>
    Deleted = 2,
}

/// <summary>Indicates whether a manifest tracks data files or delete files.</summary>
public enum ManifestContent
{
    /// <summary>The manifest contains data file entries.</summary>
    Data = 0,
    /// <summary>The manifest contains delete file entries.</summary>
    Deletes = 1,
}

/// <summary>
/// Describes a single data or delete file tracked by a manifest, including its location, format, size, and optional statistics.
/// </summary>
public sealed record DataFile
{
    /// <summary>Whether this file holds data, position deletes, or equality deletes.</summary>
    public FileContent Content { get; init; }
    /// <summary>Full path to the file.</summary>
    public required string FilePath { get; init; }
    /// <summary>Columnar format of the file.</summary>
    public FileFormat FileFormat { get; init; } = FileFormat.Parquet;
    /// <summary>Number of records in the file.</summary>
    public required long RecordCount { get; init; }
    /// <summary>Total file size in bytes.</summary>
    public required long FileSizeInBytes { get; init; }
    /// <summary>Per-column sizes in bytes, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? ColumnSizes { get; init; }
    /// <summary>Per-column non-null value counts, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? ValueCounts { get; init; }
    /// <summary>Per-column null value counts, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? NullValueCounts { get; init; }
    /// <summary>Suggested split points within the file.</summary>
    public IReadOnlyList<long>? SplitOffsets { get; init; }
    /// <summary>Identifier of the sort order used to write the file.</summary>
    public int? SortOrderId { get; init; }

    /// <summary>Per-column lower bounds for scan planning, keyed by field ID. Not serialized.</summary>
    [JsonIgnore] public IReadOnlyDictionary<int, LiteralValue>? ColumnLowerBounds { get; init; }
    /// <summary>Per-column upper bounds for scan planning, keyed by field ID. Not serialized.</summary>
    [JsonIgnore] public IReadOnlyDictionary<int, LiteralValue>? ColumnUpperBounds { get; init; }

    /// <summary>First row ID assigned to rows in this file (v3).</summary>
    public long? FirstRowId { get; init; }

    /// <summary>Path of the data file this deletion vector applies to (v3).</summary>
    public string? ReferencedDataFile { get; init; }
    /// <summary>Byte offset of the deletion vector content in the file (v3).</summary>
    public long? ContentOffset { get; init; }
    /// <summary>Size in bytes of the deletion vector content (v3).</summary>
    public long? ContentSizeInBytes { get; init; }
}

/// <summary>
/// An entry within a manifest file, tracking a single data or delete file and its lifecycle status.
/// </summary>
public sealed record ManifestEntry
{
    /// <summary>Whether the file is existing, added, or deleted in the containing snapshot.</summary>
    public required ManifestEntryStatus Status { get; init; }
    /// <summary>Snapshot ID that added or deleted this entry.</summary>
    public long? SnapshotId { get; init; }
    /// <summary>Data sequence number inherited from the snapshot that added this entry.</summary>
    public long? SequenceNumber { get; init; }
    /// <summary>File sequence number for ordering within the same snapshot.</summary>
    public long? FileSequenceNumber { get; init; }
    /// <summary>The data or delete file described by this entry.</summary>
    public required DataFile DataFile { get; init; }
}

/// <summary>
/// An entry in a manifest list (snapshot file), pointing to a single manifest and summarizing its contents.
/// </summary>
public sealed record ManifestListEntry
{
    /// <summary>Path to the manifest file.</summary>
    public required string ManifestPath { get; init; }
    /// <summary>Length of the manifest file in bytes.</summary>
    public required long ManifestLength { get; init; }
    /// <summary>Partition spec ID used by the manifest's entries.</summary>
    public int PartitionSpecId { get; init; }
    /// <summary>Whether the manifest tracks data files or delete files.</summary>
    public ManifestContent Content { get; init; }
    /// <summary>Sequence number when this manifest was added.</summary>
    public long SequenceNumber { get; init; }
    /// <summary>Minimum sequence number of all entries in this manifest.</summary>
    public long MinSequenceNumber { get; init; }
    /// <summary>Snapshot ID that created this manifest.</summary>
    public required long AddedSnapshotId { get; init; }
    /// <summary>Number of data file entries with Added status.</summary>
    public int AddedDataFilesCount { get; init; }
    /// <summary>Number of data file entries with Existing status.</summary>
    public int ExistingDataFilesCount { get; init; }
    /// <summary>Number of data file entries with Deleted status.</summary>
    public int DeletedDataFilesCount { get; init; }
    /// <summary>Total rows across added data files.</summary>
    public long AddedRowsCount { get; init; }
    /// <summary>Total rows across existing data files.</summary>
    public long ExistingRowsCount { get; init; }
    /// <summary>Total rows across deleted data files.</summary>
    public long DeletedRowsCount { get; init; }

    /// <summary>First row ID assigned to rows in this manifest (v3).</summary>
    public long? FirstRowId { get; init; }
}
