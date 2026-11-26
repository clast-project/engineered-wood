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
    /// <summary>Puffin, the format of v3 deletion vectors.</summary>
    Puffin,
}

internal sealed class FileFormatConverter : JsonConverter<FileFormat>
{
    public override FileFormat Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            "AVRO" => FileFormat.Avro,
            "PARQUET" => FileFormat.Parquet,
            "ORC" => FileFormat.Orc,
            "PUFFIN" => FileFormat.Puffin,
            var s => throw new JsonException($"Unknown file format: {s}")
        };

    public override void Write(Utf8JsonWriter writer, FileFormat value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value switch
        {
            FileFormat.Avro => "AVRO",
            FileFormat.Parquet => "PARQUET",
            FileFormat.Orc => "ORC",
            FileFormat.Puffin => "PUFFIN",
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
    /// <summary>
    /// The file's partition tuple, typed by its partition spec, or <see langword="null"/> for an unpartitioned
    /// file. Not part of the JSON manifest format.
    /// </summary>
    [JsonIgnore] public PartitionData? Partition { get; init; }
    /// <summary>The v1 <c>block_size_in_bytes</c>, which v1 manifests require; <see langword="null"/> otherwise.</summary>
    public long? BlockSizeInBytes { get; init; }
    /// <summary>Per-column sizes in bytes, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? ColumnSizes { get; init; }
    /// <summary>Per-column non-null value counts, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? ValueCounts { get; init; }
    /// <summary>Per-column null value counts, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? NullValueCounts { get; init; }
    /// <summary>Per-column NaN value counts, keyed by field ID.</summary>
    public IReadOnlyDictionary<int, long>? NanValueCounts { get; init; }
    /// <summary>Per-column distinct value counts, keyed by field ID (deprecated; never written in v3).</summary>
    public IReadOnlyDictionary<int, long>? DistinctCounts { get; init; }
    /// <summary>
    /// Per-column lower bounds exactly as the manifest stores them (Appendix D single-value binary), keyed by
    /// field ID. Kept so that a rewrite writes them back unchanged.
    /// </summary>
    public IReadOnlyDictionary<int, byte[]>? LowerBounds { get; init; }
    /// <summary>Per-column upper bounds as the manifest stores them; see <see cref="LowerBounds"/>.</summary>
    public IReadOnlyDictionary<int, byte[]>? UpperBounds { get; init; }
    /// <summary>
    /// Encryption key metadata. A file that has any is encrypted, and this library refuses to read or
    /// rewrite its manifest (see <see cref="UnsupportedTableFeatureException"/>).
    /// </summary>
    public byte[]? KeyMetadata { get; init; }
    /// <summary>For an equality delete file, the field IDs its rows are matched on.</summary>
    public IReadOnlyList<int>? EqualityIds { get; init; }
    /// <summary>Suggested split points within the file.</summary>
    public IReadOnlyList<long>? SplitOffsets { get; init; }
    /// <summary>Identifier of the sort order used to write the file.</summary>
    public int? SortOrderId { get; init; }

    /// <summary>
    /// Per-column lower bounds for scan planning, keyed by field ID. Not serialized: a manifest's bounds are
    /// in <see cref="LowerBounds"/>.
    /// </summary>
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
    // The counts are required from v2 on. A v1 manifest list may leave them out, which means "unknown", and
    // readers must then assume the manifest holds such files: null is not zero.

    /// <summary>Number of entries with Added status, or <see langword="null"/> if unknown (v1).</summary>
    public int? AddedDataFilesCount { get; init; }
    /// <summary>Number of entries with Existing status, or <see langword="null"/> if unknown (v1).</summary>
    public int? ExistingDataFilesCount { get; init; }
    /// <summary>Number of entries with Deleted status, or <see langword="null"/> if unknown (v1).</summary>
    public int? DeletedDataFilesCount { get; init; }
    /// <summary>Total rows across added files, or <see langword="null"/> if unknown (v1).</summary>
    public long? AddedRowsCount { get; init; }
    /// <summary>Total rows across existing files, or <see langword="null"/> if unknown (v1).</summary>
    public long? ExistingRowsCount { get; init; }
    /// <summary>Total rows across deleted files, or <see langword="null"/> if unknown (v1).</summary>
    public long? DeletedRowsCount { get; init; }

    /// <summary>
    /// One summary per partition field of the manifest's spec, or <see langword="null"/> when the manifest
    /// list has none (readers then cannot skip the manifest by partition).
    /// </summary>
    public IReadOnlyList<PartitionFieldSummary>? Partitions { get; init; }

    /// <summary>
    /// Encryption key metadata. A manifest that has any is encrypted, and this library refuses to read it
    /// (see <see cref="UnsupportedTableFeatureException"/>).
    /// </summary>
    public byte[]? KeyMetadata { get; init; }

    /// <summary>First row ID assigned to rows in this manifest (v3).</summary>
    public long? FirstRowId { get; init; }
}

/// <summary>
/// A manifest list's summary of one partition field over a manifest's files, used to skip the manifest.
/// </summary>
/// <param name="ContainsNull">Whether any file's value for the field is null.</param>
/// <param name="ContainsNan">Whether any file's value is NaN, or <see langword="null"/> if unknown.</param>
/// <param name="LowerBound">
/// The least non-null, non-NaN value, as Appendix D single-value binary of the partition field's type, or
/// <see langword="null"/> when every value is null or NaN.
/// </param>
/// <param name="UpperBound">The greatest such value; see <paramref name="LowerBound"/>.</param>
public sealed record PartitionFieldSummary(bool ContainsNull, bool? ContainsNan, byte[]? LowerBound, byte[]? UpperBound);
