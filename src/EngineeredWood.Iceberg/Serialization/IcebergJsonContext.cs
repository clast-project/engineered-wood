// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json.Serialization;
using EngineeredWood.Iceberg.Manifest;

namespace EngineeredWood.Iceberg.Serialization;

/// <summary>
/// Source-generated JSON serialization context for all Iceberg types.
/// Enables AOT compilation and reduces startup/allocation overhead.
/// </summary>
[JsonSerializable(typeof(TableMetadata))]
[JsonSerializable(typeof(Schema))]
[JsonSerializable(typeof(NestedField))]
[JsonSerializable(typeof(Snapshot))]
[JsonSerializable(typeof(SnapshotLogEntry))]
[JsonSerializable(typeof(MetadataLogEntry))]
[JsonSerializable(typeof(PartitionSpec))]
[JsonSerializable(typeof(PartitionField))]
[JsonSerializable(typeof(SortOrder))]
[JsonSerializable(typeof(SortField))]
[JsonSerializable(typeof(SnapshotRef))]
[JsonSerializable(typeof(Namespace))]
[JsonSerializable(typeof(TableIdentifier))]
[JsonSerializable(typeof(DataFile))]
[JsonSerializable(typeof(ManifestEntry))]
[JsonSerializable(typeof(ManifestListEntry))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, string>))]
[JsonSerializable(typeof(IReadOnlyList<Schema>))]
[JsonSerializable(typeof(IReadOnlyList<PartitionSpec>))]
[JsonSerializable(typeof(IReadOnlyList<SortOrder>))]
[JsonSerializable(typeof(IReadOnlyList<Snapshot>))]
[JsonSerializable(typeof(IReadOnlyList<SnapshotLogEntry>))]
[JsonSerializable(typeof(IReadOnlyList<MetadataLogEntry>))]
[JsonSerializable(typeof(IReadOnlyList<NestedField>))]
[JsonSerializable(typeof(IReadOnlyList<PartitionField>))]
[JsonSerializable(typeof(IReadOnlyList<SortField>))]
[JsonSerializable(typeof(IReadOnlyList<int>))]
[JsonSerializable(typeof(List<ManifestEntry>))]
[JsonSerializable(typeof(List<ManifestListEntry>))]
[JsonSerializable(typeof(IReadOnlyList<ManifestEntry>))]
[JsonSerializable(typeof(IReadOnlyList<ManifestListEntry>))]
[JsonSerializable(typeof(List<NestedField>))]
[JsonSerializable(typeof(List<int>))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.Unspecified,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = false)]
internal partial class IcebergJsonContext : JsonSerializerContext;
