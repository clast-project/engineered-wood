// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// A table-statistics (Puffin) file registered in table metadata's <c>statistics</c> list. Statistics are
/// informational: readers may ignore them, but a writer must carry them through a metadata rewrite.
/// </summary>
/// <param name="SnapshotId">The snapshot the statistics were computed for.</param>
/// <param name="StatisticsPath">The path of the Puffin statistics file.</param>
/// <param name="FileSizeInBytes">The size of the statistics file.</param>
/// <param name="FileFooterSizeInBytes">The total size of the file's footer, not just the footer payload.</param>
/// <param name="BlobMetadata">The blobs the file holds.</param>
/// <param name="KeyMetadata">Base64 implementation-specific encryption key metadata, if the file is encrypted.</param>
public sealed record StatisticsFile(
    long SnapshotId,
    string StatisticsPath,
    long FileSizeInBytes,
    long FileFooterSizeInBytes,
    IReadOnlyList<BlobMetadata> BlobMetadata,
    string? KeyMetadata = null)
{
    /// <summary>Properties this library does not model, kept so that a rewrite does not drop them.</summary>
    [JsonExtensionData, JsonInclude]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }
}

/// <summary>One blob of a <see cref="StatisticsFile"/>.</summary>
/// <param name="Type">The blob type, matching the Puffin blob type (e.g. <c>apache-datasketches-theta-v1</c>).</param>
/// <param name="SnapshotId">The snapshot the blob was computed for.</param>
/// <param name="SequenceNumber">The sequence number of that snapshot.</param>
/// <param name="Fields">The field ids, in order, the statistic was computed on.</param>
/// <param name="Properties">Additional blob properties.</param>
public sealed record BlobMetadata(
    string Type,
    long SnapshotId,
    long SequenceNumber,
    IReadOnlyList<int> Fields,
    IReadOnlyDictionary<string, string>? Properties = null)
{
    /// <summary>Properties this library does not model, kept so that a rewrite does not drop them.</summary>
    [JsonExtensionData, JsonInclude]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }
}

/// <summary>
/// A partition-statistics file registered in table metadata's <c>partition-statistics</c> list. At most one
/// is registered per snapshot.
/// </summary>
/// <param name="SnapshotId">The snapshot the statistics were computed for.</param>
/// <param name="StatisticsPath">The path of the partition statistics file.</param>
/// <param name="FileSizeInBytes">The size of the partition statistics file.</param>
public sealed record PartitionStatisticsFile(long SnapshotId, string StatisticsPath, long FileSizeInBytes)
{
    /// <summary>Properties this library does not model, kept so that a rewrite does not drop them.</summary>
    [JsonExtensionData, JsonInclude]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }
}
