// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// Identifies whether a snapshot reference is a mutable branch or an immutable tag.
/// </summary>
[JsonConverter(typeof(SnapshotRefTypeConverter))]
public enum SnapshotRefType
{
    /// <summary>A mutable branch that can be fast-forwarded to newer snapshots.</summary>
    Branch,
    /// <summary>An immutable tag pointing to a fixed snapshot.</summary>
    Tag,
}

internal sealed class SnapshotRefTypeConverter : JsonConverter<SnapshotRefType>
{
    public override SnapshotRefType Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            "branch" => SnapshotRefType.Branch,
            "tag" => SnapshotRefType.Tag,
            var s => throw new JsonException($"Unknown snapshot ref type: {s}")
        };

    public override void Write(Utf8JsonWriter writer, SnapshotRefType value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value switch
        {
            SnapshotRefType.Branch => "branch",
            SnapshotRefType.Tag => "tag",
            _ => throw new JsonException($"Unknown snapshot ref type: {value}")
        });
}

/// <summary>
/// A named reference (branch or tag) pointing to a specific snapshot, with optional retention policies.
/// </summary>
public sealed record SnapshotRef
{
    /// <summary>The snapshot this reference points to.</summary>
    public required long SnapshotId { get; init; }
    /// <summary>Whether this reference is a branch or a tag.</summary>
    public required SnapshotRefType Type { get; init; }
    /// <summary>Minimum number of snapshots to keep on a branch for expiration policies.</summary>
    public int? MinSnapshotsToKeep { get; init; }
    /// <summary>Maximum age in milliseconds of snapshots to keep on a branch.</summary>
    public long? MaxSnapshotAgeMs { get; init; }
    /// <summary>Maximum age in milliseconds before this reference itself expires.</summary>
    public long? MaxRefAgeMs { get; init; }

    /// <summary>Creates a branch reference pointing to the specified snapshot.</summary>
    public static SnapshotRef Branch(long snapshotId, int? minSnapshotsToKeep = null, long? maxSnapshotAgeMs = null) =>
        new()
        {
            SnapshotId = snapshotId,
            Type = SnapshotRefType.Branch,
            MinSnapshotsToKeep = minSnapshotsToKeep,
            MaxSnapshotAgeMs = maxSnapshotAgeMs,
        };

    /// <summary>Creates a tag reference pointing to the specified snapshot.</summary>
    public static SnapshotRef Tag(long snapshotId, long? maxRefAgeMs = null) =>
        new()
        {
            SnapshotId = snapshotId,
            Type = SnapshotRefType.Tag,
            MaxRefAgeMs = maxRefAgeMs,
        };
}
