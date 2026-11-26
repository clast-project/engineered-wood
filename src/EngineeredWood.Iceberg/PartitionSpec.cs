// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// Defines how a table is partitioned. Each spec contains an ordered list of partition fields
/// that describe how source columns are transformed into partition values.
/// </summary>
/// <param name="SpecId">The unique identifier for this partition spec.</param>
/// <param name="Fields">The ordered list of partition fields.</param>
public sealed record PartitionSpec(int SpecId, IReadOnlyList<PartitionField> Fields)
{
    /// <summary>A partition spec with no fields, representing an unpartitioned table.</summary>
    public static readonly PartitionSpec Unpartitioned = new(0, []);
}

/// <summary>
/// A single field within a partition spec, mapping a source column through a transform.
/// </summary>
/// <param name="SourceId">The field ID of the source column in the table schema.</param>
/// <param name="FieldId">The unique field ID for this partition field.</param>
/// <param name="Name">The name of the partition field.</param>
/// <param name="Transform">The transform applied to the source column to produce partition values.</param>
/// <param name="SourceIds">
/// The source field ids of a multi-argument transform (v3 <c>source-ids</c>), or <see langword="null"/> for a
/// single-argument one. When set, <paramref name="SourceId"/> is its first element, and the field's
/// transform is one this library cannot evaluate.
/// </param>
[JsonConverter(typeof(PartitionFieldConverter))]
public sealed record PartitionField(
    int SourceId, int FieldId, string Name, Transform Transform, IReadOnlyList<int>? SourceIds = null)
{
    /// <summary>
    /// Partition-field properties this library does not model, kept so that rewriting another engine's
    /// metadata does not drop them. <see langword="null"/> when there are none.
    /// </summary>
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }
}
