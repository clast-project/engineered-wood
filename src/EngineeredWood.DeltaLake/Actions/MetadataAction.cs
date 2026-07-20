// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Changes the table's metadata. The first version of a table must contain
/// a metadata action. Subsequent metadata actions completely replace the previous one.
/// At most one metadata action may appear in a single commit.
/// </summary>
public sealed record MetadataAction : DeltaAction
{
    /// <summary>Unique table identifier (GUID).</summary>
    public required string Id { get; init; }

    /// <summary>User-provided table name.</summary>
    public string? Name { get; init; }

    /// <summary>User-provided table description.</summary>
    public string? Description { get; init; }

    /// <summary>Storage format of data files (always Parquet for Delta).</summary>
    public required Format Format { get; init; }

    /// <summary>JSON-encoded Delta table schema.</summary>
    public required string SchemaString { get; init; }

    /// <summary>Ordered list of partition column names.</summary>
    public required IReadOnlyList<string> PartitionColumns { get; init; }

    /// <summary>Table configuration properties (e.g., <c>delta.appendOnly</c>).</summary>
    public IReadOnlyDictionary<string, string>? Configuration { get; init; }

    /// <summary>Table creation time in milliseconds since epoch.</summary>
    public long? CreatedTime { get; init; }
}
