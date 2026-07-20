// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Describes a deletion vector that marks rows as logically deleted
/// while they remain physically present in the data file.
/// </summary>
public sealed record DeletionVector
{
    /// <summary>
    /// Storage type: <c>"u"</c> (UUID-relative path), <c>"i"</c> (inline),
    /// or <c>"p"</c> (absolute path).
    /// </summary>
    public required string StorageType { get; init; }

    /// <summary>
    /// For inline DVs, the Base85-encoded RoaringBitmapArray.
    /// For file-based DVs, the path to the DV file.
    /// </summary>
    public required string PathOrInlineDv { get; init; }

    /// <summary>
    /// Byte offset within the file for file-based DVs. Null for inline DVs.
    /// </summary>
    public int? Offset { get; init; }

    /// <summary>Serialized size of the DV in bytes.</summary>
    public required int SizeInBytes { get; init; }

    /// <summary>Number of rows marked as deleted.</summary>
    public required long Cardinality { get; init; }

    /// <summary>
    /// Unique identifier for this DV, derived from storage type, path/data, and offset.
    /// Used as part of the reconciliation key for file actions.
    /// </summary>
    public string UniqueId => StorageType switch
    {
        "i" => $"i{PathOrInlineDv}",
        _ => $"{StorageType}{PathOrInlineDv}@{Offset ?? 0}"
    };
}
