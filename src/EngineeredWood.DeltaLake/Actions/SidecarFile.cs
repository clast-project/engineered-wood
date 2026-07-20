// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// References a sidecar Parquet file that contains add/remove file actions
/// for a V2 checkpoint. Sidecar files reside in <c>_delta_log/_sidecars/</c>.
/// </summary>
public sealed record SidecarFile : DeltaAction
{
    /// <summary>
    /// URI-encoded path to the sidecar file. Typically just the filename
    /// since sidecars always reside in <c>_delta_log/_sidecars/</c>.
    /// </summary>
    public required string Path { get; init; }

    /// <summary>Size of the sidecar file in bytes.</summary>
    public required long SizeInBytes { get; init; }

    /// <summary>Modification time in milliseconds since epoch.</summary>
    public required long ModificationTime { get; init; }

    /// <summary>Optional metadata tags.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }
}
