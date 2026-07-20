// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Describes details about a V2 checkpoint. Must be present exactly once
/// in every V2 spec checkpoint.
/// </summary>
public sealed record CheckpointMetadata : DeltaAction
{
    /// <summary>The checkpoint version.</summary>
    public required long Version { get; init; }

    /// <summary>Optional metadata tags.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }
}
