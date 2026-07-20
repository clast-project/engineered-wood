// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Model for the <c>_last_checkpoint</c> JSON file that points to
/// the most recent checkpoint for fast log replay bootstrapping.
/// </summary>
public sealed record LastCheckpointInfo
{
    /// <summary>The version number of the checkpoint.</summary>
    public required long Version { get; init; }

    /// <summary>The number of actions in the checkpoint.</summary>
    public required long Size { get; init; }

    /// <summary>
    /// Number of parts for multi-part checkpoints.
    /// Null for single-file checkpoints.
    /// </summary>
    public int? Parts { get; init; }

    /// <summary>Total size of the checkpoint in bytes.</summary>
    public long? SizeInBytes { get; init; }

    /// <summary>Number of add file actions in the checkpoint.</summary>
    public int? NumOfAddFiles { get; init; }

    /// <summary>
    /// Path to the V2 checkpoint file, relative to the table root.
    /// Null for V1 checkpoints.
    /// </summary>
    public string? V2CheckpointPath { get; init; }

    /// <summary>Whether this is a V2 checkpoint.</summary>
    public bool IsV2 => V2CheckpointPath is not null;
}
