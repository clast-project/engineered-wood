// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Configuration for a named metadata domain. System-controlled domains
/// have names starting with <c>delta.</c>. The latest action per domain
/// wins during reconciliation; <see cref="Removed"/> acts as a tombstone.
/// </summary>
public sealed record DomainMetadata : DeltaAction
{
    /// <summary>Identifier for the metadata domain.</summary>
    public required string Domain { get; init; }

    /// <summary>JSON-encoded domain configuration.</summary>
    public required string Configuration { get; init; }

    /// <summary>When true, this action acts as a tombstone removing the domain.</summary>
    public required bool Removed { get; init; }
}
