// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Records the progress of an application writing to the table,
/// enabling idempotent streaming writes. The latest transaction
/// per <see cref="AppId"/> wins during reconciliation.
/// </summary>
public sealed record TransactionId : DeltaAction
{
    /// <summary>Unique identifier for the writing application.</summary>
    public required string AppId { get; init; }

    /// <summary>Application-specific numeric version (monotonically increasing).</summary>
    public required long Version { get; init; }

    /// <summary>Timestamp of the last update in milliseconds since epoch.</summary>
    public long? LastUpdated { get; init; }
}
