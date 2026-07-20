// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Specifies the minimum reader and writer protocol versions required
/// to correctly interpret the table. Only one protocol action is active
/// at a time; the latest one wins.
/// </summary>
public sealed record ProtocolAction : DeltaAction
{
    /// <summary>Minimum reader protocol version required.</summary>
    public required int MinReaderVersion { get; init; }

    /// <summary>Minimum writer protocol version required.</summary>
    public required int MinWriterVersion { get; init; }

    /// <summary>
    /// Named reader features required (Reader v3 only).
    /// Null for Reader v1/v2.
    /// </summary>
    public IReadOnlyList<string>? ReaderFeatures { get; init; }

    /// <summary>
    /// Named writer features required (Writer v7 only).
    /// Null for Writer v2–v6.
    /// </summary>
    public IReadOnlyList<string>? WriterFeatures { get; init; }
}
