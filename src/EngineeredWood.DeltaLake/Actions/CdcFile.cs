// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// References a file containing change data capture (CDC) records
/// for a commit. CDC files are stored in the <c>_change_data</c> directory
/// and contain rows with an additional <c>_change_type</c> column.
/// </summary>
public sealed record CdcFile : DeltaAction
{
    /// <summary>URI-encoded path to the CDC file.</summary>
    public required string Path { get; init; }

    /// <summary>Partition column values for this file.</summary>
    public required IReadOnlyDictionary<string, string> PartitionValues { get; init; }

    /// <summary>File size in bytes.</summary>
    public required long Size { get; init; }

    /// <summary>Must be false — CDC files do not represent data changes themselves.</summary>
    public required bool DataChange { get; init; }

    /// <summary>Optional metadata tags.</summary>
    public IReadOnlyDictionary<string, string>? Tags { get; init; }
}
