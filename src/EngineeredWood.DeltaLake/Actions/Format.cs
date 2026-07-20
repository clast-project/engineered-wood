// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// The storage format of the data files in a Delta table.
/// The provider is always <c>"parquet"</c> for Delta tables.
/// </summary>
public sealed record Format
{
    /// <summary>The format provider name (always "parquet").</summary>
    public required string Provider { get; init; }

    /// <summary>Format-specific options.</summary>
    public IReadOnlyDictionary<string, string> Options { get; init; } =
        new Dictionary<string, string>();

    /// <summary>Default Parquet format.</summary>
    public static Format Parquet { get; } = new() { Provider = "parquet" };
}
