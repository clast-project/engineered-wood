// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc;

/// <summary>
/// Options for configuring the ORC row reader.
/// </summary>
public sealed class OrcReaderOptions
{
    /// <summary>
    /// The columns to read, by name. If null or empty, all columns are read.
    /// </summary>
    public IReadOnlyList<string>? Columns { get; set; }

    /// <summary>
    /// The number of rows to read per batch. Default is 1024.
    /// </summary>
    public int BatchSize { get; set; } = 1024;
}
