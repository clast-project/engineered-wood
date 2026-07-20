// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Specifies how data is written to a Delta table.
/// </summary>
public enum DeltaWriteMode
{
    /// <summary>Append new data to the table without modifying existing files.</summary>
    Append,

    /// <summary>Replace all existing data with the new data.</summary>
    Overwrite,
}
