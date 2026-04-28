// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table;

/// <summary>
/// Exception thrown when a Lance dataset directory or manifest cannot be
/// parsed (corrupt envelope, unsupported version, missing required field).
/// </summary>
public sealed class LanceTableFormatException : Exception
{
    public LanceTableFormatException(string message) : base(message) { }
    public LanceTableFormatException(string message, Exception inner) : base(message, inner) { }
}
