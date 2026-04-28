// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance;

/// <summary>
/// Thrown when a Lance file is malformed, truncated, or uses a file format
/// version that this reader does not support.
/// </summary>
public sealed class LanceFormatException : Exception
{
    public LanceFormatException(string message) : base(message) { }

    public LanceFormatException(string message, Exception innerException)
        : base(message, innerException) { }
}
