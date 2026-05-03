// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex;

/// <summary>
/// Thrown when a Vortex file is malformed, truncated, or uses a file format
/// version that this reader does not support.
/// </summary>
public sealed class VortexFormatException : Exception
{
    public VortexFormatException(string message) : base(message) { }

    public VortexFormatException(string message, Exception innerException)
        : base(message, innerException) { }
}
