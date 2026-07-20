// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet;

/// <summary>
/// Exception thrown when a Parquet file is malformed or contains unexpected data.
/// </summary>
public sealed class ParquetFormatException : Exception
{
    public ParquetFormatException(string message) : base(message) { }

    public ParquetFormatException(string message, Exception innerException)
        : base(message, innerException) { }
}
