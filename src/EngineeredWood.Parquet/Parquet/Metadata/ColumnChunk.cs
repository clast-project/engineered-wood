// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Location and metadata of a column chunk within a row group.
/// </summary>
public sealed class ColumnChunk
{
    /// <summary>File path if the column chunk is in an external file. Null means same file.</summary>
    public string? FilePath { get; init; }

    /// <summary>Byte offset of the column chunk in the file.</summary>
    public required long FileOffset { get; init; }

    /// <summary>Column metadata. May be null if stored in a separate metadata file.</summary>
    public ColumnMetaData? MetaData { get; init; }
}
