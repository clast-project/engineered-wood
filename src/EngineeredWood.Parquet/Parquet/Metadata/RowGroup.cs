// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Metadata for a row group.
/// </summary>
public sealed class RowGroup
{
    /// <summary>Column chunks in this row group, one per leaf column.</summary>
    public required IReadOnlyList<ColumnChunk> Columns { get; init; }

    /// <summary>Total byte size of all column chunks in this row group (uncompressed).</summary>
    public required long TotalByteSize { get; init; }

    /// <summary>Number of rows in this row group.</summary>
    public required long NumRows { get; init; }

    /// <summary>Sorting columns for this row group, if any.</summary>
    public IReadOnlyList<SortingColumn>? SortingColumns { get; init; }

    /// <summary>Total compressed byte size of all column chunks in this row group.</summary>
    public long? TotalCompressedSize { get; init; }

    /// <summary>Ordinal position of this row group in the file.</summary>
    public short? Ordinal { get; init; }
}
