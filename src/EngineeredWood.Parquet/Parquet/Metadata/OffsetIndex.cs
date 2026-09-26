// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// The location of every data page in a column chunk, one half of the Parquet page index. It is
/// stored outside the footer; <see cref="ColumnChunk.OffsetIndexOffset"/> points to it.
/// </summary>
public sealed class OffsetIndex
{
    /// <summary>
    /// One entry per data page, in file order. Dictionary pages are not listed.
    /// </summary>
    public required IReadOnlyList<PageLocation> PageLocations { get; init; }

    /// <summary>
    /// For a BYTE_ARRAY column, the unencoded size of each page's values, excluding the
    /// length prefixes. <see langword="null"/> when the writer did not record it.
    /// </summary>
    public IReadOnlyList<long>? UnencodedByteArrayDataBytes { get; init; }
}
