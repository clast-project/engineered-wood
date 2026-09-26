// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet;

/// <summary>
/// One column chunk's page index, as read by <see cref="ParquetFileReader.ReadPageIndexAsync(int, IReadOnlyList{int}?, CancellationToken)"/>.
/// </summary>
/// <remarks>
/// The bytes are fetched up front and decoded on first access, so a caller that needs only the
/// OffsetIndex never decodes the ColumnIndex. A malformed index surfaces as a
/// <see cref="ParquetFormatException"/> from the property that decodes it.
/// </remarks>
public sealed class ColumnChunkPageIndex
{
    private readonly Lazy<ColumnIndex?> _columnIndex;
    private readonly Lazy<OffsetIndex?> _offsetIndex;

    internal ColumnChunkPageIndex(int column, byte[]? columnIndexBytes, byte[]? offsetIndexBytes)
    {
        Column = column;
        HasColumnIndex = columnIndexBytes is not null;
        HasOffsetIndex = offsetIndexBytes is not null;
        _columnIndex = new Lazy<ColumnIndex?>(() =>
            columnIndexBytes is null ? null : MetadataDecoder.DecodeColumnIndex(columnIndexBytes));
        _offsetIndex = new Lazy<OffsetIndex?>(() =>
            offsetIndexBytes is null ? null : MetadataDecoder.DecodeOffsetIndex(offsetIndexBytes));
    }

    /// <summary>The leaf column's index in the schema.</summary>
    public int Column { get; }

    /// <summary>Whether the chunk has a ColumnIndex (per-page bounds and counts).</summary>
    public bool HasColumnIndex { get; }

    /// <summary>Whether the chunk has an OffsetIndex (per-page locations).</summary>
    public bool HasOffsetIndex { get; }

    /// <summary>
    /// The chunk's per-page bounds, null counts and NaN counts, or null when it has none. Writers
    /// omit it for a column without statistics, for INT96, and for a FLOAT/DOUBLE chunk with an
    /// all-NaN page under TYPE_ORDER.
    /// </summary>
    /// <exception cref="ParquetFormatException">The ColumnIndex is malformed.</exception>
    public ColumnIndex? ColumnIndex => _columnIndex.Value;

    /// <summary>
    /// The location and first row of each data page in the chunk, or null when it has none.
    /// </summary>
    /// <exception cref="ParquetFormatException">The OffsetIndex is malformed.</exception>
    public OffsetIndex? OffsetIndex => _offsetIndex.Value;
}
