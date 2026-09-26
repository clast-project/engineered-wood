// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Per-page statistics for a column chunk, one half of the Parquet page index. Every list holds
/// one entry per data page, in the order of the chunk's <see cref="OffsetIndex"/>. It is stored
/// outside the footer; <see cref="ColumnChunk.ColumnIndexOffset"/> points to it.
/// </summary>
/// <remarks>
/// Unlike <see cref="Statistics"/>, a column index has no exactness flags: a writer may truncate
/// any bound, so every bound is a bound and not necessarily a value in the page.
/// </remarks>
public sealed class ColumnIndex
{
    /// <summary>
    /// Whether each page holds only nulls. Such a page's <see cref="MinValues"/> and
    /// <see cref="MaxValues"/> entries are empty and carry no meaning.
    /// </summary>
    public required IReadOnlyList<bool> NullPages { get; init; }

    /// <summary>Lower bound of each page's non-null values, in the physical type's encoding.</summary>
    public required IReadOnlyList<byte[]> MinValues { get; init; }

    /// <summary>Upper bound of each page's non-null values, in the physical type's encoding.</summary>
    public required IReadOnlyList<byte[]> MaxValues { get; init; }

    /// <summary>Whether the bounds are ordered across pages.</summary>
    public required BoundaryOrder BoundaryOrder { get; init; }

    /// <summary>Null count of each page, or <see langword="null"/> when the writer did not record them.</summary>
    public IReadOnlyList<long>? NullCounts { get; init; }

    /// <summary>
    /// Repetition level histograms, concatenated page by page: <c>max_repetition_level + 1</c>
    /// entries per page. <see langword="null"/> when absent.
    /// </summary>
    public IReadOnlyList<long>? RepetitionLevelHistograms { get; init; }

    /// <summary>
    /// Definition level histograms, concatenated page by page: <c>max_definition_level + 1</c>
    /// entries per page. <see langword="null"/> when absent.
    /// </summary>
    public IReadOnlyList<long>? DefinitionLevelHistograms { get; init; }

    /// <summary>
    /// NaN count of each page, for FLOAT, DOUBLE and FLOAT16 columns (PARQUET-2249).
    /// <see langword="null"/> means unknown, not zero.
    /// </summary>
    public IReadOnlyList<long>? NanCounts { get; init; }
}
