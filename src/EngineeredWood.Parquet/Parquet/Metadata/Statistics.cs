// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Column statistics from the Parquet file metadata.
/// Values are stored as raw bytes in the physical type's encoding.
/// </summary>
public sealed class Statistics
{
    /// <summary>Maximum value (using signed comparison for backwards compatibility).</summary>
    public byte[]? Max { get; init; }

    /// <summary>Minimum value (using signed comparison for backwards compatibility).</summary>
    public byte[]? Min { get; init; }

    /// <summary>Count of null values in the column.</summary>
    public long? NullCount { get; init; }

    /// <summary>Count of distinct values in the column.</summary>
    public long? DistinctCount { get; init; }

    /// <summary>Maximum value (using correct logical type ordering).</summary>
    public byte[]? MaxValue { get; init; }

    /// <summary>Minimum value (using correct logical type ordering).</summary>
    public byte[]? MinValue { get; init; }

    /// <summary>Whether max and min values are exact (not truncated).</summary>
    public bool? IsMaxValueExact { get; init; }

    /// <summary>Whether min value is exact (not truncated).</summary>
    public bool? IsMinValueExact { get; init; }

    /// <summary>
    /// Count of NaN values in the column. Only meaningful for the FLOAT and
    /// DOUBLE physical types (and the FLOAT16 logical type). NaNs are excluded
    /// from <see cref="Min"/>/<see cref="Max"/> and <see cref="MinValue"/>/
    /// <see cref="MaxValue"/> under the default TYPE_ORDER ordering.
    /// <para>
    /// <see langword="null"/> means the writer did not record a NaN count; per
    /// PARQUET-2249 readers MUST then assume NaNs may be present (i.e. treat the
    /// count as unknown rather than zero).
    /// </para>
    /// </summary>
    public long? NanCount { get; init; }
}
