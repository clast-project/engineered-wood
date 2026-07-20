// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Vortex.Layouts;

namespace EngineeredWood.Vortex;

/// <summary>
/// Materialized per-zone statistics for a single column, decoded from a
/// <c>vortex.stats</c> layout. Each property corresponds to a <see cref="Stat"/>
/// and is non-null exactly when that stat appears in <see cref="PresentStats"/>.
///
/// <para>Each stat array has one row per zone (<see cref="ZoneCount"/>); the
/// final zone may cover fewer than <see cref="ZoneLen"/> rows of the underlying
/// column. Min / Max / Sum arrays are nullable — a zone whose batch was empty
/// or all-null has its validity bit cleared.</para>
///
/// <para>Typical pruning loop:
/// <code>
/// var stats = await reader.GetZoneStatsAsync(columnIdx);
/// var accepted = new HashSet&lt;int&gt;();
/// var max = (Int32Array)stats!.Max!;
/// for (int z = 0; z &lt; stats.ZoneCount; z++)
///     if (max.IsValid(z) &amp;&amp; max.GetValue(z)!.Value &gt;= 100)
///         accepted.Add(z);
/// await foreach (var batch in reader.ReadAllAsync(accepted)) { ... }
/// </code></para>
/// </summary>
public sealed class ZoneStats
{
    /// <summary>Logical row count of each zone (final zone may be shorter).</summary>
    public int ZoneLen { get; }

    /// <summary>Number of zones (= rows in each stat array).</summary>
    public int ZoneCount { get; }

    /// <summary>Stats present for this column, sorted ascending by Stat enum value.</summary>
    public IReadOnlyList<Stat> PresentStats { get; }

    /// <summary>Per-zone min (parent dtype, nullable). Null if Min isn't in <see cref="PresentStats"/>.</summary>
    public IArrowArray? Min { get; }

    /// <summary>Per-zone max (parent dtype, nullable). Null if Max isn't in <see cref="PresentStats"/>.</summary>
    public IArrowArray? Max { get; }

    /// <summary>Per-zone min-truncation flag. Always false for files this writer produces;
    /// upstream may use it to mark approximated min values for long strings.</summary>
    public BooleanArray? MinIsTruncated { get; }

    /// <summary>Per-zone max-truncation flag. See <see cref="MinIsTruncated"/>.</summary>
    public BooleanArray? MaxIsTruncated { get; }

    /// <summary>Per-zone sum (i64 / u64 / f64, nullable).</summary>
    public IArrowArray? Sum { get; }

    /// <summary>Per-zone null count (u64, non-nullable).</summary>
    public UInt64Array? NullCount { get; }

    /// <summary>Per-zone NaN count (u64, non-nullable). Floats only.</summary>
    public UInt64Array? NaNCount { get; }

    /// <summary>Per-zone canonical-Arrow byte size (u64, non-nullable).</summary>
    public UInt64Array? UncompressedSizeInBytes { get; }

    /// <summary>Per-zone "all values equal" flag (bool, nullable).</summary>
    public BooleanArray? IsConstant { get; }

    /// <summary>Per-zone "values weakly sorted" flag (bool, nullable).</summary>
    public BooleanArray? IsSorted { get; }

    /// <summary>Per-zone "values strictly sorted" flag (bool, nullable).</summary>
    public BooleanArray? IsStrictSorted { get; }

    internal ZoneStats(int zoneLen, int zoneCount, IReadOnlyList<Stat> presentStats,
        IArrowArray? min, IArrowArray? max,
        BooleanArray? minIsTruncated, BooleanArray? maxIsTruncated,
        IArrowArray? sum,
        UInt64Array? nullCount, UInt64Array? nanCount, UInt64Array? uncompressedSize,
        BooleanArray? isConstant, BooleanArray? isSorted, BooleanArray? isStrictSorted)
    {
        ZoneLen = zoneLen;
        ZoneCount = zoneCount;
        PresentStats = presentStats;
        Min = min;
        Max = max;
        MinIsTruncated = minIsTruncated;
        MaxIsTruncated = maxIsTruncated;
        Sum = sum;
        NullCount = nullCount;
        NaNCount = nanCount;
        UncompressedSizeInBytes = uncompressedSize;
        IsConstant = isConstant;
        IsSorted = isSorted;
        IsStrictSorted = isStrictSorted;
    }
}
