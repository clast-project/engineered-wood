// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Resolves the keys of an <c>add.partitionValues</c> map under column mapping. The Delta spec (and Spark) key
/// it by the PHYSICAL column name; engineered-wood commits before 2026-07 keyed it by the logical name.
///
/// <para>A map's spelling is decided ONCE, from all of its keys, never key by key. Key by key goes wrong either
/// way on a table upgraded to name mode, whose original names stay as physical names. After renaming <c>p1</c>
/// to <c>x</c> and then <c>p2</c> to <c>p1</c>, the logical name <c>p1</c> is the physical key of <c>x</c>, so
/// logical-first bound the wrong column in a spec-keyed map; and in a logical-keyed map <c>{x, p1}</c>,
/// physical-first found <c>x</c>'s physical key <c>p1</c> and read the other column's value.</para>
///
/// <para>The keys a map HAS decide it, not the keys it would need to be complete: a NULL partition value may be
/// a missing key (checkpoints written from a log whose null entries were dropped hold such maps), and requiring
/// every physical key read the rest of such a map as logical-keyed and lost its values. A map is physical when
/// every key is some partition column's physical name and one of them is no column's logical name, and logical
/// in the mirror case.</para>
///
/// <para>When every key is both a logical and a physical name, the keys cannot decide. If the two readings
/// bind some key to different columns, as renames that PERMUTE names do, the map is ambiguous
/// (<see cref="IsAmbiguous"/>): a read follows the spec (physical), and the pruner declines.</para>
/// </summary>
internal static class PartitionValueKeys
{
    /// <summary>
    /// True when <paramref name="values"/> is keyed by physical name, decided from the keys it holds; a map its
    /// keys cannot decide is read in the spec's spelling, physical. With no column mapping the two spellings are
    /// the same.
    /// </summary>
    public static bool IsPhysicallyKeyed<TValue>(
        IReadOnlyDictionary<string, TValue> values, IReadOnlyList<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical) =>
        Decide(values, partitionColumns, logicalToPhysical) ?? true;

    /// <summary>
    /// True when <paramref name="values"/>'s keys cannot decide its spelling and the two readings differ: renames
    /// that permute names (logical <c>p1</c> physical <c>p2</c>, logical <c>p2</c> physical <c>p1</c>) leave
    /// keys that say nothing about which column each belongs to. <see cref="TryGet"/> then follows the spec
    /// (physical), as a read must produce some value; a consumer that may decline, such as the pruner, should.
    /// </summary>
    public static bool IsAmbiguous<TValue>(
        IReadOnlyDictionary<string, TValue> values, IReadOnlyList<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        if (Decide(values, partitionColumns, logicalToPhysical) is not null)
            return false;
        // Every partition key present is a logical name and a physical one; ambiguous when one of them is a
        // physical name of a column other than the one it names logically.
        for (int i = 0; i < partitionColumns.Count; i++)
        {
            string physical = Physical(partitionColumns[i], logicalToPhysical);
            if (values.ContainsKey(physical) && !string.Equals(physical, partitionColumns[i], StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Looks up the value of the partition column whose LOGICAL name is <paramref name="column"/>, in the map's
    /// own spelling. <paramref name="logicalToPhysical"/> may be null, or omit a column whose physical name is
    /// its logical one.
    /// </summary>
    public static bool TryGet<TValue>(
        IReadOnlyDictionary<string, TValue> values, string column, IReadOnlyList<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical, out TValue value)
    {
        string key = IsPhysicallyKeyed(values, partitionColumns, logicalToPhysical)
            ? Physical(column, logicalToPhysical)
            : column;
        return values.TryGetValue(key, out value!);
    }

    /// <summary>
    /// The physical spelling of one key of a map whose spelling <see cref="IsPhysicallyKeyed"/> decided.
    /// </summary>
    public static string ToPhysical(
        string key, bool physicallyKeyed, IReadOnlyDictionary<string, string>? logicalToPhysical) =>
        physicallyKeyed ? key : Physical(key, logicalToPhysical);

    // True for physical, false for logical, null when the keys cannot tell. Only keys that are some partition
    // column's name in one spelling or the other are looked at; anything else in the map says nothing. Index
    // loops over the handful of partition columns, so the hot read and pruning paths allocate nothing.
    private static bool? Decide<TValue>(
        IReadOnlyDictionary<string, TValue> values, IReadOnlyList<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        bool allPhysical = true, allLogical = true, physicalOnly = false, logicalOnly = false;
        for (int i = 0; i < partitionColumns.Count; i++)
        {
            string logicalName = partitionColumns[i];
            string physicalName = Physical(logicalName, logicalToPhysical);
            for (int spelling = 0; spelling < 2; spelling++)
            {
                string key = spelling == 0 ? logicalName : physicalName;
                if ((spelling == 1 && ReferenceEquals(key, logicalName)) || !values.ContainsKey(key))
                    continue;
                bool isPhysical = spelling == 1 || IsPhysicalName(key, partitionColumns, logicalToPhysical);
                bool isLogical = spelling == 0 || IsLogicalName(key, partitionColumns);
                allPhysical &= isPhysical;
                allLogical &= isLogical;
                physicalOnly |= isPhysical && !isLogical;
                logicalOnly |= isLogical && !isPhysical;
            }
        }
        if (allPhysical && physicalOnly)
            return true;
        if (allLogical && logicalOnly)
            return false;
        return null;
    }

    private static bool IsLogicalName(string key, IReadOnlyList<string> partitionColumns)
    {
        for (int i = 0; i < partitionColumns.Count; i++)
            if (string.Equals(partitionColumns[i], key, StringComparison.Ordinal))
                return true;
        return false;
    }

    private static bool IsPhysicalName(
        string key, IReadOnlyList<string> partitionColumns, IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        for (int i = 0; i < partitionColumns.Count; i++)
            if (string.Equals(Physical(partitionColumns[i], logicalToPhysical), key, StringComparison.Ordinal))
                return true;
        return false;
    }

    private static string Physical(string logical, IReadOnlyDictionary<string, string>? logicalToPhysical) =>
        logicalToPhysical is not null && logicalToPhysical.TryGetValue(logical, out var physical)
            ? physical
            : logical;
}
