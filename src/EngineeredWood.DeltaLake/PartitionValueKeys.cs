// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Resolves the keys of an <c>add.partitionValues</c> map under column mapping. The Delta spec (and Spark) key
/// it by the PHYSICAL column name; engineered-wood commits before 2026-07 keyed it by the logical name. Every
/// lookup is PHYSICAL FIRST: on a table upgraded to name mode the original names stay as physical names, so
/// after renaming <c>p1</c> to <c>x</c> and then <c>p2</c> to <c>p1</c>, the logical name <c>p1</c> is the
/// physical key of <c>x</c>. Trying the logical name first bound the wrong column. A spec-keyed file carries
/// every partition column's physical key, so the logical fallback is only reached for a logical-keyed one.
/// </summary>
internal static class PartitionValueKeys
{
    /// <summary>
    /// Looks up the value of the partition column whose LOGICAL name is <paramref name="column"/>.
    /// <paramref name="logicalToPhysical"/> may be null, or omit a column whose physical name is its logical one.
    /// </summary>
    public static bool TryGet<TValue>(
        IReadOnlyDictionary<string, TValue> values, string column,
        IReadOnlyDictionary<string, string>? logicalToPhysical, out TValue value)
    {
        if (logicalToPhysical is not null
            && logicalToPhysical.TryGetValue(column, out var physical)
            && values.TryGetValue(physical, out value!))
        {
            return true;
        }
        return values.TryGetValue(column, out value!);
    }

    /// <summary>
    /// The physical spelling of one key of a file's partitionValues: the key itself when it is already some
    /// column's physical name, else the physical name of the column it is the logical name of.
    /// </summary>
    public static string ToPhysical(string key, IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        if (logicalToPhysical is null || logicalToPhysical.Count == 0)
            return key;
        if (PhysicalNames.GetValue(logicalToPhysical, static m => new HashSet<string>(m.Values, StringComparer.Ordinal))
            .Contains(key))
        {
            return key;
        }
        return logicalToPhysical.TryGetValue(key, out var physical) ? physical : key;
    }

    // The physical names of one map, built once per map instance: a caller canonicalizes every file's keys
    // against the same map.
    private static readonly ConditionalWeakTable<IReadOnlyDictionary<string, string>, HashSet<string>>
        PhysicalNames = new();
}
