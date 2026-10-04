// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Resolves the keys of an <c>add.partitionValues</c> map under column mapping. The Delta spec (and Spark) key
/// it by the PHYSICAL column name; engineered-wood commits before 2026-07 keyed it by the logical name.
///
/// <para>A map's spelling is decided ONCE, from every partition column, never key by key: a spec-keyed map
/// carries every partition column's physical key, and a map that does not is logical-keyed throughout. Key by
/// key goes wrong either way on a table upgraded to name mode, whose original names stay as physical names.
/// After renaming <c>p1</c> to <c>x</c> and then <c>p2</c> to <c>p1</c>, the logical name <c>p1</c> is the
/// physical key of <c>x</c>, so logical-first bound the wrong column in a spec-keyed map; and in a
/// logical-keyed map <c>{x, p1}</c>, physical-first found <c>x</c>'s physical key <c>p1</c> and read the other
/// column's value.</para>
///
/// <para>Renames can also PERMUTE names, leaving a map complete in both spellings (<see cref="IsAmbiguous"/>).
/// Nothing in the map decides it; a read follows the spec, and the pruner declines.</para>
/// </summary>
internal static class PartitionValueKeys
{
    /// <summary>
    /// True when <paramref name="values"/> is keyed by physical name: it carries every partition column's
    /// physical key. With no column mapping the two spellings are the same and this is true whenever the map is
    /// complete.
    /// </summary>
    public static bool IsPhysicallyKeyed<TValue>(
        IReadOnlyDictionary<string, TValue> values, IEnumerable<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        foreach (var column in partitionColumns)
        {
            if (!values.ContainsKey(Physical(column, logicalToPhysical)))
                return false;
        }
        return true;
    }

    /// <summary>
    /// True when <paramref name="values"/> is complete in BOTH spellings and they differ: renames that permute
    /// names (logical <c>p1</c> physical <c>p2</c>, logical <c>p2</c> physical <c>p1</c>) leave a map whose keys
    /// say nothing about which column each belongs to. <see cref="TryGet"/> then follows the spec (physical), as a
    /// read must produce some value; a consumer that may decline, such as the pruner, should.
    /// </summary>
    public static bool IsAmbiguous<TValue>(
        IReadOnlyDictionary<string, TValue> values, IEnumerable<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical)
    {
        bool spellingsDiffer = false;
        foreach (var column in partitionColumns)
        {
            if (!values.ContainsKey(column))
                return false;
            spellingsDiffer |= !string.Equals(Physical(column, logicalToPhysical), column, StringComparison.Ordinal);
        }
        return spellingsDiffer && IsPhysicallyKeyed(values, partitionColumns, logicalToPhysical);
    }

    /// <summary>
    /// Looks up the value of the partition column whose LOGICAL name is <paramref name="column"/>, in the map's
    /// own spelling. <paramref name="logicalToPhysical"/> may be null, or omit a column whose physical name is
    /// its logical one.
    /// </summary>
    public static bool TryGet<TValue>(
        IReadOnlyDictionary<string, TValue> values, string column, IEnumerable<string> partitionColumns,
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

    private static string Physical(string logical, IReadOnlyDictionary<string, string>? logicalToPhysical) =>
        logicalToPhysical is not null && logicalToPhysical.TryGetValue(logical, out var physical)
            ? physical
            : logical;
}
