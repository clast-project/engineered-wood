// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Evaluates a <see cref="Predicate"/> against a Delta <see cref="AddFile"/>
/// using both partition values and per-file column statistics. Skips files
/// that the evaluator proves cannot contain matching rows.
///
/// <para>Public so a host that assembles its own file list (e.g. an <see cref="IDataFileReader"/>-style
/// native scan that reads the active set through its own parquet reader) can apply the SAME
/// superset-safe prune verdict the library's read paths use — a pruned file is provably matchless,
/// an unresolvable reference keeps the file.</para>
/// </summary>
public sealed class DeltaFilePruner
{
    private readonly DeltaFileStatsAccessor _accessor;

    public DeltaFilePruner(StructType schema, IReadOnlyList<string> partitionColumns)
    {
        var typeMap = new Dictionary<string, string>(StringComparer.Ordinal);
        var logicalToPhysical = new Dictionary<string, string>(StringComparer.Ordinal);
        var collided = new HashSet<string>(StringComparer.Ordinal);
        // Struct leaves register under their dotted path ("s.a") — matching the flattened stats keys —
        // so a nested reference resolves with the same flat lookup as a top-level column.
        AddFields(schema, logicalPrefix: "", physicalPrefix: "", typeMap, logicalToPhysical, collided);
        // A literal dotted column name colliding with a struct leaf path is ambiguous — drop the key
        // (an unresolvable reference evaluates Unknown => the file is kept; pruning must never guess).
        foreach (var key in collided)
        {
            typeMap.Remove(key);
            logicalToPhysical.Remove(key);
        }

        var partitionSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
        _accessor = new DeltaFileStatsAccessor(typeMap, partitionSet, logicalToPhysical);
    }

    private static void AddFields(
        StructType schema, string logicalPrefix, string physicalPrefix,
        Dictionary<string, string> typeMap, Dictionary<string, string> logicalToPhysical,
        HashSet<string> collided)
    {
        foreach (var field in schema.Fields)
        {
            string logical = logicalPrefix.Length == 0 ? field.Name : logicalPrefix + "." + field.Name;
            // Column mapping: partitionValues + stats in the log are keyed by the PHYSICAL column name at
            // EVERY level (older engineered-wood commits used logical keys) — the accessor looks values up
            // under both, so track the dotted physical path alongside the logical one.
            string physName = field.Name;
            if (field.Metadata is not null
                && field.Metadata.TryGetValue(ColumnMapping.PhysicalNameKey, out var phys)
                && !string.IsNullOrEmpty(phys))
            {
                physName = phys;
            }
            string physical = physicalPrefix.Length == 0 ? physName : physicalPrefix + "." + physName;

            if (field.Type is PrimitiveType pt)
            {
                if (typeMap.ContainsKey(logical))
                {
                    collided.Add(logical);
                }
                else
                {
                    typeMap[logical] = pt.TypeName;
                    if (physical != logical)
                        logicalToPhysical[logical] = physical;
                }
            }
            else if (field.Type is StructType st)
            {
                AddFields(st, logical, physical, typeMap, logicalToPhysical, collided);
            }
            // list/map: stats only cover struct leaves — nothing to register.
        }
    }

    /// <summary>
    /// Returns true if the file might contain rows matching the predicate.
    /// Returns false only when statistics or partition values prove no rows
    /// can match.
    /// </summary>
    public bool ShouldInclude(AddFile addFile, Predicate filter)
    {
        if (filter is TruePredicate)
            return true;

        var stats = new DeltaFileStats(addFile, ColumnStats.Parse(addFile.Stats));
        return StatisticsEvaluator.Evaluate(filter, stats, _accessor)
            != FilterResult.AlwaysFalse;
    }
}

/// <summary>
/// Bundles a Delta file's partition values and parsed column statistics for
/// evaluation. Both views are needed because the predicate may reference
/// partition columns (constants per file) and data columns (typed via stats).
/// </summary>
internal sealed class DeltaFileStats
{
    public DeltaFileStats(AddFile addFile, ColumnStats? columnStats)
    {
        AddFile = addFile;
        ColumnStats = columnStats;
    }

    public AddFile AddFile { get; }
    public ColumnStats? ColumnStats { get; }
}

/// <summary>
/// Adapts <see cref="DeltaFileStats"/> for the shared
/// <see cref="StatisticsEvaluator"/>. Partition columns return their
/// constant value as both min and max (with null-count = 0); data columns
/// look up min/max from the parsed stats and decode the JSON element using
/// the column's Delta primitive type.
/// </summary>
internal sealed class DeltaFileStatsAccessor : IStatisticsAccessor<DeltaFileStats>
{
    private readonly IReadOnlyDictionary<string, string> _columnTypes;
    private readonly HashSet<string> _partitionColumns;
    private readonly IReadOnlyDictionary<string, string> _logicalToPhysical;

    public DeltaFileStatsAccessor(
        IReadOnlyDictionary<string, string> columnTypes,
        HashSet<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical = null)
    {
        _columnTypes = columnTypes;
        _partitionColumns = partitionColumns;
        _logicalToPhysical = logicalToPhysical ?? new Dictionary<string, string>();
    }

    // Looks up a per-file dictionary keyed by column name under the LOGICAL name first, then the PHYSICAL name
    // (column mapping: partitionValues/stats keys are physical per the Delta spec; older engineered-wood
    // commits used logical keys — both must resolve).
    private bool TryGet<TValue>(IReadOnlyDictionary<string, TValue>? dict, string column, out TValue value)
    {
        value = default!;
        if (dict is null)
            return false;
        if (dict.TryGetValue(column, out value!))
            return true;
        return _logicalToPhysical.TryGetValue(column, out var phys) && dict.TryGetValue(phys, out value!);
    }

    public LiteralValue? GetMinValue(DeltaFileStats stats, string column) =>
        GetBound(stats, column, isMin: true);

    public LiteralValue? GetMaxValue(DeltaFileStats stats, string column) =>
        GetBound(stats, column, isMin: false);

    public long? GetNullCount(DeltaFileStats stats, string column)
    {
        if (_partitionColumns.Contains(column))
        {
            // Partition value is constant per file. If the stored value is
            // null (the dictionary holds a null string), every row is null;
            // otherwise no row is null in this column.
            if (TryGet(stats.AddFile.PartitionValues, column, out var v))
                return v is null ? stats.AddFile.PartitionValues.Count > 0 ? GetValueCount(stats, column) : null : 0;
            return null;
        }

        return TryGet(stats.ColumnStats?.NullCount, column, out long n) ? (long?)n : null;
    }

    public long? GetValueCount(DeltaFileStats stats, string column) =>
        stats.ColumnStats?.NumRecords > 0 ? stats.ColumnStats.NumRecords : null;

    public bool IsMinExact(DeltaFileStats stats, string column) => true;
    public bool IsMaxExact(DeltaFileStats stats, string column) => true;

    private LiteralValue? GetBound(DeltaFileStats stats, string column, bool isMin)
    {
        if (!_columnTypes.TryGetValue(column, out string? typeName))
            return null;

        if (_partitionColumns.Contains(column))
        {
            if (!TryGet(stats.AddFile.PartitionValues, column, out var partVal))
                return null;
            return DeltaLiteralDecoder.FromPartitionString(partVal, typeName);
        }

        var bounds = isMin ? stats.ColumnStats?.MinValues : stats.ColumnStats?.MaxValues;
        if (!TryGet(bounds, column, out var element))
            return null;

        return DeltaLiteralDecoder.FromJson(element, typeName);
    }
}
