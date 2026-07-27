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
/// </summary>
internal sealed class DeltaFilePruner
{
    private readonly DeltaFileStatsAccessor _accessor;

    /// <param name="schema">The table schema, used to type each column's bounds.</param>
    /// <param name="partitionColumns">Partition columns, whose values are constant per file.</param>
    /// <param name="preferTypedStats">
    /// Whether a checkpoint's typed <c>stats_parsed</c> columns win over its JSON <c>stats</c> string
    /// when both are present — see <see cref="DeltaTableOptions.PreferTypedCheckpointStats"/>. A
    /// checkpoint carrying only typed statistics is read from them either way.
    /// </param>
    public DeltaFilePruner(
        StructType schema, IReadOnlyList<string> partitionColumns, bool preferTypedStats = true)
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
        _accessor = new DeltaFileStatsAccessor(
            typeMap, partitionSet, logicalToPhysical, preferTypedStats);
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

        // The JSON parse is LAZY: a file answered entirely from a checkpoint's typed columns never
        // touches its statistics string, and one that falls back for a column pays for the parse only
        // then. The blob covers every column; a predicate names one or two.
        var stats = new DeltaFileStats(addFile);
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
    private ColumnStats? _columnStats;
    private bool _parsed;

    public DeltaFileStats(AddFile addFile) => AddFile = addFile;

    public AddFile AddFile { get; }

    /// <summary>
    /// The JSON statistics, parsed on first use. Files whose bounds all come from a checkpoint's typed
    /// columns never touch this, which is where the saving is: the blob covers every column, while a
    /// predicate names one or two.
    /// </summary>
    public ColumnStats? ColumnStats
    {
        get
        {
            if (!_parsed)
            {
                _columnStats = Actions.ColumnStats.Parse(AddFile.Stats);
                _parsed = true;
            }
            return _columnStats;
        }
    }
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
    private readonly bool _preferTypedStats;

    public DeltaFileStatsAccessor(
        IReadOnlyDictionary<string, string> columnTypes,
        HashSet<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical = null,
        bool preferTypedStats = true)
    {
        _columnTypes = columnTypes;
        _partitionColumns = partitionColumns;
        _logicalToPhysical = logicalToPhysical ?? new Dictionary<string, string>();
        _preferTypedStats = preferTypedStats;
    }

    /// <summary>
    /// The typed statistics to read this file from, or null to use the JSON copy. Typed statistics
    /// win when preferred, and also when there is no JSON string to fall back to — a checkpoint
    /// written with <c>writeStatsAsJson=false</c> has nothing else to offer.
    /// </summary>
    private Checkpoint.ParsedStatsRef? TypedFor(AddFile addFile) =>
        addFile.TypedStats is { } typed && (_preferTypedStats || addFile.Stats is null)
            ? typed
            : null;

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

        if (TypedFor(stats.AddFile) is { } typed
            && TryResolveTyped(typed, column, typed.View.HasNullCount, out string resolved))
            return typed.View.GetNullCount(resolved, typed.Row);

        return TryGet(stats.ColumnStats?.NullCount, column, out long n) ? (long?)n : null;
    }

    public long? GetValueCount(DeltaFileStats stats, string column)
    {
        if (TypedFor(stats.AddFile) is { } typed)
        {
            long? records = typed.View.GetNumRecords(typed.Row);
            if (records is not null)
                return records > 0 ? records : null;
        }

        return stats.ColumnStats?.NumRecords > 0 ? stats.ColumnStats.NumRecords : null;
    }

    public bool IsMinExact(DeltaFileStats stats, string column) => true;
    public bool IsMaxExact(DeltaFileStats stats, string column) => true;

    /// <summary>
    /// True when the checkpoint's typed statistics cover this column, under its logical or physical
    /// name. False sends the lookup to the JSON copy, which may carry bounds stats_parsed omits.
    /// </summary>
    private bool TryResolveTyped(
        Checkpoint.ParsedStatsRef typed, string column,
        Func<string, bool> covers, out string resolved)
    {
        if (covers(column))
        {
            resolved = column;
            return true;
        }
        if (_logicalToPhysical.TryGetValue(column, out var physical) && covers(physical))
        {
            resolved = physical;
            return true;
        }
        resolved = column;
        return false;
    }

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

        if (TypedFor(stats.AddFile) is { } typed
            && TryResolveTyped(typed, column, typed.View.HasBound, out string resolved))
            return typed.View.GetBound(resolved, typed.Row, isMin, typeName);

        var bounds = isMin ? stats.ColumnStats?.MinValues : stats.ColumnStats?.MaxValues;
        if (!TryGet(bounds, column, out var element))
            return null;

        return DeltaLiteralDecoder.FromJson(element, typeName);
    }
}
