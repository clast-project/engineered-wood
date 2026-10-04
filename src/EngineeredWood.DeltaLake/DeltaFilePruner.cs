// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Evaluates a <see cref="Predicate"/> against a Delta <see cref="AddFile"/>
/// using both partition values and per-file column statistics. Skips files
/// that the evaluator proves cannot contain matching rows.
///
/// <para>Partition pruning and statistics pruning are ONE pass rather than two: for a partition column the
/// accessor reports the file's constant value as both bounds, for a data column it decodes the recorded
/// min/max, and a single <c>AlwaysFalse</c> from either source drops the file. A predicate that cannot be
/// resolved evaluates Unknown, which KEEPS the file — pruning never guesses.</para>
///
/// <para>Pure, and useful outside a scan: the conflict checker prunes a concurrent <c>add</c> against a
/// transaction's read predicates with it, and a host planning its own scan can apply it to a candidate set
/// it assembled itself.</para>
/// </summary>
public sealed class DeltaFilePruner
{
    private readonly DeltaFileStatsAccessor _accessor;

    /// <param name="schema">The table schema, used to type each column's bounds.</param>
    /// <param name="partitionColumns">Partition columns, whose values are constant per file.</param>
    /// <param name="preferTypedStats">
    /// Whether a checkpoint's typed <c>stats_parsed</c> columns win over its JSON <c>stats</c> string
    /// when both are present — see <c>DeltaTableOptions.PreferTypedCheckpointStats</c> in the table
    /// layer. A checkpoint carrying only typed statistics is read from them either way.
    /// </param>
    public DeltaFilePruner(
        StructType schema, IReadOnlyList<string> partitionColumns, bool preferTypedStats = true)
    {
        var typeMap = new Dictionary<string, string>(StringComparer.Ordinal);
        var logicalToPhysical = new Dictionary<string, string>(StringComparer.Ordinal);
        var collided = new HashSet<string>(StringComparer.Ordinal);
        // Struct leaves register under their dotted path ("s.a") — matching the flattened stats keys —
        // so a nested reference resolves with the same flat lookup as a top-level column.
        var noBounds = new HashSet<string>(StringComparer.Ordinal);
        AddFields(schema, logicalPrefix: "", physicalPrefix: "", typeMap, logicalToPhysical, collided, noBounds);
        // A literal dotted column name colliding with a struct leaf path is ambiguous — drop the key
        // (an unresolvable reference evaluates Unknown => the file is kept; pruning must never guess).
        foreach (var key in collided)
        {
            typeMap.Remove(key);
            logicalToPhysical.Remove(key);
        }

        // A column whose PHYSICAL name is another column's LOGICAL name has statistics no reader can attribute:
        // the key is its own in a spec-keyed map and the other column's in a logical-keyed one (UPDATE and
        // copy-on-write rewrites wrote those until #451), and renames that permute names leave nothing in the map
        // to tell them apart. Its statistics are not used; every other column keeps pruning.
        var unattributable = new HashSet<string>(StringComparer.Ordinal);
        foreach (var kv in logicalToPhysical)
        {
            if (typeMap.ContainsKey(kv.Value))
                unattributable.Add(kv.Key);
        }

        var partitionSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
        _accessor = new DeltaFileStatsAccessor(
            typeMap, partitionSet, logicalToPhysical, preferTypedStats, unattributable, noBounds);
    }

    // A column widened from float (to double) has old files whose bounds are a float's shortest text: "0.1"
    // decodes to the double 0.1, while the widened value is (double)0.1f = 0.10000000149..., outside the bound,
    // and the file was pruned. Decoding the text as a float instead would be wrong for files written after the
    // widening, whose bounds ARE doubles, and nothing says which a file is. So its bounds are not used; its null
    // counts are exact whatever the type was. Integer, decimal and date bounds decode exactly at the wider type.
    // Malformed type-change metadata is treated the same way, which only costs pruning.
    private static bool WidenedFromFloat(StructField field)
    {
        try
        {
            return Schema.TypeWidening.GetTypeChanges(field)
                .Any(c => c.FieldPath is null && c.FromType == "float");
        }
        catch (Exception e) when (e is System.Text.Json.JsonException or InvalidOperationException
            or KeyNotFoundException)
        {
            return true;
        }
    }

    private static void AddFields(
        StructType schema, string logicalPrefix, string physicalPrefix,
        Dictionary<string, string> typeMap, Dictionary<string, string> logicalToPhysical,
        HashSet<string> collided, HashSet<string> noBounds)
    {
        foreach (var field in schema.Fields)
        {
            string logical = logicalPrefix.Length == 0 ? field.Name : logicalPrefix + "." + field.Name;
            // Column mapping: partitionValues + stats in the log are keyed by the PHYSICAL column name at
            // EVERY level, so track the dotted physical path alongside the logical one. (Older engineered-wood
            // commits used logical keys; the accessor still reads their partition values, not their stats.)
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
                    if (WidenedFromFloat(field))
                        noBounds.Add(logical);
                }
            }
            else if (field.Type is StructType st)
            {
                AddFields(st, logical, physical, typeMap, logicalToPhysical, collided, noBounds);
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
    private readonly HashSet<string> _unattributable;
    private readonly HashSet<string> _noBounds;

    public DeltaFileStatsAccessor(
        IReadOnlyDictionary<string, string> columnTypes,
        HashSet<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical = null,
        bool preferTypedStats = true,
        HashSet<string>? unattributableStatsColumns = null,
        HashSet<string>? noBoundsColumns = null)
    {
        _columnTypes = columnTypes;
        _partitionColumns = partitionColumns;
        _logicalToPhysical = logicalToPhysical ?? new Dictionary<string, string>();
        _preferTypedStats = preferTypedStats;
        _unattributable = unattributableStatsColumns ?? new HashSet<string>(StringComparer.Ordinal);
        _noBounds = noBoundsColumns ?? new HashSet<string>(StringComparer.Ordinal);
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

    // A file's partition value, in its map's own spelling: physical keys (the spec), or the logical keys of an
    // older engineered-wood commit. The spelling is decided per map, not per key (see PartitionValueKeys). A map
    // complete in BOTH spellings that disagree (renames that permute names) gives the pruner nothing: a read has
    // to pick the spec's reading, but pruning can decline to guess.
    private bool TryGetPartitionValue(AddFile addFile, string column, out string value)
    {
        if (PartitionValueKeys.IsAmbiguous(addFile.PartitionValues, _partitionColumns, _logicalToPhysical))
        {
            value = default!;
            return false;
        }
        return PartitionValueKeys.TryGet(
            addFile.PartitionValues, column, _partitionColumns, _logicalToPhysical, out value);
    }

    // A file's statistic for a column: under the column's PHYSICAL name ONLY when it has one distinct from its
    // logical name, as the spec and Spark key them. There is no logical fallback, unlike partition values. A
    // statistics map need not cover every column, so a missing physical key cannot tell a logical-keyed file
    // from one that simply has no statistics for the column, while the logical name can be a DIFFERENT column's
    // key: a dropped column's physical name after a column was re-added under it, or another column's physical
    // name after chained renames on an upgraded table. Those bounds pruned files holding matching rows. A
    // logical-keyed file loses pruning for mapped columns instead, which only costs reads. Nor is the physical
    // key read for a column whose physical name is another column's logical name (see the constructor): in a
    // logical-keyed map that key is the other column's.
    private bool TryGetStat<TValue>(IReadOnlyDictionary<string, TValue>? dict, string column, out TValue value)
    {
        value = default!;
        if (dict is null || _unattributable.Contains(column))
            return false;
        return dict.TryGetValue(
            _logicalToPhysical.TryGetValue(column, out var physical) ? physical : column, out value!);
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
            if (TryGetPartitionValue(stats.AddFile, column, out var v))
                return v is null ? stats.AddFile.PartitionValues.Count > 0 ? GetValueCount(stats, column) : null : 0;
            return null;
        }

        if (TypedFor(stats.AddFile) is { } typed
            && TryResolveTyped(typed, column, typed.View.HasNullCount, out string resolved))
            return typed.View.GetNullCount(resolved, typed.Row);

        return TryGetStat(stats.ColumnStats?.NullCount, column, out long n) ? (long?)n : null;
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
    /// True when the checkpoint's typed statistics cover this column under the name its statistics are keyed
    /// by — the physical one, as <see cref="TryGetStat"/> and for the same reason. False sends the lookup to the
    /// JSON copy, which may carry bounds stats_parsed omits (a checkpoint an older engineered-wood wrote under
    /// logical names, for one). A column whose statistics cannot be attributed is not covered: the checkpoint
    /// writer laid each JSON key out under the physical column of that name, whichever column wrote it.
    /// </summary>
    private bool TryResolveTyped(
        Checkpoint.ParsedStatsRef typed, string column,
        Func<string, bool> covers, out string resolved)
    {
        resolved = _logicalToPhysical.TryGetValue(column, out var physical) ? physical : column;
        return !_unattributable.Contains(column) && covers(resolved);
    }

    private LiteralValue? GetBound(DeltaFileStats stats, string column, bool isMin)
    {
        if (!_columnTypes.TryGetValue(column, out string? typeName))
            return null;

        if (_partitionColumns.Contains(column))
        {
            if (!TryGetPartitionValue(stats.AddFile, column, out var partVal))
                return null;
            return DeltaLiteralDecoder.FromPartitionString(partVal, typeName);
        }

        if (_noBounds.Contains(column))
            return null;

        LiteralValue? bound;
        if (TypedFor(stats.AddFile) is { } typed
            && TryResolveTyped(typed, column, typed.View.HasBound, out string resolved))
        {
            bound = typed.View.GetBound(resolved, typed.Row, isMin, typeName);
        }
        else
        {
            var bounds = isMin ? stats.ColumnStats?.MinValues : stats.ColumnStats?.MaxValues;
            if (!TryGetStat(bounds, column, out var element))
                return null;
            bound = DeltaLiteralDecoder.FromJson(element, typeName);
        }

        return isMin ? bound : WidenTimestampMax(bound, typeName);
    }

    // Spark writes timestamp bounds TRUNCATED to the millisecond, so a file's true maximum can lie up to 999
    // microseconds above its recorded one; delta-spark and delta-kernel add 1 ms to every timestamp max they read
    // for the same reason. The min, truncated downwards, is already a valid lower bound. A max this engine wrote
    // at full precision only gets looser by the same millisecond.
    private static LiteralValue? WidenTimestampMax(LiteralValue? bound, string typeName)
    {
        if (bound is not { Type: LiteralValue.Kind.DateTimeOffset } max
            || typeName is not ("timestamp" or "timestamp_ntz"))
        {
            return bound;
        }
        var value = max.AsDateTimeOffset;
        return value <= DateTimeOffset.MaxValue.AddMilliseconds(-1)
            ? (LiteralValue?)LiteralValue.Of(value.AddMilliseconds(1))
            : null;
    }
}
