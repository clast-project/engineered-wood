// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Mirror of upstream's <c>stats_table_dtype</c>: derives the
/// <see cref="StructType"/> for a column's per-zone stats table given the
/// column's parent dtype and the sorted-ascending list of present
/// <see cref="Stat"/>s. Min/Max each contribute two fields (the value
/// column + a non-nullable bool truncation flag); other stats contribute
/// one. Used both to feed the array decoder a matching schema and to
/// project decoded struct fields into <see cref="ZoneStats"/>.
/// </summary>
internal static class ZoneStatsLayout
{
    private const string MinIsTruncated = "min_is_truncated";
    private const string MaxIsTruncated = "max_is_truncated";

    public static StructType BuildStructType(IArrowType columnDtype, IReadOnlyList<int> presentStats)
    {
        var fields = new List<Field>();
        foreach (var statValue in presentStats)
        {
            var stat = (Stat)statValue;
            var statDtype = StatDType(stat, columnDtype);
            if (statDtype is null) continue;
            switch (stat)
            {
                case Stat.Max:
                    fields.Add(new Field("max", statDtype, nullable: true));
                    fields.Add(new Field(MaxIsTruncated, BooleanType.Default, nullable: false));
                    break;
                case Stat.Min:
                    fields.Add(new Field("min", statDtype, nullable: true));
                    fields.Add(new Field(MinIsTruncated, BooleanType.Default, nullable: false));
                    break;
                default:
                    fields.Add(new Field(StatName(stat), statDtype, nullable: NullableForStat(stat)));
                    break;
            }
        }
        return new StructType(fields);
    }

    public static ZoneStats FromStruct(StructArray zonesStruct, IArrowType columnDtype, ZoneInfo zoneInfo)
    {
        // Walk the same field order produced by BuildStructType. Map each
        // stat to the matching property on ZoneStats. The struct's fields
        // are typed Arrow arrays.
        IArrowArray? min = null, max = null, sum = null;
        BooleanArray? minTrunc = null, maxTrunc = null;
        UInt64Array? nullCount = null, nanCount = null, uncompressedSize = null;
        BooleanArray? isConstant = null, isSorted = null, isStrictSorted = null;

        int idx = 0;
        var presentStats = new List<Stat>();
        foreach (var statValue in zoneInfo.PresentStats)
        {
            var stat = (Stat)statValue;
            if (StatDType(stat, columnDtype) is null) continue;
            presentStats.Add(stat);
            switch (stat)
            {
                case Stat.Max:
                    max = zonesStruct.Fields[idx++];
                    maxTrunc = (BooleanArray)zonesStruct.Fields[idx++];
                    break;
                case Stat.Min:
                    min = zonesStruct.Fields[idx++];
                    minTrunc = (BooleanArray)zonesStruct.Fields[idx++];
                    break;
                case Stat.Sum:
                    sum = zonesStruct.Fields[idx++];
                    break;
                case Stat.NullCount:
                    nullCount = (UInt64Array)zonesStruct.Fields[idx++];
                    break;
                case Stat.NaNCount:
                    nanCount = (UInt64Array)zonesStruct.Fields[idx++];
                    break;
                case Stat.UncompressedSizeInBytes:
                    uncompressedSize = (UInt64Array)zonesStruct.Fields[idx++];
                    break;
                case Stat.IsConstant:
                    isConstant = (BooleanArray)zonesStruct.Fields[idx++];
                    break;
                case Stat.IsSorted:
                    isSorted = (BooleanArray)zonesStruct.Fields[idx++];
                    break;
                case Stat.IsStrictSorted:
                    isStrictSorted = (BooleanArray)zonesStruct.Fields[idx++];
                    break;
            }
        }

        return new ZoneStats(
            zoneInfo.ZoneLen, zoneInfo.ZoneCount, presentStats,
            min, max, minTrunc, maxTrunc, sum,
            nullCount, nanCount, uncompressedSize,
            isConstant, isSorted, isStrictSorted);
    }

    private static IArrowType? StatDType(Stat stat, IArrowType columnDtype) => stat switch
    {
        Stat.IsConstant or Stat.IsSorted or Stat.IsStrictSorted => BooleanType.Default,
        Stat.Max or Stat.Min => columnDtype is NullType ? null : columnDtype,
        Stat.NullCount or Stat.UncompressedSizeInBytes => UInt64Type.Default,
        Stat.NaNCount => columnDtype is FloatType or DoubleType ? UInt64Type.Default : null,
        Stat.Sum => SumDType(columnDtype),
        _ => null,
    };

    private static IArrowType? SumDType(IArrowType columnDtype) => columnDtype switch
    {
        Int8Type or Int16Type or Int32Type or Int64Type => Int64Type.Default,
        UInt8Type or UInt16Type or UInt32Type or UInt64Type => UInt64Type.Default,
        FloatType or DoubleType => DoubleType.Default,
        _ => null,
    };

    private static bool NullableForStat(Stat stat) => stat switch
    {
        // Counts are always non-null. Bool flags / Sum can be null when a
        // batch is empty (no values to characterise).
        Stat.NullCount or Stat.NaNCount or Stat.UncompressedSizeInBytes => false,
        _ => true,
    };

    private static string StatName(Stat stat) => stat switch
    {
        Stat.IsConstant => "is_constant",
        Stat.IsSorted => "is_sorted",
        Stat.IsStrictSorted => "is_strict_sorted",
        Stat.Max => "max",
        Stat.Min => "min",
        Stat.Sum => "sum",
        Stat.NullCount => "null_count",
        Stat.NaNCount => "nan_count",
        Stat.UncompressedSizeInBytes => "uncompressed_size_in_bytes",
        _ => stat.ToString(),
    };
}
