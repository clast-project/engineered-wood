// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// A columnar view over a checkpoint's <c>add.stats_parsed</c>, letting a file's bounds be read
/// straight from the Arrow arrays instead of parsed out of its JSON <c>stats</c> string.
/// </summary>
/// <remarks>
/// <para>The point is what is NOT done: a predicate usually touches one or two columns, but
/// <c>ColumnStats.Parse</c> parses a file's entire stats blob — every column's min, max and null count —
/// and does it again on every query, because the pruner parses inside <c>ShouldInclude</c>. Here the
/// arrays are located once per checkpoint and a bound costs one indexed read of one column.</para>
///
/// <para>The arrays are the batch's own, so the view keeps that memory alive for as long as the snapshot
/// holds it. That trades the retained JSON strings for retained typed columns, which are smaller.</para>
/// </remarks>
internal sealed class CheckpointStatsView
{
    private readonly Int64Array? _numRecords;
    private readonly Dictionary<string, IArrowArray> _minValues;
    private readonly Dictionary<string, IArrowArray> _maxValues;
    private readonly Dictionary<string, Int64Array> _nullCounts;
    // Kept alongside the flattened lookups so the statistics can be written back out as JSON, which
    // is the only form the rewrite and commit paths speak.
    private readonly StructArray? _minStruct;
    private readonly StructArray? _maxStruct;
    private readonly StructArray? _nullCountStruct;

    private CheckpointStatsView(
        Int64Array? numRecords,
        Dictionary<string, IArrowArray> minValues,
        Dictionary<string, IArrowArray> maxValues,
        Dictionary<string, Int64Array> nullCounts,
        StructArray? minStruct,
        StructArray? maxStruct,
        StructArray? nullCountStruct)
    {
        _numRecords = numRecords;
        _minValues = minValues;
        _maxValues = maxValues;
        _nullCounts = nullCounts;
        _minStruct = minStruct;
        _maxStruct = maxStruct;
        _nullCountStruct = nullCountStruct;
    }

    /// <summary>
    /// Builds a view over an <c>add</c> struct's <c>stats_parsed</c> field, or null when the checkpoint
    /// carries no typed statistics.
    /// </summary>
    public static CheckpointStatsView? TryCreate(StructArray addStruct)
    {
        var addType = (ArrowStructType)addStruct.Data.DataType;
        int index = FieldIndex(addType, "stats_parsed");
        if (index < 0 || addStruct.Fields[index] is not StructArray stats)
            return null;

        var statsType = (ArrowStructType)stats.Data.DataType;
        var numRecords = Child(stats, statsType, "numRecords") as Int64Array;

        var minValues = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        var maxValues = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        var nullCounts = new Dictionary<string, Int64Array>(StringComparer.Ordinal);

        var minStruct = Child(stats, statsType, "minValues") as StructArray;
        var maxStruct = Child(stats, statsType, "maxValues") as StructArray;
        var nullCountStruct = Child(stats, statsType, "nullCount") as StructArray;

        Flatten(minStruct, prefix: null, minValues);
        Flatten(maxStruct, prefix: null, maxValues);

        var nullCountLeaves = new Dictionary<string, IArrowArray>(StringComparer.Ordinal);
        Flatten(nullCountStruct, prefix: null, nullCountLeaves);
        foreach (var kvp in nullCountLeaves)
        {
            if (kvp.Value is Int64Array counts)
                nullCounts[kvp.Key] = counts;
        }

        if (numRecords is null && minValues.Count == 0 && maxValues.Count == 0 && nullCounts.Count == 0)
            return null;

        return new CheckpointStatsView(
            numRecords, minValues, maxValues, nullCounts, minStruct, maxStruct, nullCountStruct);
    }

    /// <summary>Number of records in the file at <paramref name="row"/>.</summary>
    public long? GetNumRecords(int row) =>
        _numRecords is not null && !_numRecords.IsNull(row) ? _numRecords.GetValue(row) : null;

    /// <summary>Null count for one column of the file at <paramref name="row"/>.</summary>
    public long? GetNullCount(string column, int row) =>
        _nullCounts.TryGetValue(column, out var counts) && !counts.IsNull(row)
            ? counts.GetValue(row)
            : null;

    /// <summary>
    /// One bound for one column, decoded with the same rules the JSON path uses so the two produce
    /// identical literals — a prune must not depend on which copy of the statistics was read.
    /// </summary>
    public LiteralValue? GetBound(string column, int row, bool isMin, string typeName)
    {
        var arrays = isMin ? _minValues : _maxValues;
        return arrays.TryGetValue(column, out var array)
            ? DeltaLiteralDecoder.FromArrow(array, row, typeName)
            : null;
    }

    /// <summary>
    /// True when the view carries min/max for this column. Asked separately from
    /// <see cref="HasNullCount"/> because the two cover different column sets: stats_parsed counts
    /// nulls for every column but bounds only the orderable ones, so a boolean is present in one and
    /// absent from the other, and a caller that conflated them would silently lose the JSON bound.
    /// </summary>
    public bool HasBound(string column) =>
        _minValues.ContainsKey(column) || _maxValues.ContainsKey(column);

    /// <summary>True when the view carries a null count for this column.</summary>
    public bool HasNullCount(string column) => _nullCounts.ContainsKey(column);

    /// <summary>
    /// Writes one file's statistics back out as a Delta <c>stats</c> JSON string, or null when the row
    /// carries none.
    /// </summary>
    /// <remarks>
    /// <para>The inverse of what <c>StatsParsedBuilder</c> read in, and needed because JSON is the only
    /// form the rest of the log speaks: a rewritten file's statistics are widened as text, and an add
    /// re-serialised into a commit carries the string. Without this, a file read from a checkpoint
    /// written with <c>writeStatsAsJson=false</c> would lose its statistics the moment an UPDATE or a
    /// compaction moved it. delta-spark solves the same problem the same way, with
    /// <c>to_json(stats_parsed)</c>.</para>
    ///
    /// <para>Values are written in the forms <c>StatsCollector</c> emits — dates as
    /// <c>yyyy-MM-dd</c>, timestamps as ISO-8601 (with a trailing Z when the column is zoned), decimals
    /// as exact JSON numbers — so a synthesised string is interchangeable with an original one. Absent
    /// bounds are omitted rather than written as null, matching a stats blob that never had them.</para>
    /// </remarks>
    public string? BuildStatsJson(int row)
    {
        bool hasNumRecords = _numRecords is not null && !_numRecords.IsNull(row);
        if (!hasNumRecords && _minStruct is null && _maxStruct is null && _nullCountStruct is null)
            return null;

        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            if (hasNumRecords)
                writer.WriteNumber("numRecords", _numRecords!.GetValue(row)!.Value);
            WriteGroup(writer, "minValues", _minStruct, row);
            WriteGroup(writer, "maxValues", _maxStruct, row);
            WriteGroup(writer, "nullCount", _nullCountStruct, row);
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteGroup(Utf8JsonWriter writer, string name, StructArray? group, int row)
    {
        if (group is null || group.IsNull(row))
            return;

        writer.WritePropertyName(name);
        WriteStruct(writer, group, row);
    }

    private static void WriteStruct(Utf8JsonWriter writer, StructArray group, int row)
    {
        writer.WriteStartObject();
        var type = (ArrowStructType)group.Data.DataType;
        for (int i = 0; i < type.Fields.Count; i++)
        {
            var child = group.Fields[i];
            if (child.IsNull(row))
                continue;

            string fieldName = type.Fields[i].Name;
            if (child is StructArray nested)
            {
                writer.WritePropertyName(fieldName);
                WriteStruct(writer, nested, row);
            }
            else
            {
                WriteValue(writer, fieldName, child, row);
            }
        }
        writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, string name, IArrowArray array, int row)
    {
        switch (array)
        {
            // Decimal types derive from FixedSizeBinaryType — keep them ahead of anything
            // binary-shaped. Their exact digits go out as raw number text, so the property name has
            // to be written separately.
            case Decimal32Array d:
                WriteRawDecimal(writer, name, d, row, 4,
                    ((Apache.Arrow.Types.Decimal32Type)d.Data.DataType).Scale);
                break;
            case Decimal64Array d:
                WriteRawDecimal(writer, name, d, row, 8,
                    ((Apache.Arrow.Types.Decimal64Type)d.Data.DataType).Scale);
                break;
            case Decimal128Array d:
                WriteRawDecimal(writer, name, d, row, 16,
                    ((Apache.Arrow.Types.Decimal128Type)d.Data.DataType).Scale);
                break;
            case Decimal256Array d:
                WriteRawDecimal(writer, name, d, row, 32,
                    ((Apache.Arrow.Types.Decimal256Type)d.Data.DataType).Scale);
                break;
            case Int64Array a: writer.WriteNumber(name, a.GetValue(row)!.Value); break;
            case Int32Array a: writer.WriteNumber(name, a.GetValue(row)!.Value); break;
            case Int16Array a: writer.WriteNumber(name, a.GetValue(row)!.Value); break;
            case Int8Array a: writer.WriteNumber(name, a.GetValue(row)!.Value); break;
            case FloatArray a: writer.WriteNumber(name, a.GetValue(row)!.Value); break;
            case DoubleArray a: writer.WriteNumber(name, a.GetValue(row)!.Value); break;
            case BooleanArray a: writer.WriteBoolean(name, a.GetValue(row)!.Value); break;
            case StringArray a: writer.WriteString(name, a.GetString(row)); break;
            case Date32Array a:
                writer.WriteString(name, s_epoch.AddDays(a.GetValue(row)!.Value)
                    .ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                break;
            case TimestampArray a:
                writer.WriteString(name, FormatTimestamp(a, row));
                break;
            default:
                // A type with no JSON stats form: omitting it is a missing bound, never a wrong one.
                break;
        }
    }

    private static void WriteRawDecimal(
        Utf8JsonWriter writer, string name, IArrowArray array, int row, int width, int scale)
    {
        writer.WritePropertyName(name);
        writer.WriteRawValue(FormatDecimal(array, row, width, scale), skipInputValidation: true);
    }

    private static string FormatTimestamp(TimestampArray array, int row)
    {
        long micros = array.GetValue(row)!.Value;
        var value = s_epoch.AddTicks(micros * 10);
        var type = (Apache.Arrow.Types.TimestampType)array.Data.DataType;
        return type.Timezone is not null
            ? value.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture)
            : value.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Renders a decimal's exact digits as JSON number text. Never goes through
    /// <c>System.Decimal</c>: a decimal(38,30) bound has more significant digits than it can hold, and
    /// rounding one shifts the bound.
    /// </summary>
    private static string FormatDecimal(IArrowArray array, int row, int width, int scale)
    {
        var bytes = array.Data.Buffers[1].Span.Slice((row + array.Data.Offset) * width, width);
#if NET6_0_OR_GREATER
        var unscaled = new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#else
        var unscaled = new BigInteger(bytes.ToArray());
#endif
        // Today this array always comes from EngineeredWood's own reader, whose buffers are managed
        // arrays and therefore rooted by the span itself. Rooting anyway keeps the method correct for
        // any array it might be handed. See doc/arrow-span-lifetime.md.
        GC.KeepAlive(array);
        if (scale == 0)
            return unscaled.ToString(CultureInfo.InvariantCulture);

        string sign = unscaled.Sign < 0 ? "-" : "";
        string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
        if (digits.Length <= scale)
            digits = digits.PadLeft(scale + 1, '0');
        return sign + digits.Substring(0, digits.Length - scale)
            + "." + digits.Substring(digits.Length - scale);
    }

    private static readonly DateTime s_epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Flattens a bounds struct into dotted leaf paths ("payload.score"), matching the keys
    /// <see cref="Actions.ColumnStats"/> produces from the JSON so both feed the same lookups.
    /// </summary>
    private static void Flatten(IArrowArray? array, string? prefix, Dictionary<string, IArrowArray> into)
    {
        if (array is not StructArray group)
            return;

        var type = (ArrowStructType)group.Data.DataType;
        for (int i = 0; i < type.Fields.Count; i++)
        {
            string name = type.Fields[i].Name;
            string path = prefix is null ? name : prefix + "." + name;
            var child = group.Fields[i];
            if (child is StructArray)
                Flatten(child, path, into);
            else
                into[path] = child;
        }
    }

    private static IArrowArray? Child(StructArray parent, ArrowStructType type, string name)
    {
        int index = FieldIndex(type, name);
        return index < 0 ? null : parent.Fields[index];
    }

    private static int FieldIndex(ArrowStructType type, string name)
    {
        for (int i = 0; i < type.Fields.Count; i++)
        {
            if (string.Equals(type.Fields[i].Name, name, StringComparison.Ordinal))
                return i;
        }
        return -1;
    }
}

/// <summary>
/// A file's position in a <see cref="CheckpointStatsView"/> — what an
/// <see cref="Actions.AddFile"/> carries instead of its parsed stats string.
/// </summary>
internal sealed class ParsedStatsRef(CheckpointStatsView view, int row)
{
    public CheckpointStatsView View { get; } = view;

    public int Row { get; } = row;
}
