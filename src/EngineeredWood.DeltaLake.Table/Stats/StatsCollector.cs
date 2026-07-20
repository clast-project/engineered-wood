// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table.Stats;

/// <summary>
/// Collects per-column statistics (numRecords, min/max values, null counts)
/// from Arrow <see cref="RecordBatch"/> data for use in Delta Lake file actions.
/// </summary>
internal static class StatsCollector
{
    /// <summary>
    /// Collects column statistics from a RecordBatch and returns
    /// a JSON-encoded stats string.
    /// </summary>
    public static string? Collect(RecordBatch batch) =>
        Collect([batch]);

    /// <summary>
    /// Collects column statistics aggregated across multiple RecordBatches
    /// and returns a JSON-encoded stats string. Used by compaction when
    /// combining multiple batches into a single output file.
    /// </summary>
    public static string? Collect(IReadOnlyList<RecordBatch> batches)
    {
        long totalRows = 0;
        var minValues = new Dictionary<string, object?>();
        var maxValues = new Dictionary<string, object?>();
        // Values are long (a leaf) or a nested Dictionary<string, object> (a struct subtree) — the Delta
        // stats JSON nests objects mirroring the schema.
        var nullCounts = new Dictionary<string, object>();

        foreach (var batch in batches)
        {
            if (batch.Length == 0)
                continue;

            totalRows += batch.Length;

            for (int col = 0; col < batch.ColumnCount; col++)
            {
                var field = batch.Schema.FieldsList[col];
                var array = batch.Column(col);

                if (array is StructArray structCol)
                {
                    // Nested stats: recurse into struct leaves (list/map subtrees carry no per-column stats).
                    var nMin = GetOrAddNested(minValues, field.Name);
                    var nMax = GetOrAddNested(maxValues, field.Name);
                    var nNull = GetOrAddNestedCounts(nullCounts, field.Name);
                    CollectStruct(structCol, allParentValid: structCol.NullCount == 0, nMin, nMax, nNull);
                }
                else
                {
                    // Sum null counts across batches
                    long existing = nullCounts.TryGetValue(field.Name, out var ex) && ex is long l ? l : 0;
                    nullCounts[field.Name] = existing + array.NullCount;

                    // Merge min/max across batches
                    CollectMinMax(field.Name, array, minValues, maxValues);
                }
            }
        }

        if (totalRows == 0)
            return null;

        return SerializeStats(totalRows, minValues, maxValues, nullCounts);
    }

    private static string SerializeStats(
        long numRecords,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues,
        Dictionary<string, object> nullCounts)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();
        writer.WriteNumber("numRecords", numRecords);

        // minValues / maxValues nest objects for struct subtrees. Long strings are truncated to a
        // 32-char prefix on the min side; a truncated max gets its last incrementable char bumped so it
        // stays an UPPER bound (omitted when impossible) — Spark parity, applied at every nesting level.
        writer.WritePropertyName("minValues");
        WriteBoundsObject(writer, minValues, isMax: false);

        writer.WritePropertyName("maxValues");
        WriteBoundsObject(writer, maxValues, isMax: true);

        // Write nullCount (nested objects for struct subtrees)
        writer.WritePropertyName("nullCount");
        WriteNullCountObject(writer, nullCounts);

        writer.WriteEndObject();
        writer.Flush();

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static Dictionary<string, object?> GetOrAddNested(
        Dictionary<string, object?> parent, string key)
    {
        if (parent.TryGetValue(key, out var v) && v is Dictionary<string, object?> d)
            return d;
        var fresh = new Dictionary<string, object?>();
        parent[key] = fresh;
        return fresh;
    }

    private static Dictionary<string, object> GetOrAddNestedCounts(
        Dictionary<string, object> parent, string key)
    {
        if (parent.TryGetValue(key, out var v) && v is Dictionary<string, object> d)
            return d;
        var fresh = new Dictionary<string, object>();
        parent[key] = fresh;
        return fresh;
    }

    /// <summary>
    /// Recursive stats for a struct column's leaves. nullCount is EXACT (a row counts as null when the
    /// parent row is null OR the child slot is null — exactness matters, IS NULL pruning relies on it).
    /// min/max reuse the flat collectors over the child arrays; when the parent has nulls the child slot
    /// of a parent-null row may hold an arbitrary value, which can only WIDEN the bounds — a superset
    /// bound never wrongly skips a file, so it is prune-safe (same argument as deletion-vector stats).
    /// </summary>
    private static void CollectStruct(
        StructArray st, bool allParentValid,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues,
        Dictionary<string, object> nullCounts)
    {
        var structType = (Apache.Arrow.Types.StructType)st.Data.DataType;
        int offset = st.Data.Offset;
        for (int c = 0; c < st.Data.Children.Length && c < structType.Fields.Count; c++)
        {
            string childName = structType.Fields[c].Name;
            var child = ArrowArrayFactory.BuildArray(st.Data.Children[c]);

            // Exact per-row null count over the parent's logical rows (children do NOT incorporate the
            // parent's offset — index at offset + r).
            long nulls = 0;
            for (int r = 0; r < st.Length; r++)
            {
                if ((!allParentValid && st.IsNull(r)) || child.IsNull(offset + r))
                    nulls++;
            }

            if (child is StructArray nestedStruct)
            {
                var nMin = GetOrAddNested(minValues, childName);
                var nMax = GetOrAddNested(maxValues, childName);
                var nNull = GetOrAddNestedCounts(nullCounts, childName);
                CollectStruct(nestedStruct, allParentValid && nestedStruct.NullCount == 0, nMin, nMax, nNull);
            }
            else
            {
                long existing = nullCounts.TryGetValue(childName, out var ex) && ex is long l ? l : 0;
                nullCounts[childName] = existing + nulls;
                CollectMinMax(childName, child, minValues, maxValues);
            }
        }
    }

    private static void WriteBoundsObject(
        Utf8JsonWriter writer, Dictionary<string, object?> values, bool isMax)
    {
        writer.WriteStartObject();
        foreach (var kvp in values)
        {
            object? value = kvp.Value;
            if (value is Dictionary<string, object?> nested)
            {
                if (nested.Count == 0)
                    continue;
                writer.WritePropertyName(kvp.Key);
                WriteBoundsObject(writer, nested, isMax);
                continue;
            }
            if (value is DateStat date)
            {
                // A date bound is written as "yyyy-MM-dd"; an unrepresentable one is omitted (safe).
                string? iso = date.ToIsoOrNull();
                if (iso is null)
                    continue;
                writer.WritePropertyName(kvp.Key);
                writer.WriteStringValue(iso);
                continue;
            }
            if (value is DecimalStat dec)
            {
                // A high-precision decimal bound is written as a raw JSON number to preserve full precision.
                writer.WritePropertyName(kvp.Key);
                writer.WriteRawValue(dec.ToNumberString());
                continue;
            }
            if (value is string str && str.Length > StringStatMaxLength)
            {
                value = isMax ? (object?)TruncateMaxString(str) : str.Substring(0, StringStatMaxLength);
            }
            if (value is not null)
            {
                writer.WritePropertyName(kvp.Key);
                WriteStatValue(writer, value);
            }
        }
        writer.WriteEndObject();
    }

    private static void WriteNullCountObject(Utf8JsonWriter writer, Dictionary<string, object> counts)
    {
        writer.WriteStartObject();
        foreach (var kvp in counts)
        {
            if (kvp.Value is Dictionary<string, object> nested)
            {
                if (nested.Count == 0)
                    continue;
                writer.WritePropertyName(kvp.Key);
                WriteNullCountObject(writer, nested);
            }
            else if (kvp.Value is long n)
            {
                writer.WriteNumber(kvp.Key, n);
            }
        }
        writer.WriteEndObject();
    }

    private const int StringStatMaxLength = 32;

    /// <summary>
    /// Truncates a max-side string stat to an upper bound of at most <see cref="StringStatMaxLength"/>
    /// characters: the prefix with its last incrementable char bumped by one (skipping chars whose
    /// increment would create a lone surrogate). Returns null when no char can be incremented — the
    /// caller omits the stat (always safe).
    /// </summary>
    private static string? TruncateMaxString(string value)
    {
        for (int i = StringStatMaxLength - 1; i >= 0; i--)
        {
            char c = value[i];
            if (c == char.MaxValue)
                continue;
            char next = (char)(c + 1);
            if (next is >= '\ud800' and <= '\udfff')
                continue; // incrementing into the surrogate range would produce invalid UTF-16
            return value.Substring(0, i) + next;
        }
        return null;
    }

    private static void CollectMinMax(
        string name, IArrowArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        switch (array)
        {
            case Int64Array int64:
                CollectNumericMinMax(name, int64, minValues, maxValues);
                break;
            case Int32Array int32:
                CollectNumericMinMax(name, int32, minValues, maxValues);
                break;
            case Int16Array int16:
                CollectNumericMinMax(name, int16, minValues, maxValues);
                break;
            case Int8Array int8:
                CollectNumericMinMax(name, int8, minValues, maxValues);
                break;
            case DoubleArray dbl:
                CollectNumericMinMax(name, dbl, minValues, maxValues);
                break;
            case FloatArray flt:
                CollectNumericMinMax(name, flt, minValues, maxValues);
                break;
            case StringArray str:
                CollectStringMinMax(name, str, minValues, maxValues);
                break;
            case LargeStringArray lstr:
                CollectLargeStringMinMax(name, lstr, minValues, maxValues);
                break;
            case BooleanArray bln:
                CollectBooleanMinMax(name, bln, minValues, maxValues);
                break;
            case Date32Array d32:
                CollectDateMinMax(name, d32, minValues, maxValues);
                break;
            case TimestampArray ts:
                CollectTimestampMinMax(name, ts, minValues, maxValues);
                break;
            // Decimal32/64 always fit System.Decimal (precision <= 18); 128/256 may not, so read the exact
            // unscaled integer from the value bytes. Delta writes decimal bounds as JSON NUMBERS (Spark
            // emits even 38-digit values as raw numbers), which is what makes them decodable and prune.
            case Decimal32Array d32:
                CollectSmallDecimalMinMax(name, i => d32.GetValue(i), d32, minValues, maxValues);
                break;
            case Decimal64Array d64:
                CollectSmallDecimalMinMax(name, i => d64.GetValue(i), d64, minValues, maxValues);
                break;
            case Decimal128Array d128:
                CollectWideDecimalMinMax(name, d128, d128.ValueBuffer, 16,
                    ((Apache.Arrow.Types.Decimal128Type)d128.Data.DataType).Scale, minValues, maxValues);
                break;
            case Decimal256Array d256:
                CollectWideDecimalMinMax(name, d256, d256.ValueBuffer, 32,
                    ((Apache.Arrow.Types.Decimal256Type)d256.Data.DataType).Scale, minValues, maxValues);
                break;
            // For complex types (struct, list, map), we skip min/max
        }
    }

    private static void CollectNumericMinMax<T>(
        string name, PrimitiveArray<T> array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
        where T : struct, IEquatable<T>, IComparable<T>
    {
        T? min = null;
        T? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            T val = array.GetValue(i)!.Value;

            if (min is null || val.CompareTo(min.Value) < 0) min = val;
            if (max is null || val.CompareTo(max.Value) > 0) max = val;
        }

        // Merge with existing values from previous batches
        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var existingMin) && existingMin is T em)
                min = min.Value.CompareTo(em) < 0 ? min : em;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var existingMax) && existingMax is T ex)
                max = max.Value.CompareTo(ex) > 0 ? max : ex;
            maxValues[name] = max;
        }
    }

    private static void CollectStringMinMax(
        string name, StringArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        string? min = null;
        string? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            string val = array.GetString(i);

            if (min is null || string.Compare(val, min, StringComparison.Ordinal) < 0) min = val;
            if (max is null || string.Compare(val, max, StringComparison.Ordinal) > 0) max = val;
        }

        MergeStringMinMax(name, min, max, minValues, maxValues);
    }

    private static void CollectLargeStringMinMax(
        string name, LargeStringArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        string? min = null;
        string? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            string val = array.GetString(i);

            if (min is null || string.Compare(val, min, StringComparison.Ordinal) < 0) min = val;
            if (max is null || string.Compare(val, max, StringComparison.Ordinal) > 0) max = val;
        }

        MergeStringMinMax(name, min, max, minValues, maxValues);
    }

    private static void MergeStringMinMax(
        string name, string? min, string? max,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var em) && em is string es)
                min = string.Compare(min, es, StringComparison.Ordinal) < 0 ? min : es;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var ex) && ex is string xs)
                max = string.Compare(max, xs, StringComparison.Ordinal) > 0 ? max : xs;
            maxValues[name] = max;
        }
    }

    private static void CollectBooleanMinMax(
        string name, BooleanArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        bool? min = null;
        bool? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            bool val = array.GetValue(i)!.Value;

            if (min is null || (!val && min.Value)) min = val;
            if (max is null || (val && !max.Value)) max = val;
        }

        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var em) && em is bool eb)
                min = !min.Value && !eb ? false : min.Value && eb ? true : min;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var ex) && ex is bool xb)
                max = max.Value || xb;
            maxValues[name] = max;
        }
    }

    private static void CollectTimestampMinMax(
        string name, TimestampArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        // TimestampArray stores values as long (microseconds since epoch)
        long? min = null;
        long? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            long val = array.GetValue(i)!.Value;

            if (min is null || val < min.Value) min = val;
            if (max is null || val > max.Value) max = val;
        }

        // Delta stats stores timestamps as ISO 8601 strings
        if (min.HasValue)
        {
            var tsType = (Apache.Arrow.Types.TimestampType)array.Data.DataType;
            string minStr = FormatTimestamp(min.Value, tsType);
            string maxStr = FormatTimestamp(max!.Value, tsType);
            MergeStringMinMax(name, minStr, maxStr, minValues, maxValues);
        }
    }

    private static string FormatTimestamp(long value, Apache.Arrow.Types.TimestampType tsType)
    {
        // Convert to microseconds
        long micros = tsType.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => value * 1_000_000,
            Apache.Arrow.Types.TimeUnit.Millisecond => value * 1_000,
            Apache.Arrow.Types.TimeUnit.Microsecond => value,
            Apache.Arrow.Types.TimeUnit.Nanosecond => value / 1_000,
            _ => value,
        };

        var dto = DateTimeOffset.FromUnixTimeMilliseconds(micros / 1_000)
            .AddTicks((micros % 1_000) * 10);

        return tsType.Timezone is not null
            ? dto.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'")
            : dto.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff");
    }

    private static void CollectDateMinMax(
        string name, Date32Array array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        // Date32 is a day count since the Unix epoch; compare on the integer (monotonic with the date) and
        // format to Spark's "yyyy-MM-dd" only at serialization. Delta stats decode a date bound from that
        // string, so emitting a raw number (as CollectNumericMinMax would) is not decodable and never prunes.
        int? min = null;
        int? max = null;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            int day = array.GetValue(i)!.Value;
            if (min is null || day < min.Value) min = day;
            if (max is null || day > max.Value) max = day;
        }

        if (min is not null)
        {
            int m = min.Value;
            if (minValues.TryGetValue(name, out var em) && em is DateStat eMin)
                m = Math.Min(m, eMin.Days);
            minValues[name] = new DateStat(m);
        }
        if (max is not null)
        {
            int m = max.Value;
            if (maxValues.TryGetValue(name, out var ex) && ex is DateStat eMax)
                m = Math.Max(m, eMax.Days);
            maxValues[name] = new DateStat(m);
        }
    }

    /// <summary>
    /// A date column min/max bound, kept as the raw day count so bounds compare and merge as integers;
    /// rendered to the Delta stats <c>"yyyy-MM-dd"</c> form only when written.
    /// </summary>
    private readonly struct DateStat
    {
        public DateStat(int days) => Days = days;

        public int Days { get; }

        /// <summary>The bound as <c>"yyyy-MM-dd"</c>, or null when the day count falls outside
        /// <see cref="DateTime"/>'s representable range (0001-9999) — the caller then omits the bound,
        /// which is always prune-safe.</summary>
        public string? ToIsoOrNull()
        {
            try
            {
                var d = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(Days);
                return d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
            catch (ArgumentOutOfRangeException)
            {
                return null;
            }
        }
    }

    // Decimal32/64: the value always fits System.Decimal, so compare and store it directly; a null slot
    // makes GetValue return null.
    private static void CollectSmallDecimalMinMax(
        string name, Func<int, decimal?> get, IArrowArray array,
        Dictionary<string, object?> minValues, Dictionary<string, object?> maxValues)
    {
        decimal? min = null;
        decimal? max = null;
        for (int i = 0; i < array.Length; i++)
        {
            decimal? v = get(i);
            if (v is null) continue;
            if (min is null || v.Value < min.Value) min = v;
            if (max is null || v.Value > max.Value) max = v;
        }

        if (min is not null)
        {
            decimal m = min.Value;
            if (minValues.TryGetValue(name, out var em) && em is decimal e) m = Math.Min(m, e);
            minValues[name] = m;
        }
        if (max is not null)
        {
            decimal m = max.Value;
            if (maxValues.TryGetValue(name, out var ex) && ex is decimal e) m = Math.Max(m, e);
            maxValues[name] = m;
        }
    }

    // Decimal128/256: the value may exceed System.Decimal, so track the exact unscaled integer read from
    // the fixed-width little-endian value bytes (scale is constant per column). Serialized as a raw JSON
    // number so full precision survives.
    private static void CollectWideDecimalMinMax(
        string name, Apache.Arrow.Array array, ArrowBuffer valueBuffer, int width, int scale,
        Dictionary<string, object?> minValues, Dictionary<string, object?> maxValues)
    {
        BigInteger? min = null;
        BigInteger? max = null;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            BigInteger u = ToBigInteger(valueBuffer.Span.Slice(i * width, width));
            if (min is null || u < min.Value) min = u;
            if (max is null || u > max.Value) max = u;
        }

        if (min is not null)
        {
            BigInteger m = min.Value;
            if (minValues.TryGetValue(name, out var em) && em is DecimalStat e && e.Scale == scale)
                m = BigInteger.Min(m, e.Unscaled);
            minValues[name] = new DecimalStat(m, scale);
        }
        if (max is not null)
        {
            BigInteger m = max.Value;
            if (maxValues.TryGetValue(name, out var ex) && ex is DecimalStat e && e.Scale == scale)
                m = BigInteger.Max(m, e.Unscaled);
            maxValues[name] = new DecimalStat(m, scale);
        }
    }

    private static BigInteger ToBigInteger(ReadOnlySpan<byte> littleEndianTwosComplement)
    {
#if NET6_0_OR_GREATER
        return new BigInteger(littleEndianTwosComplement, isUnsigned: false, isBigEndian: false);
#else
        return new BigInteger(littleEndianTwosComplement.ToArray());
#endif
    }

    /// <summary>
    /// A high-precision decimal min/max bound kept as its exact unscaled integer plus the column's scale,
    /// rendered to the plain decimal number text (the JSON-number form Delta uses) only when written.
    /// </summary>
    private readonly struct DecimalStat
    {
        public DecimalStat(BigInteger unscaled, int scale)
        {
            Unscaled = unscaled;
            Scale = scale;
        }

        public BigInteger Unscaled { get; }

        public int Scale { get; }

        public string ToNumberString()
        {
            if (Scale <= 0)
                return Unscaled.ToString(CultureInfo.InvariantCulture);

            bool negative = Unscaled.Sign < 0;
            string digits = BigInteger.Abs(Unscaled).ToString(CultureInfo.InvariantCulture);
            if (digits.Length <= Scale)
                digits = new string('0', Scale - digits.Length + 1) + digits;
            int point = digits.Length - Scale;
            return (negative ? "-" : string.Empty)
                + digits.Substring(0, point) + "." + digits.Substring(point);
        }
    }

    private static void WriteStatValue(Utf8JsonWriter writer, object value)
    {
        switch (value)
        {
            case long l: writer.WriteNumberValue(l); break;
            case int i: writer.WriteNumberValue(i); break;
            case short s: writer.WriteNumberValue(s); break;
            case sbyte sb: writer.WriteNumberValue(sb); break;
            case double d: writer.WriteNumberValue(d); break;
            case float f: writer.WriteNumberValue(f); break;
            case decimal dm: writer.WriteNumberValue(dm); break;
            case string str: writer.WriteStringValue(str); break;
            case bool b: writer.WriteBooleanValue(b); break;
            default: writer.WriteNullValue(); break;
        }
    }
}
