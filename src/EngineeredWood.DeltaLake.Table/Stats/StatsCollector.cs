// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using EngineeredWood.Expressions;

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
    public static string? Collect(RecordBatch batch, StatsColumnSelection? selection = null) =>
        Collect([batch], selection);

    /// <summary>
    /// Collects column statistics aggregated across multiple RecordBatches
    /// and returns a JSON-encoded stats string. Used by compaction when
    /// combining multiple batches into a single output file. <paramref name="selection"/> says which columns get
    /// statistics and how long a string bound may be (the table's data-skipping properties); null selects every
    /// column with the default string length.
    /// </summary>
    public static string? Collect(IReadOnlyList<RecordBatch> batches, StatsColumnSelection? selection = null)
    {
        var accumulator = new Accumulator(selection);
        foreach (var batch in batches)
            accumulator.Add(batch);
        return accumulator.Serialize();
    }

    /// <summary>
    /// One file's statistics, built up batch by batch as the file is written, so the batches need not be kept
    /// until it is closed. <see cref="Collect(IReadOnlyList{RecordBatch}, StatsColumnSelection?)"/> is the same
    /// over a list.
    /// </summary>
    internal sealed class Accumulator
    {
        private readonly StatsColumnSelection _selection;
        private long _totalRows;
        private readonly Dictionary<string, object?> _minValues = new();
        private readonly Dictionary<string, object?> _maxValues = new();
        // Values are long (a leaf) or a nested Dictionary<string, object> (a struct subtree) — the Delta
        // stats JSON nests objects mirroring the schema.
        private readonly Dictionary<string, object> _nullCounts = new();

        public Accumulator(StatsColumnSelection? selection) => _selection = selection ?? StatsColumnSelection.All;

        public void Add(RecordBatch batch)
        {
            if (batch.Length == 0)
                return;

            _totalRows += batch.Length;

            for (int col = 0; col < batch.ColumnCount; col++)
            {
                var field = batch.Schema.FieldsList[col];
                var array = batch.Column(col);
                if (!_selection.Includes(field.Name, out var selected))
                {
                    continue;
                }

                if (array is StructArray structCol)
                {
                    // Nested stats: recurse into struct leaves (list/map subtrees carry no per-column stats).
                    var nMin = GetOrAddNested(_minValues, field.Name);
                    var nMax = GetOrAddNested(_maxValues, field.Name);
                    var nNull = GetOrAddNestedCounts(_nullCounts, field.Name);
                    CollectStruct(
                        structCol, firstElement: 0, structCol.Length, ancestorNull: null, selected, nMin, nMax, nNull);
                }
                else
                {
                    // Sum null counts across batches
                    long existing = _nullCounts.TryGetValue(field.Name, out var ex) && ex is long l ? l : 0;
                    _nullCounts[field.Name] = existing + array.NullCount;

                    // Merge min/max across batches
                    CollectMinMax(field.Name, array, _minValues, _maxValues);
                }
            }
        }

        /// <summary>
        /// Adds the Z85 variant bounds <see cref="VariantFooterStats"/> derived from the written file's footer, for
        /// the columns the selection takes bounds for. Batches cannot give these: they hold the unshredded value.
        /// </summary>
        public void AddVariantBounds(IReadOnlyDictionary<string, (string Min, string Max)> bounds)
        {
            foreach (var (name, (min, max)) in bounds)
            {
                if (!_selection.Includes(name, out _))
                    continue;
                _minValues[name] = new EncodedBound(min);
                _maxValues[name] = new EncodedBound(max);
            }
        }

        /// <summary>The stats JSON, or null when no row was added.</summary>
        public string? Serialize() => _totalRows == 0
            ? null
            : SerializeStats(_totalRows, _minValues, _maxValues, _nullCounts, _selection.StringPrefixLength);
    }

    private static string SerializeStats(
        long numRecords,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues,
        Dictionary<string, object> nullCounts,
        int stringPrefixLength)
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);

        writer.WriteStartObject();
        writer.WriteNumber("numRecords", numRecords);

        // minValues / maxValues nest objects for struct subtrees. Long strings are truncated to a
        // stringPrefixLength-char prefix on the min side; a truncated max gets its last incrementable char bumped so it
        // stays an UPPER bound (omitted when impossible) — Spark parity, applied at every nesting level.
        writer.WritePropertyName("minValues");
        WriteBoundsObject(writer, minValues, isMax: false, stringPrefixLength);

        writer.WritePropertyName("maxValues");
        WriteBoundsObject(writer, maxValues, isMax: true, stringPrefixLength);

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
    /// Recursive stats for a struct column's leaves, over <paramref name="rowCount"/> of the batch's rows, which
    /// are <paramref name="st"/>'s elements from <paramref name="firstElement"/> on. nullCount is EXACT: a row
    /// counts as null for a leaf when ANY struct above it is null on that row, not just its direct parent, or
    /// the leaf slot is (exactness matters, IS NULL pruning relies on it). <paramref name="ancestorNull"/> carries
    /// the rows some enclosing struct already made null, or is null when none did. min/max reuse the flat
    /// collectors over the child arrays, sliced to the struct's rows; a child slot under a null ancestor may hold
    /// an arbitrary value, which can only WIDEN the bounds — a superset bound never wrongly skips a file, so it is
    /// prune-safe (same argument as deletion-vector stats).
    /// </summary>
    private static void CollectStruct(
        StructArray st, int firstElement, int rowCount, bool[]? ancestorNull, StatsColumnSelection.Node? selected,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues,
        Dictionary<string, object> nullCounts)
    {
        // The rows this struct makes null, on top of its ancestors'. The mask is rented: statistics are collected on
        // every write, and a fresh row-sized array per nullable struct level was a megabyte per level per million
        // rows. A rented array may be longer than rowCount; only its first rowCount entries are read.
        bool[]? rented = null;
        bool[]? rowNull = ancestorNull;
        if (st.NullCount != 0)
        {
            rented = ArrayPool<bool>.Shared.Rent(rowCount);
            rowNull = rented;
            for (int r = 0; r < rowCount; r++)
                rowNull[r] = (ancestorNull is not null && ancestorNull[r]) || st.IsNull(firstElement + r);
        }

        try
        {
            // Children are NOT sliced with their struct: element i of the struct is slot (offset + i) of each child.
            int childFirst = st.Data.Offset + firstElement;
            var structType = (Apache.Arrow.Types.StructType)st.Data.DataType;
            for (int c = 0; c < st.Data.Children.Length && c < structType.Fields.Count; c++)
            {
                string childName = structType.Fields[c].Name;
                StatsColumnSelection.Node? below = null;
                if (selected is not null && !selected.Includes(childName, out below))
                {
                    continue;
                }
                var child = ArrowArrayFactory.BuildArray(st.Data.Children[c]);

                if (child is StructArray nestedStruct)
                {
                    var nMin = GetOrAddNested(minValues, childName);
                    var nMax = GetOrAddNested(maxValues, childName);
                    var nNull = GetOrAddNestedCounts(nullCounts, childName);
                    CollectStruct(nestedStruct, childFirst, rowCount, rowNull, below, nMin, nMax, nNull);
                }
                else
                {
                    long nulls = 0;
                    for (int r = 0; r < rowCount; r++)
                    {
                        if ((rowNull is not null && rowNull[r]) || child.IsNull(childFirst + r))
                            nulls++;
                    }
                    long existing = nullCounts.TryGetValue(childName, out var ex) && ex is long l ? l : 0;
                    nullCounts[childName] = existing + nulls;
                    // Over this struct's rows only. The child is the whole unsliced array, so reading all of it took
                    // the bounds of every row the parent was sliced from: never wrong to prune by, but looser than the
                    // file's own rows, and a write cuts batches into slices at every row-group boundary.
                    var rows = childFirst == 0 && rowCount == child.Length
                        ? child
                        : ArrowArrayFactory.Slice(child, childFirst, rowCount);
                    CollectMinMax(childName, rows, minValues, maxValues);
                }
            }
        }
        finally
        {
            if (rented is not null)
                ArrayPool<bool>.Shared.Return(rented);
        }
    }

    private static void WriteBoundsObject(
        Utf8JsonWriter writer, Dictionary<string, object?> values, bool isMax, int stringPrefixLength)
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
                WriteBoundsObject(writer, nested, isMax, stringPrefixLength);
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
            if (value is EncodedBound encoded)
            {
                // Already a complete bound (a variant's Z85 string): never truncated as a string column is.
                writer.WritePropertyName(kvp.Key);
                writer.WriteStringValue(encoded.Text);
                continue;
            }
            if (value is DecimalStat dec)
            {
                // A high-precision decimal bound is written as a raw JSON number to preserve full precision.
                writer.WritePropertyName(kvp.Key);
                writer.WriteRawValue(dec.ToNumberString());
                continue;
            }
            if (value is string str && str.Length > stringPrefixLength)
            {
                value = isMax
                    ? (object?)TruncateMaxString(str, stringPrefixLength)
                    : TruncateMinString(str, stringPrefixLength);
            }
            if (value is not null)
            {
                writer.WritePropertyName(kvp.Key);
                WriteStatValue(writer, value);
            }
        }
        writer.WriteEndObject();
    }

    /// <summary>A bound written as the string it already is, such as a variant's Z85 bound.</summary>
    private sealed record EncodedBound(string Text);

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

    /// <summary>
    /// Truncates a min-side string stat to a lower bound of at most <paramref name="maxLength"/> UTF-16
    /// code units (the table's <c>delta.dataSkippingStringPrefixLength</c>; Spark counts code points, which keeps
    /// more of a string with supplementary characters, and either prefix is a lower bound). A prefix always sorts at or below the full string, so the only requirement is that
    /// the cut land on a code point boundary: splitting a surrogate pair orphans its high half, and
    /// <see cref="Utf8JsonWriter"/> silently rewrites a lone surrogate to U+FFFD — which sorts ABOVE
    /// the supplementary character it replaced, turning the bound into one GREATER than a value in the
    /// file. Backing off by one char keeps a valid (merely looser) lower bound.
    /// </summary>
    private static string TruncateMinString(string value, int maxLength)
    {
        int length = maxLength;
        if (length > 0 && char.IsHighSurrogate(value[length - 1]))
            length--;
        return value.Substring(0, length);
    }

    /// <summary>
    /// Truncates a max-side string stat to an upper bound of at most <paramref name="maxLength"/>
    /// UTF-16 code units: the prefix with its last incrementable char bumped by one (skipping chars whose
    /// increment would create a lone surrogate). Returns null when no char can be incremented — the
    /// caller omits the stat (always safe), as it does for a length of 0, where Spark omits it too.
    /// </summary>
    private static string? TruncateMaxString(string value, int maxLength)
    {
        for (int i = maxLength - 1; i >= 0; i--)
        {
            char c = value[i];
            if (c == char.MaxValue)
                continue;
            // Replacing a LOW surrogate orphans the high half that precedes it. The increment guard
            // below does not catch this: U+DFFF + 1 is U+E000, which is outside the surrogate range,
            // so the cut would look legal while leaving invalid UTF-16 behind. The JSON writer then
            // rewrites the orphan to U+FFFD, which sorts BELOW the supplementary character it
            // replaced — no longer an upper bound. Step back to the char before the pair instead.
            if (char.IsLowSurrogate(c))
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
            // The floating cases pass an isNaN test, because a NaN does not belong where
            // T.CompareTo puts it -- see CollectNumericMinMax.
            case DoubleArray dbl:
                CollectNumericMinMax(name, dbl, minValues, maxValues, double.IsNaN);
                break;
            case FloatArray flt:
                CollectNumericMinMax(name, flt, minValues, maxValues, float.IsNaN);
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

    /// <summary>
    /// Bounds for a numeric column. <paramref name="isNaN"/> is supplied for the floating types
    /// only, and moves NaN from the bottom of the order to the top.
    /// </summary>
    /// <remarks>
    /// <c>double.CompareTo</c> sorts NaN BELOW every value, so a file holding <c>[1.0, NaN]</c>
    /// was recorded as <c>min = NaN, max = 1.0</c>. Under the order the reader uses (#204) those
    /// bounds are not merely imprecise, they are INVERTED -- <c>min &gt; max</c>, which every
    /// consumer of a range assumes cannot happen. Measured, Spark writes that same file as
    /// <c>min = 3.0, max = "NaN"</c>: the NaN is the maximum, and an all-NaN column is
    /// <c>min = max = "NaN"</c>. Both fall out of ordering NaN at the top.
    /// </remarks>
    private static void CollectNumericMinMax<T>(
        string name, PrimitiveArray<T> array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues,
        Func<T, bool>? isNaN = null)
        where T : struct, IEquatable<T>, IComparable<T>
    {
        T? min = null;
        T? max = null;

        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            T val = array.GetValue(i)!.Value;

            if (min is null || Compare(val, min.Value, isNaN) < 0) min = val;
            if (max is null || Compare(val, max.Value, isNaN) > 0) max = val;
        }

        // Merge with existing values from previous batches
        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var existingMin) && existingMin is T em)
                min = Compare(min.Value, em, isNaN) < 0 ? min : em;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var existingMax) && existingMax is T ex)
                max = Compare(max.Value, ex, isNaN) > 0 ? max : ex;
            maxValues[name] = max;
        }

        static int Compare(T a, T b, Func<T, bool>? isNaN)
        {
            if (isNaN is null)
                return a.CompareTo(b);

            bool aNaN = isNaN(a);
            bool bNaN = isNaN(b);
            if (aNaN || bNaN)
                return aNaN && bNaN ? 0 : aNaN ? 1 : -1;
            return a.CompareTo(b);
        }
    }

    // String min/max is found over the UTF-8 BYTES, decoding only the two winners: for well-formed UTF-8, byte
    // order IS the code point order CompareStrings defines, so the result is identical to comparing decoded
    // strings, without a .NET string per cell (#661). The array is validated first. One ill-formed cell sends
    // the whole array down the decoding path instead. GetString turns a bad sequence into U+FFFD, and the
    // decoded value can sort on the other side of a bound than its bytes do: cells "a", "é", 0x80 have byte
    // max "é", but 0x80 decodes to U+FFFD, above it. Fixing up only the winners cannot catch that, because
    // the offending cell is not one of them.
    private static void CollectStringMinMax(
        string name, StringArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        ReadOnlySpan<int> offsets = array.ValueOffsets;
        if (!AllCellsWellFormed(array.ValueBuffer.Span, offsets, array.Length))
        {
            CollectDecodedStringMinMax(name, array.Length, j => array.IsNull(j), j => array.GetString(j), minValues, maxValues);
            return;
        }

        int min = -1, max = -1;
        ReadOnlySpan<byte> minBytes = default, maxBytes = default;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            ReadOnlySpan<byte> val = array.GetBytes(i);
            if (min < 0)
            {
                min = max = i;
                minBytes = maxBytes = val;
                continue;
            }
            if (val.SequenceCompareTo(minBytes) < 0) { min = i; minBytes = val; }
            else if (val.SequenceCompareTo(maxBytes) > 0) { max = i; maxBytes = val; }
        }

        if (min >= 0)
            MergeStringMinMax(name, array.GetString(min), array.GetString(max), minValues, maxValues);
    }

    private static void CollectLargeStringMinMax(
        string name, LargeStringArray array,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        ReadOnlySpan<long> offsets = array.ValueOffsets;
        if (!AllCellsWellFormed(array.ValueBuffer.Span, offsets, array.Length))
        {
            CollectDecodedStringMinMax(name, array.Length, j => array.IsNull(j), j => array.GetString(j), minValues, maxValues);
            return;
        }

        int min = -1, max = -1;
        ReadOnlySpan<byte> minBytes = default, maxBytes = default;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            ReadOnlySpan<byte> val = array.GetBytes(i);
            if (min < 0)
            {
                min = max = i;
                minBytes = maxBytes = val;
                continue;
            }
            if (val.SequenceCompareTo(minBytes) < 0) { min = i; minBytes = val; }
            else if (val.SequenceCompareTo(maxBytes) > 0) { max = i; maxBytes = val; }
        }

        if (min >= 0)
            MergeStringMinMax(name, array.GetString(min), array.GetString(max), minValues, maxValues);
    }

    // True when every cell of a string array is well-formed UTF-8, checked in ONE pass over the bytes the cells
    // span rather than a call per (usually short) cell: the span is well-formed, and no cell boundary falls inside
    // a character (a boundary byte is never a continuation byte). Together these mean every cell is a run of
    // whole, valid characters. Bytes under a null slot are checked too, which can only cause a needless fallback.
    internal static bool AllCellsWellFormed(ReadOnlySpan<byte> data, ReadOnlySpan<int> offsets, int length)
    {
        if (length == 0) return true;
        int start = offsets[0], end = offsets[length];
        if (!IsWellFormedUtf8(data.Slice(start, end - start))) return false;
        for (int i = 1; i < length; i++)
        {
            int p = offsets[i];
            if (p < end && (data[p] & 0xC0) == 0x80) return false;
        }
        return true;
    }

    private static bool AllCellsWellFormed(ReadOnlySpan<byte> data, ReadOnlySpan<long> offsets, int length)
    {
        if (length == 0) return true;
        int start = checked((int)offsets[0]), end = checked((int)offsets[length]);
        if (!IsWellFormedUtf8(data.Slice(start, end - start))) return false;
        for (int i = 1; i < length; i++)
        {
            long p = offsets[i];
            if (p < end && (data[(int)p] & 0xC0) == 0x80) return false;
        }
        return true;
    }

    // The fallback for an array holding ill-formed UTF-8: order the DECODED values (bad sequences as U+FFFD),
    // so the bounds hold for every value as GetString reads it back.
    private static void CollectDecodedStringMinMax(
        string name, int length, Func<int, bool> isNull, Func<int, string> getString,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        string? min = null;
        string? max = null;

        for (int i = 0; i < length; i++)
        {
            if (isNull(i)) continue;
            string val = getString(i);

            if (min is null || CompareStrings(val, min) < 0) min = val;
            if (max is null || CompareStrings(val, max) > 0) max = val;
        }

        MergeStringMinMax(name, min, max, minValues, maxValues);
    }

    /// <summary>
    /// True when <paramref name="bytes"/> is well-formed UTF-8 (no overlong forms, surrogates, or code points
    /// above U+10FFFF), the condition under which UTF-8 byte order equals code point order and decoding is lossless.
    /// </summary>
    internal static bool IsWellFormedUtf8(ReadOnlySpan<byte> bytes) =>
#if NET8_0_OR_GREATER
        System.Text.Unicode.Utf8.IsValid(bytes);
#else
        IsWellFormedUtf8Scalar(bytes);
#endif

    // The netstandard2.0 validator (no Utf8.IsValid there). Always compiled so the tests can check it against
    // Utf8.IsValid on the modern targets. Ranges follow the Unicode well-formed byte sequences table (Table 3-7).
    internal static bool IsWellFormedUtf8Scalar(ReadOnlySpan<byte> bytes)
    {
        int i = 0;
        while (i < bytes.Length)
        {
            byte b = bytes[i];
            if (b < 0x80) { i++; continue; }

            int need;
            byte lo = 0x80, hi = 0xBF;   // allowed range of the FIRST continuation byte
            if (b >= 0xC2 && b <= 0xDF) need = 1;
            else if (b == 0xE0) { need = 2; lo = 0xA0; }
            else if (b == 0xED) { need = 2; hi = 0x9F; }   // excludes surrogates
            else if (b >= 0xE1 && b <= 0xEF) need = 2;
            else if (b == 0xF0) { need = 3; lo = 0x90; }
            else if (b == 0xF4) { need = 3; hi = 0x8F; }   // caps at U+10FFFF
            else if (b >= 0xF1 && b <= 0xF3) need = 3;
            else return false;   // 0x80..0xC1 or 0xF5..0xFF

            if (i + need >= bytes.Length)
                return false;   // truncated sequence
            byte c = bytes[i + 1];
            if (c < lo || c > hi) return false;
            for (int k = 2; k <= need; k++)
            {
                if ((bytes[i + k] & 0xC0) != 0x80) return false;
            }
            i += need + 1;
        }
        return true;
    }

    // String bounds are ordered in CODE POINT order (UTF-8 byte order), as Delta specifies and as the pruner
    // compares them: through LiteralValue, whose string comparison is the one StatisticsEvaluator evaluates
    // bounds with. StringComparison.Ordinal is UTF-16 code-unit order, which puts a supplementary character (a
    // surrogate pair) BELOW U+E000..U+FFFF; a max taken that way could be lower than a value in the file without
    // looking inverted, and the file was pruned.
    private static int CompareStrings(string a, string b) => LiteralValue.Of(a).CompareTo(LiteralValue.Of(b));

    private static void MergeStringMinMax(
        string name, string? min, string? max,
        Dictionary<string, object?> minValues,
        Dictionary<string, object?> maxValues)
    {
        if (min is not null)
        {
            if (minValues.TryGetValue(name, out var em) && em is string es)
                min = CompareStrings(min, es) < 0 ? min : es;
            minValues[name] = min;
        }
        if (max is not null)
        {
            if (maxValues.TryGetValue(name, out var ex) && ex is string xs)
                max = CompareStrings(max, xs) > 0 ? max : xs;
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
            string minStr = FormatTimestamp(min.Value, tsType, isMax: false);
            string maxStr = FormatTimestamp(max!.Value, tsType, isMax: true);
            MergeStringMinMax(name, minStr, maxStr, minValues, maxValues);
        }
    }

    private static string FormatTimestamp(
        long value, Apache.Arrow.Types.TimestampType tsType, bool isMax)
    {
        // Convert to microseconds. Second/millisecond sources scale up exactly, but a NANOSECOND
        // source has precision the microsecond ISO-8601 form below cannot carry, so it must round
        // OUTWARD — min down, max up — to stay a valid bound. Plain integer division truncates
        // toward ZERO, which rounds the max down for positive timestamps and the min up for negative
        // ones; either way the bound excludes a value actually present in the file, and the file is
        // skipped for a predicate that matches it.
        long micros = tsType.Unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => value * 1_000_000,
            Apache.Arrow.Types.TimeUnit.Millisecond => value * 1_000,
            Apache.Arrow.Types.TimeUnit.Microsecond => value,
            Apache.Arrow.Types.TimeUnit.Nanosecond => isMax
                ? DivCeil(value, 1_000)
                : DivFloor(value, 1_000),
            _ => value,
        };

        var dto = DateTimeOffset.FromUnixTimeMilliseconds(micros / 1_000)
            .AddTicks((micros % 1_000) * 10);

        return tsType.Timezone is not null
            ? dto.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'")
            : dto.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff");
    }

    /// <summary>Integer division rounding toward negative infinity. <paramref name="divisor"/> must be positive.</summary>
    private static long DivFloor(long value, long divisor) =>
        value / divisor - (value % divisor < 0 ? 1 : 0);

    /// <summary>Integer division rounding toward positive infinity. <paramref name="divisor"/> must be positive.</summary>
    private static long DivCeil(long value, long divisor) =>
        value / divisor + (value % divisor > 0 ? 1 : 0);

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
        // The value buffer is the whole array's: a sliced array's slot i is at its offset plus i. Reading from
        // slot 0 took the bounds from the wrong rows, and a bound that misses the real values lets pruning skip
        // a file holding matching rows.
        int offset = array.Data.Offset;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            BigInteger u = ToBigInteger(valueBuffer.Span.Slice((offset + i) * width, width));
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
            case double d:
                if (double.IsNaN(d) || double.IsInfinity(d))
                    writer.WriteStringValue(NonFiniteName(double.IsNaN(d), d > 0));
                else
                    writer.WriteNumberValue(d);
                break;
            case float f:
                if (float.IsNaN(f) || float.IsInfinity(f))
                    writer.WriteStringValue(NonFiniteName(float.IsNaN(f), f > 0));
                else
                    writer.WriteNumberValue(f);
                break;
            case decimal dm: writer.WriteNumberValue(dm); break;
            case string str: writer.WriteStringValue(str); break;
            case bool b: writer.WriteBooleanValue(b); break;
            default: writer.WriteNullValue(); break;
        }
    }

    /// <summary>
    /// Spells a non-finite bound the way Delta's stats JSON carries it: a quoted STRING, because
    /// JSON has no NaN or infinity.
    /// </summary>
    /// <remarks>
    /// The names are Java's <c>Double.toString</c> spellings, which is what Spark emits -- measured
    /// against Spark 4.0, a double column holding an infinity writes
    /// <c>"minValues":{"g":"-Infinity"},"maxValues":{"g":"Infinity"}</c>. Writing the .NET number
    /// instead was not an option and never had been: <see cref="Utf8JsonWriter.WriteNumberValue(double)"/>
    /// THROWS <see cref="ArgumentException"/> for all three values, so until now a Delta write of any
    /// column containing a NaN or an infinity failed outright rather than writing a wrong bound.
    /// <para>
    /// delta-rs, writing the same format, does something third: it omits a NaN from the bounds and
    /// writes <c>null</c> for an infinity. Spark's spelling is the one to match -- it loses nothing,
    /// and it is the only one that leaves a NaN visible to a reader.
    /// </para>
    /// </remarks>
    private static string NonFiniteName(bool isNaN, bool isPositive) =>
        isNaN ? "NaN" : isPositive ? "Infinity" : "-Infinity";
}
