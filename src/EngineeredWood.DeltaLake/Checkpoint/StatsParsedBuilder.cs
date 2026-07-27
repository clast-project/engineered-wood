// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Builds the <c>stats_parsed</c> struct for checkpoint files — the typed twin of the JSON
/// <c>stats</c> string, carrying <c>numRecords</c>, <c>minValues</c>, <c>maxValues</c> and
/// <c>nullCount</c> for the table's columns.
/// </summary>
/// <remarks>
/// <para>The shape follows delta-spark, which is the only definition there is: <c>stats_parsed</c>
/// appears nowhere in the protocol spec. Bounds carry each column's OWN type — a
/// <c>decimal(9,2)</c> column's bounds are <c>decimal(9,2)</c>, not an approximating double — and
/// nested structs recurse, so a bound for <c>payload.score</c> lives at
/// <c>minValues.payload.score</c>. Columns Delta considers ineligible for ordering (boolean, binary,
/// arrays, maps) are absent from <c>minValues</c>/<c>maxValues</c> while still counted in
/// <c>nullCount</c>, exactly as delta-spark writes them.</para>
///
/// <para>Values are decoded from the JSON stats string, so this inherits its precision: decimal
/// bounds go through <see cref="DecimalText"/> rather than <c>System.Decimal</c>, and a bound that
/// cannot be represented in the column's own type is written as null (no bound) rather than as a
/// rounded one that could wrongly skip a file.</para>
/// </remarks>
internal static class StatsParsedBuilder
{
    /// <summary>
    /// Builds the Arrow type for <c>stats_parsed</c>, or null when the schema yields no statistics
    /// at all (a table of nothing but arrays, say) and the column should be omitted.
    /// </summary>
    public static ArrowStructType? BuildStatsType(DeltaStructType deltaSchema)
    {
        var fields = BuildStatsFields(deltaSchema);
        return fields.Count > 0 ? new ArrowStructType(fields) : null;
    }

    /// <summary>
    /// Builds the <c>stats_parsed</c> column: one entry per action, non-null only on
    /// <see cref="AddFile"/> rows that carry stats. Null when the schema yields no statistics.
    /// </summary>
    public static StructArray? BuildStatsColumn(
        List<DeltaAction> actions, int count, DeltaStructType deltaSchema)
    {
        var statsType = BuildStatsType(deltaSchema);
        if (statsType is null)
            return null;

        var boundFields = BuildBoundFields(deltaSchema);
        var nullCountFields = BuildNullCountFields(deltaSchema);

        var numRecords = new FixedStatColumn<long>(count, Int64Type.Default, TryReadInt64);
        var minValues = boundFields.Count > 0 ? new StructStatColumn(boundFields, count) : null;
        var maxValues = boundFields.Count > 0 ? new StructStatColumn(boundFields, count) : null;
        var nullCounts = nullCountFields.Count > 0 ? new StructStatColumn(nullCountFields, count) : null;
        using var validity = new ValidityBuilder(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                bool hasStats = actions[i] is AddFile { Stats: not null } add
                    && TryParseStats(add.Stats, numRecords, minValues, maxValues, nullCounts);
                validity.Append(hasStats);

                // Every column carries exactly one entry per row. Rows without stats — and rows whose
                // stats blob aborted parsing part-way — are padded here, so a malformed blob can never
                // leave the children at different lengths.
                numRecords.PadTo(i + 1);
                minValues?.PadTo(i + 1);
                maxValues?.PadTo(i + 1);
                nullCounts?.PadTo(i + 1);
            }

            var children = new List<IArrowArray> { numRecords.Build() };
            if (minValues is not null)
                children.Add(minValues.Build());
            if (maxValues is not null)
                children.Add(maxValues.Build());
            if (nullCounts is not null)
                children.Add(nullCounts.Build());

            int nullCount = validity.NullCount;
            return new StructArray(statsType, count, children, validity.Build(), nullCount);
        }
        finally
        {
            // Build() transfers each native buffer to Arrow, so these free only what a mid-way
            // failure left behind.
            numRecords.Dispose();
            minValues?.Dispose();
            maxValues?.Dispose();
            nullCounts?.Dispose();
        }
    }

    #region Schema

    private static List<Field> BuildStatsFields(DeltaStructType deltaSchema)
    {
        var fields = new List<Field> { new("numRecords", Int64Type.Default, true) };

        var boundFields = BuildBoundFields(deltaSchema);
        if (boundFields.Count > 0)
        {
            fields.Add(new Field("minValues", new ArrowStructType(boundFields), true));
            fields.Add(new Field("maxValues", new ArrowStructType(boundFields), true));
        }

        var nullCountFields = BuildNullCountFields(deltaSchema);
        if (nullCountFields.Count > 0)
            fields.Add(new Field("nullCount", new ArrowStructType(nullCountFields), true));

        // numRecords alone is not worth a column.
        return fields.Count > 1 ? fields : [];
    }

    /// <summary>
    /// One field per orderable column, carrying that column's own Arrow type; struct columns recurse
    /// and are dropped when nothing inside them is orderable.
    /// </summary>
    private static List<Field> BuildBoundFields(DeltaStructType deltaSchema)
    {
        var fields = new List<Field>();
        foreach (var field in deltaSchema.Fields)
        {
            if (field.Type is DeltaStructType nested)
            {
                var nestedFields = BuildBoundFields(nested);
                if (nestedFields.Count > 0)
                    fields.Add(new Field(field.Name, new ArrowStructType(nestedFields), true));
                continue;
            }

            var arrowType = TryConvert(field.Type);
            if (arrowType is not null && IsOrderable(arrowType))
                fields.Add(new Field(field.Name, arrowType, true));
        }
        return fields;
    }

    /// <summary>One <c>long</c> per column that can hold nulls, structs recursing.</summary>
    private static List<Field> BuildNullCountFields(DeltaStructType deltaSchema)
    {
        var fields = new List<Field>();
        foreach (var field in deltaSchema.Fields)
        {
            if (field.Type is DeltaStructType nested)
            {
                var nestedFields = BuildNullCountFields(nested);
                if (nestedFields.Count > 0)
                    fields.Add(new Field(field.Name, new ArrowStructType(nestedFields), true));
                continue;
            }

            if (field.Type is PrimitiveType && TryConvert(field.Type) is not null)
                fields.Add(new Field(field.Name, Int64Type.Default, true));
        }
        return fields;
    }

    /// <summary>
    /// Delta orders numeric, string, date and timestamp columns and nothing else — booleans, binary,
    /// arrays and maps get no bounds (measured against delta-spark 4.1.0's own checkpoints).
    /// </summary>
    private static bool IsOrderable(IArrowType type) => type switch
    {
        // Decimal types derive from FixedSizeBinaryType, so they must precede any binary arm.
        Decimal128Type or Decimal256Type => true,
        Int8Type or Int16Type or Int32Type or Int64Type => true,
        UInt8Type or UInt16Type or UInt32Type or UInt64Type => true,
        FloatType or DoubleType => true,
        StringType => true,
        Date32Type or Date64Type => true,
        TimestampType => true,
        _ => false,
    };

    /// <summary>The column's Arrow type, or null for a Delta type this build cannot represent.</summary>
    private static IArrowType? TryConvert(DeltaDataType type)
    {
        try
        {
            return SchemaConverter.ToArrowType(type);
        }
        catch (DeltaFormatException)
        {
            return null;
        }
    }

    #endregion

    #region Values

    /// <summary>
    /// Appends one row's statistics. Returns false when the blob could not be parsed, in which case
    /// the caller pads whatever was left short; a partially-appended row is fine because every column
    /// is padded to the same length afterwards.
    /// </summary>
    private static bool TryParseStats(
        string statsJson,
        FixedStatColumn<long> numRecords,
        StructStatColumn? minValues,
        StructStatColumn? maxValues,
        StructStatColumn? nullCounts)
    {
        try
        {
            using var doc = JsonDocument.Parse(statsJson);
            var root = doc.RootElement;

            if (root.TryGetProperty("numRecords", out var nr))
                numRecords.AppendValue(nr);
            else
                numRecords.AppendNull();

            AppendGroup(minValues, root, "minValues");
            AppendGroup(maxValues, root, "maxValues");
            AppendGroup(nullCounts, root, "nullCount");
            return true;
        }
        catch (Exception e) when (e is JsonException or InvalidOperationException)
        {
            // Malformed stats (bad JSON, or a non-object where a struct is expected).
            return false;
        }
    }

    private static void AppendGroup(StructStatColumn? column, JsonElement root, string name)
    {
        if (column is null)
            return;
        if (root.TryGetProperty(name, out var group))
            column.AppendValue(group);
        else
            column.AppendNull();
    }

    /// <summary>One column of the stats struct, fed one JSON value per row.</summary>
    private interface IStatColumn : IDisposable
    {
        /// <summary>Appends the value, or a null when it isn't representable in this column's type.</summary>
        void AppendValue(JsonElement value);

        void AppendNull();

        /// <summary>Appends nulls until the column holds <paramref name="rowCount"/> entries.</summary>
        void PadTo(int rowCount);

        IArrowArray Build();
    }

    /// <summary>A nested group — the <c>minValues</c>/<c>maxValues</c>/<c>nullCount</c> structs
    /// themselves, and any struct column inside them.</summary>
    private sealed class StructStatColumn : IStatColumn
    {
        private readonly ArrowStructType _type;
        private readonly (string Name, IStatColumn Column)[] _children;
        private int _count;

        public StructStatColumn(List<Field> fields, int rowCount)
        {
            _type = new ArrowStructType(fields);
            _children = new (string, IStatColumn)[fields.Count];
            for (int i = 0; i < fields.Count; i++)
                _children[i] = (fields[i].Name, CreateColumn(fields[i], rowCount));
        }

        public void AppendValue(JsonElement value)
        {
            foreach (var (name, column) in _children)
            {
                if (value.ValueKind == JsonValueKind.Object
                    && value.TryGetProperty(name, out var child))
                    column.AppendValue(child);
                else
                    column.AppendNull();
            }
            _count++;
        }

        public void AppendNull()
        {
            foreach (var (_, column) in _children)
                column.AppendNull();
            _count++;
        }

        public void PadTo(int rowCount)
        {
            while (_count < rowCount)
                AppendNull();
        }

        public IArrowArray Build()
        {
            var arrays = new IArrowArray[_children.Length];
            for (int i = 0; i < _children.Length; i++)
                arrays[i] = _children[i].Column.Build();
            // The group itself is always present; absent values are nulls in the children.
            return new StructArray(_type, _count, arrays, ArrowBuffer.Empty, 0);
        }

        public void Dispose()
        {
            foreach (var (_, column) in _children)
                column.Dispose();
        }
    }

    private static IStatColumn CreateColumn(Field field, int rowCount) => field.DataType switch
    {
        ArrowStructType s => new StructStatColumn([.. s.Fields], rowCount),
        // Decimal types derive from FixedSizeBinaryType — keep them ahead of anything binary-shaped.
        Decimal128Type d => new DecimalStatColumn(rowCount, d),
        Int64Type => new FixedStatColumn<long>(rowCount, Int64Type.Default, TryReadInt64),
        Int32Type => new FixedStatColumn<int>(rowCount, Int32Type.Default, TryReadInt32),
        Int16Type => new FixedStatColumn<short>(rowCount, Int16Type.Default, TryReadInt16),
        Int8Type => new FixedStatColumn<sbyte>(rowCount, Int8Type.Default, TryReadInt8),
        FloatType => new FixedStatColumn<float>(rowCount, FloatType.Default, TryReadFloat),
        DoubleType => new FixedStatColumn<double>(rowCount, DoubleType.Default, TryReadDouble),
        Date32Type => new FixedStatColumn<int>(rowCount, Date32Type.Default, TryReadDate32),
        TimestampType t => new FixedStatColumn<long>(rowCount, t, TryReadTimestamp),
        StringType => new StringStatColumn(rowCount),
        // Nothing else reaches here: BuildBoundFields/BuildNullCountFields only emit the above.
        _ => new StringStatColumn(rowCount),
    };

    /// <summary>Converts a JSON stat value to the column's value type; false appends a null instead.</summary>
    private delegate bool TryReadStat<T>(JsonElement value, out T converted) where T : unmanaged;

    private static bool TryReadInt64(JsonElement value, out long converted)
    {
        converted = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out converted);
    }

    private static bool TryReadInt32(JsonElement value, out int converted)
    {
        converted = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out converted);
    }

    private static bool TryReadInt16(JsonElement value, out short converted)
    {
        converted = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetInt16(out converted);
    }

    private static bool TryReadInt8(JsonElement value, out sbyte converted)
    {
        converted = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetSByte(out converted);
    }

    private static bool TryReadFloat(JsonElement value, out float converted)
    {
        converted = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetSingle(out converted);
    }

    private static bool TryReadDouble(JsonElement value, out double converted)
    {
        converted = 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out converted);
    }

    /// <summary>Date32 holds days since the Unix epoch; Delta writes date bounds as "yyyy-MM-dd".</summary>
    private static bool TryReadDate32(JsonElement value, out int converted)
    {
        converted = 0;
        if (value.ValueKind != JsonValueKind.String
            || !DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            return false;
        converted = (int)(parsed.UtcDateTime.Date - s_epoch).TotalDays;
        return true;
    }

    /// <summary>
    /// Arrow timestamps hold microseconds since the Unix epoch; Delta writes timestamp bounds as
    /// ISO-8601 ("yyyy-MM-ddTHH:mm:ss.ffffff", with a trailing Z when the column is zoned).
    /// </summary>
    private static bool TryReadTimestamp(JsonElement value, out long converted)
    {
        converted = 0;
        if (value.ValueKind != JsonValueKind.String
            || !DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            return false;
        // 10 ticks per microsecond; TimeSpan.TicksPerMicrosecond is not on netstandard2.0.
        converted = (parsed.UtcDateTime.Ticks - s_epoch.Ticks) / 10;
        return true;
    }

    private static readonly DateTime s_epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>A fixed-width stat column; values the converter rejects land as nulls.</summary>
    private sealed class FixedStatColumn<T> : IStatColumn where T : unmanaged
    {
        private readonly FixedWidthColumn<T> _column;
        private readonly IArrowType _type;
        private readonly TryReadStat<T> _convert;

        public FixedStatColumn(int rowCount, IArrowType type, TryReadStat<T> convert)
        {
            _column = new FixedWidthColumn<T>(rowCount);
            _type = type;
            _convert = convert;
        }

        public void AppendValue(JsonElement value)
        {
            if (_convert(value, out T converted))
                _column.Append(converted);
            else
                _column.AppendNull();
        }

        public void AppendNull() => _column.AppendNull();

        public void PadTo(int rowCount) => _column.PadTo(rowCount);

        public IArrowArray Build() => _column.Build(_type);

        public void Dispose() => _column.Dispose();
    }

    /// <summary>
    /// A decimal bound, carried at the column's own precision and scale. The JSON number's digits are
    /// decoded exactly (never through <c>System.Decimal</c>, which silently rounds past ~28 digits)
    /// and rescaled to the column; a value that will not fit becomes a null rather than a wrong bound.
    /// </summary>
    private sealed class DecimalStatColumn : IStatColumn
    {
        private readonly FixedSizeBinaryColumn _column;
        private readonly Decimal128Type _type;

        public DecimalStatColumn(int rowCount, Decimal128Type type)
        {
            _type = type;
            _column = new FixedSizeBinaryColumn(rowCount, 16);
        }

        public void AppendValue(JsonElement value)
        {
            string? text = value.ValueKind switch
            {
                JsonValueKind.Number => value.GetRawText(),
                JsonValueKind.String => value.GetString(),
                _ => null,
            };

            if (text is null
                || !DecimalText.TryParse(text, out var unscaled, out int scale)
                || !TryRescale(ref unscaled, scale, _type.Scale)
                || !FitsPrecision(unscaled, _type.Precision))
            {
                _column.AppendNull();
                return;
            }

            Span<byte> bytes = stackalloc byte[16];
            WriteTwosComplement(unscaled, bytes);
            _column.Append(bytes);
        }

        public void AppendNull() => _column.AppendNull();

        public void PadTo(int rowCount) => _column.PadTo(rowCount);

        public IArrowArray Build() => _column.Build(_type);

        public void Dispose() => _column.Dispose();

        /// <summary>
        /// Restates <paramref name="unscaled"/> at <paramref name="targetScale"/>. Scaling UP is exact;
        /// scaling DOWN would drop digits, so it is refused — a truncated bound is a wrong bound, and a
        /// missing one merely costs a skipped file.
        /// </summary>
        private static bool TryRescale(ref BigInteger unscaled, int scale, int targetScale)
        {
            if (scale == targetScale)
                return true;
            if (scale > targetScale)
                return false;
            unscaled *= BigInteger.Pow(10, targetScale - scale);
            return true;
        }

        private static bool FitsPrecision(BigInteger unscaled, int precision) =>
            BigInteger.Abs(unscaled) < BigInteger.Pow(10, precision);

        /// <summary>Writes the two's-complement little-endian form Arrow's decimal buffers use.</summary>
        private static void WriteTwosComplement(BigInteger value, Span<byte> destination)
        {
            destination.Fill(value.Sign < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
            value.TryWriteBytes(destination, out _, isUnsigned: false, isBigEndian: false);
#else
            byte[] bytes = value.ToByteArray();
            bytes.AsSpan(0, Math.Min(bytes.Length, destination.Length)).CopyTo(destination);
#endif
        }
    }

    private sealed class StringStatColumn : IStatColumn
    {
        private readonly StringColumn _column;

        public StringStatColumn(int rowCount) => _column = new StringColumn(rowCount);

        public void AppendValue(JsonElement value)
        {
            if (value.ValueKind == JsonValueKind.String)
                _column.Append(value.GetString()!);
            else
                _column.AppendNull();
        }

        public void AppendNull() => _column.AppendNull();

        public void PadTo(int rowCount) => _column.PadTo(rowCount);

        public IArrowArray Build() => _column.Build();

        public void Dispose() => _column.Dispose();
    }

    #endregion
}
