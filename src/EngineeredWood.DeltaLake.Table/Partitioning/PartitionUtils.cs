// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.DeltaLake.Table.Partitioning;

/// <summary>
/// Utilities for splitting RecordBatch data by partition column values
/// and stripping/restoring partition columns.
/// </summary>
internal static class PartitionUtils
{
    /// <summary>
    /// Splits a RecordBatch into groups by unique partition column value combinations.
    /// Returns (partitionValues, dataBatchWithoutPartitionColumns) pairs.
    /// </summary>
    public static List<(Dictionary<string, string> PartitionValues, RecordBatch Data)> SplitByPartition(
        RecordBatch batch, IReadOnlyList<string> partitionColumns)
    {
        if (partitionColumns.Count == 0)
            return [(new Dictionary<string, string>(), batch)];

        // Find partition column indices
        var partColIndices = new int[partitionColumns.Count];
        for (int i = 0; i < partitionColumns.Count; i++)
        {
            partColIndices[i] = batch.Schema.GetFieldIndex(partitionColumns[i]);
            if (partColIndices[i] < 0)
                throw new InvalidOperationException(
                    $"Partition column '{partitionColumns[i]}' not found in batch schema.");
        }

        // Group rows by partition values
        var groups = new Dictionary<string, (Dictionary<string, string> Values, List<int> Rows)>(
            StringComparer.Ordinal);

        for (int row = 0; row < batch.Length; row++)
        {
            var partValues = new Dictionary<string, string>();
            var keyParts = new string[partitionColumns.Count];

            for (int p = 0; p < partitionColumns.Count; p++)
            {
                // A null partition value is stored as JSON null in add.partitionValues (the Delta spec /
                // Spark convention); __HIVE_DEFAULT_PARTITION__ is only the DIRECTORY name.
                string? value = GetStringValue(batch.Column(partColIndices[p]), row);
                partValues[partitionColumns[p]] = value!;
                keyParts[p] = value ?? "__HIVE_DEFAULT_PARTITION__";
            }

            string groupKey = string.Join("\0", keyParts);

            if (!groups.TryGetValue(groupKey, out var group))
            {
                group = (partValues, new List<int>());
                groups[groupKey] = group;
            }

            group.Rows.Add(row);
        }

        // Build non-partition schema
        var dataSchema = BuildNonPartitionSchema(batch.Schema, partColIndices);

        // Build output batches
        var result = new List<(Dictionary<string, string>, RecordBatch)>();

        foreach (var group in groups)
        {
            var dataBatch = BuildFilteredBatch(batch, dataSchema, partColIndices, group.Value.Rows);
            result.Add((group.Value.Values, dataBatch));
        }

        return result;
    }

    /// <summary>
    /// Builds a directory path for partition values (e.g., "date=2024-01-01/region=us").
    /// Values are URI-encoded for safety.
    /// </summary>
    public static string BuildPartitionPath(Dictionary<string, string> partitionValues)
    {
        if (partitionValues.Count == 0)
            return "";

        return string.Join("/",
            partitionValues.Select(kv =>
                $"{DeltaPath.EscapePathName(kv.Key)}={(kv.Value is null ? "__HIVE_DEFAULT_PARTITION__" : DeltaPath.EscapePathName(kv.Value))}"));
    }

    /// <summary>
    /// Adds partition columns as constant-value arrays to a RecordBatch read from a data file.
    /// The partition columns are appended at the positions matching the full table schema.
    /// Under column mapping a file's <paramref name="partitionValues"/> are keyed by the PHYSICAL column name
    /// (the Delta-spec convention Spark follows — physical keys survive a partition-column rename), while files
    /// written before that convention are logical-keyed — so each value is looked up under BOTH names via the
    /// optional <paramref name="logicalToPhysical"/> map.
    /// </summary>
    public static RecordBatch AddPartitionColumns(
        RecordBatch dataBatch,
        Apache.Arrow.Schema fullSchema,
        IReadOnlyDictionary<string, string> partitionValues,
        IReadOnlyList<string> partitionColumns,
        IReadOnlyDictionary<string, string>? logicalToPhysical = null)
    {
        if (partitionColumns.Count == 0)
            return dataBatch;

        var partColSet = new HashSet<string>(partitionColumns, StringComparer.Ordinal);
        var columns = new List<IArrowArray>();
        var fields = new List<Field>();
        int dataColIdx = 0;

        for (int i = 0; i < fullSchema.FieldsList.Count; i++)
        {
            var field = fullSchema.FieldsList[i];

            if (partColSet.Contains(field.Name))
            {
                // Build a constant array from the partition value (logical key, else the physical key).
                // A missing key means the writer omitted it — treated as null, like a JSON-null value.
                if (!partitionValues.TryGetValue(field.Name, out var v)
                    && (logicalToPhysical is null || !logicalToPhysical.TryGetValue(field.Name, out var phys)
                        || !partitionValues.TryGetValue(phys, out v)))
                {
                    v = null;
                }
                columns.Add(BuildConstantArray(field.DataType, v, dataBatch.Length));
                fields.Add(field);
            }
            else
            {
                if (dataColIdx < dataBatch.ColumnCount)
                {
                    columns.Add(dataBatch.Column(dataColIdx));
                    fields.Add(dataBatch.Schema.FieldsList[dataColIdx]);
                    dataColIdx++;
                }
            }
        }

        var schema = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schema.Field(f);

        return new RecordBatch(schema.Build(), columns, dataBatch.Length);
    }

    /// <summary>
    /// Appends partition columns as constant-value arrays to the end of a data batch.
    /// Required by IcebergCompatV1/V2: partition values must be materialized
    /// into the Parquet data files, positioned after the data columns.
    /// </summary>
    public static RecordBatch AppendPartitionColumns(
        RecordBatch dataBatch,
        Dictionary<string, string> partitionValues,
        DeltaLake.Schema.StructType deltaSchema,
        IReadOnlyList<string> partitionColumns,
        IReadOnlyDictionary<string, string> logicalToPhysical)
    {
        var columns = new List<IArrowArray>(dataBatch.ColumnCount + partitionColumns.Count);
        var fields = new List<Field>(dataBatch.ColumnCount + partitionColumns.Count);

        // Copy existing data columns
        for (int i = 0; i < dataBatch.ColumnCount; i++)
        {
            columns.Add(dataBatch.Column(i));
            fields.Add(dataBatch.Schema.FieldsList[i]);
        }

        // Resolve partition column types from the Delta schema and append
        foreach (string partCol in partitionColumns)
        {
            var deltaField = deltaSchema.Fields.FirstOrDefault(
                f => string.Equals(f.Name, partCol, StringComparison.Ordinal));
            if (deltaField is null)
                continue;

            var arrowType = DeltaLake.Schema.SchemaConverter.ToArrowType(deltaField.Type);

            // Use physical name if column mapping is active
            string physicalName = logicalToPhysical.TryGetValue(partCol, out string? pn)
                ? pn : partCol;

            string? value = partitionValues.TryGetValue(partCol, out var v) ? v : null;
            columns.Add(BuildConstantArray(arrowType, value, dataBatch.Length));
            fields.Add(new Field(physicalName, arrowType, deltaField.Nullable));
        }

        var schemaBuilder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schemaBuilder.Field(f);

        return new RecordBatch(schemaBuilder.Build(), columns, dataBatch.Length);
    }

    private static Apache.Arrow.Schema BuildNonPartitionSchema(
        Apache.Arrow.Schema fullSchema, int[] partColIndices)
    {
        var partSet = new HashSet<int>(partColIndices);
        var builder = new Apache.Arrow.Schema.Builder();

        for (int i = 0; i < fullSchema.FieldsList.Count; i++)
        {
            if (!partSet.Contains(i))
                builder.Field(fullSchema.FieldsList[i]);
        }

        return builder.Build();
    }

    private static RecordBatch BuildFilteredBatch(
        RecordBatch source, Apache.Arrow.Schema dataSchema,
        int[] partColIndices, List<int> rows)
    {
        var partSet = new HashSet<int>(partColIndices);
        var columns = new List<IArrowArray>();

        for (int col = 0; col < source.ColumnCount; col++)
        {
            if (partSet.Contains(col))
                continue;

            columns.Add(ArrowCompute.Take(source.Column(col), rows));
        }

        return new RecordBatch(dataSchema, columns, rows.Count);
    }

    /// <summary>
    /// Gets a string representation of a value for partition key construction.
    /// </summary>
    private static string? GetStringValue(IArrowArray array, int row)
    {
        if (array.IsNull(row))
            return null;

        return array switch
        {
            StringArray sa => sa.GetString(row),
            LargeStringArray lsa => lsa.GetString(row),
            Int64Array i64 => i64.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Int32Array i32 => i32.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Int16Array i16 => i16.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Int8Array i8 => i8.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            DoubleArray d => d.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            FloatArray f => f.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            BooleanArray b => b.GetValue(row)!.Value ? "true" : "false",
            Date32Array d32 => DateTimeOffset.FromUnixTimeSeconds(
                d32.GetValue(row)!.Value * 86400L).ToString("yyyy-MM-dd"),
            TimestampArray ts => FormatTimestampPartitionValue(ts, row),
            // Spec dot-notation decimal string.
            Decimal128Array dec => dec.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            // A type we cannot encode per the spec (binary, nested, ...) must THROW, never fall back to
            // .ToString() — that silently wrote the .NET type name as the partition value.
            _ => throw new NotSupportedException(
                $"Partition values of Arrow type {array.Data.DataType.TypeId} are not supported."),
        };
    }

    /// <summary>
    /// Builds a constant-value array of the given type and length.
    /// Used to materialize partition columns on read. A null <paramref name="value"/>, the
    /// <c>__HIVE_DEFAULT_PARTITION__</c> directory sentinel (some writers put it into
    /// <c>partitionValues</c>), or an empty string for a non-string type all decode as SQL NULL.
    ///
    /// <para>The value is decoded ONCE into its raw Arrow encoding and then tiled across the column, rather
    /// than appended per row through a typed builder. Besides being the read path's hottest loop — it runs
    /// per partition column, per batch, per file of every partitioned scan — that is what makes it lossless:
    /// a builder round-trips each value through its .NET surface type, which for <c>decimal</c> silently
    /// rounds away anything past ~28 significant digits.</para>
    /// </summary>
    private static IArrowArray BuildConstantArray(IArrowType type, string? value, int length)
    {
        bool isNull = value is null
            || value == "__HIVE_DEFAULT_PARTITION__"
            || (value.Length == 0 && type is not StringType);

        if (isNull)
            return ArrowCompute.MakeNullArray(type, length);

        // The two shapes are inverses and each gets its own path: an all-null column needs an allocated,
        // all-zero validity bitmap, while a constant column needs no validity buffer at all.
        return ArrowCompute.Repeat(type, EncodePartitionValue(type, value!), length);
    }

    /// <summary>
    /// Decodes one partition value string into the raw little-endian Arrow encoding of a single value slot
    /// (or, for a string column, the UTF-8 bytes).
    /// </summary>
    private static byte[] EncodePartitionValue(IArrowType type, string value)
    {
        var inv = System.Globalization.CultureInfo.InvariantCulture;

        switch (type)
        {
            case StringType:
                return System.Text.Encoding.UTF8.GetBytes(value);

            case Int64Type:
                return Le(BitConverter.GetBytes(long.Parse(value, inv)));
            case Int32Type:
                return Le(BitConverter.GetBytes(int.Parse(value, inv)));
            case Int16Type:
                return Le(BitConverter.GetBytes(short.Parse(value, inv)));
            case Int8Type:
                return [unchecked((byte)sbyte.Parse(value, inv))];

            case DoubleType:
                return Le(BitConverter.GetBytes(double.Parse(value, inv)));
            case FloatType:
                return Le(BitConverter.GetBytes(float.Parse(value, inv)));

            case BooleanType:
                return [bool.Parse(value) ? (byte)1 : (byte)0];

            case Decimal128Type decType:
                return EncodeDecimal128(decType, value);

            case Date32Type:
            {
                // "yyyy-MM-dd", stored as a day count from the epoch.
                var dt = DateTime.Parse(value, inv, System.Globalization.DateTimeStyles.None);
                int days = (dt.Date - new DateTime(1970, 1, 1)).Days;
                return Le(BitConverter.GetBytes(days));
            }

            case TimestampType tsType:
            {
                // "yyyy-MM-dd HH:mm:ss[.ffffff]" — no zone suffix; timestamps are stored UTC-normalized.
                var dto = DateTimeOffset.Parse(value, inv,
                    System.Globalization.DateTimeStyles.AssumeUniversal);
                long micros = (dto.UtcTicks - EpochUtcTicks) / TicksPerMicrosecond;

                // The inverse of FormatTimestampPartitionValue, and it accepts exactly the units that one
                // emits. Anything else would need the stored value scaled by a factor that discards digits,
                // and a partition value is an exact identity rather than a bound.
                long stored = tsType.Unit switch
                {
                    TimeUnit.Microsecond => micros,
                    TimeUnit.Millisecond => micros / 1_000,
                    _ => throw new NotSupportedException(
                        $"Timestamp partition columns with {tsType.Unit} precision are not supported; "
                        + "the column must be millisecond or microsecond precision."),
                };
                return Le(BitConverter.GetBytes(stored));
            }

            default:
                // Falling back to a string column would mismatch the table schema downstream — throw.
                throw new NotSupportedException(
                    $"Partition columns of Arrow type {type.TypeId} are not supported on read.");
        }
    }

    private const long TicksPerMicrosecond = 10L;
    private static readonly long EpochUtcTicks =
        new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

    /// <summary>
    /// Arrow value buffers are little-endian; <see cref="BitConverter"/> follows the host, so flip on a
    /// big-endian one rather than writing the wrong bytes there.
    /// </summary>
    private static byte[] Le(byte[] hostOrder)
    {
        // Qualified: `Array` alone is ambiguous here, since Apache.Arrow declares one too.
        if (!BitConverter.IsLittleEndian)
            System.Array.Reverse(hostOrder);
        return hostOrder;
    }

    /// <summary>
    /// Encodes a decimal partition value as Decimal128's 16-byte little-endian two's complement.
    ///
    /// <para>Parsed through <see cref="DeltaLake.Schema.DecimalText"/> rather than
    /// <see cref="decimal"/>: <c>decimal.Parse</c> silently rounds a value carrying more than ~28
    /// significant digits and throws on anything wider still, and Delta's <c>decimal(38,s)</c> range is
    /// wider than that in both directions. Measured before the change: a <c>decimal(38,10)</c> partition
    /// value of <c>1234567890123456789012345678.1234567890</c> materialized as
    /// <c>1234567890123456789012345678.1000000000</c> for every row of the partition, and a
    /// <c>decimal(38,0)</c> value above <see cref="decimal.MaxValue"/> failed the read outright.</para>
    /// </summary>
    private static byte[] EncodeDecimal128(Decimal128Type decType, string value)
    {
        if (!DeltaLake.Schema.DecimalText.TryParse(value, out var unscaled, out int scale))
        {
            throw new FormatException(
                $"Partition value '{value}' is not a valid decimal for a "
                + $"decimal({decType.Precision},{decType.Scale}) column.");
        }

        if (!DeltaLake.Schema.DecimalText.TryRescale(unscaled, scale, decType.Scale, out var scaled))
        {
            throw new FormatException(
                $"Partition value '{value}' carries more fractional digits than its column's scale of "
                + $"{decType.Scale} can hold, so restating it would change the value.");
        }

        var bytes = new byte[16];
        // Sign-extend first, so a negative value's high bytes are 0xFF rather than 0x00.
        if (scaled.Sign < 0)
            bytes.AsSpan().Fill(0xFF);

#if NET6_0_OR_GREATER
        if (!scaled.TryWriteBytes(bytes, out _, isUnsigned: false, isBigEndian: false))
            throw new OverflowException(DecimalRangeMessage(value, decType));
#else
        var le = scaled.ToByteArray(); // little-endian, signed, minimal representation
        if (le.Length > 16)
            throw new OverflowException(DecimalRangeMessage(value, decType));
        le.CopyTo(bytes, 0);
#endif
        return bytes;
    }

    private static string DecimalRangeMessage(string value, Decimal128Type decType) =>
        $"Partition value '{value}' does not fit a decimal({decType.Precision},{decType.Scale}) column's "
        + "128-bit storage.";

    private static string FormatTimestampPartitionValue(TimestampArray ts, int row)
    {
        long micros = ts.GetValue(row)!.Value;
        var tsType = (TimestampType)ts.Data.DataType;

        // Reached only from partition splitting of a batch being WRITTEN, and the write path already
        // refuses the units with no faithful encoding (SchemaConverter.ThrowIfUnsupportedTimestampUnit).
        // Throw rather than convert if one arrives anyway: a partition value is an exact identity, not a
        // bound, so rounding it produces a value that will not round-trip — and the nanosecond arm used to
        // do exactly that silently. Same rule as GetStringValue's unsupported-type arm above.
        long asMicros = tsType.Unit switch
        {
            TimeUnit.Millisecond => micros * 1_000,
            TimeUnit.Microsecond => micros,
            _ => throw new NotSupportedException(
                $"Timestamp partition columns with {tsType.Unit} precision are not supported; "
                + "cast the column to TimeUnit.Microsecond before writing."),
        };

        var dto = DateTimeOffset.FromUnixTimeMilliseconds(asMicros / 1_000)
            .AddTicks((asMicros % 1_000) * 10);

        // Both spec-legal forms; Spark omits the fraction when it is zero.
        string fmt = asMicros % 1_000_000 == 0 ? "yyyy-MM-dd HH:mm:ss" : "yyyy-MM-dd HH:mm:ss.ffffff";
        return tsType.Timezone is not null
            ? dto.ToString(fmt, System.Globalization.CultureInfo.InvariantCulture)
            : dto.UtcDateTime.ToString(fmt, System.Globalization.CultureInfo.InvariantCulture);
    }
}
