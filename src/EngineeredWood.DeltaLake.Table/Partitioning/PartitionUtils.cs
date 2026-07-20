// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

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

            columns.Add(TakeRows(source.Column(col), rows));
        }

        return new RecordBatch(dataSchema, columns, rows.Count);
    }

    /// <summary>
    /// Creates a new array containing only the specified rows from the source.
    /// </summary>
    private static IArrowArray TakeRows(IArrowArray source, List<int> rows)
    {
        switch (source)
        {
            // Extension arrays (VARIANT) filter through their STORAGE and are re-wrapped. Without
            // this a partitioned write of ANY table containing a variant column would throw, even
            // when variant is not the partition column — BuildFilteredBatch takes rows from every
            // non-partition column. Must precede the storage-shaped cases below.
            case ExtensionArray ext:
            {
                var filteredStorage = TakeRows(ext.Storage, rows);
                return ((ExtensionType)ext.Data.DataType).CreateArray(filteredStorage);
            }
            case Int64Array int64:
            {
                var b = new Int64Array.Builder();
                foreach (int r in rows) { if (int64.IsNull(r)) b.AppendNull(); else b.Append(int64.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int32Array int32:
            {
                var b = new Int32Array.Builder();
                foreach (int r in rows) { if (int32.IsNull(r)) b.AppendNull(); else b.Append(int32.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int16Array int16:
            {
                var b = new Int16Array.Builder();
                foreach (int r in rows) { if (int16.IsNull(r)) b.AppendNull(); else b.Append(int16.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int8Array int8:
            {
                var b = new Int8Array.Builder();
                foreach (int r in rows) { if (int8.IsNull(r)) b.AppendNull(); else b.Append(int8.GetValue(r)!.Value); }
                return b.Build();
            }
            case DoubleArray dbl:
            {
                var b = new DoubleArray.Builder();
                foreach (int r in rows) { if (dbl.IsNull(r)) b.AppendNull(); else b.Append(dbl.GetValue(r)!.Value); }
                return b.Build();
            }
            case FloatArray flt:
            {
                var b = new FloatArray.Builder();
                foreach (int r in rows) { if (flt.IsNull(r)) b.AppendNull(); else b.Append(flt.GetValue(r)!.Value); }
                return b.Build();
            }
            case Date32Array d32:
            {
                var b = new Date32Array.Builder();
                foreach (int r in rows)
                {
                    if (d32.IsNull(r)) b.AppendNull();
                    else
                    {
                        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        b.Append(epoch.AddDays(d32.GetValue(r)!.Value));
                    }
                }
                return b.Build();
            }
            case StringArray str:
            {
                var b = new StringArray.Builder();
                foreach (int r in rows) { if (str.IsNull(r)) b.AppendNull(); else b.Append(str.GetString(r)); }
                return b.Build();
            }
            case LargeStringArray lstr:
            {
                var b = new StringArray.Builder();
                foreach (int r in rows) { if (lstr.IsNull(r)) b.AppendNull(); else b.Append(lstr.GetString(r)); }
                return b.Build();
            }
            case BooleanArray bln:
            {
                var b = new BooleanArray.Builder();
                foreach (int r in rows) { if (bln.IsNull(r)) b.AppendNull(); else b.Append(bln.GetValue(r)!.Value); }
                return b.Build();
            }
            case TimestampArray ts:
            {
                var tsType = (TimestampType)ts.Data.DataType;
                var b = new TimestampArray.Builder(tsType);
                foreach (int r in rows)
                {
                    if (ts.IsNull(r))
                        b.AppendNull();
                    else
                    {
                        long micros = ts.GetValue(r)!.Value;
                        b.Append(DateTimeOffset.FromUnixTimeMilliseconds(micros / 1000));
                    }
                }
                return b.Build();
            }
            case BinaryArray bin:
            {
                var b = new BinaryArray.Builder();
                foreach (int r in rows) { if (bin.IsNull(r)) b.AppendNull(); else b.Append(bin.GetBytes(r)); }
                return b.Build();
            }
            case StructArray st:
            {
                // A struct's children are aligned with the parent rows but do NOT incorporate the parent's
                // logical offset (StructArray.Fields wraps Data.Children directly), so child rows are indexed
                // at parentOffset + r. Each child is taken recursively; the struct's own validity is rebuilt.
                int off = st.Data.Offset;
                var childRows = off == 0 ? rows : rows.ConvertAll(r => r + off);
                var childData = new ArrayData[st.Data.Children.Length];
                for (int c = 0; c < st.Data.Children.Length; c++)
                {
                    childData[c] = TakeRows(ArrowArrayFactory.BuildArray(st.Data.Children[c]), childRows).Data;
                }
                var validity = new ArrowBuffer.BitmapBuilder(rows.Count);
                int nullCount = 0;
                foreach (int r in rows)
                {
                    bool isNull = st.IsNull(r);
                    validity.Append(!isNull);
                    if (isNull)
                        nullCount++;
                }
                var data = new ArrayData(st.Data.DataType, rows.Count, nullCount, 0,
                                         new[] { validity.Build() }, childData);
                return ArrowArrayFactory.BuildArray(data);
            }
            default:
            {
                // Every other fixed-width type (unsigned ints, decimals, Time/Duration, HalfFloat,
                // FixedSizeBinary, ...) is filtered by copying its raw value-slot bytes — preserving the exact
                // type and avoiding per-type builders. A type we cannot slice (nested list/struct/map/...) must
                // THROW, never fall through unfiltered: an unfiltered column has the wrong length and silently
                // corrupts the partitioned write (mispaired rows in the written partition file).
                int? width = FixedWidthBytes(source.Data.DataType);
                if (width is int byteWidth)
                    return TakeFixedWidth(source, rows, byteWidth);
                throw new NotSupportedException(
                    $"PartitionUtils.TakeRows cannot filter column type {source.Data.DataType.TypeId} — "
                    + "partitioned writes do not support this column type.");
            }
        }
    }

    /// <summary>
    /// Byte width of a fixed-width Arrow type (Boolean is bit-packed → excluded, handled by its own builder
    /// case), or null for variable-width / nested types.
    /// </summary>
    private static int? FixedWidthBytes(IArrowType type) => type switch
    {
        Int8Type or UInt8Type => 1,
        Int16Type or UInt16Type or HalfFloatType => 2,
        Int32Type or UInt32Type or FloatType or Date32Type or Time32Type => 4,
        Int64Type or UInt64Type or DoubleType or Date64Type or Time64Type or TimestampType or DurationType => 8,
        // Decimal128Type / Decimal256Type derive from FixedSizeBinaryType, so ByteWidth (16 / 32) is exact.
        FixedSizeBinaryType fsb => fsb.ByteWidth,
        _ => null,
    };

    /// <summary>
    /// Takes specific rows from a fixed-width array by copying the raw value-slot bytes (accounting for the
    /// source array's logical offset) and rebuilding the validity bitmap. Type-agnostic and lossless.
    /// </summary>
    private static IArrowArray TakeFixedWidth(IArrowArray source, List<int> rows, int byteWidth)
    {
        ReadOnlySpan<byte> srcValues = source.Data.Buffers[1].Span;
        int srcOffset = source.Data.Offset;
        var valueBytes = new byte[rows.Count * byteWidth];
        var validity = new ArrowBuffer.BitmapBuilder(rows.Count);
        int nullCount = 0;
        int i = 0;
        foreach (int r in rows)
        {
            bool isNull = source.IsNull(r);
            validity.Append(!isNull);
            if (isNull)
                nullCount++;
            else
                srcValues.Slice((srcOffset + r) * byteWidth, byteWidth)
                         .CopyTo(valueBytes.AsSpan(i * byteWidth, byteWidth));
            i++;
        }

        var buffers = new[] { validity.Build(), new ArrowBuffer(valueBytes) };
        var data = new ArrayData(source.Data.DataType, rows.Count, nullCount, 0, buffers);
        return ArrowArrayFactory.BuildArray(data);
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
    /// </summary>
    private static IArrowArray BuildConstantArray(IArrowType type, string? value, int length)
    {
        var inv = System.Globalization.CultureInfo.InvariantCulture;
        bool isNull = value is null
            || value == "__HIVE_DEFAULT_PARTITION__"
            || (value.Length == 0 && type is not StringType);

        switch (type)
        {
            case StringType:
            {
                var builder = new StringArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(value);
                }
                return builder.Build();
            }
            case Int64Type:
            {
                long v = isNull ? 0 : long.Parse(value!, inv);
                var builder = new Int64Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case Int32Type:
            {
                int v = isNull ? 0 : int.Parse(value!, inv);
                var builder = new Int32Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case Int16Type:
            {
                short v = isNull ? (short)0 : short.Parse(value!, inv);
                var builder = new Int16Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case Int8Type:
            {
                sbyte v = isNull ? (sbyte)0 : sbyte.Parse(value!, inv);
                var builder = new Int8Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case DoubleType:
            {
                double v = isNull ? 0 : double.Parse(value!, inv);
                var builder = new DoubleArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case FloatType:
            {
                float v = isNull ? 0 : float.Parse(value!, inv);
                var builder = new FloatArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case Decimal128Type decType:
            {
                decimal v = isNull ? 0m : decimal.Parse(value!, System.Globalization.NumberStyles.Number, inv);
                var builder = new Decimal128Array.Builder(decType);
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case Date32Type:
            {
                // "yyyy-MM-dd"
                var dt = isNull ? default : DateTime.Parse(value!, inv,
                    System.Globalization.DateTimeStyles.None);
                var builder = new Date32Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(dt);
                }
                return builder.Build();
            }
            case BooleanType:
            {
                bool v = !isNull && bool.Parse(value!);
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(v);
                }
                return builder.Build();
            }
            case TimestampType tsType:
            {
                // "yyyy-MM-dd HH:mm:ss[.ffffff]" — no zone suffix; timestamps are stored UTC-normalized.
                var dto = isNull ? default : DateTimeOffset.Parse(value!, inv,
                    System.Globalization.DateTimeStyles.AssumeUniversal);
                var builder = new TimestampArray.Builder(tsType);
                for (int i = 0; i < length; i++)
                {
                    if (isNull) builder.AppendNull(); else builder.Append(dto);
                }
                return builder.Build();
            }
            default:
                // Falling back to a string column would mismatch the table schema downstream — throw.
                throw new NotSupportedException(
                    $"Partition columns of Arrow type {type.TypeId} are not supported on read.");
        }
    }

    private static string FormatTimestampPartitionValue(TimestampArray ts, int row)
    {
        long micros = ts.GetValue(row)!.Value;
        var tsType = (TimestampType)ts.Data.DataType;

        long asMicros = tsType.Unit switch
        {
            TimeUnit.Second => micros * 1_000_000,
            TimeUnit.Millisecond => micros * 1_000,
            TimeUnit.Microsecond => micros,
            TimeUnit.Nanosecond => micros / 1_000,
            _ => micros,
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
