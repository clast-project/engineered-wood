// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Lance.Table.Deletions;

/// <summary>
/// Builds a new <see cref="RecordBatch"/> containing only the rows that
/// are NOT marked deleted by the given <see cref="DeletionMask"/>. Each
/// column is materialised through its type-specific Arrow builder.
///
/// <para>This is the per-type case-switch approach (verbose but stable
/// across Arrow versions). When Apache.Arrow ships a public
/// <c>Take</c> kernel we can swap to that.</para>
/// </summary>
internal static class RecordBatchRowFilter
{
    public static RecordBatch Apply(
        RecordBatch batch, Apache.Arrow.Schema schema, DeletionMask mask)
    {
        if (mask.DeletedCount == 0)
            return batch;

        var keepRows = new List<int>(batch.Length - mask.DeletedCount);
        for (int i = 0; i < batch.Length; i++)
            if (!mask.IsDeleted(i)) keepRows.Add(i);

        return ApplyKeepList(batch, schema, keepRows);
    }

    /// <summary>
    /// Apply a pre-computed list of kept row indices. Used by callers
    /// that combine multiple filter sources (e.g., deletion mask plus
    /// predicate evaluation) into a single keep-list to avoid two
    /// take passes.
    /// </summary>
    public static RecordBatch ApplyKeepList(
        RecordBatch batch, Apache.Arrow.Schema schema, List<int> keepRows)
    {
        if (keepRows.Count == batch.Length) return batch;

        var columns = new IArrowArray[batch.ColumnCount];
        for (int c = 0; c < batch.ColumnCount; c++)
            columns[c] = TakeRows(batch.Column(c), keepRows);

        return new RecordBatch(schema, columns, keepRows.Count);
    }

    private static IArrowArray TakeRows(IArrowArray source, List<int> rows)
    {
        switch (source)
        {
            case Int8Array a:
            {
                var b = new Int8Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int16Array a:
            {
                var b = new Int16Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int32Array a:
            {
                var b = new Int32Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case Int64Array a:
            {
                var b = new Int64Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case UInt8Array a:
            {
                var b = new UInt8Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case UInt16Array a:
            {
                var b = new UInt16Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case UInt32Array a:
            {
                var b = new UInt32Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case UInt64Array a:
            {
                var b = new UInt64Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case FloatArray a:
            {
                var b = new FloatArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case DoubleArray a:
            {
                var b = new DoubleArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case BooleanArray a:
            {
                var b = new BooleanArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetValue(r)!.Value); }
                return b.Build();
            }
            case StringArray a:
            {
                var b = new StringArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetString(r)); }
                return b.Build();
            }
            case BinaryArray a:
            {
                var b = new BinaryArray.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetBytes(r)); }
                return b.Build();
            }
            case Date32Array a:
            {
                var b = new Date32Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(Epoch.AddDays(a.GetValue(r)!.Value)); }
                return b.Build();
            }
            case Date64Array a:
            {
                var b = new Date64Array.Builder();
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(Epoch.AddMilliseconds(a.GetValue(r)!.Value)); }
                return b.Build();
            }
            case TimestampArray a:
            {
                var b = new TimestampArray.Builder((Apache.Arrow.Types.TimestampType)a.Data.DataType);
                foreach (int r in rows) { if (a.IsNull(r)) b.AppendNull(); else b.Append(a.GetTimestamp(r)!.Value); }
                return b.Build();
            }
            case Decimal128Array a:
                return TakeFixedWidth(a, a.ValueBuffer, rows, 16, d => new Decimal128Array(d));
            case Decimal256Array a:
                return TakeFixedWidth(a, a.ValueBuffer, rows, 32, d => new Decimal256Array(d));
            default:
                throw new NotImplementedException(
                    $"Row-take not implemented for Arrow type {source.GetType().Name}.");
        }
    }

    private static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // Slices a fixed-width (decimal) array by copying each kept value's raw bytes verbatim, so full
    // precision is preserved regardless of the decimal's magnitude.
    private static IArrowArray TakeFixedWidth(
        Apache.Arrow.Array source, ArrowBuffer valueBuffer, List<int> rows, int width,
        Func<ArrayData, IArrowArray> create)
    {
        var bytes = new byte[rows.Count * width];
        var validity = new ArrowBuffer.BitmapBuilder();
        int nullCount = 0;
        var src = valueBuffer.Span;
        for (int k = 0; k < rows.Count; k++)
        {
            int r = rows[k];
            if (source.IsNull(r)) { validity.Append(false); nullCount++; continue; }
            validity.Append(true);
            src.Slice(r * width, width).CopyTo(bytes.AsSpan(k * width, width));
        }
        var data = new ArrayData(
            source.Data.DataType, rows.Count, nullCount, 0,
            new[] { validity.Build(), new ArrowBuffer(bytes) });
        return create(data);
    }
}
