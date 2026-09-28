// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Renders one value of any Arrow array as text, so that two reads of the same rows can be compared
/// value by value whatever their types. Fixed-width values render as their bytes (so NaN payloads,
/// decimals and INT96 compare exactly); nested values render their children.
/// </summary>
internal static class ArrowValues
{
    public static string Render(IArrowArray array, int row)
    {
        if (array.IsNull(row))
            return "null";

        switch (array)
        {
            case ExtensionArray extension:
                return Render(extension.Storage, row);
            case BooleanArray b:
                return b.GetValue(row)!.Value ? "true" : "false";
            case BinaryArray b: // StringArray too
                return Convert.ToBase64String(b.GetBytes(row).ToArray());
            case LargeBinaryArray b:
                return Convert.ToBase64String(b.GetBytes(row).ToArray());
            case BinaryViewArray b: // StringViewArray too
                return Convert.ToBase64String(b.GetBytes(row).ToArray());
            case FixedSizeBinaryArray b:
                return Convert.ToBase64String(b.GetBytes(row).ToArray());
            case ListArray l: // MapArray too
                return "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[row], l.GetValueLength(row)).Select(j => Render(l.Values, j))) + "]";
            case LargeListArray l:
                return "[" + string.Join(",", Enumerable.Range((int)l.ValueOffsets[row], l.GetValueLength(row)).Select(j => Render(l.Values, j))) + "]";
            case FixedSizeListArray l:
            {
                int size = ((FixedSizeListType)l.Data.DataType).ListSize;
                return "[" + string.Join(",", Enumerable.Range((row + l.Offset) * size, size).Select(j => Render(l.Values, j))) + "]";
            }
            case StructArray s:
                return "{" + string.Join(",", s.Fields.Select(f => Render(f, row))) + "}";
            case DictionaryArray d:
                return Render(d.Dictionary, IndexOf(d.Indices, row));
        }

        if (array.Data.DataType is FixedWidthType fixedWidth && fixedWidth.BitWidth % 8 == 0)
        {
            int width = fixedWidth.BitWidth / 8;
            var bytes = array.Data.Buffers[1].Span.Slice((array.Offset + row) * width, width);
            return Convert.ToBase64String(bytes.ToArray());
        }

        throw new NotSupportedException($"Cannot render {array.Data.DataType.Name}.");
    }

    /// <summary>Every column's values at <paramref name="row"/>, joined.</summary>
    public static string RenderRow(RecordBatch batch, int row) =>
        string.Join("|", Enumerable.Range(0, batch.ColumnCount).Select(c => Render(batch.Column(c), row)));

    private static int IndexOf(IArrowArray indices, int row) => indices switch
    {
        Int8Array a => a.GetValue(row)!.Value,
        Int16Array a => a.GetValue(row)!.Value,
        Int32Array a => a.GetValue(row)!.Value,
        Int64Array a => checked((int)a.GetValue(row)!.Value),
        UInt8Array a => a.GetValue(row)!.Value,
        UInt16Array a => a.GetValue(row)!.Value,
        UInt32Array a => checked((int)a.GetValue(row)!.Value),
        _ => throw new NotSupportedException(indices.Data.DataType.Name),
    };
}
