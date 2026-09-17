// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace EngineeredWood.Vortex.Tests.TestHelpers;

/// <summary>
/// Renders Arrow values and types in a canonical text form that ignores physical layout, so two
/// readers that choose different Arrow representations of the same Vortex data compare equal:
/// <c>String</c>/<c>LargeString</c>/<c>StringView</c>, the binary equivalents, every list flavour
/// (<c>List</c>, <c>LargeList</c>, <c>ListView</c>, <c>LargeListView</c>), decimals of any width,
/// and dictionary-encoded arrays all render as their logical values.
/// </summary>
internal static class ArrowValues
{
    /// <summary>
    /// A canonical logical type name, for comparing schemas. Nested fields carry their
    /// nullability (<c>?</c>), and a map its key sortedness, since both are part of the type.
    /// </summary>
    public static string TypeName(IArrowType type) => type switch
    {
        StringType or LargeStringType or StringViewType => "utf8",
        BinaryType or LargeBinaryType or BinaryViewType => "binary",
        MapType m => $"map<{FieldName(m.KeyField)},{FieldName(m.ValueField)}>{(m.KeySorted ? "(sorted)" : "")}",
        ListType l => $"list<{FieldName(l.ValueField)}>",
        LargeListType l => $"list<{FieldName(l.ValueField)}>",
        ListViewType l => $"list<{FieldName(l.ValueField)}>",
        LargeListViewType l => $"list<{FieldName(l.ValueField)}>",
        FixedSizeListType f => $"fixed_list<{FieldName(f.ValueField)}>[{f.ListSize}]",
        StructType s => "struct<" + string.Join(",", s.Fields.Select(f => $"{f.Name}:{FieldName(f)}")) + ">",
        DictionaryType d => TypeName(d.ValueType),
        Decimal32Type d => $"decimal({d.Precision},{d.Scale})",
        Decimal64Type d => $"decimal({d.Precision},{d.Scale})",
        Decimal128Type d => $"decimal({d.Precision},{d.Scale})",
        Decimal256Type d => $"decimal({d.Precision},{d.Scale})",
        // After the decimals, which derive from it.
        FixedSizeBinaryType f => $"fixed_binary({f.ByteWidth})",
        TimestampType t => $"timestamp({t.Unit},{t.Timezone ?? ""})",
        Time32Type t => $"time({t.Unit})",
        Time64Type t => $"time({t.Unit})",
        _ => type.Name,
    };

    /// <summary>A field's canonical type, marked <c>?</c> when nullable.</summary>
    public static string FieldName(Field field) => TypeName(field.DataType) + (field.IsNullable ? "?" : "");

    /// <summary>The canonical text of <paramref name="array"/>[<paramref name="index"/>].</summary>
    public static string Render(IArrowArray array, int index)
    {
        var sb = new StringBuilder();
        Append(sb, array, index);
        return sb.ToString();
    }

    private static void Append(StringBuilder sb, IArrowArray array, int index)
    {
        if (array is DictionaryArray dict)
        {
            if (dict.IsNull(index)) { sb.Append("null"); return; }
            Append(sb, dict.Dictionary, DictionaryKey(dict.Indices, index));
            return;
        }
        if (array.IsNull(index)) { sb.Append("null"); return; }

        switch (array)
        {
            case BooleanArray a: sb.Append(a.GetValue(index)!.Value ? "true" : "false"); break;
            case Int8Array a: sb.Append(a.GetValue(index)); break;
            case Int16Array a: sb.Append(a.GetValue(index)); break;
            case Int32Array a: sb.Append(a.GetValue(index)); break;
            case Int64Array a: sb.Append(a.GetValue(index)); break;
            case UInt8Array a: sb.Append(a.GetValue(index)); break;
            case UInt16Array a: sb.Append(a.GetValue(index)); break;
            case UInt32Array a: sb.Append(a.GetValue(index)); break;
            case UInt64Array a: sb.Append(a.GetValue(index)); break;
#if NET5_0_OR_GREATER
            case HalfFloatArray a: AppendFloat(sb, (double)a.GetValue(index)!.Value); break;
#endif
            // Bit-exact, so -0.0 and 0.0 differ; every NaN renders alike, since a reader has no
            // obligation to preserve a NaN's payload.
            case FloatArray a: AppendFloat(sb, a.GetValue(index)!.Value); break;
            case DoubleArray a: AppendFloat(sb, a.GetValue(index)!.Value); break;
            case Decimal32Array a: sb.Append(a.GetString(index)); break;
            case Decimal64Array a: sb.Append(a.GetString(index)); break;
            case Decimal128Array a: sb.Append(a.GetString(index)); break;
            case Decimal256Array a: sb.Append(a.GetString(index)); break;
            case StringArray a: AppendString(sb, a.GetString(index)); break;
            case StringViewArray a: AppendString(sb, a.GetString(index)); break;
            case LargeStringArray a: AppendString(sb, a.GetString(index)); break;
            case BinaryArray a: AppendBytes(sb, a.GetBytes(index)); break;
            case BinaryViewArray a: AppendBytes(sb, a.GetBytes(index)); break;
            case LargeBinaryArray a: AppendBytes(sb, a.GetBytes(index)); break;
            case FixedSizeBinaryArray a: AppendBytes(sb, a.GetBytes(index)); break;
            // Temporal values render raw; their unit and zone are part of the type.
            case TimestampArray a: sb.Append(a.GetValue(index)); break;
            case Date32Array a: sb.Append(a.GetValue(index)); break;
            case Date64Array a: sb.Append(a.GetValue(index)); break;
            case Time32Array a: sb.Append(a.GetValue(index)); break;
            case Time64Array a: sb.Append(a.GetValue(index)); break;
            case MapArray a:
                AppendEntries(sb, a.KeyValues, a.ValueOffsets[index], a.ValueOffsets[index + 1]);
                break;
            case ListArray a: AppendRange(sb, a.Values, a.ValueOffsets[index], a.ValueOffsets[index + 1]); break;
            case LargeListArray a:
                AppendRange(sb, a.Values, checked((int)a.ValueOffsets[index]), checked((int)a.ValueOffsets[index + 1]));
                break;
            case ListViewArray a:
                AppendRange(sb, a.Values, a.ValueOffsets[index], a.ValueOffsets[index] + a.Sizes[index]);
                break;
            case LargeListViewArray a:
                AppendRange(sb, a.Values, checked((int)a.ValueOffsets[index]), checked((int)(a.ValueOffsets[index] + a.Sizes[index])));
                break;
            case FixedSizeListArray a:
            {
                int size = ((FixedSizeListType)a.Data.DataType).ListSize;
                int start = (a.Offset + index) * size;
                AppendRange(sb, a.Values, start, start + size);
                break;
            }
            case StructArray a:
            {
                var fields = ((StructType)a.Data.DataType).Fields;
                sb.Append('{');
                for (int f = 0; f < fields.Count; f++)
                {
                    if (f > 0) sb.Append(", ");
                    sb.Append(fields[f].Name).Append(": ");
                    Append(sb, a.Fields[f], index);
                }
                sb.Append('}');
                break;
            }
            case NullArray: sb.Append("null"); break;
            default:
                throw new NotSupportedException($"ArrowValues cannot render {array.GetType().Name} ({array.Data.DataType.Name}).");
        }
    }

    // A list's offsets are relative to its (unsliced) values child, as Apache.Arrow exposes them.
    private static void AppendRange(StringBuilder sb, IArrowArray values, int start, int end)
    {
        sb.Append('[');
        for (int i = start; i < end; i++)
        {
            if (i > start) sb.Append(", ");
            Append(sb, values, i);
        }
        sb.Append(']');
    }

    private static void AppendEntries(StringBuilder sb, StructArray entries, int start, int end)
    {
        sb.Append('{');
        for (int i = start; i < end; i++)
        {
            if (i > start) sb.Append(", ");
            Append(sb, entries.Fields[0], i);
            sb.Append(": ");
            Append(sb, entries.Fields[1], i);
        }
        sb.Append('}');
    }

    private static int DictionaryKey(IArrowArray indices, int index) => indices switch
    {
        Int8Array a => a.GetValue(index)!.Value,
        Int16Array a => a.GetValue(index)!.Value,
        Int32Array a => a.GetValue(index)!.Value,
        Int64Array a => checked((int)a.GetValue(index)!.Value),
        UInt8Array a => a.GetValue(index)!.Value,
        UInt16Array a => a.GetValue(index)!.Value,
        UInt32Array a => checked((int)a.GetValue(index)!.Value),
        UInt64Array a => checked((int)a.GetValue(index)!.Value),
        _ => throw new NotSupportedException($"Dictionary index type {indices.Data.DataType.Name}."),
    };

    private static void AppendFloat(StringBuilder sb, double value)
    {
        if (double.IsNaN(value)) { sb.Append("NaN"); return; }
        // .NET Framework formats -0.0 as "0", so the sign is written out explicitly.
        if (value == 0) { sb.Append(BitConverter.DoubleToInt64Bits(value) < 0 ? "-0" : "0"); return; }
        sb.Append(value.ToString("R", CultureInfo.InvariantCulture));
    }

    private static void AppendString(StringBuilder sb, string? s) =>
        sb.Append('"').Append(s!.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');

    private static void AppendBytes(StringBuilder sb, ReadOnlySpan<byte> bytes)
    {
        sb.Append("0x");
        foreach (var b in bytes) sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
    }
}
