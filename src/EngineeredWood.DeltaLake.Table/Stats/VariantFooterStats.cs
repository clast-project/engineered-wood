// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Data.SqlTypes;
using System.Globalization;
using System.Numerics;
using System.Text;
using Apache.Arrow.Scalars.Variant;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using ParquetLogicalType = EngineeredWood.Parquet.Metadata.LogicalType;

namespace EngineeredWood.DeltaLake.Table.Stats;

/// <summary>
/// A shredded file's variant <c>minValues</c> / <c>maxValues</c>, derived from its Parquet footer as delta-spark
/// 4.3 derives them (<c>VariantStatsData</c>, under <c>variantShredding.parseFooterForStats</c>).
/// </summary>
/// <remarks>
/// <para>The protocol ("Statistics for Variant Columns") bounds only the leaf paths whose type is the same in
/// every row of the file. A shredded path has exactly that property when its residual <c>value</c> column is
/// null in every row: then each row's value at the path is either missing or in <c>typed_value</c>, whose
/// column statistics are the bounds. Paths are decided one at a time, so a mixed sibling does not cost a
/// uniform one its bound.</para>
///
/// <para>What follows delta-spark, measured from its jar: the Parquet types that map to a bound (signed
/// integers at their own width, DATE, DECIMAL at the width its precision implies, TIMESTAMP in microseconds,
/// FLOAT, DOUBLE, STRING); a path's statistics merged over every row group, and dropped when any row group
/// fails the residual test; the path spelling with its escapes; and strings cut to the table's prefix length,
/// a cut maximum followed by U+10FFFF. Paths below an array are not bounded: their rows are elements, not
/// file rows. A row group without statistics for a path that holds values drops the path.</para>
/// </remarks>
internal static class VariantFooterStats
{
    /// <summary>delta-spark's suffix for a truncated string maximum: the largest code point.</summary>
    private const string MaxStringSuffix = "\U0010FFFF";

    /// <summary>
    /// The Z85 bounds of each of <paramref name="variantColumns"/> that has at least one bounded path in
    /// <paramref name="footer"/>; a column with none is absent.
    /// </summary>
    public static Dictionary<string, (string Min, string Max)> Compute(
        FileMetaData footer, IReadOnlyCollection<string> variantColumns, int stringPrefixLength)
    {
        var result = new Dictionary<string, (string, string)>(StringComparer.Ordinal);
        if (variantColumns.Count == 0 || footer.RowGroups.Count == 0)
            return result;

        var schema = new SchemaDescriptor(footer.Schema);
        var columnIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int c = 0; c < schema.Columns.Count; c++)
            columnIndex[Key(schema.Columns[c].Path)] = c;

        // Each bounded column's paths, in schema order; a path is dropped (null) once any row group rules it out.
        var paths = new Dictionary<string, List<PathBound?>>(StringComparer.Ordinal);
        for (int c = 0; c < schema.Columns.Count; c++)
        {
            var column = schema.Columns[c];
            if (!variantColumns.Contains(column.Path[0]) || !TryFieldPath(column.Path, out var fieldPath))
                continue;
            if (column.MaxRepetitionLevel > 0 || Kind(column) is not BoundKind kind)
                continue;

            var siblingPath = new List<string>(column.Path) { [column.Path.Count - 1] = "value" };
            int? sibling = columnIndex.TryGetValue(Key(siblingPath), out int s) ? s : null;
            if (!paths.TryGetValue(column.Path[0], out var list))
                paths[column.Path[0]] = list = [];
            list.Add(new PathBound(fieldPath, c, sibling, kind, column.SchemaElement));
        }

        foreach (var (name, list) in paths)
        {
            for (int i = 0; i < list.Count; i++)
            {
                foreach (var rowGroup in footer.RowGroups)
                {
                    if (list[i] is { } path && !path.Merge(rowGroup))
                        list[i] = null;
                }
            }

            var min = new Dictionary<string, VariantValue>(StringComparer.Ordinal);
            var max = new Dictionary<string, VariantValue>(StringComparer.Ordinal);
            foreach (var path in list)
            {
                if (path?.ToVariants(stringPrefixLength) is not var (lo, hi))
                    continue;
                string key = NormalizedPath(path.FieldPath);
                min[key] = lo;
                max[key] = hi;
            }
            if (min.Count > 0)
                result[name] = (Encode(min), Encode(max));
        }
        return result;
    }

    private static string Key(IReadOnlyList<string> path) => string.Join("\u0000", path);

    /// <summary>
    /// The object field names a <c>typed_value</c> leaf is the shredding of: <c>v.typed_value.a.typed_value</c>
    /// is <c>["a"]</c>, and <c>v.typed_value</c> itself (a shredded scalar) is the root, <c>[]</c>. False for any
    /// other leaf, and for a path through an array (whose groups are not named <c>typed_value</c> throughout).
    /// </summary>
    private static bool TryFieldPath(IReadOnlyList<string> path, out List<string> fields)
    {
        fields = [];
        if (path.Count < 2 || path.Count % 2 != 0)
            return false;
        for (int i = 1; i < path.Count; i += 2)
        {
            if (path[i] != "typed_value")
                return false;
            if (i + 1 < path.Count)
                fields.Add(path[i + 1]);
        }
        return true;
    }

    /// <summary>delta-spark's <c>normalizedJsonPath</c>: <c>$['a']['b']</c>, with <c>'</c>, <c>\</c> and control
    /// characters escaped.</summary>
    internal static string NormalizedPath(IReadOnlyList<string> fields)
    {
        var sb = new StringBuilder("$");
        foreach (string field in fields)
        {
            sb.Append("['");
            foreach (char ch in field)
            {
                switch (ch)
                {
                    case '\\': sb.Append("\\\\"); break;
                    case '\'': sb.Append("\\'"); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (ch < ' ')
                            sb.Append("\\u").Append(((int)ch).ToString("x4", CultureInfo.InvariantCulture));
                        else
                            sb.Append(ch);
                        break;
                }
            }
            sb.Append("']");
        }
        return sb.ToString();
    }

    private static string Encode(Dictionary<string, VariantValue> fields)
    {
        var (metadata, value) = new VariantBuilder().Encode(VariantValue.FromObject(fields));
        return VariantStatsEncoding.Encode(metadata, value);
    }

    private enum BoundKind { Int8, Int16, Int32, Int64, Date, Decimal, Timestamp, TimestampNtz, Float, Double, String }

    /// <summary>The bound a leaf of this Parquet type gives, or null for one delta-spark gives none (booleans,
    /// binary, unsigned integers, UUIDs, nanosecond timestamps, times).</summary>
    private static BoundKind? Kind(ColumnDescriptor column)
    {
        var logical = column.SchemaElement.LogicalType;
        var converted = column.SchemaElement.ConvertedType;
        if (logical is ParquetLogicalType.DecimalType || (logical is null && converted == ConvertedType.Decimal))
        {
            return column.PhysicalType is PhysicalType.Int32 or PhysicalType.Int64 or PhysicalType.FixedLenByteArray
                ? BoundKind.Decimal : null;
        }

        switch (column.PhysicalType)
        {
            case PhysicalType.Int32:
                return logical switch
                {
                    null => converted switch
                    {
                        null or ConvertedType.Int32 => BoundKind.Int32,
                        ConvertedType.Int8 => BoundKind.Int8,
                        ConvertedType.Int16 => BoundKind.Int16,
                        ConvertedType.Date => BoundKind.Date,
                        _ => null,
                    },
                    ParquetLogicalType.IntType { IsSigned: true, BitWidth: 8 } => BoundKind.Int8,
                    ParquetLogicalType.IntType { IsSigned: true, BitWidth: 16 } => BoundKind.Int16,
                    ParquetLogicalType.IntType { IsSigned: true, BitWidth: 32 } => BoundKind.Int32,
                    ParquetLogicalType.DateType => BoundKind.Date,
                    _ => null,
                };
            case PhysicalType.Int64:
                return logical switch
                {
                    null => converted switch
                    {
                        null or ConvertedType.Int64 => BoundKind.Int64,
                        ConvertedType.TimestampMicros => BoundKind.Timestamp,
                        _ => null,
                    },
                    ParquetLogicalType.IntType { IsSigned: true, BitWidth: 64 } => BoundKind.Int64,
                    ParquetLogicalType.TimestampType { Unit: TimeUnit.Micros } ts =>
                        ts.IsAdjustedToUtc ? BoundKind.Timestamp : BoundKind.TimestampNtz,
                    _ => null,
                };
            case PhysicalType.Float:
                return logical is null ? BoundKind.Float : null;
            case PhysicalType.Double:
                return logical is null ? BoundKind.Double : null;
            case PhysicalType.ByteArray:
                return logical is ParquetLogicalType.StringType || (logical is null && converted == ConvertedType.Utf8)
                    ? BoundKind.String : null;
            default:
                return null;
        }
    }

    /// <summary>One path's bounds, merged over the row groups seen so far.</summary>
    private sealed class PathBound(
        List<string> fieldPath, int column, int? sibling, BoundKind kind, SchemaElement element)
    {
        private byte[]? _min;
        private byte[]? _max;

        public List<string> FieldPath { get; } = fieldPath;

        /// <summary>Folds in one row group; false when it rules the path out.</summary>
        public bool Merge(RowGroup rowGroup)
        {
            // The residual must be null in every row: a row whose value at this path has another type lands there.
            if (sibling is int s && rowGroup.Columns[s].MetaData?.Statistics?.NullCount != rowGroup.NumRows)
                return false;

            var stats = rowGroup.Columns[column].MetaData?.Statistics;
            if (stats?.NullCount == rowGroup.NumRows)
                return true; // nothing here at this path: no bound to add
            if (!TryBounds(stats, out byte[] min, out byte[] max))
                return false; // values without statistics cannot be bounded

            if (_min is null || Compare(min, _min) < 0)
                _min = min;
            if (_max is null || Compare(max, _max) > 0)
                _max = max;
            return true;
        }

        private bool TryBounds(Statistics? stats, out byte[] min, out byte[] max)
        {
            min = stats?.MinValue!;
            max = stats?.MaxValue!;
            // The deprecated min/max were written under signed ordering, which is the right one only for numbers.
            if ((min is null || max is null) && kind is not (BoundKind.String or BoundKind.Decimal))
            {
                min = stats?.Min!;
                max = stats?.Max!;
            }
            if (min is null || max is null || !WellFormed(min) || !WellFormed(max))
                return false;
            // A NaN is left out of the float min/max and counted apart, but it sorts above every number in Delta's
            // ordering: a max that ignores it is not an upper bound. So a float path is bounded only when the footer
            // says it holds NO NaN; a writer that did not record the count (null) may have skipped one (#676 review).
            // EW's own writer always records it.
            if (kind is BoundKind.Float or BoundKind.Double && stats!.NanCount != 0)
                return false;
            return !(kind is BoundKind.Float && (float.IsNaN(ReadFloat(min)) || float.IsNaN(ReadFloat(max))))
                && !(kind is BoundKind.Double && (double.IsNaN(ReadDouble(min)) || double.IsNaN(ReadDouble(max))));
        }

        private bool WellFormed(byte[] bytes) => kind switch
        {
            BoundKind.Int8 or BoundKind.Int16 or BoundKind.Int32 or BoundKind.Date or BoundKind.Float => bytes.Length == 4,
            BoundKind.Int64 or BoundKind.Timestamp or BoundKind.TimestampNtz or BoundKind.Double => bytes.Length == 8,
            BoundKind.Decimal => element.Type switch
            {
                PhysicalType.Int32 => bytes.Length == 4,
                PhysicalType.Int64 => bytes.Length == 8,
                _ => bytes.Length > 0 && bytes.Length <= 16,
            },
            _ => true,
        };

        private int Compare(byte[] a, byte[] b) => kind switch
        {
            BoundKind.Int8 or BoundKind.Int16 or BoundKind.Int32 or BoundKind.Date =>
                ReadInt32(a).CompareTo(ReadInt32(b)),
            BoundKind.Int64 or BoundKind.Timestamp or BoundKind.TimestampNtz => ReadInt64(a).CompareTo(ReadInt64(b)),
            BoundKind.Float => ReadFloat(a).CompareTo(ReadFloat(b)),
            BoundKind.Double => ReadDouble(a).CompareTo(ReadDouble(b)),
            BoundKind.Decimal => Unscaled(a).CompareTo(Unscaled(b)),
            // UTF-8 byte order is code point order, the order Parquet's string statistics use.
            _ => a.AsSpan().SequenceCompareTo(b),
        };

        /// <summary>The path's bounds as Variant values, or null when no row group held a value at it.</summary>
        public (VariantValue Min, VariantValue Max)? ToVariants(int stringPrefixLength)
        {
            if (_min is null || _max is null)
                return null;
            if (kind is BoundKind.String)
            {
                // A footer bound cut at a byte limit can end inside a character, and a replacement character in its
                // place sorts above what was cut: only a bound that decodes cleanly is kept (#676 review).
                if (!TryDecodeUtf8(_min, out string lo) || !TryDecodeUtf8(_max, out string hi))
                    return null;
                if (lo.Length > stringPrefixLength)
                    lo = Prefix(lo, stringPrefixLength);
                if (hi.Length > stringPrefixLength)
                {
                    hi = Prefix(hi, stringPrefixLength) + MaxStringSuffix;
                    // The suffix does not bound a maximum that continues past the cut with that same code point:
                    // "abcd" + U+10FFFF is below "abcd" + U+10FFFF + "x". No bound is safer than a low one.
                    if (System.Text.Encoding.UTF8.GetBytes(hi).AsSpan().SequenceCompareTo(_max) < 0)
                        return null;
                }
                return (VariantValue.FromString(lo), VariantValue.FromString(hi));
            }
            return (ToVariant(_min), ToVariant(_max));
        }

        // A cut never splits a surrogate pair: half a character is not a string the bound can hold. A prefix length of
        // 0 is a valid table setting and gives the empty string.
        private static string Prefix(string s, int length) =>
            length > 0 && char.IsHighSurrogate(s[length - 1]) ? s.Substring(0, length - 1) : s.Substring(0, length);

        private static readonly System.Text.UTF8Encoding StrictUtf8 =
            new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        private static bool TryDecodeUtf8(byte[] bytes, out string text)
        {
            try
            {
                text = StrictUtf8.GetString(bytes);
                return true;
            }
            catch (System.Text.DecoderFallbackException)
            {
                text = "";
                return false;
            }
        }

        private VariantValue ToVariant(byte[] bytes) => kind switch
        {
            BoundKind.Int8 => VariantValue.FromInt8((sbyte)ReadInt32(bytes)),
            BoundKind.Int16 => VariantValue.FromInt16((short)ReadInt32(bytes)),
            BoundKind.Int32 => VariantValue.FromInt32(ReadInt32(bytes)),
            BoundKind.Int64 => VariantValue.FromInt64(ReadInt64(bytes)),
            BoundKind.Date => VariantValue.FromDate(ReadInt32(bytes)),
            BoundKind.Timestamp => VariantValue.FromTimestamp(ReadInt64(bytes)),
            BoundKind.TimestampNtz => VariantValue.FromTimestampNtz(ReadInt64(bytes)),
            BoundKind.Float => VariantValue.FromFloat(ReadFloat(bytes)),
            BoundKind.Double => VariantValue.FromDouble(ReadDouble(bytes)),
            _ => ToDecimal(Unscaled(bytes)),
        };

        // delta-spark's appendDecimalFixedPrecision: the width follows the column's PRECISION, not the value's.
        private VariantValue ToDecimal(BigInteger unscaled)
        {
            var (scale, precision) = element.LogicalType is ParquetLogicalType.DecimalType d
                ? (d.Scale, d.Precision)
                : (element.Scale ?? 0, element.Precision ?? 38);
            if (precision <= 18)
            {
                long u = (long)unscaled;
                ulong magnitude = u < 0 ? (ulong)(-(u + 1)) + 1 : (ulong)u;
                var value = new decimal((int)(uint)magnitude, (int)(uint)(magnitude >> 32), 0, u < 0, (byte)scale);
                return precision <= 9 ? VariantValue.FromDecimal4(value) : VariantValue.FromDecimal8(value);
            }
            return VariantValue.FromDecimal16(SqlDecimal.Parse(DecimalString(unscaled, scale)));
        }

        private static string DecimalString(BigInteger unscaled, int scale)
        {
            string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(scale + 1, '0');
            string text = scale == 0 ? digits : digits.Substring(0, digits.Length - scale) + "." + digits.Substring(digits.Length - scale);
            return unscaled.Sign < 0 ? "-" + text : text;
        }

        private BigInteger Unscaled(byte[] bytes) => element.Type switch
        {
            PhysicalType.Int32 => ReadInt32(bytes),
            PhysicalType.Int64 => ReadInt64(bytes),
            _ => BigEndianSigned(bytes),
        };

        private static BigInteger BigEndianSigned(byte[] bytes)
        {
            var little = new byte[bytes.Length];
            for (int i = 0; i < bytes.Length; i++)
                little[i] = bytes[bytes.Length - 1 - i];
            return new BigInteger(little);
        }

        private static int ReadInt32(byte[] b) => BinaryPrimitives.ReadInt32LittleEndian(b);
        private static long ReadInt64(byte[] b) => BinaryPrimitives.ReadInt64LittleEndian(b);
        private static float ReadFloat(byte[] b) => BitConverter.ToSingle(b, 0);
        private static double ReadDouble(byte[] b) => BitConverter.Int64BitsToDouble(ReadInt64(b));
    }
}
