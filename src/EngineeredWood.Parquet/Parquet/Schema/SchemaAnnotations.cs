// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Schema;

/// <summary>
/// Decides which of a schema element's type annotations the reader honours.
/// </summary>
/// <remarks>
/// <para>parquet-format 2.14 (apache/parquet-format#606): a reader that meets an unrecognized logical
/// type, or one it recognizes on a physical type the spec does not allow (UUID on INT32, STRING on
/// INT64, LIST on a leaf), ignores the annotation and the column's order and reads the column as its
/// physical type. Everything downstream — the Arrow mapping, statistics decoding, predicate pushdown —
/// interprets the column through its annotation, and each place used to trust it, so an invalid one
/// built Arrow arrays over the wrong byte width. Checking once, when <see cref="SchemaDescriptor"/> is
/// built, means none of them can.</para>
///
/// <para>When the <c>logical_type</c> is dropped the legacy <c>converted_type</c> goes with it, as in
/// arrow C++: a file that sets <c>logical_type</c> is read by it alone. A <c>converted_type</c> on its
/// own is checked by the same rules.</para>
///
/// <para><c>FileMetaData.Schema</c> keeps every annotation as written; only the descriptor's copy is
/// cleaned.</para>
/// </remarks>
internal static class SchemaAnnotations
{
    /// <summary>
    /// Returns <paramref name="element"/> with any annotation the reader must ignore removed —
    /// <paramref name="element"/> itself when there is none. <paramref name="ignored"/> is true when
    /// the annotation that defined the column's order was dropped, so its min/max statistics must not
    /// be used either.
    /// </summary>
    public static SchemaElement Honoured(SchemaElement element, out bool ignored)
    {
        ignored = false;
        bool keepLogical = element.LogicalType is null || IsValid(element.LogicalType, element);
        bool keepConverted = keepLogical && (element.ConvertedType is null || IsValid(element.ConvertedType.Value, element));

        if (keepLogical && keepConverted)
            return element;

        // The order came from the logical type when there was one, and from the converted type otherwise.
        ignored = !keepLogical || element.LogicalType is null;
        return new SchemaElement
        {
            Name = element.Name,
            Type = element.Type,
            TypeLength = element.TypeLength,
            RepetitionType = element.RepetitionType,
            NumChildren = element.NumChildren,
            ConvertedType = keepConverted ? element.ConvertedType : null,
            Scale = element.Scale,
            Precision = element.Precision,
            FieldId = element.FieldId,
            LogicalType = keepLogical ? element.LogicalType : null,
        };
    }

    /// <summary>Whether <paramref name="logical"/> may annotate <paramref name="element"/> (LogicalTypes.md).</summary>
    internal static bool IsValid(LogicalType logical, SchemaElement element)
    {
        if (element.Type is not { } physical)
            return logical is LogicalType.ListType or LogicalType.MapType or LogicalType.VariantType;

        return logical switch
        {
            LogicalType.StringType or LogicalType.EnumType or LogicalType.JsonType or LogicalType.BsonType
                => physical == PhysicalType.ByteArray,
            LogicalType.UuidType => IsFixed(element, 16),
            LogicalType.Float16Type => IsFixed(element, 2),
            LogicalType.DateType => physical == PhysicalType.Int32,
            LogicalType.IntType { BitWidth: 8 or 16 or 32 } => physical == PhysicalType.Int32,
            LogicalType.IntType { BitWidth: 64 } => physical == PhysicalType.Int64,
            LogicalType.DecimalType d => DecimalFits(d.Precision, d.Scale, element),
            LogicalType.TimeType { Unit: TimeUnit.Millis } => physical == PhysicalType.Int32,
            LogicalType.TimeType => physical == PhysicalType.Int64,
            // FIXED_LEN_BYTE_ARRAY(12) is the extended-precision carrier proposed in
            // apache/parquet-format#600, which this library reads and (opt-in) writes.
            LogicalType.TimestampType => physical == PhysicalType.Int64 || IsFixed(element, ExtendedTimestamp.ByteWidth),
            LogicalType.NullType => true,
            // Group-only types on a leaf, an IntType of another width, and anything unrecognized.
            _ => false,
        };
    }

    /// <summary>Whether <paramref name="converted"/> may annotate <paramref name="element"/>.</summary>
    internal static bool IsValid(ConvertedType converted, SchemaElement element)
    {
        if (element.Type is not { } physical)
            return converted is ConvertedType.List or ConvertedType.Map or ConvertedType.MapKeyValue;

        return converted switch
        {
            ConvertedType.Utf8 or ConvertedType.Enum or ConvertedType.Json or ConvertedType.Bson
                => physical == PhysicalType.ByteArray,
            ConvertedType.Date or ConvertedType.TimeMillis
                or ConvertedType.Int8 or ConvertedType.Int16 or ConvertedType.Int32
                or ConvertedType.Uint8 or ConvertedType.Uint16 or ConvertedType.Uint32
                => physical == PhysicalType.Int32,
            ConvertedType.TimeMicros or ConvertedType.TimestampMillis or ConvertedType.TimestampMicros
                or ConvertedType.Int64 or ConvertedType.Uint64
                => physical == PhysicalType.Int64,
            ConvertedType.Decimal => element.Precision is { } p && DecimalFits(p, element.Scale ?? 0, element),
            ConvertedType.Interval => IsFixed(element, 12),
            _ => false,
        };
    }

    private static bool IsFixed(SchemaElement element, int length) =>
        element.Type == PhysicalType.FixedLenByteArray && element.TypeLength == length;

    /// <summary>
    /// DECIMAL's constraints: <c>1 &lt;= precision</c>, <c>0 &lt;= scale &lt;= precision</c>, and a
    /// precision the storage can hold — 9 digits in INT32, 18 in INT64, and in FIXED_LEN_BYTE_ARRAY(n)
    /// the digits of the largest signed n-byte integer.
    /// </summary>
    private static bool DecimalFits(int precision, int scale, SchemaElement element)
    {
        if (precision < 1 || scale < 0 || scale > precision)
            return false;

        return element.Type switch
        {
            PhysicalType.Int32 => precision <= 9,
            PhysicalType.Int64 => precision <= 18,
            PhysicalType.FixedLenByteArray => element.TypeLength is > 0 and var n && precision <= MaxDigits(n),
            PhysicalType.ByteArray => true,
            _ => false,
        };
    }

    /// <summary>
    /// floor(log10(2^(8n-1) - 1)): the decimal digits every value of a signed n-byte integer can hold.
    /// (8n-1)·log10(2) is irrational, so it is never within rounding error of an integer.
    /// </summary>
    private static int MaxDigits(int bytes) => (int)Math.Floor((8.0 * bytes - 1) * Math.Log10(2));
}
