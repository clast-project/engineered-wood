// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Converts an Arrow <see cref="Schema"/> to a flat list of Parquet <see cref="SchemaElement"/>s.
/// Reverse of <see cref="ArrowSchemaConverter"/>.
/// </summary>
internal static class ArrowToSchemaConverter
{
    /// <summary>
    /// Converts an Arrow schema to a flat list of Parquet schema elements
    /// (pre-order traversal with root "schema" element).
    /// </summary>
    public static IReadOnlyList<SchemaElement> Convert(Apache.Arrow.Schema arrowSchema)
    {
        var elements = new List<SchemaElement>();

        // Root schema element
        elements.Add(new SchemaElement
        {
            Name = "schema",
            NumChildren = arrowSchema.FieldsList.Count,
        });

        foreach (var field in arrowSchema.FieldsList)
            AddField(elements, field);

        return elements;
    }

    private static void AddField(List<SchemaElement> elements, Field field)
    {
        var repetition = field.IsNullable
            ? FieldRepetitionType.Optional
            : FieldRepetitionType.Required;

        int? fieldId = GetFieldId(field);

        switch (field.DataType)
        {
            case StructType structType:
                AddStructField(elements, field.Name, repetition, structType, fieldId);
                return;

            case ListType listType:
                AddListField(elements, field.Name, repetition, listType, fieldId);
                return;

            case MapType mapType:
                AddMapField(elements, field.Name, repetition, mapType, fieldId);
                return;

            // ExtensionType over a nested storage type: emit the storage as a
            // group, with the matching Parquet logical-type annotation. The
            // VARIANT extension takes this path; future extensions over List/
            // Map (none defined yet) would as well.
            case ExtensionType ext when ext.StorageType is StructType extStruct:
                AddStructField(elements, field.Name, repetition, extStruct, fieldId,
                    logicalType: GetExtensionLogicalType(ext));
                return;
        }

        var (physicalType, typeLength, logicalType, convertedType, scale, precision) =
            MapArrowType(field.DataType);

        elements.Add(new SchemaElement
        {
            Name = field.Name,
            Type = physicalType,
            TypeLength = typeLength,
            RepetitionType = repetition,
            LogicalType = logicalType,
            ConvertedType = convertedType,
            Scale = scale,
            Precision = precision,
            FieldId = fieldId,
        });
    }

    /// <summary>
    /// Extracts Parquet field_id from Arrow Field metadata.
    /// Looks for <c>PARQUET:field_id</c> or <c>field_id</c> keys.
    /// </summary>
    private static int? GetFieldId(Field field)
    {
        if (field.Metadata is null)
            return null;

        if (field.Metadata.TryGetValue("PARQUET:field_id", out string? id1) &&
            int.TryParse(id1, out int fid1))
            return fid1;

        if (field.Metadata.TryGetValue("field_id", out string? id2) &&
            int.TryParse(id2, out int fid2))
            return fid2;

        return null;
    }

    private static void AddStructField(
        List<SchemaElement> elements, string name,
        FieldRepetitionType repetition, StructType structType, int? fieldId = null,
        LogicalType? logicalType = null)
    {
        elements.Add(new SchemaElement
        {
            Name = name,
            RepetitionType = repetition,
            NumChildren = structType.Fields.Count,
            FieldId = fieldId,
            LogicalType = logicalType,
        });

        foreach (var child in structType.Fields)
            AddField(elements, child);
    }

    /// <summary>
    /// Maps an Arrow <see cref="ExtensionType"/> name to the Parquet logical
    /// type annotation that should ride with the storage layout. Returns null
    /// for any extension we don't have a specialised mapping for; callers
    /// then write the bare storage with no annotation.
    /// </summary>
    private static LogicalType? GetExtensionLogicalType(ExtensionType ext) => ext.Name switch
    {
        "arrow.parquet.variant" => new LogicalType.VariantType(),
        _ => null,
    };

    private static void AddListField(
        List<SchemaElement> elements, string name,
        FieldRepetitionType repetition, ListType listType, int? fieldId = null)
    {
        // 3-level encoding: optional/required group (LIST) → repeated group "list" → element
        elements.Add(new SchemaElement
        {
            Name = name,
            RepetitionType = repetition,
            NumChildren = 1,
            LogicalType = new LogicalType.ListType(),
            ConvertedType = ConvertedType.List,
            FieldId = fieldId,
        });

        // Repeated group "list" with one child
        elements.Add(new SchemaElement
        {
            Name = "list",
            RepetitionType = FieldRepetitionType.Repeated,
            NumChildren = 1,
        });

        // Element field
        var elementField = listType.ValueField;
        AddField(elements, elementField);
    }

    private static void AddMapField(
        List<SchemaElement> elements, string name,
        FieldRepetitionType repetition, MapType mapType, int? fieldId = null)
    {
        // optional/required group (MAP) → repeated group "key_value" → key + value
        elements.Add(new SchemaElement
        {
            Name = name,
            RepetitionType = repetition,
            NumChildren = 1,
            LogicalType = new LogicalType.MapType(),
            ConvertedType = ConvertedType.Map,
            FieldId = fieldId,
        });

        // Repeated group "key_value" with 2 children
        elements.Add(new SchemaElement
        {
            Name = "key_value",
            RepetitionType = FieldRepetitionType.Repeated,
            NumChildren = 2,
        });

        // Key field (always required)
        var keyField = new Field(mapType.KeyField.Name, mapType.KeyField.DataType, nullable: false);
        AddField(elements, keyField);

        // Value field
        AddField(elements, mapType.ValueField);
    }

    /// <summary>
    /// Maps an Arrow type to Parquet physical type and annotations.
    /// </summary>
    internal static (
        PhysicalType PhysicalType,
        int? TypeLength,
        LogicalType? LogicalType,
        ConvertedType? ConvertedType,
        int? Scale,
        int? Precision)
        MapArrowType(IArrowType arrowType)
    {
        return arrowType switch
        {
            BooleanType => (PhysicalType.Boolean, null, null, null, null, null),

            Int8Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(8, true), ConvertedType.Int8, null, null),
            Int16Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(16, true), ConvertedType.Int16, null, null),
            Int32Type => (PhysicalType.Int32, null, null, null, null, null),
            Int64Type => (PhysicalType.Int64, null, null, null, null, null),

            UInt8Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(8, false), ConvertedType.Uint8, null, null),
            UInt16Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(16, false), ConvertedType.Uint16, null, null),
            UInt32Type => (PhysicalType.Int32, null,
                new LogicalType.IntType(32, false), ConvertedType.Uint32, null, null),
            UInt64Type => (PhysicalType.Int64, null,
                new LogicalType.IntType(64, false), ConvertedType.Uint64, null, null),

            FloatType => (PhysicalType.Float, null, null, null, null, null),
            DoubleType => (PhysicalType.Double, null, null, null, null, null),

            Apache.Arrow.Types.StringType => (PhysicalType.ByteArray, null,
                new LogicalType.StringType(), ConvertedType.Utf8, null, null),
            LargeStringType => (PhysicalType.ByteArray, null,
                new LogicalType.StringType(), ConvertedType.Utf8, null, null),
            StringViewType => (PhysicalType.ByteArray, null,
                new LogicalType.StringType(), ConvertedType.Utf8, null, null),
            BinaryType => (PhysicalType.ByteArray, null, null, null, null, null),
            LargeBinaryType => (PhysicalType.ByteArray, null, null, null, null, null),
            BinaryViewType => (PhysicalType.ByteArray, null, null, null, null, null),

            Date32Type => (PhysicalType.Int32, null,
                new LogicalType.DateType(), ConvertedType.Date, null, null),

            TimestampType ts => (PhysicalType.Int64, null,
                new LogicalType.TimestampType(
                    ts.Timezone != null,
                    MapTimeUnit(ts.Unit)),
                // The deprecated converted_type has NO nanosecond variant — labeling ns as micros makes
                // converted_type-trusting readers misinterpret the values 1000x. Omit it for ns (per spec:
                // only the inline LogicalType describes nanoseconds).
                ts.Unit switch
                {
                    Apache.Arrow.Types.TimeUnit.Millisecond => ConvertedType.TimestampMillis,
                    Apache.Arrow.Types.TimeUnit.Nanosecond => (ConvertedType?)null,
                    _ => ConvertedType.TimestampMicros,
                },
                null, null),

            Time32Type t32 => (PhysicalType.Int32, null,
                new LogicalType.TimeType(false, MapTimeUnit(t32.Unit)),
                ConvertedType.TimeMillis,
                null, null),

            Time64Type t64 => (PhysicalType.Int64, null,
                new LogicalType.TimeType(false, MapTimeUnit(t64.Unit)),
                // No converted_type nanosecond variant exists — omit for ns (see the timestamp note above).
                t64.Unit == Apache.Arrow.Types.TimeUnit.Nanosecond ? (ConvertedType?)null : ConvertedType.TimeMicros,
                null, null),

            HalfFloatType => (PhysicalType.FixedLenByteArray, 2,
                new LogicalType.Float16Type(), null, null, null),

            // Decimal types: order matters — more specific before FixedSizeBinaryType
            Decimal32Type d32 => (PhysicalType.Int32, null,
                new LogicalType.DecimalType(d32.Scale, d32.Precision),
                ConvertedType.Decimal, d32.Scale, d32.Precision),
            Decimal64Type d64 => (PhysicalType.Int64, null,
                new LogicalType.DecimalType(d64.Scale, d64.Precision),
                ConvertedType.Decimal, d64.Scale, d64.Precision),
            Decimal128Type d128 => (PhysicalType.FixedLenByteArray, 16,
                new LogicalType.DecimalType(d128.Scale, d128.Precision),
                ConvertedType.Decimal, d128.Scale, d128.Precision),
            Decimal256Type d256 => (PhysicalType.FixedLenByteArray, 32,
                new LogicalType.DecimalType(d256.Scale, d256.Precision),
                ConvertedType.Decimal, d256.Scale, d256.Precision),

            FixedSizeBinaryType fsb => (PhysicalType.FixedLenByteArray, fsb.ByteWidth,
                null, null, null, null),

            // ExtensionType: encode using the storage type's physical layout,
            // and emit the matching logical-type annotation when we recognise
            // the extension by name. arrow.uuid -> UuidType + FLBA(16). Any
            // other extension over a supported storage type round-trips as the
            // raw storage (no annotation), which still preserves the data.
            ExtensionType ext => MapExtensionType(ext),

            _ => throw new NotSupportedException(
                $"Arrow type '{arrowType.Name}' is not supported for Parquet writing."),
        };
    }

    private static (
        PhysicalType PhysicalType,
        int? TypeLength,
        LogicalType? LogicalType,
        ConvertedType? ConvertedType,
        int? Scale,
        int? Precision)
        MapExtensionType(ExtensionType ext)
    {
        var (phys, len, _, conv, scale, prec) = MapArrowType(ext.StorageType);
        LogicalType? logical = ext.Name switch
        {
            "arrow.uuid" => new LogicalType.UuidType(),
            _ => null,
        };
        return (phys, len, logical, conv, scale, prec);
    }

    private static Metadata.TimeUnit MapTimeUnit(Apache.Arrow.Types.TimeUnit unit) => unit switch
    {
        Apache.Arrow.Types.TimeUnit.Millisecond => Metadata.TimeUnit.Millis,
        Apache.Arrow.Types.TimeUnit.Microsecond => Metadata.TimeUnit.Micros,
        Apache.Arrow.Types.TimeUnit.Nanosecond => Metadata.TimeUnit.Nanos,
        _ => Metadata.TimeUnit.Micros,
    };
}
