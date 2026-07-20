// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Converts between Avro schema trees and Arrow schemas.
/// </summary>
internal static class ArrowSchemaConverter
{
    private const int MaxSchemaDepth = 64;

    /// <summary>Converts an Avro record schema to an Arrow Schema.</summary>
    public static Apache.Arrow.Schema ToArrow(AvroRecordSchema record, ExtensionTypeRegistry? extensionRegistry = null)
    {
        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var field in record.Fields)
        {
            var (arrowType, nullable) = ToArrowType(field.Schema, 0, extensionRegistry);
            builder.Field(new Field(field.Name, arrowType, nullable));
        }
        return builder.Build();
    }

    /// <summary>
    /// Converts an Avro schema node to an Arrow type.
    /// Returns the type and whether it is nullable.
    /// </summary>
    public static (IArrowType type, bool nullable) ToArrowType(AvroSchemaNode node, int depth = 0, ExtensionTypeRegistry? extensionRegistry = null)
    {
        if (depth >= MaxSchemaDepth)
            throw new NotSupportedException(
                "Avro schema exceeds maximum nesting depth. Recursive schemas cannot be represented in Arrow.");

        switch (node)
        {
            case AvroPrimitiveSchema p:
                return (ToArrowPrimitive(p, extensionRegistry), false);

            case AvroRecordSchema r:
                var fields = new List<Field>();
                foreach (var f in r.Fields)
                {
                    var (ft, fn) = ToArrowType(f.Schema, depth + 1, extensionRegistry);
                    fields.Add(new Field(f.Name, ft, fn));
                }
                return (new StructType(fields), false);

            case AvroEnumSchema e:
                // Enum → Dictionary(Int32, Utf8)
                return (new DictionaryType(Int32Type.Default, StringType.Default, false), false);

            case AvroArraySchema a:
                var (itemType, itemNullable) = ToArrowType(a.Items, depth + 1, extensionRegistry);
                return (new ListType(new Field("item", itemType, itemNullable)), false);

            case AvroMapSchema m:
                var (valType, valNullable) = ToArrowType(m.Values, depth + 1, extensionRegistry);
                return (new MapType(
                    new Field("key", StringType.Default, false),
                    new Field("value", valType, valNullable)), false);

            case AvroFixedSchema f:
                if (f.LogicalType == "decimal")
                    return (new Decimal128Type(f.Precision ?? 38, f.Scale ?? 0), false);
                if (f.LogicalType == "duration" && f.Size == 12)
                    return (new IntervalType(IntervalUnit.MonthDayNanosecond), false);
                return (new FixedSizeBinaryType(f.Size), false);

            case AvroUnionSchema u:
                if (u.IsNullable(out var inner, out _))
                {
                    var (innerType, _) = ToArrowType(inner, depth + 1, extensionRegistry);
                    return (innerType, true);
                }
                // General union → DenseUnion
                var unionFields = new List<Field>();
                var typeIds = new int[u.Branches.Count];
                for (int i = 0; i < u.Branches.Count; i++)
                {
                    var (bt, bn) = ToArrowType(u.Branches[i], depth + 1, extensionRegistry);
                    unionFields.Add(new Field($"branch{i}", bt, bn));
                    typeIds[i] = i;
                }
                return (new UnionType(unionFields, typeIds, UnionMode.Dense), false);

            default:
                throw new NotSupportedException($"Unsupported Avro schema type: {node.Type}");
        }
    }

    private static IArrowType ToArrowPrimitive(AvroPrimitiveSchema p, ExtensionTypeRegistry? extensionRegistry = null)
    {
        // Check logical type first
        if (p.LogicalType != null)
        {
            return p.LogicalType switch
            {
                "date" => Date32Type.Default,
                "time-millis" => new Time32Type(TimeUnit.Millisecond),
                "time-micros" => new Time64Type(TimeUnit.Microsecond),
                "time-nanos" => new Time64Type(TimeUnit.Nanosecond),
                "timestamp-millis" => new TimestampType(TimeUnit.Millisecond, "UTC"),
                "timestamp-micros" => new TimestampType(TimeUnit.Microsecond, "UTC"),
                "timestamp-nanos" => new TimestampType(TimeUnit.Nanosecond, "UTC"),
                "local-timestamp-millis" => new TimestampType(TimeUnit.Millisecond, (string?)null),
                "local-timestamp-micros" => new TimestampType(TimeUnit.Microsecond, (string?)null),
                "local-timestamp-nanos" => new TimestampType(TimeUnit.Nanosecond, (string?)null),
                "decimal" => new Decimal128Type(p.Precision ?? 38, p.Scale ?? 0),
                "uuid" => MakeUuidArrowType(extensionRegistry),
                _ => ToArrowBasePrimitive(p.Type), // Unknown logical type: fall through to base
            };
        }

        return ToArrowBasePrimitive(p.Type);
    }

    /// <summary>
    /// Decodes the Avro <c>uuid</c> logical type to an Arrow type. When the
    /// caller has registered an <c>arrow.uuid</c> extension via the supplied
    /// <see cref="ExtensionTypeRegistry"/>, returns that extension type
    /// (typically <c>GuidType</c>); otherwise returns <see cref="StringType"/>
    /// per the historical Avro mapping.
    /// </summary>
    private static IArrowType MakeUuidArrowType(ExtensionTypeRegistry? registry)
    {
        if (registry is { } reg
            && reg.TryGetDefinition("arrow.uuid", out var definition)
            && definition.TryCreateType(new FixedSizeBinaryType(16), metadata: string.Empty, out var extType))
        {
            return extType;
        }
        return StringType.Default;
    }

    private static IArrowType ToArrowBasePrimitive(AvroType type) => type switch
    {
        AvroType.Null => NullType.Default,
        AvroType.Boolean => BooleanType.Default,
        AvroType.Int => Int32Type.Default,
        AvroType.Long => Int64Type.Default,
        AvroType.Float => FloatType.Default,
        AvroType.Double => DoubleType.Default,
        AvroType.Bytes => BinaryType.Default,
        AvroType.String => StringType.Default,
        _ => throw new NotSupportedException($"Unsupported Avro primitive type: {type}"),
    };

    /// <summary>Converts an Arrow Schema to an Avro record schema.</summary>
    public static AvroRecordSchema FromArrow(Apache.Arrow.Schema arrowSchema, string name = "Record", string? ns = null)
    {
        var fields = new List<AvroFieldNode>();
        foreach (var f in arrowSchema.FieldsList)
        {
            var avroType = FromArrowType(f.DataType);
            if (f.IsNullable && avroType.Type != AvroType.Union)
                avroType = new AvroUnionSchema([AvroPrimitiveSchema.Null, avroType]);
            fields.Add(new AvroFieldNode(f.Name, avroType));
        }
        return new AvroRecordSchema(name, ns, fields);
    }

    public static AvroSchemaNode FromArrowType(IArrowType type) => type switch
    {
        NullType => AvroPrimitiveSchema.Null,
        BooleanType => AvroPrimitiveSchema.Boolean,
        Int8Type or Int16Type or Int32Type => AvroPrimitiveSchema.Int,
        UInt8Type or UInt16Type => AvroPrimitiveSchema.Int,
        Int64Type or UInt32Type => AvroPrimitiveSchema.Long,
        UInt64Type => AvroPrimitiveSchema.Long, // potential overflow but best mapping
        FloatType or HalfFloatType => AvroPrimitiveSchema.Float,
        DoubleType => AvroPrimitiveSchema.Double,
        StringType => AvroPrimitiveSchema.String,
        BinaryType => AvroPrimitiveSchema.Bytes,
        Date32Type or Date64Type => new AvroPrimitiveSchema(AvroType.Int) { LogicalType = "date" },
        Time32Type t when t.Unit == TimeUnit.Millisecond
            => new AvroPrimitiveSchema(AvroType.Int) { LogicalType = "time-millis" },
        Time64Type t when t.Unit == TimeUnit.Microsecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "time-micros" },
        Time64Type t when t.Unit == TimeUnit.Nanosecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "time-nanos" },
        TimestampType ts when ts.Timezone != null && ts.Unit == TimeUnit.Millisecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "timestamp-millis" },
        TimestampType ts when ts.Timezone != null && ts.Unit == TimeUnit.Microsecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "timestamp-micros" },
        TimestampType ts when ts.Timezone != null && ts.Unit == TimeUnit.Nanosecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "timestamp-nanos" },
        TimestampType ts when ts.Timezone == null && ts.Unit == TimeUnit.Millisecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "local-timestamp-millis" },
        TimestampType ts when ts.Timezone == null && ts.Unit == TimeUnit.Microsecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "local-timestamp-micros" },
        TimestampType ts when ts.Timezone == null && ts.Unit == TimeUnit.Nanosecond
            => new AvroPrimitiveSchema(AvroType.Long) { LogicalType = "local-timestamp-nanos" },
        IntervalType it when it.Unit == IntervalUnit.MonthDayNanosecond
            => new AvroFixedSchema("duration", null, 12) { LogicalType = "duration" },
        Decimal128Type dec => new AvroFixedSchema("decimal", null, dec.ByteWidth)
            { LogicalType = "decimal", Precision = dec.Precision, Scale = dec.Scale },
        FixedSizeBinaryType fb => new AvroFixedSchema("fixed", null, fb.ByteWidth),
        DictionaryType dt => new AvroEnumSchema("Enum", null, []),
        // GuidType -> string + uuid logical type (Avro stores UUIDs as 36-char
        // strings per spec). Fall back to the storage type for any other
        // extension we don't have a specialised mapping for.
        ExtensionType ext when ext.Name == "arrow.uuid"
            => new AvroPrimitiveSchema(AvroType.String) { LogicalType = "uuid" },
        ExtensionType ext => FromArrowType(ext.StorageType),
        StructType st => FromArrowStruct(st),
        ListType lt => new AvroArraySchema(FromArrowField(lt.ValueField)),
        MapType mt => new AvroMapSchema(FromArrowField(mt.ValueField)),
        UnionType ut => new AvroUnionSchema(ut.Fields.Select(f => FromArrowType(f.DataType)).ToList()),
        _ => throw new NotSupportedException($"Arrow type {type} is not yet supported for Avro conversion."),
    };

    private static AvroSchemaNode FromArrowField(Field field)
    {
        var avroType = FromArrowType(field.DataType);
        if (field.IsNullable && avroType.Type != AvroType.Union)
            return new AvroUnionSchema([AvroPrimitiveSchema.Null, avroType]);
        return avroType;
    }

    private static int _structCounter;

    private static AvroRecordSchema FromArrowStruct(StructType st)
    {
        var fields = new List<AvroFieldNode>();
        foreach (var f in st.Fields)
        {
            var avroType = FromArrowType(f.DataType);
            if (f.IsNullable && avroType.Type != AvroType.Union)
                avroType = new AvroUnionSchema([AvroPrimitiveSchema.Null, avroType]);
            fields.Add(new AvroFieldNode(f.Name, avroType));
        }
        var name = $"Struct{Interlocked.Increment(ref _structCounter)}";
        return new AvroRecordSchema(name, null, fields);
    }
}
