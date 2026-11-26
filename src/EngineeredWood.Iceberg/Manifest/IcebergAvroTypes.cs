// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// The spec's Avro mapping for the primitive types a partition value can have (spec "Avro" appendix), and
/// conversion between Avro values and <see cref="PartitionData"/>'s physical values.
/// </summary>
internal static class IcebergAvroTypes
{
    /// <summary>Writes the Avro type for <paramref name="type"/>. Named types use Java's names.</summary>
    public static void Write(Utf8JsonWriter w, IcebergType type, int fieldId)
    {
        switch (type)
        {
            case BooleanType: w.WriteStringValue("boolean"); break;
            case IntegerType: w.WriteStringValue("int"); break;
            case LongType: w.WriteStringValue("long"); break;
            case FloatType: w.WriteStringValue("float"); break;
            case DoubleType: w.WriteStringValue("double"); break;
            case StringType: w.WriteStringValue("string"); break;
            case BinaryType: w.WriteStringValue("bytes"); break;
            case DateType: Logical(w, "int", "date"); break;
            case TimeType: Logical(w, "long", "time-micros"); break;
            case TimestampType: Logical(w, "long", "timestamp-micros", adjustToUtc: false); break;
            case TimestamptzType: Logical(w, "long", "timestamp-micros", adjustToUtc: true); break;
            case TimestampNsType: Logical(w, "long", "timestamp-nanos", adjustToUtc: false); break;
            case TimestamptzNsType: Logical(w, "long", "timestamp-nanos", adjustToUtc: true); break;
            case UuidType:
                w.WriteStartObject();
                w.WriteString("type", "fixed");
                w.WriteString("name", "uuid_fixed");
                w.WriteNumber("size", 16);
                w.WriteString("logicalType", "uuid");
                w.WriteEndObject();
                break;
            case FixedType f:
                w.WriteStartObject();
                w.WriteString("type", "fixed");
                w.WriteString("name", $"fixed_{f.Length}");
                w.WriteNumber("size", f.Length);
                w.WriteEndObject();
                break;
            case DecimalType d:
                w.WriteStartObject();
                w.WriteString("type", "fixed");
                w.WriteString("name", $"decimal_{d.Precision}_{d.Scale}");
                w.WriteNumber("size", DecimalBytes(d.Precision));
                w.WriteString("logicalType", "decimal");
                w.WriteNumber("precision", d.Precision);
                w.WriteNumber("scale", d.Scale);
                w.WriteEndObject();
                break;
            default:
                throw new NotSupportedException(
                    $"Partition field {fieldId} has type {type}, which has no manifest encoding.");
        }

        static void Logical(Utf8JsonWriter w, string avroType, string logicalType, bool? adjustToUtc = null)
        {
            w.WriteStartObject();
            w.WriteString("type", avroType);
            w.WriteString("logicalType", logicalType);
            if (adjustToUtc is { } adjust)
                w.WriteBoolean("adjust-to-utc", adjust);
            w.WriteEndObject();
        }
    }

    /// <summary>The fewest bytes whose two's complement holds every unscaled value of precision <paramref name="precision"/>.</summary>
    internal static int DecimalBytes(int precision)
    {
        var max = BigInteger.Pow(10, precision) - 1;
        int bytes = 1;
        while (BigInteger.Pow(2, 8 * bytes - 1) - 1 < max)
            bytes++;
        return bytes;
    }

    /// <summary>
    /// The Iceberg type an Avro partition-field schema stands for: what a manifest declares when no partition
    /// spec is at hand to type it.
    /// </summary>
    public static IcebergType InferType(AvroSchemaNode node)
    {
        if (node is AvroUnionSchema u && u.IsNullable(out var inner, out _))
            node = inner;

        switch (node)
        {
            case AvroPrimitiveSchema p:
                bool adjust = p.Properties.TryGetValue("adjust-to-utc", out var a) && a.ValueKind == JsonValueKind.True;
                return (p.Type, p.LogicalType) switch
                {
                    (AvroType.Boolean, _) => IcebergType.Boolean,
                    (AvroType.Int, "date") => IcebergType.Date,
                    (AvroType.Int, _) => IcebergType.Int,
                    (AvroType.Long, "time-micros") => IcebergType.Time,
                    (AvroType.Long, "timestamp-micros") => adjust ? IcebergType.Timestamptz : IcebergType.Timestamp,
                    (AvroType.Long, "timestamp-nanos") => adjust ? IcebergType.TimestamptzNs : IcebergType.TimestampNs,
                    (AvroType.Long, _) => IcebergType.Long,
                    (AvroType.Float, _) => IcebergType.Float,
                    (AvroType.Double, _) => IcebergType.Double,
                    (AvroType.String, "uuid") => IcebergType.Uuid,
                    (AvroType.String, _) => IcebergType.String,
                    (AvroType.Bytes, "decimal") => IcebergType.Decimal(p.Precision!.Value, p.Scale!.Value),
                    (AvroType.Bytes, _) => IcebergType.Binary,
                    _ => throw new InvalidDataException($"A partition field has Avro type {p.Type}."),
                };
            case AvroFixedSchema f:
                return f.LogicalType switch
                {
                    "uuid" => IcebergType.Uuid,
                    "decimal" => IcebergType.Decimal(f.Precision!.Value, f.Scale!.Value),
                    _ => new FixedType(f.Size),
                };
            default:
                throw new InvalidDataException($"A partition field has Avro type {node.Type}, which no Iceberg type maps to.");
        }
    }

    /// <summary>Converts a decoded Avro value to <paramref name="type"/>'s physical form.</summary>
    public static object? ToPhysical(object? value, IcebergType type)
    {
        if (value is null)
            return null;

        return (type, value) switch
        {
            (UuidType, byte[] b) when b.Length == 16 => GuidFromBigEndian(b),
            (UuidType, string s) => Guid.Parse(s),
            (DecimalType, byte[] b) => UnscaledFromBigEndian(b),
            // A promoted column's older manifests hold the narrower type.
            (LongType or TimeType or TimestampType or TimestamptzType or TimestampNsType or TimestamptzNsType, int i) => (long)i,
            (DoubleType, float f) => (double)f,
            (BooleanType, bool) or (IntegerType or DateType, int) or
                (LongType or TimeType or TimestampType or TimestamptzType or TimestampNsType or TimestamptzNsType, long) or
                (FloatType, float) or (DoubleType, double) or (StringType, string) or
                (BinaryType or FixedType, byte[]) => value,
            _ => throw new InvalidDataException(
                $"A partition value of CLR type {value.GetType().Name} cannot be read as Iceberg type {type}."),
        };
    }

    /// <summary>Converts a physical value of <paramref name="type"/> to the Avro value <see cref="Write"/>'s schema takes.</summary>
    public static object? ToAvro(object? value, IcebergType type)
    {
        if (value is null)
            return null;

        return (type, value) switch
        {
            (UuidType, Guid g) => GuidToBigEndian(g),
            (DecimalType d, BigInteger unscaled) => UnscaledToBigEndian(unscaled, DecimalBytes(d.Precision)),
            (FixedType f, byte[] b) when b.Length == f.Length => b,
            (BooleanType, bool) or (IntegerType or DateType, int) or
                (LongType or TimeType or TimestampType or TimestamptzType or TimestampNsType or TimestamptzNsType, long) or
                (FloatType, float) or (DoubleType, double) or (StringType, string) or (BinaryType, byte[]) => value,
            _ => throw new ArgumentException(
                $"A partition value of CLR type {value.GetType().Name} does not fit Iceberg type {type}; " +
                "see PartitionData for each type's physical form."),
        };
    }

    // A UUID's 16 bytes in big-endian (RFC 4122) order. Guid.ToByteArray puts the first three groups
    // little-endian.
    internal static Guid GuidFromBigEndian(byte[] b) => new(
        (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3],
        (short)((b[4] << 8) | b[5]),
        (short)((b[6] << 8) | b[7]),
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);

    internal static byte[] GuidToBigEndian(Guid g)
    {
        var b = g.ToByteArray();
        System.Array.Reverse(b, 0, 4);
        System.Array.Reverse(b, 4, 2);
        System.Array.Reverse(b, 6, 2);
        return b;
    }

    internal static BigInteger UnscaledFromBigEndian(byte[] b)
    {
        if (b.Length == 0)
            return BigInteger.Zero;
        var little = (byte[])b.Clone();
        System.Array.Reverse(little);
        return new BigInteger(little);
    }

    internal static byte[] UnscaledToBigEndian(BigInteger unscaled, int size)
    {
        var little = unscaled.ToByteArray();
        if (little.Length > size)
            throw new ArgumentException($"Unscaled decimal {unscaled} does not fit {size} bytes.");
        var result = new byte[size];
        byte sign = unscaled.Sign < 0 ? (byte)0xFF : (byte)0;
        for (int i = 0; i < size; i++)
            result[size - 1 - i] = i < little.Length ? little[i] : sign;
        return result;
    }
}
