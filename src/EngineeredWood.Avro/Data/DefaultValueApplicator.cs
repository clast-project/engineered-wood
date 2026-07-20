// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Data;

/// <summary>
/// Appends Avro default values (from JSON) into column builders.
/// </summary>
internal static class DefaultValueApplicator
{
    /// <summary>
    /// Appends a default value to a builder based on the schema type and JSON element.
    /// </summary>
    public static void AppendDefault(IColumnBuilder builder, JsonElement defaultValue, AvroSchemaNode schema)
    {
        // Avro spec: for unions, the default must match the first branch type.
        // A null default for a nullable union means AppendNull.
        if (schema is AvroUnionSchema union)
        {
            if (defaultValue.ValueKind == JsonValueKind.Null)
            {
                builder.AppendNull();
                return;
            }
            // For nullable unions, the default type corresponds to the first branch
            // (which is the non-null branch if null isn't first).
            // The NullableBuilder handles the dispatch, but for defaults we apply directly.
            AppendTypedDefault(builder, defaultValue, union.Branches[0]);
            return;
        }

        AppendTypedDefault(builder, defaultValue, schema);
    }

    private static void AppendTypedDefault(
        IColumnBuilder builder, JsonElement defaultValue, AvroSchemaNode schema)
    {
        // Handle logical types on primitives first
        if (schema is AvroPrimitiveSchema { LogicalType: not null } prim)
        {
            if (AppendLogicalDefault(builder, defaultValue, prim))
                return;
        }

        // Handle logical types on fixed (decimal, duration)
        if (schema is AvroFixedSchema { LogicalType: not null } fixedSchema)
        {
            if (AppendFixedLogicalDefault(builder, defaultValue, fixedSchema))
                return;
        }

        AppendBaseDefault(builder, defaultValue, schema);
    }

    /// <summary>
    /// Handles defaults for primitive logical types (date, time, timestamp, decimal-bytes, uuid).
    /// Returns true if handled.
    /// </summary>
    private static bool AppendLogicalDefault(
        IColumnBuilder builder, JsonElement defaultValue, AvroPrimitiveSchema schema)
    {
        switch (schema.LogicalType)
        {
            case "date":
                if (builder is Date32Builder db)
                {
                    db.AppendDefault(defaultValue.GetInt32());
                    return true;
                }
                break;

            case "time-millis":
                if (builder is Time32MillisBuilder tmb)
                {
                    tmb.AppendDefault(defaultValue.GetInt32());
                    return true;
                }
                break;

            case "time-micros":
                if (builder is Time64MicrosBuilder tucb)
                {
                    tucb.AppendDefault(defaultValue.GetInt64());
                    return true;
                }
                break;

            case "time-nanos":
                if (builder is Time64NanosBuilder tnb)
                {
                    tnb.AppendDefault(defaultValue.GetInt64());
                    return true;
                }
                break;

            case "timestamp-millis" or "local-timestamp-millis"
                or "timestamp-micros" or "local-timestamp-micros"
                or "timestamp-nanos" or "local-timestamp-nanos":
                if (builder is TimestampBuilder tsb)
                {
                    tsb.AppendDefault(defaultValue.GetInt64());
                    return true;
                }
                break;

            case "decimal":
                // Avro decimal default on bytes: JSON string of Unicode-escaped bytes
                if (builder is DecimalBytesBuilder dbb)
                {
                    var beBytes = DecodeAvroBytes(defaultValue.GetString()!);
                    dbb.AppendDefaultFromBigEndian(beBytes);
                    return true;
                }
                break;

            case "uuid":
                if (builder is Data.StringBuilder sb)
                {
                    sb.AppendDefault(defaultValue.GetString()!);
                    return true;
                }
                if (builder is Data.GuidBuilder gb)
                {
                    gb.AppendDefault(defaultValue.GetString()!);
                    return true;
                }
                break;
        }

        return false;
    }

    /// <summary>
    /// Handles defaults for fixed-type logical types (decimal, duration).
    /// Returns true if handled.
    /// </summary>
    private static bool AppendFixedLogicalDefault(
        IColumnBuilder builder, JsonElement defaultValue, AvroFixedSchema schema)
    {
        switch (schema.LogicalType)
        {
            case "decimal":
                if (builder is DecimalFixedBuilder dfb)
                {
                    var beBytes = DecodeAvroBytes(defaultValue.GetString()!);
                    dfb.AppendDefaultFromBigEndian(beBytes);
                    return true;
                }
                break;
        }

        return false;
    }

    private static void AppendBaseDefault(
        IColumnBuilder builder, JsonElement defaultValue, AvroSchemaNode schema)
    {
        switch (schema.Type)
        {
            case AvroType.Null:
                builder.AppendNull();
                break;

            case AvroType.Boolean:
                if (builder is BooleanBuilder bb)
                    bb.AppendDefault(defaultValue.GetBoolean());
                else
                    builder.AppendNull(); // fallback
                break;

            case AvroType.Int:
                if (builder is Int32Builder ib)
                    ib.AppendDefault(defaultValue.GetInt32());
                else if (builder is Date32Builder db)
                    db.AppendDefault(defaultValue.GetInt32());
                else if (builder is Time32MillisBuilder tmb)
                    tmb.AppendDefault(defaultValue.GetInt32());
                else
                    builder.AppendNull();
                break;

            case AvroType.Long:
                if (builder is Int64Builder lb)
                    lb.AppendDefault(defaultValue.GetInt64());
                else if (builder is TimestampBuilder tsb)
                    tsb.AppendDefault(defaultValue.GetInt64());
                else if (builder is Time64MicrosBuilder tucb)
                    tucb.AppendDefault(defaultValue.GetInt64());
                else if (builder is Time64NanosBuilder tnb)
                    tnb.AppendDefault(defaultValue.GetInt64());
                else
                    builder.AppendNull();
                break;

            case AvroType.Float:
                if (builder is FloatBuilder fb)
                    fb.AppendDefault(defaultValue.GetSingle());
                else
                    builder.AppendNull();
                break;

            case AvroType.Double:
                if (builder is DoubleBuilder dob)
                    dob.AppendDefault(defaultValue.GetDouble());
                else
                    builder.AppendNull();
                break;

            case AvroType.String:
                if (builder is Data.StringBuilder sb)
                    sb.AppendDefault(defaultValue.GetString()!);
                else
                    builder.AppendNull();
                break;

            case AvroType.Bytes:
                if (builder is BinaryBuilder binb)
                    binb.AppendDefault(defaultValue.GetBytesFromBase64());
                else
                    builder.AppendNull();
                break;

            case AvroType.Enum:
                if (schema is AvroEnumSchema enumSchema)
                {
                    var symbolName = defaultValue.GetString()!;
                    int idx = ((IList<string>)enumSchema.Symbols).IndexOf(symbolName);
                    if (idx < 0)
                        throw new InvalidOperationException(
                            $"Default enum value '{symbolName}' not found in symbols.");
                    if (builder is EnumBuilder eb)
                        eb.AppendDefault(idx);
                    else if (builder is RemappingEnumBuilder reb)
                        reb.AppendDefault(idx);
                    else
                        builder.AppendNull();
                }
                else
                    builder.AppendNull();
                break;

            default:
                // For complex types (arrays, maps, records, fixed), null is the safest fallback
                builder.AppendNull();
                break;
        }
    }

    /// <summary>
    /// Decodes an Avro bytes default value from its JSON string representation.
    /// Avro represents bytes defaults as ISO-8859-1 encoded strings (each char is a byte value 0-255).
    /// </summary>
    private static byte[] DecodeAvroBytes(string avroString)
    {
        var bytes = new byte[avroString.Length];
        for (int i = 0; i < avroString.Length; i++)
            bytes[i] = (byte)avroString[i];
        return bytes;
    }
}
