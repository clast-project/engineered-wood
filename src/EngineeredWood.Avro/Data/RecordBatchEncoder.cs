// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro.Data;

/// <summary>
/// Encodes Arrow RecordBatches into Avro binary-encoded records.
/// </summary>
internal sealed class RecordBatchEncoder
{
    private readonly AvroRecordSchema _schema;

    public RecordBatchEncoder(AvroRecordSchema schema)
    {
        _schema = schema;
    }

    /// <summary>
    /// Encodes all rows from a RecordBatch into the provided buffer.
    /// Returns the number of rows encoded.
    /// </summary>
    public int Encode(RecordBatch batch, GrowableBuffer output)
    {
        var writer = new AvroBinaryWriter(output);

        for (int row = 0; row < batch.Length; row++)
            EncodeRecord(writer, batch, row, _schema);

        return batch.Length;
    }

    private static void EncodeRecord(AvroBinaryWriter writer, RecordBatch batch, int row, AvroRecordSchema schema)
    {
        for (int col = 0; col < schema.Fields.Count; col++)
        {
            var fieldSchema = schema.Fields[col].Schema;
            var array = batch.Column(col);
            EncodeValue(writer, array, row, fieldSchema);
        }
    }

    internal static void EncodeValue(AvroBinaryWriter writer, IArrowArray array, int row, AvroSchemaNode schema)
    {
        switch (schema)
        {
            case AvroUnionSchema union when union.IsNullable(out var inner, out int nullIndex):
                if (!array.IsValid(row))
                {
                    writer.WriteUnionIndex(nullIndex);
                }
                else
                {
                    writer.WriteUnionIndex(nullIndex == 0 ? 1 : 0);
                    EncodeNonNullValue(writer, array, row, inner);
                }
                break;

            case AvroUnionSchema union:
                EncodeDenseUnion(writer, array, row, union);
                break;

            default:
                EncodeNonNullValue(writer, array, row, schema);
                break;
        }
    }

    private static void EncodeDenseUnion(AvroBinaryWriter writer, IArrowArray array, int row, AvroUnionSchema union)
    {
        var unionArray = (UnionArray)array;
        byte typeId = unionArray.TypeIds[row];
        writer.WriteUnionIndex(typeId);

        if (unionArray is DenseUnionArray denseUnion)
        {
            int offset = denseUnion.ValueOffsets[row];
            var childArray = denseUnion.Fields[typeId];
            EncodeNonNullValue(writer, childArray, offset, union.Branches[typeId]);
        }
        else
        {
            // SparseUnion: child index equals row
            var childArray = unionArray.Fields[typeId];
            EncodeNonNullValue(writer, childArray, row, union.Branches[typeId]);
        }
    }

    private static void EncodeNonNullValue(AvroBinaryWriter writer, IArrowArray array, int row, AvroSchemaNode schema)
    {
        switch (schema)
        {
            case AvroPrimitiveSchema p:
                EncodePrimitive(writer, array, row, p);
                break;
            case AvroEnumSchema:
                EncodeEnum(writer, array, row);
                break;
            case AvroFixedSchema f when f.LogicalType == "decimal":
                EncodeDecimalFixed(writer, array, row, f.Size);
                break;
            case AvroFixedSchema f when f.LogicalType == "duration" && f.Size == 12:
                EncodeDuration(writer, array, row);
                break;
            case AvroFixedSchema f:
                EncodeFixed(writer, array, row, f.Size);
                break;
            case AvroArraySchema a:
                EncodeArray(writer, array, row, a);
                break;
            case AvroMapSchema m:
                EncodeMap(writer, array, row, m);
                break;
            case AvroRecordSchema r:
                EncodeStruct(writer, array, row, r);
                break;
            default:
                throw new NotSupportedException($"Encoding Avro type {schema.Type} not yet supported.");
        }
    }

    private static void EncodePrimitive(AvroBinaryWriter writer, IArrowArray array, int row, AvroPrimitiveSchema schema)
    {
        // Logical types use the same base encoding but with different Arrow array types
        if (schema.LogicalType != null)
        {
            EncodePrimitiveWithLogicalType(writer, array, row, schema);
            return;
        }

        switch (schema.Type)
        {
            case AvroType.Null:
                writer.WriteNull();
                break;
            case AvroType.Boolean:
                writer.WriteBoolean(((BooleanArray)array).GetValue(row)!.Value);
                break;
            case AvroType.Int:
                writer.WriteInt(((Int32Array)array).GetValue(row)!.Value);
                break;
            case AvroType.Long:
                writer.WriteLong(((Int64Array)array).GetValue(row)!.Value);
                break;
            case AvroType.Float:
                writer.WriteFloat(((FloatArray)array).GetValue(row)!.Value);
                break;
            case AvroType.Double:
                writer.WriteDouble(((DoubleArray)array).GetValue(row)!.Value);
                break;
            case AvroType.String:
                writer.WriteString(((StringArray)array).GetString(row)!);
                break;
            case AvroType.Bytes:
                writer.WriteBytes(((BinaryArray)array).GetBytes(row));
                break;
            default:
                throw new NotSupportedException($"Encoding Avro primitive {schema.Type} not supported.");
        }
    }

    private static void EncodePrimitiveWithLogicalType(
        AvroBinaryWriter writer, IArrowArray array, int row, AvroPrimitiveSchema schema)
    {
        switch (schema.LogicalType)
        {
            case "date":
                writer.WriteInt(((Date32Array)array).GetValue(row)!.Value);
                break;
            case "time-millis":
                writer.WriteInt(((Time32Array)array).GetValue(row)!.Value);
                break;
            case "time-micros":
                writer.WriteLong(((Time64Array)array).GetValue(row)!.Value);
                break;
            case "time-nanos":
                writer.WriteLong(((Time64Array)array).GetValue(row)!.Value);
                break;
            case "timestamp-millis" or "local-timestamp-millis":
                writer.WriteLong(((TimestampArray)array).GetValue(row)!.Value);
                break;
            case "timestamp-micros" or "local-timestamp-micros":
                writer.WriteLong(((TimestampArray)array).GetValue(row)!.Value);
                break;
            case "timestamp-nanos" or "local-timestamp-nanos":
                writer.WriteLong(((TimestampArray)array).GetValue(row)!.Value);
                break;
            case "decimal":
                EncodeDecimalBytes(writer, array, row);
                break;
            case "uuid":
                // Either StringArray (default mapping) or GuidArray (when the
                // caller opted in via WithExtensionRegistry). Both round-trip
                // as 36-char strings on the wire.
                if (array is GuidArray ga)
                    writer.WriteString(ga.GetGuid(row)!.Value.ToString("D"));
                else
                    writer.WriteString(((StringArray)array).GetString(row)!);
                break;
            default:
                // Unknown logical type: fall through to base encoding
                EncodePrimitive(writer, array, row,
                    new AvroPrimitiveSchema(schema.Type));
                break;
        }
    }

    private static void EncodeEnum(AvroBinaryWriter writer, IArrowArray array, int row)
    {
        var dictArray = (DictionaryArray)array;
        var indices = (Int32Array)dictArray.Indices;
        writer.WriteInt(indices.GetValue(row)!.Value);
    }

    private static void EncodeFixed(AvroBinaryWriter writer, IArrowArray array, int row, int size)
    {
        var fixedArray = (FixedSizeBinaryArray)array;
        writer.WriteFixed(fixedArray.GetBytes(row));
    }

    /// <summary>
    /// Encodes an Arrow MonthDayNanosecondInterval as Avro duration
    /// (fixed 12 bytes: months u32 LE, days u32 LE, millis u32 LE).
    /// </summary>
    private static void EncodeDuration(AvroBinaryWriter writer, IArrowArray array, int row)
    {
        var intervalArray = (MonthDayNanosecondIntervalArray)array;
        var val = intervalArray.GetValue(row)!.Value;

        Span<byte> output = stackalloc byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(output, (uint)val.Months);
        BinaryPrimitives.WriteUInt32LittleEndian(output[4..], (uint)val.Days);
        BinaryPrimitives.WriteUInt32LittleEndian(output[8..], (uint)(val.Nanoseconds / 1_000_000));
        writer.WriteFixed(output);
    }

    private static void EncodeArray(AvroBinaryWriter writer, IArrowArray array, int row, AvroArraySchema schema)
    {
        var listArray = (ListArray)array;
        int start = listArray.ValueOffsets[row];
        int length = listArray.GetValueLength(row);

        if (length > 0)
        {
            writer.WriteLong(length); // block count
            var values = listArray.Values;
            for (int i = start; i < start + length; i++)
                EncodeValue(writer, values, i, schema.Items);
        }
        writer.WriteLong(0); // terminating 0-count block
    }

    private static void EncodeMap(AvroBinaryWriter writer, IArrowArray array, int row, AvroMapSchema schema)
    {
        var mapArray = (MapArray)array;
        int start = mapArray.ValueOffsets[row];
        int length = mapArray.GetValueLength(row);

        if (length > 0)
        {
            writer.WriteLong(length);
            var keys = (StringArray)mapArray.Keys;
            var values = mapArray.Values;
            for (int i = start; i < start + length; i++)
            {
                writer.WriteString(keys.GetString(i)!);
                EncodeValue(writer, values, i, schema.Values);
            }
        }
        writer.WriteLong(0); // terminating 0-count block
    }

    /// <summary>Encodes a Decimal128 value as Avro fixed bytes (little-endian → big-endian).</summary>
    private static void EncodeDecimalFixed(AvroBinaryWriter writer, IArrowArray array, int row, int fixedSize)
    {
        var fixedArray = (FixedSizeBinaryArray)array;
        var leBytes = fixedArray.GetBytes(row); // 16-byte little-endian
        // fixedSize comes from a user-controlled Avro schema and could in principle be large;
        // cap stack usage at 1 KB and rent from the pool above that.
        byte[]? rented = null;
        Span<byte> bigEndian = fixedSize <= 1024
            ? stackalloc byte[fixedSize]
            : (rented = ArrayPool<byte>.Shared.Rent(fixedSize)).AsSpan(0, fixedSize);
        try
        {
            Decimal128ToAvroFixed(leBytes, bigEndian);
            writer.WriteFixed(bigEndian);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Encodes a Decimal128 value as Avro bytes (little-endian → big-endian, minimal representation).</summary>
    private static void EncodeDecimalBytes(AvroBinaryWriter writer, IArrowArray array, int row)
    {
        var fixedArray = (FixedSizeBinaryArray)array;
        var leBytes = fixedArray.GetBytes(row); // 16-byte little-endian

        // Convert LE to BE and trim in-place on the stack
        Span<byte> bigEndian = stackalloc byte[16];
        for (int i = 0; i < 16; i++)
            bigEndian[i] = leBytes[15 - i];

        int start = TrimLeadingSignBytes(bigEndian);
        writer.WriteBytes(bigEndian[start..]);
    }

    /// <summary>Converts 16-byte little-endian Arrow Decimal128 to big-endian for Avro fixed encoding.</summary>
    private static void Decimal128ToAvroFixed(ReadOnlySpan<byte> leBytes, Span<byte> bigEndian)
    {
        // Reverse 16-byte LE to BE, sign-extend or truncate to target size
        byte signExtend = (leBytes[15] & 0x80) != 0 ? (byte)0xFF : (byte)0x00;
        bigEndian.Fill(signExtend);
        int copyLen = Math.Min(leBytes.Length, bigEndian.Length);
        for (int i = 0; i < copyLen; i++)
            bigEndian[bigEndian.Length - 1 - i] = leBytes[i];
    }

    /// <summary>
    /// Returns the start index after trimming redundant leading sign-extension bytes.
    /// The span must be big-endian two's complement.
    /// </summary>
    private static int TrimLeadingSignBytes(ReadOnlySpan<byte> bigEndian)
    {
        bool isNegative = (bigEndian[0] & 0x80) != 0;
        byte signByte = isNegative ? (byte)0xFF : (byte)0x00;
        int start = 0;
        while (start < bigEndian.Length - 1 && bigEndian[start] == signByte)
        {
            if ((bigEndian[start + 1] & 0x80) != (signByte & 0x80))
                break;
            start++;
        }
        return start;
    }

    private static void EncodeStruct(AvroBinaryWriter writer, IArrowArray array, int row, AvroRecordSchema schema)
    {
        var structArray = (StructArray)array;
        for (int i = 0; i < schema.Fields.Count; i++)
        {
            var childArray = structArray.Fields[i];
            EncodeValue(writer, childArray, row, schema.Fields[i].Schema);
        }
    }
}
