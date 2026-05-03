// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Tagged union mirroring vortex-proto's ScalarValue oneof. Phase 1 decoders
/// produce this, then encoding-specific code maps it to an Arrow value.
/// </summary>
internal readonly struct ScalarValueProto
{
    public ScalarValueKind Kind { get; }
    public bool BoolValue { get; }
    public long Int64Value { get; }
    public ulong UInt64Value { get; }
    public float F32Value { get; }
    public double F64Value { get; }
    public string? StringValue { get; }
    public byte[]? BytesValue { get; }

    private ScalarValueProto(
        ScalarValueKind kind,
        bool boolValue = false,
        long int64Value = 0,
        ulong uint64Value = 0,
        float f32Value = 0,
        double f64Value = 0,
        string? stringValue = null,
        byte[]? bytesValue = null)
    {
        Kind = kind;
        BoolValue = boolValue;
        Int64Value = int64Value;
        UInt64Value = uint64Value;
        F32Value = f32Value;
        F64Value = f64Value;
        StringValue = stringValue;
        BytesValue = bytesValue;
    }

    public static ScalarValueProto Null() => new(ScalarValueKind.Null);
    public static ScalarValueProto Bool(bool v) => new(ScalarValueKind.Bool, boolValue: v);
    public static ScalarValueProto Int64(long v) => new(ScalarValueKind.Int64, int64Value: v);
    public static ScalarValueProto UInt64(ulong v) => new(ScalarValueKind.UInt64, uint64Value: v);
    public static ScalarValueProto F32(float v) => new(ScalarValueKind.F32, f32Value: v);
    public static ScalarValueProto F64(double v) => new(ScalarValueKind.F64, f64Value: v);
    public static ScalarValueProto Str(string v) => new(ScalarValueKind.String, stringValue: v);
    public static ScalarValueProto Bytes(byte[] v) => new(ScalarValueKind.Bytes, bytesValue: v);

    /// <summary>
    /// Parse a vortex-proto ScalarValue from its serialized bytes. Field
    /// numbers (per <c>vortex-proto/proto/scalar.proto</c>):
    /// 1 null, 2 bool, 3 int64 (sint64), 4 uint64, 5 f32, 6 f64, 7 string,
    /// 8 bytes, 9 list (unsupported), 10 f16 (uint64-encoded, unsupported), 11 variant (unsupported).
    /// </summary>
    public static ScalarValueProto Parse(ReadOnlySpan<byte> bytes)
    {
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            switch (fieldNum)
            {
                case 1 when wireType == 0:
                    Varint.ReadUnsigned(bytes, ref pos);
                    return Null();
                case 2 when wireType == 0:
                    return Bool(Varint.ReadUnsigned(bytes, ref pos) != 0);
                case 3 when wireType == 0:
                    return Int64(Varint.ReadSigned(bytes, ref pos));
                case 4 when wireType == 0:
                    return UInt64((ulong)Varint.ReadUnsigned(bytes, ref pos));
                case 5 when wireType == 5:
                    {
                        var iBits = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(pos));
                        pos += 4;
#if NET8_0_OR_GREATER
                        return F32(BitConverter.Int32BitsToSingle(iBits));
#else
                        return F32(BitConverter.ToSingle(BitConverter.GetBytes(iBits), 0));
#endif
                    }
                case 6 when wireType == 1:
                    {
                        var lBits = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(pos));
                        pos += 8;
                        return F64(BitConverter.Int64BitsToDouble(lBits));
                    }
                case 7 when wireType == 2:
                    {
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        var slice = bytes.Slice(pos, len);
                        pos += len;
#if NET8_0_OR_GREATER
                        return Str(System.Text.Encoding.UTF8.GetString(slice));
#else
                        return Str(System.Text.Encoding.UTF8.GetString(slice.ToArray()));
#endif
                    }
                case 8 when wireType == 2:
                    {
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        var copy = bytes.Slice(pos, len).ToArray();
                        pos += len;
                        return Bytes(copy);
                    }
                default:
                    SkipField(bytes, ref pos, wireType);
                    break;
            }
        }
        // Empty ScalarValue payload — treat as null.
        return Null();
    }

    private static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, uint wireType)
    {
        switch (wireType)
        {
            case 0: Varint.ReadUnsigned(bytes, ref pos); break;
            case 1: pos += 8; break;
            case 2:
                {
                    var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                    pos += len;
                    break;
                }
            case 5: pos += 4; break;
            default:
                throw new VortexFormatException(
                    $"Unsupported protobuf wire type {wireType} in ScalarValue.");
        }
    }
}

internal enum ScalarValueKind
{
    Null,
    Bool,
    Int64,
    UInt64,
    F32,
    F64,
    String,
    Bytes,
}
