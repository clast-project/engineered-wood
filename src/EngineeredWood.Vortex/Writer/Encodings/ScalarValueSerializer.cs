// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Serializes a single <c>vortex.scalar.ScalarValue</c> protobuf message to
/// raw bytes. Used by min/max stats. Schema (per <c>java/vortex-jni/src/main/proto/scalar.proto</c>):
/// <code>
/// message ScalarValue {
///   oneof kind {
///     google.protobuf.NullValue null_value = 1;
///     bool bool_value = 2;
///     sint64 int64_value = 3;     // zigzag varint
///     uint64 uint64_value = 4;     // varint
///     float f32_value = 5;          // fixed32 LE
///     double f64_value = 6;          // fixed64 LE
///     string string_value = 7;       // length-delimited UTF-8
///     bytes bytes_value = 8;         // length-delimited
///     ...
///   }
/// }
/// </code>
/// Tag bytes: 0x10 (bool), 0x18 (sint64), 0x20 (uint64), 0x2D (f32 fixed32),
/// 0x31 (f64 fixed64), 0x3A (string), 0x42 (bytes).
/// </summary>
internal static class ScalarValueSerializer
{
    public static byte[] FromBool(bool value) => new byte[] { 0x10, value ? (byte)1 : (byte)0 };

    public static byte[] FromSignedInt(long value)
    {
        // Field 3, wire type 0 (varint). Body is zigzag-encoded varint.
        Span<byte> tmp = stackalloc byte[1 + 10];
        tmp[0] = 0x18;
        int n = 1 + Varint.WriteUnsigned(tmp.Slice(1), Varint.ZigzagEncode(value));
        return tmp.Slice(0, n).ToArray();
    }

    public static byte[] FromUnsignedInt(ulong value)
    {
        Span<byte> tmp = stackalloc byte[1 + 10];
        tmp[0] = 0x20;
        int n = 1 + Varint.WriteUnsigned(tmp.Slice(1), value);
        return tmp.Slice(0, n).ToArray();
    }

    public static byte[] FromFloat32(float value)
    {
        // Field 5, wire type 5 (fixed32 LE). 1 byte tag + 4 bytes body.
        var bytes = new byte[5];
        bytes[0] = 0x2D;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(1), FloatBits(value));
        return bytes;
    }

    public static byte[] FromFloat64(double value)
    {
        // Field 6, wire type 1 (fixed64 LE). 1 byte tag + 8 bytes body.
        var bytes = new byte[9];
        bytes[0] = 0x31;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(1), DoubleBits(value));
        return bytes;
    }

    /// <summary>
    /// Field 7, wire type 2 (length-delimited). Tag 0x3A + varint length + UTF-8 bytes.
    /// </summary>
    public static byte[] FromString(ReadOnlySpan<byte> utf8) =>
        FromLengthDelimited(0x3A, utf8);

    /// <summary>
    /// Field 8, wire type 2 (length-delimited). Tag 0x42 + varint length + bytes.
    /// </summary>
    public static byte[] FromBytes(ReadOnlySpan<byte> data) =>
        FromLengthDelimited(0x42, data);

    private static byte[] FromLengthDelimited(byte tag, ReadOnlySpan<byte> payload)
    {
        Span<byte> head = stackalloc byte[1 + 10];
        head[0] = tag;
        int headLen = 1 + Varint.WriteUnsigned(head.Slice(1), (ulong)payload.Length);
        var result = new byte[headLen + payload.Length];
        head.Slice(0, headLen).CopyTo(result);
        payload.CopyTo(result.AsSpan(headLen));
        return result;
    }

    private static uint FloatBits(float f)
    {
#if NET6_0_OR_GREATER
        return BitConverter.SingleToUInt32Bits(f);
#else
        // BitConverter.SingleToUInt32Bits is .NET 6+; use the safe two-step
        // path on netstandard2.0.
        return (uint)BitConverter.ToInt32(BitConverter.GetBytes(f), 0);
#endif
    }

    private static ulong DoubleBits(double d)
    {
#if NET6_0_OR_GREATER
        return BitConverter.DoubleToUInt64Bits(d);
#else
        return (ulong)BitConverter.DoubleToInt64Bits(d);
#endif
    }
}
