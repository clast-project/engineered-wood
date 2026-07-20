// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for the <c>vortex.sequence</c> array encoding, which represents
/// arithmetic sequences as <c>A[i] = base + i * multiplier</c>. The base and
/// multiplier are carried in the ArrayNode's metadata as a protobuf-encoded
/// <c>SequenceMetadata { ScalarValue base = 1; ScalarValue multiplier = 2; }</c>
/// (see <c>encodings/sequence/src/array.rs</c> upstream).
///
/// <para>Phase 1 scope: integer sequences encoded as ScalarValue.int64_value
/// (the only path our fixtures hit). Float / bool / etc. variants land alongside
/// fixtures that need them.</para>
/// </summary>
internal static class SequenceArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node, IArrowType expectedType, long expectedRowCount)
    {
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.sequence ArrayNode should have no buffer refs, got {node.BufferRefCount}.");
        if (node.ChildCount != 0)
            throw new VortexFormatException(
                $"vortex.sequence ArrayNode should have no children, got {node.ChildCount}.");

        var metaVec = node.Metadata;
        if (metaVec.Length == 0)
            throw new VortexFormatException(
                "vortex.sequence ArrayNode has empty metadata; expected a SequenceMetadata proto.");

        var metaBytes = metaVec.RawBytes(metaVec.Length);
        var (baseVal, multiplier) = ParseSequenceMetadata(metaBytes);
        return BuildArray(expectedType, expectedRowCount, baseVal, multiplier);
    }

    /// <summary>
    /// Parses <c>SequenceMetadata { ScalarValue base = 1; ScalarValue multiplier = 2; }</c>.
    /// </summary>
    private static (long Base, long Multiplier) ParseSequenceMetadata(ReadOnlySpan<byte> bytes)
    {
        long? baseVal = null;
        long? multiplier = null;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            switch (fieldNum)
            {
                case 1 when wireType == 2:
                    {
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        var sv = ScalarValueProto.Parse(bytes.Slice(pos, len));
                        if (sv.Kind != ScalarValueKind.Int64)
                            throw new NotSupportedException(
                                $"vortex.sequence base is {sv.Kind}, only Int64 is supported.");
                        baseVal = sv.Int64Value;
                        pos += len;
                        break;
                    }
                case 2 when wireType == 2:
                    {
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        var sv = ScalarValueProto.Parse(bytes.Slice(pos, len));
                        if (sv.Kind != ScalarValueKind.Int64)
                            throw new NotSupportedException(
                                $"vortex.sequence multiplier is {sv.Kind}, only Int64 is supported.");
                        multiplier = sv.Int64Value;
                        pos += len;
                        break;
                    }
                default:
                    SkipField(bytes, ref pos, wireType);
                    break;
            }
        }
        if (baseVal is null || multiplier is null)
            throw new VortexFormatException(
                "vortex.sequence metadata is missing base or multiplier.");
        return (baseVal.Value, multiplier.Value);
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
                    $"Unsupported protobuf wire type {wireType} in vortex.sequence metadata.");
        }
    }

    private static IArrowArray BuildArray(
        IArrowType type, long rowCount, long baseVal, long multiplier)
    {
        var n = checked((int)rowCount);
        return type switch
        {
            Int8Type => Build<sbyte>(n, sizeof(sbyte),
                (data, len) => new Int8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (sbyte)baseVal, (sbyte)multiplier),
            Int16Type => Build<short>(n, sizeof(short),
                (data, len) => new Int16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (short)baseVal, (short)multiplier),
            Int32Type => Build<int>(n, sizeof(int),
                (data, len) => new Int32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (int)baseVal, (int)multiplier),
            Int64Type => Build<long>(n, sizeof(long),
                (data, len) => new Int64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                baseVal, multiplier),
            UInt8Type => Build<byte>(n, sizeof(byte),
                (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (byte)baseVal, (byte)multiplier),
            UInt16Type => Build<ushort>(n, sizeof(ushort),
                (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (ushort)baseVal, (ushort)multiplier),
            UInt32Type => Build<uint>(n, sizeof(uint),
                (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (uint)baseVal, (uint)multiplier),
            UInt64Type => Build<ulong>(n, sizeof(ulong),
                (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0),
                (ulong)baseVal, (ulong)multiplier),
            _ => throw new NotSupportedException(
                $"vortex.sequence decoder does not yet support Arrow type {type}."),
        };
    }

    private static IArrowArray Build<T>(
        int rowCount, int elementSize,
        Func<byte[], int, IArrowArray> ctor,
        T baseVal, T multiplier)
        where T : struct
    {
        var bytes = new byte[(long)rowCount * elementSize];
        var span = MemoryMarshal.Cast<byte, T>(bytes.AsSpan());
        Fill(span, baseVal, multiplier);
        return ctor(bytes, rowCount);
    }

    private static void Fill<T>(Span<T> span, T baseVal, T multiplier) where T : struct
    {
        // T is constrained to numeric types via the dispatch above. Use dynamic
        // dispatch through the runtime's checked-arithmetic op_Addition; this is
        // fine for warm-up / small sequences. A perf path would specialize.
        if (typeof(T) == typeof(int))
        {
            var s = MemoryMarshal.Cast<T, int>(span);
            int b = (int)(object)baseVal!, m = (int)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = b + i * m;
        }
        else if (typeof(T) == typeof(long))
        {
            var s = MemoryMarshal.Cast<T, long>(span);
            long b = (long)(object)baseVal!, m = (long)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = b + (long)i * m;
        }
        else if (typeof(T) == typeof(short))
        {
            var s = MemoryMarshal.Cast<T, short>(span);
            short b = (short)(object)baseVal!, m = (short)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = (short)(b + i * m);
        }
        else if (typeof(T) == typeof(sbyte))
        {
            var s = MemoryMarshal.Cast<T, sbyte>(span);
            sbyte b = (sbyte)(object)baseVal!, m = (sbyte)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = (sbyte)(b + i * m);
        }
        else if (typeof(T) == typeof(uint))
        {
            var s = MemoryMarshal.Cast<T, uint>(span);
            uint b = (uint)(object)baseVal!, m = (uint)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = b + (uint)i * m;
        }
        else if (typeof(T) == typeof(ulong))
        {
            var s = MemoryMarshal.Cast<T, ulong>(span);
            ulong b = (ulong)(object)baseVal!, m = (ulong)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = b + (ulong)i * m;
        }
        else if (typeof(T) == typeof(ushort))
        {
            var s = MemoryMarshal.Cast<T, ushort>(span);
            ushort b = (ushort)(object)baseVal!, m = (ushort)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = (ushort)(b + i * m);
        }
        else if (typeof(T) == typeof(byte))
        {
            var s = MemoryMarshal.Cast<T, byte>(span);
            byte b = (byte)(object)baseVal!, m = (byte)(object)multiplier!;
            for (int i = 0; i < s.Length; i++) s[i] = (byte)(b + i * m);
        }
        else
        {
            throw new InvalidOperationException(
                $"Sequence Fill not implemented for {typeof(T)}.");
        }
    }
}
