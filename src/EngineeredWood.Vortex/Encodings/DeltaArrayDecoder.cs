// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>fastlanes.delta</c>: FastLanes-style delta-encoded primitive
/// arrays. Per upstream <c>encodings/fastlanes/src/delta/array/{mod,delta_compress}.rs</c>:
/// <list type="bullet">
///   <item>0 buffers</item>
///   <item>2 children: <c>bases</c> (LANES per 1024-element chunk) and
///     <c>deltas</c> (1024 deltas per chunk, in FastLanes <em>transposed</em>
///     UTL order; possibly bit-packed)</item>
///   <item>Metadata <c>DeltaMetadata { deltas_len: u64, offset: u32 (must be &lt; 1024) }</c></item>
/// </list>
///
/// <para>Encode pipeline (vortex):
/// <c>logical → Transpose::transpose → bases = transposed[0..LANES] → Delta::delta::&lt;LANES&gt;</c>.
/// Decode is the reverse: <c>Delta::undelta_pack</c> (= <c>Clast.FastLanes.Delta.UndeltaUnpackChunk</c>)
/// reconstructs the <em>transposed</em> buffer, then we apply
/// <c>Transpose::untranspose</c> to recover logical row order.</para>
///
/// <para>The transpose function is type-INDEPENDENT (always 16 lanes × 8 orders × 8 rows = 1024):
/// <c>transpose(idx) = (idx%16)*64 + FL_ORDER[(idx/16)%8]*8 + (idx/128)</c> with
/// <c>FL_ORDER = [0,4,2,6,1,5,3,7]</c>. <c>untranspose</c> writes
/// <c>output[transpose(i)] = input[i]</c>.</para>
///
/// <para>LANES per ptype (16 for u64, 32 for u32, 64 for u16, 128 for u8) is queried via
/// <c>Delta.LaneCount&lt;T&gt;()</c>.</para>
///
/// <para>Phase 1 scope: <c>offset == 0</c> only (no slicing); deltas child is
/// fully materialized via the recursive decoder.</para>
/// </summary>
internal static class DeltaArrayDecoder
{
    private const int ElementsPerChunk = 1024;
    private static readonly int[] FL_ORDER = new int[] { 0, 4, 2, 6, 1, 5, 3, 7 };

    private static int TransposeIndex(int idx)
    {
        int lane = idx & 0xF;
        int order = (idx >> 4) & 0x7;
        int row = idx >> 7;
        return (lane << 6) | (FL_ORDER[order] << 3) | row;
    }

    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"fastlanes.delta expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 2)
            throw new VortexFormatException(
                $"fastlanes.delta expects 2 children (bases, deltas), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var (deltasLen, offset) = ParseMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));
        if (offset >= ElementsPerChunk)
            throw new VortexFormatException(
                $"fastlanes.delta offset {offset} must be < 1024 (sub-chunk slice).");

        // Decode the deltas child first so we know the actual primitive type
        // and can compute LANES from there.
        var deltas = ArrayDecoder.DecodeNode(
            node.Child(1), serialized, arraySpecs, expectedType, checked((long)deltasLen));

        // Per upstream delta::slice: when the array is sliced, both bases and
        // deltas children are pre-sliced to cover only the chunks overlapping
        // the slice range, and `offset` is the sub-chunk offset within the
        // first kept chunk. So numChunks accounts for `offset + length`.
        int rowCount = checked((int)expectedRowCount);
        int lanes = LaneCountFor(expectedType);
        int numChunks = ((int)offset + rowCount + ElementsPerChunk - 1) / ElementsPerChunk;
        long basesLen = (long)numChunks * lanes;
        var bases = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, expectedType, basesLen);

        return Undelta(expectedType, rowCount, (int)offset, lanes, bases, deltas);
    }

    private static IArrowArray Undelta(
        IArrowType type, int rowCount, int offset, int lanes, IArrowArray bases, IArrowArray deltas)
    {
        return type switch
        {
            UInt8Type => UndeltaTo<byte>(rowCount, offset, lanes, bases, deltas,
                (data, len) => new UInt8Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            UInt16Type => UndeltaTo<ushort>(rowCount, offset, lanes, bases, deltas,
                (data, len) => new UInt16Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            UInt32Type => UndeltaTo<uint>(rowCount, offset, lanes, bases, deltas,
                (data, len) => new UInt32Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            UInt64Type => UndeltaTo<ulong>(rowCount, offset, lanes, bases, deltas,
                (data, len) => new UInt64Array(new ArrowBuffer(data), ArrowBuffer.Empty, len, 0, 0)),
            _ => throw new NotSupportedException(
                $"fastlanes.delta currently supports unsigned integer types only, got {type}."),
        };
    }

    private static IArrowArray UndeltaTo<T>(
        int rowCount, int offset, int lanes, IArrowArray bases, IArrowArray deltas,
        Func<byte[], int, IArrowArray> ctor)
        where T : unmanaged
    {
        var elementSize = Marshal.SizeOf<T>();
        var dst = new byte[(long)rowCount * elementSize];
        var basesData = ((Apache.Arrow.Array)bases).Data;
        var deltasData = ((Apache.Arrow.Array)deltas).Data;
        var basesSpan = MemoryMarshal.Cast<byte, T>(basesData.Buffers[1].Span);

        // Vortex's delta encode pipeline is `Transpose::transpose → bases =
        // transposed[0..LANES] → Delta::delta::<LANES>`. Decoding inverts:
        // (1) The deltas child has already been recursively decoded; its buffer
        //     holds the per-lane successive deltas at positions `index(row, lane)`
        //     (the FastLanes iterate-macro layout) — which is exactly the input
        //     layout `Clast.FastLanes.Delta.UndeltaChunk` consumes. UndeltaChunk
        //     produces transposed values at the same `index(row, lane)` positions.
        // (2) We then untranspose into row order via the type-INDEPENDENT
        //     16×8×8 FastLanes index permutation defined by `FL_ORDER` (see
        //     `Transpose::untranspose` in fastlanes/src/transpose.rs).
        var deltasSpan = MemoryMarshal.Cast<byte, T>(deltasData.Buffers[1].Span);

        int numChunks = (offset + rowCount + ElementsPerChunk - 1) / ElementsPerChunk;
        int sliceEnd = offset + rowCount;
        var transposed = new T[ElementsPerChunk];
        var untransposed = new T[ElementsPerChunk];
        for (int c = 0; c < numChunks; c++)
        {
            ReadOnlySpan<T> baseSlice = basesSpan.Slice(c * lanes, lanes);
            ReadOnlySpan<T> deltaSlice = deltasSpan.Slice(c * ElementsPerChunk, ElementsPerChunk);
            UndeltaChunk<T>(deltaSlice, baseSlice, transposed);

            for (int i = 0; i < ElementsPerChunk; i++)
                untransposed[TransposeIndex(i)] = transposed[i];

            int chunkStart = c * ElementsPerChunk;
            int chunkEnd = chunkStart + ElementsPerChunk;
            int overlapStart = Math.Max(chunkStart, offset);
            int overlapEnd = Math.Min(chunkEnd, sliceEnd);
            if (overlapEnd <= overlapStart) continue;

            int srcStart = overlapStart - chunkStart;
            int rowsToCopy = overlapEnd - overlapStart;
            int dstStart = overlapStart - offset;
            var srcBytes = MemoryMarshal.AsBytes(untransposed.AsSpan(srcStart, rowsToCopy));
            srcBytes.CopyTo(dst.AsSpan(dstStart * elementSize));
        }
        return ctor(dst, rowCount);
    }

    private static void UndeltaChunk<T>(
        ReadOnlySpan<T> input, ReadOnlySpan<T> baseValues, Span<T> output)
        where T : unmanaged
    {
        if (typeof(T) == typeof(byte))
            Clast.FastLanes.Delta.UndeltaChunk<byte>(
                MemoryMarshal.Cast<T, byte>(input),
                MemoryMarshal.Cast<T, byte>(baseValues),
                MemoryMarshal.Cast<T, byte>(output));
        else if (typeof(T) == typeof(ushort))
            Clast.FastLanes.Delta.UndeltaChunk<ushort>(
                MemoryMarshal.Cast<T, ushort>(input),
                MemoryMarshal.Cast<T, ushort>(baseValues),
                MemoryMarshal.Cast<T, ushort>(output));
        else if (typeof(T) == typeof(uint))
            Clast.FastLanes.Delta.UndeltaChunk<uint>(
                MemoryMarshal.Cast<T, uint>(input),
                MemoryMarshal.Cast<T, uint>(baseValues),
                MemoryMarshal.Cast<T, uint>(output));
        else if (typeof(T) == typeof(ulong))
            Clast.FastLanes.Delta.UndeltaChunk<ulong>(
                MemoryMarshal.Cast<T, ulong>(input),
                MemoryMarshal.Cast<T, ulong>(baseValues),
                MemoryMarshal.Cast<T, ulong>(output));
        else
            throw new InvalidOperationException(
                $"fastlanes.delta: UndeltaChunk not implemented for {typeof(T)}.");
    }

    private static int LaneCountFor(IArrowType type) => type switch
    {
        UInt8Type => Clast.FastLanes.Delta.LaneCount<byte>(),
        UInt16Type => Clast.FastLanes.Delta.LaneCount<ushort>(),
        UInt32Type => Clast.FastLanes.Delta.LaneCount<uint>(),
        UInt64Type => Clast.FastLanes.Delta.LaneCount<ulong>(),
        _ => throw new NotSupportedException(
            $"fastlanes.delta: no lane count for {type}."),
    };

    private static (ulong DeltasLen, uint Offset) ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        ulong deltasLen = 0;
        uint offset = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                deltasLen = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                offset = (uint)Varint.ReadUnsigned(bytes, ref pos);
            else
            {
                switch (wireType)
                {
                    case 0: Varint.ReadUnsigned(bytes, ref pos); break;
                    case 1: pos += 8; break;
                    case 2:
                        var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                        pos += len; break;
                    case 5: pos += 4; break;
                    default:
                        throw new VortexFormatException(
                            $"Unsupported wire type {wireType} in DeltaMetadata.");
                }
            }
        }
        return (deltasLen, offset);
    }
}
