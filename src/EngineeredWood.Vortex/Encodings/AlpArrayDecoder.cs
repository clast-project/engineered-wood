// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.alp</c>: ALP (Adaptive Lossless floating Point)
/// compression. Encoding scales each float by <c>10^e * 10^-f</c> at encode
/// time, rounds to a small integer, and stores the integers in a child array
/// (typically <c>fastlanes.bitpacked</c>). Decoding inverts:
/// <c>decoded[i] = encoded[i] * 10^f * 10^-e</c>.
///
/// <para>Wire format (per <c>encodings/alp/src/alp/array.rs</c>):
/// <list type="bullet">
///   <item>0 buffers</item>
///   <item>1+ children: encoded (i32 for f32, i64 for f64), optional patches</item>
///   <item>Metadata proto <c>ALPMetadata { exp_e: u32, exp_f: u32, patches }</c></item>
/// </list></para>
///
/// <para>Phase 1 scope: f64 only, no patches. Add f32 + patches when fixtures need them.</para>
/// </summary>
internal static class AlpArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.alp expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount < 1)
            throw new VortexFormatException(
                $"vortex.alp expects at least 1 child (encoded ints), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var meta = ParseAlpMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        // Decode the encoded integer child. f32 → i32, f64 → i64.
        var (encodedArrowType, isF64) = expectedType switch
        {
            FloatType => ((IArrowType)Int32Type.Default, false),
            DoubleType => (Int64Type.Default, true),
            _ => throw new NotSupportedException(
                $"vortex.alp only supports FloatType or DoubleType, got {expectedType}."),
        };

        var encoded = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, encodedArrowType, expectedRowCount);

        // The encoded child may carry a validity bitmap. Reuse it on the
        // float output — null rows pass through unchanged.
        var (nullBuffer, nullCount) = ExtractValidity(encoded);

        var rowCount = checked((int)expectedRowCount);
        var output = isF64
            ? DecodeF64((Int64Array)encoded, rowCount, meta.ExpE, meta.ExpF, nullBuffer, nullCount)
            : DecodeF32((Int32Array)encoded, rowCount, meta.ExpE, meta.ExpF, nullBuffer, nullCount);

        // Apply patches (indices + values overwrite the dict-decoded floats).
        if (meta.HasPatches)
        {
            var indicesType = PtypeIntToArrowType(meta.PatchIndicesPtype);
            var patchIndices = ArrayDecoder.DecodeNode(
                node.Child(1), serialized, arraySpecs, indicesType, (long)meta.PatchesLen);
            var patchValues = ArrayDecoder.DecodeNode(
                node.Child(2), serialized, arraySpecs, expectedType, (long)meta.PatchesLen);
            ApplyPatches(output, patchIndices, patchValues, (int)meta.PatchesOffset, isF64);
        }

        return output;
    }

    private static void ApplyPatches(
        IArrowArray output, IArrowArray indices, IArrowArray values, int patchesOffset, bool isF64)
    {
        var data = ((Apache.Arrow.Array)output).Data;
        var valueBytes = data.Buffers[1];
        // ArrowBuffer wraps an immutable IMemoryOwner; we materialized this from
        // a fresh byte[] in DecodeF32/F64, so we can mutate via the underlying memory.
        var bytes = valueBytes.Memory.ToArray(); // copy out
        if (isF64)
        {
            var doubles = MemoryMarshal.Cast<byte, double>(bytes.AsSpan());
            var srcArr = (DoubleArray)values;
            for (int k = 0; k < indices.Length; k++)
            {
                var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
                doubles[rowIdx] = srcArr.GetValue(k)!.Value;
            }
        }
        else
        {
            var floats = MemoryMarshal.Cast<byte, float>(bytes.AsSpan());
            var srcArr = (FloatArray)values;
            for (int k = 0; k < indices.Length; k++)
            {
                var rowIdx = GetIntAtIndex(indices, k) - patchesOffset;
                floats[rowIdx] = srcArr.GetValue(k)!.Value;
            }
        }
        // Replace the values buffer with the patched copy.
        data.Buffers[1] = new ArrowBuffer(bytes);
    }

    private static int GetIntAtIndex(IArrowArray array, int i) => array switch
    {
        UInt8Array u8 => u8.GetValue(i)!.Value,
        UInt16Array u16 => u16.GetValue(i)!.Value,
        UInt32Array u32 => checked((int)u32.GetValue(i)!.Value),
        UInt64Array u64 => checked((int)u64.GetValue(i)!.Value),
        Int8Array i8 => i8.GetValue(i)!.Value,
        Int16Array i16 => i16.GetValue(i)!.Value,
        Int32Array i32 => i32.GetValue(i)!.Value,
        Int64Array i64 => checked((int)i64.GetValue(i)!.Value),
        _ => throw new VortexFormatException(
            $"vortex.alp patch indices type {array.GetType().Name} not supported."),
    };

    private static IArrowType PtypeIntToArrowType(int ptype) => ptype switch
    {
        0 => UInt8Type.Default,
        1 => UInt16Type.Default,
        2 => UInt32Type.Default,
        3 => UInt64Type.Default,
        4 => Int8Type.Default,
        5 => Int16Type.Default,
        6 => Int32Type.Default,
        7 => Int64Type.Default,
        _ => throw new VortexFormatException($"Unsupported ptype {ptype}."),
    };

    private static (ArrowBuffer NullBuffer, int NullCount) ExtractValidity(IArrowArray array)
    {
        // Apache.Arrow exposes the null bitmap on the array's underlying ArrayData.
        // For our purposes we use the array's NullCount + ArrowBuffer accessor.
        var data = (array as Apache.Arrow.Array)?.Data;
        if (data is null || data.NullCount == 0 || data.Buffers.Length == 0)
            return (ArrowBuffer.Empty, 0);
        return (data.Buffers[0], data.NullCount);
    }

    // F10[k] = 10^k. Indices 0..18 for f64, 0..10 for f32 (matches upstream).
    private static readonly double[] F10D =
    {
        1d, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
        1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
    };

    // IF10[k] = 10^-k.
    private static readonly double[] IF10D =
    {
        1d, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
        1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18,
    };

    private static IArrowArray DecodeF64(
        Int64Array encoded, int rowCount, int expE, int expF, ArrowBuffer nullBuffer, int nullCount)
    {
        if (expE >= F10D.Length || expF >= F10D.Length)
            throw new VortexFormatException(
                $"vortex.alp: exp_e={expE} or exp_f={expF} out of range for f64 (max {F10D.Length - 1}).");
        double f = F10D[expF];
        double ie = IF10D[expE];

        var bytes = new byte[(long)rowCount * sizeof(double)];
        var span = MemoryMarshal.Cast<byte, double>(bytes.AsSpan());
        // Read raw integer values directly from the buffer (skip the GetValue
        // null check — null rows still have valid bit patterns we'd compute,
        // but the output's null bitmap masks them out).
        var encodedData = (encoded as Apache.Arrow.Array)?.Data!;
        var rawInts = MemoryMarshal.Cast<byte, long>(encodedData.Buffers[1].Span);
        for (int i = 0; i < rowCount; i++)
            span[i] = rawInts[i] * f * ie;

        return new DoubleArray(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    private static IArrowArray DecodeF32(
        Int32Array encoded, int rowCount, int expE, int expF, ArrowBuffer nullBuffer, int nullCount)
    {
        if (expE >= F10D.Length || expF >= F10D.Length)
            throw new VortexFormatException(
                $"vortex.alp: exp_e={expE} or exp_f={expF} out of range for f32.");
        double f = F10D[expF];
        double ie = IF10D[expE];

        var bytes = new byte[(long)rowCount * sizeof(float)];
        var span = MemoryMarshal.Cast<byte, float>(bytes.AsSpan());
        var encodedData = (encoded as Apache.Arrow.Array)?.Data!;
        var rawInts = MemoryMarshal.Cast<byte, int>(encodedData.Buffers[1].Span);
        for (int i = 0; i < rowCount; i++)
            span[i] = (float)(rawInts[i] * f * ie);

        return new FloatArray(new ArrowBuffer(bytes), nullBuffer, rowCount, nullCount, 0);
    }

    internal readonly struct AlpMeta
    {
        public int ExpE { get; init; }
        public int ExpF { get; init; }
        public bool HasPatches { get; init; }
        public ulong PatchesLen { get; init; }
        public ulong PatchesOffset { get; init; }
        public int PatchIndicesPtype { get; init; }
    }

    /// <summary>
    /// Parses <c>ALPMetadata</c> proto:
    ///   field 1 (varint): exp_e (u32)
    ///   field 2 (varint): exp_f (u32)
    ///   field 3 (length-delim, optional): patches (PatchesMetadata embedded)
    /// </summary>
    private static AlpMeta ParseAlpMetadata(ReadOnlySpan<byte> bytes)
    {
        int expE = 0, expF = 0;
        bool hasPatches = false;
        ulong patchesLen = 0, patchesOffset = 0;
        int patchIndicesPtype = 2;

        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
                expE = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0)
                expF = (int)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 2)
            {
                hasPatches = true;
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                ParsePatchesMetadata(bytes.Slice(pos, len), out patchesLen, out patchesOffset, out patchIndicesPtype);
                pos += len;
            }
            else SkipField(bytes, ref pos, wireType);
        }
        return new AlpMeta
        {
            ExpE = expE, ExpF = expF, HasPatches = hasPatches,
            PatchesLen = patchesLen, PatchesOffset = patchesOffset,
            PatchIndicesPtype = patchIndicesPtype,
        };
    }

    private static void ParsePatchesMetadata(
        ReadOnlySpan<byte> bytes, out ulong len, out ulong offset, out int indicesPtype)
    {
        len = 0; offset = 0; indicesPtype = 2;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (uint)Varint.ReadUnsigned(bytes, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0) len = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 2 && wireType == 0) offset = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            else if (fieldNum == 3 && wireType == 0) indicesPtype = (int)Varint.ReadUnsigned(bytes, ref pos);
            else SkipField(bytes, ref pos, wireType);
        }
    }

    private static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, uint wireType)
    {
        switch (wireType)
        {
            case 0: Varint.ReadUnsigned(bytes, ref pos); break;
            case 1: pos += 8; break;
            case 2:
                var len = (int)Varint.ReadUnsigned(bytes, ref pos);
                pos += len;
                break;
            case 5: pos += 4; break;
            default:
                throw new VortexFormatException(
                    $"Unsupported protobuf wire type {wireType} in ALPMetadata.");
        }
    }
}
