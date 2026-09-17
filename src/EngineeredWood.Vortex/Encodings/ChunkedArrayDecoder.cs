// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.chunked</c> used as an array encoding (as opposed to the layout of the
/// same name): one array made of consecutive chunks of the same dtype, as upstream's flat layout
/// strategy writes a multi-chunk column into a single segment.
///
/// <para>Wire format (<c>vortex-array/src/arrays/chunked/vtable</c>): no buffers, empty metadata,
/// and children <c>[chunk_offsets, chunk_0, …, chunk_n-1]</c>, where <c>chunk_offsets</c> is a
/// non-nullable u64 array of n + 1 row offsets starting at 0. Nulls live in the chunks; the
/// chunked array has no validity of its own.</para>
/// </summary>
internal static class ChunkedArrayDecoder
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
                $"vortex.chunked expects 0 buffers, got {node.BufferRefCount}.");
        if (node.Metadata.Length != 0)
            throw new VortexFormatException(
                $"vortex.chunked expects empty metadata, got {node.Metadata.Length} bytes.");
        if (node.ChildCount < 1)
            throw new VortexFormatException("vortex.chunked has no chunk_offsets child.");

        int chunkCount = node.ChildCount - 1;
        var offsets = (UInt64Array)ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, UInt64Type.Default, chunkCount + 1);
        if (offsets.GetValue(0) != 0 || offsets.GetValue(chunkCount) != (ulong)expectedRowCount)
            throw new VortexFormatException(
                $"vortex.chunked offsets span [{offsets.GetValue(0)}, {offsets.GetValue(chunkCount)}), " +
                $"but the array has {expectedRowCount} rows.");

        var chunks = new List<IArrowArray>(chunkCount);
        for (int c = 0; c < chunkCount; c++)
        {
            ulong start = offsets.GetValue(c)!.Value, end = offsets.GetValue(c + 1)!.Value;
            if (end < start)
                throw new VortexFormatException(
                    $"vortex.chunked offsets decrease at chunk {c} ({start} to {end}).");
            if (end == start)
                continue;
            chunks.Add(ArrayDecoder.DecodeNode(
                node.Child(1 + c), serialized, arraySpecs, expectedType, checked((long)(end - start))));
        }

        return chunks.Count switch
        {
            // No chunk to decode (upstream writes an empty chunked array as just its offsets, [0])
            // or only empty ones: either way the result is a typed empty array.
            0 => ArrowCompute.MakeNullArray(expectedType, 0),
            1 => chunks[0],
            _ => ArrowArrayConcatenator.Concatenate(chunks),
        };
    }
}
