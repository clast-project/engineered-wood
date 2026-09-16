// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.zstd</c> (edition <c>core2025.06.0</c>): Zstd-compressed primitive,
/// string and binary arrays, which upstream's "compact" compressor
/// (<c>BtrBlocksCompressorBuilder::with_compact</c>) writes for strings and binary. Wire format
/// (per <c>encodings/zstd/src/{array,lib}.rs</c>):
/// <list type="bullet">
///   <item>buffers: the shared Zstd dictionary when <c>dictionary_size</c> is non-zero, then
///     one buffer per frame</item>
///   <item>0-1 children: the validity (bool) of every row</item>
///   <item>metadata: protobuf <c>ZstdMetadata { 1: uint32 dictionary_size, 2: repeated
///     ZstdFrameMetadata { 1: uint64 uncompressed_size, 2: uint64 n_values } }</c></item>
/// </list>
///
/// <para>Only valid values are compressed, concatenated across frames in row order: primitive
/// values as their little-endian bytes, strings and binary as a u32 little-endian length followed
/// by the bytes. Frames split at value boundaries, and each is a standalone Zstd frame (compressed
/// with the dictionary, when there is one) whose header declares its content size. The decoder
/// decompresses every frame and scatters the values over the valid rows.</para>
/// </summary>
internal static class ZstdArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (node.ChildCount > 1)
            throw new VortexFormatException(
                $"vortex.zstd expects 0 or 1 children (validity), got {node.ChildCount}.");

        var metaVec = node.Metadata;
        var (dictionarySize, frames) = ParseMetadata(metaVec.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : metaVec.RawBytes(metaVec.Length));

        int firstFrame = dictionarySize == 0 ? 0 : 1;
        if (node.BufferRefCount != firstFrame + frames.Count)
            throw new VortexFormatException(
                $"vortex.zstd has {node.BufferRefCount} buffers; its metadata describes " +
                $"{firstFrame + frames.Count} ({firstFrame} dictionary, {frames.Count} frames).");
        for (int i = 0; i < node.BufferRefCount; i++)
        {
            var desc = serialized.Message.Buffer(node.BufferRef(i));
            if (desc.Compression != BufferCompression.None)
                throw new NotSupportedException(
                    $"vortex.zstd buffer {i} has buffer compression {desc.Compression}, which the format reserves but no writer produces.");
        }

        int rowCount = checked((int)expectedRowCount);
        ArrowBuffer validity = ArrowBuffer.Empty;
        int nullCount = 0;
        if (node.ChildCount == 1)
        {
            validity = BoolArrayDecoder.ReadBitmap(node.Child(0), serialized, expectedRowCount);
            nullCount = BoolArrayDecoder.CountNulls(validity.Span, rowCount);
            if (nullCount == 0)
                validity = ArrowBuffer.Empty;
        }
        int validCount = rowCount - nullCount;

        long valueCount = 0;
        foreach (var frame in frames)
            valueCount += checked((long)frame.ValueCount);
        if (valueCount != validCount)
            throw new VortexFormatException(
                $"vortex.zstd frames hold {valueCount} values, but {validCount} of {rowCount} rows are valid.");

        byte[]? dictionary = dictionarySize == 0
            ? null
            : serialized.BufferBytes(node.BufferRef(0)).ToArray();
        if (dictionary is not null && dictionary.Length != dictionarySize)
            throw new VortexFormatException(
                $"vortex.zstd dictionary is {dictionary.Length} bytes; metadata says {dictionarySize}.");

        var values = Decompress(node, serialized, firstFrame, frames, dictionary);

        return expectedType switch
        {
            StringType or BinaryType => BuildVarBin(expectedType, values, validity, nullCount, rowCount, validCount),
            _ => BuildPrimitive(expectedType, values, validity, nullCount, rowCount, validCount),
        };
    }

    /// <summary>Decompresses every frame, in order, into one buffer of valid values.</summary>
    private static byte[] Decompress(
        ArrayNode node, SerializedArray serialized, int firstFrame,
        IReadOnlyList<Frame> frames, byte[]? dictionary)
    {
        long total = 0;
        foreach (var frame in frames)
            total += checked((long)frame.UncompressedSize);
        if (total > int.MaxValue)
            throw new NotSupportedException(
                $"vortex.zstd array decompresses to {total} bytes, more than this reader can hold in one buffer.");

        var output = new byte[total];
        using var decompressor = new ZstdSharp.Decompressor();
        if (dictionary is not null)
            decompressor.LoadDictionary(dictionary);

        int written = 0;
        for (int i = 0; i < frames.Count; i++)
        {
            var compressed = serialized.BufferBytes(node.BufferRef(firstFrame + i));
            int expected = (int)frames[i].UncompressedSize;
            var declared = ZstdSharp.Decompressor.GetDecompressedSize(compressed);
            if (declared != (ulong)expected)
                throw new VortexFormatException(
                    $"vortex.zstd frame {i} header declares {declared} bytes; metadata says {expected}.");
            int n;
            try
            {
                n = decompressor.Unwrap(compressed, output.AsSpan(written, expected));
            }
            catch (ZstdSharp.ZstdException e)
            {
                throw new VortexFormatException($"vortex.zstd frame {i} failed to decompress: {e.Message}", e);
            }
            if (n != expected)
                throw new VortexFormatException(
                    $"vortex.zstd frame {i} decompressed to {n} bytes; metadata says {expected}.");
            written += n;
        }
        return output;
    }

    private static IArrowArray BuildPrimitive(
        IArrowType type, byte[] values, ArrowBuffer validity, int nullCount, int rowCount, int validCount)
    {
        int width = type switch
        {
            Int8Type or UInt8Type => 1,
#if NET6_0_OR_GREATER
            HalfFloatType => 2,
#else
            HalfFloatType => throw new NotSupportedException(
                "HalfFloat (F16) decode requires System.Half (net6+); netstandard2.0 builds "
                + "of Apache.Arrow don't ship HalfFloatArray."),
#endif
            Int16Type or UInt16Type => 2,
            Int32Type or UInt32Type or FloatType => 4,
            Int64Type or UInt64Type or DoubleType => 8,
            _ => throw new NotSupportedException($"vortex.zstd decoder does not support Arrow type {type}."),
        };
        if (values.Length != (long)validCount * width)
            throw new VortexFormatException(
                $"vortex.zstd holds {values.Length} bytes for {validCount} values of {width} bytes.");

        byte[] data;
        if (nullCount == 0)
        {
            data = values;
        }
        else
        {
            // Scatter the valid values; null rows stay zero.
            data = new byte[(long)rowCount * width];
            var bits = validity.Span;
            int src = 0;
            for (int row = 0; row < rowCount; row++)
            {
                if ((bits[row >> 3] & (1 << (row & 7))) == 0)
                    continue;
                Buffer.BlockCopy(values, src, data, row * width, width);
                src += width;
            }
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(
            type, rowCount, nullCount, 0, new[] { validity, new ArrowBuffer(data) }));
    }

    internal static IArrowArray BuildVarBin(
        IArrowType type, byte[] values, ArrowBuffer validity, int nullCount, int rowCount, int validCount)
    {
        // Values are length-prefixed and back to back, so the payload bytes only need the
        // prefixes squeezed out; null rows repeat the previous offset.
        var offsets = new byte[((long)rowCount + 1) * 4];
        var data = new byte[values.Length - Math.Min(values.Length, (long)validCount * 4)];
        var bits = validity.Span;
        int pos = 0, end = 0, seen = 0;
        for (int row = 0; row < rowCount; row++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(row * 4), end);
            if (nullCount != 0 && (bits[row >> 3] & (1 << (row & 7))) == 0)
                continue;
            if (values.Length - pos < 4)
                throw new VortexFormatException($"vortex.zstd value {seen} has no length prefix.");
            uint len = BinaryPrimitives.ReadUInt32LittleEndian(values.AsSpan(pos));
            pos += 4;
            // The output holds the payload bytes once every prefix is removed, so a length that
            // doesn't leave room for the remaining prefixes is corrupt too.
            long room = Math.Min(values.Length - pos, data.Length - end);
            if (len > room)
                throw new VortexFormatException(
                    $"vortex.zstd value {seen} is {len} bytes, more than the decompressed data holds.");
            Buffer.BlockCopy(values, pos, data, end, (int)len);
            pos += (int)len;
            end += (int)len;
            seen++;
        }
        BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(rowCount * 4), end);
        if (pos != values.Length)
            throw new VortexFormatException(
                $"vortex.zstd has {values.Length - pos} bytes left after its {validCount} values.");

        var offsetsBuf = new ArrowBuffer(offsets);
        var dataBuf = new ArrowBuffer(data);
        return type is StringType
            ? new StringArray(rowCount, offsetsBuf, dataBuf, validity, nullCount, offset: 0)
            : new BinaryArray(BinaryType.Default, rowCount, offsetsBuf, dataBuf, validity, nullCount, offset: 0);
    }

    private readonly record struct Frame(ulong UncompressedSize, ulong ValueCount);

    private static (uint DictionarySize, List<Frame> Frames) ParseMetadata(ReadOnlySpan<byte> bytes)
    {
        uint dictionarySize = 0;
        var frames = new List<Frame>();
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            var field = tag >> 3;
            var wire = tag & 7;
            if (field == 1 && wire == 0)
            {
                dictionarySize = checked((uint)(ulong)Varint.ReadUnsigned(bytes, ref pos));
            }
            else if (field == 2 && wire == 2)
            {
                int len = checked((int)Varint.ReadUnsigned(bytes, ref pos));
                frames.Add(ParseFrame(bytes.Slice(pos, len)));
                pos += len;
            }
            else
            {
                Skip(bytes, ref pos, wire);
            }
        }
        return (dictionarySize, frames);
    }

    private static Frame ParseFrame(ReadOnlySpan<byte> bytes)
    {
        ulong size = 0, count = 0;
        int pos = 0;
        while (pos < bytes.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            if ((tag & 7) != 0)
            {
                Skip(bytes, ref pos, tag & 7);
                continue;
            }
            var value = (ulong)Varint.ReadUnsigned(bytes, ref pos);
            if (tag >> 3 == 1) size = value;
            else if (tag >> 3 == 2) count = value;
        }
        return new Frame(size, count);
    }

    private static void Skip(ReadOnlySpan<byte> bytes, ref int pos, ulong wire) =>
        ProtobufWire.SkipField(bytes, ref pos, wire, "vortex.zstd metadata");
}
