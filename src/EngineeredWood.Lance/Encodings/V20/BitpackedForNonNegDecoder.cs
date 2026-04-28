// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a Lance v2.0 <see cref="BitpackedForNonNeg"/> ArrayEncoding via
/// the <c>Clast.FastLanes</c> package. Values are packed in 1024-element
/// chunks at a fixed <c>compressed_bits_per_value</c> across the whole
/// buffer; the last chunk is padded with zeros to 1024 elements before
/// packing.
/// </summary>
internal sealed class BitpackedForNonNegDecoder : IArrayDecoder
{
    private readonly BitpackedForNonNeg _encoding;

    public BitpackedForNonNegDecoder(BitpackedForNonNeg encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        ulong compressedBits = _encoding.CompressedBitsPerValue;
        ulong uncompressedBits = _encoding.UncompressedBitsPerValue;

        if (compressedBits == 0)
            throw new LanceFormatException("BitpackedForNonNeg compressed_bits_per_value must be > 0.");
        if (compressedBits > uncompressedBits)
            throw new LanceFormatException(
                $"BitpackedForNonNeg compressed_bits ({compressedBits}) > uncompressed_bits ({uncompressedBits}).");

        int valueBytesPerItem = uncompressedBits switch
        {
            8 => 1,
            16 => 2,
            32 => 4,
            64 => 8,
            _ => throw new NotImplementedException(
                $"BitpackedForNonNeg uncompressed_bits_per_value={uncompressedBits} is not supported (8/16/32/64 only)."),
        };

        if (targetType is FixedWidthType fw && (ulong)fw.BitWidth != uncompressedBits)
            throw new LanceFormatException(
                $"BitpackedForNonNeg uncompressed_bits={uncompressedBits} does not match target type {targetType} ({fw.BitWidth}).");

        if (_encoding.Buffer is null)
            throw new LanceFormatException("BitpackedForNonNeg encoding has no buffer reference.");

        ReadOnlyMemory<byte> packed = context.Resolve(_encoding.Buffer);

        int length = checked((int)numRows);
        int chunkSize = Clast.FastLanes.BitPacking.ElementsPerChunk;
        int numChunks = (length + chunkSize - 1) / chunkSize;
        int packedBytesPerChunk = chunkSize * (int)compressedBits / 8;

        long requiredBytes = (long)numChunks * packedBytesPerChunk;
        if (packed.Length < requiredBytes)
            throw new LanceFormatException(
                $"BitpackedForNonNeg buffer too small: need {requiredBytes} bytes for {numChunks} chunks, have {packed.Length}.");

        var output = new byte[checked(length * valueBytesPerItem)];
        UnpackAll(packed.Span, length, valueBytesPerItem, (int)compressedBits, packedBytesPerChunk, chunkSize, output);

        var data = new ArrayData(
            targetType, length,
            nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(output) });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static void UnpackAll(
        ReadOnlySpan<byte> packed, int length, int valueBytesPerItem,
        int bitWidth, int packedBytesPerChunk, int chunkSize, byte[] output)
    {
        int chunksDecoded = 0;
        int packedOffset = 0;
        int remaining = length;

        switch (valueBytesPerItem)
        {
            case 1:
                {
                    Span<byte> chunkBuf = stackalloc byte[chunkSize];
                    while (remaining > 0)
                    {
                        Clast.FastLanes.BitPacking.UnpackChunk<byte>(
                            bitWidth, packed.Slice(packedOffset, packedBytesPerChunk), chunkBuf);
                        int copy = Math.Min(remaining, chunkSize);
                        chunkBuf.Slice(0, copy).CopyTo(output.AsSpan(chunksDecoded * chunkSize, copy));
                        chunksDecoded++; packedOffset += packedBytesPerChunk; remaining -= copy;
                    }
                    break;
                }
            case 2:
                {
                    Span<ushort> chunkBuf = stackalloc ushort[chunkSize];
                    while (remaining > 0)
                    {
                        Clast.FastLanes.BitPacking.UnpackChunk<ushort>(
                            bitWidth, packed.Slice(packedOffset, packedBytesPerChunk), chunkBuf);
                        int copy = Math.Min(remaining, chunkSize);
                        System.Runtime.InteropServices.MemoryMarshal.AsBytes(chunkBuf.Slice(0, copy))
                            .CopyTo(output.AsSpan(chunksDecoded * chunkSize * 2, copy * 2));
                        chunksDecoded++; packedOffset += packedBytesPerChunk; remaining -= copy;
                    }
                    break;
                }
            case 4:
                {
                    int byteCount = chunkSize * sizeof(uint);
                    byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount);
                    try
                    {
                        Span<uint> chunkBuf = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, uint>(
                            rented.AsSpan(0, byteCount));
                        while (remaining > 0)
                        {
                            Clast.FastLanes.BitPacking.UnpackChunk<uint>(
                                bitWidth, packed.Slice(packedOffset, packedBytesPerChunk), chunkBuf);
                            int copy = Math.Min(remaining, chunkSize);
                            System.Runtime.InteropServices.MemoryMarshal.AsBytes(chunkBuf.Slice(0, copy))
                                .CopyTo(output.AsSpan(chunksDecoded * chunkSize * 4, copy * 4));
                            chunksDecoded++; packedOffset += packedBytesPerChunk; remaining -= copy;
                        }
                    }
                    finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
                    break;
                }
            case 8:
                {
                    int byteCount = chunkSize * sizeof(ulong);
                    byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount);
                    try
                    {
                        Span<ulong> chunkBuf = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(
                            rented.AsSpan(0, byteCount));
                        while (remaining > 0)
                        {
                            Clast.FastLanes.BitPacking.UnpackChunk<ulong>(
                                bitWidth, packed.Slice(packedOffset, packedBytesPerChunk), chunkBuf);
                            int copy = Math.Min(remaining, chunkSize);
                            System.Runtime.InteropServices.MemoryMarshal.AsBytes(chunkBuf.Slice(0, copy))
                                .CopyTo(output.AsSpan(chunksDecoded * chunkSize * 8, copy * 8));
                            chunksDecoded++; packedOffset += packedBytesPerChunk; remaining -= copy;
                        }
                    }
                    finally { System.Buffers.ArrayPool<byte>.Shared.Return(rented); }
                    break;
                }
        }
    }
}
