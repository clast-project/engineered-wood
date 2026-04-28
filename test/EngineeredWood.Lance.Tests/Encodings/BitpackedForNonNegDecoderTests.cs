// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Encodings;
using EngineeredWood.Lance.Encodings.V20;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Tests.Encodings;

/// <summary>
/// Round-trip tests for the v2.0 <see cref="BitpackedForNonNeg"/> decoder.
/// pylance does not emit this encoding for v2.0, so the test data is
/// hand-packed via <c>Clast.FastLanes.BitPacking.PackChunk</c> — exercising
/// the same Fastlanes wire format that <c>BitpackedForNonNegDecoder</c>
/// reads.
/// </summary>
public class BitpackedForNonNegDecoderTests
{
    [Fact]
    public void UInt32_SingleChunk_BitWidth7()
    {
        var values = new uint[100];
        for (int i = 0; i < values.Length; i++) values[i] = (uint)i;

        byte[] packed = PackUInt32(values, bitWidth: 7);
        var arr = (UInt32Array)Decode(packed, compressedBits: 7, uncompressedBits: 32,
                                       numRows: values.Length, targetType: UInt32Type.Default);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal((uint)i, arr.GetValue(i));
    }

    [Fact]
    public void UInt32_MultiChunk_BitWidth11()
    {
        // 1500 sequential values → 2 fastlanes chunks (last one partial).
        var values = new uint[1500];
        for (int i = 0; i < values.Length; i++) values[i] = (uint)i;

        byte[] packed = PackUInt32(values, bitWidth: 11);
        var arr = (UInt32Array)Decode(packed, compressedBits: 11, uncompressedBits: 32,
                                       numRows: values.Length, targetType: UInt32Type.Default);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal((uint)i, arr.GetValue(i));
    }

    [Fact]
    public void UInt8_SingleChunk_BitWidth4()
    {
        var values = new byte[1024];
        for (int i = 0; i < values.Length; i++) values[i] = (byte)(i % 16);

        byte[] packed = PackUInt8(values, bitWidth: 4);
        var arr = (UInt8Array)Decode(packed, compressedBits: 4, uncompressedBits: 8,
                                      numRows: values.Length, targetType: UInt8Type.Default);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal((byte)(i % 16), arr.GetValue(i));
    }

    [Fact]
    public void Compressed_Bits_Greater_Than_Uncompressed_Throws()
    {
        var encoding = MakeEncoding(compressedBits: 33, uncompressedBits: 32, buffer: new byte[16]);
        var decoder = new BitpackedForNonNegDecoder(encoding);
        var ctx = new PageContext(new[] { (ReadOnlyMemory<byte>)new byte[16] });

        Assert.Throws<LanceFormatException>(
            () => decoder.Decode(1, UInt32Type.Default, ctx));
    }

    private static IArrowArray Decode(
        byte[] buffer, int compressedBits, int uncompressedBits,
        int numRows, IArrowType targetType)
    {
        var encoding = MakeEncoding(compressedBits, uncompressedBits, buffer);
        var decoder = new BitpackedForNonNegDecoder(encoding);
        var ctx = new PageContext(new[] { (ReadOnlyMemory<byte>)buffer });
        return decoder.Decode(numRows, targetType, ctx);
    }

    private static BitpackedForNonNeg MakeEncoding(int compressedBits, int uncompressedBits, byte[] buffer) =>
        new BitpackedForNonNeg
        {
            CompressedBitsPerValue = (ulong)compressedBits,
            UncompressedBitsPerValue = (ulong)uncompressedBits,
            Buffer = new Proto.Encodings.V20.Buffer
            {
                BufferIndex = 0,
                BufferType = Proto.Encodings.V20.Buffer.Types.BufferType.Page,
            },
        };

    /// <summary>
    /// Pack <paramref name="values"/> into the Fastlanes wire format used by
    /// Lance's BitpackedForNonNeg encoding. The last chunk is zero-padded to
    /// 1024 elements before packing, matching the Lance writer's behavior.
    /// </summary>
    private static byte[] PackUInt32(uint[] values, int bitWidth)
    {
        const int chunkSize = Clast.FastLanes.BitPacking.ElementsPerChunk;
        int packedBytesPerChunk = chunkSize * bitWidth / 8;
        int numChunks = (values.Length + chunkSize - 1) / chunkSize;
        var packed = new byte[numChunks * packedBytesPerChunk];

        for (int c = 0; c < numChunks; c++)
        {
            var chunk = new uint[chunkSize];
            int offset = c * chunkSize;
            int copy = Math.Min(chunkSize, values.Length - offset);
            System.Array.Copy(values, offset, chunk, 0, copy);
            // Tail is already zero-filled.

            Clast.FastLanes.BitPacking.PackChunk<uint>(
                bitWidth, chunk,
                packed.AsSpan(c * packedBytesPerChunk, packedBytesPerChunk));
        }
        return packed;
    }

    private static byte[] PackUInt8(byte[] values, int bitWidth)
    {
        const int chunkSize = Clast.FastLanes.BitPacking.ElementsPerChunk;
        int packedBytesPerChunk = chunkSize * bitWidth / 8;
        int numChunks = (values.Length + chunkSize - 1) / chunkSize;
        var packed = new byte[numChunks * packedBytesPerChunk];

        for (int c = 0; c < numChunks; c++)
        {
            var chunk = new byte[chunkSize];
            int offset = c * chunkSize;
            int copy = Math.Min(chunkSize, values.Length - offset);
            System.Array.Copy(values, offset, chunk, 0, copy);

            Clast.FastLanes.BitPacking.PackChunk<byte>(
                bitWidth, chunk,
                packed.AsSpan(c * packedBytesPerChunk, packedBytesPerChunk));
        }
        return packed;
    }
}
