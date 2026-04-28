// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Encodings;
using EngineeredWood.Lance.Encodings.V20;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Tests.Encodings;

public class BitpackedDecoderTests
{
    [Fact]
    public void Unsigned_3Bit_To_UInt32()
    {
        ulong[] values = { 1, 2, 3, 4, 5, 6, 7, 0 };
        byte[] packed = PackLsb(values, bitsPerValue: 3);

        var arr = (UInt32Array)Decode(
            packed, compressedBits: 3, uncompressedBits: 32, signed: false,
            numRows: values.Length, targetType: UInt32Type.Default);

        Assert.Equal(new uint?[] { 1, 2, 3, 4, 5, 6, 7, 0 }, arr.ToArray());
    }

    [Fact]
    public void Signed_4Bit_To_Int32_SignExtends()
    {
        // 4-bit two's-complement values.
        int[] expected = { -8, -1, 0, 7, -4, 2, -1 };
        ulong[] packed = new ulong[expected.Length];
        for (int i = 0; i < expected.Length; i++)
            packed[i] = (ulong)(expected[i] & 0xF); // low 4 bits

        byte[] bytes = PackLsb(packed, bitsPerValue: 4);

        var arr = (Int32Array)Decode(
            bytes, compressedBits: 4, uncompressedBits: 32, signed: true,
            numRows: expected.Length, targetType: Int32Type.Default);

        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], arr.GetValue(i));
    }

    [Fact]
    public void Unsigned_Width_Equal_To_Uncompressed_Is_Passthrough()
    {
        // 8-bit packed into uint8 is just a byte copy.
        byte[] bytes = { 0, 1, 2, 255, 128 };
        var arr = (UInt8Array)Decode(
            bytes, compressedBits: 8, uncompressedBits: 8, signed: false,
            numRows: bytes.Length, targetType: UInt8Type.Default);

        Assert.Equal(new byte?[] { 0, 1, 2, 255, 128 }, arr.ToArray());
    }

    [Fact]
    public void Signed_16Bit_Full_Width_Negatives()
    {
        short[] expected = { -1, -32768, 0, 32767, -100 };
        ulong[] packed = new ulong[expected.Length];
        for (int i = 0; i < expected.Length; i++)
            packed[i] = (ulong)(ushort)expected[i];

        byte[] bytes = PackLsb(packed, bitsPerValue: 16);

        var arr = (Int16Array)Decode(
            bytes, compressedBits: 16, uncompressedBits: 16, signed: true,
            numRows: expected.Length, targetType: Int16Type.Default);

        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], arr.GetValue(i));
    }

    [Fact]
    public void Width_Mismatch_With_Target_Throws()
    {
        // Target Int16 (16 bits) doesn't match encoding's uncompressed=32.
        Assert.Throws<LanceFormatException>(
            () => Decode(new byte[4],
                compressedBits: 4, uncompressedBits: 32, signed: false,
                numRows: 1, targetType: Int16Type.Default));
    }

    [Fact]
    public void Compressed_Bits_Greater_Than_Uncompressed_Throws()
    {
        Assert.Throws<LanceFormatException>(
            () => Decode(new byte[16],
                compressedBits: 40, uncompressedBits: 32, signed: false,
                numRows: 1, targetType: Int32Type.Default));
    }

    [Fact]
    public void Buffer_Too_Small_Throws()
    {
        // Claim 10 rows of 8-bit packed but only provide 5 bytes.
        Assert.Throws<LanceFormatException>(
            () => Decode(new byte[5],
                compressedBits: 8, uncompressedBits: 8, signed: false,
                numRows: 10, targetType: UInt8Type.Default));
    }

    private static IArrowArray Decode(
        byte[] buffer, int compressedBits, int uncompressedBits, bool signed,
        int numRows, IArrowType targetType)
    {
        var encoding = MakeEncoding(compressedBits, uncompressedBits, signed, buffer);
        var decoder = new BitpackedDecoder(encoding);
        var ctx = new PageContext(new[] { (ReadOnlyMemory<byte>)buffer });
        return decoder.Decode(numRows, targetType, ctx);
    }

    private static Bitpacked MakeEncoding(
        int compressedBits, int uncompressedBits, bool signed, byte[] buffer) =>
        new Bitpacked
        {
            CompressedBitsPerValue = (ulong)compressedBits,
            UncompressedBitsPerValue = (ulong)uncompressedBits,
            Signed = signed,
            Buffer = new Proto.Encodings.V20.Buffer
            {
                BufferIndex = 0,
                BufferType = Proto.Encodings.V20.Buffer.Types.BufferType.Page,
            },
        };

    /// <summary>
    /// Produce the LSB-first bit-packed wire layout Lance's simple
    /// <see cref="Bitpacked"/> encoding expects. Each input value contributes
    /// <paramref name="bitsPerValue"/> bits starting at bit 0 of byte 0.
    /// </summary>
    internal static byte[] PackLsb(ulong[] values, int bitsPerValue)
    {
        long totalBits = (long)values.Length * bitsPerValue;
        int totalBytes = checked((int)((totalBits + 7) / 8));
        var output = new byte[totalBytes];

        long bitPos = 0;
        foreach (ulong v in values)
        {
            for (int b = 0; b < bitsPerValue; b++)
            {
                if (((v >> b) & 1UL) != 0)
                    output[bitPos >> 3] |= (byte)(1 << ((int)bitPos & 7));
                bitPos++;
            }
        }
        return output;
    }
}
