// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="Bitpacked"/> v2.0 ArrayEncoding — the simple
/// LSB-first bit-packing variant (as opposed to <c>BitpackedForNonNeg</c>,
/// which uses the fastlanes/1024-chunk scheme and is not yet supported).
///
/// <para>Layout: values are packed <c>compressed_bits_per_value</c> bits each,
/// LSB-first inside every byte, contiguously across byte boundaries. The
/// output is <c>num_rows</c> values stored as fixed-width integers of
/// <c>uncompressed_bits_per_value</c> bits in little-endian byte order.
/// If <see cref="Bitpacked.Signed"/> is true the high bit of each compressed
/// value is interpreted as a sign bit and the value is sign-extended into
/// its uncompressed width.</para>
/// </summary>
internal sealed class BitpackedDecoder : IArrayDecoder
{
    private readonly Bitpacked _encoding;

    public BitpackedDecoder(Bitpacked encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        ulong compBits = _encoding.CompressedBitsPerValue;
        ulong uncompBits = _encoding.UncompressedBitsPerValue;

        ValidateWidths(compBits, uncompBits, targetType);

        if (_encoding.Buffer is null)
            throw new LanceFormatException("Bitpacked encoding has no buffer reference.");

        ReadOnlyMemory<byte> buffer = context.Resolve(_encoding.Buffer);
        int length = checked((int)numRows);
        int uncompBytes = checked((int)(uncompBits / 8));
        var output = new byte[length * uncompBytes];

        UnpackLsb(
            buffer.Span,
            compressedBits: (int)compBits,
            numValues: length,
            signExtend: _encoding.Signed,
            uncompBytes: uncompBytes,
            output);

        var data = new ArrayData(
            targetType, length,
            nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(output) });
        return ArrowArrayFactory.BuildArray(data);
    }

    private static void ValidateWidths(ulong compBits, ulong uncompBits, IArrowType targetType)
    {
        if (compBits == 0 || compBits > 64)
            throw new LanceFormatException(
                $"Bitpacked compressed_bits_per_value must be in [1, 64]; got {compBits}.");
        if (compBits > uncompBits)
            throw new LanceFormatException(
                $"Bitpacked compressed_bits_per_value ({compBits}) exceeds uncompressed_bits_per_value ({uncompBits}).");
        if (uncompBits != 8 && uncompBits != 16 && uncompBits != 32 && uncompBits != 64)
            throw new NotImplementedException(
                $"Bitpacked uncompressed_bits_per_value of {uncompBits} is not supported (only 8, 16, 32, 64).");

        if (targetType is FixedWidthType fw)
        {
            if ((ulong)fw.BitWidth != uncompBits)
                throw new LanceFormatException(
                    $"Bitpacked uncompressed_bits_per_value={uncompBits} does not match target type {targetType} width ({fw.BitWidth}).");
        }
        else
        {
            throw new LanceFormatException(
                $"Bitpacked encoding cannot target non-fixed-width Arrow type {targetType}.");
        }
    }

    internal static void UnpackLsb(
        ReadOnlySpan<byte> src,
        int compressedBits,
        int numValues,
        bool signExtend,
        int uncompBytes,
        Span<byte> dest)
    {
        ulong valueMask = compressedBits == 64 ? ulong.MaxValue : (1UL << compressedBits) - 1;
        ulong signBit = 1UL << (compressedBits - 1);
        ulong signExtendMask = compressedBits == 64 ? 0UL : ~valueMask;

        long bitPos = 0;
        for (int i = 0; i < numValues; i++)
        {
            ulong value = ReadBitsLsb(src, bitPos, compressedBits);
            bitPos += compressedBits;

            if (signExtend && (value & signBit) != 0)
                value |= signExtendMask;

            WriteLittleEndian(dest.Slice(i * uncompBytes, uncompBytes), value);
        }
    }

    private static ulong ReadBitsLsb(ReadOnlySpan<byte> src, long bitPos, int numBits)
    {
        int bytePos = checked((int)(bitPos >> 3));
        int bitOffset = (int)(bitPos & 7);
        ulong acc = 0;
        int loaded = 0;

        while (loaded < numBits)
        {
            if (bytePos >= src.Length)
                throw new LanceFormatException(
                    $"Bitpacked buffer exhausted at bit {bitPos + loaded}; have {src.Length} bytes.");

            byte b = src[bytePos];
            int availInByte = 8 - bitOffset;
            int toTake = Math.Min(numBits - loaded, availInByte);
            ulong chunk = ((ulong)b >> bitOffset) & ((1UL << toTake) - 1);
            acc |= chunk << loaded;

            loaded += toTake;
            bitOffset = 0;
            bytePos++;
        }

        return acc;
    }

    private static void WriteLittleEndian(Span<byte> dest, ulong value)
    {
        for (int i = 0; i < dest.Length; i++)
        {
            dest[i] = (byte)(value & 0xFF);
            value >>= 8;
        }
    }
}
