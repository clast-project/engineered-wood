// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

/// <summary>
/// Reads an ORC <c>DECIMAL</c> column as Arrow <c>Decimal128</c> of the column's precision and scale.
/// </summary>
/// <remarks>
/// <para>DATA holds each unscaled value as a zigzag varint of up to 128 bits, and SECONDARY each value's OWN
/// scale (signed RLE). A writer need not use the column's scale for every value, so each is rescaled to it: up
/// by multiplying, down by dividing with truncation toward zero, as the C++ reader (pyarrow) does. A value that
/// does not fit 128 bits after rescaling is refused.</para>
/// <para>A file from Hive 0.11 declares no precision or scale (both 0). Such a column reads as
/// <c>decimal(38, 6)</c>, the C++ reader's default (<c>forcedScaleOnHive11Decimal</c>); see
/// <see cref="OrcSchema"/>.</para>
/// </remarks>
internal sealed class DecimalColumnReader : ColumnReader
{
    /// <summary>The precision and scale of a decimal column whose type declares none (Hive 0.11).</summary>
    internal const int Hive11Precision = 38;
    internal const int Hive11Scale = 6;

    private readonly ColumnEncoding.Types.Kind _encoding;
    private readonly int _precision;
    private readonly int _scale;
    private OrcByteStream? _dataStream;
    private RleDecoderV2? _secondaryDecoderV2;
    private RleDecoderV1? _secondaryDecoderV1;

    public DecimalColumnReader(int columnId, ColumnEncoding.Types.Kind encoding, int precision, int scale) : base(columnId)
    {
        _encoding = encoding;
        if (precision > 0)
        {
            _precision = precision;
            _scale = scale;
        }
        else
        {
            _precision = Hive11Precision;
            _scale = Hive11Scale;
        }
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        // DATA stream is always varint for decimal (not RLE)
    }

    public void SetSecondaryStream(OrcByteStream stream)
    {
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _secondaryDecoderV2 = new RleDecoderV2(stream, signed: true, legacyPatchedBase: LegacyEngineeredWood);
        else
            _secondaryDecoderV1 = new RleDecoderV1(stream, signed: true);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        var scales = new long[nonNullCount];
        if (nonNullCount > 0)
        {
            if (_secondaryDecoderV2 != null)
                _secondaryDecoderV2.ReadValues(scales);
            else if (_secondaryDecoderV1 != null)
                _secondaryDecoderV1.ReadValues(scales);
            else
                scales.AsSpan().Fill(_scale);
        }

        const int byteWidth = 16;
        using var valueBuf = new NativeBuffer<byte>(batchSize * byteWidth, zeroFill: nullCount > 0);
        var valueBytes = valueBuf.Span;
        using var nullBuf = new NativeBuffer<byte>((batchSize + 7) / 8, zeroFill: true);
        var nullBits = nullBuf.Span;
        int rawIdx = 0;

        for (int i = 0; i < batchSize; i++)
        {
            if (present != null && !present[i])
                continue;

            nullBits[i >> 3] |= (byte)(1 << (i & 7));
            if (_dataStream == null)
                throw MissingStream("decimal", "DATA");
            var value = Rescale(ReadVarInt128(_dataStream), (int)scales[rawIdx++], _scale, ColumnId);
            var dest = valueBytes.Slice(i * byteWidth, byteWidth);
            BinaryPrimitives.WriteUInt64LittleEndian(dest, (ulong)value);
            BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(8), (ulong)(value >> 64));
        }

        var arrowType = new Decimal128Type(_precision, _scale);
        var nullBuffer = nullCount > 0 ? nullBuf.Build() : ArrowBuffer.Empty;
        var arrayData = new ArrayData(arrowType, batchSize, nullCount, 0, [nullBuffer, valueBuf.Build()]);
        return new Decimal128Array(arrayData);
    }

    /// <summary><paramref name="value"/>, stored at scale <paramref name="from"/>, at scale <paramref name="to"/>:
    /// scaled up exactly, or down truncating toward zero.</summary>
    internal static Int128 Rescale(Int128 value, int from, int to, int columnId)
    {
        if (from == to)
            return value;
        if (from < to)
        {
            int up = to - from;
            if (up > 38)
                throw ScaleOutOfRange(from, to, columnId);
            try
            {
                return checked(value * Pow10(up));
            }
            catch (OverflowException)
            {
                throw new InvalidDataException(
                    $"ORC decimal column {columnId}: a value stored at scale {from} does not fit 128 bits at the column's scale {to}.");
            }
        }

        int down = from - to;
        // Beyond 38 digits nothing representable is left but zero.
        return down > 38 ? Int128.Zero : value / Pow10(down);
    }

    private static InvalidDataException ScaleOutOfRange(int from, int to, int columnId) =>
        new($"ORC decimal column {columnId}: a value's scale {from} is too far from the column's scale {to}.");

    private static Int128 Pow10(int exponent)
    {
        Int128 result = Int128.One;
        for (int i = 0; i < exponent; i++)
            result *= 10;
        return result;
    }

    /// <summary>A zigzag varint of up to 128 bits (19 bytes).</summary>
    internal static Int128 ReadVarInt128(OrcByteStream stream)
    {
        UInt128 result = UInt128.Zero;
        int shift = 0;
        while (true)
        {
            int b = stream.ReadByte();
            if (b < 0)
                throw new InvalidDataException("Unexpected end of decimal varint.");
            if (shift >= 128 || (shift == 126 && (b & 0x7C) != 0))
                throw new InvalidDataException("An ORC decimal varint is longer than 128 bits.");
            result |= (UInt128)(uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                break;
            shift += 7;
        }
        var half = (Int128)(result >> 1);
        return (result & UInt128.One) == UInt128.Zero ? half : ~half;
    }
}
