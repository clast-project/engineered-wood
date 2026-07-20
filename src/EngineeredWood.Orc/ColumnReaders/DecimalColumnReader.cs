// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class DecimalColumnReader : ColumnReader
{
    private readonly ColumnEncoding.Types.Kind _encoding;
    private readonly int _precision;
    private readonly int _scale;
    private OrcByteStream? _dataStream;
    private OrcByteStream? _secondaryStream;
    private RleDecoderV2? _secondaryDecoderV2;
    private RleDecoderV1? _secondaryDecoderV1;

    public DecimalColumnReader(int columnId, ColumnEncoding.Types.Kind encoding, int precision, int scale) : base(columnId)
    {
        _encoding = encoding;
        _precision = precision > 0 ? precision : 38;
        _scale = scale;
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        // DATA stream is always varint for decimal (not RLE)
    }

    public void SetSecondaryStream(OrcByteStream stream)
    {
        _secondaryStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _secondaryDecoderV2 = new RleDecoderV2(stream, signed: true);
        else
            _secondaryDecoderV1 = new RleDecoderV1(stream, signed: true);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);

        if (_precision <= 18)
        {
            return ReadDecimal64(batchSize, present, nonNullCount);
        }
        else
        {
            return ReadDecimal128(batchSize, present, nonNullCount);
        }
    }

    private IArrowArray ReadDecimal64(int batchSize, bool[]? present, int nonNullCount)
    {
        var rawPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var rawValues = rawPool.AsSpan(0, nonNullCount);
            if (nonNullCount > 0 && _dataStream != null)
            {
                for (int r = 0; r < nonNullCount; r++)
                    rawValues[r] = ReadBigVarInt(_dataStream);
            }

            var arrowType = new Decimal128Type(_precision, _scale);
            int byteWidth = 16;
            int nullCount = present == null ? 0 : batchSize - nonNullCount;

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
                WriteLongAsDecimal128(rawValues[rawIdx++], valueBytes.Slice(i * byteWidth, byteWidth));
            }

            var valueBuffer = valueBuf.Build();
            var nullBuffer = nullCount > 0 ? nullBuf.Build() : ArrowBuffer.Empty;
            var arrayData = new ArrayData(arrowType, batchSize, nullCount, 0,
                [nullBuffer, valueBuffer]);
            return new Decimal128Array(arrayData);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(rawPool);
        }
    }

    private IArrowArray ReadDecimal128(int batchSize, bool[]? present, int nonNullCount)
    {
        var arrowType = new Decimal128Type(_precision, _scale);
        int byteWidth = 16;

        long[]? scales = null;
        if (_secondaryStream != null && nonNullCount > 0)
        {
            scales = new long[nonNullCount];
            if (_secondaryDecoderV2 != null)
                _secondaryDecoderV2.ReadValues(scales);
            else
                _secondaryDecoderV1?.ReadValues(scales);
        }

        var valueBytes = new byte[batchSize * byteWidth];
        var nullBits = new byte[(batchSize + 7) / 8];
        int rawIdx = 0;
        int nullCount = 0;

        for (int i = 0; i < batchSize; i++)
        {
            if (present != null && !present[i])
            {
                nullCount++;
            }
            else
            {
                nullBits[i >> 3] |= (byte)(1 << (i & 7));
                if (_dataStream != null)
                {
                    long value = ReadBigVarInt(_dataStream);
                    // Adjust scale if needed
                    int orcScale = scales != null ? (int)scales[rawIdx] : _scale;
                    if (orcScale != _scale)
                    {
                        int diff = _scale - orcScale;
                        if (diff > 0)
                        {
                            for (int d = 0; d < diff; d++) value *= 10;
                        }
                        else
                        {
                            for (int d = 0; d < -diff; d++) value /= 10;
                        }
                    }
                    WriteLongAsDecimal128(value, valueBytes.AsSpan(i * byteWidth, byteWidth));
                }
                rawIdx++;
            }
        }

        var valueBuffer = new ArrowBuffer(valueBytes);
        var nullBuffer = nullCount > 0 ? new ArrowBuffer(nullBits) : ArrowBuffer.Empty;
        var arrayData = new ArrayData(arrowType, batchSize, nullCount, 0,
            [nullBuffer, valueBuffer]);
        return new Decimal128Array(arrayData);
    }

    /// <summary>
    /// Writes a long as a 128-bit little-endian two's complement integer.
    /// </summary>
    private static void WriteLongAsDecimal128(long value, Span<byte> dest)
    {
        BinaryPrimitives.WriteInt64LittleEndian(dest, value);
        // Sign-extend the upper 8 bytes
        long signExtend = value < 0 ? -1L : 0L;
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(8), signExtend);
    }

    private static long ReadBigVarInt(OrcByteStream stream)
    {
        long result = 0;
        int shift = 0;
        while (true)
        {
            int b = stream.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of decimal varint.");
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return (long)((ulong)result >> 1) ^ -(result & 1);
    }
}
