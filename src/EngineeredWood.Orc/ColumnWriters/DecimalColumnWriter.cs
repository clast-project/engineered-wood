// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class DecimalColumnWriter : ColumnWriter
{
    private readonly int _precision;
    private readonly int _scale;
    private readonly GrowableBuffer _dataStream = new();
    private readonly GrowableBuffer _secondaryStream = new();
    private readonly RleEncoderV2 _scaleEncoder;

    // Statistics
    private long _minLong = long.MaxValue;
    private long _maxLong = long.MinValue;
    private bool _hasValues;

    public DecimalColumnWriter(int columnId, int precision, int scale) : base(columnId)
    {
        _precision = precision;
        _scale = scale;

        _scaleEncoder = new RleEncoderV2(_secondaryStream, signed: true);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        WriteVarInt((Decimal128Array)array);
    }

    private void WriteVarInt(Decimal128Array array)
    {
        Span<long> scales = stackalloc long[1];
        scales[0] = _scale;

        for (int i = 0; i < array.Length; i++)
        {
            if (!array.IsValid(i)) continue;

            long value = ReadDecimal128AsLong(array, i);
            _hasValues = true;
            if (value < _minLong) _minLong = value;
            if (value > _maxLong) _maxLong = value;
            if (BloomFilter != null)
                BloomFilter.AddBytes(System.Text.Encoding.UTF8.GetBytes(FormatDecimal(value, _scale)));

            // Zigzag encode and write as varint
            ulong zz = (ulong)((value << 1) ^ (value >> 63));
            while (zz > 0x7F)
            {
                _dataStream.WriteByte((byte)(0x80 | (zz & 0x7F)));
                zz >>= 7;
            }
            _dataStream.WriteByte((byte)zz);
            _scaleEncoder.WriteValues(scales);
        }
    }

    private static long ReadDecimal128AsLong(Decimal128Array array, int index)
    {
        // Decimal128Array stores values as 16-byte little-endian two's complement
        // For precision <= 38, we read the low 8 bytes as the value
        // (high 8 bytes are sign extension for values that fit in a long)
        var bytes = array.ValueBuffer.Span.Slice(index * 16, 16);
        return BinaryPrimitives.ReadInt64LittleEndian(bytes);
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.DecimalStatistics = new DecimalStatistics
            {
                Minimum = FormatDecimal(_minLong, _scale),
                Maximum = FormatDecimal(_maxLong, _scale),
            };
        }
        return stats;
    }

    private static string FormatDecimal(long unscaled, int scale)
    {
        if (scale == 0) return unscaled.ToString();

        bool negative = unscaled < 0;
        var abs = negative ? unchecked(-unscaled) : unscaled;
        var s = abs.ToString();

        if (s.Length <= scale)
            s = new string('0', scale - s.Length + 1) + s;

        var result = s.Insert(s.Length - scale, ".");
        return negative ? "-" + result : result;
    }

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes() + _dataStream.Length + _secondaryStream.Length + _scaleEncoder.BufferedCount * 8L;

    public override ColumnEncoding GetEncoding()
    {
        return new ColumnEncoding { Kind = ColumnEncoding.Types.Kind.DirectV2 };
    }

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _scaleEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
        positions.Add((ulong)_secondaryStream.Length);
        positions.Add(0); // RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(0); // DATA (varint/raw): no extras
        extrasPerStream.Add(1); // SECONDARY (RLE v2): rle_remaining
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _minLong = long.MaxValue;
        _maxLong = long.MinValue;
        _hasValues = false;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _scaleEncoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Secondary, _secondaryStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _secondaryStream.Reset();
        _minLong = long.MaxValue;
        _maxLong = long.MinValue;
        _hasValues = false;
    }
}
