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
    private Int128 _min = Int128.MaxValue;
    private Int128 _max = Int128.MinValue;
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

            var value = ReadDecimal128(array, i);
            _hasValues = true;
            if (value < _min) _min = value;
            if (value > _max) _max = value;
            if (BloomFilter != null)
                BloomFilter.AddBytes(System.Text.Encoding.UTF8.GetBytes(FormatDecimal(value, _scale)));

            // Zigzag encode and write as a varint of up to 128 bits.
            var zz = (UInt128)((value << 1) ^ (value >> 127));
            while (zz > 0x7F)
            {
                _dataStream.WriteByte((byte)(0x80 | ((uint)zz & 0x7F)));
                zz >>= 7;
            }
            _dataStream.WriteByte((byte)(uint)zz);
            _scaleEncoder.WriteValues(scales);
        }
    }

    /// <summary>All 128 bits of an Arrow Decimal128 value (16 bytes, little-endian two's complement).</summary>
    private static Int128 ReadDecimal128(Decimal128Array array, int index)
    {
        var bytes = array.ValueBuffer.Span.Slice((array.Offset + index) * 16, 16);
        var value = new Int128(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes));
        // `array` has no use past the span it produced, and the read is a call. See
        // doc/arrow-span-lifetime.md.
        GC.KeepAlive(array);
        return value;
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.DecimalStatistics = new DecimalStatistics
            {
                Minimum = FormatDecimal(_min, _scale),
                Maximum = FormatDecimal(_max, _scale),
            };
        }
        return stats;
    }

    internal static string FormatDecimal(Int128 unscaled, int scale)
    {
        if (scale == 0) return unscaled.ToString(null, System.Globalization.CultureInfo.InvariantCulture);

        bool negative = unscaled < 0;
        // A decimal(38) value is far inside Int128's range, so its negation cannot overflow.
        var abs = negative ? -unscaled : unscaled;
        var s = abs.ToString(null, System.Globalization.CultureInfo.InvariantCulture);

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
        _min = Int128.MaxValue;
        _max = Int128.MinValue;
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
        _min = Int128.MaxValue;
        _max = Int128.MinValue;
        _hasValues = false;
    }
}
