// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class TimestampColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private readonly GrowableBuffer _secondaryStream = new();
    private readonly RleEncoderV2 _dataEncoder;
    private readonly RleEncoderV2 _secondaryEncoder;
    private long _minMillis = long.MaxValue;
    private long _maxMillis = long.MinValue;
    private bool _hasValues;

    public TimestampColumnWriter(int columnId) : base(columnId)
    {
        _dataEncoder = new RleEncoderV2(_dataStream, signed: true);
        _secondaryEncoder = new RleEncoderV2(_secondaryStream, signed: false);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (TimestampArray)array;
        Span<long> sec = stackalloc long[1];
        Span<long> nano = stackalloc long[1];

        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;

            long epochNanos = a.GetValue(i)!.Value;
            long millis = epochNanos / 1_000_000L;
            _hasValues = true;
            if (millis < _minMillis) _minMillis = millis;
            if (millis > _maxMillis) _maxMillis = millis;
            BloomFilter?.AddLong(millis);

            long seconds = Math.DivRem(epochNanos, 1_000_000_000L, out long nanos);
            if (nanos < 0) { seconds--; nanos += 1_000_000_000L; }

            sec[0] = seconds;
            nano[0] = EncodeNanos(nanos);
            _dataEncoder.WriteValues(sec);
            _secondaryEncoder.WriteValues(nano);
        }
    }

    private static long EncodeNanos(long nanos)
    {
        if (nanos == 0) return 0;

        // ORC encoding: trailing zeros determine scale
        // scale 0 = nanoseconds, 1 = microseconds, 2 = milliseconds, 3 = seconds
        int scale = 0;
        long value = nanos;
        if (value % 1000 == 0) { value /= 1000; scale = 1; }
        if (scale > 0 && value % 1000 == 0) { value /= 1000; scale = 2; }
        if (scale > 1 && value % 1000 == 0) { value /= 1000; scale = 3; }

        return (value << 3) | (long)scale;
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.TimestampStatistics = new TimestampStatistics
            {
                Minimum = _minMillis,
                Maximum = _maxMillis,
            };
        }
        return stats;
    }

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes()
        + _dataStream.Length + _dataEncoder.BufferedCount * 8L
        + _secondaryStream.Length + _secondaryEncoder.BufferedCount * 8L;

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.DirectV2 };

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _dataEncoder.Flush();
        _secondaryEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
        positions.Add(0); // DATA RLE remaining
        positions.Add((ulong)_secondaryStream.Length);
        positions.Add(0); // SECONDARY RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(1); // DATA (RLE v2): rle_remaining
        extrasPerStream.Add(1); // SECONDARY (RLE v2): rle_remaining
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _minMillis = long.MaxValue;
        _maxMillis = long.MinValue;
        _hasValues = false;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _dataEncoder.Flush();
        _secondaryEncoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Secondary, _secondaryStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _secondaryStream.Reset();
        _minMillis = long.MaxValue;
        _maxMillis = long.MinValue;
        _hasValues = false;
    }
}
