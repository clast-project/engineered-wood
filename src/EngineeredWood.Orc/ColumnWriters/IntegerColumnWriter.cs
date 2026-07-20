// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class IntegerColumnWriter : ColumnWriter
{
    private readonly IArrowType _arrowType;
    private readonly GrowableBuffer _dataStream = new();
    private readonly RleEncoderV2 _encoder;
    private long _min = long.MaxValue;
    private long _max = long.MinValue;
    private long _sum;
    private bool _hasValues;

    public IntegerColumnWriter(int columnId, IArrowType arrowType) : base(columnId)
    {
        _arrowType = arrowType;
        _encoder = new RleEncoderV2(_dataStream, signed: true);
    }

    public override void Write(IArrowArray array)
    {
        int nonNullCount = WritePresent(array);

        // Cap stack buffer at 128 longs (1 KB) — _encoder.WriteValues is called every time
        // the buffer fills, so a smaller chunk size is fine.
        Span<long> values = stackalloc long[Math.Min(nonNullCount, 128)];
        int written = 0;

        switch (array)
        {
            case Int16Array a:
                for (int i = 0; i < a.Length; i++)
                {
                    if (!a.IsValid(i)) continue;
                    long v16 = a.GetValue(i)!.Value;
                    TrackValue(v16);
                    BloomFilter?.AddLong(v16);
                    values[written++] = v16;
                    if (written == values.Length) { _encoder.WriteValues(values.Slice(0, written)); written = 0; }
                }
                break;
            case Int32Array a:
                for (int i = 0; i < a.Length; i++)
                {
                    if (!a.IsValid(i)) continue;
                    long v32 = a.GetValue(i)!.Value;
                    TrackValue(v32);
                    BloomFilter?.AddLong(v32);
                    values[written++] = v32;
                    if (written == values.Length) { _encoder.WriteValues(values.Slice(0, written)); written = 0; }
                }
                break;
            case Int64Array a:
                for (int i = 0; i < a.Length; i++)
                {
                    if (!a.IsValid(i)) continue;
                    long v64 = a.GetValue(i)!.Value;
                    TrackValue(v64);
                    BloomFilter?.AddLong(v64);
                    values[written++] = v64;
                    if (written == values.Length) { _encoder.WriteValues(values.Slice(0, written)); written = 0; }
                }
                break;
        }

        if (written > 0)
            _encoder.WriteValues(values.Slice(0, written));
    }

    private void TrackValue(long v)
    {
        _hasValues = true;
        if (v < _min) _min = v;
        if (v > _max) _max = v;
        _sum += v;
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.IntStatistics = new IntegerStatistics
            {
                Minimum = _min,
                Maximum = _max,
                Sum = _sum,
            };
        }
        return stats;
    }

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes() + _dataStream.Length + _encoder.BufferedCount * 8L;

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.DirectV2 };

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _encoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
        positions.Add(0); // RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(1); // DATA (RLE v2): rle_remaining
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _min = long.MaxValue;
        _max = long.MinValue;
        _sum = 0;
        _hasValues = false;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _encoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _min = long.MaxValue;
        _max = long.MinValue;
        _sum = 0;
        _hasValues = false;
    }
}
