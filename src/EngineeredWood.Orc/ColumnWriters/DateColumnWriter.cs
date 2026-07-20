// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class DateColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private readonly RleEncoderV2 _encoder;
    private int _min = int.MaxValue;
    private int _max = int.MinValue;
    private bool _hasValues;

    public DateColumnWriter(int columnId) : base(columnId)
    {
        _encoder = new RleEncoderV2(_dataStream, signed: true);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (Date32Array)array;
        Span<long> v = stackalloc long[1];
        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;
            int day = a.GetValue(i)!.Value;
            _hasValues = true;
            if (day < _min) _min = day;
            if (day > _max) _max = day;
            BloomFilter?.AddLong(day);
            v[0] = day;
            _encoder.WriteValues(v);
        }
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.DateStatistics = new DateStatistics
            {
                Minimum = _min,
                Maximum = _max,
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
        _min = int.MaxValue;
        _max = int.MinValue;
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
        _min = int.MaxValue;
        _max = int.MinValue;
        _hasValues = false;
    }
}
