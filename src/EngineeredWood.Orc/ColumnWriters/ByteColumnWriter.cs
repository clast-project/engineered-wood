// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class ByteColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private readonly ByteRleEncoder _encoder;
    private long _min = long.MaxValue;
    private long _max = long.MinValue;
    private long _sum;
    private bool _hasValues;

    public ByteColumnWriter(int columnId) : base(columnId)
    {
        _encoder = new ByteRleEncoder(_dataStream);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (Int8Array)array;
        Span<byte> v = stackalloc byte[1];
        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;
            sbyte val = a.GetValue(i)!.Value;
            _hasValues = true;
            if (val < _min) _min = val;
            if (val > _max) _max = val;
            _sum += val;
            BloomFilter?.AddLong(val);
            v[0] = (byte)val;
            _encoder.WriteValues(v);
        }
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

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes() + _dataStream.Length;

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.Direct };

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _encoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
        positions.Add(0); // byte RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(1); // DATA (byte RLE): rle_remaining
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
