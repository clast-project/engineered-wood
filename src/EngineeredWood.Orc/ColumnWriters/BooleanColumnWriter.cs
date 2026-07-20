// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class BooleanColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private readonly BooleanEncoder _encoder;
    private long _trueCount;

    public BooleanColumnWriter(int columnId) : base(columnId)
    {
        _encoder = new BooleanEncoder(_dataStream);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (BooleanArray)array;
        Span<bool> v = stackalloc bool[1];
        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;
            bool val = a.GetValue(i)!.Value;
            if (val) _trueCount++;
            BloomFilter?.AddLong(val ? 1L : 0L);
            v[0] = val;
            _encoder.WriteValues(v);
        }
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        stats.BucketStatistics = new BucketStatistics();
        stats.BucketStatistics.Count.Add((ulong)_trueCount);
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
        positions.Add(0); // bit offset
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(2); // DATA (boolean): rle_remaining + bit_offset
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _trueCount = 0;
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
        _trueCount = 0;
    }
}
