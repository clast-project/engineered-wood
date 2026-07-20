// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class BinaryColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private readonly GrowableBuffer _lengthStream = new();
    private readonly RleEncoderV2 _lengthEncoder;
    private long _totalLength;

    public BinaryColumnWriter(int columnId) : base(columnId)
    {
        _lengthEncoder = new RleEncoderV2(_lengthStream, signed: false);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (BinaryArray)array;
        Span<long> len = stackalloc long[1];
        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;
            var bytes = a.GetBytes(i);
            BloomFilter?.AddBytes(bytes);
            _totalLength += bytes.Length;
            len[0] = bytes.Length;
            _lengthEncoder.WriteValues(len);
            _dataStream.Write(bytes);
        }
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        stats.BinaryStatistics = new BinaryStatistics
        {
            Sum = _totalLength,
        };
        return stats;
    }

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes() + _dataStream.Length + _lengthStream.Length + _lengthEncoder.BufferedCount * 8L;

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.DirectV2 };

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _lengthEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
        positions.Add((ulong)_lengthStream.Length);
        positions.Add(0); // RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(0); // DATA (raw): no extras
        extrasPerStream.Add(1); // LENGTH (RLE v2): rle_remaining
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _totalLength = 0;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _lengthEncoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Length, _lengthStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _lengthStream.Reset();
        _totalLength = 0;
    }
}
