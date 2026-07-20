// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class MapColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _lengthStream = new();
    private readonly RleEncoderV2 _lengthEncoder;
    private ColumnWriter? _keyWriter;
    private ColumnWriter? _valueWriter;
    private long _minChildren = long.MaxValue;
    private long _maxChildren = long.MinValue;
    private long _totalChildren;
    private bool _hasValues;

    public MapColumnWriter(int columnId) : base(columnId)
    {
        _lengthEncoder = new RleEncoderV2(_lengthStream, signed: false);
    }

    public void SetKeyWriter(ColumnWriter writer) => _keyWriter = writer;
    public void SetValueWriter(ColumnWriter writer) => _valueWriter = writer;

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var mapArray = (MapArray)array;

        // Write lengths for each map entry
        Span<long> len = stackalloc long[1];
        for (int i = 0; i < mapArray.Length; i++)
        {
            if (!mapArray.IsValid(i)) continue;
            int start = mapArray.ValueOffsets[i];
            int end = mapArray.ValueOffsets[i + 1];
            int length = end - start;
            len[0] = length;
            _lengthEncoder.WriteValues(len);
            _hasValues = true;
            if (length < _minChildren) _minChildren = length;
            if (length > _maxChildren) _maxChildren = length;
            _totalChildren += length;
        }

        // Write all key and value arrays
        var keyValues = mapArray.KeyValues;
        if (keyValues.Length > 0)
        {
            _keyWriter?.Write(keyValues.Fields[0]);
            _valueWriter?.Write(keyValues.Fields[1]);
        }
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.CollectionStatistics = new CollectionStatistics
            {
                MinChildren = (ulong)_minChildren,
                MaxChildren = (ulong)_maxChildren,
                TotalChildren = (ulong)_totalChildren,
            };
        }
        return stats;
    }

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes() + _lengthStream.Length + _lengthEncoder.BufferedCount * 8L;

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.DirectV2 };

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _lengthEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_lengthStream.Length);
        positions.Add(0); // RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(1); // LENGTH (RLE v2): rle_remaining
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _lengthEncoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Length, _lengthStream));
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _minChildren = long.MaxValue;
        _maxChildren = long.MinValue;
        _totalChildren = 0;
        _hasValues = false;
    }

    public override void Reset()
    {
        base.Reset();
        _lengthStream.Reset();
        _minChildren = long.MaxValue;
        _maxChildren = long.MinValue;
        _totalChildren = 0;
        _hasValues = false;
    }
}
