// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class ListColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _lengthStream = new();
    private readonly RleEncoderV2 _lengthEncoder;
    private ColumnWriter? _elementWriter;
    private long _minChildren = long.MaxValue;
    private long _maxChildren = long.MinValue;
    private long _totalChildren;
    private bool _hasValues;

    public ListColumnWriter(int columnId) : base(columnId)
    {
        _lengthEncoder = new RleEncoderV2(_lengthStream, signed: false);
    }

    public void SetElementWriter(ColumnWriter writer) => _elementWriter = writer;
    public ColumnWriter? ElementWriter => _elementWriter;

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var listArray = (ListArray)array;

        // Write lengths for each list entry and collect element arrays
        Span<long> len = stackalloc long[1];
        int totalElements = 0;

        for (int i = 0; i < listArray.Length; i++)
        {
            if (!listArray.IsValid(i)) continue;
            int start = listArray.ValueOffsets[i];
            int end = listArray.ValueOffsets[i + 1];
            int length = end - start;
            len[0] = length;
            _lengthEncoder.WriteValues(len);
            _hasValues = true;
            if (length < _minChildren) _minChildren = length;
            if (length > _maxChildren) _maxChildren = length;
            _totalChildren += length;
            totalElements += length;
        }

        // Write all element values as one batch
        if (_elementWriter != null && totalElements > 0)
        {
            _elementWriter.Write(listArray.Values);
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
