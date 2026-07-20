// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class FloatColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private double _min = double.PositiveInfinity;
    private double _max = double.NegativeInfinity;
    private double _sum;
    private bool _hasValues;

    public FloatColumnWriter(int columnId) : base(columnId) { }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (FloatArray)array;
        Span<byte> tmp = stackalloc byte[4];
        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;
            float v = a.GetValue(i)!.Value;
            _hasValues = true;
            if (v < _min) _min = v;
            if (v > _max) _max = v;
            _sum += v;
            BloomFilter?.AddDouble(v);
#if NET8_0_OR_GREATER
            BinaryPrimitives.WriteSingleLittleEndian(tmp, v);
#else
            MemoryMarshal.Write(tmp, ref v);
#endif
            _dataStream.Write(tmp);
        }
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.DoubleStatistics = new DoubleStatistics
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

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(0); // DATA (raw): no extras
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _min = double.PositiveInfinity;
        _max = double.NegativeInfinity;
        _sum = 0;
        _hasValues = false;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _min = double.PositiveInfinity;
        _max = double.NegativeInfinity;
        _sum = 0;
        _hasValues = false;
    }
}

internal sealed class DoubleColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private double _min = double.PositiveInfinity;
    private double _max = double.NegativeInfinity;
    private double _sum;
    private bool _hasValues;

    public DoubleColumnWriter(int columnId) : base(columnId) { }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (DoubleArray)array;
        Span<byte> tmp = stackalloc byte[8];
        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;
            double v = a.GetValue(i)!.Value;
            _hasValues = true;
            if (v < _min) _min = v;
            if (v > _max) _max = v;
            _sum += v;
            BloomFilter?.AddDouble(v);
#if NET8_0_OR_GREATER
            BinaryPrimitives.WriteDoubleLittleEndian(tmp, v);
#else
            MemoryMarshal.Write(tmp, ref v);
#endif
            _dataStream.Write(tmp);
        }
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            stats.DoubleStatistics = new DoubleStatistics
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

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(0); // DATA (raw): no extras
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _min = double.PositiveInfinity;
        _max = double.NegativeInfinity;
        _sum = 0;
        _hasValues = false;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _min = double.PositiveInfinity;
        _max = double.NegativeInfinity;
        _sum = 0;
        _hasValues = false;
    }
}
