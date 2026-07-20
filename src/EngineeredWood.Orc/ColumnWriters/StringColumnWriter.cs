// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

internal sealed class StringColumnWriter : ColumnWriter
{
    private readonly EncodingFamily _encodingFamily;
    private readonly int _dictSizeThreshold;

    // Statistics
    private string? _minValue;
    private string? _maxValue;
    private long _totalLength;

    // Direct encoding streams
    private readonly GrowableBuffer _dataStream = new();
    private readonly GrowableBuffer _lengthStream = new();
    private readonly RleEncoderV2 _lengthEncoder;

    // Dictionary encoding state
    private Dictionary<string, int>? _dictionary;
    private List<byte[]>? _dictEntries;
    private List<int>? _dictIndices;
    private bool _useDictionary;
    private bool _dictionaryOverflow;

    public StringColumnWriter(int columnId, EncodingFamily encoding, int dictSizeThreshold) : base(columnId)
    {
        _encodingFamily = encoding;
        _dictSizeThreshold = dictSizeThreshold;
        _lengthEncoder = new RleEncoderV2(_lengthStream, signed: false);

        if (encoding == EncodingFamily.DictionaryV2)
        {
            _useDictionary = true;
            _dictionary = new Dictionary<string, int>();
            _dictEntries = [];
            _dictIndices = [];
        }
    }

    public override void Write(IArrowArray array)
    {
        int nonNullCount = WritePresent(array);
        var a = (StringArray)array;

        if (_useDictionary && !_dictionaryOverflow)
        {
            WriteDictionary(a);
        }
        else
        {
            WriteDirect(a);
        }
    }

    private void TrackString(string str)
    {
        _totalLength += str.Length;
        if (_minValue == null || string.Compare(str, _minValue, StringComparison.Ordinal) < 0)
            _minValue = str;
        if (_maxValue == null || string.Compare(str, _maxValue, StringComparison.Ordinal) > 0)
            _maxValue = str;
    }

    private void WriteDirect(StringArray array) => WriteDirectFrom(array, 0);

    private void WriteDirectFrom(StringArray array, int startIndex)
    {
        Span<long> len = stackalloc long[1];
        for (int i = startIndex; i < array.Length; i++)
        {
            if (!array.IsValid(i)) continue;
            TrackString(array.GetString(i)!);
            var bytes = array.GetBytes(i);
            BloomFilter?.AddBytes(bytes);
            len[0] = bytes.Length;
            _lengthEncoder.WriteValues(len);
            _dataStream.Write(bytes);
        }
    }

    private void WriteDictionary(StringArray array)
    {
        for (int i = 0; i < array.Length; i++)
        {
            if (!array.IsValid(i)) continue;
            var str = array.GetString(i)!;

            if (!_dictionary!.TryGetValue(str, out int idx))
            {
                if (_dictionary.Count >= _dictSizeThreshold)
                {
                    // Dictionary overflow - fall back to direct encoding
                    FallbackToDirect();
                    // Write remaining values (from i onwards) directly
                    WriteDirectFrom(array, i);
                    return;
                }
                idx = _dictionary.Count;
                _dictionary[str] = idx;
                _dictEntries!.Add(Encoding.UTF8.GetBytes(str));
            }
            TrackString(str);
            if (BloomFilter != null)
                BloomFilter.AddBytes(System.Text.Encoding.UTF8.GetBytes(str));
            _dictIndices!.Add(idx);
        }
    }

    private void FallbackToDirect()
    {
        _useDictionary = false;
        _dictionaryOverflow = true;

        // Re-encode all previously buffered dictionary indices as direct
        if (_dictIndices != null && _dictIndices.Count > 0)
        {
            Span<long> len = stackalloc long[1];
            foreach (int idx in _dictIndices)
            {
                var bytes = _dictEntries![idx];
                len[0] = bytes.Length;
                _lengthEncoder.WriteValues(len);
                _dataStream.Write(bytes);
            }
        }

        _dictionary = null;
        _dictEntries = null;
        _dictIndices = null;
    }

    public override long EstimateBufferedBytes()
    {
        long bytes = base.EstimateBufferedBytes() + _dataStream.Length + _lengthStream.Length + _lengthEncoder.BufferedCount * 8L;
        if (_dictEntries != null)
        {
            foreach (var e in _dictEntries)
                bytes += e.Length;
        }
        if (_dictIndices != null)
            bytes += _dictIndices.Count * 4L;
        return bytes;
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_minValue != null)
        {
            stats.StringStatistics = new StringStatistics
            {
                Minimum = _minValue,
                Maximum = _maxValue,
                Sum = _totalLength,
            };
        }
        return stats;
    }

    public override ColumnEncoding GetEncoding()
    {
        if (_useDictionary)
            return new ColumnEncoding
            {
                Kind = ColumnEncoding.Types.Kind.DictionaryV2,
                DictionarySize = (uint)(_dictEntries?.Count ?? 0)
            };
        return new ColumnEncoding { Kind = ColumnEncoding.Types.Kind.DirectV2 };
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);

        if (_useDictionary)
        {
            // Encode dictionary data + lengths
            var dictDataStream = new GrowableBuffer();
            var dictLengthStream = new GrowableBuffer();
            var dictLengthEncoder = new RleEncoderV2(dictLengthStream, signed: false);

            Span<long> len = stackalloc long[1];
            foreach (var entry in _dictEntries!)
            {
                dictDataStream.Write(entry);
                len[0] = entry.Length;
                dictLengthEncoder.WriteValues(len);
            }
            dictLengthEncoder.Flush();

            // Encode indices as DATA stream
            var indicesStream = new GrowableBuffer();
            var indicesEncoder = new RleEncoderV2(indicesStream, signed: false);
            Span<long> idx = stackalloc long[1];
            foreach (int index in _dictIndices!)
            {
                idx[0] = index;
                indicesEncoder.WriteValues(idx);
            }
            indicesEncoder.Flush();

            streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, indicesStream));
            streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.DictionaryData, dictDataStream));
            streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Length, dictLengthStream));
        }
        else
        {
            _lengthEncoder.Flush();
            streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
            streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Length, _lengthStream));
        }
    }

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _lengthEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        if (_useDictionary)
        {
            // Dictionary encoding: DATA (indices, RLE v2)
            positions.Add((ulong)(_dictIndices?.Count ?? 0));
            positions.Add(0); // RLE remaining
        }
        else
        {
            // Direct encoding: DATA (raw), LENGTH (RLE v2)
            positions.Add((ulong)_dataStream.Length);
            positions.Add((ulong)_lengthStream.Length);
            positions.Add(0); // RLE remaining
        }
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        if (_useDictionary)
        {
            extrasPerStream.Add(1); // DATA (RLE v2 indices): rle_remaining
        }
        else
        {
            extrasPerStream.Add(0); // DATA (raw bytes): no extras
            extrasPerStream.Add(1); // LENGTH (RLE v2): rle_remaining
        }
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _minValue = null;
        _maxValue = null;
        _totalLength = 0;
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _lengthStream.Reset();
        _minValue = null;
        _maxValue = null;
        _totalLength = 0;

        if (_encodingFamily == EncodingFamily.DictionaryV2)
        {
            _useDictionary = true;
            _dictionaryOverflow = false;
            _dictionary = new Dictionary<string, int>();
            _dictEntries = [];
            _dictIndices = [];
        }
    }
}
