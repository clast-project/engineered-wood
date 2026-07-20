// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using System.Text;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class StringColumnReader : ColumnReader
{
    private readonly ColumnEncoding.Types.Kind _encoding;
    private OrcByteStream? _dataStream;
    private OrcByteStream? _lengthStream;
    private OrcByteStream? _dictionaryDataStream;

    // Persistent decoders
    private RleDecoderV2? _dataDecoderV2;
    private RleDecoderV1? _dataDecoderV1;
    private RleDecoderV2? _lengthDecoderV2;
    private RleDecoderV1? _lengthDecoderV1;

    // For dictionary encoding, we cache the dictionary as UTF-8 byte arrays
    private byte[][]? _dictUtf8;
    private int _dictionarySize;

    public StringColumnReader(int columnId, ColumnEncoding.Types.Kind encoding) : base(columnId)
    {
        _encoding = encoding;
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2 or ColumnEncoding.Types.Kind.DictionaryV2)
            _dataDecoderV2 = new RleDecoderV2(stream, signed: false);
        else
            _dataDecoderV1 = new RleDecoderV1(stream, signed: false);
    }

    public void SetLengthStream(OrcByteStream stream)
    {
        _lengthStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2 or ColumnEncoding.Types.Kind.DictionaryV2)
            _lengthDecoderV2 = new RleDecoderV2(stream, signed: false);
        else
            _lengthDecoderV1 = new RleDecoderV1(stream, signed: false);
    }

    public void SetDictionaryDataStream(OrcByteStream stream) => _dictionaryDataStream = stream;
    public void SetDictionarySize(int size) => _dictionarySize = size;

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);

        if (_encoding is ColumnEncoding.Types.Kind.Dictionary or ColumnEncoding.Types.Kind.DictionaryV2)
            return ReadDictionaryEncoded(batchSize, present, nonNullCount);
        else
            return ReadDirectEncoded(batchSize, present, nonNullCount);
    }

    private IArrowArray ReadDirectEncoded(int batchSize, bool[]? present, int nonNullCount)
    {
        // Read string byte lengths
        var lengthPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var lengths = lengthPool.AsSpan(0, nonNullCount);
            if (nonNullCount > 0)
            {
                if (_lengthDecoderV2 != null) _lengthDecoderV2.ReadValues(lengths);
                else _lengthDecoderV1?.ReadValues(lengths);
            }

            // Compute total bytes and offsets
            long totalBytes = 0;
            for (int i = 0; i < nonNullCount; i++)
                totalBytes += lengths[i];

            // Build offsets array: batchSize + 1 entries
            using var offsetBuf = new NativeBuffer<int>(batchSize + 1, zeroFill: false);
            var offsets = offsetBuf.Span;

            int rawIdx = 0;
            int currentOffset = 0;
            for (int i = 0; i < batchSize; i++)
            {
                offsets[i] = currentOffset;
                if (present == null || present[i])
                    currentOffset += (int)lengths[rawIdx++];
            }
            offsets[batchSize] = currentOffset;

            // Read all string bytes contiguously from the data stream
            using var dataBuf = new NativeBuffer<byte>(currentOffset > 0 ? currentOffset : 1, zeroFill: false);
            if (currentOffset > 0 && _dataStream != null)
                _dataStream.ReadExactly(dataBuf.Span.Slice(0, currentOffset));

            int nullCount = present == null ? 0 : batchSize - nonNullCount;
            var nullBuffer = CreateValidityBuffer(present, batchSize);

            return new StringArray(batchSize, offsetBuf.Build(), dataBuf.Build(), nullBuffer, nullCount);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(lengthPool);
        }
    }

    private IArrowArray ReadDictionaryEncoded(int batchSize, bool[]? present, int nonNullCount)
    {
        if (_dictUtf8 == null)
            BuildDictionary();

        // Read dictionary indices
        var indicesPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var indices = indicesPool.AsSpan(0, nonNullCount);
            if (nonNullCount > 0)
            {
                if (_dataDecoderV2 != null) _dataDecoderV2.ReadValues(indices);
                else _dataDecoderV1?.ReadValues(indices);
            }

            // Compute total bytes needed
            int totalBytes = 0;
            int rawIdx = 0;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
                {
                    int dictIdx = (int)indices[rawIdx++];
                    if (_dictUtf8 == null || dictIdx < 0 || dictIdx >= _dictUtf8.Length)
                        throw new InvalidDataException($"Dictionary index {dictIdx} out of range (dict size={_dictUtf8?.Length ?? 0})");
                    totalBytes += _dictUtf8[dictIdx].Length;
                }
            }

            // Build offsets and copy string bytes
            using var offsetBuf = new NativeBuffer<int>(batchSize + 1, zeroFill: false);
            var offsets = offsetBuf.Span;

            using var dataBuf = new NativeBuffer<byte>(totalBytes > 0 ? totalBytes : 1, zeroFill: false);
            var data = dataBuf.Span;

            rawIdx = 0;
            int currentOffset = 0;
            for (int i = 0; i < batchSize; i++)
            {
                offsets[i] = currentOffset;
                if (present == null || present[i])
                {
                    var entry = _dictUtf8![(int)indices[rawIdx++]];
                    entry.AsSpan().CopyTo(data.Slice(currentOffset));
                    currentOffset += entry.Length;
                }
            }
            offsets[batchSize] = currentOffset;

            int nullCount = present == null ? 0 : batchSize - nonNullCount;
            var nullBuffer = CreateValidityBuffer(present, batchSize);

            return new StringArray(batchSize, offsetBuf.Build(), dataBuf.Build(), nullBuffer, nullCount);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(indicesPool);
        }
    }

    private void BuildDictionary()
    {
        if (_dictionaryDataStream == null || _lengthStream == null)
        {
            _dictUtf8 = [];
            return;
        }

        // Read dictionary entry lengths
        var lengths = new long[_dictionarySize];
        if (_lengthDecoderV2 != null) _lengthDecoderV2.ReadValues(lengths);
        else _lengthDecoderV1?.ReadValues(lengths);

        // Read all dictionary bytes at once
        long totalBytes = 0;
        for (int i = 0; i < _dictionarySize; i++)
            totalBytes += lengths[i];

        var allBytes = new byte[totalBytes];
        if (totalBytes > 0)
            _dictionaryDataStream.ReadExactly(allBytes);

        // Slice into per-entry byte arrays
        _dictUtf8 = new byte[_dictionarySize][];
        int offset = 0;
        for (int i = 0; i < _dictionarySize; i++)
        {
            int len = (int)lengths[i];
            _dictUtf8[i] = allBytes.AsSpan(offset, len).ToArray();
            offset += len;
        }
    }
}
