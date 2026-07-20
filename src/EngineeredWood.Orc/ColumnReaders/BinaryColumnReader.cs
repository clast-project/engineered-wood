// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class BinaryColumnReader : ColumnReader
{
    private readonly ColumnEncoding.Types.Kind _encoding;
    private OrcByteStream? _dataStream;
    private OrcByteStream? _lengthStream;
    private RleDecoderV2? _lengthDecoderV2;
    private RleDecoderV1? _lengthDecoderV1;

    public BinaryColumnReader(int columnId, ColumnEncoding.Types.Kind encoding) : base(columnId)
    {
        _encoding = encoding;
    }

    public void SetDataStream(OrcByteStream stream) => _dataStream = stream;

    public void SetLengthStream(OrcByteStream stream)
    {
        _lengthStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _lengthDecoderV2 = new RleDecoderV2(stream, signed: false);
        else
            _lengthDecoderV1 = new RleDecoderV1(stream, signed: false);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

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
            int totalBytes = 0;
            for (int i = 0; i < nonNullCount; i++)
                totalBytes += (int)lengths[i];

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

            // Read all binary data contiguously
            using var dataBuf = new NativeBuffer<byte>(currentOffset > 0 ? currentOffset : 1, zeroFill: false);
            if (currentOffset > 0 && _dataStream != null)
                _dataStream.ReadExactly(dataBuf.Span.Slice(0, currentOffset));

            var nullBuffer = CreateValidityBuffer(present, batchSize);
            var arrayData = new ArrayData(Apache.Arrow.Types.BinaryType.Default, batchSize, nullCount, 0,
                [nullBuffer, offsetBuf.Build(), dataBuf.Build()]);
            return new BinaryArray(arrayData);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(lengthPool);
        }
    }
}
