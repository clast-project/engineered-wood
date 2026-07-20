// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class IntegerColumnReader : ColumnReader
{
    private readonly ColumnEncoding.Types.Kind _encoding;
    private readonly IArrowType _arrowType;
    private OrcByteStream? _dataStream;
    private RleDecoderV2? _decoderV2;
    private RleDecoderV1? _decoderV1;

    public IntegerColumnReader(int columnId, ColumnEncoding.Types.Kind encoding, IArrowType arrowType) : base(columnId)
    {
        _encoding = encoding;
        _arrowType = arrowType;
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2 or ColumnEncoding.Types.Kind.DictionaryV2)
            _decoderV2 = new RleDecoderV2(stream, signed: true);
        else
            _decoderV1 = new RleDecoderV1(stream, signed: true);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);

        var rawPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var rawValues = rawPool.AsSpan(0, nonNullCount);
            if (nonNullCount > 0)
            {
                if (_decoderV2 != null)
                    _decoderV2.ReadValues(rawValues);
                else
                    _decoderV1?.ReadValues(rawValues);
            }

            return BuildArrowArray(rawValues, present, batchSize, nonNullCount);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(rawPool);
        }
    }

    private IArrowArray BuildArrowArray(ReadOnlySpan<long> rawValues, bool[]? present, int batchSize, int nonNullCount)
    {
        int nullCount = present == null ? 0 : batchSize - nonNullCount;
        var nullBuffer = CreateValidityBuffer(present, batchSize);
        int rawIdx = 0;

        if (_arrowType is Int16Type)
        {
            using var buf = new NativeBuffer<short>(batchSize, zeroFill: nullCount > 0);
            var values = buf.Span;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
                    values[i] = (short)rawValues[rawIdx++];
            }
            return new Int16Array(buf.Build(), nullBuffer, batchSize, nullCount, 0);
        }
        else if (_arrowType is Int32Type)
        {
            using var buf = new NativeBuffer<int>(batchSize, zeroFill: nullCount > 0);
            var values = buf.Span;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
                    values[i] = (int)rawValues[rawIdx++];
            }
            return new Int32Array(buf.Build(), nullBuffer, batchSize, nullCount, 0);
        }
        else
        {
            using var buf = new NativeBuffer<long>(batchSize, zeroFill: nullCount > 0);
            var values = buf.Span;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
                    values[i] = rawValues[rawIdx++];
            }
            return new Int64Array(buf.Build(), nullBuffer, batchSize, nullCount, 0);
        }
    }
}
