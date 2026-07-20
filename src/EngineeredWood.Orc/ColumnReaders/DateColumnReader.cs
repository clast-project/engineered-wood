// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class DateColumnReader : ColumnReader
{
    private readonly ColumnEncoding.Types.Kind _encoding;
    private OrcByteStream? _dataStream;
    private RleDecoderV2? _decoderV2;
    private RleDecoderV1? _decoderV1;

    public DateColumnReader(int columnId, ColumnEncoding.Types.Kind encoding) : base(columnId)
    {
        _encoding = encoding;
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _decoderV2 = new RleDecoderV2(stream, signed: true);
        else
            _decoderV1 = new RleDecoderV1(stream, signed: true);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        var rawPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var rawValues = rawPool.AsSpan(0, nonNullCount);
            if (nonNullCount > 0)
            {
                if (_decoderV2 != null) _decoderV2.ReadValues(rawValues);
                else _decoderV1?.ReadValues(rawValues);
            }

            using var buf = new NativeBuffer<int>(batchSize, zeroFill: nullCount > 0);
            var values = buf.Span;
            int rawIdx = 0;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
                    values[i] = (int)rawValues[rawIdx++];
            }

            var nullBuffer = CreateValidityBuffer(present, batchSize);
            var arrayData = new ArrayData(Date32Type.Default, batchSize, nullCount, 0,
                [nullBuffer, buf.Build()]);
            return new Date32Array(arrayData);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(rawPool);
        }
    }
}
