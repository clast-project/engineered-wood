// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class BooleanColumnReader : ColumnReader
{
    private OrcByteStream? _dataStream;
    private BooleanDecoder? _decoder;

    public BooleanColumnReader(int columnId) : base(columnId) { }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        _decoder = new BooleanDecoder(stream);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        var rawValues = new bool[nonNullCount];
        if (nonNullCount > 0)
        {
            _decoder?.ReadValues(rawValues);
        }

        int byteCount = (batchSize + 7) / 8;
        using var valueBuf = new NativeBuffer<byte>(byteCount, zeroFill: true);
        var valueBytes = valueBuf.Span;

        int rawIdx = 0;
        for (int i = 0; i < batchSize; i++)
        {
            if (present == null || present[i])
            {
                if (rawValues[rawIdx++])
                    valueBytes[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        var validityBuffer = CreateValidityBuffer(present, batchSize);
        return new BooleanArray(valueBuf.Build(), validityBuffer, batchSize, nullCount, 0);
    }
}
