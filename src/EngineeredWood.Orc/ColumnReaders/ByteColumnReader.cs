// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class ByteColumnReader : ColumnReader
{
    private OrcByteStream? _dataStream;
    private ByteRleDecoder? _decoder;

    public ByteColumnReader(int columnId) : base(columnId) { }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        _decoder = new ByteRleDecoder(stream);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        var rawValues = new byte[nonNullCount];
        if (nonNullCount > 0)
        {
            _decoder?.ReadValues(rawValues);
        }

        using var buf = new NativeBuffer<sbyte>(batchSize, zeroFill: nullCount > 0);
        var values = buf.Span;
        int rawIdx = 0;
        for (int i = 0; i < batchSize; i++)
        {
            if (present == null || present[i])
                values[i] = (sbyte)rawValues[rawIdx++];
        }

        var nullBuffer = CreateValidityBuffer(present, batchSize);
        return new Int8Array(buf.Build(), nullBuffer, batchSize, nullCount, 0);
    }
}
