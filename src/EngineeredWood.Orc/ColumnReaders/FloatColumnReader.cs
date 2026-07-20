// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using EngineeredWood.Orc.Encodings;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class FloatColumnReader : ColumnReader
{
    private OrcByteStream? _dataStream;

    public FloatColumnReader(int columnId) : base(columnId) { }

    public void SetDataStream(OrcByteStream stream) => _dataStream = stream;

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        using var buf = new NativeBuffer<float>(batchSize, zeroFill: nullCount > 0);
        if (_dataStream != null)
        {
            var values = buf.Span;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
#if NET8_0_OR_GREATER
                    values[i] = BinaryPrimitives.ReadSingleLittleEndian(_dataStream.ReadSpan(4));
#else
                    values[i] = MemoryMarshal.Read<float>(_dataStream.ReadSpan(4));
#endif
            }
        }

        var nullBuffer = CreateValidityBuffer(present, batchSize);
        return new FloatArray(buf.Build(), nullBuffer, batchSize, nullCount, 0);
    }
}

internal sealed class DoubleColumnReader : ColumnReader
{
    private OrcByteStream? _dataStream;

    public DoubleColumnReader(int columnId) : base(columnId) { }

    public void SetDataStream(OrcByteStream stream) => _dataStream = stream;

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        using var buf = new NativeBuffer<double>(batchSize, zeroFill: nullCount > 0);
        if (_dataStream != null)
        {
            var values = buf.Span;
            for (int i = 0; i < batchSize; i++)
            {
                if (present == null || present[i])
#if NET8_0_OR_GREATER
                    values[i] = BinaryPrimitives.ReadDoubleLittleEndian(_dataStream.ReadSpan(8));
#else
                    values[i] = MemoryMarshal.Read<double>(_dataStream.ReadSpan(8));
#endif
            }
        }

        var nullBuffer = CreateValidityBuffer(present, batchSize);
        return new DoubleArray(buf.Build(), nullBuffer, batchSize, nullCount, 0);
    }
}
