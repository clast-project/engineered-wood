// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class ListColumnReader : ColumnReader
{
    private readonly OrcSchema _schema;
    private readonly ColumnEncoding.Types.Kind _encoding;
    private ColumnReader? _elementReader;
    private OrcByteStream? _lengthStream;
    private RleDecoderV2? _lengthDecoderV2;
    private RleDecoderV1? _lengthDecoderV1;

    public ListColumnReader(OrcSchema schema, ColumnEncoding.Types.Kind encoding) : base(schema.ColumnId)
    {
        _schema = schema;
        _encoding = encoding;
    }

    public void SetElementReader(ColumnReader reader) => _elementReader = reader;
    public ColumnReader? ElementReader => _elementReader;

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
        int nullCount = batchSize - nonNullCount;

        var lengthPool = System.Buffers.ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var lengths = lengthPool.AsSpan(0, nonNullCount);
            if (nonNullCount > 0)
            {
                if (_lengthDecoderV2 != null) _lengthDecoderV2.ReadValues(lengths);
                else _lengthDecoderV1?.ReadValues(lengths);
            }

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

            IArrowArray elements;
            if (_elementReader != null && currentOffset > 0)
            {
                elements = _elementReader.ReadBatch(currentOffset);
            }
            else
            {
                var elementType = _schema.Children[0].ToArrowType();
                elements = CreateEmptyArray(elementType);
            }

            var validityBuffer = CreateValidityBuffer(present, batchSize);
            var listType = new ListType(new Field("item", _schema.Children[0].ToArrowType(), true));

            return new ListArray(listType, batchSize, offsetBuf.Build(), elements, validityBuffer, nullCount);
        }
        finally
        {
            System.Buffers.ArrayPool<long>.Shared.Return(lengthPool);
        }
    }

    private static IArrowArray CreateEmptyArray(IArrowType type)
    {
        if (type is StringType)
            return new StringArray.Builder().Build();
        if (type is Int32Type)
            return new Int32Array.Builder().Build();
        if (type is Int64Type)
            return new Int64Array.Builder().Build();
        return new StringArray.Builder().Build();
    }
}
