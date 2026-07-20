// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class MapColumnReader : ColumnReader
{
    private readonly OrcSchema _schema;
    private readonly ColumnEncoding.Types.Kind _encoding;
    private ColumnReader? _keyReader;
    private ColumnReader? _valueReader;
    private OrcByteStream? _lengthStream;
    private RleDecoderV2? _lengthDecoderV2;
    private RleDecoderV1? _lengthDecoderV1;

    public MapColumnReader(OrcSchema schema, ColumnEncoding.Types.Kind encoding) : base(schema.ColumnId)
    {
        _schema = schema;
        _encoding = encoding;
    }

    public void SetKeyReader(ColumnReader reader) => _keyReader = reader;
    public void SetValueReader(ColumnReader reader) => _valueReader = reader;
    public ColumnReader? KeyReader => _keyReader;
    public ColumnReader? ValueReader => _valueReader;

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

            IArrowArray keys, values;
            if (currentOffset > 0)
            {
                keys = _keyReader!.ReadBatch(currentOffset);
                values = _valueReader!.ReadBatch(currentOffset);
            }
            else
            {
                keys = new StringArray.Builder().Build();
                values = new StringArray.Builder().Build();
            }

            var keyField = new Field("key", _schema.Children[0].ToArrowType(), false);
            var valueField = new Field("value", _schema.Children[1].ToArrowType(), true);
            var structFields = new StructType(new[] { keyField, valueField });
            var structArray = new StructArray(structFields, currentOffset, new[] { keys, values }, ArrowBuffer.Empty, 0);

            var validityBuffer = CreateValidityBuffer(present, batchSize);
            var mapType = new MapType(keyField, valueField);
            return new MapArray(mapType, batchSize, offsetBuf.Build(), structArray, validityBuffer, nullCount);
        }
        finally
        {
            System.Buffers.ArrayPool<long>.Shared.Return(lengthPool);
        }
    }
}
