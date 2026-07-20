// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

internal sealed class TimestampColumnReader : ColumnReader
{
    private readonly ColumnEncoding.Types.Kind _encoding;
    private readonly bool _isInstant;
    private OrcByteStream? _dataStream;
    private OrcByteStream? _secondaryStream;
    private RleDecoderV2? _dataDecoderV2;
    private RleDecoderV1? _dataDecoderV1;
    private RleDecoderV2? _secondaryDecoderV2;
    private RleDecoderV1? _secondaryDecoderV1;

    public TimestampColumnReader(int columnId, ColumnEncoding.Types.Kind encoding, bool isInstant) : base(columnId)
    {
        _encoding = encoding;
        _isInstant = isInstant;
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _dataDecoderV2 = new RleDecoderV2(stream, signed: true);
        else
            _dataDecoderV1 = new RleDecoderV1(stream, signed: true);
    }

    public void SetSecondaryStream(OrcByteStream stream)
    {
        _secondaryStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _secondaryDecoderV2 = new RleDecoderV2(stream, signed: false);
        else
            _secondaryDecoderV1 = new RleDecoderV1(stream, signed: false);
    }

    public override IArrowArray ReadBatch(int batchSize)
    {
        var present = ReadPresent(batchSize);
        int nonNullCount = CountNonNull(present, batchSize);
        int nullCount = present == null ? 0 : batchSize - nonNullCount;

        var secPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        var datPool = ArrayPool<long>.Shared.Rent(nonNullCount);
        try
        {
            var seconds = datPool.AsSpan(0, nonNullCount);
            var nanos = secPool.AsSpan(0, nonNullCount);

            if (nonNullCount > 0)
            {
                if (_dataDecoderV2 != null) _dataDecoderV2.ReadValues(seconds);
                else _dataDecoderV1?.ReadValues(seconds);

                if (_secondaryDecoderV2 != null) _secondaryDecoderV2.ReadValues(nanos);
                else _secondaryDecoderV1?.ReadValues(nanos);
            }

            for (int i = 0; i < nonNullCount; i++)
                nanos[i] = DecodeNanos(nanos[i]);

            string? tz = _isInstant ? "UTC" : null;
            var arrowType = new TimestampType(TimeUnit.Nanosecond, tz);

            using var buf = new NativeBuffer<long>(batchSize, zeroFill: nullCount > 0);
            var epochNanos = buf.Span;
            int rawIdx = 0;

            using var nullBuf = new NativeBuffer<byte>((batchSize + 7) / 8, zeroFill: true);
            var nullBits = nullBuf.Span;

            for (int i = 0; i < batchSize; i++)
            {
                if (present != null && !present[i])
                    continue;

                nullBits[i >> 3] |= (byte)(1 << (i & 7));
                epochNanos[i] = seconds[rawIdx] * 1_000_000_000L + nanos[rawIdx];
                rawIdx++;
            }

            var valueBuffer = buf.Build();
            var nullBuffer = nullCount > 0 ? nullBuf.Build() : ArrowBuffer.Empty;
            return new TimestampArray(arrowType, valueBuffer, nullBuffer, batchSize, nullCount, 0);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(datPool);
            ArrayPool<long>.Shared.Return(secPool);
        }
    }

    private static readonly long[] ScaleMultipliers = [1, 1_000, 1_000_000, 1_000_000_000,
        1_000_000_000_000, 1_000_000_000_000_000, 1_000_000_000_000_000_000, 0 /* unused */];

    private static long DecodeNanos(long encoded)
    {
        if (encoded == 0) return 0;
        int scale = (int)(encoded & 0x07);
        long nanos = (long)((ulong)encoded >> 3);
        return nanos * ScaleMultipliers[scale];
    }
}
