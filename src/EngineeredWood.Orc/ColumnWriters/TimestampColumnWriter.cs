// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;
using ProtoStream = EngineeredWood.Orc.Proto.Stream;

namespace EngineeredWood.Orc.ColumnWriters;

/// <summary>
/// Writes an Arrow timestamp column as ORC <c>TIMESTAMP</c> (no zone: the wall clock) or <c>TIMESTAMP_INSTANT</c>.
/// Every stripe names <c>GMT</c> as its writer time zone, so a <c>TIMESTAMP</c>'s wall clock is stored as if UTC.
/// </summary>
/// <remarks>
/// As the C++ writer (pyarrow) writes it: DATA is the seconds since <c>2015-01-01 00:00:00 UTC</c>, truncated
/// toward zero, and SECONDARY the rest in nanoseconds, with the value's sign, its trailing decimal zeros removed
/// (<see cref="FormatNanos"/>). The C++ reader reads that back exactly for every value; see
/// <see cref="ColumnReaders.TimestampColumnReader"/> for the Java writer's convention and its readers.
/// </remarks>
internal sealed class TimestampColumnWriter : ColumnWriter
{
    private readonly GrowableBuffer _dataStream = new();
    private readonly GrowableBuffer _secondaryStream = new();
    private readonly RleEncoderV2 _dataEncoder;
    private readonly RleEncoderV2 _secondaryEncoder;
    private long _minMillis = long.MaxValue;
    private long _maxMillis = long.MinValue;
    private bool _hasValues;

    public TimestampColumnWriter(int columnId) : base(columnId)
    {
        _dataEncoder = new RleEncoderV2(_dataStream, signed: true);
        _secondaryEncoder = new RleEncoderV2(_secondaryStream, signed: false);
    }

    public override void Write(IArrowArray array)
    {
        WritePresent(array);
        var a = (TimestampArray)array;
        long nanosPerUnit = ((TimestampType)a.Data.DataType).Unit switch
        {
            TimeUnit.Second => 1_000_000_000L,
            TimeUnit.Millisecond => 1_000_000L,
            TimeUnit.Microsecond => 1_000L,
            _ => 1L,
        };
        Span<long> sec = stackalloc long[1];
        Span<long> nano = stackalloc long[1];

        for (int i = 0; i < a.Length; i++)
        {
            if (!a.IsValid(i)) continue;

            long epochNanos = checked(a.GetValue(i)!.Value * nanosPerUnit);
            // Statistics and the Bloom filter are in milliseconds since the Unix epoch, floored.
            long millis = FloorDiv(epochNanos, 1_000_000L);
            _hasValues = true;
            if (millis < _minMillis) _minMillis = millis;
            if (millis > _maxMillis) _maxMillis = millis;
            BloomFilter?.AddLong(millis);

            // Truncated toward zero: the nanoseconds keep the value's sign.
            long seconds = Math.DivRem(epochNanos, 1_000_000_000L, out long nanos);

            sec[0] = seconds - ColumnReaders.TimestampColumnReader.OrcEpochSeconds;
            nano[0] = FormatNanos(nanos);
            _dataEncoder.WriteValues(sec);
            _secondaryEncoder.WriteValues(nano);
        }
    }

    /// <summary>
    /// ORC's nanosecond encoding, as the Java and C++ writers produce it: a value with at least two trailing
    /// decimal zeros is stored with up to eight of them removed, and the low three bits count them less one.
    /// Signed, for the C++ writer's negative nanoseconds.
    /// </summary>
    internal static long FormatNanos(long nanos)
    {
        if (nanos == 0 || nanos % 100 != 0)
            return nanos << 3;

        nanos /= 100;
        int zeros = 1;
        while (nanos % 10 == 0 && zeros < 7)
        {
            nanos /= 10;
            zeros++;
        }
        return (nanos << 3) | (long)zeros;
    }

    private static long FloorDiv(long a, long b)
    {
        long q = a / b;
        return (a % b != 0 && (a < 0) != (b < 0)) ? q - 1 : q;
    }

    public override ColumnStatistics GetStatistics()
    {
        var stats = base.GetStatistics();
        if (_hasValues)
        {
            // The writer zone is GMT, so the local and UTC statistics are the same milliseconds.
            stats.TimestampStatistics = new TimestampStatistics
            {
                Minimum = _minMillis,
                Maximum = _maxMillis,
                MinimumUtc = _minMillis,
                MaximumUtc = _maxMillis,
            };
        }
        return stats;
    }

    public override long EstimateBufferedBytes() => base.EstimateBufferedBytes()
        + _dataStream.Length + _dataEncoder.BufferedCount * 8L
        + _secondaryStream.Length + _secondaryEncoder.BufferedCount * 8L;

    public override ColumnEncoding GetEncoding() => new() { Kind = ColumnEncoding.Types.Kind.DirectV2 };

    public override void FlushEncoders()
    {
        base.FlushEncoders();
        _dataEncoder.Flush();
        _secondaryEncoder.Flush();
    }

    public override void GetStreamPositions(IList<ulong> positions)
    {
        base.GetStreamPositions(positions);
        positions.Add((ulong)_dataStream.Length);
        positions.Add(0); // DATA RLE remaining
        positions.Add((ulong)_secondaryStream.Length);
        positions.Add(0); // SECONDARY RLE remaining
    }

    public override void GetPositionLayout(IList<int> extrasPerStream)
    {
        base.GetPositionLayout(extrasPerStream);
        extrasPerStream.Add(1); // DATA (RLE v2): rle_remaining
        extrasPerStream.Add(1); // SECONDARY (RLE v2): rle_remaining
    }

    public override void ResetStatistics()
    {
        base.ResetStatistics();
        _minMillis = long.MaxValue;
        _maxMillis = long.MinValue;
        _hasValues = false;
    }

    public override void GetStreams(List<OrcStream> streams)
    {
        base.GetStreams(streams);
        _dataEncoder.Flush();
        _secondaryEncoder.Flush();
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Data, _dataStream));
        streams.Add(new OrcStream(ColumnId, ProtoStream.Types.Kind.Secondary, _secondaryStream));
    }

    public override void Reset()
    {
        base.Reset();
        _dataStream.Reset();
        _secondaryStream.Reset();
        _minMillis = long.MaxValue;
        _maxMillis = long.MinValue;
        _hasValues = false;
    }
}
