// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Arrow;
using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Encodings;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.ColumnReaders;

/// <summary>
/// Reads an ORC <c>TIMESTAMP</c> or <c>TIMESTAMP_INSTANT</c> column as nanoseconds since the Unix epoch: naive
/// (no zone) for <c>TIMESTAMP</c>, holding the wall clock it was written with, and UTC for
/// <c>TIMESTAMP_INSTANT</c>.
/// </summary>
/// <remarks>
/// <para><b>Encoding.</b> DATA holds signed seconds since <c>2015-01-01 00:00:00</c>: in UTC for
/// <c>TIMESTAMP_INSTANT</c>, and in the stripe's writer time zone for <c>TIMESTAMP</c> (UTC when the stripe
/// names none). SECONDARY holds the nanoseconds, with trailing decimal zeros removed: the low three bits
/// <c>z</c> say that the value was divided by <c>10^(z+1)</c> (0: not at all). See <see cref="ParseNanos"/>.</para>
/// <para><b>Negative timestamps.</b> The C++ writer (pyarrow, Arrow) splits a value by truncation toward zero,
/// so the nanoseconds carry the value's sign and adding them to the seconds is exact. The Java writer stores
/// seconds truncated from milliseconds and always-positive nanoseconds, so a timestamp before 1970 with at least
/// a millisecond's fraction comes out one second late. The C++ reader takes a second off exactly then (seconds
/// below zero and nanoseconds above 999,999), and so does this one: both writers' files read as the C++ reader
/// reads them. In a writer time zone other than UTC (only Java writers name one), the test is made on the
/// instant before converting to the wall clock, as the Java reader makes it; the C++ reader makes it on the
/// wall clock, which takes a second off values between the epoch and the zone's offset from it.</para>
/// <para><b>Files EngineeredWood wrote before this was fixed</b> (<c>software_version</c>
/// <c>"EngineeredWood.Orc 0.1"</c>) used the Unix epoch and their own nanosecond scale (#584). They are read
/// with those rules, so that a file read back before the fix still reads the same.</para>
/// </remarks>
internal sealed class TimestampColumnReader : ColumnReader
{
    /// <summary>2015-01-01T00:00:00Z, in seconds since the Unix epoch: the base of ORC's DATA stream.</summary>
    internal const long OrcEpochSeconds = 1_420_070_400;

    private readonly ColumnEncoding.Types.Kind _encoding;
    private readonly bool _isInstant;
    private OrcByteStream? _dataStream;
    private OrcByteStream? _secondaryStream;
    private RleDecoderV2? _dataDecoderV2;
    private RleDecoderV1? _dataDecoderV1;
    private RleDecoderV2? _secondaryDecoderV2;
    private RleDecoderV1? _secondaryDecoderV1;

    private bool _legacyEngineeredWood;
    private OrcWriterTimeZone? _writerZone;

    public TimestampColumnReader(int columnId, ColumnEncoding.Types.Kind encoding, bool isInstant) : base(columnId)
    {
        _encoding = encoding;
        _isInstant = isInstant;
    }

    /// <summary>
    /// What the stripe's footer and the file say about how to read this column: the stripe's writer time zone
    /// (null or empty when it names none), and whether the file was written by EngineeredWood before #584.
    /// </summary>
    internal void Configure(string? writerTimeZone, bool legacyEngineeredWood)
    {
        _legacyEngineeredWood = legacyEngineeredWood;
        // TIMESTAMP_INSTANT is UTC by definition; only TIMESTAMP is relative to the writer's zone.
        _writerZone = _isInstant || legacyEngineeredWood ? null : OrcWriterTimeZone.For(writerTimeZone);
    }

    public void SetDataStream(OrcByteStream stream)
    {
        _dataStream = stream;
        if (_encoding is ColumnEncoding.Types.Kind.DirectV2)
            _dataDecoderV2 = new RleDecoderV2(stream, signed: true, legacyPatchedBase: LegacyEngineeredWood);
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
                epochNanos[i] = _legacyEngineeredWood
                    ? LegacyValue(seconds[rawIdx], nanos[rawIdx])
                    : Value(seconds[rawIdx], nanos[rawIdx], _writerZone);
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

    /// <summary>
    /// One value: <paramref name="data"/> and <paramref name="secondary"/> as stored, to nanoseconds since the
    /// Unix epoch. With <paramref name="zone"/> (a <c>TIMESTAMP</c> written in a zone other than UTC), the result
    /// is the wall clock in that zone, as the instant's UTC fields.
    /// </summary>
    internal static long Value(long data, long secondary, OrcWriterTimeZone? zone)
    {
        long seconds = zone is null ? data + OrcEpochSeconds : zone.InstantSeconds(data);
        long nanos = ParseNanos(secondary);
        // The Java writer's quirk (see the remarks). It truncated the instant, so it is undone on the instant,
        // before any zone conversion, as the Java reader does.
        if (seconds < 0 && nanos > 999_999)
            seconds--;
        if (zone is not null)
            seconds = zone.WallClockSeconds(seconds);

        // Seconds and nanoseconds of opposite signs are brought to one sign first, so that only a result that
        // does not fit overflows, not an intermediate product (long.MinValue nanoseconds is -9,223,372,037 s
        // plus 145,224,192 ns).
        if (seconds < 0 && nanos > 0)
        {
            seconds++;
            nanos -= 1_000_000_000L;
        }
        else if (seconds > 0 && nanos < 0)
        {
            seconds--;
            nanos += 1_000_000_000L;
        }
        return checked(seconds * 1_000_000_000L + nanos);
    }

    private static readonly long[] PowersOfTen =
        [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000];

    /// <summary>
    /// ORC's nanosecond encoding: the low three bits <c>z</c> count the trailing zeros removed, less one (0: none
    /// were), and the rest is the value, SIGNED (the C++ writer stores a negative value's nanoseconds negative).
    /// </summary>
    internal static long ParseNanos(long secondary)
    {
        int zeros = (int)(secondary & 7);
        long value = secondary >> 3;
        return zeros == 0 ? value : value * PowersOfTen[zeros + 1];
    }

    private static readonly long[] LegacyScale = [1, 1_000, 1_000_000, 1_000_000_000, 0, 0, 0, 0];

    /// <summary>A value as EngineeredWood wrote it before #584: Unix-epoch seconds, and nanoseconds whose low
    /// three bits counted thousands removed.</summary>
    private static long LegacyValue(long data, long secondary)
    {
        long nanos = secondary == 0 ? 0 : (long)((ulong)secondary >> 3) * LegacyScale[secondary & 7];
        return data * 1_000_000_000L + nanos;
    }
}

/// <summary>
/// A stripe's writer time zone, for reading a <c>TIMESTAMP</c> column: its DATA stream counts seconds from
/// <c>2015-01-01 00:00:00</c> in this zone, and the wall clock a value was written with is its instant in this
/// zone. Null from <see cref="For"/> means UTC, where no conversion is needed.
/// </summary>
internal sealed class OrcWriterTimeZone
{
    private readonly TimeZoneInfo _zone;
    private readonly long _epochSeconds;

    // Offsets by 15-minute bucket, kept only for buckets with one offset at both ends; see Offset.
    private readonly Dictionary<long, long> _offsets = [];

    private OrcWriterTimeZone(TimeZoneInfo zone)
    {
        _zone = zone;
        // 2015-01-01 00:00:00 in the zone, as a UTC instant.
        var local = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        _epochSeconds = TimestampColumnReader.OrcEpochSeconds - (long)zone.GetUtcOffset(local).TotalSeconds;
    }

    /// <summary>The zone named <paramref name="name"/>, or null for UTC (also when the name is empty, as when a
    /// stripe names none: every implementation reads such a stripe as UTC).</summary>
    internal static OrcWriterTimeZone? For(string? name)
    {
        if (string.IsNullOrEmpty(name) || IsUtc(name!))
            return null;

        TimeZoneInfo zone;
        try
        {
            zone = TimeZoneInfo.FindSystemTimeZoneById(name!);
        }
        catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            throw new NotSupportedException(
                $"The ORC file's TIMESTAMP values were written in time zone '{name}', which this runtime cannot " +
                "resolve, so their wall-clock times cannot be computed. IANA zone names need .NET 6 or later " +
                "(on Windows, with ICU).", ex);
        }

        return zone.BaseUtcOffset == TimeSpan.Zero && !zone.SupportsDaylightSavingTime
            && zone.GetAdjustmentRules().Length == 0
            ? null
            : new OrcWriterTimeZone(zone);
    }

    private static bool IsUtc(string name) =>
        name is "UTC" or "GMT" or "Z" or "Etc/UTC" or "Etc/GMT" or "Etc/UCT" or "UCT" or "Etc/Universal"
            or "Universal" or "Etc/Zulu" or "Zulu" or "Etc/Greenwich" or "Greenwich" or "GMT0" or "Etc/GMT0"
            or "GMT+0" or "GMT-0" or "Etc/GMT+0" or "Etc/GMT-0";

    /// <summary>The instant, in seconds since the Unix epoch, <paramref name="data"/> seconds after
    /// 2015-01-01 00:00:00 in this zone.</summary>
    internal long InstantSeconds(long data) => data + _epochSeconds;

    /// <summary>The wall clock in this zone, as seconds read as UTC, of <paramref name="instant"/> (seconds since
    /// the Unix epoch).</summary>
    internal long WallClockSeconds(long instant) => instant + Offset(instant);

    private long Offset(long instant)
    {
        // A zone's offset changes only at its transitions, so it is cached per 15-minute bucket; a bucket is
        // cached only when its two ends agree, so the one holding a transition is always computed exactly.
        long bucket = FloorDiv(instant, 900);
        if (_offsets.TryGetValue(bucket, out long cached))
            return cached;

        long start = OffsetAt(bucket * 900);
        long end = OffsetAt(bucket * 900 + 899);
        if (start == end)
        {
            _offsets[bucket] = start;
            return start;
        }
        return OffsetAt(instant);
    }

    private long OffsetAt(long instant) =>
        (long)_zone.GetUtcOffset(DateTimeOffset.FromUnixTimeSeconds(instant)).TotalSeconds;

    private static long FloorDiv(long a, long b)
    {
        long q = a / b;
        return (a % b != 0 && (a < 0) != (b < 0)) ? q - 1 : q;
    }
}
