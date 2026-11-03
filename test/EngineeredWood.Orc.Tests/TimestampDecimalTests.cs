// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.IO.Compression;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.ColumnReaders;
using EngineeredWood.Orc.ColumnWriters;

namespace EngineeredWood.Orc.Tests;

/// <summary>
/// ORC timestamps (#584) and decimals (#583) as the rest of the ORC world writes and reads them. The fixtures'
/// expected values are the Apache ORC project's own dumps (<c>TestData/expected</c>, from its Java tools), which
/// hold the timestamps as wall clocks and the decimals at the column's scale; every row is compared.
/// </summary>
public class TimestampDecimalTests
{
    private static List<JsonElement> Expected(string orcFileName)
    {
        var gzPath = Path.Combine(
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "TestData", "expected")),
            Path.GetFileNameWithoutExtension(orcFileName) + ".jsn.gz");
        var rows = new List<JsonElement>();
        using var gz = new GZipStream(File.OpenRead(gzPath), CompressionMode.Decompress);
        using var sr = new StreamReader(gz);
        string? line;
        while ((line = sr.ReadLine()) != null)
        {
            if (!string.IsNullOrWhiteSpace(line))
                rows.Add(JsonDocument.Parse(line).RootElement.Clone());
        }
        return rows;
    }

    private static async Task<List<IArrowArray>> ReadColumnAsync(string orcFileName, string column)
    {
        await using var reader = await OrcReader.OpenAsync(TestHelpers.GetTestFilePath(orcFileName));
        var arrays = new List<IArrowArray>();
        await foreach (var batch in reader.CreateRowReader(new OrcReaderOptions { BatchSize = 1000 }))
            arrays.Add(batch.Column(column));
        return arrays;
    }

    /// <summary>A naive timestamp as the Java dump prints it: <c>yyyy-MM-dd HH:mm:ss.f…</c>, at least one fraction
    /// digit, trailing zeros dropped.</summary>
    private static string JavaTimestamp(long epochNanos)
    {
        long seconds = Math.DivRem(epochNanos, 1_000_000_000L, out long nanos);
        if (nanos < 0) { seconds--; nanos += 1_000_000_000L; }
        var wall = DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
        string fraction = nanos.ToString("D9", CultureInfo.InvariantCulture).TrimEnd('0');
        return wall.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "." + (fraction.Length == 0 ? "0" : fraction);
    }

    private static string Unscaled(Decimal128Array array, int i)
    {
        var bytes = array.ValueBuffer.Span.Slice((array.Offset + i) * 16, 16);
        var value = new Int128(
            System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)),
            System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes));
        return DecimalColumnWriter.FormatDecimal(value, ((Decimal128Type)array.Data.DataType).Scale);
    }

    [Theory]
    [InlineData("orc-file-11-format.orc", "ts")]
    [InlineData("over1k_bloom.orc", "_col8")]
    public async Task Timestamps_ReadAsTheJavaToolsDumpThem(string file, string column)
    {
        var expected = Expected(file);
        int row = 0;
        foreach (TimestampArray array in await ReadColumnAsync(file, column))
        {
            for (int i = 0; i < array.Length; i++, row++)
            {
                var want = expected[row].GetProperty(column);
                if (want.ValueKind == JsonValueKind.Null)
                    Assert.True(array.IsNull(i), $"row {row}");
                else
                    Assert.Equal(want.GetString(), JavaTimestamp(array.GetValue(i)!.Value));
            }
        }
        Assert.Equal(expected.Count, row);
    }

    [Theory]
    [InlineData("orc-file-11-format.orc", "decimal1", 38, 6)]
    [InlineData("over1k_bloom.orc", "_col9", 4, 2)]
    [InlineData("decimal.orc", "_col0", 10, 5)]
    public async Task Decimals_ReadAsTheJavaToolsDumpThem(string file, string column, int precision, int scale)
    {
        var expected = Expected(file);
        int row = 0;
        foreach (Decimal128Array array in await ReadColumnAsync(file, column))
        {
            var type = (Decimal128Type)array.Data.DataType;
            Assert.Equal((precision, scale), (type.Precision, type.Scale));
            for (int i = 0; i < array.Length; i++, row++)
            {
                var want = expected[row].GetProperty(column);
                if (want.ValueKind == JsonValueKind.Null)
                    Assert.True(array.IsNull(i), $"row {row}");
                else
                    Assert.Equal(want.GetRawText(), Unscaled(array, i));
            }
        }
        Assert.Equal(expected.Count, row);
    }

    [Fact]
    public async Task AHive11Decimal_IsDecimal38Scale6_InTheSchemaToo()
    {
        await using var reader = await OrcReader.OpenAsync(TestHelpers.GetTestFilePath("orc-file-11-format.orc"));
        var field = reader.Schema.ToArrowSchema().GetFieldByName("decimal1");
        Assert.Equal((38, 6), (((Decimal128Type)field.DataType).Precision, ((Decimal128Type)field.DataType).Scale));
    }

    // ── The encodings ────────────────────────────────────────────────────────────────────────────────────

    [Theory]
    // What pyarrow's ORC writer (C++ 2.2.2) stores, read from its files.
    [InlineData(1L, 8L)]
    [InlineData(999_999L, 7_999_992L)]
    [InlineData(1_000_000L, 13L)]
    [InlineData(500_000_000L, 47L)]
    [InlineData(-1L, -8L)]
    [InlineData(-500_000_000L, -33L)]
    [InlineData(-1_000_000L, -3L)]
    [InlineData(999_999_999L, 7_999_999_992L)]
    [InlineData(0L, 0L)]
    public void Nanos_EncodeAsTheCppWriterDoes_AndBack(long nanos, long encoded)
    {
        Assert.Equal(encoded, TimestampColumnWriter.FormatNanos(nanos));
        Assert.Equal(nanos, TimestampColumnReader.ParseNanos(encoded));
    }

    [Fact]
    public void Nanos_RoundTrip_ForEveryTrailingZeroCount()
    {
        for (long magnitude = 1; magnitude < 1_000_000_000L; magnitude *= 10)
        {
            foreach (long nanos in new[] { magnitude, 7 * magnitude, -magnitude, -3 * magnitude, magnitude + 1 })
            {
                if (Math.Abs(nanos) >= 1_000_000_000L)
                    continue;
                Assert.Equal(nanos, TimestampColumnReader.ParseNanos(TimestampColumnWriter.FormatNanos(nanos)));
            }
        }
    }

    [Theory]
    // The Java writer: seconds truncated from milliseconds, positive nanoseconds. -1.5 s is stored as -1 s and
    // 0.5 s, and the reader takes the second off.
    [InlineData(-1L - 1_420_070_400L, 47L, -1_500_000_000L)]
    // A sub-millisecond fraction needs no correction: -0.9999995 s is -1 s and 500 ns.
    [InlineData(-1L - 1_420_070_400L, 500L << 3, -999_999_500L)]
    // The C++ writer: truncated seconds, signed nanoseconds.
    [InlineData(-1L - 1_420_070_400L, -33L, -1_500_000_000L)]
    [InlineData(-1_420_070_400L, -33L, -500_000_000L)]
    public void NegativeTimestamps_ReadAsTheCppReaderReadsThem(long data, long secondary, long epochNanos)
    {
        Assert.Equal(epochNanos, TimestampColumnReader.Value(data, secondary, zone: null));
    }

    [Fact]
    public void TheExtremes_ReadWithoutAnIntermediateOverflow()
    {
        // long.MinValue ns is -9,223,372,037 s + 145,224,192 ns; a Java writer stores -9,223,372,036 s and the
        // reader takes the second off. seconds * 1e9 alone would underflow before the fraction is added.
        long minData = -9_223_372_036L - TimestampColumnReader.OrcEpochSeconds;
        Assert.Equal(long.MinValue,
            TimestampColumnReader.Value(minData, TimestampColumnWriter.FormatNanos(145_224_192), zone: null));
        // The C++ writer's form of the same value: truncated seconds, negative nanoseconds.
        Assert.Equal(long.MinValue,
            TimestampColumnReader.Value(-9_223_372_036L - TimestampColumnReader.OrcEpochSeconds,
                TimestampColumnWriter.FormatNanos(-854_775_808), zone: null));
        Assert.Equal(long.MaxValue,
            TimestampColumnReader.Value(9_223_372_036L - TimestampColumnReader.OrcEpochSeconds,
                TimestampColumnWriter.FormatNanos(854_775_807), zone: null));
        // One past the end still overflows.
        Assert.Throws<OverflowException>(() =>
            TimestampColumnReader.Value(9_223_372_036L - TimestampColumnReader.OrcEpochSeconds,
                TimestampColumnWriter.FormatNanos(854_775_808), zone: null));
    }

    [Fact]
    public async Task CoarseUnits_AreWrittenBeyondTheNanosecondRange()
    {
        // 10,000,000,000 s is 2286-11-20: past what nanoseconds hold, but its seconds and milliseconds fit.
        long[] seconds = [10_000_000_000L, -10_000_000_000L, 1];
        var type = new TimestampType(TimeUnit.Second, "UTC");
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("ts", type, false)).Build();
        var path = Path.Combine(Path.GetTempPath(), $"ew-coarse-{Guid.NewGuid():N}.orc");
        try
        {
            await using (var writer = OrcWriter.Create(path, schema))
            {
                await writer.WriteBatchAsync(new RecordBatch(schema,
                    [new TimestampArray(type, new ArrowBuffer.Builder<long>().AppendRange(seconds).Build(),
                        ArrowBuffer.Empty, seconds.Length, 0, 0)], seconds.Length));
                await writer.CloseAsync();
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var stats = reader.Footer.Statistics[1].TimestampStatistics;
            Assert.Equal(-10_000_000_000_000L, stats.MinimumUtc);
            Assert.Equal(10_000_000_000_000L, stats.MaximumUtc);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [SkippableFact]
    public void AWriterZone_GivesTheWallClockItWasWrittenWith_AcrossDaylightSaving()
    {
        OrcWriterTimeZone? zone;
        try
        {
            zone = OrcWriterTimeZone.For("America/Los_Angeles");
        }
        catch (NotSupportedException)
        {
            // .NET Framework cannot resolve IANA names; the reader says so rather than guess.
            Skip.If(true, "This runtime cannot resolve IANA time zones.");
            return;
        }
        Assert.NotNull(zone);

        // The ORC epoch in Los Angeles is 2015-01-01 08:00:00Z (PST, -8).
        const long epochLa = 1_420_070_400L + 8 * 3600;
        // A Java writer stores an instant as seconds from that epoch. 2015-07-01 19:00:00Z is 12:00 PDT (-7):
        long summer = DateTimeOffset.Parse("2015-07-01T19:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeSeconds() - epochLa;
        // and 2015-12-01 20:00:00Z is 12:00 PST (-8).
        long winter = DateTimeOffset.Parse("2015-12-01T20:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeSeconds() - epochLa;

        Assert.Equal("2015-07-01 12:00:00.0", JavaTimestamp(TimestampColumnReader.Value(summer, 0, zone)));
        Assert.Equal("2015-12-01 12:00:00.0", JavaTimestamp(TimestampColumnReader.Value(winter, 0, zone)));

        // The pre-1970 correction belongs to the instant, not the wall clock: 1970-01-01 00:00:00.5Z is after
        // the epoch, though its wall clock (1969-12-31 16:00:00.5 PST) is before it, so nothing is taken off.
        Assert.Equal("1969-12-31 16:00:00.5", JavaTimestamp(TimestampColumnReader.Value(-epochLa, 47, zone)));
        // 1969-12-31 23:59:58.5Z, as Java stores it (-1 s, 0.5 s), is corrected to 15:59:58.5 PST.
        Assert.Equal("1969-12-31 15:59:58.5", JavaTimestamp(TimestampColumnReader.Value(-1 - epochLa, 47, zone)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("GMT")]
    [InlineData("UTC")]
    [InlineData("Etc/UTC")]
    public void UtcZones_NeedNoConversion(string name)
    {
        Assert.Null(OrcWriterTimeZone.For(name));
    }

    [Fact]
    public void AnUnknownZone_IsRefused_NotGuessed()
    {
        var ex = Assert.Throws<NotSupportedException>(() => OrcWriterTimeZone.For("Not/AZone"));
        Assert.Contains("Not/AZone", ex.Message);
    }

    [Theory]
    [InlineData(2, 5, 12_345L, 12_345_000L)]
    [InlineData(5, 2, 123_456_789L, 123_456L)]
    [InlineData(5, 2, -123_456_789L, -123_456L)]
    [InlineData(3, 3, 42L, 42L)]
    public void Decimals_RescaleToTheColumn_TruncatingTowardZero(int from, int to, long value, long expected)
    {
        Assert.Equal((Int128)expected, DecimalColumnReader.Rescale(value, from, to, columnId: 1));
    }

    [Fact]
    public void ARescaleThatOverflows_IsRefused()
    {
        var big = Int128.MaxValue / 10;
        Assert.Throws<InvalidDataException>(() => DecimalColumnReader.Rescale(big, 0, 2, columnId: 1));
    }

    // ── EngineeredWood's own files ───────────────────────────────────────────────────────────────────────

    public static TheoryData<TimeUnit, string?> Units() => new()
    {
        { TimeUnit.Nanosecond, null }, { TimeUnit.Microsecond, null }, { TimeUnit.Millisecond, null },
        { TimeUnit.Second, null }, { TimeUnit.Nanosecond, "UTC" }, { TimeUnit.Microsecond, "America/New_York" },
    };

    [Theory]
    [MemberData(nameof(Units))]
    public async Task Timestamps_RoundTrip_InEveryUnit_ScaledToNanoseconds(TimeUnit unit, string? zone)
    {
        long[] values = [0, 1, -1, 1_420_070_400, -1_420_070_401, 4_102_444_800, -62_135_596_800 / 1000];
        long perUnit = unit switch
        {
            TimeUnit.Second => 1_000_000_000L,
            TimeUnit.Millisecond => 1_000_000L,
            TimeUnit.Microsecond => 1_000L,
            _ => 1L,
        };
        var type = new TimestampType(unit, zone);
        // Built from raw values so every unit is exact.
        var array = new TimestampArray(type, new ArrowBuffer.Builder<long>().AppendRange(values).Build(),
            ArrowBuffer.Empty, values.Length, 0, 0);
        var schema = new Apache.Arrow.Schema([new Field("ts", type, true)], null);

        var path = Path.Combine(Path.GetTempPath(), $"ew_orc_ts_{Guid.NewGuid():N}.orc");
        try
        {
            await using (var writer = OrcWriter.Create(path, schema))
            {
                await writer.WriteBatchAsync(new RecordBatch(schema, [array], values.Length));
                await writer.CloseAsync();
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var read = new List<long>();
            string? readZone = null;
            await foreach (var batch in reader.CreateRowReader())
            {
                var ts = (TimestampArray)batch.Column(0);
                readZone = ((TimestampType)ts.Data.DataType).Timezone;
                for (int i = 0; i < ts.Length; i++)
                    read.Add(ts.GetValue(i)!.Value);
            }

            Assert.Equal(values.Select(v => v * perUnit), read);
            // A zone-aware timestamp is an instant, whatever its zone; it comes back as UTC.
            Assert.Equal(zone is null ? null : "UTC", readZone);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Decimals_RoundTrip_Beyond64Bits()
    {
        var type = new Decimal128Type(38, 4);
        // decimal(38)'s largest magnitude: 38 nines (no Int128.Parse on .NET Framework's polyfill).
        Int128 nines = 0;
        for (int i = 0; i < 38; i++)
            nines = nines * 10 + 9;
        Int128[] values = [0, 1, -1, long.MaxValue, (Int128)long.MaxValue + 1, (Int128)long.MinValue - 1, nines, -nines];
        var bytes = new byte[values.Length * 16];
        for (int i = 0; i < values.Length; i++)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(i * 16), (ulong)values[i]);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(i * 16 + 8), (ulong)(values[i] >> 64));
        }
        var array = new Decimal128Array(new ArrayData(type, values.Length, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
        var schema = new Apache.Arrow.Schema([new Field("d", type, false)], null);

        var path = Path.Combine(Path.GetTempPath(), $"ew_orc_dec_{Guid.NewGuid():N}.orc");
        try
        {
            await using (var writer = OrcWriter.Create(path, schema))
            {
                await writer.WriteBatchAsync(new RecordBatch(schema, [array], values.Length));
                await writer.CloseAsync();
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var read = new List<string>();
            await foreach (var batch in reader.CreateRowReader())
            {
                var d = (Decimal128Array)batch.Column(0);
                for (int i = 0; i < d.Length; i++)
                    read.Add(Unscaled(d, i));
            }
            Assert.Equal(values.Select(v => DecimalColumnWriter.FormatDecimal(v, 4)), read);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task AFileEngineeredWoodWroteBeforeTheFix_StillReadsAsWritten()
    {
        // Written by EngineeredWood.Orc 0.1 (main before #584): Unix-epoch seconds and its own nanosecond scale.
        long[] written = [0, 1_500_000_000, -1_500_000_000, 1_700_000_000_123_456_789, -86_400_000_000_123, 999_999_999, -1];
        await using var reader = await OrcReader.OpenAsync(TestHelpers.GetTestFilePath("engineeredwood-0.1-timestamps.orc"));
        Assert.Equal("EngineeredWood.Orc 0.1", reader.Footer.SoftwareVersion);
        var naive = new List<long>();
        var utc = new List<long>();
        await foreach (var batch in reader.CreateRowReader())
        {
            var n = (TimestampArray)batch.Column("naive");
            var u = (TimestampArray)batch.Column("utc");
            for (int i = 0; i < n.Length; i++)
            {
                naive.Add(n.GetValue(i)!.Value);
                utc.Add(u.GetValue(i)!.Value);
            }
        }
        Assert.Equal(written, naive);
        Assert.Equal(written, utc);
    }

    // ── RLEv2 Patched Base for signed values (#584) ──────────────────────────────────────────────────────

    /// <summary>512 small signed values with three far outliers, one of them negative: data that an RLEv2
    /// encoder writes as one Patched Base run with a negative base.</summary>
    internal static long[] PatchedBaseValues()
    {
        var values = new long[512];
        for (int i = 0; i < values.Length; i++)
            values[i] = (i % 7) * 13 - 40;
        values[17] = -1_000_000_000_000;
        values[200] = 900_000_000_000;
        values[401] = -77_777_777_777;
        return values;
    }

    [Fact]
    public void SignedOutliers_AreWrittenAsPatchedBase_AndReadBack()
    {
        var values = PatchedBaseValues();
        var buffer = new EngineeredWood.Buffers.GrowableBuffer();
        var encoder = new EngineeredWood.Orc.Encodings.RleEncoderV2(buffer, signed: true);
        encoder.WriteValues(values);
        encoder.Flush();
        var bytes = buffer.WrittenSpan.ToArray();

        // The run's header: the top two bits are its encoding, 2 for Patched Base.
        Assert.Equal(2, bytes[0] >> 6);

        var decoded = new long[values.Length];
        new EngineeredWood.Orc.Encodings.RleDecoderV2(
            new EngineeredWood.Orc.Encodings.OrcByteStream(bytes, 0, bytes.Length), signed: true).ReadValues(decoded);
        Assert.Equal(values, decoded);
    }

    [Fact]
    public void UnsignedValuesWithTheTopBitSet_SurviveTheEncoder()
    {
        // SECONDARY is unsigned, but a negative timestamp's nanoseconds are a negative bit pattern: 0 and 1 ns
        // alternating, with -1 ns at row 17, are 0/8 with one -8. Patched Base cannot hold that offset.
        var values = new long[512];
        for (int i = 0; i < values.Length; i++)
            values[i] = TimestampColumnWriter.FormatNanos(i % 2);
        values[17] = TimestampColumnWriter.FormatNanos(-1);

        var buffer = new EngineeredWood.Buffers.GrowableBuffer();
        var encoder = new EngineeredWood.Orc.Encodings.RleEncoderV2(buffer, signed: false);
        encoder.WriteValues(values);
        encoder.Flush();
        var bytes = buffer.WrittenSpan.ToArray();

        var decoded = new long[values.Length];
        new EngineeredWood.Orc.Encodings.RleDecoderV2(
            new EngineeredWood.Orc.Encodings.OrcByteStream(bytes, 0, bytes.Length), signed: false).ReadValues(decoded);
        Assert.Equal(values, decoded);
    }

    [Fact]
    public async Task NegativeNanoseconds_AmongPositiveOnes_RoundTrip()
    {
        var nanos = new long[512];
        for (int i = 0; i < nanos.Length; i++)
            nanos[i] = i % 2;
        nanos[17] = -1;
        var type = new TimestampType(TimeUnit.Nanosecond, "UTC");
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("ts", type, false)).Build();
        var path = Path.Combine(Path.GetTempPath(), $"ew-negnanos-{Guid.NewGuid():N}.orc");
        try
        {
            await using (var writer = OrcWriter.Create(path, schema))
            {
                await writer.WriteBatchAsync(new RecordBatch(schema,
                    [new TimestampArray(type, new ArrowBuffer.Builder<long>().AppendRange(nanos).Build(),
                        ArrowBuffer.Empty, nanos.Length, 0, 0)], nanos.Length));
                await writer.CloseAsync();
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var read = new List<long>();
            await foreach (var batch in reader.CreateRowReader())
            {
                var ts = (TimestampArray)batch.Column("ts");
                for (int i = 0; i < ts.Length; i++)
                    read.Add(ts.GetValue(i)!.Value);
            }
            Assert.Equal(nanos, read);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task AFileEngineeredWoodWroteBeforeTheFix_KeepsItsPatchedBaseValues()
    {
        // Written by EngineeredWood.Orc 0.1, which zigzagged signed Patched Base runs.
        await using var reader = await OrcReader.OpenAsync(TestHelpers.GetTestFilePath("engineeredwood-0.1-patched-base.orc"));
        var read = new List<long>();
        await foreach (var batch in reader.CreateRowReader())
        {
            var v = (Int64Array)batch.Column("v");
            for (int i = 0; i < v.Length; i++)
                read.Add(v.GetValue(i)!.Value);
        }
        Assert.Equal(PatchedBaseValues(), read);
    }

    [Fact]
    public void APatchTooWideForOneWord_FallsBackToDirect()
    {
        // 511 zeros and one 2^62: one data bit, and a 62-bit patch padded to 64, which with the 8-bit gap is
        // 72 bits, more than the one 64-bit word a patch entry is written in.
        var values = new long[512];
        values[300] = 1L << 62;
        var buffer = new EngineeredWood.Buffers.GrowableBuffer();
        var encoder = new EngineeredWood.Orc.Encodings.RleEncoderV2(buffer, signed: false);
        encoder.WriteValues(values);
        encoder.Flush();
        var bytes = buffer.WrittenSpan.ToArray();

        var decoded = new long[values.Length];
        new EngineeredWood.Orc.Encodings.RleDecoderV2(
            new EngineeredWood.Orc.Encodings.OrcByteStream(bytes, 0, bytes.Length), signed: false).ReadValues(decoded);
        Assert.Equal(values, decoded);
    }

    [Theory]
    [InlineData("9", "10", -1)]
    [InlineData("-1", "-2", 1)]
    [InlineData("-10", "-9", -1)]
    [InlineData("-0.5", "0.25", -1)]
    [InlineData("1.50", "1.5", 0)]
    [InlineData("0.001", "0.01", -1)]
    [InlineData("-0", "0", 0)]
    [InlineData("99999999999999999999999999999999999999", "100000000000000000000000000000000000000", -1)]
    [InlineData("12.345", "12.34", 1)]
    public void DecimalStatistics_CompareByValue(string a, string b, int expected)
    {
        Assert.Equal(expected, DecimalColumnWriter.CompareDecimalStrings(a, b));
        Assert.Equal(-expected, DecimalColumnWriter.CompareDecimalStrings(b, a));
    }

    [Fact]
    public async Task DecimalStatistics_MergeAcrossStripesByValue()
    {
        var type = new Decimal128Type(10, 0);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("d", type, false)).Build();
        var path = Path.Combine(Path.GetTempPath(), $"ew-decstats-{Guid.NewGuid():N}.orc");
        try
        {
            // One stripe per batch: as strings, 9 > 10 and -1 < -2.
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { StripeSize = 1 }))
            {
                foreach (var v in new[] { 9m, 10m, -1m, -2m })
                {
                    var array = new Decimal128Array.Builder(type).Append(v).Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [array], 1));
                }
                await writer.CloseAsync();
            }
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.True(reader.NumberOfStripes > 1);
            var stats = reader.Footer.Statistics[1].DecimalStatistics;
            Assert.Equal("-2", stats.Minimum);
            Assert.Equal("10", stats.Maximum);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
