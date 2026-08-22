// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#pragma warning disable EWPARQUET0004 // These tests exist to exercise the experimental carrier.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Reading the extended-precision timestamp carrier proposed in apache/parquet-format#600: TIMESTAMP on
/// FIXED_LEN_BYTE_ARRAY(12), holding a signed 96-bit little-endian count of the declared unit since the
/// epoch.
///
/// Pinned against <c>flba12_timestamp.parquet</c> from apache/parquet-testing#123 — three columns
/// (millis, micros, nanos) over the same six timestamps, two of which need more than 64 bits in
/// nanoseconds. Values below are the fixture's own documented table, not something this reader produced.
///
/// No-ops when the fixture is absent, which it is until #123 merges and the submodule is bumped.
/// </summary>
public class ExtendedTimestampReadTests
{
    private const string Fixture = "flba12_timestamp.parquet";

    private static readonly string[] Columns = ["timestamp_millis", "timestamp_micros", "timestamp_nanos"];

    // Epoch seconds, in row order, from the fixture's README table.
    private static readonly long[] EpochSeconds =
    [
        0,              // 1970-01-01T00:00:00Z — all-zero bytes
        1,              // 1970-01-01T00:00:01Z
        -1,             // 1969-12-31T23:59:59Z — exercises two's complement
        9_223_372_036,  // 2262-04-11T23:47:16Z — near the INT64-nanos max
        253_402_300_799, // 9999-12-31T23:59:59Z — NANOS exceeds INT64
        -62_135_596_800, // 0001-01-01T00:00:00Z — NANOS below INT64 minimum
    ];

    private const int MinRow = 5; // year 0001
    private const int MaxRow = 4; // year 9999

    private static long ScaleOf(TimeUnit unit) => unit switch
    {
        TimeUnit.Millisecond => 1_000L,
        TimeUnit.Microsecond => 1_000_000L,
        _ => 1_000_000_000L,
    };

    private static TimeUnit DeclaredUnitOf(string column) => column switch
    {
        "timestamp_millis" => TimeUnit.Millisecond,
        "timestamp_micros" => TimeUnit.Microsecond,
        _ => TimeUnit.Nanosecond,
    };

    private static string? FixturePath()
    {
        var path = TestData.GetPath(Fixture);
        return File.Exists(path) ? path : null;
    }

    private static async Task<IArrowArray?> ReadAsync(string column, ParquetReadOptions? options = null)
    {
        var path = FixturePath();
        if (path is null) return null;

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var batch = await reader.ReadRowGroupAsync(0, [column]);
        return batch.Column(0);
    }

    [Theory]
    [InlineData("timestamp_millis")]
    [InlineData("timestamp_micros")]
    public async Task ReadsAtTheDeclaredUnit_ByDefault(string column)
    {
        var array = await ReadAsync(column);
        if (array is null) return;

        var timestamps = Assert.IsType<TimestampArray>(array);
        var type = Assert.IsType<TimestampType>(timestamps.Data.DataType);
        var unit = DeclaredUnitOf(column);

        Assert.Equal(unit, type.Unit);
        // The fixture declares isAdjustedToUTC on all three columns.
        Assert.Equal("UTC", type.Timezone);
        Assert.Equal(EpochSeconds.Length, timestamps.Length);

        long scale = ScaleOf(unit);
        for (int i = 0; i < EpochSeconds.Length; i++)
        {
            Assert.Equal(EpochSeconds[i] * scale, timestamps.Values[i]);
        }
    }

    [Fact]
    public async Task NanosecondsOutOfInt64RangeAreReported_NotWrapped()
    {
        var path = FixturePath();
        if (path is null) return;

        // Two of the six rows are outside timestamp[ns]. Wrapping them would produce a plausible date,
        // which is the failure mode worth refusing.
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var error = await Assert.ThrowsAsync<ParquetFormatException>(
            async () => await reader.ReadRowGroupAsync(0, ["timestamp_nanos"]));

        Assert.Contains("timestamp_nanos", error.Message, StringComparison.Ordinal);
        Assert.Contains("TimestampMicroseconds", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("timestamp_millis")]
    [InlineData("timestamp_micros")]
    [InlineData("timestamp_nanos")]
    public async Task MicrosecondsReadEveryRow(string column)
    {
        // Microseconds span roughly ±292,000 years, so this mode never reports a range error — including
        // for the nanosecond column that the declared-unit default refuses.
        var array = await ReadAsync(
            column,
            new ParquetReadOptions
            {
                ExtendedTimestampOutput = ExtendedTimestampOutputKind.TimestampMicroseconds,
            });
        if (array is null) return;

        var timestamps = Assert.IsType<TimestampArray>(array);
        var type = Assert.IsType<TimestampType>(timestamps.Data.DataType);
        Assert.Equal(TimeUnit.Microsecond, type.Unit);

        for (int i = 0; i < EpochSeconds.Length; i++)
        {
            Assert.Equal(EpochSeconds[i] * 1_000_000L, timestamps.Values[i]);
        }
    }

    [Theory]
    [InlineData("timestamp_millis")]
    [InlineData("timestamp_micros")]
    [InlineData("timestamp_nanos")]
    public async Task RawBytesAreTheFixtureBytes(string column)
    {
        var array = await ReadAsync(
            column,
            new ParquetReadOptions
            {
                ExtendedTimestampOutput = ExtendedTimestampOutputKind.FixedSizeBinary,
            });
        if (array is null) return;

        var bytes = Assert.IsType<FixedSizeBinaryArray>(array);
        Assert.Equal(12, ((FixedSizeBinaryType)bytes.Data.DataType).ByteWidth);
        Assert.Equal(EpochSeconds.Length, bytes.Length);

        long scale = ScaleOf(DeclaredUnitOf(column));
        for (int i = 0; i < EpochSeconds.Length; i++)
        {
            // Rebuilt from the documented epoch seconds, so this compares the file against the spec
            // rather than against our own decoder.
            var expected = new byte[12];
            Int128 value = (Int128)EpochSeconds[i] * (Int128)scale;
            for (int b = 0; b < 12; b++)
            {
                expected[b] = (byte)(ulong)(value & (Int128)0xFF);
                value >>= 8;
            }

            Assert.Equal(expected, bytes.GetBytes(i).ToArray());
        }
    }

    [Fact]
    public async Task TheExtremeRowsAreTheOnesThatNeedTheWiderCarrier()
    {
        // Guards the fixture's premise: if these two rows ever fit an int64 nanosecond count, the file has
        // stopped testing what it exists to test and the default-mode refusal above would go quiet.
        var array = await ReadAsync(
            "timestamp_nanos",
            new ParquetReadOptions
            {
                ExtendedTimestampOutput = ExtendedTimestampOutputKind.FixedSizeBinary,
            });
        if (array is null) return;

        var bytes = Assert.IsType<FixedSizeBinaryArray>(array);
        foreach (int row in new[] { MinRow, MaxRow })
        {
            var high = bytes.GetBytes(row).Slice(8).ToArray();
            // A value that fit int64 would have its top four bytes as pure sign extension of byte 7.
            byte signFill = (byte)((bytes.GetBytes(row)[7] & 0x80) != 0 ? 0xFF : 0x00);
            Assert.False(
                high[0] == signFill && high[1] == signFill && high[2] == signFill && high[3] == signFill,
                $"row {row} fits an int64 nanosecond count, so it no longer tests the wider carrier");
        }
    }
}
