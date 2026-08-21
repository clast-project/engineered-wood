// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// INT96 is deprecated, but Hive, Impala and Spark before 3.0 wrote it for every timestamp, so it
/// turns up in files that already exist. These tests pin the decode against PyArrow, which is the
/// oracle the expected values below were taken from (issue #184).
/// </summary>
public class Int96TimestampTests
{
    // timestamp_col of alltypes_plain.parquet, as microseconds since the Unix epoch.
    private static readonly long[] AllTypesPlainMicros =
    [
        1235865600000000, 1235865660000000, 1238544000000000, 1238544060000000,
        1233446400000000, 1233446460000000, 1230768000000000, 1230768060000000,
    ];

    private static async Task<IArrowArray> ReadTimestampColumnAsync(
        string fileName, ParquetReadOptions? options = null, string column = "timestamp_col")
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath(fileName));
        using var reader = new ParquetFileReader(file, ownsFile: false, options);
        var batch = await reader.ReadRowGroupAsync(0, [column]);
        return batch.Column(0);
    }

    [Fact]
    public async Task Int96_ReadsAsNaiveMicrosecondTimestamp_ByDefault()
    {
        var array = await ReadTimestampColumnAsync("alltypes_plain.parquet");

        var timestamps = Assert.IsType<TimestampArray>(array);
        var type = Assert.IsType<TimestampType>(timestamps.Data.DataType);
        Assert.Equal(TimeUnit.Microsecond, type.Unit);
        // INT96 carries no zone, and every reader that decodes it presents the result as naive.
        Assert.Null(type.Timezone);
        Assert.Equal(AllTypesPlainMicros, timestamps.Values.ToArray());
    }

    [Fact]
    public async Task Int96_ReadsAsNanosecondTimestamp_WhenAsked()
    {
        var array = await ReadTimestampColumnAsync(
            "alltypes_plain.parquet",
            new ParquetReadOptions { Int96Output = Int96OutputKind.TimestampNanoseconds });

        var timestamps = Assert.IsType<TimestampArray>(array);
        var type = Assert.IsType<TimestampType>(timestamps.Data.DataType);
        Assert.Equal(TimeUnit.Nanosecond, type.Unit);
        Assert.Null(type.Timezone);
        Assert.Equal(AllTypesPlainMicros.Select(us => us * 1000), timestamps.Values.ToArray());
    }

    [Fact]
    public async Task Int96_ReadsAsRawBytes_WhenAsked()
    {
        var array = await ReadTimestampColumnAsync(
            "alltypes_plain.parquet",
            new ParquetReadOptions { Int96Output = Int96OutputKind.FixedSizeBinary });

        var bytes = Assert.IsType<FixedSizeBinaryArray>(array);
        Assert.Equal(12, Assert.IsType<FixedSizeBinaryType>(bytes.Data.DataType).ByteWidth);
        Assert.Equal(AllTypesPlainMicros.Length, bytes.Length);

        // 8 bytes of nanoseconds-within-day, then a 4-byte Julian day — the first value is midnight.
        var first = bytes.GetBytes(0).ToArray();
        Assert.Equal(0, BitConverter.ToInt64(first, 0));
        Assert.Equal(2454892, BitConverter.ToInt32(first, 8));
    }

    /// <summary>
    /// The dictionary-encoded copy of the same data takes a different decode path
    /// (<c>DictionaryDecoder</c> rather than <c>PlainDecoder</c>) into the same value buffer.
    /// </summary>
    [Fact]
    public async Task Int96_DictionaryEncoded_DecodesToTheSameTimestamps()
    {
        var array = await ReadTimestampColumnAsync("alltypes_dictionary.parquet");

        var timestamps = Assert.IsType<TimestampArray>(array);
        Assert.Equal(new[] { 1230768000000000L, 1230768060000000L }, timestamps.Values.ToArray());
    }

    [Fact]
    public async Task Int96_BatchedRead_DecodesEveryBatch()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { BatchSize = 3 });

        var seen = new List<long>();
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0, ["timestamp_col"]))
        {
            using (batch)
                seen.AddRange(Assert.IsType<TimestampArray>(batch.Column(0)).Values.ToArray());
        }

        Assert.Equal(AllTypesPlainMicros, seen);
    }

    /// <summary>
    /// <c>int96_from_spark.parquet</c> holds a year-290000 value Spark overflowed on write (see the
    /// corpus's int96_from_spark.md, which records the six microsecond values it was built from).
    /// Reading the Julian day signed and letting the microsecond arithmetic wrap inverts that
    /// overflow exactly, so all six come back — where PyArrow, reading the day unsigned, returns
    /// -7188624036226874385 for the last one.
    /// </summary>
    [Fact]
    public async Task Int96_SparkOverflowFile_ReadsAtMicrosecondResolution()
    {
        var array = await ReadTimestampColumnAsync("int96_from_spark.parquet", column: "a");

        var timestamps = Assert.IsType<TimestampArray>(array);
        Assert.Equal(6, timestamps.Length);
        Assert.Equal(1704141296123456L, timestamps.GetValue(0));
        Assert.Equal(1704070800000000L, timestamps.GetValue(1));
        Assert.Equal(253402225200000000L, timestamps.GetValue(2)); // 9999-12-31, out of ns range
        Assert.Equal(1735599600000000L, timestamps.GetValue(3));
        Assert.Null(timestamps.GetValue(4));
        Assert.Equal(9089380393200000000L, timestamps.GetValue(5)); // 290000-12-31, recovered
    }

    /// <summary>
    /// The same file at nanosecond resolution: PyArrow silently wraps 9999-12-31 round to
    /// 1816-03-29. We would rather say we cannot represent it than hand back a plausible date.
    /// </summary>
    [Fact]
    public async Task Int96_OutOfNanosecondRange_ThrowsNamingTheOptionThatReadsIt()
    {
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ReadTimestampColumnAsync(
                "int96_from_spark.parquet",
                new ParquetReadOptions { Int96Output = Int96OutputKind.TimestampNanoseconds },
                column: "a"));

        Assert.Contains("timestamp[ns]", exception.Message);
        Assert.Contains("column 'a'", exception.Message);
        Assert.Contains("Int96OutputKind.TimestampMicroseconds", exception.Message);
        Assert.Contains("Int96OutputKind.FixedSizeBinary", exception.Message);
    }

    [Fact]
    public async Task Int96_OutOfRangeFile_StillReadsAsRawBytes()
    {
        var array = await ReadTimestampColumnAsync(
            "int96_from_spark.parquet",
            new ParquetReadOptions { Int96Output = Int96OutputKind.FixedSizeBinary },
            column: "a");

        Assert.Equal(6, array.Length);
        Assert.IsType<FixedSizeBinaryArray>(array);
    }

    /// <summary>
    /// The value buffer is sized <c>capacity * 12</c> for INT96 and narrowing leaves that
    /// allocation alone, so an odd row count leaves a byte length that is not a whole number of
    /// <c>long</c>s. The nullable build path then reverse-scatters over
    /// <c>GetWritableValueSpan&lt;long&gt;()</c>, which is where that would matter. Every INT96 file
    /// in the corpus has an even row count, so the case is reached directly.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    public void Int96_OddRowCount_ScattersWithoutRunningOffTheBuffer(int rowCount)
    {
        // 2009-03-01T00:00:00Z, then one row later per minute; the last row is null.
        const int JulianDay = 2454892;
        int nonNull = rowCount - 1;

        using var state = new ColumnBuildState(
            PhysicalType.Int96, maxDefLevel: 1, maxRepLevel: 0, capacity: rowCount);

        var defLevels = state.ReserveDefLevels(rowCount);
        for (int i = 0; i < rowCount; i++)
            defLevels[i] = (byte)(i < nonNull ? 1 : 0);

        var values = state.ReserveFixedBytes(nonNull, 12);
        for (int i = 0; i < nonNull; i++)
        {
            var slot = values.Slice(i * 12, 12);
            BinaryPrimitives.WriteInt64LittleEndian(slot, i * 60_000_000_000L);
            BinaryPrimitives.WriteInt32LittleEndian(slot.Slice(8), JulianDay);
        }

        var field = new Field(
            "ts", new TimestampType(TimeUnit.Microsecond, (TimeZoneInfo?)null), nullable: true);
        var timestamps = Assert.IsType<TimestampArray>(
            ArrowArrayBuilder.Build(state, field, rowCount));

        Assert.Equal(rowCount, timestamps.Length);
        for (int i = 0; i < nonNull; i++)
            Assert.Equal(1235865600000000L + i * 60_000_000L, timestamps.GetValue(i));
        Assert.Null(timestamps.GetValue(rowCount - 1));
    }

    /// <summary>
    /// The file-level schema a caller inspects before reading has to agree with the arrays the
    /// read produces — they are derived by different code paths.
    /// </summary>
    [Theory]
    [InlineData(Int96OutputKind.TimestampMicroseconds)]
    [InlineData(Int96OutputKind.TimestampNanoseconds)]
    [InlineData(Int96OutputKind.FixedSizeBinary)]
    public async Task Int96_SchemaMatchesTheArrayItProduces(Int96OutputKind kind)
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { Int96Output = kind });

        using var batch = await reader.ReadRowGroupAsync(0, ["timestamp_col"]);

        var declared = batch.Schema.GetFieldByName("timestamp_col").DataType;
        Assert.Equal(declared, batch.Column(0).Data.DataType);
    }
}
