// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Parquet's TIMESTAMP and TIME annotations carry only MILLIS, MICROS and NANOS, so a
/// second-precision Arrow column has no unit to map onto. The writer once fell back to MICROS
/// without touching the values — relabelling rather than rescaling, so <c>Timestamp(Second)</c> read
/// back a million times too small and <c>Time32(Second)</c> produced an illegal INT32/TIME(MICROS)
/// pairing. It then refused such columns outright, which was honest but left a whole Arrow type
/// unwritable. It now rescales them to milliseconds, which is what PyArrow does.
/// </summary>
public class TimestampUnitCoercionTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampUnitCoercionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-tsunit-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private string TempPath(string name) => Path.Combine(_tempDir, name);

    private static IArrowArray Int64Backed(IArrowType type, long value, bool timestamp)
    {
        var values = new ArrowBuffer.Builder<long>();
        values.Append(value);
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        var data = new ArrayData(type, 1, 0, 0, [validity.Build(), values.Build()]);
        return timestamp ? new TimestampArray(data) : new Time64Array(data);
    }

    private static IArrowArray Int32Backed(IArrowType type, int value)
    {
        var values = new ArrowBuffer.Builder<int>();
        values.Append(value);
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        return new Time32Array(new ArrayData(type, 1, 0, 0, [validity.Build(), values.Build()]));
    }

    private async Task WriteAsync(string file, IArrowType type, IArrowArray array)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", type, true)).Build();
        await using var f = new LocalSequentialFile(TempPath(file));
        await using var writer = new ParquetFileWriter(f, ownsFile: false);
        await writer.WriteRowGroupAsync(new RecordBatch(schema, [array], 1));
        await writer.CloseAsync();
    }

    [Fact]
    public async Task Write_SecondPrecisionTimestamp_RescalesToMillis()
    {
        var type = new TimestampType(TimeUnit.Second, "UTC");
        await WriteAsync("ts_second.parquet", type, Int64Backed(type, 1_700_000_000L, timestamp: true));

        await using var rf = new LocalRandomAccessFile(TempPath("ts_second.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        // Milliseconds on the way out, as PyArrow also reports for its own second-precision column,
        // but the instant is the one that went in — the values were rescaled, not relabelled.
        var read = (TimestampType)batch.Schema.FieldsList[0].DataType;
        Assert.Equal(TimeUnit.Millisecond, read.Unit);
        Assert.Equal("UTC", read.Timezone);
        Assert.Equal(
            new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero),
            ((TimestampArray)batch.Column(0)).GetTimestamp(0)!.Value);
    }

    [Fact]
    public async Task Write_SecondPrecisionTime32_RescalesToMillis()
    {
        var type = new Time32Type(TimeUnit.Second);
        await WriteAsync("time32_second.parquet", type, Int32Backed(type, 3661));

        await using var rf = new LocalRandomAccessFile(TempPath("time32_second.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(TimeUnit.Millisecond, ((Time32Type)batch.Schema.FieldsList[0].DataType).Unit);
        Assert.Equal(3_661_000, ((Time32Array)batch.Column(0)).GetValue(0));
    }

    [Fact]
    public async Task Write_SecondPrecisionTimestamp_NestedInStruct_RescalesToMillis()
    {
        // The rescale is a pass over the batch, not a schema decision, so it has to reach a column
        // that is not at the top level. Pin that.
        var second = new TimestampType(TimeUnit.Second, "UTC");
        var structType = new Apache.Arrow.Types.StructType([new Field("ts", second, true)]);

        var child = Int64Backed(second, 1_700_000_000L, timestamp: true);
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        var structArray = new StructArray(structType, 1, [child], validity.Build(), nullCount: 0);

        await WriteAsync("struct_second.parquet", structType, structArray);

        await using var rf = new LocalRandomAccessFile(TempPath("struct_second.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var read = (StructArray)batch.Column(0);
        Assert.Equal(
            new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero),
            ((TimestampArray)read.Fields[0]).GetTimestamp(0)!.Value);
    }

    [Fact]
    public async Task Write_SecondPrecisionTimestamp_OverflowingMillis_IsRefused()
    {
        // Rescaling is only correct while it fits. An instant that does not is the one case that
        // still refuses, rather than silently wrapping.
        var type = new TimestampType(TimeUnit.Second, "UTC");

        var error = await Assert.ThrowsAsync<NotSupportedException>(
            () => WriteAsync("ts_overflow.parquet", type, Int64Backed(type, long.MaxValue, timestamp: true)));

        Assert.Contains("millisecond", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Write_TimestampZoneName_SurvivesThroughArrowSchema()
    {
        // Parquet stores isAdjustedToUTC and no zone name, so the name only survives in
        // ARROW:schema. Without it this reads back as UTC — which is what DuckDB returns.
        var type = new TimestampType(TimeUnit.Microsecond, "America/New_York");
        await WriteAsync("ts_zone.parquet", type, Int64Backed(type, 1_700_000_000_000_000L, timestamp: true));

        await using var rf = new LocalRandomAccessFile(TempPath("ts_zone.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(
            "America/New_York",
            ((TimestampType)batch.Schema.FieldsList[0].DataType).Timezone);
    }

    [Fact]
    public async Task Write_WithoutArrowSchema_LosesTheZoneName()
    {
        // The opt-out has to actually opt out: no ARROW:schema entry, and the zone falls back to UTC.
        var type = new TimestampType(TimeUnit.Microsecond, "America/New_York");
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", type, true)).Build();
        string path = TempPath("ts_no_arrow_schema.parquet");

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(
            file, ownsFile: false, new ParquetWriteOptions { WriteArrowSchema = false }))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(
                schema, [Int64Backed(type, 1_700_000_000_000_000L, timestamp: true)], 1));
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        Assert.DoesNotContain(
            metadata.KeyValueMetadata ?? [],
            entry => entry.Key == "ARROW:schema");

        // The zone the caller wrote is gone, which is what the opt-out costs. Parquet keeps only
        // isAdjustedToUTC, so what remains can only be UTC -- how that zone is *spelled* is a
        // separate concern with its own tests, and asserting it here would couple the two.
        var batch = await reader.ReadRowGroupAsync(0);
        var zone = ((TimestampType)batch.Schema.FieldsList[0].DataType).Timezone;
        Assert.NotEqual("America/New_York", zone);
        Assert.NotNull(zone);
    }

    [Theory]
    [InlineData(TimeUnit.Millisecond, 1_700_000_000_000L)]
    [InlineData(TimeUnit.Microsecond, 1_700_000_000_000_000L)]
    [InlineData(TimeUnit.Nanosecond, 1_700_000_000_000_000_000L)]
    public async Task Write_SupportedTimestampUnits_RoundTripExactly(TimeUnit unit, long raw)
    {
        // Every unit Parquet can actually annotate must keep working, values intact — nanoseconds
        // included, which Parquet supports even though Delta does not.
        var type = new TimestampType(unit, "UTC");
        string file = $"ts_{unit}.parquet";
        await WriteAsync(file, type, Int64Backed(type, raw, timestamp: true));

        await using var rf = new LocalRandomAccessFile(TempPath(file));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        var read = new List<DateTimeOffset>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var arr = (TimestampArray)batch.Column(0);
            for (int i = 0; i < arr.Length; i++)
                read.Add(arr.GetTimestamp(i)!.Value);
        }

        Assert.Equal(new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero), Assert.Single(read));
    }

    [Fact]
    public async Task Write_MillisecondTime32_StillAccepted()
    {
        var type = new Time32Type(TimeUnit.Millisecond);
        await WriteAsync("time32_millis.parquet", type, Int32Backed(type, 3_661_000));

        await using var rf = new LocalRandomAccessFile(TempPath("time32_millis.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);

        await foreach (var batch in reader.ReadAllAsync())
        {
            var arr = (Time32Array)batch.Column(0);
            Assert.Equal(3_661_000, arr.GetValue(0));
        }
    }
}
