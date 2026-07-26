// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Parquet's TIMESTAMP and TIME annotations carry only MILLIS, MICROS and NANOS. A second-precision
/// Arrow column has no unit to map onto, and the writer used to fall back to MICROS without touching
/// the values -- relabelling rather than rescaling them. Both outcomes were silent:
/// <c>Timestamp(Second)</c> read back a million times too small, and <c>Time32(Second)</c> produced an
/// INT32 column annotated TIME(MICROS), an illegal pairing whose file could not be read at all.
/// </summary>
public class TimestampUnitRejectionTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampUnitRejectionTests()
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
    public async Task Write_SecondPrecisionTimestamp_Rejected()
    {
        var type = new TimestampType(TimeUnit.Second, "UTC");

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => WriteAsync("ts_second.parquet", type, Int64Backed(type, 1_700_000_000L, timestamp: true)));

        Assert.Contains("Second", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Microsecond", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Write_SecondPrecisionTime32_Rejected()
    {
        var type = new Time32Type(TimeUnit.Second);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => WriteAsync("time32_second.parquet", type, Int32Backed(type, 3661)));
    }

    [Fact]
    public async Task Write_SecondPrecisionTimestamp_NestedInStruct_Rejected()
    {
        // MapArrowType is reached per leaf while building the Parquet schema tree, so nesting is
        // covered without a separate walk. Pin that.
        var second = new TimestampType(TimeUnit.Second, "UTC");
        var structType = new Apache.Arrow.Types.StructType([new Field("ts", second, true)]);

        var child = Int64Backed(second, 1_700_000_000L, timestamp: true);
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        var structArray = new StructArray(structType, 1, [child], validity.Build(), nullCount: 0);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => WriteAsync("struct_second.parquet", structType, structArray));
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
