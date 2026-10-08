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

    private Task WriteAsync(string file, IArrowType type, IArrowArray array) =>
        WriteAsync(file, type, array, buffered: false);

    // BufferedParquetWriter once skipped both the rescale and ARROW:schema (#394), so the tests
    // that pin either one run against both writers.
    private async Task WriteAsync(
        string file, IArrowType type, IArrowArray array, bool buffered, ParquetWriteOptions? options = null)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", type, true)).Build();
        var batch = new RecordBatch(schema, [array], array.Length);
        await using var f = new LocalSequentialFile(TempPath(file));
        if (buffered)
        {
            await using var writer = new BufferedParquetWriter(f, ownsFile: false, options);
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }
        else
        {
            await using var writer = new ParquetFileWriter(f, ownsFile: false, options);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_SecondPrecisionTimestamp_RescalesToMillis(bool buffered)
    {
        var type = new TimestampType(TimeUnit.Second, "UTC");
        await WriteAsync("ts_second.parquet", type, Int64Backed(type, 1_700_000_000L, timestamp: true), buffered);

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_SecondPrecisionTime32_RescalesToMillis(bool buffered)
    {
        var type = new Time32Type(TimeUnit.Second);
        await WriteAsync("time32_second.parquet", type, Int32Backed(type, 3661), buffered);

        await using var rf = new LocalRandomAccessFile(TempPath("time32_second.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(TimeUnit.Millisecond, ((Time32Type)batch.Schema.FieldsList[0].DataType).Unit);
        Assert.Equal(3_661_000, ((Time32Array)batch.Column(0)).GetValue(0));
    }

    [Fact]
    public async Task BufferedWrite_EveryAppendedBatchIsRescaled()
    {
        // The buffered writer captures its schema from the first batch but keeps appending, so a
        // rescale applied only to that first batch would write later ones a thousand times too small.
        var type = new TimestampType(TimeUnit.Second, "UTC");
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("v", type, true)).Build();
        string path = TempPath("ts_second_buffered.parquet");

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false))
        {
            await writer.AppendAsync(new RecordBatch(
                schema, [Int64Backed(type, 1_700_000_000L, timestamp: true)], 1));
            await writer.AppendAsync(new RecordBatch(
                schema, [Int64Backed(type, 1_700_000_001L, timestamp: true)], 1));
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var read = new List<DateTimeOffset>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var arr = (TimestampArray)batch.Column(0);
            for (int i = 0; i < arr.Length; i++)
                read.Add(arr.GetTimestamp(i)!.Value);
        }

        Assert.Equal(
            [
                new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero),
                new DateTimeOffset(2023, 11, 14, 22, 13, 21, TimeSpan.Zero),
            ],
            read);
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_TimestampZoneName_SurvivesThroughArrowSchema(bool buffered)
    {
        // Parquet stores isAdjustedToUTC and no zone name, so the name only survives in
        // ARROW:schema. Without it this reads back as UTC — which is what DuckDB returns.
        var type = new TimestampType(TimeUnit.Microsecond, "America/New_York");
        await WriteAsync(
            "ts_zone.parquet", type, Int64Backed(type, 1_700_000_000_000_000L, timestamp: true), buffered);

        await using var rf = new LocalRandomAccessFile(TempPath("ts_zone.parquet"));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(
            "America/New_York",
            ((TimestampType)batch.Schema.FieldsList[0].DataType).Timezone);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_WithoutArrowSchema_LosesTheZoneName(bool buffered)
    {
        // The opt-out has to actually opt out: no ARROW:schema entry, and the zone falls back to UTC.
        var type = new TimestampType(TimeUnit.Microsecond, "America/New_York");
        string path = TempPath("ts_no_arrow_schema.parquet");
        await WriteAsync(
            "ts_no_arrow_schema.parquet", type, Int64Backed(type, 1_700_000_000_000_000L, timestamp: true),
            buffered, new ParquetWriteOptions { WriteArrowSchema = false });

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

    [Fact]
    public async Task CoercionKeepsFieldMetadataSuchAsFieldIds()
    {
        // Rebuilding a Field to change its type drops everything else on it unless the metadata is
        // carried across -- and PARQUET:field_id lives in exactly that metadata. Silently losing it
        // because a table happened to contain a second-precision column would break Iceberg and
        // Delta column mapping for that file.
        var metadata = new Dictionary<string, string> { ["PARQUET:field_id"] = "7" };
        var coerced = new Field(
            "at", new TimestampType(TimeUnit.Second, "UTC"), nullable: true, metadata);
        var untouched = new Field(
            "id", Int32Type.Default, nullable: false,
            new Dictionary<string, string> { ["PARQUET:field_id"] = "9" });
        var schema = new Apache.Arrow.Schema.Builder().Field(coerced).Field(untouched).Build();

        string path = TempPath("ts_field_ids.parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(
                schema,
                [
                    Int64Backed(new TimestampType(TimeUnit.Second, "UTC"), 1_700_000_000L, timestamp: true),
                    new Int32Array.Builder().Append(1).Build(),
                ],
                1));
            await writer.CloseAsync();
        }

        await using var rf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var elements = (await reader.ReadMetadataAsync()).Schema;

        // The coerced column keeps its id, and so does the one that was never touched.
        Assert.Equal(7, elements.Single(element => element.Name == "at").FieldId);
        Assert.Equal(9, elements.Single(element => element.Name == "id").FieldId);
    }

    // A struct sliced below a list once read back shifted by its own offset: the rescale rebuilt it
    // over StructArray.Fields, which Arrow already slices, and then applied the struct's offset a
    // second time (#486). Only a rescaled column takes that path, so the millisecond case is the
    // control.
    [Theory]
    [InlineData(TimeUnit.Second, false, false)]
    [InlineData(TimeUnit.Second, false, true)]
    [InlineData(TimeUnit.Second, true, false)]
    [InlineData(TimeUnit.Second, true, true)]
    [InlineData(TimeUnit.Millisecond, false, false)]
    [InlineData(TimeUnit.Millisecond, true, false)]
    public async Task Write_SlicedStructUnderList_KeepsItsRows(TimeUnit unit, bool time32, bool buffered)
    {
        IArrowType temporal = time32 ? new Time32Type(unit) : new TimestampType(unit, "UTC");
        var structType = new StructType(
            [new Field("ts", temporal, true), new Field("n", Int64Type.Default, true)]);

        IArrowArray ts = time32
            ? new Time32Array.Builder((Time32Type)temporal).AppendRange([100, 200, 300, 400]).Build()
            : Int64Column(temporal, [100, 200, 300, 400]);
        var n = new Int64Array.Builder().AppendRange([1, 2, 3, 4]).Build();
        var full = new StructArray(structType, 4, [ts, n], ArrowBuffer.Empty, nullCount: 0);
        var sliced = (StructArray)full.Slice(1, 3);

        var listType = new ListType(new Field("item", structType, true));
        var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3]).Build();
        var list = new ListArray(listType, 2, offsets, sliced, ArrowBuffer.Empty, nullCount: 0);

        string file = $"sliced_struct_{unit}_{time32}_{buffered}.parquet";
        await WriteAsync(file, listType, list, buffered);

        await using var rf = new LocalRandomAccessFile(TempPath(file));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var readList = (ListArray)batch.Column(0);
        var values = (StructArray)readList.Values;
        int first = readList.ValueOffsets[0];
        long scale = unit == TimeUnit.Second ? 1000 : 1;
        var read = new List<(long Ts, long N)>();
        for (int i = first; i < readList.ValueOffsets[readList.Length]; i++)
        {
            long t = values.Fields[0] is Time32Array t32
                ? t32.GetValue(i)!.Value
                : ((TimestampArray)values.Fields[0]).GetValue(i)!.Value;
            read.Add((t, ((Int64Array)values.Fields[1]).GetValue(i)!.Value));
        }

        Assert.Equal([(200 * scale, 2L), (300 * scale, 3L), (400 * scale, 4L)], read);
    }

    // The map arm rebuilds its entries struct the same way, so an entries struct with an offset of
    // its own has to survive too.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_MapWithSlicedEntries_KeepsItsRows(bool buffered)
    {
        var second = new TimestampType(TimeUnit.Second, "UTC");
        var keyField = new Field("key", second, false);
        var valueField = new Field("value", Int64Type.Default, true);
        var mapType = new MapType(keyField, valueField);

        var keys = Int64Column(second, [100, 200, 300, 400]);
        var values = new Int64Array.Builder().AppendRange([1, 2, 3, 4]).Build();
        var entries = new StructArray(
            new StructType([keyField, valueField]), 4, [keys, values], ArrowBuffer.Empty, nullCount: 0);
        var sliced = (StructArray)entries.Slice(1, 3);

        var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3]).Build();
        var map = new MapArray(mapType, 2, offsets, sliced, ArrowBuffer.Empty, nullCount: 0);

        string file = $"sliced_map_{buffered}.parquet";
        await WriteAsync(file, mapType, map, buffered);

        await using var rf = new LocalRandomAccessFile(TempPath(file));
        await using var reader = new ParquetFileReader(rf, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var readMap = (MapArray)batch.Column(0);
        var readKeys = (TimestampArray)readMap.Keys;
        var readValues = (Int64Array)readMap.Values;
        var read = new List<(long Key, long Value)>();
        for (int i = readMap.ValueOffsets[0]; i < readMap.ValueOffsets[readMap.Length]; i++)
            read.Add((readKeys.GetValue(i)!.Value, readValues.GetValue(i)!.Value));

        Assert.Equal([(200_000L, 2L), (300_000L, 3L), (400_000L, 4L)], read);
    }

    // The read side shares the same rebuild to restore zones. The reader's own arrays are unsliced,
    // but a sliced one must not shift either.
    [Fact]
    public void ToDeclaredUnits_SlicedStructUnderList_KeepsItsRows()
    {
        var derived = new TimestampType(TimeUnit.Millisecond, (string?)null);
        var structType = new StructType(
            [new Field("ts", derived, true), new Field("n", Int64Type.Default, true)]);
        var full = new StructArray(
            structType, 4,
            [Int64Column(derived, [100, 200, 300, 400]),
             new Int64Array.Builder().AppendRange([1, 2, 3, 4]).Build()],
            ArrowBuffer.Empty, nullCount: 0);
        var sliced = (StructArray)full.Slice(1, 3);
        var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3]).Build();
        var list = new ListArray(
            new ListType(new Field("item", structType, true)), 2, offsets, sliced,
            ArrowBuffer.Empty, nullCount: 0);

        var declared = new ListType(new Field("item", new StructType(
            [new Field("ts", new TimestampType(TimeUnit.Millisecond, "UTC"), true),
             new Field("n", Int64Type.Default, true)]), true));
        var restored = (ListArray)EngineeredWood.Parquet.Data.TimeUnitRescaler.ToDeclaredUnits(list, declared);

        var values = (StructArray)restored.Values;
        Assert.Equal("UTC", ((TimestampType)values.Fields[0].Data.DataType).Timezone);
        var ts = (TimestampArray)values.Fields[0];
        var n = (Int64Array)values.Fields[1];
        Assert.Equal([200L, 300L, 400L], Enumerable.Range(0, 3).Select(i => ts.GetValue(i)!.Value));
        Assert.Equal([2L, 3L, 4L], Enumerable.Range(0, 3).Select(i => n.GetValue(i)!.Value));
    }

    private static TimestampArray Int64Column(IArrowType type, long[] raw)
    {
        var values = new ArrowBuffer.Builder<long>().AppendRange(raw).Build();
        return new TimestampArray(new ArrayData(type, raw.Length, 0, 0, [ArrowBuffer.Empty, values]));
    }
}
