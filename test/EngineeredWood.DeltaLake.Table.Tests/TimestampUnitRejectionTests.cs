// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Two Arrow timestamp units have no faithful encoding here, and nothing in the write path narrows
/// the unit. NANOSECOND exceeds the microsecond precision of Delta timestamps and of the ISO-8601
/// file statistics, so its low digits would be dropped. SECOND has no Parquet unit at all, so the
/// writer annotates the column MICROS and leaves the values untouched — they read back a million
/// times too small. Rejecting at write keeps the choice with the caller rather than silently
/// altering their data.
/// </summary>
public class TimestampUnitRejectionTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(
        Path.GetTempPath(), "ew-ts-unit-reject-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema SchemaWith(TimeUnit unit, string? tz = "UTC") =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("ts", new TimestampType(unit, tz), true))
            .Build();

    private static RecordBatch BatchWith(Apache.Arrow.Schema schema, TimeUnit unit, string? tz, params long[] values)
    {
        var type = new TimestampType(unit, tz);
        var ids = new Int64Array.Builder();
        for (int i = 0; i < values.Length; i++) ids.Append(i);

        var buffer = new ArrowBuffer.Builder<long>();
        foreach (long v in values) buffer.Append(v);
        var validity = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < values.Length; i++) validity.Append(true);
        var data = new ArrayData(type, values.Length, 0, 0, [validity.Build(), buffer.Build()]);

        return new RecordBatch(schema, [ids.Build(), new TimestampArray(data)], values.Length);
    }

    [Fact]
    public async Task CreateAsync_NanosecondTimestamp_Rejected()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateAsync(fs, SchemaWith(TimeUnit.Nanosecond)));

        Assert.Contains("microsecond", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nanosecond", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateAsync_NanosecondTimestampNtz_Rejected()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateAsync(fs, SchemaWith(TimeUnit.Nanosecond, tz: null)));
    }

    [Fact]
    public async Task WriteAsync_NanosecondBatchIntoMicrosecondTable_Rejected()
    {
        // The table is created microsecond-clean, so this exercises the WRITE path rather than
        // schema creation: nothing here goes through table creation to catch the unit.
        var fs = new LocalTableFileSystem(_tempDir);
        var microSchema = SchemaWith(TimeUnit.Microsecond);
        await using var table = await DeltaTable.CreateAsync(fs, microSchema);

        var nanoSchema = SchemaWith(TimeUnit.Nanosecond);
        var batch = BatchWith(nanoSchema, TimeUnit.Nanosecond, "UTC", 1_000_000_123L, 500_000_001L);

        await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([batch]));
    }

    private static Apache.Arrow.Schema NestedSchema(IArrowType inner) =>
        new Apache.Arrow.Schema.Builder().Field(new Field("outer", inner, true)).Build();

    public static TheoryData<string, IArrowType> NestedNanosecondTypes()
    {
        var nano = new TimestampType(TimeUnit.Nanosecond, "UTC");
        return new TheoryData<string, IArrowType>
        {
            { "struct", new Apache.Arrow.Types.StructType([new Field("ts", nano, true)]) },
            { "list", new ListType(new Field("item", nano, true)) },
            { "map-value", new Apache.Arrow.Types.MapType(
                new Field("key", StringType.Default, false), new Field("value", nano, true)) },
            { "list-of-struct", new ListType(new Field("item",
                new Apache.Arrow.Types.StructType([new Field("ts", nano, true)]), true)) },
        };
    }

    [Theory]
    [MemberData(nameof(NestedNanosecondTypes))]
    public async Task Write_NanosecondNestedInsideComplexType_Rejected(string label, IArrowType inner)
    {
        Assert.NotNull(label);
        var fs = new LocalTableFileSystem(_tempDir);

        // Creation converts the schema, so this covers the SchemaConverter arm for nested types.
        await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateAsync(fs, NestedSchema(inner)));
    }

    [Fact]
    public void MapType_IsNotAListType_SoTheWalkArmOrderIsIrrelevant()
    {
        // The nested walk and FromArrowType both match ListType before MapType, which is only safe
        // because Apache.Arrow models the two as unrelated types. Were MapType ever to derive from
        // ListType, the list arm would swallow maps and skip their key/value types entirely — so pin
        // it, alongside the behaviour that actually matters.
        var nano = new TimestampType(TimeUnit.Nanosecond, "UTC");
        var mapType = new Apache.Arrow.Types.MapType(
            new Field("key", StringType.Default, false), new Field("value", nano, true));

        Assert.False(mapType is ListType, "MapType deriving from ListType would break arm ordering");
        Assert.Throws<DeltaFormatException>(
            () => EngineeredWood.DeltaLake.Schema.SchemaConverter.FromArrowSchema(NestedSchema(mapType)));
    }

    [Theory]
    [InlineData(TimeUnit.Microsecond, 1_700_000_000_000_000L)]
    [InlineData(TimeUnit.Millisecond, 1_700_000_000_000L)]
    public async Task Write_SupportedUnits_RoundTripValuesExactly(TimeUnit unit, long raw)
    {
        // Microsecond and millisecond both have a Parquet unit that carries them exactly, so their
        // VALUES must survive — asserting only the row count would have missed the second-unit bug,
        // where every value came back a million times too small.
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = SchemaWith(unit);
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.WriteAsync([BatchWith(schema, unit, "UTC", raw)]);

        var expected = new DateTimeOffset(2023, 11, 14, 22, 13, 20, TimeSpan.Zero);
        var read = new List<DateTimeOffset>();
        await foreach (var b in table.ReadAllAsync())
        {
            var arr = (TimestampArray)b.Column(1);
            for (int i = 0; i < arr.Length; i++)
                read.Add(arr.GetTimestamp(i)!.Value);
        }

        Assert.Equal(expected, Assert.Single(read));
    }

    [Fact]
    public async Task CreateAsync_SecondTimestamp_Rejected()
    {
        // Parquet has no second-precision timestamp unit: the writer annotates the column MICROS and
        // leaves the values alone, so 1700000000s reads back as 1970-01-01T00:28:20Z.
        var fs = new LocalTableFileSystem(_tempDir);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateAsync(fs, SchemaWith(TimeUnit.Second)));

        Assert.Contains("second", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteAsync_SecondBatchIntoMicrosecondTable_Rejected()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, SchemaWith(TimeUnit.Microsecond));

        var secondSchema = SchemaWith(TimeUnit.Second);
        var batch = BatchWith(secondSchema, TimeUnit.Second, "UTC", 1_700_000_000L);

        await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([batch]));
    }
}
