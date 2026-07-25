// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Delta's timestamp and timestamp_ntz are microsecond precision. A nanosecond Arrow column has
/// digits neither the Delta schema nor its ISO-8601 file statistics can carry, and nothing in the
/// write path narrows the unit -- the value would reach Parquet as a nanosecond column under a
/// schema advertising microseconds, with bounds that had to drop the remainder. Rejecting at write
/// keeps that decision with the caller instead of silently altering their data.
/// </summary>
public class NanosecondTimestampRejectionTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(
        Path.GetTempPath(), "ew-ns-reject-" + Guid.NewGuid().ToString("N"));

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
    [InlineData(TimeUnit.Microsecond)]
    [InlineData(TimeUnit.Millisecond)]
    [InlineData(TimeUnit.Second)]
    public async Task Write_NonNanosecondUnits_StillAccepted(TimeUnit unit)
    {
        // Only nanoseconds lose data. The other units scale up exactly and must keep working.
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = SchemaWith(unit);
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.WriteAsync([BatchWith(schema, unit, "UTC", 1L, 2L, 3L)]);

        long rows = 0;
        await foreach (var b in table.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(3, rows);
    }
}
