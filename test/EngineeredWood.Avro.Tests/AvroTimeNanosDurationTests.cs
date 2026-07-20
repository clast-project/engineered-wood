// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Tests for time-nanos and duration logical types.
/// </summary>
public class AvroTimeNanosDurationTests
{
    // ─── Helpers ───

    private static byte[] WriteAndSerialize(Apache.Arrow.Schema arrowSchema, RecordBatch batch,
        AvroCodec codec = AvroCodec.Null, string? avroSchemaJson = null)
    {
        using var ms = new MemoryStream();
        var builder = new AvroWriterBuilder(arrowSchema)
            .WithCompression(codec);
        if (avroSchemaJson != null)
            builder = builder.WithAvroSchema(new AvroSchema(avroSchemaJson));
        using (var writer = builder.Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }
        return ms.ToArray();
    }

    private static RecordBatch ReadBack(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new AvroReaderBuilder().Build(ms);
        var result = reader.ReadNextBatch();
        Assert.NotNull(result);
        return result;
    }

    // ═══════════════════════════════════════════════════════════════
    // time-nanos
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void RoundTrip_TimeNanos()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("t", new Time64Type(TimeUnit.Nanosecond), false))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "t", "type": {"type": "long", "logicalType": "time-nanos"}}
            ]
        }
        """;

        var timeBuilder = new Time64Array.Builder(new Time64Type(TimeUnit.Nanosecond));
        timeBuilder.Append(0L);                      // midnight
        timeBuilder.Append(1_000_000_000L);           // 1 second
        timeBuilder.Append(45045_123456789L);         // 12h 30m 45.123456789s

        var batch = new RecordBatch(arrowSchema, [timeBuilder.Build()], 3);
        var data = WriteAndSerialize(arrowSchema, batch, avroSchemaJson: avroSchemaJson);
        var result = ReadBack(data);

        Assert.Equal(3, result.Length);
        var arr = (Time64Array)result.Column(0);
        Assert.Equal(TimeUnit.Nanosecond, ((Time64Type)arr.Data.DataType).Unit);
        Assert.Equal(0L, arr.GetValue(0));
        Assert.Equal(1_000_000_000L, arr.GetValue(1));
        Assert.Equal(45045_123456789L, arr.GetValue(2));
    }

    [Fact]
    public void RoundTrip_NullableTimeNanos()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("t", new Time64Type(TimeUnit.Nanosecond), true))
            .Build();

        var avroSchemaJson = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "t", "type": ["null", {"type": "long", "logicalType": "time-nanos"}]}
            ]
        }
        """;

        var timeBuilder = new Time64Array.Builder(new Time64Type(TimeUnit.Nanosecond));
        timeBuilder.Append(500_000_000L);
        timeBuilder.AppendNull();
        timeBuilder.Append(999_999_999L);

        var batch = new RecordBatch(arrowSchema, [timeBuilder.Build()], 3);
        var data = WriteAndSerialize(arrowSchema, batch, avroSchemaJson: avroSchemaJson);
        var result = ReadBack(data);

        Assert.Equal(3, result.Length);
        var arr = (Time64Array)result.Column(0);
        Assert.True(arr.IsValid(0));
        Assert.Equal(500_000_000L, arr.GetValue(0));
        Assert.False(arr.IsValid(1));
        Assert.True(arr.IsValid(2));
        Assert.Equal(999_999_999L, arr.GetValue(2));
    }

    [Fact]
    public void ArrowSchemaConverter_TimeNanos_RoundTrips()
    {
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("t", new Time64Type(TimeUnit.Nanosecond), false))
            .Build();

        var avroRecord = ArrowSchemaConverter.FromArrow(arrowSchema);
        Assert.Single(avroRecord.Fields);
        var prim = Assert.IsType<AvroPrimitiveSchema>(avroRecord.Fields[0].Schema);
        Assert.Equal(AvroType.Long, prim.Type);
        Assert.Equal("time-nanos", prim.LogicalType);

        var backToArrow = ArrowSchemaConverter.ToArrow(avroRecord);
        var time64 = Assert.IsType<Time64Type>(backToArrow.FieldsList[0].DataType);
        Assert.Equal(TimeUnit.Nanosecond, time64.Unit);
    }

    [Fact]
    public void Schema_TimeNanos_ParsedCorrectly()
    {
        var json = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "t", "type": {"type": "long", "logicalType": "time-nanos"}}
            ]
        }
        """;

        var record = (AvroRecordSchema)AvroSchemaParser.Parse(json);
        var fieldSchema = Assert.IsType<AvroPrimitiveSchema>(record.Fields[0].Schema);
        Assert.Equal(AvroType.Long, fieldSchema.Type);
        Assert.Equal("time-nanos", fieldSchema.LogicalType);
    }

    // ═══════════════════════════════════════════════════════════════
    // duration
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void RoundTrip_Duration()
    {
        var avroSchemaJson = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "dur", "type": {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}}
            ]
        }
        """;

        var intervalType = new IntervalType(IntervalUnit.MonthDayNanosecond);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("dur", intervalType, false))
            .Build();

        // Build 3 duration values using MonthDayNanosecondInterval
        // dur1: 2 months, 15 days, 3600000 millis (1 hour) → 3_600_000_000_000 ns
        // dur2: 0 months, 0 days, 0 millis
        // dur3: 12 months, 365 days, 86400000 millis (24 hours) → 86_400_000_000_000 ns
        var builder = new MonthDayNanosecondIntervalArray.Builder();
        builder.Append(new MonthDayNanosecondInterval(2, 15, 3_600_000_000_000L));
        builder.Append(new MonthDayNanosecondInterval(0, 0, 0L));
        builder.Append(new MonthDayNanosecondInterval(12, 365, 86_400_000_000_000L));

        var batch = new RecordBatch(arrowSchema, [builder.Build()], 3);
        var data = WriteAndSerialize(arrowSchema, batch, avroSchemaJson: avroSchemaJson);
        var result = ReadBack(data);

        Assert.Equal(3, result.Length);
        var arr = (MonthDayNanosecondIntervalArray)result.Column(0);

        var v0 = arr.GetValue(0)!.Value;
        Assert.Equal(2, v0.Months);
        Assert.Equal(15, v0.Days);
        Assert.Equal(3_600_000_000_000L, v0.Nanoseconds);

        var v1 = arr.GetValue(1)!.Value;
        Assert.Equal(0, v1.Months);
        Assert.Equal(0, v1.Days);
        Assert.Equal(0L, v1.Nanoseconds);

        var v2 = arr.GetValue(2)!.Value;
        Assert.Equal(12, v2.Months);
        Assert.Equal(365, v2.Days);
        Assert.Equal(86_400_000_000_000L, v2.Nanoseconds);
    }

    [Fact]
    public void RoundTrip_NullableDuration()
    {
        var avroSchemaJson = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "dur", "type": ["null", {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}]}
            ]
        }
        """;

        var intervalType = new IntervalType(IntervalUnit.MonthDayNanosecond);
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("dur", intervalType, true))
            .Build();

        var builder = new MonthDayNanosecondIntervalArray.Builder();
        builder.Append(new MonthDayNanosecondInterval(1, 2, 3_000_000_000L)); // 3000 ms
        builder.AppendNull();
        builder.Append(new MonthDayNanosecondInterval(6, 30, 500_000_000L));  // 500 ms

        var batch = new RecordBatch(arrowSchema, [builder.Build()], 3);
        var data = WriteAndSerialize(arrowSchema, batch, avroSchemaJson: avroSchemaJson);
        var result = ReadBack(data);

        Assert.Equal(3, result.Length);
        var arr = (MonthDayNanosecondIntervalArray)result.Column(0);
        Assert.True(arr.IsValid(0));
        var v0 = arr.GetValue(0)!.Value;
        Assert.Equal(1, v0.Months);
        Assert.Equal(2, v0.Days);
        Assert.Equal(3_000_000_000L, v0.Nanoseconds);
        Assert.False(arr.IsValid(1));
        Assert.True(arr.IsValid(2));
        var v2 = arr.GetValue(2)!.Value;
        Assert.Equal(6, v2.Months);
        Assert.Equal(30, v2.Days);
        Assert.Equal(500_000_000L, v2.Nanoseconds);
    }

    [Fact]
    public void Schema_Duration_ParsedCorrectly()
    {
        var json = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "dur", "type": {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}}
            ]
        }
        """;

        var record = (AvroRecordSchema)AvroSchemaParser.Parse(json);
        var fixedSchema = Assert.IsType<AvroFixedSchema>(record.Fields[0].Schema);
        Assert.Equal(12, fixedSchema.Size);
        Assert.Equal("duration", fixedSchema.LogicalType);
    }

    [Fact]
    public void ArrowSchemaConverter_Duration_MapsToMonthDayNanosecondInterval()
    {
        var json = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "dur", "type": {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}}
            ]
        }
        """;

        var schema = (AvroRecordSchema)AvroSchemaParser.Parse(json);
        var arrowSchema = ArrowSchemaConverter.ToArrow(schema);
        var arrowType = Assert.IsType<IntervalType>(arrowSchema.FieldsList[0].DataType);
        Assert.Equal(IntervalUnit.MonthDayNanosecond, arrowType.Unit);
    }

    [Fact]
    public void ArrowSchemaConverter_Duration_RoundTrips()
    {
        // Arrow MonthDayNanosecond → Avro fixed(12)/duration → Arrow MonthDayNanosecond
        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("dur", new IntervalType(IntervalUnit.MonthDayNanosecond), false))
            .Build();

        var avroRecord = ArrowSchemaConverter.FromArrow(arrowSchema);
        var fixedSchema = Assert.IsType<AvroFixedSchema>(avroRecord.Fields[0].Schema);
        Assert.Equal(12, fixedSchema.Size);
        Assert.Equal("duration", fixedSchema.LogicalType);

        var backToArrow = ArrowSchemaConverter.ToArrow(avroRecord);
        var intervalType = Assert.IsType<IntervalType>(backToArrow.FieldsList[0].DataType);
        Assert.Equal(IntervalUnit.MonthDayNanosecond, intervalType.Unit);
    }

    [Fact]
    public void Schema_Duration_RoundTripsViaJson()
    {
        var json = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "dur", "type": {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}}
            ]
        }
        """;

        var parsed = AvroSchemaParser.Parse(json);
        var serialized = AvroSchemaWriter.ToJson(parsed);
        var reparsed = (AvroRecordSchema)AvroSchemaParser.Parse(serialized);

        var fixedSchema = Assert.IsType<AvroFixedSchema>(reparsed.Fields[0].Schema);
        Assert.Equal(12, fixedSchema.Size);
        Assert.Equal("duration", fixedSchema.LogicalType);
    }

    // ═══════════════════════════════════════════════════════════════
    // Combined test
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public void RoundTrip_TimeNanosAndDuration_Together()
    {
        var avroSchemaJson = """
        {
            "type": "record", "name": "Test",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "time_ns", "type": {"type": "long", "logicalType": "time-nanos"}},
                {"name": "dur", "type": {"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}}
            ]
        }
        """;

        var arrowSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("time_ns", new Time64Type(TimeUnit.Nanosecond), false))
            .Field(new Field("dur", new IntervalType(IntervalUnit.MonthDayNanosecond), false))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var timeBuilder = new Time64Array.Builder(new Time64Type(TimeUnit.Nanosecond));
        var durBuilder = new MonthDayNanosecondIntervalArray.Builder();

        idBuilder.Append(1);
        idBuilder.Append(2);
        timeBuilder.Append(123_456_789L);
        timeBuilder.Append(86399_999_999_999L); // 23:59:59.999999999
        durBuilder.Append(new MonthDayNanosecondInterval(1, 7, 60_000_000_000L));   // 60000 ms
        durBuilder.Append(new MonthDayNanosecondInterval(0, 1, 1_000_000_000L));    // 1000 ms

        var batch = new RecordBatch(arrowSchema,
            [idBuilder.Build(), timeBuilder.Build(), durBuilder.Build()], 2);
        var data = WriteAndSerialize(arrowSchema, batch, avroSchemaJson: avroSchemaJson);
        var result = ReadBack(data);

        Assert.Equal(2, result.Length);
        Assert.Equal(1, ((Int32Array)result.Column(0)).GetValue(0));
        Assert.Equal(2, ((Int32Array)result.Column(0)).GetValue(1));

        var timeArr = (Time64Array)result.Column(1);
        Assert.Equal(123_456_789L, timeArr.GetValue(0));
        Assert.Equal(86399_999_999_999L, timeArr.GetValue(1));

        var durArr = (MonthDayNanosecondIntervalArray)result.Column(2);
        var d0 = durArr.GetValue(0)!.Value;
        Assert.Equal(1, d0.Months);
        Assert.Equal(7, d0.Days);
        Assert.Equal(60_000_000_000L, d0.Nanoseconds);
        var d1 = durArr.GetValue(1)!.Value;
        Assert.Equal(0, d1.Months);
        Assert.Equal(1, d1.Days);
        Assert.Equal(1_000_000_000L, d1.Nanoseconds);
    }
}
