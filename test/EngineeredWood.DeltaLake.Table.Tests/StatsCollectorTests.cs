// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.Stats;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class StatsCollectorTests
{
    [Fact]
    public void Collect_IntegerColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder()
            .Append(10).Append(5).Append(20).Build();
        var batch = new RecordBatch(schema, [ids], 3);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(3, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(5, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt64());
        Assert.Equal(20, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt64());
        Assert.Equal(0, doc.RootElement.GetProperty("nullCount").GetProperty("id").GetInt64());
    }

    [Fact]
    public void Collect_StringColumn()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var names = new StringArray.Builder()
            .Append("charlie").Append("alice").AppendNull().Append("bob").Build();
        var batch = new RecordBatch(schema, [names], 4);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(4, doc.RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal("alice", doc.RootElement.GetProperty("minValues").GetProperty("name").GetString());
        Assert.Equal("charlie", doc.RootElement.GetProperty("maxValues").GetProperty("name").GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("name").GetInt64());
    }

    [Fact]
    public void Collect_MultipleColumns()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Build();

        var ids = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new DoubleArray.Builder().Append(1.5).AppendNull().Append(3.7).Build();
        var batch = new RecordBatch(schema, [ids, values], 3);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal(1, doc.RootElement.GetProperty("minValues").GetProperty("id").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("maxValues").GetProperty("id").GetInt32());
        Assert.Equal(1.5, doc.RootElement.GetProperty("minValues").GetProperty("value").GetDouble(), 5);
        Assert.Equal(3.7, doc.RootElement.GetProperty("maxValues").GetProperty("value").GetDouble(), 5);
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("value").GetInt64());
    }

    [Fact]
    public void Collect_EmptyBatch_ReturnsNull()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var ids = new Int64Array.Builder().Build();
        var batch = new RecordBatch(schema, [ids], 0);

        Assert.Null(StatsCollector.Collect(batch));
    }

    [Fact]
    public void Collect_DateColumn_EmitsIsoStrings()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", Date32Type.Default, true))
            .Build();

        var dates = new Date32Array.Builder()
            .Append(new DateTime(2021, 6, 1, 0, 0, 0, DateTimeKind.Utc))
            .Append(new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc))
            .AppendNull()
            .Append(new DateTime(2021, 12, 31, 0, 0, 0, DateTimeKind.Utc))
            .Build();
        var batch = new RecordBatch(schema, [dates], 4);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        var min = doc.RootElement.GetProperty("minValues").GetProperty("d");
        var max = doc.RootElement.GetProperty("maxValues").GetProperty("d");
        // Delta stores date bounds as "yyyy-MM-dd" STRINGS (a raw day number is not decodable and never
        // prunes) — this is the format Spark writes and EW's DeltaLiteralDecoder reads.
        Assert.Equal(JsonValueKind.String, min.ValueKind);
        Assert.Equal("2021-01-01", min.GetString());
        Assert.Equal("2021-12-31", max.GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("d").GetInt64());
    }

    private static RecordBatch Decimal128Batch(
        string name, int precision, int scale, params BigInteger?[] unscaled)
    {
        var type = new Decimal128Type(precision, scale);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field(name, type, true)).Build();
        const int w = 16;
        var bytes = new byte[unscaled.Length * w];
        var nulls = new ArrowBuffer.BitmapBuilder();
        int nullCount = 0;
        for (int i = 0; i < unscaled.Length; i++)
        {
            if (unscaled[i] is null) { nulls.Append(false); nullCount++; continue; }
            nulls.Append(true);
            var bi = unscaled[i]!.Value;
            var dest = bytes.AsSpan(i * w, w);
            dest.Fill(bi.Sign < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
            bi.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
            var bb = bi.ToByteArray();
            bb.AsSpan(0, Math.Min(bb.Length, w)).CopyTo(dest);
#endif
        }
        var data = new ArrayData(type, unscaled.Length, nullCount, 0,
            [nulls.Build(), new ArrowBuffer(bytes)]);
        return new RecordBatch(schema, [new Decimal128Array(data)], unscaled.Length);
    }

    [Fact]
    public void Collect_DecimalColumn_EmitsJsonNumbers()
    {
        // decimal(12,2): unscaled 1234 / 5678 -> 12.34 / 56.78, written as JSON NUMBERS (the form Delta
        // uses and can decode/prune on).
        var batch = Decimal128Batch("amt", 12, 2, 1234, 5678, null);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        var min = doc.RootElement.GetProperty("minValues").GetProperty("amt");
        var max = doc.RootElement.GetProperty("maxValues").GetProperty("amt");
        Assert.Equal(JsonValueKind.Number, min.ValueKind);
        Assert.Equal(12.34m, min.GetDecimal());
        Assert.Equal(56.78m, max.GetDecimal());
        Assert.Equal(1, doc.RootElement.GetProperty("nullCount").GetProperty("amt").GetInt64());
    }

    [Fact]
    public void Collect_DecimalColumn_HighPrecision_PreservedAsRawNumber()
    {
        // decimal(38,0): 10^31 exceeds System.Decimal and must survive as a raw 32-digit JSON number,
        // exactly as Spark writes it.
        var huge = BigInteger.Pow(10, 31);
        var batch = Decimal128Batch("big", 38, 0, huge, 5);

        string? stats = StatsCollector.Collect(batch);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        var min = doc.RootElement.GetProperty("minValues").GetProperty("big");
        var max = doc.RootElement.GetProperty("maxValues").GetProperty("big");
        Assert.Equal(JsonValueKind.Number, max.ValueKind);
        Assert.Equal("5", min.GetRawText());
        Assert.Equal("1" + new string('0', 31), max.GetRawText()); // 10^31, full precision preserved
    }

    [Fact]
    public void Collect_DateColumn_MergesAcrossBatches()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", Date32Type.Default, false))
            .Build();

        RecordBatch Batch(params DateTime[] days)
        {
            var b = new Date32Array.Builder();
            foreach (var d in days) b.Append(DateTime.SpecifyKind(d, DateTimeKind.Utc));
            return new RecordBatch(schema, [b.Build()], days.Length);
        }

        string? stats = StatsCollector.Collect(
        [
            Batch(new DateTime(2020, 5, 5), new DateTime(2020, 8, 8)),
            Batch(new DateTime(2019, 1, 1), new DateTime(2021, 3, 3)),
        ]);
        Assert.NotNull(stats);

        var doc = JsonDocument.Parse(stats);
        Assert.Equal("2019-01-01", doc.RootElement.GetProperty("minValues").GetProperty("d").GetString());
        Assert.Equal("2021-03-03", doc.RootElement.GetProperty("maxValues").GetProperty("d").GetString());
    }
}
