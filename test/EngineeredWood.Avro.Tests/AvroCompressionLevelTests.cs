// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;

namespace EngineeredWood.Avro.Tests;

public class AvroCompressionLevelTests
{
    public static IEnumerable<object[]> CodecsAndLevels()
    {
        var codecs = new[] { AvroCodec.Deflate, AvroCodec.Zstandard, AvroCodec.Lz4 };
        var levels = new[]
        {
            BlockCompressionLevel.Fastest,
            BlockCompressionLevel.Optimal,
            BlockCompressionLevel.SmallestSize,
        };
        foreach (var c in codecs)
            foreach (var l in levels)
                yield return new object[] { c, l };
    }

    [Theory]
    [MemberData(nameof(CodecsAndLevels))]
    public void RoundTrip_AtLevel_PreservesData(AvroCodec codec, BlockCompressionLevel level)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var values = new long[10_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = i % 17;

        var b = new Int64Array.Builder();
        foreach (var v in values) b.Append(v);
        var batch = new RecordBatch(schema, [b.Build()], values.Length);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(codec)
            .WithCompressionLevel(level)
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        int row = 0;
        while (reader.ReadNextBatch() is RecordBatch result)
        {
            var col = (Int64Array)result.Column(0);
            for (int i = 0; i < col.Length; i++)
                Assert.Equal(values[row++], col.GetValue(i));
        }
        Assert.Equal(values.Length, row);
    }

    [Fact]
    public void Zstandard_SmallestSize_NotLargerThan_Fastest()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var values = new long[50_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = i % 31;

        long fastest = WriteAndMeasure(schema, values, BlockCompressionLevel.Fastest);
        long smallest = WriteAndMeasure(schema, values, BlockCompressionLevel.SmallestSize);

        Assert.True(smallest <= fastest, $"Zstd SmallestSize ({smallest}) > Fastest ({fastest})");
    }

    [Fact]
    public void Zstandard_CustomLevel_RoundTrips()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var values = new long[2_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = i;

        var b = new Int64Array.Builder();
        foreach (var v in values) b.Append(v);
        var batch = new RecordBatch(schema, [b.Build()], values.Length);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(AvroCodec.Zstandard)
            .WithCustomCompressionLevel(22)
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        int row = 0;
        while (reader.ReadNextBatch() is RecordBatch result)
        {
            var col = (Int64Array)result.Column(0);
            for (int i = 0; i < col.Length; i++)
                Assert.Equal(values[row++], col.GetValue(i));
        }
        Assert.Equal(values.Length, row);
    }

    private static long WriteAndMeasure(Apache.Arrow.Schema schema, long[] values, BlockCompressionLevel level)
    {
        var b = new Int64Array.Builder();
        foreach (var v in values) b.Append(v);
        var batch = new RecordBatch(schema, [b.Build()], values.Length);

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(schema)
            .WithCompression(AvroCodec.Zstandard)
            .WithCompressionLevel(level)
            .Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }
        return ms.Length;
    }
}
