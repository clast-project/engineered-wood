// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Buffers;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.Tests;

public class OrcWriterTests
{
    private static string GetTempPath() => Path.Combine(Path.GetTempPath(), $"storc_test_{Guid.NewGuid():N}.orc");

    [Fact]
    public async Task RoundTrip_IntegerColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            // Write
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var idArray = new Int64Array.Builder().AppendRange([1L, 2L, 3L, 100L, -50L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [idArray], 5));
            }

            // Read back
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);
            Assert.Single(reader.Schema.Children);

            var rowReader = reader.CreateRowReader();
            var batches = new List<RecordBatch>();
            await foreach (var batch in rowReader)
                batches.Add(batch);

            Assert.Single(batches);
            var col = (Int64Array)batches[0].Column(0);
            Assert.Equal(5, col.Length);
            Assert.Equal(1L, col.GetValue(0));
            Assert.Equal(2L, col.GetValue(1));
            Assert.Equal(3L, col.GetValue(2));
            Assert.Equal(100L, col.GetValue(3));
            Assert.Equal(-50L, col.GetValue(4));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_StringColumn_Direct()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("name", StringType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var arr = new StringArray.Builder().Append("hello").Append("world").Append("foo").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (StringArray)batch.Column(0);
                Assert.Equal("hello", col.GetString(0));
                Assert.Equal("world", col.GetString(1));
                Assert.Equal("foo", col.GetString(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_StringColumn_Dictionary()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("color", StringType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.DictionaryV2
            }))
            {
                var arr = new StringArray.Builder()
                    .Append("red").Append("blue").Append("red").Append("green").Append("blue")
                    .Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (StringArray)batch.Column(0);
                Assert.Equal("red", col.GetString(0));
                Assert.Equal("blue", col.GetString(1));
                Assert.Equal("red", col.GetString(2));
                Assert.Equal("green", col.GetString(3));
                Assert.Equal("blue", col.GetString(4));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MultipleColumns()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("id", Int32Type.Default, nullable: false),
                new Field("value", DoubleType.Default, nullable: false),
                new Field("flag", BooleanType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var ids = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                var vals = new DoubleArray.Builder().AppendRange([1.5, 2.5, 3.5]).Build();
                var flags = new BooleanArray.Builder().Append(true).Append(false).Append(true).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [ids, vals, flags], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                Assert.Equal(3, batch.Length);
                var ids = (Int32Array)batch.Column(0);
                var vals = (DoubleArray)batch.Column(1);
                var flags = (BooleanArray)batch.Column(2);

                Assert.Equal(10, ids.GetValue(0));
                Assert.Equal(20, ids.GetValue(1));
                Assert.Equal(30, ids.GetValue(2));
                Assert.Equal(1.5, vals.GetValue(0));
                Assert.Equal(2.5, vals.GetValue(1));
                Assert.Equal(3.5, vals.GetValue(2));
                Assert.True(flags.GetValue(0));
                Assert.False(flags.GetValue(1));
                Assert.True(flags.GetValue(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().Append(1).AppendNull().Append(3).AppendNull().Append(5).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(5, col.Length);
                Assert.Equal(1L, col.GetValue(0));
                Assert.False(col.IsValid(1));
                Assert.Equal(3L, col.GetValue(2));
                Assert.False(col.IsValid(3));
                Assert.Equal(5L, col.GetValue(4));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_WithCompression_Zstd()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.Zstd }))
            {
                var arr = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => (long)i)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1000, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            long total = 0;
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    total += col.GetValue(i)!.Value;
            }
            Assert.Equal(499500L, total); // sum 0..999
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MultipleBatches()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("x", Int32Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                for (int batch = 0; batch < 10; batch++)
                {
                    var arr = new Int32Array.Builder().AppendRange(Enumerable.Range(batch * 100, 100)).Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 100));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1000, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            int count = 0;
            await foreach (var batch in rowReader)
                count += batch.Length;
            Assert.Equal(1000, count);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task StripeSizeEnforcement_CreatesMultipleStripes()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("data", Int64Type.Default, nullable: false)], null);

            // Use a very small stripe size (1 KB) to force multiple stripes
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                StripeSize = 1024 // 1 KB
            }))
            {
                // Write 10,000 rows in batches of 1000
                // At 8 bytes per int64, 10,000 rows ≈ 80 KB, should create multiple stripes
                for (int batch = 0; batch < 10; batch++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(batch * 1000, 1000).Select(i => (long)i))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(10000, reader.NumberOfRows);
            Assert.True(reader.NumberOfStripes > 1, $"Expected multiple stripes but got {reader.NumberOfStripes}");

            // Verify all data reads back correctly
            var rowReader = reader.CreateRowReader();
            long total = 0;
            int count = 0;
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    total += col.GetValue(i)!.Value;
                count += batch.Length;
            }
            Assert.Equal(10000, count);
            Assert.Equal(49995000L, total); // sum 0..9999
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task StripeSizeEnforcement_DefaultStripeSize_SingleStripe()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("x", Int32Type.Default, nullable: false)], null);

            // Default stripe size is 64 MB, small data should stay in one stripe
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None
            }))
            {
                var arr = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 100));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(100, reader.NumberOfRows);
            Assert.Equal(1, reader.NumberOfStripes);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_FloatColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("f", FloatType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new FloatArray.Builder().AppendRange([1.0f, 2.5f, -3.14f]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (FloatArray)batch.Column(0);
                Assert.Equal(1.0f, col.GetValue(0));
                Assert.Equal(2.5f, col.GetValue(1));
                Assert.Equal(-3.14f, col.GetValue(2)!.Value, 0.001f);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RowIndex_BasicStructure()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            // Write 25,000 rows in batches of 1,000 with stride 10,000 → 3 row groups
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                RowIndexStride = 10_000
            }))
            {
                for (int b = 0; b < 25; b++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(b * 1000, 1000).Select(i => (long)i))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(25_000, reader.NumberOfRows);
            Assert.Equal((uint)10_000, reader.Footer.RowIndexStride);

            // Read row index for stripe 0
            var indexes = await reader.ReadRowIndexAsync(0);
            Assert.True(indexes.ContainsKey(0)); // root struct
            Assert.True(indexes.ContainsKey(1)); // id column

            // 3 row groups: [0..9999], [10000..19999], [20000..24999]
            var idIndex = indexes[1];
            Assert.Equal(3, idIndex.Entry.Count);

            // First row group starts at position 0
            Assert.True(idIndex.Entry[0].Positions.Count > 0);
            Assert.Equal(0UL, idIndex.Entry[0].Positions[0]); // byte offset = 0

            // Second row group starts at a non-zero position
            Assert.True(idIndex.Entry[1].Positions[0] > 0);

            // Verify all data reads correctly
            var rowReader = reader.CreateRowReader();
            long total = 0;
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    total += col.GetValue(i)!.Value;
            }
            Assert.Equal(Enumerable.Range(0, 25_000).Sum(i => (long)i), total);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RowIndex_WithStatistics()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: false)], null);

            // Write 20,000 rows in batches of 1,000 with stride 10,000 → 2 row groups
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                RowIndexStride = 10_000
            }))
            {
                for (int b = 0; b < 20; b++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(b * 1000, 1000).Select(i => (long)i))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var indexes = await reader.ReadRowIndexAsync(0);
            var valIndex = indexes[1];
            Assert.Equal(2, valIndex.Entry.Count);

            // Row group 0: values 0..9999
            var rg0 = valIndex.Entry[0];
            Assert.NotNull(rg0.Statistics);
            Assert.Equal(10_000UL, rg0.Statistics.NumberOfValues);
            Assert.Equal(0L, rg0.Statistics.IntStatistics.Minimum);
            Assert.Equal(9_999L, rg0.Statistics.IntStatistics.Maximum);

            // Row group 1: values 10000..19999
            var rg1 = valIndex.Entry[1];
            Assert.NotNull(rg1.Statistics);
            Assert.Equal(10_000UL, rg1.Statistics.NumberOfValues);
            Assert.Equal(10_000L, rg1.Statistics.IntStatistics.Minimum);
            Assert.Equal(19_999L, rg1.Statistics.IntStatistics.Maximum);

            // File-level stats should still be correct
            var fileStats = reader.Footer.Statistics[1];
            Assert.Equal(20_000UL, fileStats.NumberOfValues);
            Assert.Equal(0L, fileStats.IntStatistics.Minimum);
            Assert.Equal(19_999L, fileStats.IntStatistics.Maximum);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RowIndex_WithCompression()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zstd,
                RowIndexStride = 5_000
            }))
            {
                for (int b = 0; b < 15; b++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(b * 1000, 1000).Select(i => (long)i))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(15_000, reader.NumberOfRows);

            var indexes = await reader.ReadRowIndexAsync(0);
            var valIndex = indexes[1];
            Assert.Equal(3, valIndex.Entry.Count); // 3 row groups of 5000

            // Verify data reads correctly through compression
            var rowReader = reader.CreateRowReader();
            long total = 0;
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    total += col.GetValue(i)!.Value;
            }
            Assert.Equal(Enumerable.Range(0, 15_000).Sum(i => (long)i), total);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RowIndex_Disabled()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                RowIndexStride = 0 // disabled
            }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            // IndexLength should be 0
            Assert.Equal(0UL, reader.Footer.Stripes[0].IndexLength);

            var indexes = await reader.ReadRowIndexAsync(0);
            Assert.Empty(indexes);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RowIndex_MultipleStripes()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("data", Int64Type.Default, nullable: false)], null);

            // Use StripeSize=1024 with large random-ish data to force multiple stripes
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                StripeSize = 4096,
                RowIndexStride = 500
            }))
            {
                var rng = new Random(42);
                for (int batch = 0; batch < 20; batch++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(0, 1000).Select(_ => (long)rng.Next()))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.True(reader.NumberOfStripes > 1);

            // Each stripe should have row index entries
            for (int s = 0; s < reader.NumberOfStripes; s++)
            {
                Assert.True(reader.Footer.Stripes[s].IndexLength > 0,
                    $"Stripe {s} should have index data");
                var indexes = await reader.ReadRowIndexAsync(s);
                Assert.True(indexes.ContainsKey(1)); // data column
                Assert.True(indexes[1].Entry.Count > 0);
            }

            // Verify all data reads correctly
            var rowReader = reader.CreateRowReader();
            int count = 0;
            await foreach (var batch in rowReader)
                count += batch.Length;
            Assert.Equal(20000, count);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_IntegerColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L, 100L, -50L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            // File-level statistics: column 0 = root struct, column 1 = "id"
            var footer = reader.Footer;
            Assert.True(footer.Statistics.Count >= 2);

            var idStats = footer.Statistics[1];
            Assert.Equal(5UL, idStats.NumberOfValues);
            Assert.False(idStats.HasNull);
            Assert.NotNull(idStats.IntStatistics);
            Assert.Equal(-50L, idStats.IntStatistics.Minimum);
            Assert.Equal(100L, idStats.IntStatistics.Maximum);
            Assert.Equal(56L, idStats.IntStatistics.Sum); // 1+2+3+100-50
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_Disabled()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                EnableStatistics = false
            }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var footer = reader.Footer;
            Assert.Empty(footer.Statistics);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().Append(10).AppendNull().Append(30).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var stats = reader.Footer.Statistics[1];
            Assert.Equal(2UL, stats.NumberOfValues); // only non-null
            Assert.True(stats.HasNull);
            Assert.Equal(10L, stats.IntStatistics.Minimum);
            Assert.Equal(30L, stats.IntStatistics.Maximum);
            Assert.Equal(40L, stats.IntStatistics.Sum);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_MultipleTypes()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("id", Int32Type.Default, nullable: false),
                new Field("value", DoubleType.Default, nullable: false),
                new Field("flag", BooleanType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var ids = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                var vals = new DoubleArray.Builder().AppendRange([1.5, 2.5, 3.5]).Build();
                var flags = new BooleanArray.Builder().Append(true).Append(false).Append(true).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [ids, vals, flags], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var footer = reader.Footer;

            // Column 1: id (int)
            var idStats = footer.Statistics[1];
            Assert.Equal(10L, idStats.IntStatistics.Minimum);
            Assert.Equal(30L, idStats.IntStatistics.Maximum);
            Assert.Equal(60L, idStats.IntStatistics.Sum);

            // Column 2: value (double)
            var valStats = footer.Statistics[2];
            Assert.Equal(1.5, valStats.DoubleStatistics.Minimum);
            Assert.Equal(3.5, valStats.DoubleStatistics.Maximum);
            Assert.Equal(7.5, valStats.DoubleStatistics.Sum);

            // Column 3: flag (boolean)
            var flagStats = footer.Statistics[3];
            Assert.Single(flagStats.BucketStatistics.Count);
            Assert.Equal(2UL, flagStats.BucketStatistics.Count[0]); // 2 true values
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_StringColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("name", StringType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var arr = new StringArray.Builder().Append("banana").Append("apple").Append("cherry").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var stats = reader.Footer.Statistics[1];
            Assert.Equal(3UL, stats.NumberOfValues);
            Assert.Equal("apple", stats.StringStatistics.Minimum);
            Assert.Equal("cherry", stats.StringStatistics.Maximum);
            Assert.Equal(17L, stats.StringStatistics.Sum); // banana(6)+apple(5)+cherry(6)
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_StructColumn()
    {
        var path = GetTempPath();
        try
        {
            var structType = new StructType([
                new Field("x", Int32Type.Default, nullable: false),
                new Field("y", StringType.Default, nullable: false),
            ]);
            var schema = new Schema([new Field("s", structType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var xs = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                var ys = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
                var structArr = new StructArray(structType, 3, [xs, ys], ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [structArr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                Assert.Equal(3, batch.Length);
                var col = (StructArray)batch.Column(0);
                var xCol = (Int32Array)col.Fields[0];
                var yCol = (StringArray)col.Fields[1];
                Assert.Equal(1, xCol.GetValue(0));
                Assert.Equal(2, xCol.GetValue(1));
                Assert.Equal(3, xCol.GetValue(2));
                Assert.Equal("a", yCol.GetString(0));
                Assert.Equal("b", yCol.GetString(1));
                Assert.Equal("c", yCol.GetString(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_ListColumn()
    {
        var path = GetTempPath();
        try
        {
            var listType = new ListType(new Field("item", Int32Type.Default, nullable: false));
            var schema = new Schema([new Field("nums", listType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // Build list array: [[1,2], [3], [4,5,6]]
                var values = new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5, 6]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3, 6]).Build();
                var listArr = new ListArray(listType, 3, offsets, values, ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [listArr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                Assert.Equal(3, batch.Length);
                var col = (ListArray)batch.Column(0);

                // First list: [1, 2]
                var list0 = (Int32Array)col.GetSlicedValues(0);
                Assert.Equal(2, list0.Length);
                Assert.Equal(1, list0.GetValue(0));
                Assert.Equal(2, list0.GetValue(1));

                // Second list: [3]
                var list1 = (Int32Array)col.GetSlicedValues(1);
                Assert.Equal(1, list1.Length);
                Assert.Equal(3, list1.GetValue(0));

                // Third list: [4, 5, 6]
                var list2 = (Int32Array)col.GetSlicedValues(2);
                Assert.Equal(3, list2.Length);
                Assert.Equal(4, list2.GetValue(0));
                Assert.Equal(5, list2.GetValue(1));
                Assert.Equal(6, list2.GetValue(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MapColumn()
    {
        var path = GetTempPath();
        try
        {
            var keyField = new Field("key", StringType.Default, nullable: false);
            var valueField = new Field("value", Int32Type.Default, nullable: true);
            var mapType = new MapType(keyField, valueField);
            var schema = new Schema([new Field("m", mapType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // Build map array: [{"a":1, "b":2}, {"c":3}]
                var keys = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
                var vals = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();

                var kvStructType = new StructType([keyField, valueField]);
                var kvStruct = new StructArray(kvStructType, 3, [keys, vals], ArrowBuffer.Empty, 0);

                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3]).Build();
                var mapArr = new MapArray(mapType, 2, offsets, kvStruct, ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArr], 2));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(2, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                Assert.Equal(2, batch.Length);
                var col = (MapArray)batch.Column(0);

                // Access the underlying key/value arrays
                var kvStruct = col.KeyValues;
                var allKeys = (StringArray)kvStruct.Fields[0];
                var allVals = (Int32Array)kvStruct.Fields[1];

                Assert.Equal(3, allKeys.Length);
                Assert.Equal("a", allKeys.GetString(0));
                Assert.Equal("b", allKeys.GetString(1));
                Assert.Equal("c", allKeys.GetString(2));

                Assert.Equal(1, allVals.GetValue(0));
                Assert.Equal(2, allVals.GetValue(1));
                Assert.Equal(3, allVals.GetValue(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_NestedStruct()
    {
        var path = GetTempPath();
        try
        {
            var innerType = new StructType([
                new Field("a", Int32Type.Default, nullable: false),
                new Field("b", Int32Type.Default, nullable: false),
            ]);
            var schema = new Schema([
                new Field("id", Int32Type.Default, nullable: false),
                new Field("nested", innerType, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var ids = new Int32Array.Builder().AppendRange([10, 20]).Build();
                var aArr = new Int32Array.Builder().AppendRange([100, 200]).Build();
                var bArr = new Int32Array.Builder().AppendRange([300, 400]).Build();
                var nested = new StructArray(innerType, 2, [aArr, bArr], ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [ids, nested], 2));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(2, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var idCol = (Int32Array)batch.Column(0);
                Assert.Equal(10, idCol.GetValue(0));
                Assert.Equal(20, idCol.GetValue(1));

                var nestedCol = (StructArray)batch.Column(1);
                var aCol = (Int32Array)nestedCol.Fields[0];
                var bCol = (Int32Array)nestedCol.Fields[1];
                Assert.Equal(100, aCol.GetValue(0));
                Assert.Equal(200, aCol.GetValue(1));
                Assert.Equal(300, bCol.GetValue(0));
                Assert.Equal(400, bCol.GetValue(1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_ListOfStruct()
    {
        var path = GetTempPath();
        try
        {
            var elemType = new StructType([
                new Field("x", Int32Type.Default, nullable: false),
            ]);
            var listType = new ListType(new Field("item", elemType, nullable: false));
            var schema = new Schema([new Field("items", listType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // [[{x:1},{x:2}], [{x:3}]]
                var xArr = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                var elemArr = new StructArray(elemType, 3, [xArr], ArrowBuffer.Empty, 0);
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3]).Build();
                var listArr = new ListArray(listType, 2, offsets, elemArr, ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [listArr], 2));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(2, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (ListArray)batch.Column(0);

                var list0 = (StructArray)col.GetSlicedValues(0);
                Assert.Equal(2, list0.Length);
                Assert.Equal(1, ((Int32Array)list0.Fields[0]).GetValue(0));
                Assert.Equal(2, ((Int32Array)list0.Fields[0]).GetValue(1));

                var list1 = (StructArray)col.GetSlicedValues(1);
                Assert.Equal(1, list1.Length);
                Assert.Equal(3, ((Int32Array)list1.Fields[0]).GetValue(0));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_ByteColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("b", Int8Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int8Array.Builder().Append(-128).Append(-1).Append(0).Append(1).Append(127).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int8Array)batch.Column(0);
                Assert.Equal(5, col.Length);
                Assert.Equal((sbyte)-128, col.GetValue(0));
                Assert.Equal((sbyte)-1, col.GetValue(1));
                Assert.Equal((sbyte)0, col.GetValue(2));
                Assert.Equal((sbyte)1, col.GetValue(3));
                Assert.Equal((sbyte)127, col.GetValue(4));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_ShortColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("s", Int16Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int16Array.Builder().Append(-32768).Append(-1).Append(0).Append(1).Append(32767).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int16Array)batch.Column(0);
                Assert.Equal(5, col.Length);
                Assert.Equal((short)-32768, col.GetValue(0));
                Assert.Equal((short)-1, col.GetValue(1));
                Assert.Equal((short)0, col.GetValue(2));
                Assert.Equal((short)1, col.GetValue(3));
                Assert.Equal((short)32767, col.GetValue(4));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_BinaryColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("data", BinaryType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new BinaryArray.Builder().Append(System.Array.Empty<byte>()).Append(new byte[] { 0x01, 0x02 }).Append(new byte[] { 0xFF }).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (BinaryArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.Equal(System.Array.Empty<byte>(), col.GetBytes(0).ToArray());
                Assert.Equal(new byte[] { 0x01, 0x02 }, col.GetBytes(1).ToArray());
                Assert.Equal(new byte[] { 0xFF }, col.GetBytes(2).ToArray());
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_BinaryColumn_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("data", BinaryType.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new BinaryArray.Builder().Append(new byte[] { 0x01, 0x02 }).AppendNull().Append(new byte[] { 0x03 }).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (BinaryArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.True(col.IsValid(0));
                Assert.Equal(new byte[] { 0x01, 0x02 }, col.GetBytes(0).ToArray());
                Assert.False(col.IsValid(1));
                Assert.True(col.IsValid(2));
                Assert.Equal(new byte[] { 0x03 }, col.GetBytes(2).ToArray());
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_TimestampColumn()
    {
        var path = GetTempPath();
        try
        {
            var tsType = new TimestampType(Apache.Arrow.Types.TimeUnit.Nanosecond, (string?)null);
            var schema = new Schema([new Field("ts", tsType, nullable: false)], null);

            var ts1 = new DateTimeOffset(2024, 1, 15, 12, 30, 0, TimeSpan.Zero);
#if NET8_0_OR_GREATER
            var ts2 = DateTimeOffset.UnixEpoch;
#else
            var ts2 = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
#endif
            var ts3 = new DateTimeOffset(2024, 6, 15, 8, 0, 0, 500, TimeSpan.Zero).AddTicks(1234); // sub-ms precision

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var builder = new TimestampArray.Builder(tsType);
                builder.Append(ts1);
                builder.Append(ts2);
                builder.Append(ts3);
                var arr = builder.Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (TimestampArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                // Verify by converting back to ticks for comparison
                var v0 = col.GetValue(0)!.Value; // epoch nanos
                var v1 = col.GetValue(1)!.Value;
                Assert.Equal(ts1.ToUnixTimeMilliseconds() * 1_000_000L, v0);
                Assert.Equal(0L, v1);
                Assert.NotNull(col.GetValue(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_TimestampColumn_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var tsType = new TimestampType(Apache.Arrow.Types.TimeUnit.Nanosecond, (string?)null);
            var schema = new Schema([new Field("ts", tsType, nullable: true)], null);

            var ts1 = new DateTimeOffset(2024, 1, 15, 12, 30, 0, TimeSpan.Zero);
            var ts3 = new DateTimeOffset(2024, 6, 15, 8, 0, 0, TimeSpan.Zero);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var builder = new TimestampArray.Builder(tsType);
                builder.Append(ts1);
                builder.AppendNull();
                builder.Append(ts3);
                var arr = builder.Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (TimestampArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.True(col.IsValid(0));
                Assert.Equal(ts1.ToUnixTimeMilliseconds() * 1_000_000L, col.GetValue(0)!.Value);
                Assert.False(col.IsValid(1));
                Assert.True(col.IsValid(2));
                Assert.Equal(ts3.ToUnixTimeMilliseconds() * 1_000_000L, col.GetValue(2)!.Value);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_AllNullColumn_Integer()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendNull().AppendNull().AppendNull().Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.False(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.False(col.IsValid(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_AllNullColumn_String()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", StringType.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var arr = new StringArray.Builder().AppendNull().AppendNull().AppendNull().Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (StringArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.False(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.False(col.IsValid(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_AllNullColumn_Boolean()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", BooleanType.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new BooleanArray.Builder().AppendNull().AppendNull().AppendNull().Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (BooleanArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.False(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.False(col.IsValid(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_AllNullColumn_Double()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", DoubleType.Default, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new DoubleArray.Builder().AppendNull().AppendNull().AppendNull().Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (DoubleArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.False(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.False(col.IsValid(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_SingleRow()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().Append(42L).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(1, col.Length);
                Assert.Equal(42L, col.GetValue(0));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_Compression_Zlib()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.Zlib }))
            {
                var arr = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => (long)i)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1000, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(0L, col.GetValue(0));
                Assert.Equal(999L, col.GetValue(col.Length - 1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_Compression_Lz4()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.Lz4 }))
            {
                var arr = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => (long)i)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1000, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(0L, col.GetValue(0));
                Assert.Equal(999L, col.GetValue(col.Length - 1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_Compression_Snappy()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.Snappy }))
            {
                var arr = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => (long)i)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1000, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(0L, col.GetValue(0));
                Assert.Equal(999L, col.GetValue(col.Length - 1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_Compression_Zlib_MultipleTypes()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("id", Int32Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
                new Field("val", DoubleType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zlib,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var ids = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 100)).Build();
                var names = new StringArray.Builder();
                for (int i = 0; i < 100; i++)
                    names.Append($"item_{i}");
                var nameArr = names.Build();
                var vals = new DoubleArray.Builder().AppendRange(Enumerable.Range(0, 100).Select(i => i * 1.5)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [ids, nameArr, vals], 100));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(100, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                Assert.Equal(100, batch.Length);

                var idCol = (Int32Array)batch.Column(0);
                Assert.Equal(0, idCol.GetValue(0));
                Assert.Equal(99, idCol.GetValue(99));

                var nameCol = (StringArray)batch.Column(1);
                for (int i = 0; i < 100; i++)
                    Assert.Equal($"item_{i}", nameCol.GetString(i));

                var valCol = (DoubleArray)batch.Column(2);
                Assert.Equal(0.0, valCol.GetValue(0));
                Assert.Equal(99 * 1.5, valCol.GetValue(99));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static Decimal128Array BuildDecimal128Array(Decimal128Type type, long[] values)
    {
        var bytes = new byte[values.Length * 16];
        for (int i = 0; i < values.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 16), values[i]);
            long sign = values[i] < 0 ? -1L : 0L;
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 16 + 8), sign);
        }
        var valueBuf = new ArrowBuffer(bytes);
        var nullBuf = ArrowBuffer.Empty;
        var data = new ArrayData(type, values.Length, 0, 0, [nullBuf, valueBuf]);
        return new Decimal128Array(data);
    }

    private static Decimal128Array BuildDecimal128ArrayWithNulls(Decimal128Type type, long?[] values)
    {
        var bytes = new byte[values.Length * 16];
        var nullBits = new byte[(values.Length + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].HasValue)
            {
                long v = values[i]!.Value;
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 16), v);
                long sign = v < 0 ? -1L : 0L;
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 16 + 8), sign);
                nullBits[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                nullCount++;
            }
        }
        var valueBuf = new ArrowBuffer(bytes);
        var nullBuf = nullCount > 0 ? new ArrowBuffer(nullBits) : ArrowBuffer.Empty;
        var data = new ArrayData(type, values.Length, nullCount, 0, [nullBuf, valueBuf]);
        return new Decimal128Array(data);
    }

    [Fact]
    public async Task RoundTrip_DecimalColumn()
    {
        var path = GetTempPath();
        try
        {
            var decType = new Decimal128Type(10, 2);
            var schema = new Schema([new Field("amount", decType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // Values are unscaled: 1234 means 12.34 at scale 2
                var arr = BuildDecimal128Array(decType, [1234L, 5678L, -999L]);
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Decimal128Array)batch.Column(0);
                Assert.Equal(3, col.Length);

                // Read back the raw 128-bit values as longs
                long v0 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(0, 16));
                long v1 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(16, 16));
                long v2 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(32, 16));
                Assert.Equal(1234L, v0);
                Assert.Equal(5678L, v1);
                Assert.Equal(-999L, v2);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_DecimalColumn_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var decType = new Decimal128Type(10, 2);
            var schema = new Schema([new Field("amount", decType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = BuildDecimal128ArrayWithNulls(decType, [100L, null, 300L]);
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Decimal128Array)batch.Column(0);
                Assert.Equal(3, col.Length);
                Assert.True(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.True(col.IsValid(2));

                long v0 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(0, 16));
                long v2 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(32, 16));
                Assert.Equal(100L, v0);
                Assert.Equal(300L, v2);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task StripeStatistics_SingleStripe()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L, 100L, -50L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var metadata = await reader.ReadMetadataAsync();
            Assert.NotNull(metadata);
            Assert.Single(metadata.StripeStats); // 1 stripe

            var stripeStats = metadata.StripeStats[0];
            // Column 0 = root struct, Column 1 = "id"
            Assert.True(stripeStats.ColStats.Count >= 2);

            var rootStats = stripeStats.ColStats[0];
            Assert.Equal(5UL, rootStats.NumberOfValues);

            var idStats = stripeStats.ColStats[1];
            Assert.Equal(5UL, idStats.NumberOfValues);
            Assert.Equal(-50L, idStats.IntStatistics.Minimum);
            Assert.Equal(100L, idStats.IntStatistics.Maximum);
            Assert.Equal(56L, idStats.IntStatistics.Sum);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task StripeStatistics_MultipleStripes()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("data", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                StripeSize = 1024
            }))
            {
                for (int batch = 0; batch < 10; batch++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(batch * 1000, 1000).Select(i => (long)i))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.True(reader.NumberOfStripes > 1);

            var metadata = await reader.ReadMetadataAsync();
            Assert.NotNull(metadata);
            Assert.Equal(reader.NumberOfStripes, metadata.StripeStats.Count);

            // Each stripe should have stats for root + 1 column
            foreach (var stripeStats in metadata.StripeStats)
            {
                Assert.Equal(2, stripeStats.ColStats.Count);
                Assert.True(stripeStats.ColStats[0].NumberOfValues > 0); // root
                Assert.True(stripeStats.ColStats[1].NumberOfValues > 0); // data column
                Assert.NotNull(stripeStats.ColStats[1].IntStatistics);
            }

            // Verify first stripe min starts at 0
            Assert.Equal(0L, metadata.StripeStats[0].ColStats[1].IntStatistics.Minimum);

            // Verify stripe stats are non-overlapping (each stripe has increasing ranges)
            for (int i = 1; i < metadata.StripeStats.Count; i++)
            {
                var prev = metadata.StripeStats[i - 1].ColStats[1].IntStatistics;
                var curr = metadata.StripeStats[i].ColStats[1].IntStatistics;
                Assert.True(curr.Minimum > prev.Maximum,
                    $"Stripe {i} min ({curr.Minimum}) should be > stripe {i-1} max ({prev.Maximum})");
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task StripeStatistics_Disabled()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                EnableStatistics = false
            }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var metadata = await reader.ReadMetadataAsync();
            Assert.Null(metadata); // No metadata section when stats disabled
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task StripeStatistics_WithCompression()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zstd,
                StripeSize = 1024
            }))
            {
                for (int batch = 0; batch < 5; batch++)
                {
                    var arr = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(batch * 500, 500).Select(i => (long)i))
                        .Build();
                    await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 500));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var metadata = await reader.ReadMetadataAsync();
            Assert.NotNull(metadata);
            Assert.Equal(reader.NumberOfStripes, metadata.StripeStats.Count);

            // All data should round-trip correctly
            var rowReader = reader.CreateRowReader();
            long total = 0;
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    total += col.GetValue(i)!.Value;
            }
            Assert.Equal(Enumerable.Range(0, 2500).Sum(i => (long)i), total);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_DecimalColumn()
    {
        var path = GetTempPath();
        try
        {
            var decType = new Decimal128Type(10, 2);
            var schema = new Schema([new Field("amount", decType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 12.34, 56.78, -9.99
                var arr = BuildDecimal128Array(decType, [1234L, 5678L, -999L]);
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var stats = reader.Footer.Statistics[1];
            Assert.Equal(3UL, stats.NumberOfValues);
            Assert.NotNull(stats.DecimalStatistics);
            Assert.Equal("-9.99", stats.DecimalStatistics.Minimum);
            Assert.Equal("56.78", stats.DecimalStatistics.Maximum);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Statistics_ListColumn_CollectionStatistics()
    {
        var path = GetTempPath();
        try
        {
            var listType = new ListType(Int32Type.Default);
            var schema = new Schema([new Field("items", listType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
            }))
            {
                // Lists with lengths: 2, 0, 3, null, 1
                var builder = new ListArray.Builder(Int32Type.Default);
                var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;

                builder.Append(); valueBuilder.Append(1); valueBuilder.Append(2); // length 2
                builder.Append(); // length 0 (empty list)
                builder.Append(); valueBuilder.Append(3); valueBuilder.Append(4); valueBuilder.Append(5); // length 3
                builder.AppendNull(); // null
                builder.Append(); valueBuilder.Append(6); // length 1

                await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            // Column 0 = struct root, column 1 = list, column 2 = list element
            var listStats = reader.Footer.Statistics[1];
            Assert.NotNull(listStats.CollectionStatistics);
            Assert.Equal(0UL, listStats.CollectionStatistics.MinChildren); // empty list
            Assert.Equal(3UL, listStats.CollectionStatistics.MaxChildren);
            Assert.Equal(6UL, listStats.CollectionStatistics.TotalChildren); // 2+0+3+1 = 6
            Assert.Equal(4UL, listStats.NumberOfValues); // 4 non-null lists
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task Statistics_MapColumn_CollectionStatistics()
    {
        var path = GetTempPath();
        try
        {
            var mapType = new MapType(StringType.Default, Int32Type.Default);
            var schema = new Schema([new Field("m", mapType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
            }))
            {
                var keyBuilder = new StringArray.Builder();
                var valBuilder = new Int32Array.Builder();
                var offsetBuilder = new ArrowBuffer.Builder<int>();
                var validityBuilder = new ArrowBuffer.BitmapBuilder();
                offsetBuilder.Append(0);

                // Map with 2 entries
                keyBuilder.Append("a"); valBuilder.Append(1);
                keyBuilder.Append("b"); valBuilder.Append(2);
                offsetBuilder.Append(2); validityBuilder.Append(true);

                // Map with 1 entry
                keyBuilder.Append("c"); valBuilder.Append(3);
                offsetBuilder.Append(3); validityBuilder.Append(true);

                // Null map
                offsetBuilder.Append(3); validityBuilder.Append(false);

                // Map with 3 entries
                keyBuilder.Append("d"); valBuilder.Append(4);
                keyBuilder.Append("e"); valBuilder.Append(5);
                keyBuilder.Append("f"); valBuilder.Append(6);
                offsetBuilder.Append(6); validityBuilder.Append(true);

                var keys = keyBuilder.Build();
                var vals = valBuilder.Build();
                var structFields = new IArrowArray[] { keys, vals };
                var structType = new StructType([
                    new Field("key", StringType.Default, false),
                    new Field("value", Int32Type.Default, true)
                ]);
                var keyValues = new StructArray(structType, keys.Length, structFields, ArrowBuffer.Empty);
                var mapArray = new MapArray(mapType, 4, offsetBuilder.Build(),
                    keyValues, validityBuilder.Build(), 1);

                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArray], 4));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var mapStats = reader.Footer.Statistics[1];
            Assert.NotNull(mapStats.CollectionStatistics);
            Assert.Equal(1UL, mapStats.CollectionStatistics.MinChildren);
            Assert.Equal(3UL, mapStats.CollectionStatistics.MaxChildren);
            Assert.Equal(6UL, mapStats.CollectionStatistics.TotalChildren); // 2+1+3 = 6
            Assert.Equal(3UL, mapStats.NumberOfValues); // 3 non-null maps
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task Statistics_ListColumn_CollectionStatistics_MultiStripe()
    {
        var path = GetTempPath();
        try
        {
            var listType = new ListType(Int32Type.Default);
            var schema = new Schema([new Field("items", listType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                StripeSize = 1, // force new stripe per batch
            }))
            {
                // Stripe 1: lists with lengths 5, 2
                var b1 = new ListArray.Builder(Int32Type.Default);
                var v1 = (Int32Array.Builder)b1.ValueBuilder;
                b1.Append(); for (int i = 0; i < 5; i++) v1.Append(i); // length 5
                b1.Append(); v1.Append(10); v1.Append(11); // length 2
                await writer.WriteBatchAsync(new RecordBatch(schema, [b1.Build()], 2));

                // Stripe 2: lists with lengths 1, 4
                var b2 = new ListArray.Builder(Int32Type.Default);
                var v2 = (Int32Array.Builder)b2.ValueBuilder;
                b2.Append(); v2.Append(20); // length 1
                b2.Append(); for (int i = 0; i < 4; i++) v2.Append(30 + i); // length 4
                await writer.WriteBatchAsync(new RecordBatch(schema, [b2.Build()], 2));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            // File-level stats should be merged across stripes
            var listStats = reader.Footer.Statistics[1];
            Assert.NotNull(listStats.CollectionStatistics);
            Assert.Equal(1UL, listStats.CollectionStatistics.MinChildren); // min(2,1) = 1
            Assert.Equal(5UL, listStats.CollectionStatistics.MaxChildren); // max(5,4) = 5
            Assert.Equal(12UL, listStats.CollectionStatistics.TotalChildren); // 5+2+1+4 = 12
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_DateColumn()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("d", Date32Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // Date32 stores days since epoch
                var arr = new Date32Array.Builder().Append(new DateTime(2024, 1, 15)).Append(new DateTime(2024, 6, 30)).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 2));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Date32Array)batch.Column(0);
                Assert.Equal(2, col.Length);
                // Verify dates round-trip correctly
                int day0 = col.GetValue(0)!.Value;
                int day1 = col.GetValue(1)!.Value;
                Assert.Equal(new DateTime(2024, 1, 15), new DateTime(1970, 1, 1).AddDays(day0));
                Assert.Equal(new DateTime(2024, 6, 30), new DateTime(1970, 1, 1).AddDays(day1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_ShortRepeatEncoding()
    {
        // Write a small number of identical values (3-10) to trigger Short Repeat
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 5 identical values → should use Short Repeat
                var arr = new Int64Array.Builder().AppendRange([42L, 42L, 42L, 42L, 42L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(5, col.Length);
                for (int i = 0; i < 5; i++)
                    Assert.Equal(42L, col.GetValue(i));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_ShortRepeatEncoding_Signed()
    {
        // Short Repeat with negative value (tests zigzag in Short Repeat)
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([-100L, -100L, -100L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(3, col.Length);
                for (int i = 0; i < 3; i++)
                    Assert.Equal(-100L, col.GetValue(i));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_PatchedBaseEncoding()
    {
        // Data with mostly small values and a few outliers to trigger Patched Base
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 100 small values (0-50) with 3 outliers (1000000+)
                var builder = new Int64Array.Builder();
                var rng = new Random(123);
                for (int i = 0; i < 100; i++)
                {
                    if (i == 10 || i == 50 || i == 90)
                        builder.Append(1_000_000L + rng.Next(1000));
                    else
                        builder.Append(rng.Next(50));
                }
                var arr = builder.Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 100));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(100, reader.NumberOfRows);

            // Re-generate the same data to verify
            var expected = new long[100];
            var rng2 = new Random(123);
            for (int i = 0; i < 100; i++)
            {
                if (i == 10 || i == 50 || i == 90)
                    expected[i] = 1_000_000L + rng2.Next(1000);
                else
                    expected[i] = rng2.Next(50);
            }

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(100, col.Length);
                for (int i = 0; i < 100; i++)
                    Assert.Equal(expected[i], col.GetValue(i));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_PatchedBaseEncoding_WithCompression()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("val", Int64Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.Zlib }))
            {
                var builder = new Int64Array.Builder();
                var rng = new Random(456);
                for (int i = 0; i < 200; i++)
                {
                    if (i % 40 == 0)
                        builder.Append(10_000_000L + rng.Next(10000));
                    else
                        builder.Append(rng.Next(100));
                }
                var arr = builder.Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 200));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(200, reader.NumberOfRows);

            var expected = new long[200];
            var rng2 = new Random(456);
            for (int i = 0; i < 200; i++)
            {
                if (i % 40 == 0)
                    expected[i] = 10_000_000L + rng2.Next(10000);
                else
                    expected[i] = rng2.Next(100);
            }

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    Assert.Equal(expected[i], col.GetValue(i));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task UserMetadata_RoundTrip()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int32Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                UserMetadata = new Dictionary<string, byte[]>
                {
                    ["author"] = System.Text.Encoding.UTF8.GetBytes("test-user"),
                    ["version"] = [1, 2, 3],
                }
            }))
            {
                var arr = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var meta = reader.UserMetadata;

            Assert.Equal(2, meta.Count);
            Assert.Equal("test-user", System.Text.Encoding.UTF8.GetString(meta["author"]));
            Assert.Equal(new byte[] { 1, 2, 3 }, meta["version"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task UserMetadata_Empty()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int32Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int32Array.Builder().Append(1).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var meta = reader.UserMetadata;
            Assert.Empty(meta);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_UnionColumn()
    {
        var path = GetTempPath();
        try
        {
            var unionType = new UnionType(
                [
                    new Field("i", Int32Type.Default, nullable: true),
                    new Field("s", StringType.Default, nullable: true),
                ],
                [0, 1],
                UnionMode.Dense);
            var schema = new Schema([new Field("u", unionType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // Build DenseUnionArray: [int(10), string("hello"), int(20), string("world"), int(30)]
                var typeIds = new byte[] { 0, 1, 0, 1, 0 };
                var offsets = new int[] { 0, 0, 1, 1, 2 };

                var intChild = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                var strChild = new StringArray.Builder().Append("hello").Append("world").Build();

                var typeIdBuf = new ArrowBuffer(typeIds);
                var offsetBuf = new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray());

                var unionArr = new DenseUnionArray(unionType, 5, [intChild, strChild], typeIdBuf, offsetBuf);
                await writer.WriteBatchAsync(new RecordBatch(schema, [unionArr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                Assert.Equal(5, batch.Length);
                var col = (DenseUnionArray)batch.Column(0);

                // Verify type IDs
                Assert.Equal(0, col.TypeIds[0]); // int
                Assert.Equal(1, col.TypeIds[1]); // string
                Assert.Equal(0, col.TypeIds[2]); // int
                Assert.Equal(1, col.TypeIds[3]); // string
                Assert.Equal(0, col.TypeIds[4]); // int

                // Verify int child values
                var ints = (Int32Array)col.Fields[0];
                Assert.Equal(3, ints.Length);
                Assert.Equal(10, ints.GetValue(0));
                Assert.Equal(20, ints.GetValue(1));
                Assert.Equal(30, ints.GetValue(2));

                // Verify string child values
                var strs = (StringArray)col.Fields[1];
                Assert.Equal(2, strs.Length);
                Assert.Equal("hello", strs.GetString(0));
                Assert.Equal("world", strs.GetString(1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_UnionColumn_WithCompression()
    {
        var path = GetTempPath();
        try
        {
            var unionType = new UnionType(
                [
                    new Field("a", Int64Type.Default, nullable: true),
                    new Field("b", DoubleType.Default, nullable: true),
                ],
                [0, 1],
                UnionMode.Dense);
            var schema = new Schema([new Field("u", unionType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zstd,
            }))
            {
                var typeIds = new byte[] { 0, 0, 1, 1, 0 };
                var offsets = new int[] { 0, 1, 0, 1, 2 };

                var intChild = new Int64Array.Builder().AppendRange([100L, 200L, 300L]).Build();
                var dblChild = new DoubleArray.Builder().AppendRange([1.5, 2.5]).Build();

                var typeIdBuf = new ArrowBuffer(typeIds);
                var offsetBuf = new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray());

                var unionArr = new DenseUnionArray(unionType, 5, [intChild, dblChild], typeIdBuf, offsetBuf);
                await writer.WriteBatchAsync(new RecordBatch(schema, [unionArr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (DenseUnionArray)batch.Column(0);
                Assert.Equal(5, col.Length);

                var longs = (Int64Array)col.Fields[0];
                Assert.Equal(3, longs.Length);
                Assert.Equal(100L, longs.GetValue(0));
                Assert.Equal(200L, longs.GetValue(1));
                Assert.Equal(300L, longs.GetValue(2));

                var doubles = (DoubleArray)col.Fields[1];
                Assert.Equal(2, doubles.Length);
                Assert.Equal(1.5, doubles.GetValue(0));
                Assert.Equal(2.5, doubles.GetValue(1));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task UserMetadata_WithCompression()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int32Type.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zstd,
                UserMetadata = new Dictionary<string, byte[]>
                {
                    ["key1"] = System.Text.Encoding.UTF8.GetBytes("value1"),
                }
            }))
            {
                var arr = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var meta = reader.UserMetadata;
            Assert.Single(meta);
            Assert.Equal("value1", System.Text.Encoding.UTF8.GetString(meta["key1"]));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_DictionaryFallback_ExceedsThreshold()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("name", StringType.Default, nullable: false)], null);

            // Use a very small dictionary threshold to force fallback
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.DictionaryV2,
                DictionaryKeySizeThreshold = 5
            }))
            {
                // Write 10 unique strings — exceeds threshold of 5
                var builder = new StringArray.Builder();
                for (int i = 0; i < 10; i++)
                    builder.Append($"unique_{i}");
                await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 10));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(10, reader.NumberOfRows);

            // Verify encoding fell back to Direct
            var encoding = reader.Footer.Stripes[0];

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (StringArray)batch.Column(0);
                Assert.Equal(10, col.Length);
                for (int i = 0; i < 10; i++)
                    Assert.Equal($"unique_{i}", col.GetString(i));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_DictionaryFallback_MixedBatches()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("name", StringType.Default, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.DictionaryV2,
                DictionaryKeySizeThreshold = 3
            }))
            {
                // First batch: 3 unique strings (at threshold)
                var b1 = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [b1], 3));

                // Second batch: more unique strings (should trigger fallback)
                var b2 = new StringArray.Builder().Append("d").Append("e").Append("f").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [b2], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(6, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            var all = new List<string>();
            await foreach (var batch in rowReader)
            {
                var col = (StringArray)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    all.Add(col.GetString(i)!);
            }

            Assert.Equal(["a", "b", "c", "d", "e", "f"], all);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_PerColumnEncodingOverride()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("dict_col", StringType.Default, nullable: false),
                new Field("direct_col", StringType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.DictionaryV2,
                ColumnEncodings = new Dictionary<string, EncodingFamily>
                {
                    ["direct_col"] = EncodingFamily.V2 // override to direct
                }
            }))
            {
                var dictArr = new StringArray.Builder().Append("red").Append("blue").Append("red").Build();
                var directArr = new StringArray.Builder().Append("x").Append("y").Append("z").Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [dictArr, directArr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var dictCol = (StringArray)batch.Column(0);
                Assert.Equal("red", dictCol.GetString(0));
                Assert.Equal("blue", dictCol.GetString(1));
                Assert.Equal("red", dictCol.GetString(2));

                var directCol = (StringArray)batch.Column(1);
                Assert.Equal("x", directCol.GetString(0));
                Assert.Equal("y", directCol.GetString(1));
                Assert.Equal("z", directCol.GetString(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_SmallCompressionBlockSize()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            // Use a very small block size to force multiple compression blocks
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zlib,
                CompressionBlockSize = 64 // very small — forces many blocks
            }))
            {
                var builder = new Int64Array.Builder();
                for (int i = 0; i < 500; i++)
                    builder.Append(i);
                await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 500));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(500, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(500, col.Length);
                for (int i = 0; i < 500; i++)
                    Assert.Equal((long)i, col.GetValue(i));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_SmallCompressionBlockSize_Snappy()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("name", StringType.Default, nullable: false),
                new Field("value", DoubleType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Snappy,
                CompressionBlockSize = 128
            }))
            {
                var names = new StringArray.Builder();
                var values = new DoubleArray.Builder();
                for (int i = 0; i < 200; i++)
                {
                    names.Append($"item_{i}");
                    values.Append(i * 1.5);
                }
                await writer.WriteBatchAsync(new RecordBatch(schema, [names.Build(), values.Build()], 200));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(200, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var nameCol = (StringArray)batch.Column(0);
                var valueCol = (DoubleArray)batch.Column(1);
                for (int i = 0; i < 200; i++)
                {
                    Assert.Equal($"item_{i}", nameCol.GetString(i));
                    Assert.Equal(i * 1.5, valueCol.GetValue(i));
                }
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MapColumn_StringToInt()
    {
        var path = GetTempPath();
        try
        {
            var keyField = new Field("key", StringType.Default, nullable: false);
            var valueField = new Field("value", Int32Type.Default, nullable: true);
            var mapType = new MapType(keyField, valueField);
            var schema = new Schema([new Field("m", mapType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // 3 rows: {"a":1, "b":2}, {"c":3}, {"d":4, "e":5, "f":6}
                var keys = new StringArray.Builder()
                    .Append("a").Append("b").Append("c").Append("d").Append("e").Append("f").Build();
                var values = new Int32Array.Builder()
                    .AppendRange([1, 2, 3, 4, 5, 6]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3, 6]).Build();

                var structType = new StructType([keyField, valueField]);
                var structArr = new StructArray(structType, 6, [keys, values], ArrowBuffer.Empty, 0);
                var mapArr = new MapArray(mapType, 3, offsets, structArr, ArrowBuffer.Empty, 0);

                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (MapArray)batch.Column(0);
                Assert.Equal(3, col.Length);

                // Row 0: 2 entries
                Assert.Equal(0, col.ValueOffsets[0]);
                Assert.Equal(2, col.ValueOffsets[1]);
                // Row 1: 1 entry
                Assert.Equal(3, col.ValueOffsets[2]);
                // Row 2: 3 entries
                Assert.Equal(6, col.ValueOffsets[3]);

                var mapKeys = (StringArray)col.KeyValues.Fields[0];
                var mapVals = (Int32Array)col.KeyValues.Fields[1];
                Assert.Equal("a", mapKeys.GetString(0));
                Assert.Equal("b", mapKeys.GetString(1));
                Assert.Equal("c", mapKeys.GetString(2));
                Assert.Equal("d", mapKeys.GetString(3));
                Assert.Equal("e", mapKeys.GetString(4));
                Assert.Equal("f", mapKeys.GetString(5));
                Assert.Equal(1, mapVals.GetValue(0));
                Assert.Equal(2, mapVals.GetValue(1));
                Assert.Equal(3, mapVals.GetValue(2));
                Assert.Equal(4, mapVals.GetValue(3));
                Assert.Equal(5, mapVals.GetValue(4));
                Assert.Equal(6, mapVals.GetValue(5));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MapColumn_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var keyField = new Field("key", StringType.Default, nullable: false);
            var valueField = new Field("value", Int64Type.Default, nullable: true);
            var mapType = new MapType(keyField, valueField);
            var schema = new Schema([new Field("m", mapType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // 3 rows: {"x":10}, null, {"y":20, "z":30}
                var keys = new StringArray.Builder().Append("x").Append("y").Append("z").Build();
                var values = new Int64Array.Builder().AppendRange([10L, 20L, 30L]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 1, 1, 3]).Build();

                var structType = new StructType([keyField, valueField]);
                var structArr = new StructArray(structType, 3, [keys, values], ArrowBuffer.Empty, 0);

                // Validity: row 0 = valid, row 1 = null, row 2 = valid
                var validityBuilder = new ArrowBuffer.BitmapBuilder();
                validityBuilder.Append(true);
                validityBuilder.Append(false);
                validityBuilder.Append(true);
                var validity = validityBuilder.Build();

                var mapArr = new MapArray(mapType, 3, offsets, structArr, validity, 1);
                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (MapArray)batch.Column(0);
                Assert.Equal(3, col.Length);

                Assert.True(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.True(col.IsValid(2));

                // Row 0: 1 entry
                Assert.Equal(0, col.ValueOffsets[0]);
                Assert.Equal(1, col.ValueOffsets[1]);

                // Row 2: 2 entries
                var mapKeys = (StringArray)col.KeyValues.Fields[0];
                Assert.Equal("x", mapKeys.GetString(0));
                Assert.Equal("y", mapKeys.GetString(1));
                Assert.Equal("z", mapKeys.GetString(2));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MapColumn_EmptyMaps()
    {
        var path = GetTempPath();
        try
        {
            var keyField = new Field("key", Int32Type.Default, nullable: false);
            var valueField = new Field("value", Int32Type.Default, nullable: true);
            var mapType = new MapType(keyField, valueField);
            var schema = new Schema([new Field("m", mapType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
            }))
            {
                // 3 rows: {1:10}, {}, {2:20, 3:30}
                var keys = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                var values = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 1, 1, 3]).Build();

                var structType = new StructType([keyField, valueField]);
                var structArr = new StructArray(structType, 3, [keys, values], ArrowBuffer.Empty, 0);
                var mapArr = new MapArray(mapType, 3, offsets, structArr, ArrowBuffer.Empty, 0);

                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (MapArray)batch.Column(0);
                Assert.Equal(3, col.Length);

                // Row 0: 1 entry
                Assert.Equal(1, col.ValueOffsets[1] - col.ValueOffsets[0]);
                // Row 1: empty
                Assert.Equal(0, col.ValueOffsets[2] - col.ValueOffsets[1]);
                // Row 2: 2 entries
                Assert.Equal(2, col.ValueOffsets[3] - col.ValueOffsets[2]);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_MapColumn_WithCompression()
    {
        var path = GetTempPath();
        try
        {
            var keyField = new Field("key", StringType.Default, nullable: false);
            var valueField = new Field("value", Int64Type.Default, nullable: true);
            var mapType = new MapType(keyField, valueField);
            var schema = new Schema([new Field("m", mapType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zstd,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // Write 50 rows of maps
                var keyBuilder = new StringArray.Builder();
                var valBuilder = new Int64Array.Builder();
                var offsetBuilder = new ArrowBuffer.Builder<int>();
                offsetBuilder.Append(0);

                int totalEntries = 0;
                for (int row = 0; row < 50; row++)
                {
                    int numEntries = (row % 3) + 1; // 1, 2, or 3 entries per map
                    for (int j = 0; j < numEntries; j++)
                    {
                        keyBuilder.Append($"k{row}_{j}");
                        valBuilder.Append(row * 100L + j);
                        totalEntries++;
                    }
                    offsetBuilder.Append(totalEntries);
                }

                var structType = new StructType([keyField, valueField]);
                var structArr = new StructArray(structType, totalEntries,
                    [keyBuilder.Build(), valBuilder.Build()], ArrowBuffer.Empty, 0);
                var mapArr = new MapArray(mapType, 50, offsetBuilder.Build(), structArr, ArrowBuffer.Empty, 0);

                await writer.WriteBatchAsync(new RecordBatch(schema, [mapArr], 50));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(50, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (MapArray)batch.Column(0);
                Assert.Equal(50, col.Length);

                var mapKeys = (StringArray)col.KeyValues.Fields[0];
                var mapVals = (Int64Array)col.KeyValues.Fields[1];

                // Verify row 0: 1 entry ("k0_0" => 0)
                Assert.Equal(1, col.ValueOffsets[1] - col.ValueOffsets[0]);
                Assert.Equal("k0_0", mapKeys.GetString(col.ValueOffsets[0]));
                Assert.Equal(0L, mapVals.GetValue(col.ValueOffsets[0]));

                // Verify row 1: 2 entries
                Assert.Equal(2, col.ValueOffsets[2] - col.ValueOffsets[1]);

                // Verify row 2: 3 entries
                Assert.Equal(3, col.ValueOffsets[3] - col.ValueOffsets[2]);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RoundTrip_DateColumn_EdgeCases()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("d", Date32Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Date32Array.Builder()
                    .Append(new DateTime(1970, 1, 1))   // epoch = day 0
                    .Append(new DateTime(2020, 1, 1))    // 18262
                    .Append(new DateTime(1969, 12, 31))  // pre-epoch = -1
                    .Append(new DateTime(2000, 1, 1))    // 10957
                    .Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 4));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(4, reader.NumberOfRows);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Date32Array)batch.Column(0);
                Assert.Equal(4, col.Length);
                // Verify known epoch day values
                Assert.Equal(0, col.GetValue(0));      // 1970-01-01
                Assert.Equal(18262, col.GetValue(1));   // 2020-01-01
                Assert.Equal(-1, col.GetValue(2));      // 1969-12-31
                Assert.Equal(10957, col.GetValue(3));   // 2000-01-01
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_DateColumn_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("d", Date32Type.Default, nullable: true)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Date32Array.Builder()
                    .Append(new DateTime(1970, 4, 11))  // day 100
                    .AppendNull()
                    .Append(new DateTime(1970, 7, 19))  // day 200 (approx)
                    .AppendNull()
                    .Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 4));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Date32Array)batch.Column(0);
                Assert.True(col.IsValid(0));
                Assert.NotNull(col.GetValue(0));
                Assert.False(col.IsValid(1));
                Assert.True(col.IsValid(2));
                Assert.NotNull(col.GetValue(2));
                Assert.False(col.IsValid(3));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_FloatColumn_SpecialValues()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("f", FloatType.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new FloatArray.Builder()
                    .Append(float.NaN)
                    .Append(float.PositiveInfinity)
                    .Append(float.NegativeInfinity)
                    .Append(-0.0f)
                    .Append(float.MinValue)
                    .Append(float.MaxValue)
                    .Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 6));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (FloatArray)batch.Column(0);
                Assert.True(float.IsNaN(col.GetValue(0)!.Value));
                Assert.Equal(float.PositiveInfinity, col.GetValue(1));
                Assert.Equal(float.NegativeInfinity, col.GetValue(2));
                Assert.Equal(-0.0f, col.GetValue(3));
                Assert.Equal(float.MinValue, col.GetValue(4));
                Assert.Equal(float.MaxValue, col.GetValue(5));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_DoubleColumn_SpecialValues()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("d", DoubleType.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new DoubleArray.Builder()
                    .Append(double.NaN)
                    .Append(double.PositiveInfinity)
                    .Append(double.NegativeInfinity)
                    .Append(-0.0)
                    .Append(double.MinValue)
                    .Append(double.MaxValue)
                    .Append(double.Epsilon)
                    .Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 7));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (DoubleArray)batch.Column(0);
                Assert.True(double.IsNaN(col.GetValue(0)!.Value));
                Assert.Equal(double.PositiveInfinity, col.GetValue(1));
                Assert.Equal(double.NegativeInfinity, col.GetValue(2));
                Assert.Equal(-0.0, col.GetValue(3));
                Assert.Equal(double.MinValue, col.GetValue(4));
                Assert.Equal(double.MaxValue, col.GetValue(5));
                Assert.Equal(double.Epsilon, col.GetValue(6));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_TimestampColumn_PreEpoch()
    {
        var path = GetTempPath();
        try
        {
            var tsType = new TimestampType(TimeUnit.Nanosecond, TimeZoneInfo.Utc);
            var schema = new Schema([new Field("ts", tsType, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var builder = new TimestampArray.Builder(tsType);
                // 1969-12-31T23:59:59Z (1 second before epoch)
                builder.Append(new DateTimeOffset(1969, 12, 31, 23, 59, 59, TimeSpan.Zero));
                // 1960-06-15T10:30:00Z
                builder.Append(new DateTimeOffset(1960, 6, 15, 10, 30, 0, TimeSpan.Zero));
                // Epoch exactly
                builder.Append(new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero));
                await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (TimestampArray)batch.Column(0);
                // Pre-epoch: -1_000_000_000 nanoseconds
                Assert.Equal(-1_000_000_000L, col.GetValue(0));
                // 1960-06-15T10:30:00Z
                long expected1960 = new DateTimeOffset(1960, 6, 15, 10, 30, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1_000_000L;
                Assert.Equal(expected1960, col.GetValue(1));
                // Epoch = 0
                Assert.Equal(0L, col.GetValue(2));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_TimestampColumn_SubMillisecond()
    {
        var path = GetTempPath();
        try
        {
            var tsType = new TimestampType(TimeUnit.Nanosecond, TimeZoneInfo.Utc);
            var schema = new Schema([new Field("ts", tsType, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var builder = new TimestampArray.Builder(tsType);
                // 2024-01-15T12:30:00.123456789Z — full nanosecond precision
                // epoch nanos = seconds * 1e9 + nanos
                long epochNanos = new DateTimeOffset(2024, 1, 15, 12, 30, 0, TimeSpan.Zero).ToUnixTimeSeconds() * 1_000_000_000L + 123_456_789L;
                // Build manually using the nanos value
                var arr = new TimestampArray.Builder(tsType);
                // Use DateTimeOffset with tick precision (100ns)
#if NET6_0_OR_GREATER
                var dt = new DateTimeOffset(2024, 1, 15, 12, 30, 0, 123, 456, TimeSpan.Zero).AddTicks(7); // +700ns ≈ 789
#else
                var dt = new DateTimeOffset(2024, 1, 15, 12, 30, 0, 123, TimeSpan.Zero).AddTicks(4560 + 7); // microseconds as ticks + 700ns
#endif
                arr.Append(dt);
                // Pure millisecond value
                arr.Append(new DateTimeOffset(2024, 1, 15, 12, 30, 0, 500, TimeSpan.Zero));
                // Pure microsecond value
#if NET6_0_OR_GREATER
                arr.Append(new DateTimeOffset(2024, 1, 15, 12, 30, 0, 0, 250, TimeSpan.Zero));
#else
                arr.Append(new DateTimeOffset(2024, 1, 15, 12, 30, 0, 0, TimeSpan.Zero).AddTicks(2500));
#endif
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr.Build()], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (TimestampArray)batch.Column(0);
                Assert.Equal(3, col.Length);
                // Just verify they round-trip (precision may vary by platform)
                Assert.NotNull(col.GetValue(0));
                // 500ms = 500_000_000 nanoseconds offset from whole second
                long val1 = col.GetValue(1)!.Value;
                long expectedMs = new DateTimeOffset(2024, 1, 15, 12, 30, 0, 500, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1_000_000L;
                Assert.Equal(expectedMs, val1);
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_DecimalColumn_HighPrecision()
    {
        var path = GetTempPath();
        try
        {
            // precision=18, scale=6 — fits in long
            var decType = new Decimal128Type(18, 6);
            var schema = new Schema([new Field("amount", decType, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // Value: 123456.789012 stored as 123456789012 with scale=6
                var bytes = new byte[3 * 16];
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0), 123456789012L);
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8), 0L); // sign extension
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(16), -999999L); // negative
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(24), -1L); // sign extension
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(32), 0L); // zero
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(40), 0L); // sign extension
                var valueBuf = new ArrowBuffer(bytes);
                var data = new ArrayData(decType, 3, 0, 0, [ArrowBuffer.Empty, valueBuf]);
                var arr = new Decimal128Array(data);
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Decimal128Array)batch.Column(0);
                Assert.Equal(3, col.Length);
                var v0 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(0, 8));
                Assert.Equal(123456789012L, v0);
                var v1 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(16, 8));
                Assert.Equal(-999999L, v1);
                var v2 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(32, 8));
                Assert.Equal(0L, v2);
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_DecimalColumn_WithNulls_Negative()
    {
        var path = GetTempPath();
        try
        {
            var decType = new Decimal128Type(10, 2);
            var schema = new Schema([new Field("amount", decType, nullable: true)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 3 rows: 12.34, null, -56.78
                var bytes = new byte[3 * 16];
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(0), 1234L);
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8), 0L);
                // row 1 is null, value doesn't matter
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(16), 0L);
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(24), 0L);
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(32), -5678L);
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(40), -1L);
                var valueBuf = new ArrowBuffer(bytes);
                var validityBuf = new ArrowBuffer.BitmapBuilder();
                validityBuf.Append(true);
                validityBuf.Append(false);
                validityBuf.Append(true);
                var data = new ArrayData(decType, 3, 1, 0, [validityBuf.Build(), valueBuf]);
                var arr = new Decimal128Array(data);
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Decimal128Array)batch.Column(0);
                Assert.True(col.IsValid(0));
                Assert.False(col.IsValid(1));
                Assert.True(col.IsValid(2));
                var v0 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(0, 8));
                Assert.Equal(1234L, v0);
                var v2 = BinaryPrimitives.ReadInt64LittleEndian(col.ValueBuffer.Span.Slice(32, 8));
                Assert.Equal(-5678L, v2);
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task ReadOptions_ColumnSelection_NonContiguous()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("a", Int32Type.Default, nullable: false),
                new Field("b", StringType.Default, nullable: false),
                new Field("c", DoubleType.Default, nullable: false),
                new Field("d", BooleanType.Default, nullable: false),
            ], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var a = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                var b = new StringArray.Builder().Append("x").Append("y").Append("z").Build();
                var c = new DoubleArray.Builder().AppendRange([1.1, 2.2, 3.3]).Build();
                var d = new BooleanArray.Builder().Append(true).Append(false).Append(true).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [a, b, c, d], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            // Select only columns "a" and "c" (skipping "b" and "d")
            var rowReader = reader.CreateRowReader(new OrcReaderOptions { Columns = ["a", "c"] });
            Assert.Equal(2, rowReader.ArrowSchema.FieldsList.Count);
            Assert.Equal("a", rowReader.ArrowSchema.FieldsList[0].Name);
            Assert.Equal("c", rowReader.ArrowSchema.FieldsList[1].Name);

            await foreach (var batch in rowReader)
            {
                Assert.Equal(2, batch.ColumnCount);
                var colA = (Int32Array)batch.Column(0);
                var colC = (DoubleArray)batch.Column(1);
                Assert.Equal(1, colA.GetValue(0));
                Assert.Equal(2, colA.GetValue(1));
                Assert.Equal(3, colA.GetValue(2));
                Assert.Equal(1.1, colC.GetValue(0));
                Assert.Equal(2.2, colC.GetValue(1));
                Assert.Equal(3.3, colC.GetValue(2));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task ReadOptions_BatchSize_One()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([10L, 20L, 30L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 1 });
            var batches = new List<RecordBatch>();
            await foreach (var batch in rowReader)
                batches.Add(batch);

            Assert.Equal(3, batches.Count);
            Assert.Equal(10L, ((Int64Array)batches[0].Column(0)).GetValue(0));
            Assert.Equal(20L, ((Int64Array)batches[1].Column(0)).GetValue(0));
            Assert.Equal(30L, ((Int64Array)batches[2].Column(0)).GetValue(0));
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task ReadOptions_BatchSize_LargerThanRows()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 10000 });
            var batches = new List<RecordBatch>();
            await foreach (var batch in rowReader)
                batches.Add(batch);

            Assert.Single(batches);
            Assert.Equal(3, batches[0].Length);
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task Writer_ThrowsOnWriteAfterDispose()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None });
            writer.Dispose();

            var arr = new Int64Array.Builder().Append(1).Build();
            await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 1)));
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task Writer_DisposeFlushesProperly()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            // No explicit Close — just Dispose via `using`
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }
            // File should be readable after dispose
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(3, reader.NumberOfRows);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                Assert.Equal(1L, col.GetValue(0));
                Assert.Equal(3L, col.GetValue(2));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_StructOfStruct()
    {
        var path = GetTempPath();
        try
        {
            var innerStructType = new StructType([
                new Field("x", Int32Type.Default, nullable: false),
                new Field("y", Int32Type.Default, nullable: false),
            ]);
            var outerStructType = new StructType([
                new Field("name", StringType.Default, nullable: false),
                new Field("point", innerStructType, nullable: false),
            ]);
            var schema = new Schema([new Field("s", outerStructType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var names = new StringArray.Builder().Append("a").Append("b").Build();
                var xs = new Int32Array.Builder().AppendRange([10, 20]).Build();
                var ys = new Int32Array.Builder().AppendRange([30, 40]).Build();
                var innerStruct = new StructArray(innerStructType, 2, [xs, ys], ArrowBuffer.Empty, 0);
                var outerStruct = new StructArray(outerStructType, 2, [names, innerStruct], ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [outerStruct], 2));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(2, reader.NumberOfRows);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var outer = (StructArray)batch.Column(0);
                var nameCol = (StringArray)outer.Fields[0];
                Assert.Equal("a", nameCol.GetString(0));
                Assert.Equal("b", nameCol.GetString(1));
                var inner = (StructArray)outer.Fields[1];
                var xCol = (Int32Array)inner.Fields[0];
                var yCol = (Int32Array)inner.Fields[1];
                Assert.Equal(10, xCol.GetValue(0));
                Assert.Equal(20, xCol.GetValue(1));
                Assert.Equal(30, yCol.GetValue(0));
                Assert.Equal(40, yCol.GetValue(1));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_ListOfLists()
    {
        var path = GetTempPath();
        try
        {
            var innerListType = new ListType(new Field("item", Int32Type.Default, nullable: false));
            var outerListType = new ListType(new Field("inner", innerListType, nullable: false));
            var schema = new Schema([new Field("nested", outerListType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 2 outer rows: [[1,2],[3]], [[4,5,6]]
                // Inner lists: [1,2], [3], [4,5,6]
                var elements = new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5, 6]).Build();
                var innerOffsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3, 6]).Build();
                var innerList = new ListArray(innerListType, 3, innerOffsets, elements, ArrowBuffer.Empty, 0);

                var outerOffsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 3]).Build();
                var outerList = new ListArray(outerListType, 2, outerOffsets, innerList, ArrowBuffer.Empty, 0);

                await writer.WriteBatchAsync(new RecordBatch(schema, [outerList], 2));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(2, reader.NumberOfRows);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var outer = (ListArray)batch.Column(0);
                Assert.Equal(2, outer.Length);

                // Row 0: 2 inner lists
                Assert.Equal(2, outer.ValueOffsets[1] - outer.ValueOffsets[0]);
                // Row 1: 1 inner list
                Assert.Equal(1, outer.ValueOffsets[2] - outer.ValueOffsets[1]);

                var inner = (ListArray)outer.Values;
                var values = (Int32Array)inner.Values;

                // Inner[0] = [1,2]
                Assert.Equal(2, inner.ValueOffsets[1] - inner.ValueOffsets[0]);
                Assert.Equal(1, values.GetValue(inner.ValueOffsets[0]));
                Assert.Equal(2, values.GetValue(inner.ValueOffsets[0] + 1));

                // Inner[1] = [3]
                Assert.Equal(1, inner.ValueOffsets[2] - inner.ValueOffsets[1]);
                Assert.Equal(3, values.GetValue(inner.ValueOffsets[1]));

                // Inner[2] = [4,5,6]
                Assert.Equal(3, inner.ValueOffsets[3] - inner.ValueOffsets[2]);
                Assert.Equal(4, values.GetValue(inner.ValueOffsets[2]));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_StructColumn_NullableFields()
    {
        var path = GetTempPath();
        try
        {
            var structType = new StructType([
                new Field("name", StringType.Default, nullable: true),
                new Field("value", Int32Type.Default, nullable: true),
            ]);
            var schema = new Schema([new Field("s", structType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var names = new StringArray.Builder().Append("hello").AppendNull().Append("world").Build();
                var values = new Int32Array.Builder().AppendNull().Append(42).Append(99).Build();
                var structArr = new StructArray(structType, 3, [names, values], ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [structArr], 3));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var s = (StructArray)batch.Column(0);
                var nameCol = (StringArray)s.Fields[0];
                var valCol = (Int32Array)s.Fields[1];

                Assert.Equal("hello", nameCol.GetString(0));
                Assert.True(nameCol.IsNull(1));
                Assert.Equal("world", nameCol.GetString(2));

                Assert.True(valCol.IsNull(0));
                Assert.Equal(42, valCol.GetValue(1));
                Assert.Equal(99, valCol.GetValue(2));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_MultipleStripes_SmallStripeSize()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                StripeSize = 100 // Very small — force multiple stripes
            }))
            {
                // Write 500 values in 5 batches of 100
                for (int batch = 0; batch < 5; batch++)
                {
                    var builder = new Int64Array.Builder();
                    for (int i = 0; i < 100; i++)
                        builder.Append(batch * 100 + i);
                    await writer.WriteBatchAsync(new RecordBatch(schema, [builder.Build()], 100));
                }
            }
            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(500, reader.NumberOfRows);
            Assert.True(reader.NumberOfStripes > 1, $"Expected multiple stripes, got {reader.NumberOfStripes}");

            // Read all data back with small batch size to exercise stripe transitions
            var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 75 });
            var allValues = new List<long>();
            await foreach (var batch in rowReader)
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    allValues.Add(col.GetValue(i)!.Value);
            }
            Assert.Equal(500, allValues.Count);
            for (int i = 0; i < 500; i++)
                Assert.Equal(i, allValues[i]);
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public void RleV2_ExtremeValues_RoundTrip()
    {
        var original = new long[] { long.MinValue, long.MaxValue, 0, -1, 1, long.MinValue + 1, long.MaxValue - 1 };
        var buf = new GrowableBuffer();
        var encoder = new EngineeredWood.Orc.Encodings.RleEncoderV2(buf, signed: true);
        encoder.WriteValues(original);
        encoder.Flush();

        var bytes = buf.WrittenSpan.ToArray();
        var decoder = new EngineeredWood.Orc.Encodings.RleDecoderV2(new EngineeredWood.Orc.Encodings.OrcByteStream(bytes, 0, bytes.Length), signed: true);
        var decoded = new long[original.Length];
        decoder.ReadValues(decoded);

        for (int i = 0; i < original.Length; i++)
            Assert.Equal(original[i], decoded[i]);
    }

    [Fact]
    public void RleV2_FlushWithoutData_NoOutput()
    {
        var buf = new GrowableBuffer();
        var encoder = new EngineeredWood.Orc.Encodings.RleEncoderV2(buf, signed: true);
        encoder.Flush();
        encoder.Flush(); // double flush
        Assert.Equal(0, buf.Length);
    }

    [Fact]
    public async Task RoundTrip_StringColumn_Unicode()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("s", StringType.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                var arr = new StringArray.Builder()
                    .Append("hello")
                    .Append("")  // empty string
                    .Append("\u00e9\u00e8\u00ea") // accented chars
                    .Append("\ud83d\ude00") // emoji 😀
                    .Append("\u4e16\u754c") // Chinese "世界"
                    .Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 5));
            }
            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (StringArray)batch.Column(0);
                Assert.Equal("hello", col.GetString(0));
                Assert.Equal("", col.GetString(1));
                Assert.Equal("\u00e9\u00e8\u00ea", col.GetString(2));
                Assert.Equal("\ud83d\ude00", col.GetString(3));
                Assert.Equal("\u4e16\u754c", col.GetString(4));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RowIndex_PositionsIncreaseAcrossRowGroups()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
            ], null);

            // Write 30,000 rows with stride 10,000 → 3 row groups
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                RowIndexStride = 10_000,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                for (int b = 0; b < 30; b++)
                {
                    var ids = new Int64Array.Builder()
                        .AppendRange(Enumerable.Range(b * 1000, 1000).Select(i => (long)i)).Build();
                    var names = new StringArray.Builder();
                    for (int i = 0; i < 1000; i++) names.Append($"item_{b * 1000 + i}");
                    await writer.WriteBatchAsync(new RecordBatch(schema, [ids, names.Build()], 1000));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var indexes = await reader.ReadRowIndexAsync(0);

            // Verify positions are strictly increasing for both columns
            foreach (int colId in new[] { 1, 2 }) // id=1, name=2
            {
                var idx = indexes[colId];
                Assert.Equal(3, idx.Entry.Count);
                // Each entry's first position (byte offset) should increase
                for (int e = 1; e < idx.Entry.Count; e++)
                {
                    Assert.True(idx.Entry[e].Positions[0] > idx.Entry[e - 1].Positions[0],
                        $"Column {colId}: row group {e} position should be > row group {e - 1}");
                }
            }

            // Verify each row group entry has statistics
            var idIdx = indexes[1];
            for (int e = 0; e < idIdx.Entry.Count; e++)
            {
                Assert.NotNull(idIdx.Entry[e].Statistics);
                Assert.True(idIdx.Entry[e].Statistics.NumberOfValues > 0);
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_ListColumn_WithNulls()
    {
        var path = GetTempPath();
        try
        {
            var listType = new ListType(new Field("item", Int32Type.Default, nullable: false));
            var schema = new Schema([new Field("nums", listType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 4 rows: [1,2], null, [3], null
                var elements = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 2, 2, 3, 3]).Build();
                var validity = new ArrowBuffer.BitmapBuilder();
                validity.Append(true);
                validity.Append(false);
                validity.Append(true);
                validity.Append(false);

                var listArr = new ListArray(listType, 4, offsets, elements, validity.Build(), 2);
                await writer.WriteBatchAsync(new RecordBatch(schema, [listArr], 4));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(4, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (ListArray)batch.Column(0);
                Assert.Equal(4, col.Length);

                // Row 0: [1, 2]
                Assert.True(col.IsValid(0));
                Assert.Equal(2, col.ValueOffsets[1] - col.ValueOffsets[0]);

                // Row 1: null
                Assert.False(col.IsValid(1));

                // Row 2: [3]
                Assert.True(col.IsValid(2));
                Assert.Equal(1, col.ValueOffsets[3] - col.ValueOffsets[2]);

                // Row 3: null
                Assert.False(col.IsValid(3));

                var values = (Int32Array)col.Values;
                Assert.Equal(1, values.GetValue(col.ValueOffsets[0]));
                Assert.Equal(2, values.GetValue(col.ValueOffsets[0] + 1));
                Assert.Equal(3, values.GetValue(col.ValueOffsets[2]));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_ListColumn_NullableElements()
    {
        var path = GetTempPath();
        try
        {
            var listType = new ListType(new Field("item", Int32Type.Default, nullable: true));
            var schema = new Schema([new Field("nums", listType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 2 rows: [1, null, 3], [null, 5]
                var elements = new Int32Array.Builder()
                    .Append(1).AppendNull().Append(3).AppendNull().Append(5).Build();
                var offsets = new ArrowBuffer.Builder<int>().AppendRange([0, 3, 5]).Build();
                var listArr = new ListArray(listType, 2, offsets, elements, ArrowBuffer.Empty, 0);
                await writer.WriteBatchAsync(new RecordBatch(schema, [listArr], 2));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var col = (ListArray)batch.Column(0);
                Assert.Equal(2, col.Length);

                var values = (Int32Array)col.Values;
                // Row 0: [1, null, 3]
                Assert.Equal(3, col.ValueOffsets[1] - col.ValueOffsets[0]);
                Assert.Equal(1, values.GetValue(col.ValueOffsets[0]));
                Assert.True(values.IsNull(col.ValueOffsets[0] + 1));
                Assert.Equal(3, values.GetValue(col.ValueOffsets[0] + 2));

                // Row 1: [null, 5]
                Assert.Equal(2, col.ValueOffsets[2] - col.ValueOffsets[1]);
                Assert.True(values.IsNull(col.ValueOffsets[1]));
                Assert.Equal(5, values.GetValue(col.ValueOffsets[1] + 1));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_StructColumn_NullStructRows()
    {
        var path = GetTempPath();
        try
        {
            var structType = new StructType([
                new Field("x", Int32Type.Default, nullable: false),
                new Field("y", Int32Type.Default, nullable: false),
            ]);
            var schema = new Schema([new Field("s", structType, nullable: true)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                // 4 rows: {1,2}, null, {3,4}, null
                var xs = new Int32Array.Builder().AppendRange([1, 0, 3, 0]).Build();
                var ys = new Int32Array.Builder().AppendRange([2, 0, 4, 0]).Build();

                var validity = new ArrowBuffer.BitmapBuilder();
                validity.Append(true);
                validity.Append(false);
                validity.Append(true);
                validity.Append(false);

                var structArr = new StructArray(structType, 4, [xs, ys], validity.Build(), 2);
                await writer.WriteBatchAsync(new RecordBatch(schema, [structArr], 4));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(4, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                var s = (StructArray)batch.Column(0);
                Assert.Equal(4, s.Length);

                Assert.True(s.IsValid(0));
                Assert.False(s.IsValid(1));
                Assert.True(s.IsValid(2));
                Assert.False(s.IsValid(3));

                var xCol = (Int32Array)s.Fields[0];
                var yCol = (Int32Array)s.Fields[1];
                Assert.Equal(1, xCol.GetValue(0));
                Assert.Equal(2, yCol.GetValue(0));
                Assert.Equal(3, xCol.GetValue(2));
                Assert.Equal(4, yCol.GetValue(2));
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_MultiStripe_WithColumnSelection()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([
                new Field("a", Int64Type.Default, nullable: false),
                new Field("b", StringType.Default, nullable: false),
                new Field("c", DoubleType.Default, nullable: false),
            ], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                StripeSize = 200, // force multiple stripes
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                for (int batch = 0; batch < 10; batch++)
                {
                    var aArr = new Int64Array.Builder();
                    var bArr = new StringArray.Builder();
                    var cArr = new DoubleArray.Builder();
                    for (int i = 0; i < 100; i++)
                    {
                        int row = batch * 100 + i;
                        aArr.Append(row);
                        bArr.Append($"s{row}");
                        cArr.Append(row * 0.5);
                    }
                    await writer.WriteBatchAsync(new RecordBatch(schema, [aArr.Build(), bArr.Build(), cArr.Build()], 100));
                }
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(1000, reader.NumberOfRows);
            Assert.True(reader.NumberOfStripes > 1, $"Expected multiple stripes, got {reader.NumberOfStripes}");

            // Read only columns "a" and "c" across all stripes
            var rowReader = reader.CreateRowReader(new OrcReaderOptions { Columns = ["a", "c"] });
            Assert.Equal(2, rowReader.ArrowSchema.FieldsList.Count);

            var allA = new List<long>();
            var allC = new List<double>();
            await foreach (var batch in rowReader)
            {
                var aCol = (Int64Array)batch.Column(0);
                var cCol = (DoubleArray)batch.Column(1);
                for (int i = 0; i < batch.Length; i++)
                {
                    allA.Add(aCol.GetValue(i)!.Value);
                    allC.Add(cCol.GetValue(i)!.Value);
                }
            }

            Assert.Equal(1000, allA.Count);
            for (int i = 0; i < 1000; i++)
            {
                Assert.Equal(i, allA[i]);
                Assert.Equal(i * 0.5, allC[i]);
            }
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public void RleV2_UnsignedExtremeValues_RoundTrip()
    {
        // Test unsigned mode with large values that would be negative if signed
        var original = new long[]
        {
            0, 1, (long)(ulong.MaxValue >> 1), // long.MaxValue
            100, 255, 65535, 16777215,
        };

        var buf = new GrowableBuffer();
        var encoder = new EngineeredWood.Orc.Encodings.RleEncoderV2(buf, signed: false);
        encoder.WriteValues(original);
        encoder.Flush();

        var bytes = buf.WrittenSpan.ToArray();
        var decoder = new EngineeredWood.Orc.Encodings.RleDecoderV2(new EngineeredWood.Orc.Encodings.OrcByteStream(bytes, 0, bytes.Length), signed: false);
        var decoded = new long[original.Length];
        decoder.ReadValues(decoded);

        for (int i = 0; i < original.Length; i++)
            Assert.Equal(original[i], decoded[i]);
    }

    [Fact]
    public async Task ReadOptions_InvalidColumnName_Throws()
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions { Compression = CompressionKind.None }))
            {
                var arr = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], 3));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            // Request a column that doesn't exist — should throw
            Assert.Throws<ArgumentException>(() =>
                reader.CreateRowReader(new OrcReaderOptions { Columns = ["nonexistent"] }));
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task RoundTrip_SparseUnionColumn()
    {
        var path = GetTempPath();
        try
        {
            var unionType = new UnionType(
                [
                    new Field("i", Int32Type.Default, nullable: true),
                    new Field("s", StringType.Default, nullable: true),
                ],
                [0, 1],
                UnionMode.Sparse);
            var schema = new Schema([new Field("u", unionType, nullable: false)], null);

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.None,
                DefaultStringEncoding = EncodingFamily.V2
            }))
            {
                // 5 rows: int(10), str("hello"), int(20), str("world"), int(30)
                var typeIds = new byte[] { 0, 1, 0, 1, 0 };

                // Sparse: each child is full-length with nulls for non-selected rows
                var intChild = new Int32Array.Builder()
                    .Append(10).AppendNull().Append(20).AppendNull().Append(30).Build();
                var strChild = new StringArray.Builder()
                    .AppendNull().Append("hello").AppendNull().Append("world").AppendNull().Build();

                var typeIdBuf = new ArrowBuffer(typeIds);
                var unionArr = new SparseUnionArray(unionType, 5, [intChild, strChild], typeIdBuf);
                await writer.WriteBatchAsync(new RecordBatch(schema, [unionArr], 5));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(5, reader.NumberOfRows);

            var rowReader = reader.CreateRowReader();
            await foreach (var batch in rowReader)
            {
                // ORC reader produces DenseUnionArray
                var col = (DenseUnionArray)batch.Column(0);
                Assert.Equal(5, col.Length);

                // Verify type tags
                Assert.Equal(0, col.TypeIds[0]);
                Assert.Equal(1, col.TypeIds[1]);
                Assert.Equal(0, col.TypeIds[2]);
                Assert.Equal(1, col.TypeIds[3]);
                Assert.Equal(0, col.TypeIds[4]);

                // Verify int values (child 0): [10, 20, 30]
                var intChild = (Int32Array)col.Fields[0];
                Assert.Equal(3, intChild.Length);
                Assert.Equal(10, intChild.GetValue(0));
                Assert.Equal(20, intChild.GetValue(1));
                Assert.Equal(30, intChild.GetValue(2));

                // Verify string values (child 1): ["hello", "world"]
                var strChild = (StringArray)col.Fields[1];
                Assert.Equal(2, strChild.Length);
                Assert.Equal("hello", strChild.GetString(0));
                Assert.Equal("world", strChild.GetString(1));
            }
        }
        finally { File.Delete(path); }
    }
}
