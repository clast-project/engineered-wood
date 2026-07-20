// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.Orc.Proto;

namespace EngineeredWood.Orc.Tests;

public class CompressionLevelTests
{
    private static string GetTempPath() => Path.Combine(Path.GetTempPath(), $"storc_lvl_{Guid.NewGuid():N}.orc");

    public static IEnumerable<object[]> CodecsAndLevels()
    {
        var codecs = new[] { CompressionKind.Zstd, CompressionKind.Zlib, CompressionKind.Lz4 };
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
    public async Task RoundTrip_AtLevel_PreservesData(CompressionKind kind, BlockCompressionLevel level)
    {
        var path = GetTempPath();
        try
        {
            var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

            // Use a redundant pattern so compression actually does work and exercises the level.
            var values = new long[20_000];
            for (int i = 0; i < values.Length; i++)
                values[i] = i % 17;

            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = kind,
                CompressionLevel = level,
            }))
            {
                var arr = new Int64Array.Builder().AppendRange(values).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], values.Length));
            }

            await using var reader = await OrcReader.OpenAsync(path);
            Assert.Equal(values.Length, reader.NumberOfRows);

            int row = 0;
            await foreach (var batch in reader.CreateRowReader())
            {
                var col = (Int64Array)batch.Column(0);
                for (int i = 0; i < col.Length; i++)
                    Assert.Equal(values[row++], col.GetValue(i));
            }
            Assert.Equal(values.Length, row);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task SmallestSize_ProducesNoLargerFileThan_Fastest_Zstd()
    {
        var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
        var values = new long[50_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = i % 31;

        long fastestSize = await WriteAndMeasure(schema, values, BlockCompressionLevel.Fastest);
        long smallestSize = await WriteAndMeasure(schema, values, BlockCompressionLevel.SmallestSize);

        Assert.True(smallestSize <= fastestSize,
            $"Zstd SmallestSize ({smallestSize}) > Fastest ({fastestSize})");
    }

    private static async Task<long> WriteAndMeasure(Schema schema, long[] values, BlockCompressionLevel level)
    {
        var path = GetTempPath();
        try
        {
            await using (var writer = OrcWriter.Create(path, schema, new OrcWriterOptions
            {
                Compression = CompressionKind.Zstd,
                CompressionLevel = level,
            }))
            {
                var arr = new Int64Array.Builder().AppendRange(values).Build();
                await writer.WriteBatchAsync(new RecordBatch(schema, [arr], values.Length));
            }
            return new FileInfo(path).Length;
        }
        finally
        {
            File.Delete(path);
        }
    }
}
