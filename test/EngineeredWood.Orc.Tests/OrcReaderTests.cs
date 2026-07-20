// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc.Tests;

public class OrcReaderTests
{
    [Fact]
    public async Task CanOpenAndReadMetadata_Test1()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.test1.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        Assert.True(reader.NumberOfRows > 0);
        Assert.True(reader.NumberOfStripes > 0);
        Assert.NotNull(reader.Schema);
    }

    [Fact]
    public async Task CanOpenAndReadMetadata_Demo12Zlib()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        Assert.True(reader.NumberOfRows > 0);
        Assert.Equal(EngineeredWood.Orc.Proto.CompressionKind.Zlib, reader.Compression);
    }

    [Fact]
    public async Task CanOpenEmptyFile()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.emptyFile.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        Assert.Equal(0, reader.NumberOfRows);
    }

    [Fact]
    public async Task CanOpenAndReadMetadata_Demo11None()
    {
        var path = TestHelpers.GetTestFilePath("demo-11-none.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        Assert.True(reader.NumberOfRows > 0);
        Assert.Equal(EngineeredWood.Orc.Proto.CompressionKind.None, reader.Compression);
    }

    [Fact]
    public async Task SchemaConvertsToArrow()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var arrowSchema = reader.Schema.ToArrowSchema();
        Assert.True(arrowSchema.FieldsList.Count > 0);
    }

    [Fact]
    public async Task CanReadAllBatches_Demo11None()
    {
        var path = TestHelpers.GetTestFilePath("demo-11-none.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 1024 });
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            Assert.True(batch.Length > 0);
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadAllBatches_Demo12Zlib()
    {
        var path = TestHelpers.GetTestFilePath("demo-12-zlib.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader(new OrcReaderOptions { BatchSize = 1024 });
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            Assert.True(batch.Length > 0);
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadTest1File()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.test1.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader();
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadSnappyFile()
    {
        var path = TestHelpers.GetTestFilePath("TestOrcFile.testSnappy.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        Assert.Equal(EngineeredWood.Orc.Proto.CompressionKind.Snappy, reader.Compression);

        var rowReader = reader.CreateRowReader();
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadNullsAtEndSnappy()
    {
        var path = TestHelpers.GetTestFilePath("nulls-at-end-snappy.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        Assert.Equal(EngineeredWood.Orc.Proto.CompressionKind.Snappy, reader.Compression);

        var rowReader = reader.CreateRowReader();
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadDecimalFile()
    {
        var path = TestHelpers.GetTestFilePath("decimal.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader();
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadOrc11FormatFile()
    {
        var path = TestHelpers.GetTestFilePath("orc-file-11-format.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader();
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }

    [Fact]
    public async Task CanReadOver1kBloomFile()
    {
        var path = TestHelpers.GetTestFilePath("over1k_bloom.orc");
        await using var reader = await OrcReader.OpenAsync(path);

        var rowReader = reader.CreateRowReader();
        long totalRows = 0;

        await foreach (var batch in rowReader)
        {
            totalRows += batch.Length;
        }

        Assert.Equal(reader.NumberOfRows, totalRows);
    }
}
