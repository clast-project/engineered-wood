// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compression;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet;

public class ParquetFileReaderTests
{
    [Fact]
    public async Task ReadMetadata_AlltypesPlain()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();

        Assert.Equal(1, metadata.Version);
        Assert.Equal(8, metadata.NumRows);
        Assert.Single(metadata.RowGroups);
        Assert.True(metadata.Schema.Count > 1);
    }

    [Fact]
    public async Task ReadMetadata_IsCached()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var first = await reader.ReadMetadataAsync();
        var second = await reader.ReadMetadataAsync();

        Assert.Same(first, second);
    }

    [Fact]
    public async Task GetSchema_BuildsFromMetadata()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var schema = await reader.GetSchemaAsync();

        Assert.NotNull(schema);
        Assert.Equal("schema", schema.Root.Name);
        Assert.True(schema.Columns.Count > 0);

        var names = schema.Columns.Select(c => c.DottedPath).ToList();
        Assert.Contains("id", names);
        Assert.Contains("bool_col", names);
    }

    [Fact]
    public async Task GetSchema_IsCached()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var first = await reader.GetSchemaAsync();
        var second = await reader.GetSchemaAsync();

        Assert.Same(first, second);
    }

    [Fact]
    public async Task ReadMetadata_TruncatedFile_Throws()
    {
        // Create a file that's too small to be valid
        var tempPath = Path.GetTempFileName();
        try
        {
            await WriteAllBytesAsync(tempPath, new byte[] { 0x01, 0x02, 0x03 });
            await using var file = new LocalRandomAccessFile(tempPath);
            await using var reader = new ParquetFileReader(file, ownsFile: false);

            await Assert.ThrowsAsync<ParquetFormatException>(
                () => reader.ReadMetadataAsync().AsTask());
        }
        finally
        {
            File.Delete(tempPath);
        }
    }

    [Fact]
    public async Task ReadMetadata_BadMagic_Throws()
    {
        // Create a file with sufficient length but wrong magic
        var tempPath = Path.GetTempFileName();
        try
        {
            var data = new byte[64];
            // Write something that looks like a footer length but with wrong magic
            data[^8] = 10; // footer length = 10
            data[^4] = (byte)'X'; // wrong magic
            data[^3] = (byte)'X';
            data[^2] = (byte)'X';
            data[^1] = (byte)'X';
            await WriteAllBytesAsync(tempPath, data);

            await using var file = new LocalRandomAccessFile(tempPath);
            await using var reader = new ParquetFileReader(file, ownsFile: false);

            await Assert.ThrowsAsync<ParquetFormatException>(
                () => reader.ReadMetadataAsync().AsTask());
        }
        finally
        {
            File.Delete(tempPath);
        }
    }

    [Fact]
    public async Task ReadMetadata_NestedListsFile()
    {
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("nested_lists.snappy.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.True(metadata.NumRows > 0);

        var schema = await reader.GetSchemaAsync();
        var hasRepLevel = schema.Columns.Any(c => c.MaxRepetitionLevel > 0);
        Assert.True(hasRepLevel);
    }

    [Fact]
    public async Task ReadMetadata_SnappyCompressedFile()
    {
        await using var file = new LocalRandomAccessFile(
            TestData.GetPath("alltypes_plain.snappy.parquet"));
        await using var reader = new ParquetFileReader(file, ownsFile: false);

        var metadata = await reader.ReadMetadataAsync();
        Assert.True(metadata.NumRows > 0);
        Assert.All(metadata.RowGroups[0].Columns,
            c => Assert.Equal(CompressionCodec.Snappy, c.MetaData!.Codec));
    }

    [Fact]
    public async Task AllTestFiles_ParseThroughFullPipeline()
    {
        var files = TestData.GetAllParquetFiles().ToList();
        Assert.NotEmpty(files);

        var failures = new List<string>();
        foreach (var filePath in files)
        {
            var fileName = Path.GetFileName(filePath);
            if (fileName.Contains("encrypted", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                await using var file = new LocalRandomAccessFile(filePath);
                await using var reader = new ParquetFileReader(file, ownsFile: false);

                var metadata = await reader.ReadMetadataAsync();
                var schema = await reader.GetSchemaAsync();

                // Basic sanity: schema should have columns if there are row groups with column chunks
                if (metadata.RowGroups.Count > 0 && metadata.RowGroups[0].Columns.Count > 0)
                    Assert.True(schema.Columns.Count > 0, $"{fileName}: no leaf columns found");
            }
            catch (Exception ex)
            {
                failures.Add($"{fileName}: {ex.Message}");
            }
        }

        Assert.True(failures.Count == 0,
            $"Failed to parse {failures.Count} file(s):\n" + string.Join("\n", failures));
    }

    [Fact]
    public async Task Dispose_OwnsFile_DisposesUnderlying()
    {
        var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        var reader = new ParquetFileReader(file, ownsFile: true);

        await reader.DisposeAsync();

        // Attempting to use the file after the reader disposed it should fail
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => file.GetLengthAsync().AsTask());
    }

    [Fact]
    public async Task Dispose_DoesNotOwnFile_LeavesOpen()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("alltypes_plain.parquet"));
        var reader = new ParquetFileReader(file, ownsFile: false);

        await reader.DisposeAsync();

        // File should still be usable
        var length = await file.GetLengthAsync();
        Assert.True(length > 0);
    }

#if NET8_0_OR_GREATER
    private static Task WriteAllBytesAsync(string path, byte[] bytes) => File.WriteAllBytesAsync(path, bytes);
#else
    private static Task WriteAllBytesAsync(string path, byte[] bytes) { File.WriteAllBytes(path, bytes); return Task.CompletedTask; }
#endif
}
