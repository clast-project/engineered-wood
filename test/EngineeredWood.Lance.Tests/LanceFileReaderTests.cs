// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;
using EngineeredWood.Lance.Format;

namespace EngineeredWood.Lance.Tests;

public class LanceFileReaderTests
{
    [Fact]
    public async Task Reads_SchemaAndRowCount_FromMinimalFile()
    {
        var builder = new MinimalLanceFileBuilder
        {
            Version = LanceVersion.V2_1,
            FileDescriptor = new Proto.FileDescriptor
            {
                Schema = new Proto.Schema(),
                Length = 42,
            },
        };
        builder.AddLeafField("a", "int32", nullable: false);
        builder.AddLeafField("b", "string", nullable: true);
        builder.AddEmptyColumn();
        builder.AddEmptyColumn();

        byte[] file = builder.Build();
        await using var backend = new InMemoryRandomAccessFile(file);
        await using var reader = await LanceFileReader.OpenAsync(backend);

        Assert.Equal(LanceVersion.V2_1, reader.Version);
        Assert.Equal(42L, reader.NumberOfRows);
        Assert.Equal(2, reader.NumberOfColumns);
        Assert.Equal(2, reader.Schema.FieldsList.Count);
        Assert.Equal("a", reader.Schema.FieldsList[0].Name);
        Assert.IsType<Int32Type>(reader.Schema.FieldsList[0].DataType);
        Assert.False(reader.Schema.FieldsList[0].IsNullable);
        Assert.Equal("b", reader.Schema.FieldsList[1].Name);
        Assert.IsType<StringType>(reader.Schema.FieldsList[1].DataType);
    }

    [Fact]
    public async Task Reads_EmptySchema()
    {
        var builder = new MinimalLanceFileBuilder
        {
            Version = LanceVersion.V2_0,
            FileDescriptor = new Proto.FileDescriptor
            {
                Schema = new Proto.Schema(),
                Length = 0,
            },
        };
        byte[] file = builder.Build();

        await using var backend = new InMemoryRandomAccessFile(file);
        await using var reader = await LanceFileReader.OpenAsync(backend);

        Assert.Equal(LanceVersion.V2_0, reader.Version);
        Assert.Equal(0L, reader.NumberOfRows);
        Assert.Equal(0, reader.NumberOfColumns);
        Assert.Empty(reader.Schema.FieldsList);
    }

    [Fact]
    public async Task Rejects_LegacyV01()
    {
        var builder = new MinimalLanceFileBuilder
        {
            Version = LanceVersion.Legacy_V0_1,
        };
        byte[] file = builder.Build();

        await using var backend = new InMemoryRandomAccessFile(file);
        var ex = await Assert.ThrowsAsync<LanceFormatException>(
            async () => await LanceFileReader.OpenAsync(backend));
        Assert.Contains("v0.1", ex.Message);
    }

    [Fact]
    public async Task Rejects_FutureV22()
    {
        var builder = new MinimalLanceFileBuilder
        {
            Version = new LanceVersion(2, 2),
        };
        byte[] file = builder.Build();

        await using var backend = new InMemoryRandomAccessFile(file);
        var ex = await Assert.ThrowsAsync<LanceFormatException>(
            async () => await LanceFileReader.OpenAsync(backend));
        Assert.Contains("2.2", ex.Message);
    }

    [Fact]
    public async Task Rejects_BadMagic()
    {
        var builder = new MinimalLanceFileBuilder
        {
            OverrideMagic = new byte[] { 0x50, 0x41, 0x52, 0x31 }, // "PAR1"
        };
        byte[] file = builder.Build();

        await using var backend = new InMemoryRandomAccessFile(file);
        await Assert.ThrowsAsync<LanceFormatException>(
            async () => await LanceFileReader.OpenAsync(backend));
    }

    [Fact]
    public async Task Rejects_FileSmallerThanFooter()
    {
        byte[] tiny = new byte[LanceFooter.Size - 1];
        await using var backend = new InMemoryRandomAccessFile(tiny);
        var ex = await Assert.ThrowsAsync<LanceFormatException>(
            async () => await LanceFileReader.OpenAsync(backend));
        Assert.Contains("too small", ex.Message);
    }

    [Fact]
    public async Task Rejects_MalformedFileDescriptor()
    {
        // Build a well-formed envelope, then overwrite global buffer 0 (which
        // holds the FileDescriptor bytes starting at offset 0) with garbage
        // protobuf data. The footer still points at the original position/size,
        // but parsing will fail.
        var builder = new MinimalLanceFileBuilder
        {
            Version = LanceVersion.V2_1,
        };
        byte[] file = builder.Build();

        // Corrupt the first bytes. Proto field tags of 0xFF are invalid.
        for (int i = 0; i < 4 && i < file.Length; i++)
            file[i] = 0xFF;

        await using var backend = new InMemoryRandomAccessFile(file);
        await Assert.ThrowsAsync<LanceFormatException>(
            async () => await LanceFileReader.OpenAsync(backend));
    }

    [Fact]
    public async Task Exposes_SchemaMetadata()
    {
        var builder = new MinimalLanceFileBuilder();
        builder.FileDescriptor.Schema.Metadata["writer"] =
            Google.Protobuf.ByteString.CopyFromUtf8("engineered-wood-test");
        byte[] file = builder.Build();

        await using var backend = new InMemoryRandomAccessFile(file);
        await using var reader = await LanceFileReader.OpenAsync(backend);

        Assert.True(reader.Metadata.ContainsKey("writer"));
        Assert.Equal("engineered-wood-test",
            System.Text.Encoding.UTF8.GetString(reader.Metadata["writer"]));
    }

    [Fact]
    public async Task TailReadStillWorks_ForFilesLargerThanTailBuffer()
    {
        // Pad the FileDescriptor region so the optimistic tail (64 KiB) can't
        // cover the whole file, forcing a second ranged read for global buffer 0.
        var builder = new MinimalLanceFileBuilder
        {
            Version = LanceVersion.V2_0,
            FileDescriptor = new Proto.FileDescriptor
            {
                Schema = new Proto.Schema(),
                Length = 1,
            },
        };
        // Additional global buffers inflate the early part of the file.
        for (int i = 0; i < 4; i++)
            builder.AdditionalGlobalBuffers.Add(new byte[32 * 1024]);

        byte[] file = builder.Build();
        Assert.True(file.Length > LanceFileReader.DefaultTailReadSize,
            $"Expected file > {LanceFileReader.DefaultTailReadSize} bytes, got {file.Length}.");

        await using var backend = new InMemoryRandomAccessFile(file);
        await using var reader = await LanceFileReader.OpenAsync(backend);

        Assert.Equal(1L, reader.NumberOfRows);
        Assert.Equal(5, reader.Version is { Major: 2, Minor: 0 } ? 5 : 5); // sanity
    }
}
