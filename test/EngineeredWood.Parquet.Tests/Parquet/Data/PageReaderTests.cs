// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using EngineeredWood.Tests.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// <see cref="PageReader"/>, the one walker of page headers. The property the page index and
/// encryption both need is that a data page's ordinal is its position in the chunk, so a walk that
/// starts at page <i>k</i>, located through the OffsetIndex, numbers it <i>k</i>. The oracle is the
/// OffsetIndex other writers wrote: page <i>k</i> of the walk must be at its entry <i>k</i>.
/// </summary>
public class PageReaderTests : IDisposable
{
    private readonly string _tempDir;

    public PageReaderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-page-reader-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private readonly record struct Walked(int Offset, int HeaderSize, PageType Type, int CompressedPageSize, int Ordinal);

    /// <summary>
    /// For every chunk with an OffsetIndex, repeated columns included: a walk from the chunk's start
    /// numbers the data pages 0, 1, 2… at the index's locations, and a walk started at any data page
    /// with that page's position reads the same pages, numbered the same, from there on.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public void Fixture_OrdinalsArePositionsInTheOffsetIndex(string fileName)
    {
        Assert.True(AssertOrdinalsArePositions(TestData.GetPath(fileName)) > 0, "no chunk compared");
    }

    [Fact]
    public async Task EwFile_OrdinalsArePositionsInTheOffsetIndex()
    {
        string path = await WriteAsync(dictionary: true);
        Assert.Equal(2, AssertOrdinalsArePositions(path));
    }

    [Fact]
    public async Task APageThatOverrunsTheBytes_IsAFormatError()
    {
        string path = await WriteAsync(dictionary: false);
        var (chunk, column, meta) = FirstChunk(path);
        var truncated = chunk.AsSpan(0, chunk.Length - 1).ToArray();

        var ex = Assert.Throws<ParquetFormatException>(() => WalkAll(truncated, column));
        Assert.Contains("Column 'id'", ex.Message);
        Assert.Contains("truncated", ex.Message);

        // The column readers surface it as is, where they used to throw an ArgumentOutOfRangeException.
        Assert.Throws<ParquetFormatException>(() => PageMapBuilder.Build(truncated, column, meta));
    }

    [Fact]
    public async Task ACorruptHeader_NamesTheColumnAndTheNextOrdinal()
    {
        string path = await WriteAsync(dictionary: false);
        var (chunk, column, _) = FirstChunk(path);
        var pages = WalkAll(chunk, column);
        var corrupt = (byte[])chunk.Clone();
        corrupt[pages[2].Offset] = 0xFF; // not a Thrift field header

        var ex = Assert.Throws<ParquetFormatException>(() => WalkAll(corrupt, column));
        Assert.Contains($"Column 'id': corrupted page header at byte offset {pages[2].Offset} (before data page 2)", ex.Message);
    }

    [Fact]
    public void NoBytes_NoPages()
    {
        var reader = new PageReader(ReadOnlySpan<byte>.Empty, new SchemaDescriptor(MinimalSchema()).Columns[0]);
        Assert.False(reader.TryRead(out _));
    }

    // ───── Helpers ─────

    private static int AssertOrdinalsArePositions(string path)
    {
        byte[] file = File.ReadAllBytes(path);
        var metadata = ReadFooter(file);
        var schema = new SchemaDescriptor(metadata.Schema);
        int compared = 0;

        foreach (var rowGroup in metadata.RowGroups)
        {
            for (int c = 0; c < rowGroup.Columns.Count; c++)
            {
                var chunk = rowGroup.Columns[c];
                if (chunk.OffsetIndexOffset is not { } indexOffset || chunk.MetaData is not { } meta)
                    continue;

                var column = schema.Columns[c];
                long start = ChunkStart(meta);
                var bytes = file.AsSpan(checked((int)start), checked((int)meta.TotalCompressedSize));
                var locations = MetadataDecoder.DecodeOffsetIndex(
                    file.AsSpan(checked((int)indexOffset), chunk.OffsetIndexLength!.Value)).PageLocations;
                string where = $"{Path.GetFileName(path)} column {column.DottedPath}";

                var all = WalkAll(bytes, column);
                var data = all.Where(p => p.Ordinal >= 0).ToList();
                Assert.All(all.Where(p => p.Ordinal < 0), p => Assert.False(p.Type is PageType.DataPage or PageType.DataPageV2, where));
                Assert.Equal(locations.Count, data.Count);

                for (int k = 0; k < data.Count; k++)
                {
                    Assert.Equal(k, data[k].Ordinal);
                    Assert.Equal(locations[k].Offset - start, data[k].Offset);
                    Assert.Equal(locations[k].CompressedPageSize, data[k].HeaderSize + data[k].CompressedPageSize);

                    // Start at page k, as a reader that skipped pages 0 to k-1 would.
                    int from = checked((int)(locations[k].Offset - start));
                    var tail = WalkAll(bytes.Slice(from), column, firstOrdinal: k)
                        .Select(p => p with { Offset = p.Offset + from });
                    Assert.Equal(all.SkipWhile(p => p.Offset < from), tail);
                }

                compared++;
            }
        }

        return compared;
    }

    private static List<Walked> WalkAll(ReadOnlySpan<byte> bytes, ColumnDescriptor column, int firstOrdinal = 0)
    {
        var walked = new List<Walked>();
        var reader = new PageReader(bytes, column, firstOrdinal);
        while (reader.TryRead(out var page))
        {
            Assert.Equal(page.Header.CompressedPageSize, page.Payload.Length);
            walked.Add(new Walked(page.Offset, page.HeaderSize, page.Header.Type, page.Header.CompressedPageSize, page.Ordinal));
        }

        Assert.Equal(bytes.Length, reader.Position);
        return walked;
    }

    private (byte[] Chunk, ColumnDescriptor Column, ColumnMetaData Meta) FirstChunk(string path)
    {
        byte[] file = File.ReadAllBytes(path);
        var metadata = ReadFooter(file);
        var meta = metadata.RowGroups[0].Columns[0].MetaData!;
        var chunk = file.AsSpan(checked((int)ChunkStart(meta)), checked((int)meta.TotalCompressedSize)).ToArray();
        return (chunk, new SchemaDescriptor(metadata.Schema).Columns[0], meta);
    }

    /// <summary>A flat id column and a list column, in pages small enough that each chunk has several.</summary>
    private async Task<string> WriteAsync(bool dictionary)
    {
        const int rows = 5_000;
        var id = new Int64Array.Builder();
        var list = new ListArray.Builder(Int32Type.Default);
        var values = (Int32Array.Builder)list.ValueBuilder;
        for (int r = 0; r < rows; r++)
        {
            id.Append(r % 50);
            list.Append();
            for (int i = 0; i < r % 5; i++)
                values.Append(r + i);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("xs", new ListType(Int32Type.Default), true))
            .Build();
        var batch = new RecordBatch(schema, [id.Build(), list.Build()], rows);

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            DictionaryEnabled = dictionary,
            DataPageSize = 1024,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static List<SchemaElement> MinimalSchema() =>
    [
        new SchemaElement { Name = "schema", NumChildren = 1 },
        new SchemaElement { Name = "id", Type = PhysicalType.Int64, RepetitionType = FieldRepetitionType.Required },
    ];

    /// <summary>Where the reader starts a chunk: its dictionary or symbol-table page, else its first data page.</summary>
    private static long ChunkStart(ColumnMetaData meta) =>
        meta.DictionaryPageOffset is > 0 and long dpo ? dpo
        : meta.SymbolTablePageOffset is > 0 and long stpo ? stpo
        : meta.DataPageOffset;

    private static FileMetaData ReadFooter(byte[] bytes)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - length, length));
    }
}
