// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Proto.Encodings.V21;
using EngineeredWood.Lance.Proto.V2;
using Any = Google.Protobuf.WellKnownTypes.Any;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Byte-level checks of the mini-block chunks <see cref="LanceFileWriter"/>
/// emits, against what pylance writes and lance 14 insists on. The pylance
/// cross-validation tests catch the same defects but skip when pylance is not
/// installed (as in CI); these don't need it.
/// </summary>
public class LanceFileWriterChunkGrammarTests
{
    // #994: lance 14 checks a Flat buffer's size exactly ("Flat buffer has 16
    // bytes, expected 12"). pylance records 12 for three int32s and pads the
    // chunk to 8 bytes without counting the padding.
    [Fact]
    public async Task Flat_RecordsExactValueBufferSize()
    {
        var chunk = await WriteAndReadSingleChunkAsync(
            new Int32Array.Builder().Append(0x11111111).Append(0x22222222).Append(0x33333333).Build());

        Assert.False(chunk.Layout.HasLargeChunk);
        Assert.Equal(12u, chunk.ValueBufferSize);
        Assert.Equal(0, chunk.ChunkBytes.Length % 8);
    }

    [Fact]
    public async Task Bool_RecordsExactValueBufferSize()
    {
        var chunk = await WriteAndReadSingleChunkAsync(
            new BooleanArray.Builder().Append(true).Append(false).Append(true).Build());

        Assert.Equal(1u, chunk.ValueBufferSize);
    }

    // Variable: pylance pads each chunk to the offset width (4 bytes for u32
    // offsets) and records that padded size; lance's legacy binary decoder
    // refuses a size that isn't a multiple of it.
    [Fact]
    public async Task Variable_RecordsSizeRoundedToOffsetWidth()
    {
        var chunk = await WriteAndReadSingleChunkAsync(
            new StringArray.Builder().Append("a").Append("b").Append("c").Build());

        // 4 u32 offsets (16 bytes) + 3 data bytes = 19: pylance records 20, not 24.
        Assert.Equal(20u, chunk.ValueBufferSize);
    }

    // #997: lance 14 refuses a v2.2 mini-block page without has_large_chunk,
    // which means u32 chunk-metadata words and a u32 value-buffer size.
    [Fact]
    public async Task Map_FileUsesLargeChunkGrammar()
    {
        string path = TempPath();
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("kvs", BuildMap());
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(LanceVersion.V2_2, reader.Version);
            byte[] file = File.ReadAllBytes(path);
            int pages = 0;
            foreach (var column in reader.ColumnMetadatas)
            {
                foreach (var page in column.Pages)
                {
                    var layout = MiniBlock(page);
                    Assert.True(layout.HasLargeChunk);
                    pages++;

                    // One chunk: a single u32 word, and its byte count matches the
                    // chunk-data buffer.
                    Assert.Equal(4ul, page.BufferSizes[0]);
                    uint word = BinaryPrimitives.ReadUInt32LittleEndian(
                        file.AsSpan((int)page.BufferOffsets[0], 4));
                    Assert.Equal(page.BufferSizes[1], ((word >> 4) + 1) * 8);
                }
            }
            Assert.Equal(2, pages);  // key + value leaves

            var read = (MapArray)await reader.ReadColumnAsync(0);
            Assert.Equal("c", ((StringArray)read.Keys).GetString(2));
            Assert.Equal(3, ((Int32Array)read.Values).GetValue(2));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    // A v2.1-grammar page already on disk can't be part of a v2.2 file, so a
    // Map column after one must be refused rather than producing a file no
    // lance reader accepts.
    [Fact]
    public async Task Map_AfterAMiniBlockColumn_IsRefused()
    {
        string path = TempPath();
        try
        {
            await using var writer = await LanceFileWriter.CreateAsync(path);
            await writer.WriteColumnAsync(
                "id", new Int32Array.Builder().Append(1).Append(2).Append(3).Build());
            var ex = await Assert.ThrowsAsync<NotSupportedException>(
                () => writer.WriteColumnAsync("kvs", BuildMap()));
            Assert.Contains("LanceVersion.V2_2", ex.Message);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task Map_BeforeOtherColumns_PutsThemInTheLargeChunkGrammarToo()
    {
        string path = TempPath();
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("kvs", BuildMap());
                await writer.WriteColumnAsync(
                    "id", new Int32Array.Builder().Append(1).Append(2).Append(3).Build());
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            var idPage = reader.ColumnMetadatas[reader.ColumnMetadatas.Count - 1].Pages[0];
            Assert.True(MiniBlock(idPage).HasLargeChunk);
            Assert.Equal(4ul, idPage.BufferSizes[0]);

            byte[] file = File.ReadAllBytes(path);
            // Header: num_levels (u16) then a u32 value-buffer size.
            Assert.Equal(12u, BinaryPrimitives.ReadUInt32LittleEndian(
                file.AsSpan((int)idPage.BufferOffsets[1] + 2, 4)));

            var ids = (Int32Array)await reader.ReadColumnAsync(1);
            Assert.Equal(new int?[] { 1, 2, 3 }, ids.ToArray());
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    // Copilot on #999: the Bool decoder read def_size as u32 in a large-chunk
    // file. It is a u16 in both formats; only buffer sizes widen.
    [Fact]
    public async Task MapThenNullableBool_RoundTrips()
    {
        string path = TempPath();
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("kvs", BuildMap());
                await writer.WriteColumnAsync(
                    "b", new BooleanArray.Builder().Append(true).AppendNull().Append(false).Build());
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            var b = (BooleanArray)await reader.ReadColumnAsync(1);
            Assert.Equal(new bool?[] { true, null, false }, new[] { b.GetValue(0), b.GetValue(1), b.GetValue(2) });
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task ExplicitV2_2_AllowsAMapAfterOtherColumns()
    {
        string path = TempPath();
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path, version: LanceVersion.V2_2))
            {
                await writer.WriteColumnAsync(
                    "id", new Int32Array.Builder().Append(1).Append(2).Append(3).Build());
                await writer.WriteColumnAsync("kvs", BuildMap());
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            Assert.Equal(LanceVersion.V2_2, reader.Version);
            foreach (var column in reader.ColumnMetadatas)
                foreach (var page in column.Pages)
                    Assert.True(MiniBlock(page).HasLargeChunk);
            var ids = (Int32Array)await reader.ReadColumnAsync(0);
            Assert.Equal(new int?[] { 1, 2, 3 }, ids.ToArray());
            var map = (MapArray)await reader.ReadColumnAsync(1);
            Assert.Equal("c", ((StringArray)map.Keys).GetString(2));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    // An explicit version is kept: a writer created for v2.1 doesn't move to
    // v2.2 even when the Map is its first column.
    [Fact]
    public async Task ExplicitV2_1_RefusesAMap()
    {
        string path = TempPath();
        try
        {
            await using var writer = await LanceFileWriter.CreateAsync(path, version: LanceVersion.V2_1);
            var ex = await Assert.ThrowsAsync<NotSupportedException>(
                () => writer.WriteColumnAsync("kvs", BuildMap()));
            Assert.Contains("created for v2.1", ex.Message);
            Assert.Equal(LanceVersion.V2_1, writer.Version);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    [Fact]
    public async Task UnsupportedVersion_IsRefused()
    {
        string path = TempPath();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await LanceFileWriter.CreateAsync(path, version: LanceVersion.V2_0));
        Assert.False(File.Exists(path));
    }

    private sealed record SingleChunk(MiniBlockLayout Layout, uint ValueBufferSize, byte[] ChunkBytes);

    private static async Task<SingleChunk> WriteAndReadSingleChunkAsync(IArrowArray array)
    {
        string path = TempPath();
        try
        {
            await using (var writer = await LanceFileWriter.CreateAsync(path))
            {
                await writer.WriteColumnAsync("c", array);
                await writer.FinishAsync();
            }

            await using var reader = await LanceFileReader.OpenAsync(path);
            var page = Assert.Single(Assert.Single(reader.ColumnMetadatas).Pages);
            var layout = MiniBlock(page);
            Assert.Null(layout.DefCompression);  // no nulls: header = num_levels + value size
            Assert.Null(layout.RepCompression);
            Assert.Equal(2ul, page.BufferSizes[0]);  // one u16 chunk word

            byte[] file = File.ReadAllBytes(path);
            byte[] chunk = file.AsSpan((int)page.BufferOffsets[1], (int)page.BufferSizes[1]).ToArray();
            uint size = BinaryPrimitives.ReadUInt16LittleEndian(chunk.AsSpan(2, 2));

            // Our own reader still reads it back.
            var read = await reader.ReadColumnAsync(0);
            Assert.Equal(array.Length, read.Length);
            return new SingleChunk(layout, size, chunk);
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    private static MiniBlockLayout MiniBlock(ColumnMetadata.Types.Page page)
    {
        var any = Any.Parser.ParseFrom(page.Encoding.Direct.Encoding);
        var layout = PageLayout.Parser.ParseFrom(any.Value);
        Assert.Equal(PageLayout.LayoutOneofCase.MiniBlockLayout, layout.LayoutCase);
        return layout.MiniBlockLayout;
    }

    private static MapArray BuildMap()
    {
        // map<string, int32> with 3 rows: {"a":1,"b":2}, {}, {"c":3}.
        var keys = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        var values = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var entriesType = new StructType(new[]
        {
            new Field("key", StringType.Default, nullable: false),
            new Field("value", Int32Type.Default, nullable: true),
        });
        var entries = new StructArray(new ArrayData(
            entriesType, length: 3, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty },
            new[] { keys.Data, values.Data }));

        var offsets = new byte[4 * sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(0, 4), 0);
        BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(4, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(8, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(12, 4), 3);

        return new MapArray(new ArrayData(
            new MapType(entriesType), length: 3, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(offsets) },
            new[] { entries.Data }));
    }

    private static string TempPath() =>
        Path.Combine(Path.GetTempPath(), $"ew-chunk-grammar-{Guid.NewGuid():N}.lance");
}
