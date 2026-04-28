// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Proto.Encodings.V21;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using LanceField = EngineeredWood.Lance.Proto.Field;
using LanceFileDescriptor = EngineeredWood.Lance.Proto.FileDescriptor;
using LanceSchema = EngineeredWood.Lance.Proto.Schema;
using V2Encoding = EngineeredWood.Lance.Proto.V2.Encoding;
using V2DirectEncoding = EngineeredWood.Lance.Proto.V2.DirectEncoding;
using V2ColumnMetadata = EngineeredWood.Lance.Proto.V2.ColumnMetadata;
using ZstdCompressor = ZstdSharp.Compressor;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// v2.1 cases that pylance does not emit and therefore have no
/// cross-validation file. Each test hand-builds a Lance v2.1 file with a
/// specific encoding pattern so that decoder paths can still be exercised.
/// </summary>
public class HandCraftedV21Tests
{
    /// <summary>
    /// MiniBlockLayout with <c>value_compression = General(ZSTD, Flat(32))</c>
    /// — the per-chunk wrapped-compression case described in the
    /// <c>encodings_v2_1.proto</c> comment for <see cref="General"/>.
    ///
    /// <para>Pylance has not been observed emitting this layout, but the proto
    /// allows it: each chunk's value buffer is a single ZSTD frame whose
    /// decompressed output is processed by the inner <c>Flat</c> encoding.</para>
    /// </summary>
    [Fact]
    public async Task MiniBlock_GeneralZstd_Flat_Int32_SingleChunk()
    {
        int[] values = Enumerable.Range(0, 10).Select(i => i * 100).ToArray();

        byte[] file = BuildMiniBlockGeneralZstdFlatInt32(values, valuesPerChunk: values.Length);
        await using var backend = new InMemoryRandomAccessFile(file);
        await using var reader = await LanceFileReader.OpenAsync(backend);

        Assert.Equal(LanceVersion.V2_1, reader.Version);
        Assert.Equal((long)values.Length, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(values.Length, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetValue(i));
    }

    [Fact]
    public async Task MiniBlock_GeneralZstd_Flat_Int32_MultiChunk()
    {
        // 2048 values split across 2 chunks of 1024 each, each chunk's value
        // buffer is independently ZSTD-compressed. Exercises the chunk loop
        // under General wrapping.
        int[] values = Enumerable.Range(0, 2048).Select(i => unchecked(i * 1103515245 + 12345)).ToArray();

        byte[] file = BuildMiniBlockGeneralZstdFlatInt32(values, valuesPerChunk: 1024);
        await using var backend = new InMemoryRandomAccessFile(file);
        await using var reader = await LanceFileReader.OpenAsync(backend);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(values.Length, arr.Length);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], arr.GetValue(i));
    }

    private static byte[] BuildMiniBlockGeneralZstdFlatInt32(int[] values, int valuesPerChunk)
    {
        const int MiniBlockAlignment = 8;

        // Split into chunks of `valuesPerChunk` items each (last chunk may be smaller).
        int numChunks = (values.Length + valuesPerChunk - 1) / valuesPerChunk;
        var chunkBlobs = new List<byte[]>(numChunks);
        var chunkSizes = new List<int>(numChunks);
        var chunkLogNumValues = new List<int>(numChunks);

        // log_num_values for non-last chunks must be a power of 2 marker.
        // Only enforced when we actually have non-last chunks.
        int logVpc = 0;
        if (numChunks > 1)
        {
            while ((1 << logVpc) < valuesPerChunk) logVpc++;
            if ((1 << logVpc) != valuesPerChunk)
                throw new ArgumentException("valuesPerChunk must be a power of 2 when there is more than one chunk.");
        }

        for (int c = 0; c < numChunks; c++)
        {
            int start = c * valuesPerChunk;
            int count = Math.Min(valuesPerChunk, values.Length - start);
            byte[] raw = new byte[count * 4];
            for (int i = 0; i < count; i++)
                BinaryPrimitives.WriteInt32LittleEndian(raw.AsSpan(i * 4, 4), values[start + i]);
            byte[] compressed;
            using (var zstd = new ZstdCompressor(level: 3))
                compressed = zstd.Wrap(raw).ToArray();

            int headerBytes = 2 + 2; // num_levels + value_buf_size
            int paddedHeader = AlignUp(headerBytes, MiniBlockAlignment);
            int paddedValueBuf = AlignUp(compressed.Length, MiniBlockAlignment);
            int chunkSize = paddedHeader + paddedValueBuf;

            byte[] chunkBytes = new byte[chunkSize];
            BinaryPrimitives.WriteUInt16LittleEndian(chunkBytes.AsSpan(0, 2), 0); // num_levels
            BinaryPrimitives.WriteUInt16LittleEndian(chunkBytes.AsSpan(2, 2), (ushort)compressed.Length);
            System.Array.Copy(compressed, 0, chunkBytes, paddedHeader, compressed.Length);

            chunkBlobs.Add(chunkBytes);
            chunkSizes.Add(chunkSize);
            chunkLogNumValues.Add(c == numChunks - 1 ? 0 : logVpc);
        }

        // Concatenate chunks and build per-chunk metadata words.
        int totalChunkBytes = chunkSizes.Sum();
        byte[] chunkData = new byte[totalChunkBytes];
        int writeOff = 0;
        for (int c = 0; c < numChunks; c++)
        {
            System.Array.Copy(chunkBlobs[c], 0, chunkData, writeOff, chunkSizes[c]);
            writeOff += chunkSizes[c];
        }

        byte[] chunkMeta = new byte[numChunks * 2];
        for (int c = 0; c < numChunks; c++)
        {
            int dividedBytes = chunkSizes[c] / MiniBlockAlignment - 1;
            if (dividedBytes < 0 || dividedBytes > (1 << 12) - 1)
                throw new InvalidOperationException(
                    $"Chunk size {chunkSizes[c]} doesn't fit in v2.1 u16 chunk metadata word.");
            ushort metaWord = checked((ushort)((dividedBytes << 4) | (chunkLogNumValues[c] & 0xF)));
            BinaryPrimitives.WriteUInt16LittleEndian(chunkMeta.AsSpan(c * 2, 2), metaWord);
        }

        // Reuse `chunkBytes` variable name for the concatenated chunk data
        // for backward-compat with the rest of the function.
        byte[] chunkBytes_concat = chunkData;

        // Build the page layout proto. value_compression = General(ZSTD, Flat(32)).
        var pageLayout = new PageLayout
        {
            MiniBlockLayout = new MiniBlockLayout
            {
                NumItems = (ulong)values.Length,
                NumBuffers = 1,
                ValueCompression = new CompressiveEncoding
                {
                    General = new General
                    {
                        Compression = new BufferCompression
                        {
                            Scheme = CompressionScheme.CompressionAlgorithmZstd,
                        },
                        Values = new CompressiveEncoding
                        {
                            Flat = new Flat { BitsPerValue = 32 },
                        },
                    },
                },
            },
        };
        pageLayout.MiniBlockLayout.Layers.Add(RepDefLayer.RepdefAllValidItem);
        byte[] pageLayoutBytes = WrapEncoding(pageLayout);

        // Build the FileDescriptor: one int32 leaf field, total length=N rows.
        var fd = new LanceFileDescriptor
        {
            Schema = new LanceSchema(),
            Length = (ulong)values.Length,
        };
        fd.Schema.Fields.Add(new LanceField
        {
            Type = LanceField.Types.Type.Leaf,
            Name = "x",
            Id = 1,
            ParentId = 0,
            LogicalType = "int32",
            Nullable = false,
        });
        byte[] fdBytes = fd.ToByteArray();

        // Compute file-absolute offsets for the layout:
        //   [FileDescriptor][chunk meta buf][chunk data buf][ColumnMetadata]
        //   [CMO table][GBO table][Footer]
        long offFd = 0;
        long offChunkMeta = offFd + fdBytes.Length;
        long offChunkData = offChunkMeta + chunkMeta.Length;
        long offColMeta = offChunkData + chunkBytes_concat.Length;

        // Build the ColumnMetadata with one Page that references the
        // file-absolute offsets of the two page buffers.
        var page = new V2ColumnMetadata.Types.Page
        {
            Length = (ulong)values.Length,
            Encoding = new V2Encoding
            {
                Direct = new V2DirectEncoding { Encoding = ByteString.CopyFrom(pageLayoutBytes) },
            },
        };
        page.BufferOffsets.Add((ulong)offChunkMeta);
        page.BufferSizes.Add((ulong)chunkMeta.Length);
        page.BufferOffsets.Add((ulong)offChunkData);
        page.BufferSizes.Add((ulong)chunkBytes_concat.Length);

        var cm = new V2ColumnMetadata();
        cm.Pages.Add(page);
        byte[] cmBytes = cm.ToByteArray();

        long offCmoTable = offColMeta + cmBytes.Length;
        long offGboTable = offCmoTable + 16; // 1 column × 16 bytes
        long offFooter = offGboTable + 16;   // 1 global buffer × 16 bytes

        using var ms = new MemoryStream();
        ms.Write(fdBytes);
        ms.Write(chunkMeta);
        ms.Write(chunkBytes_concat);
        ms.Write(cmBytes);

        // CMO table: one entry per column (offset, size).
        WriteOffsetSize(ms, offColMeta, cmBytes.Length);

        // GBO table: one entry per global buffer (FileDescriptor only).
        WriteOffsetSize(ms, offFd, fdBytes.Length);

        // Footer (40 bytes): 4×u64 + 2×u32 + 4-byte magic.
        var footer = new LanceFooter(
            ColumnMetaStart: offColMeta,
            CmoTableOffset: offCmoTable,
            GboTableOffset: offGboTable,
            NumGlobalBuffers: 1,
            NumColumns: 1,
            Version: LanceVersion.V2_1);
        byte[] footerBytes = new byte[LanceFooter.Size];
        footer.WriteTo(footerBytes);
        ms.Write(footerBytes);

        return ms.ToArray();
    }

    private static int AlignUp(int value, int alignment) =>
        (value + alignment - 1) & ~(alignment - 1);

    private static void WriteOffsetSize(MemoryStream ms, long offset, long size)
    {
        Span<byte> buf = stackalloc byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(buf.Slice(0, 8), offset);
        BinaryPrimitives.WriteInt64LittleEndian(buf.Slice(8, 8), size);
        ms.Write(buf.ToArray());
    }

    /// <summary>
    /// Wrap a v2.1 PageLayout in the two-layer encoding envelope the file
    /// format expects: <c>Encoding.direct.encoding</c> = bytes of a
    /// <c>google.protobuf.Any</c> whose <c>.value</c> is the PageLayout.
    /// </summary>
    private static byte[] WrapEncoding(PageLayout layout)
    {
        var any = new Any
        {
            TypeUrl = "/lance.encodings21.PageLayout",
            Value = layout.ToByteString(),
        };
        return any.ToByteArray();
    }
}
