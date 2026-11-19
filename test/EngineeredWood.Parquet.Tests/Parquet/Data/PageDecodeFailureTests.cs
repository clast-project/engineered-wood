// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;
using Encoding = EngineeredWood.Parquet.Encoding;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A page whose header is sound but whose data disagrees with it is a
/// <see cref="ParquetFormatException"/>, from both the whole-chunk read and the batched one. The
/// decoders trust their data, and such a page used to escape as whatever a decoder or codec hit
/// (<see cref="IndexOutOfRangeException"/>, ZstdSharp's own exception type), or to read as wrong
/// values from a buffer it had not filled (#426). Each chunk here is built by hand, page by page.
/// </summary>
public class PageDecodeFailureTests : IDisposable
{
    private readonly string _tempDir;

    public PageDecodeFailureTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-decode-failure-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// A data page that refers to an entry past the end of the dictionary. A BYTE_ARRAY index read
    /// an offset that is not there, and the others read past the end of their arrays.
    /// </summary>
    [Theory]
    [InlineData(PhysicalType.Int32)]
    [InlineData(PhysicalType.ByteArray)]
    public void ADictionaryIndexPastTheEnd_IsRefused(PhysicalType type)
    {
        // Three entries, so two-bit indices can reach 3, one past the end.
        var chunk = DictionaryChunk(type, index: 3);
        AssertRefused(chunk, "a data page refers to dictionary entry 3, but the dictionary has 3 entries");
    }

    [Theory]
    [InlineData(PhysicalType.Int32)]
    [InlineData(PhysicalType.ByteArray)]
    public void TheLastDictionaryIndex_Reads(PhysicalType type)
    {
        var chunk = DictionaryChunk(type, index: 2);
        var whole = ReadWhole(chunk);
        var batched = ReadBatched(chunk);
        Assert.Equal(4, whole.Length);
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(ArrowValues.Render(whole, 0), ArrowValues.Render(whole, i));
            Assert.Equal(ArrowValues.Render(whole, i), ArrowValues.Render(batched, i));
        }
        if (type == PhysicalType.Int32)
            Assert.Equal(30, ((Int32Array)whole).GetValue(0));
        else
            Assert.Equal("ccc"u8.ToArray(), ((BinaryArray)whole).GetBytes(0).ToArray());
        GC.KeepAlive(whole);
    }

    [Fact]
    public void DictionaryIndicesWiderThan32Bits_AreRefused()
    {
        var chunk = DictionaryChunk(PhysicalType.Int32, index: 0, bitWidth: 33);
        AssertRefused(chunk, "a dictionary-encoded page's indices are 33 bits wide");
    }

    /// <summary>
    /// A DELTA_BINARY_PACKED page whose data ends inside its own header: the decoder fails as it
    /// fails, and the read reports that as a format error naming the page, keeping what it threw.
    /// </summary>
    [Fact]
    public void ADecoderFailure_IsAFormatErrorNamingThePage()
    {
        // Block of 128 in 4 miniblocks, 4 values, first value 0, and then nothing: no min delta.
        byte[] payload = [0x80, 0x01, 0x04, 0x04, 0x00];
        var chunk = Chunk(PhysicalType.Int32, CompressionCodec.Uncompressed, numValues: 4,
            (DataPage(4, Encoding.DeltaBinaryPacked, payload.Length), payload));

        foreach (var ex in AssertRefused(chunk, "Column 'n': data page 0 could not be decoded"))
        {
            Assert.NotNull(ex.InnerException);
            Assert.IsNotType<ParquetFormatException>(ex.InnerException);
            Assert.Contains(ex.InnerException!.GetType().Name, ex.Message);
        }
    }

    /// <summary>
    /// A compressed page whose data decompresses to fewer bytes than its header declares. The tail of
    /// the pooled buffer, left by whatever used it before, was decoded as if it were this page's.
    /// </summary>
    [Fact]
    public void APageDecompressingShortOfItsDeclaredSize_IsRefused()
    {
        var chunk = PlainSnappyChunk(declaredUncompressed: 20);
        AssertRefused(chunk, "a page declares 20 uncompressed bytes, but its data decompresses to 16");
    }

    /// <summary>
    /// One that decompresses to more: five values' bytes in a page of four that declares 16. Most
    /// codecs throw when the destination is too small, but Gzip stops when it is full and reports it
    /// full, so the page passed as its first 16 bytes.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Brotli)]
    [InlineData(CompressionCodec.Lz4)]
    public void APageDecompressingPastItsDeclaredSize_IsRefused(CompressionCodec codec)
    {
        var chunk = PlainCompressedChunk(codec, declaredUncompressed: 16, values: [1, 2, 3, 4, 5]);
        var whole = Assert.Throws<ParquetFormatException>(() => ReadWhole(chunk));
        var batched = Assert.Throws<ParquetFormatException>(() => ReadBatched(chunk));
        Assert.Equal(whole.Message, batched.Message);
    }

    [Fact]
    public void APageDecompressingToItsDeclaredSize_Reads()
    {
        var chunk = PlainSnappyChunk(declaredUncompressed: 16);
        // The decoded arrays, not the encoded chunk, own the memory the spans read (#834 review round 1).
        var whole = (Int32Array)ReadWhole(chunk);
        Assert.Equal([1, 2, 3, 4], whole.Values.ToArray());
        var batched = (Int32Array)ReadBatched(chunk);
        Assert.Equal([1, 2, 3, 4], batched.Values.ToArray());
        GC.KeepAlive(whole);
        GC.KeepAlive(batched);
    }

    /// <summary>
    /// A compressed page declaring more uncompressed bytes than its whole chunk is refused before
    /// a buffer of that size is rented. Its data decompressed to what it holds, so this used to read.
    /// </summary>
    [Fact]
    public void ACompressedPageDeclaringMoreThanItsChunk_IsRefusedBeforeAllocating()
    {
        var chunk = PlainSnappyChunk(declaredUncompressed: 16, chunkUncompressed: 30);
        chunk = chunk with { Bytes = Rewrite(chunk.Bytes, uncompressed: 50_000_000) };

#if NET
        long before = GC.GetAllocatedBytesForCurrentThread();
#endif
        AssertRefused(chunk, "declares 50000000 uncompressed bytes, more than its whole column chunk (30)");
#if NET
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 10_000_000, "the refusal allocated the declared size");
#endif
    }

    /// <summary>
    /// A compressed page declaring more than its codec can expand its stored bytes to is refused from its header,
    /// before a buffer of the declared size is rented, even where its chunk's total, which the file also states,
    /// allows it. A page declaring exactly the bound passes that check and is refused only once decompressed, for
    /// decompressing to less: the bound is not a byte tighter than the formula.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Lz4)]
    [InlineData(CompressionCodec.Lz4Hadoop)]
    public void ACompressedPageDeclaringMoreThanItsCodecCanExpandTo_IsRefused(CompressionCodec codec)
    {
        // Lz4Hadoop reads a raw LZ4 block too, which is what the test compressor writes for it.
        var chunk = PlainCompressedChunk(
            codec == CompressionCodec.Lz4Hadoop ? CompressionCodec.Lz4 : codec, declaredUncompressed: 16,
            chunkUncompressed: int.MaxValue);
        chunk = chunk with { Meta = Copy(chunk.Meta, int.MaxValue, codec) };
        int stored = PageHeaderDecoder.Decode(chunk.Bytes, out _).CompressedPageSize;
        long most = Decompressor.MaxDecompressedLength(codec, stored)!.Value;

        var refused = chunk with { Bytes = Rewrite(chunk.Bytes, uncompressed: checked((int)most + 1)) };
        AssertRefused(refused,
            $"declares {most + 1} bytes decompressed from {stored} {codec} bytes, more than {codec} can expand them to ({most}).");

        var atTheBound = chunk with { Bytes = Rewrite(chunk.Bytes, uncompressed: (int)most) };
        foreach (var ex in AssertRefused(atTheBound, $"a page declares {most} uncompressed bytes, but its data decompresses to"))
            Assert.DoesNotContain("can expand them to", ex.Message);
    }

    /// <summary>
    /// The most compressible page there is, a megabyte and more of zeros, compressed by each codec's encoder, reads:
    /// no encoder EngineeredWood writes with expands its data past the bound the reader holds it to. Brotli has no
    /// bound and reads too.
    /// </summary>
    [Theory]
    [InlineData(CompressionCodec.Snappy)]
    [InlineData(CompressionCodec.Gzip)]
    [InlineData(CompressionCodec.Zstd)]
    [InlineData(CompressionCodec.Lz4)]
    [InlineData(CompressionCodec.Brotli)]
    public void TheMostCompressiblePage_ReadsUnderItsCodecsBound(CompressionCodec codec)
    {
        const int values = 1 << 20;
        byte[] plain = new byte[values * 4];
        byte[] compressed = new byte[Compressor.GetMaxCompressedLength(codec, plain.Length)];
        compressed = compressed.AsSpan(0, Compressor.Compress(codec, plain, compressed)).ToArray();
        var chunk = Chunk(PhysicalType.Int32, codec, numValues: values,
            (DataPage(values, Encoding.Plain, compressed.Length, plain.Length), compressed));

        var whole = (Int32Array)ReadWhole(chunk);
        Assert.Equal(values, whole.Length);
        Assert.True(whole.Values.ToArray().All(v => v == 0));
        var batched = (Int32Array)ReadBatched(chunk);
        Assert.Equal(values, batched.Length);
        Assert.True(batched.Values.ToArray().All(v => v == 0));
        GC.KeepAlive(whole);
        GC.KeepAlive(batched);
    }

    /// <summary>
    /// A V2 page whose value section is stored uncompressed in a compressed chunk decompresses nothing, so its
    /// uncompressed size sizes nothing and is not held to the codec's bound, as an uncompressed chunk's is not.
    /// </summary>
    [Fact]
    public void AV2PageStoredUncompressed_IsNotHeldToItsCodecsBound()
    {
        byte[] payload = Plain(1, 2, 3, 4);
        var header = new PageHeader
        {
            Type = PageType.DataPageV2,
            UncompressedPageSize = 50_000,
            CompressedPageSize = payload.Length,
            DataPageHeaderV2 = new DataPageHeaderV2
            {
                NumValues = 4,
                NumNulls = 0,
                NumRows = 4,
                Encoding = Encoding.Plain,
                DefinitionLevelsByteLength = 0,
                RepetitionLevelsByteLength = 0,
                IsCompressed = false,
            },
        };
        var chunk = Chunk(PhysicalType.Int32, CompressionCodec.Snappy, numValues: 4, (header, payload));
        Assert.True(50_000 > Decompressor.MaxDecompressedLength(CompressionCodec.Snappy, payload.Length));

        var whole = (Int32Array)ReadWhole(chunk);
        Assert.Equal([1, 2, 3, 4], whole.Values.ToArray());
        var batched = (Int32Array)ReadBatched(chunk);
        Assert.Equal([1, 2, 3, 4], batched.Values.ToArray());
        GC.KeepAlive(whole);
        GC.KeepAlive(batched);
    }

    /// <summary>
    /// An all-null V2 page whose value section is a compressed empty stream, as EngineeredWood writes one, reads.
    /// One whose header declares more than that stream can decompress to is refused from its header, although
    /// its levels would have shown that nothing needs decompressing (#935 review): the header lies, and pyarrow
    /// and DuckDB refuse the same page as they decompress it (DataFusion reads it). Checking every page's header
    /// in one place is worth refusing a page no honest writer produces.
    /// </summary>
    [Fact]
    public void AnAllNullV2PagesCompressedEmptySection_IsHeldToItsCodecsBound()
    {
        // Four nulls: one RLE run of four 0 definition levels (V2 levels carry no length prefix).
        byte[] levels = [4 << 1, 0];
        byte[] empty = new byte[Compressor.GetMaxCompressedLength(CompressionCodec.Snappy, 0)];
        empty = empty.AsSpan(0, Compressor.Compress(CompressionCodec.Snappy, [], empty)).ToArray();
        long most = Decompressor.MaxDecompressedLength(CompressionCodec.Snappy, empty.Length)!.Value;

        var honest = AllNull(declaredValueBytes: 0);
        var whole = ReadWhole(honest);
        Assert.Equal(4, whole.NullCount);
        var batched = ReadBatched(honest);
        Assert.Equal(4, batched.NullCount);
        GC.KeepAlive(whole);
        GC.KeepAlive(batched);

        AssertRefused(AllNull(declaredValueBytes: checked((int)most + 1)),
            $"declares {most + 1} bytes decompressed from {empty.Length} Snappy bytes, more than Snappy can expand them to ({most}).");

        TestChunk AllNull(int declaredValueBytes)
        {
            byte[] payload = [.. levels, .. empty];
            var header = new PageHeader
            {
                Type = PageType.DataPageV2,
                UncompressedPageSize = levels.Length + declaredValueBytes,
                CompressedPageSize = payload.Length,
                DataPageHeaderV2 = new DataPageHeaderV2
                {
                    NumValues = 4,
                    NumNulls = 4,
                    NumRows = 4,
                    Encoding = Encoding.Plain,
                    DefinitionLevelsByteLength = levels.Length,
                    RepetitionLevelsByteLength = 0,
                    IsCompressed = true,
                },
            };
            return Chunk(PhysicalType.Int32, CompressionCodec.Snappy, numValues: 4, typeLength: null, nullable: true,
                (header, payload));
        }
    }

    /// <summary>
    /// An uncompressed chunk's pages are read in place, so their uncompressed size is not read; a
    /// wrong one is not refused, as other readers do not refuse it.
    /// </summary>
    [Fact]
    public void AnUncompressedPageDeclaringMoreThanItsChunk_Reads()
    {
        byte[] payload = Plain(1, 2, 3, 4);
        var chunk = Chunk(PhysicalType.Int32, CompressionCodec.Uncompressed, numValues: 4,
            (DataPage(4, Encoding.Plain, payload.Length, uncompressed: 50_000_000), payload));
        // The decoded arrays, not the encoded chunk, own the memory the spans read (#834 review round 1).
        var whole = (Int32Array)ReadWhole(chunk);
        Assert.Equal([1, 2, 3, 4], whole.Values.ToArray());
        var batched = (Int32Array)ReadBatched(chunk);
        Assert.Equal([1, 2, 3, 4], batched.Values.ToArray());
        GC.KeepAlive(whole);
        GC.KeepAlive(batched);
    }

    /// <summary>
    /// <see cref="ParquetReadOptions.MaxPageUncompressedSize"/> refuses a page declaring more, on
    /// every read path, and a page declaring exactly the limit reads.
    /// </summary>
    [Fact]
    public async Task MaxPageUncompressedSize_RefusesALargerPageOnEveryReadPath()
    {
        string path = await WriteFileAsync();
        int largest = await LargestPageAsync(path);

        foreach (var read in ReadPaths)
        {
            var refused = await Assert.ThrowsAsync<ParquetFormatException>(() => read(path, largest - 1));
            Assert.Contains($"more than the {largest - 1} that MaxPageUncompressedSize allows", refused.Message);

            Assert.Equal(await read(path, null), await read(path, largest));
        }
    }

    /// <summary>
    /// A BYTE_STREAM_SPLIT page one byte short of its values. The SIMD decode kernels load without
    /// bounds checks, so the reader refuses the page itself, naming the column, before it reserves
    /// output for the values. A null InnerException shows the refusal is that check, not a decoder
    /// failure wrapped afterwards. 32 values reach both the AVX2 and the 128-bit kernels (#916).
    /// </summary>
    [Theory]
    [InlineData(PhysicalType.Float, 4)]
    [InlineData(PhysicalType.Int32, 4)]
    [InlineData(PhysicalType.Double, 8)]
    [InlineData(PhysicalType.Int64, 8)]
    [InlineData(PhysicalType.FixedLenByteArray, 8)]
    [InlineData(PhysicalType.FixedLenByteArray, 3)]
    public void AByteStreamSplitPageShortOfItsValues_IsRefused(PhysicalType type, int width)
    {
        const int values = 32;
        var chunk = ByteStreamSplitChunk(type, width, values, new byte[values * width - 1]);

        foreach (var ex in AssertRefused(chunk,
            $"Column 'n': BYTE_STREAM_SPLIT page holds {values * width - 1} bytes, " +
            $"but {values} values of {width} bytes need {values * width}."))
        {
            Assert.Null(ex.InnerException);
        }
    }

    [Theory]
    [InlineData(PhysicalType.Float, 4)]
    [InlineData(PhysicalType.Int32, 4)]
    [InlineData(PhysicalType.Double, 8)]
    [InlineData(PhysicalType.Int64, 8)]
    [InlineData(PhysicalType.FixedLenByteArray, 8)]
    [InlineData(PhysicalType.FixedLenByteArray, 3)]
    public void AByteStreamSplitPageOfExactlyItsValues_Reads(PhysicalType type, int width)
    {
        const int values = 32;
        byte[] expected = new byte[values * width];
        new Random(width).NextBytes(expected);
        // Stream s holds byte s of every value.
        byte[] payload = new byte[expected.Length];
        for (int s = 0; s < width; s++)
            for (int i = 0; i < values; i++)
                payload[s * values + i] = expected[i * width + s];

        var chunk = ByteStreamSplitChunk(type, width, values, payload);
        // The decoded arrays, not the encoded chunk, own the memory the spans read (#834 review round 1).
        var whole = ReadWhole(chunk);
        var batched = ReadBatched(chunk);
        Assert.Equal(expected, whole.Data.Buffers[1].Span[..expected.Length].ToArray());
        Assert.Equal(expected, batched.Data.Buffers[1].Span[..expected.Length].ToArray());
        GC.KeepAlive(whole);
        GC.KeepAlive(batched);
    }

    // ───── Helpers ─────

    /// <summary>One uncompressed BYTE_STREAM_SPLIT data page of <paramref name="values"/> required values.</summary>
    private static TestChunk ByteStreamSplitChunk(PhysicalType type, int width, int values, byte[] payload) =>
        Chunk(type, CompressionCodec.Uncompressed, numValues: values,
            typeLength: type == PhysicalType.FixedLenByteArray ? width : null,
            (DataPage(values, Encoding.ByteStreamSplit, payload.Length), payload));

    private sealed record TestChunk(byte[] Bytes, ColumnDescriptor Column, ColumnMetaData Meta, Field Field);

    /// <summary>Asserts both reads refuse the chunk with <paramref name="expected"/> in the message.</summary>
    private static ParquetFormatException[] AssertRefused(TestChunk chunk, string expected)
    {
        var whole = Assert.Throws<ParquetFormatException>(() => ReadWhole(chunk));
        Assert.Contains(expected, whole.Message);
        var batched = Assert.Throws<ParquetFormatException>(() => ReadBatched(chunk));
        Assert.Contains(expected, batched.Message);
        return [whole, batched];
    }

    private static IArrowArray ReadWhole(TestChunk chunk) =>
        ColumnChunkReader.ReadColumn(chunk.Bytes, chunk.Column, chunk.Meta, (int)chunk.Meta.NumValues, chunk.Field).Array;

    private static IArrowArray ReadBatched(TestChunk chunk)
    {
        var map = PageMapBuilder.Build(chunk.Bytes, chunk.Column, chunk.Meta);
        return ColumnChunkReader.ReadColumnBatchFromSlice(
            chunk.Bytes, 0, chunk.Column, chunk.Meta, map, 0, map.Pages.Length - 1, chunk.Field).Array;
    }

    /// <summary>
    /// A dictionary of three entries (10, 20, 30, or "a", "bb", "ccc") and a data page of four
    /// values that all refer to entry <paramref name="index"/>.
    /// </summary>
    private static TestChunk DictionaryChunk(PhysicalType type, int index, int bitWidth = 2)
    {
        byte[] dictionary = type == PhysicalType.Int32
            ? Plain(10, 20, 30)
            : [.. Bytes("a"), .. Bytes("bb"), .. Bytes("ccc")];
        // The bit width, then one RLE run: its header is the count shifted left, then the value.
        byte[] data = [(byte)bitWidth, 4 << 1, .. new byte[(bitWidth + 7) / 8]];
        data[2] = (byte)index;

        return Chunk(type, CompressionCodec.Uncompressed, numValues: 4,
            (new PageHeader
            {
                Type = PageType.DictionaryPage,
                UncompressedPageSize = dictionary.Length,
                CompressedPageSize = dictionary.Length,
                DictionaryPageHeader = new DictionaryPageHeader { NumValues = 3, Encoding = Encoding.Plain },
            }, dictionary),
            (DataPage(4, Encoding.RleDictionary, data.Length), data));

        static byte[] Bytes(string s)
        {
            byte[] b = new byte[4 + s.Length];
            BinaryPrimitives.WriteInt32LittleEndian(b, s.Length);
            System.Text.Encoding.ASCII.GetBytes(s).CopyTo(b, 4);
            return b;
        }
    }

    /// <summary>The PLAIN values 1, 2, 3, 4, Snappy-compressed, in a page declaring <paramref name="declaredUncompressed"/>.</summary>
    private static TestChunk PlainSnappyChunk(int declaredUncompressed, long? chunkUncompressed = null) =>
        PlainCompressedChunk(CompressionCodec.Snappy, declaredUncompressed, chunkUncompressed);

    /// <summary>
    /// A page of four values whose data is the PLAIN <paramref name="values"/> (by default 1, 2, 3, 4),
    /// compressed, declaring <paramref name="declaredUncompressed"/>.
    /// </summary>
    private static TestChunk PlainCompressedChunk(
        CompressionCodec codec, int declaredUncompressed, long? chunkUncompressed = null, int[]? values = null)
    {
        byte[] plain = Plain(values ?? [1, 2, 3, 4]);
        byte[] compressed = new byte[Compressor.GetMaxCompressedLength(codec, plain.Length)];
        compressed = compressed.AsSpan(0, Compressor.Compress(codec, plain, compressed)).ToArray();

        var chunk = Chunk(PhysicalType.Int32, codec, numValues: 4,
            (DataPage(4, Encoding.Plain, compressed.Length, declaredUncompressed), compressed));
        return chunkUncompressed is { } total
            ? chunk with { Meta = Copy(chunk.Meta, total) }
            : chunk;
    }

    private static TestChunk Chunk(
        PhysicalType type, CompressionCodec codec, long numValues, params (PageHeader Header, byte[] Payload)[] pages) =>
        Chunk(type, codec, numValues, typeLength: null, pages);

    private static TestChunk Chunk(
        PhysicalType type, CompressionCodec codec, long numValues, int? typeLength,
        params (PageHeader Header, byte[] Payload)[] pages) =>
        Chunk(type, codec, numValues, typeLength, nullable: false, pages);

    private static TestChunk Chunk(
        PhysicalType type, CompressionCodec codec, long numValues, int? typeLength, bool nullable,
        params (PageHeader Header, byte[] Payload)[] pages)
    {
        var bytes = new List<byte>();
        long uncompressed = 0;
        foreach (var (header, payload) in pages)
        {
            byte[] encoded = MetadataEncoder.EncodePageHeader(header);
            bytes.AddRange(encoded);
            bytes.AddRange(payload);
            uncompressed += encoded.Length + header.UncompressedPageSize;
        }

        var column = new SchemaDescriptor(
        [
            new SchemaElement { Name = "schema", NumChildren = 1 },
            new SchemaElement
            {
                Name = "n", Type = type, TypeLength = typeLength,
                RepetitionType = nullable ? FieldRepetitionType.Optional : FieldRepetitionType.Required,
            },
        ]).Columns[0];
        var meta = new ColumnMetaData
        {
            Type = type,
            Encodings = [Encoding.Plain],
            PathInSchema = ["n"],
            Codec = codec,
            NumValues = numValues,
            TotalUncompressedSize = uncompressed,
            TotalCompressedSize = bytes.Count,
            DataPageOffset = 0,
        };
        IArrowType arrowType = type switch
        {
            PhysicalType.Int32 => Int32Type.Default,
            PhysicalType.Int64 => Int64Type.Default,
            PhysicalType.Float => FloatType.Default,
            PhysicalType.Double => DoubleType.Default,
            PhysicalType.FixedLenByteArray => new FixedSizeBinaryType(typeLength!.Value),
            _ => BinaryType.Default,
        };
        return new TestChunk([.. bytes], column, meta, new Field("n", arrowType, nullable));
    }

    private static PageHeader DataPage(int values, Encoding encoding, int size, int? uncompressed = null) => new()
    {
        Type = PageType.DataPage,
        UncompressedPageSize = uncompressed ?? size,
        CompressedPageSize = size,
        DataPageHeader = new DataPageHeader
        {
            NumValues = values,
            Encoding = encoding,
            DefinitionLevelEncoding = Encoding.Rle,
            RepetitionLevelEncoding = Encoding.Rle,
        },
    };

    /// <summary>Re-encodes the chunk's first (only) data page header with another uncompressed size.</summary>
    private static byte[] Rewrite(byte[] chunk, int uncompressed)
    {
        var header = PageHeaderDecoder.Decode(chunk, out int headerSize);
        byte[] encoded = MetadataEncoder.EncodePageHeader(new PageHeader
        {
            Type = header.Type,
            UncompressedPageSize = uncompressed,
            CompressedPageSize = header.CompressedPageSize,
            DataPageHeader = header.DataPageHeader,
        });
        return [.. encoded, .. chunk.AsSpan(headerSize)];
    }

    private static ColumnMetaData Copy(ColumnMetaData m, long totalUncompressed, CompressionCodec? codec = null) => new()
    {
        Type = m.Type,
        Encodings = m.Encodings,
        PathInSchema = m.PathInSchema,
        Codec = codec ?? m.Codec,
        NumValues = m.NumValues,
        TotalUncompressedSize = totalUncompressed,
        TotalCompressedSize = m.TotalCompressedSize,
        DataPageOffset = m.DataPageOffset,
    };

    private static byte[] Plain(params int[] values)
    {
        byte[] bytes = new byte[values.Length * 4];
        for (int i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4), values[i]);
        return bytes;
    }

    /// <summary>
    /// Each read path, as a function of the limit, returning the values read: the whole row group,
    /// batches over a header-scanned page map, and row ranges over an OffsetIndex page map.
    /// </summary>
    private static readonly Func<string, int?, Task<string>>[] ReadPaths =
    [
        async (path, limit) =>
        {
            await using var reader = Open(path, limit);
            return Render(await reader.ReadRowGroupAsync(0));
        },
        async (path, limit) =>
        {
            await using var reader = Open(path, limit, batchSize: 300);
            var rendered = new List<string>();
            await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
                rendered.Add(Render(batch));
            return string.Join("|", rendered);
        },
        async (path, limit) =>
        {
            await using var reader = Open(path, limit, batchSize: 300);
            var rendered = new List<string>();
            await foreach (var batch in reader.ReadRowRangesAsync(0, [new RowRange(100, 1_900)]))
                rendered.Add(Render(batch));
            return string.Join("|", rendered);
        },
    ];

    private static ParquetFileReader Open(string path, int? limit, int? batchSize = null) =>
        new(new LocalRandomAccessFile(path), ownsFile: true, ParquetReadOptions.Default with
        {
            MaxPageUncompressedSize = limit,
            BatchSize = batchSize,
        });

    private static string Render(RecordBatch batch) => string.Join(",", Enumerable.Range(0, batch.Length)
        .Select(r => string.Join(":", Enumerable.Range(0, batch.ColumnCount).Select(c => ArrowValues.Render(batch.Column(c), r)))));

    /// <summary>A Snappy-compressed file of a dictionary-encoded and a plain column, in small pages.</summary>
    private async Task<string> WriteFileAsync()
    {
        const int rows = 2_000;
        var id = new Int64Array.Builder();
        var n = new Int32Array.Builder();
        for (int r = 0; r < rows; r++)
        {
            id.Append(r);
            if (r % 7 == 0) n.AppendNull(); else n.Append(r % 13);
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("n", Int32Type.Default, true))
            .Build();

        string path = Path.Combine(_tempDir, "limit.parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            Compression = CompressionCodec.Snappy,
            DataPageSize = 1024,
        }))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [id.Build(), n.Build()], rows));
            await writer.CloseAsync();
        }
        return path;
    }

    /// <summary>The largest uncompressed size any page of the file declares.</summary>
    private static async Task<int> LargestPageAsync(string path)
    {
        await using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        var metadata = await reader.ReadMetadataAsync();
        var columns = new SchemaDescriptor(metadata.Schema).Columns;
        byte[] bytes = File.ReadAllBytes(path);
        int largest = 0;
        for (int c = 0; c < columns.Count; c++)
        {
            var meta = metadata.RowGroups[0].Columns[c].MetaData!;
            long start = meta.DictionaryPageOffset is > 0 and long dpo ? dpo : meta.DataPageOffset;
            var pages = new PageReader(bytes.AsSpan(checked((int)start), checked((int)meta.TotalCompressedSize)), columns[c]);
            while (pages.TryRead(out var page))
                largest = Math.Max(largest, page.Header.UncompressedPageSize);
        }
        return largest;
    }
}
