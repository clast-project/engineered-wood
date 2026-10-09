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

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A page header that decodes as Thrift but holds a count or size no page can have is a
/// <see cref="ParquetFormatException"/>. The decoders used to trust these values: a negative value
/// count reached a <c>stackalloc</c> and overflowed the stack, killing the process, and others
/// surfaced as <see cref="ArgumentOutOfRangeException"/> or <see cref="OverflowException"/> from
/// deep in a decoder. Each case here rewrites one field of a real header written by EW.
/// </summary>
public class PageHeaderValidationTests : IDisposable
{
    private readonly string _tempDir;

    public PageHeaderValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-header-validation-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<string> V1Cases() => new() { "negative values", "no data_page_header", "negative uncompressed size" };

    public static TheoryData<string> V2Cases() => new()
    {
        "negative values", "negative nulls", "more nulls than values",
        "negative repetition length", "negative definition length", "levels longer than the page",
        "non-null values but no value bytes",
        "no data_page_header_v2",
    };

    /// <summary>Row counts are checked only where they are read: in a repeated column's pages.</summary>
    public static TheoryData<string> V2RepeatedCases() => new() { "negative rows", "more rows than values" };

    public static TheoryData<string> DictionaryCases() => new() { "negative entries", "no dictionary_page_header" };

    [Theory]
    [MemberData(nameof(V1Cases))]
    public async Task V1DataPage(string defect)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V1, dictionary: false, column: "n");
        AssertRefused(chunk, Tamper(chunk, PageType.DataPage, defect));
    }

    [Theory]
    [MemberData(nameof(V2Cases))]
    public async Task V2DataPage(string defect)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: false, column: "n");
        AssertRefused(chunk, Tamper(chunk, PageType.DataPageV2, defect));
    }

    [Theory]
    [MemberData(nameof(V2RepeatedCases))]
    public async Task V2DataPageOfARepeatedColumn(string defect)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: false, column: "xs");
        AssertRefused(chunk, Tamper(chunk, PageType.DataPageV2, defect));
    }

    /// <summary>
    /// A flat column has a value slot per row, so a V2 page's num_rows is redundant there. pyarrow,
    /// DuckDB and DataFusion all read a flat page whose num_rows is wrong, and so did EW's whole-chunk
    /// read; its batched read refused it. Both now take the rows from the values.
    /// </summary>
    [Theory]
    [InlineData(-10)]
    [InlineData(10)]
    public async Task AFlatV2PageWithAWrongRowCount_ReadsItsValues(int delta)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: false, column: "n");
        byte[] tampered = RewriteDataPage(chunk.Bytes, ordinal: 2, h =>
            Copy(h, v2: Copy(h.DataPageHeaderV2!, numRows: h.DataPageHeaderV2!.NumRows + delta)));

        var expected = (Int32Array)ReadColumn(chunk, chunk.Bytes).Array;
        var whole = (Int32Array)ReadColumn(chunk, tampered).Array;
        Assert.Equal(Values(expected), Values(whole));

        var map = PageMapBuilder.Build(tampered, chunk.Column, chunk.Meta);
        Assert.Equal(chunk.RowCount, map.TotalRows);
        Assert.Equal(map.CumulativeValues, map.CumulativeRows);

        static List<int?> Values(Int32Array a) => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? null : a.GetValue(i)).ToList();
    }

    /// <summary>
    /// A V2 page whose num_nulls disagrees with its definition levels. Its values were decoded by the
    /// header's count but placed by the levels, so a page claiming too few nulls decoded more values
    /// than its data holds, and one claiming all of them decoded none and read unwritten memory. The
    /// levels decide now, as in arrow-rs (#426).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AV2PageWhoseNullCountDisagreesWithItsLevels_ReadsItsValues(bool allNull)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: false, column: "n");
        byte[] tampered = RewriteDataPage(chunk.Bytes, ordinal: 2, h =>
            Copy(h, v2: Copy(h.DataPageHeaderV2!, numNulls: allNull ? h.DataPageHeaderV2!.NumValues : 0)));

        var expected = Values(ReadColumn(chunk, chunk.Bytes).Array);
        Assert.Equal(expected, Values(ReadColumn(chunk, tampered).Array));

        var map = PageMapBuilder.Build(tampered, chunk.Column, chunk.Meta);
        var batched = ColumnChunkReader.ReadColumnBatchFromSlice(
            tampered, 0, chunk.Column, chunk.Meta, map, 0, map.Pages.Length - 1,
            new Field("n", Int32Type.Default, nullable: true));
        Assert.Equal(expected, Values(batched.Array));

        static List<int?> Values(IArrowArray array)
        {
            var a = (Int32Array)array;
            return Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? null : a.GetValue(i)).ToList();
        }
    }

    [Theory]
    [MemberData(nameof(DictionaryCases))]
    public async Task DictionaryPage(string defect)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: true, column: "n");
        AssertRefused(chunk, Tamper(chunk, PageType.DictionaryPage, defect));
    }

    /// <summary>
    /// The case that overflowed the stack: a V2 page of a flat column with repetition-level bytes
    /// and a negative value count. The test host dies here without the check.
    /// </summary>
    [Fact]
    public async Task NegativeValuesWithRepetitionBytesOnAFlatColumn_IsRefusedNotAStackOverflow()
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: false, column: "n");
        byte[] tampered = RewriteFirst(chunk.Bytes, PageType.DataPageV2, h => Copy(h, v2: Copy(h.DataPageHeaderV2!, numValues: -5, repetitionLength: 1)));

        var ex = Assert.Throws<ParquetFormatException>(() => ReadColumn(chunk, tampered));
        Assert.Contains("is malformed: it holds -5 values", ex.Message);
    }

    /// <summary>A page may not hold more values than the chunk's metadata left room for.</summary>
    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task APageWithMoreValuesThanTheChunk_IsRefused(DataPageVersion version)
    {
        var chunk = await FirstChunkAsync(version, dictionary: false, column: "n");
        int rows = chunk.RowCount;
        byte[] tampered = RewriteFirst(chunk.Bytes, version == DataPageVersion.V1 ? PageType.DataPage : PageType.DataPageV2, h =>
            version == DataPageVersion.V1
                ? Copy(h, v1: Copy(h.DataPageHeader!, numValues: rows + 1))
                : Copy(h, v2: Copy(h.DataPageHeaderV2!, numValues: rows + 1, numRows: rows + 1)));

        var ex = Assert.Throws<ParquetFormatException>(() => ReadColumn(chunk, tampered));
        Assert.Contains($"holds {rows + 1} values, but the chunk's metadata leaves room for only {rows} more", ex.Message);
    }

    /// <summary>
    /// The batched read's header-scanned page map sizes its buffers from the pages, so it must refuse
    /// the same over-full page the whole read does. It used to pass it on to the level decoder.
    /// </summary>
    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task ALaterPageWithMoreValuesThanTheChunk_IsRefusedByThePageMapToo(DataPageVersion version)
    {
        var chunk = await FirstChunkAsync(version, dictionary: false, column: "n");
        byte[] tampered = RewriteDataPage(chunk.Bytes, ordinal: 3, h => h.Type == PageType.DataPage
            ? Copy(h, v1: Copy(h.DataPageHeader!, numValues: h.DataPageHeader!.NumValues + chunk.RowCount))
            : Copy(h, v2: Copy(h.DataPageHeaderV2!, numValues: h.DataPageHeaderV2!.NumValues + chunk.RowCount,
                numRows: h.DataPageHeaderV2.NumRows + chunk.RowCount)));

        var whole = Assert.Throws<ParquetFormatException>(() => ReadColumn(chunk, tampered));
        var mapped = Assert.Throws<ParquetFormatException>(() => PageMapBuilder.Build(tampered, chunk.Column, chunk.Meta));
        Assert.Contains("data page 3", mapped.Message);
        Assert.Contains("leaves room for only", mapped.Message);
        Assert.Equal(whole.Message, mapped.Message);
    }

    /// <summary>A chunk whose pages hold fewer values than its metadata is refused by the map as by the whole read.</summary>
    [Fact]
    public async Task AChunkMissingItsLastPage_IsRefusedByThePageMap()
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: false, column: "n");
        var header = PageHeaderDecoder.Decode(chunk.Bytes, out int headerSize);
        byte[] firstPageOnly = chunk.Bytes.AsSpan(0, headerSize + header.CompressedPageSize).ToArray();

        var ex = Assert.Throws<ParquetFormatException>(() => PageMapBuilder.Build(firstPageOnly, chunk.Column, chunk.Meta));
        Assert.Contains($"expected {chunk.Meta.NumValues} values but only read {header.DataPageHeaderV2!.NumValues}", ex.Message);
    }

    /// <summary>
    /// Thrift a header cannot be. Each used to escape the decoder as something other than a
    /// <see cref="ParquetFormatException"/>, or to read as a zero that misaligned every later page.
    /// </summary>
    [Theory]
    [InlineData("varint cut off by the end of the bytes", new byte[] { 0x15, 0x80 }, "Unexpected end of Thrift data reading a varint")]
    [InlineData("varint longer than 10 bytes", new byte[] { 0x15, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x81, 0x01 }, "longer than 10 bytes")]
    [InlineData("tenth varint byte past bit 63", new byte[] { 0x15, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02, 0x00 }, "overflows 64 bits")]
    [InlineData("i32 out of range", new byte[] { 0x15, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0x00 }, "out of range")]
    [InlineData("no compressed_page_size", new byte[] { 0x15, 0x00, 0x15, 0x02, 0x00 }, "missing required 'compressed_page_size'")]
    [InlineData("no uncompressed_page_size", new byte[] { 0x15, 0x00, 0x25, 0x02, 0x00 }, "missing required 'uncompressed_page_size'")]
    [InlineData("data page with no num_values", new byte[] { 0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x2C, 0x25, 0x00, 0x00, 0x00 }, "DataPageHeader is missing required 'num_values'")]
    [InlineData("dictionary page with no num_values", new byte[] { 0x15, 0x04, 0x15, 0x00, 0x15, 0x00, 0x4C, 0x25, 0x00, 0x00, 0x00 }, "DictionaryPageHeader is missing required 'num_values'")]
    [InlineData("list claiming more elements than bytes", new byte[] { 0x15, 0x00, 0x15, 0x02, 0x15, 0x02, 0x09, 0x28, 0xF1, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x00 }, "Thrift list claims 2147483647 elements")]
    [InlineData("map claiming more entries than bytes", new byte[] { 0x15, 0x00, 0x15, 0x02, 0x15, 0x02, 0x0B, 0x28, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x55, 0x00 }, "Thrift map claims 2147483647 entries")]
    [InlineData("double cut off by the end of the bytes", new byte[] { 0x15, 0x00, 0x15, 0x02, 0x15, 0x02, 0x07, 0x28, 0x00, 0x00 }, "skipping a double")]
    [InlineData("data page with no encoding", new byte[] { 0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x2C, 0x15, 0x02, 0x00, 0x00 }, "DataPageHeader is missing required 'encoding'")]
    [InlineData("data page with no level encodings", new byte[] { 0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x2C, 0x15, 0x02, 0x15, 0x00, 0x00, 0x00 }, "DataPageHeader is missing required 'definition_level_encoding'")]
    [InlineData("V2 data page with no encoding", new byte[] { 0x15, 0x06, 0x15, 0x00, 0x15, 0x00, 0x5C, 0x15, 0x02, 0x15, 0x00, 0x15, 0x02, 0x25, 0x00, 0x15, 0x00, 0x00, 0x00 }, "DataPageHeaderV2 is missing required 'encoding'")]
    [InlineData("dictionary page with no encoding", new byte[] { 0x15, 0x04, 0x15, 0x00, 0x15, 0x00, 0x4C, 0x15, 0x02, 0x00, 0x00 }, "DictionaryPageHeader is missing required 'encoding'")]
    public void MalformedThrift_IsAFormatError(string label, byte[] bytes, string expected)
    {
        _ = label;
        var column = new SchemaDescriptor(
        [
            new SchemaElement { Name = "schema", NumChildren = 1 },
            new SchemaElement { Name = "n", Type = PhysicalType.Int32, RepetitionType = FieldRepetitionType.Optional },
        ]).Columns[0];

        var ex = Assert.Throws<ParquetFormatException>(() =>
        {
            var reader = new PageReader(bytes, column);
            reader.TryRead(out _);
        });
        Assert.Contains("corrupted page header", ex.Message);
        Assert.Contains(expected, ex.InnerException!.Message);
    }

    /// <summary>
    /// A dictionary's arrays are sized from its header's count before the data is read, so a small
    /// page declaring int.MaxValue entries must be refused rather than allocate gigabytes.
    /// </summary>
    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(1_000_000)]
    public async Task ADictionaryDeclaringMoreEntriesThanItsDataHolds_IsRefused(int entries)
    {
        var chunk = await FirstChunkAsync(DataPageVersion.V2, dictionary: true, column: "n");
        byte[] tampered = RewriteFirst(chunk.Bytes, PageType.DictionaryPage, h =>
            Copy(h, dictionary: new DictionaryPageHeader { NumValues = entries, Encoding = h.DictionaryPageHeader!.Encoding }));

#if NET
        long before = GC.GetAllocatedBytesForCurrentThread();
#endif
        var ex = Assert.Throws<ParquetFormatException>(() => ReadColumn(chunk, tampered));
        Assert.Contains($"declares {entries} entries", ex.Message);
#if NET
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 1_000_000, "the refusal allocated as if the count were real");
#endif
    }

    /// <summary>
    /// An unknown field is skipped. A bool inside a list is a byte of its own, unlike a bool field,
    /// so skipping a <c>list&lt;bool&gt;</c> must consume one byte per element or the fields after
    /// it are misread.
    /// </summary>
    [Fact]
    public void AnUnknownListOfBools_IsSkippedWithoutMisaligningTheHeader()
    {
        byte[] bytes =
        [
            0x15, 0x00,                         // 1: type = DATA_PAGE
            0x15, 0x02,                         // 2: uncompressed_page_size = 1
            0x09, 0x28, 0x31, 0x01, 0x02, 0x01, // 20: list<bool> [true, false, true], unknown to the reader
            0x05, 0x06, 0x02,                   // 3: compressed_page_size = 1 (long-form field header)
            0x0C, 0x0A,                         // 5: data_page_header (long-form field header)
            0x15, 0x02, 0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x00, // num_values = 1, three encodings = PLAIN
            0x00,                               // stop
            0x2A,                               // the page's one byte
        ];

        var column = new SchemaDescriptor(
        [
            new SchemaElement { Name = "schema", NumChildren = 1 },
            new SchemaElement { Name = "n", Type = PhysicalType.Int32, RepetitionType = FieldRepetitionType.Optional },
        ]).Columns[0];
        var reader = new PageReader(bytes, column);
        Assert.True(reader.TryRead(out var page));
        Assert.Equal(PageType.DataPage, page.Header.Type);
        Assert.Equal(1, page.Header.CompressedPageSize);
        Assert.Equal(1, page.NumValues);
        Assert.Equal(new byte[] { 0x2A }, page.Payload.ToArray());
        Assert.False(reader.TryRead(out _));
    }

    [Fact]
    public async Task UntamperedChunks_Read()
    {
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { false, true })
        {
            var chunk = await FirstChunkAsync(version, dictionary, column: "n");
            Assert.Equal(chunk.RowCount, ReadColumn(chunk, chunk.Bytes).Array.Length);
        }
    }

    // ───── Helpers ─────

    private sealed record Chunk(byte[] Bytes, ColumnDescriptor Column, ColumnMetaData Meta, int RowCount);

    private static void AssertRefused(Chunk chunk, byte[] tampered)
    {
        var ex = Assert.Throws<ParquetFormatException>(() =>
        {
            var reader = new PageReader(tampered, chunk.Column);
            while (reader.TryRead(out _)) { }
        });
        Assert.Contains($"Column '{chunk.Column.DottedPath}': the ", ex.Message);
        Assert.Contains("is malformed", ex.Message);

        // And through a whole-column read, the path that used to crash or throw from a decoder.
        Assert.Throws<ParquetFormatException>(() => ReadColumn(chunk, tampered));
    }

    private static ColumnResult ReadColumn(Chunk chunk, byte[] bytes) =>
        ColumnChunkReader.ReadColumn(
            bytes, chunk.Column, chunk.Meta, chunk.RowCount, new Field(chunk.Column.Path[^1], Int32Type.Default, nullable: true));

    private static byte[] Tamper(Chunk chunk, PageType type, string defect) => RewriteFirst(chunk.Bytes, type, h => defect switch
    {
        "negative values" when type == PageType.DataPage => Copy(h, v1: Copy(h.DataPageHeader!, numValues: -1)),
        "no data_page_header" => Copy(h, dropSubHeader: true),
        "negative uncompressed size" => Copy(h, uncompressedSize: -1),

        "negative values" => Copy(h, v2: Copy(h.DataPageHeaderV2!, numValues: -1)),
        "negative nulls" => Copy(h, v2: Copy(h.DataPageHeaderV2!, numNulls: -1)),
        "more nulls than values" => Copy(h, v2: Copy(h.DataPageHeaderV2!, numNulls: h.DataPageHeaderV2!.NumValues + 1)),
        "negative rows" => Copy(h, v2: Copy(h.DataPageHeaderV2!, numRows: -1)),
        "more rows than values" => Copy(h, v2: Copy(h.DataPageHeaderV2!, numRows: h.DataPageHeaderV2!.NumValues + 1)),
        "negative repetition length" => Copy(h, v2: Copy(h.DataPageHeaderV2!, repetitionLength: -1)),
        "negative definition length" => Copy(h, v2: Copy(h.DataPageHeaderV2!, definitionLength: -1)),
        "levels longer than the page" => Copy(h, v2: Copy(h.DataPageHeaderV2!, definitionLength: h.CompressedPageSize + 1)),
        // The definition levels now claim the whole page; RLE stops at its own count, so the levels
        // still decode, and before the check the page read as garbage values from an unwritten buffer.
        "non-null values but no value bytes" => Copy(h, v2: Copy(h.DataPageHeaderV2!, definitionLength: h.CompressedPageSize)),
        "no data_page_header_v2" => Copy(h, dropSubHeader: true),

        "negative entries" => Copy(h, dictionary: new DictionaryPageHeader { NumValues = -1, Encoding = h.DictionaryPageHeader!.Encoding }),
        "no dictionary_page_header" => Copy(h, dropSubHeader: true),

        _ => throw new ArgumentOutOfRangeException(nameof(defect), defect, null),
    });

    /// <summary>Re-encodes the header of data page <paramref name="ordinal"/> in <paramref name="chunk"/>.</summary>
    private static byte[] RewriteDataPage(byte[] chunk, int ordinal, Func<PageHeader, PageHeader> rewrite)
    {
        int position = 0, seen = 0;
        while (true)
        {
            var header = PageHeaderDecoder.Decode(chunk.AsSpan(position), out int headerSize);
            if (header.Type is PageType.DataPage or PageType.DataPageV2 && seen++ == ordinal)
            {
                byte[] encoded = MetadataEncoder.EncodePageHeader(rewrite(header));
                return [.. chunk.AsSpan(0, position), .. encoded, .. chunk.AsSpan(position + headerSize)];
            }

            position += headerSize + header.CompressedPageSize;
        }
    }

    /// <summary>
    /// Re-encodes the header of the first page of <paramref name="type"/> in <paramref name="chunk"/>.
    /// The new header may differ in length, so the chunk is rebuilt around it.
    /// </summary>
    private static byte[] RewriteFirst(byte[] chunk, PageType type, Func<PageHeader, PageHeader> rewrite)
    {
        int position = 0;
        while (true)
        {
            var header = PageHeaderDecoder.Decode(chunk.AsSpan(position), out int headerSize);
            int end = position + headerSize + header.CompressedPageSize;
            if (header.Type == type)
            {
                byte[] encoded = MetadataEncoder.EncodePageHeader(rewrite(header));
                return [.. chunk.AsSpan(0, position), .. encoded, .. chunk.AsSpan(position + headerSize)];
            }

            position = end;
        }
    }

    private static PageHeader Copy(
        PageHeader h,
        DataPageHeader? v1 = null,
        DataPageHeaderV2? v2 = null,
        DictionaryPageHeader? dictionary = null,
        int? uncompressedSize = null,
        bool dropSubHeader = false) => new()
    {
        Type = h.Type,
        UncompressedPageSize = uncompressedSize ?? h.UncompressedPageSize,
        CompressedPageSize = h.CompressedPageSize,
        Crc = h.Crc,
        DataPageHeader = dropSubHeader ? null : v1 ?? h.DataPageHeader,
        DataPageHeaderV2 = dropSubHeader ? null : v2 ?? h.DataPageHeaderV2,
        DictionaryPageHeader = dropSubHeader ? null : dictionary ?? h.DictionaryPageHeader,
        SymbolTablePageHeader = dropSubHeader ? null : h.SymbolTablePageHeader,
    };

    private static DataPageHeader Copy(DataPageHeader h, int numValues) => new()
    {
        NumValues = numValues,
        Encoding = h.Encoding,
        DefinitionLevelEncoding = h.DefinitionLevelEncoding,
        RepetitionLevelEncoding = h.RepetitionLevelEncoding,
    };

    private static DataPageHeaderV2 Copy(
        DataPageHeaderV2 h,
        int? numValues = null,
        int? numNulls = null,
        int? numRows = null,
        int? repetitionLength = null,
        int? definitionLength = null) => new()
    {
        NumValues = numValues ?? h.NumValues,
        NumNulls = numNulls ?? h.NumNulls,
        NumRows = numRows ?? h.NumRows,
        Encoding = h.Encoding,
        RepetitionLevelsByteLength = repetitionLength ?? h.RepetitionLevelsByteLength,
        DefinitionLevelsByteLength = definitionLength ?? h.DefinitionLevelsByteLength,
        IsCompressed = h.IsCompressed,
    };

    /// <summary>Writes a small file and returns the chunk of <paramref name="column"/> in its only row group.</summary>
    private async Task<Chunk> FirstChunkAsync(DataPageVersion version, bool dictionary, string column)
    {
        const int rows = 2_000;
        var id = new Int64Array.Builder();
        var n = new Int32Array.Builder();
        var xs = new ListArray.Builder(Int32Type.Default);
        var elements = (Int32Array.Builder)xs.ValueBuilder;
        for (int r = 0; r < rows; r++)
        {
            id.Append(r);
            if (r % 7 == 0) n.AppendNull(); else n.Append(r % 13);
            xs.Append();
            for (int e = 0; e < r % 3; e++)
                elements.Append(r + e);
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("xs", new ListType(Int32Type.Default), true))
            .Build();

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            Compression = CompressionCodec.Uncompressed,
            DataPageSize = 1024,
        }))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [id.Build(), n.Build(), xs.Build()], rows));
            await writer.CloseAsync();
        }

        byte[] bytes = File.ReadAllBytes(path);
        int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        var metadata = MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - length, length));
        var descriptors = new SchemaDescriptor(metadata.Schema);
        int c = descriptors.Columns.ToList().FindIndex(d => d.Path[0] == column);
        var meta = metadata.RowGroups[0].Columns[c].MetaData!;
        long start = meta.DictionaryPageOffset is > 0 and long dpo ? dpo : meta.DataPageOffset;
        byte[] chunk = bytes.AsSpan(checked((int)start), checked((int)meta.TotalCompressedSize)).ToArray();
        return new Chunk(chunk, descriptors.Columns[c], meta, rows);
    }
}
