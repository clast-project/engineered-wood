// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet.Metadata;

/// <summary>
/// <c>ColumnMetaData.encoding_stats</c> (field 13): the per-page-type encoding counts that are the only
/// footer evidence a chunk never left its dictionary.
/// </summary>
/// <remarks>
/// The oracles are the files, not EW's own codec. For a fixture, the decoded counts must equal a tally of
/// the chunk's page headers, and the list's bytes, as a separate reference encoder spells them, must
/// appear in the footer as the fixture's writer wrote it and as EW re-encodes it. For EW's writer, the
/// counts it records must equal the tally of the pages it wrote.
/// </remarks>
public class EncodingStatsTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;

    public EncodingStatsTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-encstats-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── Fixtures ─────

    /// <summary>Every parquet-testing fixture with at least one chunk that records encoding_stats.</summary>
    public static TheoryData<string> FixturesWithEncodingStats()
    {
        var data = new TheoryData<string>();
        foreach (string path in TestData.GetAllParquetFiles().OrderBy(p => p, StringComparer.Ordinal))
        {
            FileMetaData metadata;
            try
            {
                metadata = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes(Path.GetFileName(path)));
            }
            catch (Exception)
            {
                continue; // encrypted footers and deliberately malformed fixtures
            }

            if (metadata.RowGroups.Any(rg => rg.Columns.Any(c => c.MetaData?.EncodingStats is not null)))
                data.Add(Path.GetFileName(path));
        }
        return data;
    }

    [Fact]
    public void Fixtures_IncludeSomeWithEncodingStats()
    {
        // parquet-mr and parquet-cpp both write the field, so a corpus without it means discovery broke.
        int count = FixturesWithEncodingStats().Count;
        _output.WriteLine($"{count} fixtures carry encoding_stats");
        Assert.True(count >= 10, $"only {count} fixtures found");
    }

    [Theory]
    [MemberData(nameof(FixturesWithEncodingStats))]
    public void Fixture_EncodingStatsMatchThePageHeaders(string fileName)
    {
        byte[] file = File.ReadAllBytes(TestData.GetPath(fileName));
        var metadata = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes(fileName));
        int chunks = 0;

        foreach (var chunk in metadata.RowGroups.SelectMany(rg => rg.Columns))
        {
            if (chunk.MetaData?.EncodingStats is not { } stats)
                continue;

            // parquet-cpp counts a V2 data page as DATA_PAGE (page_v2_empty_compressed), so the page
            // types are compared with the two data-page kinds folded together.
            Assert.Equal(Tally(WalkPages(file, chunk.MetaData), foldDataPages: true),
                Tally(stats, foldDataPages: true));
            chunks++;
        }

        _output.WriteLine($"{fileName}: {chunks} chunks ({metadata.CreatedBy})");
    }

    [Theory]
    [MemberData(nameof(FixturesWithEncodingStats))]
    public void Fixture_EncodingStatsBytesSurviveTheRoundTrip(string fileName)
    {
        byte[] footer = TestData.ReadFooterBytes(fileName);
        var metadata = MetadataDecoder.DecodeFileMetaData(footer);
        byte[] reencoded = MetadataEncoder.EncodeFileMetaData(metadata);
        var redecoded = MetadataDecoder.DecodeFileMetaData(reencoded);

        var original = metadata.RowGroups.SelectMany(rg => rg.Columns).ToList();
        var roundTripped = redecoded.RowGroups.SelectMany(rg => rg.Columns).ToList();
        for (int i = 0; i < original.Count; i++)
        {
            if (original[i].MetaData?.EncodingStats is not { } stats)
                continue;

            Assert.Equal(stats, roundTripped[i].MetaData!.EncodingStats);
            byte[] listBytes = ReferenceEncodeList(stats);
            Assert.True(Contains(footer, listBytes), "the writer's footer does not hold the reference bytes");
            Assert.True(Contains(reencoded, listBytes), "EW's re-encoded footer does not hold the reference bytes");
        }
    }

    /// <summary>
    /// The one fixture chunk that left its dictionary partway through. Its encoding list cannot say so,
    /// and its encoding_stats can: this is what dictionary pruning (#57) must decline.
    /// </summary>
    [Fact]
    public void Fixture_AChunkThatFellBack_IsVisibleOnlyInTheStats()
    {
        var dictionaryChunks = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes("large_string_map.brotli.parquet"))
            .RowGroups.SelectMany(rg => rg.Columns)
            .Select(c => c.MetaData!)
            .Where(m => m.EncodingStats!.Any(s => s.PageType == PageType.DictionaryPage))
            .ToList();
        static bool FellBack(ColumnMetaData m) =>
            m.EncodingStats!.Any(s => s.PageType == PageType.DataPage && s.Encoding == Encoding.Plain);

        var fellBack = Assert.Single(dictionaryChunks, FellBack);
        Assert.Contains(fellBack.EncodingStats!, s => s.PageType == PageType.DataPage && s.Encoding == Encoding.RleDictionary);

        // A sibling that never left its dictionary lists the same SET of encodings.
        var stayed = dictionaryChunks.First(m => !FellBack(m));
        Assert.Equal(new HashSet<Encoding>(stayed.Encodings), new HashSet<Encoding>(fellBack.Encodings));
    }

    // ───── Codec ─────

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(20)] // 15 or more entries takes the long-form list header
    public void RoundTrips(int entries)
    {
        var stats = Enumerable.Range(0, entries)
            .Select(i => new PageEncodingStats(
                i % 2 == 0 ? PageType.DataPage : PageType.DictionaryPage,
                (Encoding)(i % 9),
                i * 1_000_003))
            .ToArray();

        var decoded = MetadataDecoder.DecodeFileMetaData(MetadataEncoder.EncodeFileMetaData(OneChunk(stats)));

        Assert.Equal(stats, decoded.RowGroups[0].Columns[0].MetaData!.EncodingStats);
    }

    [Fact]
    public void Absent_StaysNull()
    {
        var decoded = MetadataDecoder.DecodeFileMetaData(MetadataEncoder.EncodeFileMetaData(OneChunk(null)));

        Assert.Null(decoded.RowGroups[0].Columns[0].MetaData!.EncodingStats);
    }

    /// <summary>
    /// An entry without its required <c>count</c> drops the WHOLE list: a reader may conclude from the
    /// list that no page of some kind exists, and an entry quietly missing would make that wrong.
    /// </summary>
    [Fact]
    public void AnIncompleteEntry_DropsTheWholeList()
    {
        var stats = new[]
        {
            new PageEncodingStats(PageType.DictionaryPage, Encoding.Plain, 1),
            new PageEncodingStats(PageType.DataPage, Encoding.Plain, 7),
        };
        byte[] footer = MetadataEncoder.EncodeFileMetaData(OneChunk(stats));

        // Strip field 3 (0x15, then the zigzag varint 14) from the second entry.
        byte[] good = ReferenceEncodeList(stats);
        byte[] broken = good.Take(good.Length - 3).Append((byte)0x00).ToArray();
        Assert.Equal(new byte[] { 0x15, 0x0E, 0x00 }, good.Skip(good.Length - 3).ToArray());
        byte[] tampered = Replace(footer, good, broken);

        var decoded = MetadataDecoder.DecodeFileMetaData(tampered);

        Assert.Null(decoded.RowGroups[0].Columns[0].MetaData!.EncodingStats);
    }

    [Fact]
    public void ANegativeCount_DropsTheWholeList()
    {
        var stats = new[]
        {
            new PageEncodingStats(PageType.DictionaryPage, Encoding.Plain, 1),
            new PageEncodingStats(PageType.DataPage, Encoding.Plain, -1),
        };

        var decoded = MetadataDecoder.DecodeFileMetaData(MetadataEncoder.EncodeFileMetaData(OneChunk(stats)));

        Assert.Null(decoded.RowGroups[0].Columns[0].MetaData!.EncodingStats);
    }

    /// <summary>
    /// Entries a reference writer would never produce, spliced into an otherwise valid footer in place of
    /// a one-entry list: each must drop the list rather than yield a guess.
    /// </summary>
    [Theory]
    // page_type twice (DataPage, then DictionaryPage via a long-form header: delta 0, zigzag id 1 = 0x02).
    [InlineData(new byte[] { 0x15, 0x00, 0x05, 0x02, 0x04, 0x15, 0x00, 0x15, 0x02, 0x00 })]
    // count as an i64 (type 6) rather than an i32.
    [InlineData(new byte[] { 0x15, 0x00, 0x15, 0x00, 0x16, 0x02, 0x00 })]
    // page_type as an i64, then encoding and count as i32s.
    [InlineData(new byte[] { 0x16, 0x00, 0x15, 0x00, 0x15, 0x02, 0x00 })]
    public void AMalformedEntry_DropsTheWholeList(byte[] entry)
    {
        var valid = new[] { new PageEncodingStats(PageType.DataPage, Encoding.Plain, 1) };
        byte[] footer = MetadataEncoder.EncodeFileMetaData(OneChunk(valid));

        byte[] list = new byte[] { 0x1C }.Concat(entry).ToArray(); // one struct element
        var decoded = MetadataDecoder.DecodeFileMetaData(Replace(footer, ReferenceEncodeList(valid), list));

        var metadata = decoded.RowGroups[0].Columns[0].MetaData!;
        Assert.Null(metadata.EncodingStats);
        Assert.Equal(4, metadata.DataPageOffset); // the rest of the chunk still decodes
    }

    // ───── EW's writer ─────

    public static TheoryData<bool, DataPageVersion, bool, bool> WriterCases()
    {
        var data = new TheoryData<bool, DataPageVersion, bool, bool>();
        foreach (bool buffered in new[] { false, true })
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool nullable in new[] { true, false })
            data.Add(buffered, version, dictionary, nullable);
        return data;
    }

    [Theory]
    [MemberData(nameof(WriterCases))]
    public async Task Writer_RecordsThePagesItWrote(
        bool buffered, DataPageVersion version, bool dictionary, bool nullable)
    {
        var options = new ParquetWriteOptions
        {
            Compression = CompressionCodec.Uncompressed,
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            DataPageSize = 1024, // several data pages per chunk
        };
        string path = await WriteFile(options, buffered, nullable);

        byte[] file = File.ReadAllBytes(path);
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();

        foreach (var chunk in metadata.RowGroups.SelectMany(rg => rg.Columns))
        {
            var stats = chunk.MetaData!.EncodingStats;
            Assert.NotNull(stats);
            var pages = WalkPages(file, chunk.MetaData);
            Assert.Equal(Tally(pages), Tally(stats!));

            var dataPageType = version == DataPageVersion.V2 ? PageType.DataPageV2 : PageType.DataPage;
            Assert.True(pages.Count(p => p.PageType == dataPageType) > 1, "expected several data pages");
            if (dictionary)
            {
                Assert.Contains(stats!, s => s.PageType == PageType.DictionaryPage && s.Count == 1);
                Assert.All(stats!.Where(s => s.PageType != PageType.DictionaryPage),
                    s => Assert.True(s.Encoding is Encoding.RleDictionary or Encoding.PlainDictionary, $"{s}"));
            }
            else
            {
                Assert.DoesNotContain(stats!, s => s.PageType == PageType.DictionaryPage);
            }
        }
    }

    private async Task<string> WriteFile(ParquetWriteOptions options, bool buffered, bool nullable)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, nullable))
            .Field(new Field("name", StringType.Default, nullable))
            .Build();

        const int rows = 5_000;
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            if (nullable && i % 7 == 0)
            {
                ids.AppendNull();
                names.AppendNull();
            }
            else
            {
                ids.Append(i % 50);
                names.Append("name-" + (i % 40));
            }
        }
        var batch = new RecordBatch(schema, [ids.Build(), names.Build()], rows);

        await using var file = new LocalSequentialFile(path);
        if (buffered)
        {
            await using var writer = new BufferedParquetWriter(file, ownsFile: false, options);
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }
        else
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }
        return path;
    }

    // ───── Helpers ─────

    private static FileMetaData OneChunk(IReadOnlyList<PageEncodingStats>? stats) => new()
    {
        Version = 2,
        Schema =
        [
            new SchemaElement { Name = "schema", NumChildren = 1 },
            new SchemaElement { Name = "id", Type = PhysicalType.Int32, RepetitionType = FieldRepetitionType.Required },
        ],
        NumRows = 1,
        RowGroups =
        [
            new RowGroup
            {
                Columns =
                [
                    new ColumnChunk
                    {
                        FileOffset = 4,
                        MetaData = new ColumnMetaData
                        {
                            Type = PhysicalType.Int32,
                            Encodings = [Encoding.Plain],
                            PathInSchema = ["id"],
                            Codec = CompressionCodec.Uncompressed,
                            NumValues = 1,
                            TotalUncompressedSize = 4,
                            TotalCompressedSize = 4,
                            DataPageOffset = 4,
                            EncodingStats = stats,
                        },
                    },
                ],
                TotalByteSize = 4,
                NumRows = 1,
            },
        ],
    };

    /// <summary>Every page of the chunk except an FSST symbol table, with the encoding its header names.</summary>
    private static List<PageEncodingStats> WalkPages(byte[] file, ColumnMetaData metadata)
    {
        // The smaller positive offset: a chunk of zero rows can hold only a dictionary page and record
        // data_page_offset as 0 (column_chunk_key_value_metadata), and some writers record a
        // dictionary_page_offset of 0 for a chunk with no dictionary.
        long start = new[] { metadata.DictionaryPageOffset ?? 0, metadata.DataPageOffset }.Where(o => o > 0).Min();
        long end = start + metadata.TotalCompressedSize;
        var pages = new List<PageEncodingStats>();

        for (long position = start; position < end;)
        {
            var header = PageHeaderDecoder.Decode(file.AsSpan(checked((int)position)), out int headerLength);
            Encoding? encoding = header.Type switch
            {
                PageType.DataPage => header.DataPageHeader!.Encoding,
                PageType.DataPageV2 => header.DataPageHeaderV2!.Encoding,
                PageType.DictionaryPage => header.DictionaryPageHeader!.Encoding,
                _ => null,
            };
            if (encoding is { } e)
                pages.Add(new PageEncodingStats(header.Type, e, 1));
            position += headerLength + header.CompressedPageSize;
        }

        return pages;
    }

    private static SortedDictionary<(PageType, Encoding), int> Tally(
        IEnumerable<PageEncodingStats> stats, bool foldDataPages = false)
    {
        var tally = new SortedDictionary<(PageType, Encoding), int>();
        foreach (var s in stats)
        {
            var type = foldDataPages && s.PageType == PageType.DataPageV2 ? PageType.DataPage : s.PageType;
            tally.TryGetValue((type, s.Encoding), out int n);
            tally[(type, s.Encoding)] = n + s.Count;
        }
        return tally;
    }

    /// <summary>
    /// The Thrift compact bytes of a <c>list&lt;PageEncodingStats&gt;</c>, spelled out here rather than
    /// taken from <see cref="MetadataEncoder"/>, so that matching them against a footer checks the encoder.
    /// </summary>
    private static byte[] ReferenceEncodeList(IReadOnlyList<PageEncodingStats> stats)
    {
        var bytes = new List<byte>();
        const byte structType = 0x0C;
        if (stats.Count < 15)
        {
            bytes.Add((byte)((stats.Count << 4) | structType));
        }
        else
        {
            bytes.Add(0xF0 | structType);
            Varint(bytes, (uint)stats.Count);
        }

        foreach (var s in stats)
        {
            foreach (int value in new[] { (int)s.PageType, (int)s.Encoding, s.Count })
            {
                bytes.Add(0x15); // field delta 1, type i32
                Varint(bytes, (uint)((value << 1) ^ (value >> 31)));
            }
            bytes.Add(0x00);
        }
        return bytes.ToArray();

        static void Varint(List<byte> output, uint value)
        {
            while (value >= 0x80)
            {
                output.Add((byte)(value | 0x80));
                value >>= 7;
            }
            output.Add((byte)value);
        }
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        for (int i = 0; i + needle.Length <= haystack.Length; i++)
        {
            if (haystack.AsSpan(i, needle.Length).SequenceEqual(needle))
                return i;
        }
        return -1;
    }

    private static bool Contains(byte[] haystack, byte[] needle) => IndexOf(haystack, needle) >= 0;

    private static byte[] Replace(byte[] haystack, byte[] needle, byte[] replacement)
    {
        int at = IndexOf(haystack, needle);
        Assert.True(at >= 0);
        return haystack.Take(at).Concat(replacement).Concat(haystack.Skip(at + needle.Length)).ToArray();
    }
}
