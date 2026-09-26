// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Tests.Parquet.Interop;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Every data page of a repeated leaf must begin at a record boundary (repetition level 0). V2 data
/// pages require it outright, and an OffsetIndex requires it of V1 pages too. The writer used to cut
/// pages every N level entries, so pages began in the middle of a row (#389).
/// </summary>
/// <remarks>
/// The assertion walks the page headers and decodes each page's repetition levels directly, rather
/// than round-tripping: every reader EngineeredWood tests against except arrow-rs reassembles a row
/// split across pages without complaint, so a round trip cannot see the defect.
/// </remarks>
public class RowAlignedPagesTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public RowAlignedPagesTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-rowaligned-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public static TheoryData<DataPageVersion, bool> PageVersionsAndDictionary() => new()
    {
        { DataPageVersion.V1, false },
        { DataPageVersion.V1, true },
        { DataPageVersion.V2, false },
        { DataPageVersion.V2, true },
    };

    /// <summary>The issue's repro: <c>list&lt;int64&gt;</c>, 50 rows of 1,000 elements, 4 KiB pages.</summary>
    [Theory]
    [MemberData(nameof(PageVersionsAndDictionary))]
    public async Task FixedLengthLists_PagesStartAtRowBoundaries(DataPageVersion version, bool dictionary)
    {
        var batch = BuildInt64Lists(rows: 50, length: _ => 1000, distinct: dictionary ? 40 : int.MaxValue);
        var pages = await WriteAndWalkAsync(batch, version, dictionary, dataPageSize: 4096, maxRepLevel: 1);

        AssertRowAligned(pages, version, expectedRows: 50);
        Assert.True(pages.Count > 1, "the case must actually produce several pages to test anything");
    }

    /// <summary>
    /// Variable lengths, with empty lists, null lists and null elements: each of those is a record
    /// of one level entry, so a cut point that only ever looked at list starts would miss them.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageVersionsAndDictionary))]
    public async Task RaggedNullableLists_PagesStartAtRowBoundaries(DataPageVersion version, bool dictionary)
    {
        const int rows = 2000;
        var rng = new Random(389);
        var lengths = new int[rows];
        for (int i = 0; i < rows; i++)
            lengths[i] = rng.Next(8) switch { 0 => -1, 1 => 0, _ => rng.Next(1, 200) };

        var batch = BuildInt64Lists(rows, i => lengths[i], distinct: dictionary ? 50 : int.MaxValue,
            nullElementEvery: 7);
        var pages = await WriteAndWalkAsync(batch, version, dictionary, dataPageSize: 1024, maxRepLevel: 1);

        AssertRowAligned(pages, version, expectedRows: rows);
        Assert.True(pages.Count > 1);
    }

    /// <summary>
    /// A record larger than the target page size gets a page of its own rather than being split,
    /// which is what the spec allows and what parquet-mr and arrow-rs do.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageVersionsAndDictionary))]
    public async Task OversizedRecord_IsNotSplit(DataPageVersion version, bool dictionary)
    {
        // Row 5 alone is roughly 20x the page size.
        var batch = BuildInt64Lists(rows: 12, length: i => i == 5 ? 10_000 : 3,
            distinct: dictionary ? 30 : int.MaxValue);
        var pages = await WriteAndWalkAsync(batch, version, dictionary, dataPageSize: 4096, maxRepLevel: 1);

        AssertRowAligned(pages, version, expectedRows: 12);
        Assert.Contains(pages, p => p.RepLevels.Length == 10_000 && p.RepLevels.Count(r => r == 0) == 1);
    }

    /// <summary>Two repetition levels: a cut at an inner-list boundary (rep = 1) is still mid-row.</summary>
    [Theory]
    [MemberData(nameof(PageVersionsAndDictionary))]
    public async Task NestedLists_PagesStartAtOuterRowBoundaries(DataPageVersion version, bool dictionary)
    {
        const int rows = 300;
        var innerType = new ListType(new Field("item", Int32Type.Default, nullable: true));
        var builder = new ListArray.Builder(innerType);
        var inner = (ListArray.Builder)builder.ValueBuilder;
        var leaf = (Int32Array.Builder)inner.ValueBuilder;
        int modulus = dictionary ? 25 : int.MaxValue;
        int next = 0;
        for (int r = 0; r < rows; r++)
        {
            builder.Append();
            for (int i = 0; i < 1 + r % 6; i++)
            {
                inner.Append();
                for (int j = 0; j < 40; j++)
                    leaf.Append(next++ % modulus);
            }
        }

        var array = builder.Build();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("nested", array.Data.DataType, nullable: true))
            .Build();
        var batch = new RecordBatch(schema, [array], rows);

        var pages = await WriteAndWalkAsync(batch, version, dictionary, dataPageSize: 2048, maxRepLevel: 2);

        AssertRowAligned(pages, version, expectedRows: rows);
        Assert.True(pages.Count > 1);
    }

    /// <summary>A map's key and value leaves are repeated too; both columns are walked.</summary>
    [Theory]
    [MemberData(nameof(PageVersionsAndDictionary))]
    public async Task Maps_PagesStartAtRowBoundaries(DataPageVersion version, bool dictionary)
    {
        const int rows = 400;
        var mapType = new MapType(StringType.Default, Int64Type.Default);
        var builder = new MapArray.Builder(mapType);
        var keys = (StringArray.Builder)builder.KeyBuilder;
        var values = (Int64Array.Builder)builder.ValueBuilder;
        long next = 0;
        for (int r = 0; r < rows; r++)
        {
            builder.Append();
            for (int i = 0; i < 1 + r % 50; i++)
            {
                keys.Append(dictionary ? $"k{i}" : $"key-{r}-{i}");
                values.Append(dictionary ? next++ % 30 : next++);
            }
        }

        var array = builder.Build();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("m", array.Data.DataType, nullable: true))
            .Build();
        var batch = new RecordBatch(schema, [array], rows);

        string path = await WriteAsync(batch, Options(version, dictionary, dataPageSize: 1024));
        await AssertRoundTripsAsync(path, batch);

        foreach (int column in new[] { 0, 1 })
        {
            var pages = WalkDataPages(path, column, maxRepLevel: 1);
            AssertRowAligned(pages, version, expectedRows: rows);
            Assert.True(pages.Count > 1);
        }
    }

    /// <summary>
    /// The default options at a realistic size, from the issue: 20 rows of 500,000 elements split
    /// 9 of 11 pages. Nothing in the option set has to be unusual for the defect to show; only the
    /// codec is turned off, because the walker needs to see the levels.
    /// </summary>
    [Fact]
    public async Task DefaultOptions_LargeLists_PagesStartAtRowBoundaries()
    {
        var batch = BuildInt64Lists(rows: 20, length: _ => 500_000, distinct: int.MaxValue);
        string path = await WriteAsync(batch,
            ParquetWriteOptions.Default with { Compression = CompressionCodec.Uncompressed });

        var pages = WalkDataPages(path, column: 0, maxRepLevel: 1);
        AssertRowAligned(pages, DataPageVersion.V2, expectedRows: 20);
    }

    /// <summary>
    /// <see cref="BufferedParquetWriter"/> shares the page loop, but it only ever buffers top-level
    /// primitive columns, so it has no repeated leaf to misalign. Pinned so that if it ever gains
    /// nested support, this test fails and the walker assertion above gets extended to it.
    /// </summary>
    [Fact]
    public async Task BufferedWriter_RefusesRepeatedColumns()
    {
        var batch = BuildInt64Lists(rows: 10, length: _ => 10, distinct: 5);
        string path = Path.Combine(_tempDir, "buffered.parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new BufferedParquetWriter(file, ownsFile: false);

        var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await writer.AppendAsync(batch);
            await writer.FlushRowGroupAsync();
        });
        _output.WriteLine($"{e.GetType().Name}: {e.Message}");
    }

    /// <summary>
    /// The arrow-rs regression from the issue. DataFusion refused the V2 + dictionary, 4 KiB-page
    /// file with "first repetition level of batch must be 0". The control is the same data with the
    /// default page size, which lands in one page and so cannot be misaligned; the two must decode
    /// identically in the same reader.
    /// </summary>
    [SkippableFact]
    public async Task DataFusion_ReadsSmallPageDictionaryLists()
    {
        ExternalParquetReaders.Require();

        var batch = BuildInt64Lists(rows: 50, length: _ => 1000, distinct: 40);
        string control = await WriteAsync(batch, Options(DataPageVersion.V2, dictionary: true,
            dataPageSize: ParquetWriteOptions.Default.DataPageSize));
        string small = await WriteAsync(batch, Options(DataPageVersion.V2, dictionary: true, dataPageSize: 4096));

        Assert.Single(WalkDataPages(control, column: 0, maxRepLevel: 1));
        Assert.True(WalkDataPages(small, column: 0, maxRepLevel: 1).Count > 1);

        var fromControl = ExternalParquetReaders.Read(ExternalParquetReaders.DataFusion, control);
        Skip.IfNot(fromControl.Installed, $"datafusion is not installed: {fromControl.Error}");
        Assert.True(fromControl.Result is not null, $"DataFusion failed on the single-page control: {fromControl.Error}");

        var fromSmall = ExternalParquetReaders.Read(ExternalParquetReaders.DataFusion, small);
        Assert.True(fromSmall.Result is not null,
            $"DataFusion read the single-page control but refused the small-page file: {fromSmall.Error}");

        Assert.Equal(50, fromSmall.Result!.Rows);
        Assert.Equal(fromControl.Result!.Columns, fromSmall.Result.Columns);
        _output.WriteLine($"datafusion v{fromSmall.Result.Version}: {fromSmall.Result.Rows} rows");
    }

    // ───── Helpers ─────

    private sealed record DataPage(int[] RepLevels, int? HeaderNumRows);

    private static ParquetWriteOptions Options(DataPageVersion version, bool dictionary, int dataPageSize) =>
        ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            DataPageSize = dataPageSize,
            // The walker reads V1 levels straight out of the page body, which it can only do uncompressed.
            Compression = CompressionCodec.Uncompressed,
        };

    private async Task<List<DataPage>> WriteAndWalkAsync(
        RecordBatch batch, DataPageVersion version, bool dictionary, int dataPageSize, int maxRepLevel)
    {
        string path = await WriteAsync(batch, Options(version, dictionary, dataPageSize));
        await AssertRoundTripsAsync(path, batch);

        var pages = WalkDataPages(path, column: 0, maxRepLevel);
        var metadata = ReadMetadata(path).RowGroups[0].Columns[0].MetaData!;
        Assert.Equal(dictionary, metadata.DictionaryPageOffset is not null);
        return pages;
    }

    private async Task<string> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private static async Task AssertRoundTripsAsync(string path, RecordBatch expected)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var actual = await reader.ReadRowGroupAsync(0);

        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(Render(expected.Column(0), i), Render(actual.Column(0), i));
    }

    private static void AssertRowAligned(List<DataPage> pages, DataPageVersion version, int expectedRows)
    {
        int rows = 0;
        for (int p = 0; p < pages.Count; p++)
        {
            var page = pages[p];
            int pageRows = page.RepLevels.Count(r => r == 0);

            Assert.True(page.RepLevels.Length > 0, $"page {p} of {pages.Count} is empty");
            Assert.True(page.RepLevels[0] == 0,
                $"page {p} of {pages.Count} begins mid-row (first repetition level {page.RepLevels[0]})");

            if (version == DataPageVersion.V2)
                Assert.Equal(pageRows, page.HeaderNumRows);

            rows += pageRows;
        }

        Assert.Equal(expectedRows, rows);
    }

    private static FileMetaData ReadMetadata(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength));
    }

    /// <summary>
    /// Walks one column chunk's pages header by header and decodes each data page's repetition
    /// levels. Requires an uncompressed file: V1 stores its levels inside the compressed body.
    /// </summary>
    private static List<DataPage> WalkDataPages(string path, int column, int maxRepLevel)
    {
        byte[] bytes = File.ReadAllBytes(path);
        var metadata = ReadMetadata(path).RowGroups[0].Columns[column].MetaData!;
        Assert.Equal(CompressionCodec.Uncompressed, metadata.Codec);

        int bitWidth = 32 - System.Numerics.BitOperations.LeadingZeroCount((uint)maxRepLevel);
        long position = metadata.DictionaryPageOffset ?? metadata.DataPageOffset;
        long end = position + metadata.TotalCompressedSize;
        var pages = new List<DataPage>();

        while (position < end)
        {
            var header = PageHeaderDecoder.Decode(bytes.AsSpan(checked((int)position)), out int headerLength);
            var body = bytes.AsSpan(checked((int)position + headerLength), header.CompressedPageSize);
            position += headerLength + header.CompressedPageSize;

            switch (header.Type)
            {
                case PageType.DataPageV2:
                {
                    var v2 = header.DataPageHeaderV2!;
                    var levels = new int[v2.NumValues];
                    new RleBitPackedDecoder(body[..v2.RepetitionLevelsByteLength], bitWidth).ReadBatch(levels);
                    pages.Add(new DataPage(levels, v2.NumRows));
                    break;
                }
                case PageType.DataPage:
                {
                    int length = BinaryPrimitives.ReadInt32LittleEndian(body);
                    var levels = new int[header.DataPageHeader!.NumValues];
                    new RleBitPackedDecoder(body.Slice(4, length), bitWidth).ReadBatch(levels);
                    pages.Add(new DataPage(levels, null));
                    break;
                }
            }
        }

        return pages;
    }

    /// <summary>
    /// <c>list&lt;int64&gt;</c>. A negative length is a null list. Values cycle through
    /// <paramref name="distinct"/> so the dictionary is taken or declined as the case needs.
    /// </summary>
    private static RecordBatch BuildInt64Lists(
        int rows, Func<int, int> length, int distinct, int nullElementEvery = 0)
    {
        var builder = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)builder.ValueBuilder;
        long next = 0;
        for (int r = 0; r < rows; r++)
        {
            int n = length(r);
            if (n < 0)
            {
                builder.AppendNull();
                continue;
            }

            builder.Append();
            for (int i = 0; i < n; i++, next++)
            {
                if (nullElementEvery > 0 && next % nullElementEvery == 0) values.AppendNull();
                else values.Append(next % distinct);
            }
        }

        var array = builder.Build();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("values", array.Data.DataType, nullable: true))
            .Build();
        return new RecordBatch(schema, [array], rows);
    }

    private static string Render(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return "null";

        switch (array)
        {
            case MapArray map:
            {
                var sb = new StringBuilder("{");
                for (int i = map.ValueOffsets[index]; i < map.ValueOffsets[index + 1]; i++)
                    sb.Append(Render(map.Keys, i)).Append(':').Append(Render(map.Values, i)).Append(',');
                return sb.Append('}').ToString();
            }
            case ListArray list:
            {
                var sb = new StringBuilder("[");
                for (int i = list.ValueOffsets[index]; i < list.ValueOffsets[index + 1]; i++)
                    sb.Append(Render(list.Values, i)).Append(',');
                return sb.Append(']').ToString();
            }
            case Int64Array a: return a.GetValue(index)!.Value.ToString();
            case Int32Array a: return a.GetValue(index)!.Value.ToString();
            case StringArray a: return a.GetString(index);
            default: throw new NotSupportedException(array.GetType().Name);
        }
    }
}
