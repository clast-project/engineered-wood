// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using Xunit.Abstractions;

namespace EngineeredWood.Tests.Parquet.Metadata;

/// <summary>
/// The Thrift codec for the page index: <see cref="ColumnIndex"/>, <see cref="OffsetIndex"/> and the
/// four <see cref="ColumnChunk"/> fields that locate them.
/// </summary>
/// <remarks>
/// The parquet-testing fixtures that carry a page index are the oracle, in two ways. EW must
/// re-encode each index to the same bytes its writer produced, which checks the encoder against
/// parquet-mr, parquet-cpp and arrow-rs rather than against EW's own decoder. And the decoded
/// OffsetIndex must agree with the page headers it describes, which checks the decoder against the
/// file rather than against the encoder.
/// </remarks>
public class PageIndexCodecTests
{
    private readonly ITestOutputHelper _output;

    public PageIndexCodecTests(ITestOutputHelper output) => _output = output;

    /// <summary>Every parquet-testing fixture whose footer locates at least one page index.</summary>
    public static TheoryData<string> FixturesWithPageIndexes()
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
                // Encrypted footers (PARE) and deliberately malformed fixtures; neither is this test's subject.
                continue;
            }

            if (metadata.RowGroups.Any(rg => rg.Columns.Any(c => c.OffsetIndexOffset is not null || c.ColumnIndexOffset is not null)))
                data.Add(Path.GetFileName(path));
        }

        return data;
    }

    [Fact]
    public void Fixtures_IncludeThePageIndexCorpus()
    {
        // The plan counted 26. A drop means the discovery above started skipping files it should not.
        int count = FixturesWithPageIndexes().Count;
        Assert.True(count >= 26, $"only {count} fixtures found");
    }

    [Theory]
    [MemberData(nameof(FixturesWithPageIndexes))]
    public void Fixture_ReEncodesToTheWriterBytes(string fileName)
    {
        byte[] file = File.ReadAllBytes(TestData.GetPath(fileName));
        var metadata = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes(fileName));
        int offsetIndexes = 0, columnIndexes = 0;

        foreach (var rowGroup in metadata.RowGroups)
        {
            foreach (var chunk in rowGroup.Columns)
            {
                if (chunk.OffsetIndexOffset is { } oOffset)
                {
                    var original = Slice(file, oOffset, chunk.OffsetIndexLength);
                    var index = MetadataDecoder.DecodeOffsetIndex(original);
                    Assert.Equal(original.ToArray(), MetadataEncoder.EncodeOffsetIndex(index));
                    offsetIndexes++;
                }

                if (chunk.ColumnIndexOffset is { } cOffset)
                {
                    var original = Slice(file, cOffset, chunk.ColumnIndexLength);
                    var index = MetadataDecoder.DecodeColumnIndex(original);
                    Assert.Equal(original.ToArray(), MetadataEncoder.EncodeColumnIndex(index));
                    columnIndexes++;
                }
            }
        }

        _output.WriteLine($"{fileName}: {offsetIndexes} offset indexes, {columnIndexes} column indexes ({metadata.CreatedBy})");
    }

    /// <summary>
    /// Each <see cref="PageLocation"/> must name a data page header at its offset, with the page's full
    /// size, in file order and on strictly increasing rows; and the column index, when present, must
    /// cover the same pages.
    /// </summary>
    [Theory]
    [MemberData(nameof(FixturesWithPageIndexes))]
    public void Fixture_OffsetIndexMatchesThePageHeaders(string fileName)
    {
        byte[] file = File.ReadAllBytes(TestData.GetPath(fileName));
        var metadata = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes(fileName));

        foreach (var rowGroup in metadata.RowGroups)
        {
            foreach (var chunk in rowGroup.Columns)
            {
                if (chunk.OffsetIndexOffset is not { } oOffset)
                    continue;

                var offsetIndex = MetadataDecoder.DecodeOffsetIndex(Slice(file, oOffset, chunk.OffsetIndexLength));
                var locations = offsetIndex.PageLocations;
                var dataPages = WalkDataPages(file, chunk.MetaData!);

                Assert.Equal(dataPages.Count, locations.Count);
                Assert.Equal(0, locations[0].FirstRowIndex);
                for (int p = 0; p < locations.Count; p++)
                {
                    Assert.Equal(dataPages[p].Offset, locations[p].Offset);
                    Assert.Equal(dataPages[p].Size, locations[p].CompressedPageSize);
                    if (p > 0)
                        Assert.True(locations[p].FirstRowIndex > locations[p - 1].FirstRowIndex);
                    if (dataPages[p].NumRows is { } rows)
                    {
                        long next = p + 1 < locations.Count ? locations[p + 1].FirstRowIndex : rowGroup.NumRows;
                        Assert.Equal(rows, next - locations[p].FirstRowIndex);
                    }
                }

                if (chunk.ColumnIndexOffset is { } cOffset)
                {
                    var columnIndex = MetadataDecoder.DecodeColumnIndex(Slice(file, cOffset, chunk.ColumnIndexLength));
                    Assert.Equal(locations.Count, columnIndex.NullPages.Count);
                }
            }
        }
    }

    [Fact]
    public void Fixture_NullPagesAreDecoded()
    {
        // int32_with_null_pages is built so that some pages hold nothing but nulls.
        var indexes = ColumnIndexes("int32_with_null_pages.parquet").ToList();
        Assert.Contains(indexes, ci => ci.NullPages.Contains(true));
        Assert.Contains(indexes, ci => ci.NullPages.Contains(false));

        foreach (var ci in indexes)
        {
            for (int p = 0; p < ci.NullPages.Count; p++)
            {
                if (ci.NullPages[p])
                    Assert.True(ci.NullCounts![p] > 0);
            }
        }
    }

    [Fact]
    public void Fixture_NanCountsAreDecoded()
    {
        // floating_orders_nan_count carries PARQUET-2249 nan_counts in its column indexes.
        Assert.Contains(ColumnIndexes("floating_orders_nan_count.parquet"),
            ci => ci.NanCounts is { } counts && counts.Any(n => n > 0));
    }

    // ───── Round trips ─────

    [Fact]
    public void ColumnIndex_RoundTripsEveryField()
    {
        // 20 pages, so every list takes the long-form header (a count of 15 or more).
        const int pages = 20;
        var original = new ColumnIndex
        {
            NullPages = Enumerable.Range(0, pages).Select(p => p % 3 == 0).ToArray(),
            MinValues = Enumerable.Range(0, pages).Select(p => p % 3 == 0 ? [] : new[] { (byte)p, (byte)0xFF }).ToArray(),
            MaxValues = Enumerable.Range(0, pages).Select(p => p % 3 == 0 ? [] : new byte[] { (byte)(p + 1) }).ToArray(),
            BoundaryOrder = BoundaryOrder.Descending,
            NullCounts = Enumerable.Range(0, pages).Select(p => (long)p * 1_000_000_007).ToArray(),
            RepetitionLevelHistograms = Enumerable.Range(0, pages * 2).Select(i => (long)i).ToArray(),
            DefinitionLevelHistograms = Enumerable.Range(0, pages * 3).Select(i => -(long)i).ToArray(),
            NanCounts = Enumerable.Range(0, pages).Select(p => (long)(p % 2)).ToArray(),
        };

        var decoded = MetadataDecoder.DecodeColumnIndex(MetadataEncoder.EncodeColumnIndex(original));

        Assert.Equal(original.NullPages, decoded.NullPages);
        Assert.Equal(original.MinValues, decoded.MinValues);
        Assert.Equal(original.MaxValues, decoded.MaxValues);
        Assert.Equal(original.BoundaryOrder, decoded.BoundaryOrder);
        Assert.Equal(original.NullCounts, decoded.NullCounts);
        Assert.Equal(original.RepetitionLevelHistograms, decoded.RepetitionLevelHistograms);
        Assert.Equal(original.DefinitionLevelHistograms, decoded.DefinitionLevelHistograms);
        Assert.Equal(original.NanCounts, decoded.NanCounts);
    }

    [Fact]
    public void ColumnIndex_OmittedOptionalFieldsStayNull()
    {
        var original = new ColumnIndex
        {
            NullPages = [false],
            MinValues = [[1]],
            MaxValues = [[2]],
            BoundaryOrder = BoundaryOrder.Unordered,
        };

        var decoded = MetadataDecoder.DecodeColumnIndex(MetadataEncoder.EncodeColumnIndex(original));

        Assert.Null(decoded.NullCounts);
        Assert.Null(decoded.RepetitionLevelHistograms);
        Assert.Null(decoded.DefinitionLevelHistograms);
        Assert.Null(decoded.NanCounts);
    }

    [Fact]
    public void OffsetIndex_RoundTrips()
    {
        var original = new OffsetIndex
        {
            PageLocations = Enumerable.Range(0, 17)
                .Select(p => new PageLocation(4L + p * 5_000_000_000L, 1000 + p, p * 3_000_000_000L))
                .ToArray(),
            UnencodedByteArrayDataBytes = Enumerable.Range(0, 17).Select(p => (long)p * 7).ToArray(),
        };

        var decoded = MetadataDecoder.DecodeOffsetIndex(MetadataEncoder.EncodeOffsetIndex(original));

        Assert.Equal(original.PageLocations, decoded.PageLocations);
        Assert.Equal(original.UnencodedByteArrayDataBytes, decoded.UnencodedByteArrayDataBytes);

        var bare = MetadataDecoder.DecodeOffsetIndex(MetadataEncoder.EncodeOffsetIndex(
            new OffsetIndex { PageLocations = [new PageLocation(4, 100, 0)] }));
        Assert.Null(bare.UnencodedByteArrayDataBytes);
    }

    [Fact]
    public void ColumnChunk_IndexLocationsRoundTripThroughTheFooter()
    {
        var original = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes("alltypes_tiny_pages.parquet"));
        Assert.All(original.RowGroups.SelectMany(rg => rg.Columns), c =>
        {
            Assert.NotNull(c.OffsetIndexOffset);
            Assert.NotNull(c.OffsetIndexLength);
        });

        var decoded = MetadataDecoder.DecodeFileMetaData(MetadataEncoder.EncodeFileMetaData(original));

        var before = original.RowGroups.SelectMany(rg => rg.Columns).ToList();
        var after = decoded.RowGroups.SelectMany(rg => rg.Columns).ToList();
        Assert.Equal(before.Count, after.Count);
        for (int i = 0; i < before.Count; i++)
        {
            Assert.Equal(before[i].OffsetIndexOffset, after[i].OffsetIndexOffset);
            Assert.Equal(before[i].OffsetIndexLength, after[i].OffsetIndexLength);
            Assert.Equal(before[i].ColumnIndexOffset, after[i].ColumnIndexOffset);
            Assert.Equal(before[i].ColumnIndexLength, after[i].ColumnIndexLength);
        }
    }

    [Fact]
    public void ColumnChunk_WithoutIndexesWritesNoIndexFields()
    {
        var original = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes("alltypes_plain.parquet"));
        var decoded = MetadataDecoder.DecodeFileMetaData(MetadataEncoder.EncodeFileMetaData(original));

        Assert.All(decoded.RowGroups.SelectMany(rg => rg.Columns), c =>
        {
            Assert.Null(c.OffsetIndexOffset);
            Assert.Null(c.OffsetIndexLength);
            Assert.Null(c.ColumnIndexOffset);
            Assert.Null(c.ColumnIndexLength);
        });
    }

    // ───── Malformed input ─────

    [Fact]
    public void ColumnIndex_MissingRequiredFieldIsRejected()
    {
        // Field 1 (null_pages) only, then stop.
        byte[] bytes = MetadataEncoder.EncodeColumnIndex(new ColumnIndex
        {
            NullPages = [false],
            MinValues = [[1]],
            MaxValues = [[2]],
            BoundaryOrder = BoundaryOrder.Ascending,
        });
        int field2 = Array.IndexOf(bytes, (byte)0x19, 1); // delta 1, type LIST: min_values' header
        byte[] truncated = [.. bytes.AsSpan(0, field2), 0x00];

        var e = Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeColumnIndex(truncated));
        Assert.Contains("min_values", e.Message);
    }

    [Fact]
    public void ColumnIndex_ListsOfDifferentLengthsAreRejected()
    {
        byte[] bytes = MetadataEncoder.EncodeColumnIndex(new ColumnIndex
        {
            NullPages = [false, false],
            MinValues = [[1], [2]],
            MaxValues = [[2]],
            BoundaryOrder = BoundaryOrder.Unordered,
        });

        var e = Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeColumnIndex(bytes));
        Assert.Contains("max_values", e.Message);
    }

    [Fact]
    public void ColumnIndex_NullCountsOfTheWrongLengthAreRejected()
    {
        byte[] bytes = MetadataEncoder.EncodeColumnIndex(new ColumnIndex
        {
            NullPages = [false, false],
            MinValues = [[1], [2]],
            MaxValues = [[2], [3]],
            BoundaryOrder = BoundaryOrder.Unordered,
            NullCounts = [0],
        });

        Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeColumnIndex(bytes));
    }

    [Fact]
    public void OffsetIndex_ACountLargerThanTheInputIsRejectedBeforeAllocating()
    {
        // Field 1, LIST of STRUCT in long form, claiming int.MaxValue entries, and nothing after it.
        byte[] bytes = [0x19, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0x07];

        var e = Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeOffsetIndex(bytes));
        Assert.Contains("page_locations", e.Message);
    }

    [Fact]
    public void OffsetIndex_WrongElementTypeIsRejected()
    {
        // Field 1 as a LIST of one I32 rather than of PageLocation structs.
        byte[] bytes = [0x19, 0x15, 0x02, 0x00];

        Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeOffsetIndex(bytes));
    }

    [Fact]
    public void PageLocation_MissingRequiredFieldIsRejected()
    {
        // One PageLocation holding only field 1 (offset = 4).
        byte[] bytes = [0x19, 0x1C, 0x16, 0x08, 0x00, 0x00];

        var e = Assert.Throws<ParquetFormatException>(() => MetadataDecoder.DecodeOffsetIndex(bytes));
        Assert.Contains("compressed_page_size", e.Message);
    }

    // ───── Helpers ─────

    private static ReadOnlySpan<byte> Slice(byte[] file, long offset, int? length)
    {
        Assert.NotNull(length);
        return file.AsSpan(checked((int)offset), length!.Value);
    }

    private static IEnumerable<ColumnIndex> ColumnIndexes(string fileName)
    {
        byte[] file = File.ReadAllBytes(TestData.GetPath(fileName));
        var metadata = MetadataDecoder.DecodeFileMetaData(TestData.ReadFooterBytes(fileName));
        return metadata.RowGroups
            .SelectMany(rg => rg.Columns)
            .Where(c => c.ColumnIndexOffset is not null)
            .Select(c => MetadataDecoder.DecodeColumnIndex(Slice(file, c.ColumnIndexOffset!.Value, c.ColumnIndexLength)))
            .ToList();
    }

    private sealed record DataPageSpan(long Offset, int Size, int? NumRows);

    /// <summary>Walks a column chunk's page headers and returns its data pages, dictionary pages excluded.</summary>
    private static List<DataPageSpan> WalkDataPages(byte[] file, ColumnMetaData metadata)
    {
        long start = metadata.DictionaryPageOffset is { } d && d > 0 && d < metadata.DataPageOffset
            ? d
            : metadata.DataPageOffset;
        long end = start + metadata.TotalCompressedSize;
        var pages = new List<DataPageSpan>();

        for (long position = start; position < end;)
        {
            var header = PageHeaderDecoder.Decode(file.AsSpan(checked((int)position)), out int headerLength);
            int size = headerLength + header.CompressedPageSize;
            if (header.Type is PageType.DataPage or PageType.DataPageV2)
                pages.Add(new DataPageSpan(position, size, header.DataPageHeaderV2?.NumRows));
            position += size;
        }

        return pages;
    }
}
