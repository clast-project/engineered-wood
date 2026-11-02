// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Tests.Parquet.Metadata;
using Xunit.Abstractions;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Row-range reads (<see cref="ParquetFileReader.ReadRowRangesAsync"/>, doc/parquet-page-index.md R-3):
/// only the rows in the ranges, reading only their pages, and
/// <see cref="ParquetReadOptions.FilterUsePageIndex"/>, which reads through them.
/// </summary>
/// <remarks>
/// The oracle is a full read of the same row group: the rows a range read returns must be exactly
/// the full read's rows at those positions, value for value. The byte counts show the pages outside
/// the ranges are not fetched.
/// </remarks>
public class PageIndexRowRangeReadTests : IDisposable
{
    private const int Rows = 5000;

    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public PageIndexRowRangeReadTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-rowranges-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ───── The rows are the full read's rows ─────

    public static TheoryData<DataPageVersion, bool, int?, long?> Layouts()
    {
        var data = new TheoryData<DataPageVersion, bool, int?, long?>();
        foreach (var version in new[] { DataPageVersion.V1, DataPageVersion.V2 })
        foreach (bool dictionary in new[] { true, false })
        {
            data.Add(version, dictionary, null, null);
            data.Add(version, dictionary, 97, null);
            data.Add(version, dictionary, null, 4096);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task RangeRead_ReturnsExactlyTheRangesRows(DataPageVersion version, bool dictionary, int? batchSize, long? maxBytes)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageVersion = version,
            DictionaryEnabled = dictionary,
            DataPageRowCountLimit = 250,
        });
        var options = new ParquetReadOptions { BatchSize = batchSize, MaxBatchByteSize = maxBytes };

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
            await AssertRangeReadAsync(path, options, 0, ranges, expected, batchSize);
    }

    /// <summary>Without an OffsetIndex, pages are found by scanning headers; the rows are the same.</summary>
    [Fact]
    public async Task NoPageIndex_SameRows()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            WritePageIndex = false,
            DataPageRowCountLimit = 250,
        });

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, expected, batchSize: null);
    }

    /// <summary>
    /// A row group with a nested column is decoded whole and cut to the ranges: the rows are the same,
    /// lists and structs included.
    /// </summary>
    [Fact]
    public async Task NestedColumns_SameRows()
    {
        string path = await WriteAsync(WithNested(Flat(Rows)), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
        {
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, expected, batchSize: null);
            await AssertRangeReadAsync(path, new ParquetReadOptions { BatchSize = 97 }, 0, ranges, expected, batchSize: 97);
        }
    }

    /// <summary>
    /// Every fixture with a page index: a few ranges in each row group read the same rows as a full
    /// read. The fixtures come from parquet-mr, parquet-cpp and parquet-rs, with dictionary, delta and
    /// nested encodings EW's writer does not produce.
    /// </summary>
    [Theory]
    [MemberData(nameof(PageIndexCodecTests.FixturesWithPageIndexes), MemberType = typeof(PageIndexCodecTests))]
    public async Task Fixture_SameRows(string fileName)
    {
        string path = TestData.GetPath(fileName);
        using var probe = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        var metadata = await probe.ReadMetadataAsync();

        int checkedGroups = 0;
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            int rows = checked((int)metadata.RowGroups[g].NumRows);
            if (rows == 0)
                continue;

            List<string> expected;
            try
            {
                expected = await FullRowsAsync(path, g);
            }
            catch (Exception e) when (Unreadable(e))
            {
                _output.WriteLine($"row group {g} unreadable here ({e.GetType().Name}); skipped");
                continue;
            }

            var ranges = rows < 5
                ? new[] { new RowRange(0, rows) }
                : new[] { new RowRange(rows / 5, 2 * rows / 5), new RowRange(3 * rows / 5, 3 * rows / 5 + 1), new RowRange(rows - 1, rows) };
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, g, ranges, expected, batchSize: null);
            await AssertRangeReadAsync(path, new ParquetReadOptions { BatchSize = 7 }, g, ranges, expected, batchSize: 7);
            checkedGroups++;
        }

        _output.WriteLine($"{checkedGroups} row groups checked");
    }

    // ───── Only the ranges' pages are read ─────

    /// <summary>One 250-row page of 20 is about 5% of the data; the read fetches little more.</summary>
    [Fact]
    public async Task OnePage_FetchesAFractionOfTheBytes()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });

        long whole = await BytesReadAsync(path, reader => reader.ReadRowGroupBatchesAsync(0));
        long one = await BytesReadAsync(path, reader => reader.ReadRowRangesAsync(0, [new RowRange(1000, 1250)]));

        _output.WriteLine($"one page {one:N0} bytes, whole row group {whole:N0}");
        Assert.True(one < whole / 5, $"one page read {one:N0} of {whole:N0} bytes");
    }

    // ───── FilterUsePageIndex ─────

    [Fact]
    public async Task FilterUsePageIndex_ReadsOnlyTheCandidateRows()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var filter = Ex.And(
            Ex.GreaterThanOrEqual("sorted", LiteralValue.Of(1100L)),
            Ex.LessThan("sorted", LiteralValue.Of(1300L)));

        var on = await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });
        var off = await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false });
        var expected = await FullRowsAsync(path, 0);

        // The pages [1000, 1250) and [1250, 1500).
        Assert.Equal(expected.Skip(1000).Take(500), on);
        Assert.Equal(expected, off);

        long onBytes = await BytesReadAsync(path, reader => reader.ReadAllAsync(),
            new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });
        long offBytes = await BytesReadAsync(path, reader => reader.ReadAllAsync(), new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false });
        _output.WriteLine($"on {onBytes:N0} bytes, off {offBytes:N0}");
        Assert.True(onBytes < offBytes / 4, $"read {onBytes:N0} of {offBytes:N0} bytes");
    }

    /// <summary>On by default (#435): a filter alone narrows by the page index.</summary>
    [Fact]
    public async Task FilterUsePageIndex_IsOnByDefault()
    {
        Assert.True(ParquetReadOptions.Default.FilterUsePageIndex);

        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var filter = Ex.Equal("sorted", LiteralValue.Of(1300L));
        var expected = await FullRowsAsync(path, 0);

        Assert.Equal(expected.Skip(1250).Take(250), await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter }));
    }

    /// <summary>
    /// Statistics keep the row group (each half of the AND is possible somewhere in it), but no page is
    /// in both halves: the row group is skipped without reading a data page.
    /// </summary>
    [Fact]
    public async Task FilterUsePageIndex_SkipsARowGroupNoPageCanMatch()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var filter = Ex.And(
            Ex.LessThan("sorted", LiteralValue.Of(100L)),
            Ex.GreaterThan("sorted", LiteralValue.Of(4900L)));

        Assert.Empty(await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true }));
        Assert.Equal(Rows, (await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false })).Count);
    }

    /// <summary>With several row groups, each is narrowed on its own, and ruled-out ones are skipped.</summary>
    [Fact]
    public async Task FilterUsePageIndex_SeveralRowGroups()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = 2000,
        });
        var filter = Ex.Or(
            Ex.Equal("sorted", LiteralValue.Of(10L)),
            Ex.Equal("sorted", LiteralValue.Of(4500L)));

        var on = await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true, BatchSize = 100 });
        var expected = new List<string>();
        for (int g = 0; g < 3; g++)
            expected.AddRange(await FullRowsAsync(path, g));

        // Row group 0 keeps [0, 250); row group 1 is ruled out by statistics; row group 2 (rows 4000 on)
        // keeps [500, 750) of its own, file rows [4500, 4750).
        Assert.Equal(expected.Take(250).Concat(expected.Skip(4500).Take(250)), on);
    }

    // ───── ReadWithPositionsAsync: each batch's first row in the file ─────

    public static TheoryData<bool, int?, bool> PositionLayouts()
    {
        var data = new TheoryData<bool, int?, bool>();
        foreach (bool pageIndex in new[] { true, false })
        foreach (int? batchSize in new int?[] { null, 97 })
        foreach (bool nested in new[] { false, true })
            data.Add(pageIndex, batchSize, nested);
        return data;
    }

    /// <summary>
    /// <c>sorted</c> holds each row's file position, so every row of every batch must sit at the batch's
    /// FirstRow plus its index: after a skipped row group, after a skipped page range within one, and
    /// across a range split into several batches.
    /// </summary>
    [Theory]
    [MemberData(nameof(PositionLayouts))]
    public async Task ReadWithPositions_EveryRowIsAtItsFilePosition(bool pageIndex, int? batchSize, bool nested)
    {
        var rows = Flat(Rows);
        string path = await WriteAsync(nested ? WithNested(rows) : rows, ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = 2000,
        });
        var options = new ParquetReadOptions { FilterUsePageIndex = pageIndex, BatchSize = batchSize };

        // Row group 0 keeps two page ranges, [0, 250) and [1500, 1750), row group 1 is ruled out, and
        // row group 2 keeps [4500, 4750); without the index, groups 0 and 2 whole.
        var filter = Ex.Or(
            Ex.Or(Ex.Equal("sorted", LiteralValue.Of(10L)), Ex.Equal("sorted", LiteralValue.Of(1600L))),
            Ex.Equal("sorted", LiteralValue.Of(4600L)));
        long[] expected = pageIndex
            ? [.. Positions(0, 250), .. Positions(1500, 250), .. Positions(4500, 250)]
            : [.. Positions(0, 2000), .. Positions(4000, 1000)];

        Assert.Equal(expected, await PositionsReadAsync(path, options, filter));

        // No filter: every row, at its own position.
        Assert.Equal(Positions(0, Rows), await PositionsReadAsync(path, options, filter: null));

        static IEnumerable<long> Positions(long start, int count) => Enumerable.Range(0, count).Select(i => start + i);
    }

    /// <summary>A null filter reads under the options' filter, as ReadAllAsync does.</summary>
    [Fact]
    public async Task ReadWithPositions_NullFilter_TakesTheOptionsFilter()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var options = new ParquetReadOptions
        {
            Filter = Ex.Equal("sorted", LiteralValue.Of(1300L)),
            FilterUsePageIndex = true,
        };

        Assert.Equal(Enumerable.Range(1250, 250).Select(i => (long)i), await PositionsReadAsync(path, options, filter: null));
    }

    /// <summary>
    /// The positions read, row by row, from <c>sorted</c>, after checking each row against its batch's
    /// FirstRow.
    /// </summary>
    private static async Task<List<long>> PositionsReadAsync(string path, ParquetReadOptions options, Predicate? filter)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true, options);
        var positions = new List<long>();
        await foreach (var positioned in reader.ReadWithPositionsAsync(filter))
        {
            using var batch = positioned.Batch;
            var sorted = (Int64Array)batch.Column("sorted");
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(positioned.FirstRow + i, sorted.GetValue(i));
                positions.Add(sorted.GetValue(i)!.Value);
            }
        }

        return positions;
    }

    // ───── Requests: round trips, not only bytes, decide object storage ─────

    /// <summary>
    /// A selective filter costs one request more than without the index: the index of the row group it
    /// keeps, which also carries the OffsetIndexes its page maps need, and whose dictionary pages come
    /// with its data pages.
    /// </summary>
    [Fact]
    public async Task FilterUsePageIndex_SelectiveFilter_CostsOneRequest()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = 1000,
        });
        var filter = Ex.Equal("sorted", LiteralValue.Of(2100L));

        int off = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false });
        int on = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });

        Assert.Equal(off + 1, on);
    }

    /// <summary>
    /// A filter the index cannot narrow costs one request more per window of row groups, not per row
    /// group: 132 row groups are three windows of at most 64.
    /// </summary>
    [Theory]
    [InlineData(1000, 1)]  // 5 row groups
    [InlineData(38, 3)]    // 132 row groups
    public async Task FilterUsePageIndex_FilterItCannotNarrow_OneRequestPerWindow(int rowsPerGroup, int windows)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = rowsPerGroup,
        });

        // Random ints: every row group and every page holds values on both sides of the median.
        var filter = Ex.LessThan("ints", LiteralValue.Of(int.MaxValue / 2));

        int off = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false });
        int on = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });

        Assert.Equal(off + windows, on);
        Assert.Equal(
            await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false }),
            await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true }));
    }

    /// <summary>
    /// The read-ahead stops before a row group that would take it past its byte budget, rather than
    /// after: with a budget of 2.5 row groups' indexes, 20 row groups are 10 windows of 2, not 7 of 3.
    /// </summary>
    [Fact]
    public async Task FilterUsePageIndex_ReadAheadStaysWithinItsBudget()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 50,
            RowGroupMaxRows = 250,
        });
        var filter = Ex.LessThan("ints", LiteralValue.Of(int.MaxValue / 2));

        // What the read-ahead asks for per row group: the filter column's ColumnIndex, and every
        // column's OffsetIndex (all columns are flat, and all are read).
        long largest;
        using (var probe = Open(path))
        {
            var metadata = await probe.ReadMetadataAsync();
            Assert.Equal(20, metadata.RowGroups.Count);
            largest = metadata.RowGroups.Max(rg =>
                rg.Columns[1].ColumnIndexLength!.Value + rg.Columns.Sum(c => (long)c.OffsetIndexLength!.Value));
        }

        int off = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false });
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(counting, ownsFile: true,
            new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true }))
        {
            reader.PageIndexReadAheadBudgetBytes = largest * 5 / 2;
            await foreach (var batch in reader.ReadAllAsync())
                batch.Dispose();
        }

        Assert.Equal(off + 10, counting.Requests.Count);
    }

    /// <summary>
    /// Where statistics already decide a row group (here every row matches), the index is never read, so
    /// the option costs nothing: a batched read makes exactly the requests it makes without it, page maps'
    /// OffsetIndexes included.
    /// </summary>
    [Fact]
    public async Task FilterUsePageIndex_RowGroupsStatisticsDecide_CostNothing()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = 1000,
        });
        var filter = Ex.GreaterThanOrEqual("sorted", LiteralValue.Of(0L));

        int off = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, BatchSize = 97, FilterUsePageIndex = false });
        int on = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, BatchSize = 97, FilterUsePageIndex = true });

        Assert.Equal(off, on);
    }

    /// <summary>
    /// A projection with a nested column is decoded whole, so it builds no page maps and needs no
    /// OffsetIndexes: the selective filter still costs one request, not one more for the projection's
    /// indexes (which the read-ahead and the narrowing once disagreed about).
    /// </summary>
    [Fact]
    public async Task FilterUsePageIndex_NestedProjection_CostsOneRequest()
    {
        string path = await WriteAsync(WithNested(Flat(Rows)), ParquetWriteOptions.Default with
        {
            DataPageRowCountLimit = 250,
            RowGroupMaxRows = 1000,
        });
        var filter = Ex.Equal("sorted", LiteralValue.Of(2100L));

        int off = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false });
        int on = await RequestsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true });

        Assert.Equal(off + 1, on);
    }

    /// <summary>
    /// A footer with a ColumnIndex offset but no length has no ColumnIndex, as for
    /// <see cref="ParquetFileReader.ReadPageIndexAsync"/>: the rows are not narrowed, and nothing throws.
    /// </summary>
    [Fact]
    public async Task HalfSpecifiedColumnIndex_IsNoIndex()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        FooterRewrite.Rewrite(path, metadata =>
        {
            var rowGroup = metadata.RowGroups[0];
            var columns = rowGroup.Columns.ToArray();
            columns[0] = FooterRewrite.With(columns[0], nameof(EngineeredWood.Parquet.Metadata.ColumnChunk.ColumnIndexLength), (int?)null);
            return FooterRewrite.With(metadata, nameof(EngineeredWood.Parquet.Metadata.FileMetaData.RowGroups),
                new[] { FooterRewrite.With(rowGroup, nameof(EngineeredWood.Parquet.Metadata.RowGroup.Columns), columns) });
        });
        var filter = Ex.Equal("sorted", LiteralValue.Of(2100L));

        using (var reader = Open(path))
            Assert.Equal([new RowRange(0, Rows)], await reader.GetCandidateRowRangesAsync(0, filter));
        Assert.Equal(
            await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = false }),
            await ReadAllRowsAsync(path, new ParquetReadOptions { Filter = filter, FilterUsePageIndex = true }));
    }

    /// <summary>
    /// A batched read finds a column's pages from its OffsetIndex; its dictionary page is fetched in the
    /// same request as its first data pages, not in a request of its own.
    /// </summary>
    [Fact]
    public async Task BatchedRead_FetchesTheDictionaryWithTheFirstPages()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions { BatchSize = 97 });
        var metadata = await reader.ReadMetadataAsync();
        var low = metadata.RowGroups[0].Columns[2].MetaData!;
        Assert.Equal("low", low.PathInSchema![0]);
        long dictionary = low.DictionaryPageOffset!.Value;
        var dictionaryRange = new FileRange(dictionary, low.DataPageOffset - dictionary);

        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
            batch.Dispose();

        var carrying = Assert.Single(counting.RequestRanges, ranges => ranges.Contains(dictionaryRange));
        Assert.True(carrying.Count > 1, "the dictionary page was read on its own");
    }

    /// <summary>
    /// Where the OffsetIndex and the metadata agree the first data page starts, what lies before it must
    /// be a dictionary page. Bytes that are not one are refused, rather than decoded as a dictionary.
    /// </summary>
    [Fact]
    public async Task BatchedRead_CorruptDictionaryPage_IsRefused()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        long dictionary;
        using (var probe = Open(path))
            dictionary = (await probe.ReadMetadataAsync()).RowGroups[0].Columns[2].MetaData!.DictionaryPageOffset!.Value;
        byte[] bytes = File.ReadAllBytes(path);
        bytes.AsSpan(checked((int)dictionary), 16).Fill(0xFF);
        File.WriteAllBytes(path, bytes);

        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true, new ParquetReadOptions { BatchSize = 97 });
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in reader.ReadRowGroupBatchesAsync(0, ["low"]))
                batch.Dispose();
        });
    }

    // ───── Header-scanned page maps (#625) ─────

    /// <summary>
    /// Without an OffsetIndex, a read of many batches reads every chunk in one request to scan its headers,
    /// then decodes the batches from those chunks: one request, not one per column, and each byte once.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NoPageIndex_ManyBatches_ReadTheChunksOnceInOneRequest(bool ranged)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            WritePageIndex = false,
            DataPageRowCountLimit = 250,
        });
        RowRange[] ranges = [.. Enumerable.Range(0, 10).Select(k => new RowRange(k * 500 + 10, k * 500 + 260))];

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true,
            ranged ? ParquetReadOptions.Default : new ParquetReadOptions { BatchSize = 97 });
        var metadata = await reader.ReadMetadataAsync();
        int before = counting.Requests.Count;
        long bytesBefore = counting.Bytes;

        await foreach (var batch in ranged ? reader.ReadRowRangesAsync(0, ranges) : reader.ReadRowGroupBatchesAsync(0))
            batch.Dispose();

        var columns = metadata.RowGroups[0].Columns;
        Assert.Equal([columns.Count], counting.Requests.Skip(before));
        long chunkBytes = columns.Sum(c => c.MetaData!.TotalCompressedSize);
        long read = counting.Bytes - bytesBefore;
        Assert.True(read < chunkBytes * 11 / 10, $"read {read:N0} bytes of {chunkBytes:N0} in the chunks");
    }

    /// <summary>
    /// A budget smaller than the chunks splits the scan into several requests, and the chunks past it are let
    /// go once scanned, their pages fetched by the windows: the rows are the same however the budget falls,
    /// for ranges, row and byte batch limits.
    /// </summary>
    /// <param name="parts">The budget as a fraction of the row group's bytes; 0 for a budget of one byte, -1
    /// for the default, which holds the whole row group here.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(-1)]
    public async Task NoPageIndex_ScanBudget_RowsUnchanged(int parts)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            WritePageIndex = false,
            DataPageRowCountLimit = 250,
        });
        long budget = parts < 0 ? ParquetFileReader.DefaultHeaderScanBudgetBytes : 1;
        int columns;
        using (var probe = Open(path))
        {
            var metadata = await probe.ReadMetadataAsync();
            columns = metadata.RowGroups[0].Columns.Count;
            if (parts > 0)
                budget = metadata.RowGroups[0].Columns.Sum(c => c.MetaData!.TotalCompressedSize) / parts;
        }

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
        {
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, expected, batchSize: null, headerScanBudget: budget);
            await AssertRangeReadAsync(path, new ParquetReadOptions { BatchSize = 97 }, 0, ranges, expected, batchSize: 97, headerScanBudget: budget);
            await AssertRangeReadAsync(path, new ParquetReadOptions { MaxBatchByteSize = 4096 }, 0, ranges, expected, batchSize: null, headerScanBudget: budget);
        }

        // The scan's own requests: one per chunk with a one-byte budget, as before #625; one in all when
        // the budget holds the row group.
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions { BatchSize = 97 });
        reader.HeaderScanBudgetBytes = budget;
        await reader.ReadMetadataAsync();
        int before = counting.Requests.Count;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
            batch.Dispose();
        var requests = counting.Requests.Skip(before).ToList();
        _output.WriteLine($"budget {budget:N0}: requests of {string.Join(", ", requests)} ranges");
        if (parts == 0)
            Assert.Equal(columns, requests.TakeWhile(r => r == 1).Count());
        else if (parts < 0)
            Assert.Equal([columns], requests);
    }

    /// <summary>
    /// A wide row group's scan is cut at <see cref="BatchPageWindow.MaxRangesPerWindow"/> chunks a request,
    /// so a coalescing reader never issues more GETs at once than a page window would.
    /// </summary>
    [Fact]
    public async Task NoPageIndex_WideRowGroup_ScanRequestsAreCappedInRanges()
    {
        const int columns = 100, rows = 1000;
        var schema = new Apache.Arrow.Schema.Builder();
        var arrays = new List<IArrowArray>();
        for (int c = 0; c < columns; c++)
        {
            schema.Field(new Field("c" + c, Int32Type.Default, false));
            var builder = new Int32Array.Builder();
            for (int r = 0; r < rows; r++)
                builder.Append(r * columns + c);
            arrays.Add(builder.Build());
        }

        string path = await WriteAsync(new RecordBatch(schema.Build(), arrays, rows),
            ParquetWriteOptions.Default with { WritePageIndex = false, DataPageRowCountLimit = 100 });

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions { BatchSize = 97 });
        await reader.ReadMetadataAsync();
        int before = counting.Requests.Count;
        long read = 0, sum = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
        {
            read += batch.Length;
            var last = (Int32Array)batch.Column(columns - 1);
            for (int i = 0; i < last.Length; i++)
                sum += last.GetValue(i)!.Value;
            batch.Dispose();
        }

        var requests = counting.Requests.Skip(before).ToList();
        _output.WriteLine($"requests of {string.Join(", ", requests)} ranges");
        Assert.Equal([BatchPageWindow.MaxRangesPerWindow, columns - BatchPageWindow.MaxRangesPerWindow], requests.Take(2));
        Assert.All(requests, count => Assert.True(count <= BatchPageWindow.MaxRangesPerWindow, $"a request of {count} ranges"));
        Assert.Equal(rows, read);
        Assert.Equal(Enumerable.Range(0, rows).Sum(r => (long)r * columns + columns - 1), sum);
    }

    /// <summary>
    /// A projection's chunks are separated by the unprojected ones, which a coalescing reader fetches with
    /// them: those gaps count toward the scan budget, so a budget that holds the projected chunks alone
    /// takes more than one request, none of whose coalesced span exceeds it.
    /// </summary>
    [Fact]
    public async Task NoPageIndex_ScanBudget_CountsTheGapsBetweenChunks()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with
        {
            WritePageIndex = false,
            DataPageRowCountLimit = 250,
        });
        string[] projected = ["sorted", "low", "doubles", "times"];

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions { BatchSize = 97 });
        var metadata = await reader.ReadMetadataAsync();
        long budget = metadata.RowGroups[0].Columns
            .Where(c => projected.Contains(c.MetaData!.PathInSchema[0]))
            .Sum(c => c.MetaData!.TotalCompressedSize);
        reader.HeaderScanBudgetBytes = budget;
        int before = counting.Requests.Count;
        long read = 0, sum = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0, projected))
        {
            read += batch.Length;
            var sorted = (Int64Array)batch.Column(0);
            for (int i = 0; i < sorted.Length; i++)
                sum += sorted.GetValue(i)!.Value;
            batch.Dispose();
        }

        var first = counting.RequestRanges[before].OrderBy(r => r.Offset).ToList();
        long span = first[^1].Offset + first[^1].Length - first[0].Offset;
        _output.WriteLine($"budget {budget:N0}: first request of {first.Count} chunks spanning {span:N0} bytes");
        Assert.InRange(first.Count, 1, projected.Length - 1);
        Assert.True(first.Count == 1 || span <= budget, $"a request spanning {span:N0} bytes");
        Assert.Equal(Rows, read);
        Assert.Equal((long)Rows * (Rows - 1) / 2, sum);
    }

    // ───── One request for many batches (#622) ─────

    /// <summary>
    /// A read that returns many batches fetches their pages in one request, not one per batch: ten
    /// ranges, or a row group cut into 52 batches, cost the OffsetIndex read and one more.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ManyBatches_FetchTheirPagesInOneRequest(bool ranged)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        RowRange[] ranges = [.. Enumerable.Range(0, 10).Select(k => new RowRange(k * 500 + 10, k * 500 + 260))];

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true,
            ranged ? ParquetReadOptions.Default : new ParquetReadOptions { BatchSize = 97 });
        await reader.ReadMetadataAsync();
        int before = counting.Requests.Count;

        int batches = 0;
        await foreach (var batch in ranged ? reader.ReadRowRangesAsync(0, ranges) : reader.ReadRowGroupBatchesAsync(0))
        {
            batches++;
            batch.Dispose();
        }

        Assert.Equal(ranged ? 10 : 52, batches);
        Assert.Equal(2, counting.Requests.Count - before);
    }

    /// <summary>
    /// The skipped pages between ranges stay unread when their pages are fetched together: the request
    /// asks for no more bytes than the per-batch requests did.
    /// </summary>
    [Fact]
    public async Task ManyRanges_OneRequest_StillSkipsThePagesBetween()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        RowRange[] ranges = [.. Enumerable.Range(0, 10).Select(k => new RowRange(k * 500 + 10, k * 500 + 240))];

        long windowed = await BytesReadAsync(path, reader => reader.ReadRowRangesAsync(0, ranges));
        long perBatch = await BytesReadAsync(path, reader =>
        {
            reader.BatchPageWindowBudgetBytes = 1;
            return reader.ReadRowRangesAsync(0, ranges);
        });
        long whole = await BytesReadAsync(path, reader => reader.ReadRowGroupBatchesAsync(0));

        _output.WriteLine($"windowed {windowed:N0}, per batch {perBatch:N0}, whole {whole:N0}");
        Assert.Equal(perBatch, windowed);
        Assert.True(windowed < whole * 2 / 3, $"read {windowed:N0} of {whole:N0} bytes");
    }

    /// <summary>
    /// A budget cuts the batches into windows, each one request, and the rows are the same wherever the
    /// windows fall, including a page shared by batches on both sides of a window's edge.
    /// </summary>
    /// <param name="parts">The budget as a fraction of the row group's bytes; 0 for a budget of one byte.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(4)]
    [InlineData(16)]
    public async Task WindowBudget_CutsTheRequests_RowsUnchanged(int parts)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        long budget = 1;
        if (parts > 0)
        {
            using var probe = Open(path);
            var metadata = await probe.ReadMetadataAsync();
            budget = metadata.RowGroups[0].Columns.Sum(c => c.MetaData!.TotalCompressedSize) / parts;
        }

        var expected = await FullRowsAsync(path, 0);
        foreach (var ranges in RangeSets(Rows))
        {
            await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, expected, batchSize: null, budget);
            await AssertRangeReadAsync(path, new ParquetReadOptions { BatchSize = 97 }, 0, ranges, expected, batchSize: 97, budget);
            await AssertRangeReadAsync(path, new ParquetReadOptions { MaxBatchByteSize = 4096 }, 0, ranges, expected, batchSize: null, budget);
        }

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions { BatchSize = 97 });
        reader.BatchPageWindowBudgetBytes = budget;
        await reader.ReadMetadataAsync();
        int before = counting.Requests.Count;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
            batch.Dispose();

        // The OffsetIndex read, then the windows. A budget smaller than any batch makes a request for each
        // batch that reaches new pages, one per 250-row page, as the reads before windows did; a batch
        // inside rows already decoded joins the window before it, since it asks for nothing.
        int windows = counting.Requests.Count - before - 1;
        _output.WriteLine($"budget {budget:N0}: {windows} windows for 52 batches");
        if (parts == 0)
            Assert.Equal(Rows / 250, windows);
        else
            Assert.InRange(windows, parts, Math.Min(51, 2 * parts + 1));
    }

    /// <summary>
    /// A sparse read whose pages fit the byte budget many times over still asks for no more ranges per
    /// request than a coalescing reader should send at once: 50 runs in each of the columns are cut
    /// into several windows, and the rows are the same.
    /// </summary>
    [Fact]
    public async Task SparseRead_WindowsAreCappedInRanges()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 50 });
        RowRange[] ranges = [.. Enumerable.Range(0, 50).Select(k => new RowRange(k * 100 + 10, k * 100 + 40))];

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(counting, ownsFile: true))
        {
            var metadata = await reader.ReadMetadataAsync();
            Assert.True(metadata.RowGroups[0].Columns.Count * ranges.Length > 2 * BatchPageWindow.MaxRangesPerWindow);
            int before = counting.Requests.Count;
            await foreach (var batch in reader.ReadRowRangesAsync(0, ranges))
                batch.Dispose();

            var requests = counting.Requests.Skip(before).ToList();
            _output.WriteLine($"{requests.Count} requests of {string.Join(", ", requests)} ranges");
            Assert.All(requests, count => Assert.True(count <= BatchPageWindow.MaxRangesPerWindow, $"a request of {count} ranges"));
            Assert.InRange(requests.Count, 3, ranges.Length);
        }

        await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, ranges, await FullRowsAsync(path, 0), batchSize: null);
    }

    /// <summary>
    /// One-row batches over pages that fit the byte budget at once: a window plans at most
    /// <see cref="BatchPageWindow.MaxBatchesPerWindow"/> of them rather than every batch of the row
    /// group, and a window whose batches need no new pages asks for nothing.
    /// </summary>
    [Fact]
    public async Task OneRowBatches_WindowsAreCappedInBatches()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });

        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, new ParquetReadOptions { BatchSize = 1 });
        await reader.ReadMetadataAsync();
        int before = counting.Requests.Count;
        int batches = 0;
        await foreach (var batch in reader.ReadRowGroupBatchesAsync(0))
        {
            batches++;
            batch.Dispose();
        }

        // The OffsetIndex read, then one request per window that reaches new pages.
        int windows = counting.Requests.Count - before - 1;
        Assert.Equal(Rows, batches);
        Assert.InRange(windows, 2, (Rows + BatchPageWindow.MaxBatchesPerWindow - 1) / BatchPageWindow.MaxBatchesPerWindow);
    }

    // ───── Arguments ─────

    [Theory]
    [InlineData(10, 10)]   // empty
    [InlineData(-1, 10)]   // before the row group
    [InlineData(4990, 5001)] // past it
    public async Task InvalidRange_Throws(long start, long end)
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default);
        using var reader = Open(path);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var batch in reader.ReadRowRangesAsync(0, [new RowRange(start, end)]))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task OverlappingOrUnorderedRanges_Throw()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default);
        using var reader = Open(path);

        foreach (var ranges in new[] { new[] { new RowRange(0, 100), new RowRange(50, 150) }, [new RowRange(200, 300), new RowRange(0, 100)] })
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await foreach (var batch in reader.ReadRowRangesAsync(0, ranges))
                    batch.Dispose();
            });
        }
    }

    [Fact]
    public async Task NoRanges_NoBatches_AndTouchingRangesAreAllowed()
    {
        string path = await WriteAsync(Flat(Rows), ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        using var reader = Open(path);

        await foreach (var _ in reader.ReadRowRangesAsync(0, []))
            Assert.Fail("a batch from no ranges");

        var expected = await FullRowsAsync(path, 0);
        await AssertRangeReadAsync(path, ParquetReadOptions.Default, 0, [new RowRange(0, 100), new RowRange(100, 300)], expected, batchSize: null);
    }

    // ───── Helpers ─────

    /// <summary>
    /// Single rows at both ends, a page exactly, ranges that cross pages and sit inside one, many small
    /// ranges, and the whole row group.
    /// </summary>
    private static IEnumerable<RowRange[]> RangeSets(int rows)
    {
        yield return [new RowRange(0, 1)];
        yield return [new RowRange(rows - 1, rows)];
        yield return [new RowRange(250, 500)];
        yield return [new RowRange(100, 120), new RowRange(130, 131), new RowRange(1000, 2600), new RowRange(rows - 200, rows)];
        yield return [new RowRange(0, rows)];

        var random = new Random(430);
        var many = new List<RowRange>();
        long at = random.Next(50);
        while (at < rows)
        {
            long end = Math.Min(rows, at + 1 + random.Next(40));
            many.Add(new RowRange(at, end));
            at = end + 1 + random.Next(400);
        }

        yield return [.. many];
    }

    private async Task AssertRangeReadAsync(
        string path, ParquetReadOptions options, int rowGroup, IReadOnlyList<RowRange> ranges, List<string> expected, int? batchSize,
        long? windowBudget = null, long? headerScanBudget = null)
    {
        await using var input = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(input, ownsFile: false, options);
        if (windowBudget is { } budget)
            reader.BatchPageWindowBudgetBytes = budget;
        if (headerScanBudget is { } scanBudget)
            reader.HeaderScanBudgetBytes = scanBudget;

        var actual = new List<string>();
        int range = 0;
        long intoRange = 0;
        await foreach (var batch in reader.ReadRowRangesAsync(rowGroup, ranges))
        {
            using (batch)
            {
                Assert.True(batch.Length > 0, "an empty batch");
                if (batchSize is { } limit)
                    Assert.True(batch.Length <= limit, $"a batch of {batch.Length} rows over {limit}");

                // A batch lies inside one range: it never spans the gap to the next.
                Assert.True(range < ranges.Count, "more rows than the ranges hold");
                Assert.True(intoRange + batch.Length <= ranges[range].Length,
                    $"a batch of {batch.Length} rows crosses the end of range {ranges[range]}");
                intoRange += batch.Length;
                if (intoRange == ranges[range].Length)
                {
                    range++;
                    intoRange = 0;
                }

                for (int r = 0; r < batch.Length; r++)
                    actual.Add(ArrowValues.RenderRow(batch, r));
            }
        }

        var wanted = ranges.SelectMany(r => expected.Skip((int)r.Start).Take((int)r.Length)).ToList();
        Assert.Equal(wanted.Count, actual.Count);
        for (int i = 0; i < wanted.Count; i++)
            Assert.True(wanted[i] == actual[i], $"row {i} of the ranges {string.Join(" ", ranges)}: expected {wanted[i]}, got {actual[i]}");
    }

    private static async Task<List<string>> FullRowsAsync(string path, int rowGroup)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true);
        using var batch = await reader.ReadRowGroupAsync(rowGroup);
        return Enumerable.Range(0, batch.Length).Select(r => ArrowValues.RenderRow(batch, r)).ToList();
    }

    private static async Task<List<string>> ReadAllRowsAsync(string path, ParquetReadOptions options)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true, options);
        var rows = new List<string>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                    rows.Add(ArrowValues.RenderRow(batch, r));
            }
        }

        return rows;
    }

    /// <summary>The requests a whole filtered read makes, footer included.</summary>
    private static async Task<int> RequestsAsync(string path, ParquetReadOptions options)
    {
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, options);
        await foreach (var batch in reader.ReadAllAsync())
            batch.Dispose();
        return counting.Requests.Count;
    }

    /// <summary>The bytes a read asks for, footer and index included.</summary>
    private static async Task<long> BytesReadAsync(
        string path, Func<ParquetFileReader, IAsyncEnumerable<RecordBatch>> read, ParquetReadOptions? options = null)
    {
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, options);
        await reader.ReadMetadataAsync();
        long before = counting.Bytes;
        await foreach (var batch in read(reader))
            batch.Dispose();
        return counting.Bytes - before;
    }

    private static bool Unreadable(Exception e) => e switch
    {
        NotSupportedException or ParquetFormatException => true,
        AggregateException aggregate => aggregate.InnerExceptions.All(Unreadable),
        _ => false,
    };

    /// <summary>
    /// Flat columns whose encodings differ page to page: a sorted long, a random int, a low-cardinality
    /// string (dictionary), unique strings, a double, a bool, a timestamp, and a nullable int with runs
    /// of nulls, some a whole page long.
    /// </summary>
    private static RecordBatch Flat(int rows)
    {
        var random = new Random(3);
        var sorted = new Int64Array.Builder();
        var ints = new Int32Array.Builder();
        var low = new StringArray.Builder();
        var unique = new StringArray.Builder();
        var doubles = new DoubleArray.Builder();
        var flags = new BooleanArray.Builder();
        var times = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var nullable = new Int32Array.Builder();
        var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int i = 0; i < rows; i++)
        {
            sorted.Append(i);
            ints.Append(random.Next());
            low.Append("k" + random.Next(20));
            unique.Append($"value-{random.Next():x8}-{i}");
            doubles.Append(random.NextDouble());
            flags.Append(random.Next(3) == 0);
            times.Append(epoch.AddSeconds(i));
            if (i % 700 < 300 || (i >= 2500 && i < 2750)) nullable.AppendNull(); else nullable.Append(i);
        }

        return new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("sorted", Int64Type.Default, false))
                .Field(new Field("ints", Int32Type.Default, false))
                .Field(new Field("low", StringType.Default, false))
                .Field(new Field("unique", StringType.Default, false))
                .Field(new Field("doubles", DoubleType.Default, false))
                .Field(new Field("flags", BooleanType.Default, false))
                .Field(new Field("times", new TimestampType(TimeUnit.Microsecond, "UTC"), false))
                .Field(new Field("nullable", Int32Type.Default, true))
                .Build(),
            [sorted.Build(), ints.Build(), low.Build(), unique.Build(), doubles.Build(), flags.Build(), times.Build(), nullable.Build()],
            rows);
    }

    /// <summary><paramref name="flat"/> plus a list of ints and a struct.</summary>
    private static RecordBatch WithNested(RecordBatch flat)
    {
        var list = new ListArray.Builder(Int32Type.Default);
        var elements = (Int32Array.Builder)list.ValueBuilder;
        var inner = new Int64Array.Builder();
        for (int i = 0; i < flat.Length; i++)
        {
            if (i % 11 == 0)
            {
                list.AppendNull();
            }
            else
            {
                list.Append();
                for (int j = 0; j < i % 4; j++)
                    elements.Append(i * 10 + j);
            }

            inner.Append(-i);
        }

        var listArray = list.Build();
        var structType = new StructType([new Field("a", Int64Type.Default, false)]);
        var fields = flat.Schema.FieldsList.ToList();
        fields.Add(new Field("list", listArray.Data.DataType, true));
        fields.Add(new Field("struct", structType, false));
        var arrays = Enumerable.Range(0, flat.ColumnCount).Select(flat.Column).ToList();
        arrays.Add(listArray);
        arrays.Add(new StructArray(structType, flat.Length, [inner.Build()], ArrowBuffer.Empty, 0));
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, flat.Length);
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

    private static ParquetFileReader Open(string path) =>
        new(new LocalRandomAccessFile(path), ownsFile: true);
}
