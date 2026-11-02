// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// <see cref="ParquetFileReader.ReadWithPositionsAsync(Predicate?, IReadOnlyList{string}?, RowSkipPredicate?, CancellationToken)"/>:
/// the row groups a <see cref="RowSkipPredicate"/> skips are never read, not even their dictionaries or page
/// indexes for pruning, and the rows after them keep their file positions.
/// </summary>
public class RowSkipReadTests : IDisposable
{
    private const int Rows = 5000;
    private const int GroupRows = 1000;

    private readonly string _tempDir;

    public RowSkipReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-rowskip-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// A row group is skipped only when every one of its rows is excluded: exactly one group, two adjacent
    /// ones, a group plus a row of the next (the next is read), and a group less one row (nothing skipped).
    /// </summary>
    [Theory]
    [InlineData(1000, 2000, new[] { 1 })]
    [InlineData(1000, 3000, new[] { 1, 2 })]
    [InlineData(1000, 2001, new[] { 1 })]
    [InlineData(1001, 2000, new int[0])]
    [InlineData(0, 5000, new[] { 0, 1, 2, 3, 4 })]
    public async Task SkipsExactlyTheGroupsEveryRowOfWhichIsExcluded(long from, long to, int[] skippedGroups)
    {
        string path = await WriteAsync();
        var asked = new List<(long, long)>();
        bool Skip(long first, long count)
        {
            asked.Add((first, count));
            return first >= from && first + count <= to;
        }

        var positions = await PositionsAsync(path, new ParquetReadOptions(), filter: null, Skip);

        Assert.Equal(Enumerable.Range(0, Rows / GroupRows).Select(g => ((long)g * GroupRows, (long)GroupRows)), asked);
        var expected = Enumerable.Range(0, Rows).Select(i => (long)i)
            .Where(p => !skippedGroups.Contains((int)(p / GroupRows)));
        Assert.Equal(expected, positions);
    }

    /// <summary>
    /// A skipped group's column chunks are never requested, nor what pruning would read of them for it: with a
    /// predicate statistics leave undecided, the dictionary and Bloom read-ahead for the kept groups leaves the
    /// skipped groups out of its windows. Both read exact ranges, so a skipped group's bytes stay unread.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ASkippedGroupsChunksAreNeverRead(bool withFilter, bool bloom)
    {
        string path = await WriteAsync(bloom);
        int[] skippedGroups = [1, 3];

        // "low" holds k0..k19 in every group, so statistics cannot decide k3: each kept group's dictionary,
        // Bloom filter (when written) and page index are all consulted.
        var options = new ParquetReadOptions { FilterUseBloomFilters = bloom };
        Predicate? filter = withFilter ? Ex.Equal("low", LiteralValue.Of("k3")) : null;
        var (metadata, requested, positions) = await ReadRecordingAsync(path, options, filter, skippedGroups);

        Assert.DoesNotContain(positions, p => skippedGroups.Contains((int)(p / GroupRows)));
        Assert.Contains(positions, p => p / GroupRows == 4);
        foreach (int g in skippedGroups)
            AssertUnread(requested, ChunkRanges(metadata.RowGroups[g]));

        // The kept groups' dictionaries (and Bloom filters) were read ahead in one request, so the window did
        // span the skipped groups' positions: the check above is not vacuous.
        if (withFilter)
        {
            var dictionary = metadata.RowGroups[0].Columns[2].MetaData!.DictionaryPageOffset;
            Assert.Contains(requested, r => r.Offset == dictionary);
        }
    }

    /// <summary>
    /// The page-index read-ahead leaves skipped groups out too. Page indexes sit together at the end of the
    /// file and are read as one span across small gaps, so a skipped group's index BETWEEN kept groups' indexes
    /// is read with them (bytes, not a request); one past the last kept group's is not read at all.
    /// </summary>
    [Fact]
    public async Task TrailingSkippedGroupsPageIndexesAreNeverRead()
    {
        string path = await WriteAsync();
        int[] skippedGroups = [3, 4];
        var filter = Ex.Equal("low", LiteralValue.Of("k3"));
        var (metadata, requested, _) = await ReadRecordingAsync(path, new ParquetReadOptions(), filter, skippedGroups);

        // The OffsetIndexes follow every ColumnIndex, so the trailing groups' OffsetIndexes lie past the span.
        foreach (int g in skippedGroups)
        {
            AssertUnread(requested, metadata.RowGroups[g].Columns
                .Select(c => (c.OffsetIndexOffset!.Value, (long)c.OffsetIndexLength!.Value)));
        }

        var kept = metadata.RowGroups[2].Columns[2];
        Assert.Contains(requested, r => r.Offset <= kept.OffsetIndexOffset && kept.OffsetIndexOffset < r.Offset + r.Length);
    }

    /// <summary>Statistics pruning and the skip work together: each removes its own groups.</summary>
    [Fact]
    public async Task StatisticsPruningAndSkippingCompose()
    {
        string path = await WriteAsync();
        var filter = Ex.GreaterThanOrEqual("sorted", LiteralValue.Of(2500L)); // statistics drop groups 0 and 1
        bool Skip(long first, long count) => first == 3000;                  // group 3

        var positions = await PositionsAsync(path, new ParquetReadOptions(), filter, Skip);

        // Group 2 is kept by statistics; the page index narrows it to the pages from 2500 on.
        Assert.Equal(
            Enumerable.Range(2500, 500).Concat(Enumerable.Range(4000, 1000)).Select(i => (long)i),
            positions);
    }

    /// <summary>With no skip predicate, the read is the read without one, request for request.</summary>
    [Fact]
    public async Task NoSkipPredicateReadsAsBefore()
    {
        string path = await WriteAsync();
        var filter = Ex.Equal("low", LiteralValue.Of("k3"));

        var before = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(before, ownsFile: true))
            // The token-taking overload keeps both three-argument shapes, positional token and `default`:
            // the skip overload must not make either ambiguous or move the token.
            await foreach (var positioned in reader.ReadWithPositionsAsync(filter, null, CancellationToken.None))
                positioned.Batch.Dispose();
        using (var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true))
            await foreach (var positioned in reader.ReadWithPositionsAsync(filter, null, default))
                positioned.Batch.Dispose();

        var after = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(after, ownsFile: true))
            await foreach (var positioned in reader.ReadWithPositionsAsync(filter, null, (_, _) => false, CancellationToken.None))
                positioned.Batch.Dispose();

        Assert.Equal(before.Requests, after.Requests);
        Assert.Equal(before.Bytes, after.Bytes);
    }

    // ---- runs inside a kept row group (#618 step 4) ---------------------------------------------------------

    private const int OneGroupRows = 100_000;

    /// <summary>Deleted row ranges, as a skip predicate: a span is skipped when it lies inside one of them.</summary>
    private static RowSkipPredicate Excluding(params (long Start, long End)[] runs) =>
        (first, count) => runs.Any(r => first >= r.Start && first + count <= r.End);

    /// <summary>
    /// A run of at least two probe blocks is found, and widened to its exact ends; a run shorter than one block
    /// is not (its rows are dropped by the caller as before). Runs at the group's ends, and two runs, likewise.
    /// </summary>
    [Fact]
    public void SkippedRuns_FindsLongRunsExactly()
    {
        // 100,000 rows: blocks of 1,563.
        Assert.Equal([new RowRange(40_017, 60_003)],
            ParquetFileReader.SkippedRuns(Excluding((40_017, 60_003)), 0, OneGroupRows));
        Assert.Equal([new RowRange(0, 5_000), new RowRange(97_001, OneGroupRows)],
            ParquetFileReader.SkippedRuns(Excluding((0, 5_000), (97_001, OneGroupRows)), 0, OneGroupRows));
        Assert.Null(ParquetFileReader.SkippedRuns(Excluding((50_000, 51_000)), 0, OneGroupRows));

        // Positions are the file's: the same run in a group starting at row 1,000,000.
        Assert.Equal([new RowRange(40_017, 60_003)],
            ParquetFileReader.SkippedRuns(Excluding((1_040_017, 1_060_003)), 1_000_000, OneGroupRows));

        // A group no bigger than one block is never probed.
        int asked = 0;
        Assert.Null(ParquetFileReader.SkippedRuns((_, _) => { asked++; return true; }, 0, 1000));
        Assert.Equal(0, asked);
    }

    [Fact]
    public void SubtractRanges_LeavesTheRowsOutsideTheCut()
    {
        RowRange[] whole = [new RowRange(0, 100)];
        Assert.Equal([new RowRange(0, 10), new RowRange(20, 100)],
            ParquetFileReader.SubtractRanges(whole, [new RowRange(10, 20)]));
        Assert.Equal([new RowRange(10, 20)],
            ParquetFileReader.SubtractRanges(whole, [new RowRange(0, 10), new RowRange(20, 100)]));
        Assert.Empty(ParquetFileReader.SubtractRanges(whole, whole));

        // Against several ranges, as the page index leaves them: a cut spanning a gap, one inside a range.
        RowRange[] narrowed = [new RowRange(0, 30), new RowRange(50, 80), new RowRange(90, 100)];
        Assert.Equal([new RowRange(0, 20), new RowRange(60, 70), new RowRange(75, 80), new RowRange(90, 100)],
            ParquetFileReader.SubtractRanges(narrowed, [new RowRange(20, 60), new RowRange(70, 75)]));
    }

    /// <summary>
    /// A long deleted run inside the one row group is not read: every row outside it comes back at its file
    /// position, and the read fetches well under what it fetches without the skip.
    /// </summary>
    [Theory]
    [InlineData(30_000, 70_000)]
    [InlineData(0, 50_000)]
    [InlineData(60_000, OneGroupRows)]
    public async Task ALongRunInsideAKeptGroup_IsNotRead(long from, long to)
    {
        string path = await WriteOneGroupAsync();
        var (positions, bytes) = await ReadOneGroupAsync(path, Excluding((from, to)));
        var (allPositions, allBytes) = await ReadOneGroupAsync(path, (_, _) => false);

        Assert.Equal(Enumerable.Range(0, OneGroupRows).Select(i => (long)i), allPositions);
        Assert.Equal(allPositions.Where(p => p < from || p >= to), positions);
        double kept = 1 - (to - from) / (double)OneGroupRows;
        Assert.True(bytes < allBytes * (kept + 0.1), $"read {bytes:N0} of {allBytes:N0} bytes, keeping {kept:P0} of the rows");
    }

    /// <summary>
    /// Many runs, short ones the probe cannot see among them: the rows outside the runs all come back, each at
    /// its position, and the skipped rows that do come back are only those of runs too short to cut.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(777)]
    public async Task ManyRuns_RowsArePositionedExactly(int? batchSize)
    {
        string path = await WriteOneGroupAsync();
        (long Start, long End)[] runs =
            [(1_000, 1_500), (5_000, 15_000), (20_000, 20_100), (33_333, 66_666), (80_000, 84_000), (99_990, OneGroupRows)];
        var (positions, _) = await ReadOneGroupAsync(path, Excluding(runs), new ParquetReadOptions { BatchSize = batchSize });

        bool InRun(long p) => runs.Any(r => p >= r.Start && p < r.End);
        Assert.Equal(Enumerable.Range(0, OneGroupRows).Select(i => (long)i).Where(p => !InRun(p)),
            positions.Where(p => !InRun(p)));
        Assert.All(positions.Where(InRun), p => Assert.True(
            runs.Any(r => p >= r.Start && p < r.End && r.End - r.Start < 2 * 1563), $"row {p} of a long run was read"));
        Assert.Equal(positions.Distinct().Count(), positions.Count);
    }

    /// <summary>
    /// Without an OffsetIndex, a ranged read would scan each chunk's headers, reading it whole: the run is not
    /// cut, and the read is the read without the skip, request for request.
    /// </summary>
    [Fact]
    public async Task WithoutAnOffsetIndex_NothingIsCut()
    {
        string path = await WriteOneGroupAsync(pageIndex: false);
        var skip = Excluding((30_000, 70_000));

        var with = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(with, ownsFile: true))
            await foreach (var positioned in reader.ReadWithPositionsAsync(null, null, skip, CancellationToken.None))
                positioned.Batch.Dispose();
        var without = new RequestCountingFile(new LocalRandomAccessFile(path));
        using (var reader = new ParquetFileReader(without, ownsFile: true))
            await foreach (var positioned in reader.ReadWithPositionsAsync(null, null, (_, _) => false, CancellationToken.None))
                positioned.Batch.Dispose();

        Assert.Equal(without.Requests, with.Requests);
        Assert.Equal(without.Bytes, with.Bytes);
    }

    /// <summary>The page index's narrowing and the cut compose: the rows are those the page index keeps less the run.</summary>
    [Fact]
    public async Task PageIndexNarrowingAndTheCutCompose()
    {
        string path = await WriteOneGroupAsync();
        var filter = Ex.LessThan("sorted", LiteralValue.Of(80_000L));
        var positions = await PositionsAsync(path, new ParquetReadOptions(), filter, Excluding((10_000, 50_000)));

        Assert.Equal(
            Enumerable.Range(0, 10_000).Concat(Enumerable.Range(50_000, 30_000)).Select(i => (long)i),
            positions);
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    /// <summary>A skipped group's column chunks, from the first page, and its Bloom filters.</summary>
    private static IEnumerable<(long Offset, long Length)> ChunkRanges(RowGroup rowGroup)
    {
        foreach (var chunk in rowGroup.Columns)
        {
            var meta = chunk.MetaData!;
            long start = meta.DictionaryPageOffset is { } d && d > 0 && d < meta.DataPageOffset
                ? d : meta.DataPageOffset;
            yield return (start, meta.TotalCompressedSize);
            if (meta.BloomFilterOffset is { } bf)
                yield return (bf, meta.BloomFilterLength ?? 1);
        }
    }

    private static void AssertUnread(List<FileRange> requested, IEnumerable<(long Offset, long Length)> ranges)
    {
        foreach (var (offset, length) in ranges)
        {
            // The one overlap allowed: the reader pads a dictionary chunk's read by up to 100 bytes past its end
            // (the PARQUET-816 workaround), which can run into the next group's first chunk.
            Assert.DoesNotContain(requested, r =>
                r.Offset < offset + length && offset < r.Offset + r.Length
                && !(r.Offset < offset && r.Offset + r.Length <= offset + 100));
        }
    }

    /// <summary>
    /// Reads every row, skipping <paramref name="skippedGroups"/>, recording each range requested and checking each
    /// row's position against its batch's FirstRow.
    /// </summary>
    private static async Task<(FileMetaData Metadata, List<FileRange> Requested, List<long> Positions)> ReadRecordingAsync(
        string path, ParquetReadOptions options, Predicate? filter, int[] skippedGroups)
    {
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, options);
        var metadata = await reader.ReadMetadataAsync();
        var positions = new List<long>();
        await foreach (var positioned in reader.ReadWithPositionsAsync(
            filter, null, (first, _) => skippedGroups.Contains((int)(first / GroupRows)), CancellationToken.None))
        {
            using var batch = positioned.Batch;
            var sorted = (Int64Array)batch.Column("sorted");
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(positioned.FirstRow + i, sorted.GetValue(i));
                positions.Add(positioned.FirstRow + i);
            }
        }

        return (metadata, counting.RequestRanges.SelectMany(r => r).ToList(), positions);
    }

    private static async Task<List<long>> PositionsAsync(
        string path, ParquetReadOptions options, Predicate? filter, RowSkipPredicate skip)
    {
        using var reader = new ParquetFileReader(new LocalRandomAccessFile(path), ownsFile: true, options);
        var positions = new List<long>();
        await foreach (var positioned in reader.ReadWithPositionsAsync(filter, null, skip, CancellationToken.None))
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

    /// <summary>Reads every column of the one-group file with positions, returning them and the bytes read.</summary>
    private static async Task<(List<long> Positions, long Bytes)> ReadOneGroupAsync(
        string path, RowSkipPredicate skip, ParquetReadOptions? options = null)
    {
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        using var reader = new ParquetFileReader(counting, ownsFile: true, options);
        await reader.ReadMetadataAsync();
        long before = counting.Bytes;
        var positions = new List<long>();
        await foreach (var positioned in reader.ReadWithPositionsAsync(null, null, skip, CancellationToken.None))
        {
            using var batch = positioned.Batch;
            var sorted = (Int64Array)batch.Column("sorted");
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(positioned.FirstRow + i, sorted.GetValue(i));
                positions.Add(positioned.FirstRow + i);
            }
        }

        return (positions, counting.Bytes - before);
    }

    /// <summary>One row group of <see cref="OneGroupRows"/> rows in 1,000-row pages.</summary>
    private async Task<string> WriteOneGroupAsync(bool pageIndex = true)
    {
        var random = new Random(6184);
        var sorted = new Int64Array.Builder();
        var ints = new Int32Array.Builder();
        var text = new StringArray.Builder();
        for (int i = 0; i < OneGroupRows; i++)
        {
            sorted.Append(i);
            ints.Append(random.Next());
            text.Append("v" + random.Next().ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("sorted", Int64Type.Default, false))
                .Field(new Field("ints", Int32Type.Default, false))
                .Field(new Field("text", StringType.Default, false))
                .Build(),
            [sorted.Build(), ints.Build(), text.Build()],
            OneGroupRows);

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, ParquetWriteOptions.Default with
        {
            RowGroupMaxRows = OneGroupRows,
            DataPageRowCountLimit = 1000,
            WritePageIndex = pageIndex,
        });
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    private async Task<string> WriteAsync(bool bloom = false)
    {
        var random = new Random(618);
        var sorted = new Int64Array.Builder();
        var ints = new Int32Array.Builder();
        var low = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            sorted.Append(i);
            ints.Append(random.Next());
            low.Append("k" + (i % 20));
        }

        var batch = new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field("sorted", Int64Type.Default, false))
                .Field(new Field("ints", Int32Type.Default, false))
                .Field(new Field("low", StringType.Default, false))
                .Build(),
            [sorted.Build(), ints.Build(), low.Build()],
            Rows);

        var options = ParquetWriteOptions.Default with
        {
            RowGroupMaxRows = GroupRows,
            DataPageRowCountLimit = 250,
        };
        if (bloom)
            options = options with { BloomFilterColumns = new HashSet<string> { "low" } };

        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }
}
