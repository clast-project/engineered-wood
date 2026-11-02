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
