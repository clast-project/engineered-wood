// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// The two-pass selected read (#502 phase 1b): the output columns no predicate reads are decoded only over the
/// batches where a row was selected. <see cref="SelectedReadTests"/> checks the rows it returns; this checks
/// that the skipped batches' pages really are never decoded.
/// </summary>
/// <remarks>
/// The probe is a page checksum. One page of the output column <c>note</c>, far from any selected row, has a
/// byte flipped, and the read validates checksums when it decodes a page. A one-pass read decodes that page
/// and fails; a two-pass read never decodes it and succeeds. A two-pass read whose selection DOES reach that
/// page fails too, which shows the corruption is caught when the page is decoded at all.
/// </remarks>
public class TwoPassReadTests : IDisposable
{
    private const int Rows = 40_000;
    private readonly string _tempDir;

    public TwoPassReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-twopass-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    [Fact]
    public async Task ABatchWithNoSelectedRowIsNeverDecoded()
    {
        var (path, corruptFrom) = await WriteCorruptedAsync();
        Assert.True(corruptFrom >= 3 * ParquetFileReader.TwoPassBlockRows, "the corrupted page must lie past the selected rows' batches");

        // Selects rows in the first batch only; the corrupted page is batches away.
        var scan = Scan(maxId: 1_000);
        var rows = await ReadIdsAsync(path, scan, twoPass: true);
        Assert.Equal(Enumerable.Range(0, 1_000).Select(i => (long)i), rows);
    }

    /// <summary>
    /// The same with pruning by a predicate the page index cannot narrow: pruning then hands back the whole row
    /// group as one range rather than none, and the read must still go in two passes.
    /// </summary>
    [Fact]
    public async Task ABatchWithNoSelectedRowIsNeverDecodedWhenPruningNarrowsNothing()
    {
        var (path, _) = await WriteCorruptedAsync();

        // Every row satisfies id >= 0, so the page index keeps every page.
        var scan = Scan(maxId: 1_000) with
        {
            Prune = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", EngineeredWood.Expressions.LiteralValue.Of(0L)),
        };
        var rows = await ReadIdsAsync(path, scan, twoPass: true);
        Assert.Equal(Enumerable.Range(0, 1_000).Select(i => (long)i), rows);
    }

    /// <summary>
    /// By default, two passes only for a file on an object store: a local file reads in one pass (and so decodes
    /// the corrupted page), one reporting a concurrency hint reads in two (and never does).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ByDefaultOnlyAnObjectStoreFileReadsInTwoPasses(bool objectStore)
    {
        var (path, _) = await WriteCorruptedAsync();
        var error = await Record.ExceptionAsync(() => ReadIdsAsync(path, Scan(maxId: 1_000), twoPass: null, objectStore));
        if (objectStore)
            Assert.Null(error);
        else
            Assert.NotNull(error);
    }

    /// <summary>A local file that says what an object-store reader says: how many reads it serves at once.</summary>
    private sealed class ObjectStoreLike(IRandomAccessFile inner) : IRandomAccessFile, IReadConcurrencyHint
    {
        public int? PreferredConcurrentReads => 8;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) => inner.GetLengthAsync(cancellationToken);

        public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(range, cancellationToken);

        public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default) =>
            inner.ReadRangesAsync(ranges, cancellationToken);

        public ValueTask DisposeAsync() => default; // the local file is disposed by its owner

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// The first pass never hands a predicate more rows at once than <see cref="ParquetReadOptions.BatchSize"/>:
    /// it follows the batch size when the predicate's column is not returned, and when it is (and so would be kept
    /// whole for the group) the group is read in one pass, which follows it too. The rows are right either way.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APredicateNeverSeesMoreRowsThanTheBatchSize(bool returnPredicateColumn)
    {
        var (path, corruptFrom) = await WriteCorruptedAsync();
        var sizes = new System.Collections.Concurrent.ConcurrentBag<int>();
        var scan = new ParquetScan
        {
            Columns = returnPredicateColumn ? ["id", "note"] : ["note"],
            RowFilter = new RowFilter(new RowPredicate(["id"], batch =>
            {
                sizes.Add(batch.Length);
                var ids = (Int64Array)batch.Column(0);
                var builder = new BooleanArray.Builder().Reserve(ids.Length);
                for (int i = 0; i < ids.Length; i++)
                    builder.Append(ids.GetValue(i) < 1_000);
                return builder.Build();
            })),
        };

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { BatchSize = 3_000 })
        {
            TwoPassReads = true,
        };
        long rows = 0;
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
                rows += selected.Batch.Length;
        }

        Assert.Equal(1_000, rows);
        Assert.NotEmpty(sizes);
        Assert.All(sizes, size => Assert.InRange(size, 0, 3_000));
        Assert.True(corruptFrom > 0); // the corrupted page is past the selection; validation is off here anyway
    }

    /// <summary>
    /// A two-pass read builds its second pass's page maps from the prefetched chunks' headers: it never requests an
    /// OffsetIndex, which would be a request made after the first pass, the round trip the prefetch avoids.
    /// </summary>
    [Fact]
    public async Task ATwoPassReadNeverRequestsAnOffsetIndex()
    {
        var (path, _) = await WriteCorruptedAsync();
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        EngineeredWood.Parquet.Metadata.FileMetaData metadata;
        await using (counting)
        using (var reader = new ParquetFileReader(counting, ownsFile: false) { TwoPassReads = true })
        {
            metadata = await reader.ReadMetadataAsync();
            var ids = new List<long>();
            await foreach (var selected in reader.ReadSelectedAsync(Scan(maxId: 1_000)))
            {
                using (selected)
                    ids.Add(selected.Batch.Length);
            }

            Assert.Equal(1_000, ids.Sum());
        }

        // The footer read takes the file's last bytes, which in a file this small hold the page indexes too; it is
        // not a request for them, so it is left out.
        long fileLength = new FileInfo(path).Length;
        var requested = counting.RequestRanges.SelectMany(r => r).Where(r => r.Offset + r.Length != fileLength).ToList();
        foreach (var chunk in metadata.RowGroups[0].Columns)
        {
            long offset = chunk.OffsetIndexOffset!.Value, length = chunk.OffsetIndexLength!.Value;
            Assert.DoesNotContain(requested, r => r.Offset < offset + length && offset < r.Offset + r.Length);
        }
    }

    /// <summary>
    /// A first pass cut into batches builds page maps, from the OffsetIndexes that planning already read with the
    /// page index: it does not request them again. The footer is read exactly, so that its read holds no index,
    /// and the prune is one only the page index can judge, so that planning reads the indexes at all.
    /// </summary>
    [Fact]
    public async Task ABatchedFirstPassReusesThePlannedOffsetIndexes()
    {
        var (path, _) = await WriteCorruptedAsync();
        var counting = new RequestCountingFile(new LocalRandomAccessFile(path));
        EngineeredWood.Parquet.Metadata.FileMetaData metadata;
        await using (counting)
        using (var reader = new ParquetFileReader(counting, ownsFile: false,
                   new ParquetReadOptions { BatchSize = 3_000, FooterReadSize = 0 }) { TwoPassReads = true })
        {
            metadata = await reader.ReadMetadataAsync();
            var scan = Scan(maxId: 1_000) with
            {
                Columns = ["note"], // the predicate column is not returned, so the first pass follows BatchSize
                // Undecided by the row group's statistics (its minimum is 0), so the page index is read; every page
                // holds an id of 1 or more, so it keeps them all and hands back the whole group as one range.
                Prune = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", EngineeredWood.Expressions.LiteralValue.Of(1L)),
            };
            long rows = 0;
            await foreach (var selected in reader.ReadSelectedAsync(scan))
            {
                using (selected)
                    rows += selected.Batch.Length;
            }

            Assert.Equal(1_000, rows);
        }

        // The page index's one read-ahead request holds every index; nothing asks for one again.
        var indexes = metadata.RowGroups[0].Columns
            .SelectMany(c => new[] { (c.ColumnIndexOffset, c.ColumnIndexLength), (c.OffsetIndexOffset, c.OffsetIndexLength) })
            .Where(r => r.Item1 is not null)
            .Select(r => (Offset: r.Item1!.Value, Length: (long)r.Item2!.Value))
            .ToList();
        int requests = counting.RequestRanges.Count(request => request.Any(
            r => indexes.Any(ix => r.Offset < ix.Offset + ix.Length && ix.Offset < r.Offset + r.Length)));
        Assert.Equal(1, requests);
    }

    [Fact]
    public async Task AOnePassReadDecodesTheSamePageAndFails()
    {
        var (path, _) = await WriteCorruptedAsync();
        var error = await Record.ExceptionAsync(() => ReadIdsAsync(path, Scan(maxId: 1_000), twoPass: false));
        Assert.NotNull(error);
    }

    [Fact]
    public async Task ATwoPassReadWhoseSelectionReachesThePageFails()
    {
        var (path, corruptFrom) = await WriteCorruptedAsync();
        var error = await Record.ExceptionAsync(() => ReadIdsAsync(path, Scan(maxId: corruptFrom + 1), twoPass: true));
        Assert.NotNull(error);
    }

    // Rows whose id is below maxId, read through an opaque predicate (no pruning narrows the row group, so the
    // read is eligible for two passes), returning id and note.
    private static ParquetScan Scan(long maxId) => new()
    {
        Columns = ["id", "note"],
        RowFilter = new RowFilter(new RowPredicate(["id"], batch =>
        {
            var ids = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder().Reserve(ids.Length);
            for (int i = 0; i < ids.Length; i++)
                builder.Append(ids.GetValue(i) < maxId);
            return builder.Build();
        })),
    };

    private static async Task<List<long>> ReadIdsAsync(string path, ParquetScan scan, bool? twoPass, bool objectStore = false)
    {
        await using var local = new LocalRandomAccessFile(path);
        IRandomAccessFile file = objectStore ? new ObjectStoreLike(local) : local;
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { PageChecksumValidation = true })
        {
            TwoPassReads = twoPass,
        };
        var ids = new List<long>();
        await foreach (var selected in reader.ReadSelectedAsync(scan))
        {
            using (selected)
            {
                var column = (Int64Array)selected.Batch.Column(0);
                var notes = (StringArray)selected.Batch.Column(1);
                for (int i = 0; i < column.Length; i++)
                {
                    ids.Add(column.GetValue(i)!.Value);
                    Assert.Equal(NoteFor(column.GetValue(i)!.Value), notes.GetString(i));
                }
            }
        }

        return ids;
    }

    private static string NoteFor(long id) => "note-" + id.ToString("D12", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// One row group of 40,000 rows in 2,000-row pages, with page checksums, and one byte flipped at the end of the
    /// <c>note</c> page holding row 30,000. Returns the first row of that page.
    /// </summary>
    private async Task<(string Path, long CorruptFrom)> WriteCorruptedAsync()
    {
        string path = Path.Combine(_tempDir, "data.parquet");
        var ids = new Int64Array.Builder();
        var notes = new StringArray.Builder();
        for (long i = 0; i < Rows; i++)
        {
            ids.Append(i);
            notes.Append(NoteFor(i));
        }

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("note", StringType.Default, false))
            .Build();
        using (var batch = new RecordBatch(schema, [ids.Build(), notes.Build()], Rows))
        {
            await using var output = new LocalSequentialFile(path);
            await using var writer = new ParquetFileWriter(output, ownsFile: false, ParquetWriteOptions.Default with
            {
                RowGroupMaxRows = Rows,
                DataPageRowCountLimit = 2_000,
                PageChecksumEnabled = true,
            });
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        long offset, firstRow;
        await using (var file = new LocalRandomAccessFile(path))
        using (var reader = new ParquetFileReader(file, ownsFile: false))
        {
            var index = (await reader.ReadPageIndexAsync(0, ["note"])).Single().OffsetIndex!;
            var page = index.PageLocations.Last(p => p.FirstRowIndex <= 30_000);
            offset = page.Offset + page.CompressedPageSize - 1;
            firstRow = page.FirstRowIndex;
        }

        var bytes = File.ReadAllBytes(path);
        bytes[offset] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        return (path, firstRow);
    }
}
