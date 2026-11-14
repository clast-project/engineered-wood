// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// A read with no scan budget (one row group at a time) from an object store fetches a row group's chunks whole and
/// at once when a batch size splits the group, rather than in page windows one round trip after another (#652).
/// Windows over a coalescing reader also fetched the columns' later pages again, merged across the gaps.
/// </summary>
public class UnbudgetedPrefetchTests : IDisposable
{
    private const int Rows = 6_000;
    private const int BatchRows = 500;
    private readonly string _tempDir;
    private readonly string _path;

    public UnbudgetedPrefetchTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-unbudgeted-prefetch-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _path = Path.Combine(_tempDir, "data.parquet");
        Write(_path);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_tempDir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>One row group of three columns in pages of 250 rows, with OffsetIndexes (the writer's default), so
    /// a batched read plans its pages from them and holds nothing it scanned.</summary>
    private static void Write(string path)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("name", StringType.Default, false))
            .Field(new Field("value", DoubleType.Default, true))
            .Build();
        using var file = new LocalSequentialFile(path);
        using var writer = new ParquetFileWriter(file, ownsFile: false,
            ParquetWriteOptions.Default with { DataPageRowCountLimit = 250 });
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        var values = new DoubleArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            names.Append($"row-{i:D8}");
            if (i % 7 == 0)
                values.AppendNull();
            else
                values.Append(i * 0.5);
        }
        using var batch = new RecordBatch(schema, [ids.Build(), names.Build(), values.Build()], Rows);
        writer.WriteRowGroupAsync(batch).AsTask().GetAwaiter().GetResult();
        writer.CloseAsync().AsTask().GetAwaiter().GetResult();
    }

    private static ParquetReadOptions Options(int? batchSize = BatchRows) =>
        new() { MaxConcurrentRowGroups = 1, BatchSize = batchSize };

    /// <summary>The rows read, the requests that fetched any chunk bytes, and how many chunk bytes they fetched.</summary>
    private sealed record Result(List<long> Ids, List<string> Names, int Requests, long DataBytes);

    /// <summary>Reads every row, with windows of one batch each (so a read that windows asks once per batch).</summary>
    private async Task<Result> ReadAsync(ParquetReadOptions options, int? hint, long? prefetchCap = null)
    {
        await using var local = new LocalRandomAccessFile(_path);
        var file = new RecordingFile(local, hint);
        using var reader = new ParquetFileReader(file, ownsFile: false, options) { BatchPageWindowBudgetBytes = 1 };
        if (prefetchCap is long cap)
            reader.UnbudgetedPrefetchMaxBytes = cap;
        var metadata = await reader.ReadMetadataAsync();
        var chunks = metadata.RowGroups[0].Columns.Select(c => c.MetaData!).ToList();
        long dataStart = chunks.Min(c => c.DictionaryPageOffset is long d and > 0 ? Math.Min(d, c.DataPageOffset) : c.DataPageOffset);
        long dataEnd = chunks.Max(c => (c.DictionaryPageOffset is long d and > 0 ? Math.Min(d, c.DataPageOffset) : c.DataPageOffset) + c.TotalCompressedSize);
        file.Clear();

        var ids = new List<long>();
        var names = new List<string>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
            {
                Assert.True(batch.Length <= (options.BatchSize ?? Rows));
                var idColumn = (Int64Array)batch.Column(0);
                var nameColumn = (StringArray)batch.Column(1);
                for (int r = 0; r < batch.Length; r++)
                {
                    ids.Add(idColumn.GetValue(r)!.Value);
                    names.Add(nameColumn.GetString(r)!);
                }
            }
        }

        // Only the chunks' bytes: the OffsetIndexes the read plans pages from are a request of their own.
        long dataBytes = file.Requests.Sum(request => request.Sum(r => Overlap(r, dataStart, dataEnd)));
        int dataRequests = file.Requests.Count(request => request.Any(r => Overlap(r, dataStart, dataEnd) > 0));
        return new Result(ids, names, dataRequests, dataBytes);
    }

    private static long Overlap(FileRange r, long start, long end) =>
        Math.Max(0, Math.Min(r.Offset + r.Length, end) - Math.Max(r.Offset, start));

    private long DataLength()
    {
        using var local = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(local, ownsFile: false);
        var metadata = reader.ReadMetadataAsync().AsTask().GetAwaiter().GetResult();
        return metadata.RowGroups[0].Columns.Sum(c => c.MetaData!.TotalCompressedSize);
    }

    [Fact]
    public async Task FromAnObjectStore_ABatchedReadFetchesEachChunkOnce_InOneRequest()
    {
        var expected = await ReadAsync(Options(), hint: null);
        var actual = await ReadAsync(Options(), hint: 16);

        Assert.Equal(Enumerable.Range(0, Rows).Select(i => (long)i), actual.Ids);
        Assert.Equal(expected.Names, actual.Names);
        // Every chunk, whole, in the one ReadRangesAsync; nothing of them again.
        Assert.Equal(DataLength(), actual.DataBytes);
        Assert.Equal(1, actual.Requests);
        // Without the hint the read keeps to its windows: a request per batch.
        Assert.True(expected.Requests >= Rows / BatchRows, $"{expected.Requests} requests");
    }

    [Fact]
    public async Task ChunksOverTheCap_StayInPageWindows()
    {
        var result = await ReadAsync(Options(), hint: 16, prefetchCap: DataLength() - 1);

        Assert.Equal(Enumerable.Range(0, Rows).Select(i => (long)i), result.Ids);
        Assert.True(result.Requests >= Rows / BatchRows, $"{result.Requests} requests");
    }

    [Fact]
    public async Task AtTheCap_TheChunksAreFetchedWhole()
    {
        var result = await ReadAsync(Options(), hint: 16, prefetchCap: DataLength());

        Assert.Equal(1, result.Requests);
        Assert.Equal(DataLength(), result.DataBytes);
    }

    [Fact]
    public async Task AnUnbatchedRead_IsUnchanged()
    {
        var withHint = await ReadAsync(Options(batchSize: null), hint: 16);
        var without = await ReadAsync(Options(batchSize: null), hint: null);

        Assert.Equal(without.Ids, withHint.Ids);
        Assert.Equal(without.Requests, withHint.Requests);
        Assert.Equal(without.DataBytes, withHint.DataBytes);
    }

    [Fact]
    public async Task ABatchSizeThatDoesNotSplitTheGroup_IsUnchanged()
    {
        var withHint = await ReadAsync(Options(batchSize: Rows), hint: 16);
        var without = await ReadAsync(Options(batchSize: Rows), hint: null);

        Assert.Equal(without.Ids, withHint.Ids);
        Assert.Equal(without.Requests, withHint.Requests);
        Assert.Equal(without.DataBytes, withHint.DataBytes);
    }

    [Fact]
    public async Task AMaxBatchByteSize_AlsoFetchesWhole()
    {
        var options = new ParquetReadOptions { MaxConcurrentRowGroups = 1, MaxBatchByteSize = 16 * 1024 };
        var result = await ReadAsync(options, hint: 16);

        Assert.Equal(Enumerable.Range(0, Rows).Select(i => (long)i), result.Ids);
        Assert.Equal(1, result.Requests);
        Assert.Equal(DataLength(), result.DataBytes);
    }

    /// <summary>Page windows are 32 MiB from an object store (round trips cost more than bytes held there), and
    /// stay 8 MiB from a local file, where a window read long before its decode costs (#622, #652).</summary>
    [Theory]
    [InlineData(16, BatchPageWindow.ObjectStoreBudgetBytes)]
    [InlineData(1, BatchPageWindow.ObjectStoreBudgetBytes)]
    [InlineData(null, BatchPageWindow.DefaultBudgetBytes)]
    [InlineData(0, BatchPageWindow.DefaultBudgetBytes)]
    public async Task TheWindowBudget_FollowsTheFilesHint(int? hint, long expected)
    {
        await using var local = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(new RecordingFile(local, hint), ownsFile: false);
        Assert.Equal(expected, reader.BatchPageWindowBudgetBytes);

        reader.BatchPageWindowBudgetBytes = 5;
        Assert.Equal(5, reader.BatchPageWindowBudgetBytes);
    }

    [Fact]
    public async Task ALocalFile_KeepsTheEightMiBWindow()
    {
        await using var local = new LocalRandomAccessFile(_path);
        using var reader = new ParquetFileReader(local, ownsFile: false);
        Assert.Equal(8L * 1024 * 1024, reader.BatchPageWindowBudgetBytes);
    }

    /// <summary>A file that records each request (a ReadRangesAsync is one) and its ranges, and optionally
    /// reports a concurrency hint, as an object store's file does.</summary>
    private sealed class RecordingFile(IRandomAccessFile inner, int? hint) : IRandomAccessFile, IReadConcurrencyHint
    {
        private readonly object _lock = new();

        /// <summary>Each request's ranges: a ReadRangesAsync is one request.</summary>
        public List<IReadOnlyList<FileRange>> Requests { get; } = [];

        public int? PreferredConcurrentReads => hint;

        public void Clear()
        {
            lock (_lock)
            {
                Requests.Clear();
            }
        }

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default) =>
            inner.GetLengthAsync(cancellationToken);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(FileRange range, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                Requests.Add([range]);
            }
            return inner.ReadAsync(range, cancellationToken);
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                Requests.Add([.. ranges]);
            }
            return inner.ReadRangesAsync(ranges, cancellationToken);
        }

        public ValueTask DisposeAsync() => default;

        public void Dispose()
        {
        }
    }
}
