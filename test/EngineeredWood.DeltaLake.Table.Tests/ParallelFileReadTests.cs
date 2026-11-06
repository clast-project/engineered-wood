// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A table scan reading several data files at once (#607) must return exactly what a serial read returns —
/// the same batches, in the same order, with the same metadata columns — and must stop cleanly on an error,
/// an early exit or cancellation. The pipeline's own guarantees are tested in <c>OrderedScanTests</c>, in
/// EngineeredWood.Core.Tests.
/// </summary>
public class ParallelFileReadTests : IDisposable
{
    private const int Files = 10;
    private const int RowsPerFile = 350;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("name", StringType.Default, true))
        .Build();

    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public ParallelFileReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_parallel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static DeltaTableOptions Options(int? window, long budget = 256L * 1024 * 1024) => new()
    {
        // Several row groups per file, so a file is several batches and the budget has something to bound.
        ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = 100 },
        MaxConcurrentReads = window,
        MaxBufferedReadBytes = budget,
    };

    /// <summary>Ten files of 350 rows (four row groups each), with row tracking and two deletion vectors: one
    /// cutting across two row groups of the first file, one deleting a WHOLE row group of the third, whose
    /// batch the read then drops.</summary>
    private async Task CreateTableAsync()
    {
        await using var table = await DeltaTable.CreateAsync(
            _fs, Schema, Options(1), enableDeletionVectors: true, enableRowTracking: true);
        for (int f = 0; f < Files; f++)
        {
            var ids = new Int64Array.Builder();
            var names = new StringArray.Builder();
            for (int r = 0; r < RowsPerFile; r++)
            {
                long id = (long)f * RowsPerFile + r;
                ids.Append(id);
                if (id % 11 == 0) names.AppendNull(); else names.Append($"row-{id}-of-the-test-table"); // > 12 bytes: a view stores it out of line
            }
            await table.WriteAsync([new RecordBatch(Schema, [ids.Build(), names.Build()], RowsPerFile)]);
        }

        await table.DeleteAsync(Ex.And(
            Ex.GreaterThanOrEqual("id", LiteralValue.Of(150L)), Ex.LessThan("id", LiteralValue.Of(260L))));
        await table.DeleteAsync(Ex.And(
            Ex.GreaterThanOrEqual("id", LiteralValue.Of(800L)), Ex.LessThan("id", LiteralValue.Of(900L))));
    }

    private Task<DeltaTable> OpenAsync(int? window, long budget = 256L * 1024 * 1024) =>
        DeltaTable.OpenAsync(_fs, Options(window, budget)).AsTask();

    /// <summary>Every batch as rows of rendered values, batch boundaries kept: equal means the same batches
    /// in the same order with the same contents.</summary>
    private static async Task<List<List<string>>> RenderAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        var rendered = new List<List<string>>();
        await foreach (var batch in batches)
        {
            var header = string.Join(",", batch.Schema.FieldsList.Select(f => f.Name));
            var rows = new List<string> { header };
            for (int i = 0; i < batch.Length; i++)
            {
                rows.Add(string.Join("|", Enumerable.Range(0, batch.ColumnCount)
                    .Select(c => Render(batch.Column(c), i))));
            }
            rendered.Add(rows);
            batch.Dispose();
        }
        return rendered;
    }

    private static string Render(IArrowArray array, int i) => array switch
    {
        _ when array.IsNull(i) => "null",
        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        _ => throw new NotSupportedException(array.Data.DataType.ToString()),
    };

    public static TheoryData<DeltaRowMetadata, long> Cases() => new()
    {
        { DeltaRowMetadata.None, 256L * 1024 * 1024 },
        { DeltaRowMetadata.RowAddress, 256L * 1024 * 1024 },
        { DeltaRowMetadata.Locator, 256L * 1024 * 1024 },
        { DeltaRowMetadata.RowTracking, 256L * 1024 * 1024 },
        { DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking, 256L * 1024 * 1024 },
        // A budget smaller than any batch: every reservation goes through the over-budget admissions.
        { DeltaRowMetadata.None, 1 },
        { DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking, 1 },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ParallelRead_EqualsSerialRead(DeltaRowMetadata metadata, long budget)
    {
        await CreateTableAsync();
        await using var serial = await OpenAsync(1);
        await using var parallel = await OpenAsync(4, budget);
        var options = new DeltaReadOptions { Metadata = metadata };

        var expected = await RenderAsync(serial.ReadAsync(options));
        var actual = await RenderAsync(parallel.ReadAsync(options));

        Assert.Equal(Files * RowsPerFile - 110 - 100, expected.Sum(b => b.Count - 1));
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task ParallelRead_EqualsSerialRead_WithAFilterAndAProjection()
    {
        await CreateTableAsync();
        await using var serial = await OpenAsync(1);
        await using var parallel = await OpenAsync(8);
        // Prunes the first files away (RowAddress ordinals must still be the unpruned ones) and row groups
        // inside the file the bound falls in.
        var options = new DeltaReadOptions
        {
            Columns = ["name"],
            Filter = Ex.GreaterThanOrEqual("id", LiteralValue.Of(1234L)),
            Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator,
        };

        var expected = await RenderAsync(serial.ReadAsync(options));
        Assert.NotEmpty(expected);
        Assert.Equal(expected, await RenderAsync(parallel.ReadAsync(options)));
    }

    [Fact]
    public async Task ReadRowsAsync_InParallel_EqualsSerial()
    {
        await CreateTableAsync();
        await using var serial = await OpenAsync(1);
        await using var parallel = await OpenAsync(4);

        // Every seventh row of every file, by locator.
        var locators = new List<RecordBatch>();
        await foreach (var b in serial.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            locators.Add(b);
        var everySeventh = RowSelection.FromLocatorColumns(locators.Select(b =>
        {
            var keep = Enumerable.Range(0, b.Length).Where(i => i % 7 == 0).ToList();
            return new RecordBatch(b.Schema,
                Enumerable.Range(0, b.ColumnCount).Select(c => EngineeredWood.Arrow.ArrowCompute.Take(b.Column(c), keep)),
                keep.Count);
        }));

        var options = new DeltaRowReadOptions
        {
            Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        };
        var expected = await RenderAsync(serial.ReadRowsAsync(everySeventh, options));
        Assert.NotEmpty(expected);
        Assert.Equal(expected, await RenderAsync(parallel.ReadRowsAsync(everySeventh, options)));
    }

    /// <summary>The largest batch a full read of the table yields, in the bytes the budget counts.</summary>
    private async Task<long> LargestBatchBytesAsync()
    {
        await using var serial = await OpenAsync(1);
        long largest = 0;
        var options = new DeltaReadOptions
        {
            Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        };
        await foreach (var b in serial.ReadAsync(options))
        {
            largest = Math.Max(largest, RollingDataFileWriter.EstimatedBytes(b));
            b.Dispose();
        }
        return largest;
    }

    /// <summary>A slow consumer with a budget smaller than one batch: the reads must wait for it, so only a
    /// couple of batches are ever held, not every file's channel full (4 files x 4 queued + 1 decoding).</summary>
    [Fact]
    public async Task ASlowConsumer_HoldsTheScanToTheBudget()
    {
        await CreateTableAsync();
        long largest = await LargestBatchBytesAsync();
        await using var table = await OpenAsync(4, budget: 1);

        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
        {
            b.Dispose();
            await Task.Delay(2);
        }

        Assert.NotNull(table.LastScanBudget);
        Assert.InRange(table.LastScanBudget!.Peak, 1, 3 * largest);
        Assert.Equal(0, table.LastScanBudget.Used);
    }

    /// <summary>The same for ReadRowsAsync, whose per-file read at first did not reserve at all: its producers
    /// filled their channels whatever the budget.</summary>
    [Fact]
    public async Task ReadRowsAsync_HoldsTheScanToTheBudget_Too()
    {
        await CreateTableAsync();
        long largest = await LargestBatchBytesAsync();
        await using var serial = await OpenAsync(1);

        // Every row, so each batch read is kept whole.
        var locators = new List<RecordBatch>();
        await foreach (var b in serial.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            locators.Add(b);
        var everyRow = RowSelection.FromLocatorColumns(locators);

        await using var table = await OpenAsync(4, budget: 1);
        int rows = 0;
        var options = new DeltaRowReadOptions { Metadata = DeltaRowMetadata.Locator };
        await foreach (var b in table.ReadRowsAsync(everyRow, options))
        {
            rows += b.Length;
            b.Dispose();
            await Task.Delay(2);
        }

        Assert.Equal(Files * RowsPerFile - 210, rows);
        Assert.NotNull(table.LastScanBudget);
        Assert.InRange(table.LastScanBudget!.Peak, 1, 3 * largest);
        Assert.Equal(0, table.LastScanBudget.Used);
    }

    /// <summary>
    /// ReadRowsAsync releases each batch it reads once the selected rows are copied out of it — except when a
    /// column is a string VIEW, whose taken array still points into the source's data buffers. Read with view
    /// output, every string must survive the source being dropped and the garbage collector running.
    /// </summary>
    [Fact]
    public async Task ReadRowsAsync_WithViewColumns_KeepsTheTakenStringsIntact()
    {
        await CreateTableAsync();
        var viewOptions = Options(4) with
        {
            ParquetReadOptions = new ParquetReadOptions { ByteArrayOutput = ByteArrayOutputKind.ViewType },
        };
        await using var serial = await OpenAsync(1);
        await using var table = await DeltaTable.OpenAsync(_fs, viewOptions);

        var locators = new List<RecordBatch>();
        await foreach (var b in serial.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            locators.Add(b);
        var everyThird = RowSelection.FromLocatorColumns(locators.Select(b =>
        {
            var keep = Enumerable.Range(0, b.Length).Where(i => i % 3 == 0).ToList();
            return new RecordBatch(b.Schema,
                Enumerable.Range(0, b.ColumnCount).Select(c => EngineeredWood.Arrow.ArrowCompute.Take(b.Column(c), keep)),
                keep.Count);
        }));

        // Held across the whole read, then collected: anything freed under them shows up as wrong strings.
        var held = new List<RecordBatch>();
        await foreach (var b in table.ReadRowsAsync(everyThird))
            held.Add(b);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.Contains(held, b => b.Column(1) is StringViewArray);
        foreach (var b in held)
        {
            var ids = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
            {
                long id = ids.GetValue(i)!.Value;
                Assert.Equal(id % 11 == 0 ? "null" : $"row-{id}-of-the-test-table", Render(b.Column(1), i));
            }
            b.Dispose();
        }
    }

    [Fact]
    public async Task ReadAtTimestampAsync_ReadsInPathOrder_AndInParallel()
    {
        await CreateTableAsync();
        await using var serial = await OpenAsync(1);
        await using var parallel = await OpenAsync(4);
        var now = DateTimeOffset.UtcNow.AddMinutes(1);

        var expected = await RenderAsync(serial.ReadAsync());
        Assert.Equal(expected, await RenderAsync(serial.ReadAtTimestampAsync(now)));
        Assert.Equal(expected, await RenderAsync(parallel.ReadAtTimestampAsync(now)));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(17)]
    public async Task AnEarlyExit_EndsTheScan_AndTheTableReadsOnNormally(int take)
    {
        await CreateTableAsync();
        await using var table = await OpenAsync(4);

        int seen = 0;
        await foreach (var batch in table.ReadAsync())
        {
            batch.Dispose();
            if (++seen == take)
                break;
        }
        Assert.Equal(take, seen);

        // Nothing was left running against the table's files.
        var all = await RenderAsync(table.ReadAsync());
        Assert.Equal(Files * RowsPerFile - 210, all.Sum(b => b.Count - 1));
    }

    [Fact]
    public async Task Cancelling_TheToken_StopsTheScan()
    {
        await CreateTableAsync();
        await using var table = await OpenAsync(4);
        using var cts = new CancellationTokenSource();

        int seen = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in table.ReadAsync(cancellationToken: cts.Token))
            {
                batch.Dispose();
                if (++seen == 2)
                    cts.Cancel();
            }
        });
        Assert.Equal(2, seen);
    }

    /// <summary>A codec-seam reader that delegates to the built-in codec, fails on one file, and records how
    /// many files it was reading at once.</summary>
    private sealed class FlakyReader(ITableFileSystem fs, string? failOn) : IDataFileReader
    {
        private int _running;
        public int MaxRunning;

        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            string relativePath, IReadOnlyList<string>? physicalColumns,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            int now = Interlocked.Increment(ref _running);
            int seen;
            while (now > (seen = Volatile.Read(ref MaxRunning)))
                Interlocked.CompareExchange(ref MaxRunning, now, seen);
            try
            {
                await Task.Delay(20, cancellationToken); // long enough for reads to overlap when they may
                await using var file = await fs.OpenReadAsync(relativePath, cancellationToken);
                using var reader = new ParquetFileReader(file, ownsFile: false);
                int batches = 0;
                await foreach (var b in reader.ReadAllAsync(
                                   columnNames: physicalColumns, cancellationToken: cancellationToken))
                {
                    if (relativePath == failOn && ++batches == 2)
                        throw new IOException($"cannot read {relativePath}");
                    yield return b;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _running);
            }
        }
    }

    private async Task<DeltaTable> OpenWithReaderAsync(IDataFileReader reader, int? window) =>
        await DeltaTable.OpenAsync(_fs, Options(window) with
        {
#pragma warning disable EWDELTA0001
            DataFileReader = reader,
#pragma warning restore EWDELTA0001
        });

    [Fact]
    public async Task AFailingFile_ThrowsAtItsPosition_AfterTheFilesBeforeIt()
    {
        await CreateTableAsync();
        var paths = DeltaTable.OrderedActiveFiles((await OpenAsync(1)).CurrentSnapshot)
            .Select(f => f.Path).ToList();
        string failing = paths[5];

        await using var table = await OpenWithReaderAsync(new FlakyReader(_fs, failing), window: 4);
        var seenPaths = new List<string>();
        var ex = await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            {
                var path = ((StringArray)batch.Column(batch.ColumnCount - 2)).GetString(0);
                if (seenPaths.Count == 0 || seenPaths[^1] != path)
                    seenPaths.Add(path);
                batch.Dispose();
            }
        });

        Assert.Equal($"cannot read {failing}", ex.Message);
        // Files 0-4 in order, then the failing file's first batch: where a serial read would have thrown.
        Assert.Equal(paths.Take(6), seenPaths);
    }

    [Fact]
    public async Task TheCodecSeam_IsReadSerially_UnlessTheHostOptsIn()
    {
        await CreateTableAsync();

        var byDefault = new FlakyReader(_fs, failOn: null);
        await using (var table = await OpenWithReaderAsync(byDefault, window: null))
            await RenderAsync(table.ReadAsync());
        Assert.Equal(1, byDefault.MaxRunning);

        var optedIn = new FlakyReader(_fs, failOn: null);
        await using (var table = await OpenWithReaderAsync(optedIn, window: 4))
        {
            await using var serial = await OpenAsync(1);
            Assert.Equal(await RenderAsync(serial.ReadAsync()), await RenderAsync(table.ReadAsync()));
        }
        Assert.InRange(optedIn.MaxRunning, 2, 4);
    }

    [Fact]
    public void TheOptions_RefuseANonPositiveWindowOrBudget()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeltaTableOptions { MaxConcurrentReads = 0 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeltaTableOptions { MaxBufferedReadBytes = 0 });
        Assert.Null(new DeltaTableOptions().MaxConcurrentReads);
    }

    // ── #612: a scan split by (file, row group) ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The table's file system, counting the data files it has open and, optionally, delaying every data read
    /// and recording how many were in flight at once, or refusing to open one file.
    /// </summary>
    private sealed class TrackingFileSystem(
        ITableFileSystem inner, int delayMs = 0, string? failOpen = null, string? stallOpen = null)
        : ITableFileSystem
    {
        private int _open;
        private int _reading;
        public int Opened;
        public int MaxReading;

        public int Open => Volatile.Read(ref _open);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            if (!path.EndsWith(".parquet", StringComparison.Ordinal) || path.Contains("_delta_log"))
                return await inner.OpenReadAsync(path, cancellationToken);
            if (failOpen is not null && path.Contains(failOpen))
                throw new IOException($"cannot open {failOpen}");
            if (stallOpen is not null && path.Contains(stallOpen))
                await Task.Delay(Timeout.Infinite, cancellationToken); // until the scan gives up on it
            var file = await inner.OpenReadAsync(path, cancellationToken);
            Interlocked.Increment(ref _open);
            Interlocked.Increment(ref Opened);
            return new TrackedFile(file, this);
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);

        private sealed class TrackedFile(IRandomAccessFile file, TrackingFileSystem owner) : IRandomAccessFile
        {
            private int _closed;

            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => file.GetLengthAsync(cancellationToken);

            public async ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
            {
                await EnterAsync(cancellationToken);
                try
                {
                    return await file.ReadAsync(range, cancellationToken);
                }
                finally
                {
                    Interlocked.Decrement(ref owner._reading);
                }
            }

            public async ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                await EnterAsync(cancellationToken);
                try
                {
                    return await file.ReadRangesAsync(ranges, cancellationToken);
                }
                finally
                {
                    Interlocked.Decrement(ref owner._reading);
                }
            }

            private async Task EnterAsync(CancellationToken cancellationToken)
            {
                int now = Interlocked.Increment(ref owner._reading);
                int seen;
                while (now > (seen = Volatile.Read(ref owner.MaxReading)))
                    Interlocked.CompareExchange(ref owner.MaxReading, now, seen);
                if (owner._delayMs > 0)
                    await Task.Delay(owner._delayMs, cancellationToken);
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _closed, 1) == 0)
                    Interlocked.Decrement(ref owner._open);
                file.Dispose();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }
        }

        private readonly int _delayMs = delayMs;
    }

    private static Task<DeltaTable> OpenTrackedAsync(TrackingFileSystem fs, int? window) =>
        DeltaTable.OpenAsync(fs, Options(window)).AsTask();

    [Theory]
    [InlineData(1, 0)]
    [InlineData(4, 0)]
    [InlineData(4, 1)]
    [InlineData(4, 6)]
    public async Task EveryFileTheScanOpens_IsClosed_HoweverTheScanEnds(int window, int take)
    {
        await CreateTableAsync();
        var fs = new TrackingFileSystem(_fs);
        await using var table = await OpenTrackedAsync(fs, window);

        int seen = 0;
        await foreach (var batch in table.ReadAsync())
        {
            batch.Dispose();
            if (++seen == take)
                break;
        }

        Assert.True(fs.Opened > 0);
        Assert.Equal(0, fs.Open);
    }

    [Fact]
    public async Task Cancelling_ClosesEveryFileTheScanOpened()
    {
        await CreateTableAsync();
        var fs = new TrackingFileSystem(_fs);
        await using var table = await OpenTrackedAsync(fs, 4);
        using var cts = new CancellationTokenSource();

        int seen = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in table.ReadAsync(cancellationToken: cts.Token))
            {
                batch.Dispose();
                if (++seen == 3)
                    cts.Cancel();
            }
        });

        Assert.Equal(0, fs.Open);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AFileThatCannotBeOpened_ThrowsAtItsPosition_AndTheOthersAreClosed(int window)
    {
        await CreateTableAsync();
        List<string> paths;
        await using (var probe = await OpenAsync(1))
            paths = DeltaTable.OrderedActiveFiles(probe.CurrentSnapshot).Select(f => f.Path).ToList();
        string failing = paths[5];

        var fs = new TrackingFileSystem(_fs, failOpen: failing);
        await using var table = await OpenTrackedAsync(fs, window);
        var seenPaths = new List<string>();
        var ex = await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            {
                var path = ((StringArray)batch.Column(batch.ColumnCount - 2)).GetString(0);
                if (seenPaths.Count == 0 || seenPaths[^1] != path)
                    seenPaths.Add(path);
                batch.Dispose();
            }
        });

        Assert.Equal($"cannot open {failing}", ex.Message);
        // Every batch of files 0-4, in order, and nothing after: where a serial read would have thrown.
        Assert.Equal(paths.Take(5), seenPaths);
        Assert.Equal(0, fs.Open);
    }

    [Fact]
    public async Task AStalledOpeningAhead_DoesNotHoldBackAnEarlierError()
    {
        await CreateTableAsync();
        List<string> paths;
        await using (var probe = await OpenAsync(1))
            paths = DeltaTable.OrderedActiveFiles(probe.CurrentSnapshot).Select(f => f.Path).ToList();

        // With a window of 4, file 4 is opened ahead, and stalls, before the planner reaches file 3, which fails.
        var fs = new TrackingFileSystem(_fs, failOpen: paths[3], stallOpen: paths[4]);
        await using var table = await OpenTrackedAsync(fs, 4);
        var read = Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var batch in table.ReadAsync())
                batch.Dispose();
        });

        var finished = await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.True(finished == read, "the scan waited on the stalled opening instead of reporting the error");
        Assert.Equal($"cannot open {paths[3]}", (await read).Message);
        Assert.Equal(0, fs.Open);
    }

    [Fact]
    public async Task OneFile_IsReadByRowGroup_InParallel()
    {
        // One file of twelve row groups: a scan split by file could only read it serially.
        await using (var create = await DeltaTable.CreateAsync(_fs, Schema, Options(1), enableDeletionVectors: true))
        {
            var ids = new Int64Array.Builder();
            var names = new StringArray.Builder();
            for (int r = 0; r < 1_200; r++)
            {
                ids.Append(r);
                names.Append($"row-{r}-of-the-one-file");
            }
            await create.WriteAsync([new RecordBatch(Schema, [ids.Build(), names.Build()], 1_200)]);
            await create.DeleteAsync(Ex.And(
                Ex.GreaterThanOrEqual("id", LiteralValue.Of(250L)), Ex.LessThan("id", LiteralValue.Of(420L))));
        }

        await using var serial = await OpenAsync(1);
        var options = new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator };
        var expected = await RenderAsync(serial.ReadAsync(options));
        Assert.Equal(1_200 - 170, expected.Sum(b => b.Count - 1));

        var fs = new TrackingFileSystem(_fs, delayMs: 20);
        await using var parallel = await OpenTrackedAsync(fs, 4);
        Assert.Equal(expected, await RenderAsync(parallel.ReadAsync(options)));
        Assert.Equal(1, fs.Opened);
        Assert.Equal(0, fs.Open);
        // Its row groups' reads overlapped, on the one open file.
        Assert.InRange(fs.MaxReading, 2, 4);
        Assert.NotNull(parallel.LastScanBudget);
        Assert.Equal(0, parallel.LastScanBudget!.Used);
    }

    // ── #632: the table file system suggests the window ────────────────────────────────────────────────────

    /// <summary>The table's file system, suggesting a window through <see cref="IReadConcurrencyHint"/>.</summary>
    private sealed class HintingFileSystem(ITableFileSystem inner, int? hint) : ITableFileSystem, IReadConcurrencyHint
    {
        public int? PreferredConcurrentReads => hint;
        public PathNameConstraints PathConstraints => inner.PathConstraints;
        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
            => inner.OpenReadAsync(path, cancellationToken);
        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);
    }

    /// <summary>
    /// The default window is the file system's hint; an explicit <see cref="DeltaTableOptions.MaxConcurrentReads"/>
    /// wins over it, and the codec seam stays serial unless the host opts in. No hint, or one below 1, leaves the
    /// CPU-based default (-1 here).
    /// </summary>
    [Theory]
    [InlineData(9, null, false, 9)]
    [InlineData(9, 2, false, 2)]
    [InlineData(9, null, true, 1)]
    [InlineData(9, 3, true, 3)]
    [InlineData(0, null, false, -1)]
    [InlineData(null, null, false, -1)]
    public async Task TheFileSystemsHint_SetsTheDefaultWindow_AndAnExplicitSettingWins(
        int? hint, int? option, bool seam, int expected)
    {
        await CreateTableAsync();
        var options = Options(option);
        if (seam)
        {
#pragma warning disable EWDELTA0001
            options = options with { DataFileReader = new FlakyReader(_fs, failOn: null) };
#pragma warning restore EWDELTA0001
        }
        await using var table = await DeltaTable.OpenAsync(new HintingFileSystem(_fs, hint), options);

        Assert.Equal(expected == -1 ? Math.Min(4, Environment.ProcessorCount) : expected, table.EffectiveConcurrentReads);
    }

    /// <summary>
    /// The hint reaches the scheduler, not just <see cref="DeltaTable.EffectiveConcurrentReads"/>: with every data
    /// read delayed, a hint of 16 keeps more reads in flight than the CPU default ever can (its window of at most 4
    /// row groups, plus at most 4 footers of files opened ahead), and a hint of 1 reads one thing at a time.
    /// Either way the batches equal a serial read's. Measured: 16 in flight for a hint of 16, 6 for the default.
    /// </summary>
    [Theory]
    [InlineData(16)]
    [InlineData(1)]
    public async Task AHintedScan_KeepsTheHintedReadsInFlight_AndEqualsTheSerialRead(int hint)
    {
        await CreateTableAsync();
        await using var serial = await OpenAsync(1);
        var tracking = new TrackingFileSystem(_fs, delayMs: 20);
        await using var hinted = await DeltaTable.OpenAsync(new HintingFileSystem(tracking, hint), Options(null));
        var options = new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator };

        Assert.Equal(await RenderAsync(serial.ReadAsync(options)), await RenderAsync(hinted.ReadAsync(options)));
        if (hint == 1)
        {
            Assert.Equal(1, tracking.MaxReading);
            Assert.Null(hinted.LastScanBudget);
        }
        else
        {
            Assert.InRange(tracking.MaxReading, 2 * 4 + 1, 2 * hint);
            Assert.NotNull(hinted.LastScanBudget);
        }
        Assert.Equal(0, tracking.Open);
    }

    // ── #713: the files opened ahead are planned ahead ─────────────────────────────────────────────────────

    /// <summary>
    /// A filtered scan plans the files it has opened ahead at once, not one after another (#713). Each file's
    /// row-group pruning here needs a read of its own (the dictionary of a <c>tag</c> column whose statistics
    /// span the value, which the dictionary then proves absent), and against an object store that read is a
    /// round trip: planned one file at a time, the scan paid one round trip per file, in series. Every row group
    /// is pruned, so the only reads besides the footers are the planning's, and several files must have one in
    /// flight at once. Measured before the fix: 1.
    /// </summary>
    [Fact]
    public async Task AFilteredScan_PlansTheFilesItOpenedAhead_AtOnce()
    {
        const int files = 8;
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("tag", StringType.Default, false))
            .Build();
        await using (var create = await DeltaTable.CreateAsync(_fs, schema, Options(1)))
        {
            for (int f = 0; f < files; f++)
            {
                var ids = new Int64Array.Builder();
                var tags = new StringArray.Builder();
                for (int r = 0; r < RowsPerFile; r++)
                {
                    ids.Append((long)f * RowsPerFile + r);
                    tags.Append(r % 2 == 0 ? "a" : "c");
                }
                await create.WriteAsync([new RecordBatch(schema, [ids.Build(), tags.Build()], RowsPerFile)]);
            }
        }

        var fs = new PlanningReadsFileSystem(_fs, delayMs: 50);
        await using var table = await DeltaTable.OpenAsync(fs, Options(files));
        long rows = 0;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = Ex.Equal("tag", LiteralValue.Of("b")) }))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        // The dictionaries pruned every row group (the statistics cannot: "b" lies between "a" and "c").
        Assert.Equal(0, rows);
        Assert.Equal(files, fs.FilesRead);
        Assert.InRange(fs.MaxFilesPlanning, 2, files);
    }

    /// <summary>
    /// Counts, over the data files, the most that had a read in flight at once other than a footer read (one
    /// ending at the end of the file, or at its last 8 bytes: the length and magic). Every read is delayed.
    /// </summary>
    private sealed class PlanningReadsFileSystem(ITableFileSystem inner, int delayMs) : ITableFileSystem
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, int> _inFlight = new(StringComparer.Ordinal);
        private readonly HashSet<string> _read = new(StringComparer.Ordinal);
        public int MaxFilesPlanning;

        public int FilesRead
        {
            get
            {
                lock (_gate)
                    return _read.Count;
            }
        }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken);
            return path.EndsWith(".parquet", StringComparison.Ordinal) && !path.Contains("_delta_log")
                ? new File(file, path, this)
                : file;
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
            => inner.ReadAllBytesAsync(path, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);

        private void Enter(string path, bool planning)
        {
            lock (_gate)
            {
                _read.Add(path);
                if (!planning)
                    return;
                _inFlight[path] = _inFlight.TryGetValue(path, out int n) ? n + 1 : 1;
                MaxFilesPlanning = Math.Max(MaxFilesPlanning, _inFlight.Count);
            }
        }

        private void Exit(string path, bool planning)
        {
            if (!planning)
                return;
            lock (_gate)
            {
                if (--_inFlight[path] == 0)
                    _inFlight.Remove(path);
            }
        }

        private sealed class File(IRandomAccessFile file, string path, PlanningReadsFileSystem owner) : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => file.GetLengthAsync(cancellationToken);

            public async ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
            {
                bool planning = !await IsFooterAsync([range], cancellationToken);
                owner.Enter(path, planning);
                try
                {
                    await Task.Delay(owner._delayMs, cancellationToken);
                    return await file.ReadAsync(range, cancellationToken);
                }
                finally
                {
                    owner.Exit(path, planning);
                }
            }

            public async ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                bool planning = !await IsFooterAsync(ranges, cancellationToken);
                owner.Enter(path, planning);
                try
                {
                    await Task.Delay(owner._delayMs, cancellationToken);
                    return await file.ReadRangesAsync(ranges, cancellationToken);
                }
                finally
                {
                    owner.Exit(path, planning);
                }
            }

            private async ValueTask<bool> IsFooterAsync(IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken)
            {
                long length = await file.GetLengthAsync(cancellationToken);
                return ranges.All(r => r.End == length || r.End == length - 8);
            }

            public void Dispose() => file.Dispose();

            public ValueTask DisposeAsync() => file.DisposeAsync();
        }

        private readonly int _delayMs = delayMs;
    }

    // ── #633: ReadRowsAsync by row group ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// ReadRowsAsync reads only the row groups that hold a selected row: no byte of any other row group's column
    /// chunks is requested, and a file with no selected row is never opened. File 0's row group 1 is cut by its
    /// deletion vector (rows 150..259; position 160 is selected but deleted), and file 2's row group 1 is deleted
    /// whole (position 150 selected): neither may change what comes back.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task ReadRowsAsync_NeverRequestsARowGroupHoldingNoSelectedRow(int window)
    {
        await CreateTableAsync();
        // The data files by the order they were written (file w holds ids w * RowsPerFile and on), not by path.
        var paths = new string[Files];
        await using (var t = await OpenAsync(1))
        {
            await foreach (var b in t.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
            {
                long firstId = ((Int64Array)b.Column(0)).GetValue(0)!.Value;
                paths[firstId / RowsPerFile] = ((StringArray)b.Column(b.ColumnCount - 2)).GetString(0);
                b.Dispose();
            }
        }

        // File -> selected positions; row groups hold 100 rows each.
        var selected = new Dictionary<int, long[]>
        {
            [0] = [120, 130, 160],
            [2] = [150],
            [3] = [310, 349],
            [5] = [0],
        };
        var selection = RowSelection.ByPath(selected.ToDictionary(
            kv => paths[kv.Key], kv => (IReadOnlyCollection<long>)kv.Value));

        var fs = new DataFileCountingFileSystem(_fs);
        await using var table = await DeltaTable.OpenAsync(fs, Options(window));
        var ids = new List<long>();
        await foreach (var b in table.ReadRowsAsync(selection))
        {
            var col = (Int64Array)b.Column(0);
            for (int i = 0; i < b.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
            b.Dispose();
        }

        // Files come back in path order, which is not the order they were written in.
        Assert.Equal(new long[] { 120, 130, 3 * RowsPerFile + 310, 3 * RowsPerFile + 349, 5 * RowsPerFile }, ids.OrderBy(i => i));
        // File 2's only selected row group is deleted whole, so it is opened (for its footer and vector) but none
        // of its row groups is read; files 1, 4 and 6-9 are never opened.
        Assert.Equal(
            new[] { paths[0], paths[2], paths[3], paths[5] }.OrderBy(p => p, StringComparer.Ordinal),
            fs.OpenedDataFiles.OrderBy(p => p, StringComparer.Ordinal));

        var reads = fs.DataReads;
        foreach (var (f, positions) in selected)
        {
            var kept = new HashSet<int>(f == 2 ? [] : positions.Select(p => (int)(p / 100)));
            var fileReads = reads.Where(r => r.Path == paths[f]).Select(r => r.Range).ToList();

            await using var file = await _fs.OpenReadAsync(paths[f]);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var footer = await reader.ReadMetadataAsync();
            for (int g = 0; g < footer.RowGroups.Count; g++)
            {
                bool touched = footer.RowGroups[g].Columns.Any(c => fileReads.Any(r => Overlaps(c.MetaData!, r)));
                Assert.True(kept.Contains(g) == touched,
                    $"file {f}, row group {g}: read {touched}, expected {kept.Contains(g)}");
            }
        }

        static bool Overlaps(EngineeredWood.Parquet.Metadata.ColumnMetaData chunk, FileRange range)
        {
            long start = chunk.DictionaryPageOffset is long d && d > 0 && d < chunk.DataPageOffset
                ? d : chunk.DataPageOffset;
            long end = start + chunk.TotalCompressedSize;
            return range.Offset < end && start < range.End;
        }
    }

    /// <summary>
    /// Inside a kept row group, ReadRowsAsync also cuts out long runs holding no selected row, so that pages wholly
    /// inside them are never requested, and the rows after each cut keep their absolute positions. One file of
    /// 10,000 rows in ONE row group of 100-row pages, with a page index; ids equal positions equal row ids. A
    /// deletion vector deletes 4000..4099, and the selected 4050 lies in it. The selection {5, 1500, 4050, 4100,
    /// 9999} leaves two long gaps, (1500, 4050) and (4100, 9999), with targets on both sides of each.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task ReadRowsAsync_SkipsThePagesOfALongUnselectedRun_AndKeepsAbsolutePositions(int window)
    {
        const int rows = 10_000;
        const int rowsPerPage = 100;
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = new ParquetWriteOptions
            {
                RowGroupMaxRows = rows,
                DataPageRowCountLimit = rowsPerPage,
                WritePageIndex = true,
            },
            MaxConcurrentReads = window,
        };
        var root = new LocalTableFileSystem(Path.Combine(_tempDir, "pages"));
        await using (var create = await DeltaTable.CreateAsync(
                         root, Schema, options, enableDeletionVectors: true, enableRowTracking: true))
        {
            var ids = new Int64Array.Builder();
            var names = new StringArray.Builder();
            for (int r = 0; r < rows; r++)
            {
                ids.Append(r);
                names.Append($"row-{r}-of-the-one-file");
            }
            await create.WriteAsync([new RecordBatch(Schema, [ids.Build(), names.Build()], rows)]);
            await create.DeleteAsync(Ex.And(
                Ex.GreaterThanOrEqual("id", LiteralValue.Of(4000L)), Ex.LessThan("id", LiteralValue.Of(4100L))));
        }

        var fs = new DataFileCountingFileSystem(root);
        await using var table = await DeltaTable.OpenAsync(fs, options);
        var addFile = Assert.Single(table.CurrentSnapshot.ActiveFiles).Value;
        Assert.NotNull(addFile.DeletionVector);
        long[] targets = [5, 1500, 4050, 4100, 9999];
        var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [addFile.Path] = targets,
        });

        var got = new List<(long Id, long Position, long RowId)>();
        var rowOptions = new DeltaRowReadOptions { Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking };
        await foreach (var b in table.ReadRowsAsync(selection, rowOptions))
        {
            var id = (Int64Array)b.Column(0);
            var position = (Int64Array)b.Column(b.Schema.GetFieldIndex(rowOptions.MetadataPrefix + RowSelection.RowIndexColumnSuffix));
            var rowId = (Int64Array)b.Column(b.Schema.GetFieldIndex(rowOptions.MetadataPrefix + DeltaMetadataColumns.RowIdSuffix));
            for (int i = 0; i < b.Length; i++)
                got.Add((id.GetValue(i)!.Value, position.GetValue(i)!.Value, rowId.GetValue(i)!.Value));
            b.Dispose();
        }

        // 4050 is deleted; every other target comes back at its own position, with its own row id.
        Assert.Equal(new long[] { 5, 1500, 4100, 9999 }.Select(v => (v, v, v)), got);

        // No page wholly inside a gap is requested; every page holding a returned row is.
        var reads = fs.DataReads.Where(r => r.Path == addFile.Path).Select(r => r.Range).ToList();
        await using var file = await root.OpenReadAsync(addFile.Path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var chunks = await reader.ReadPageIndexAsync(0);
        Assert.Equal(2, chunks.Count); // id and name: a fresh file materializes no row-tracking columns
        foreach (var chunk in chunks)
        {
            var pages = chunk.OffsetIndex!.PageLocations;
            Assert.True(pages.Count >= rows / rowsPerPage / 2, $"{pages.Count} pages");
            // Counted per column: with 100-row pages, 24 lie wholly in (1500, 4050) and 57 in (4100, 9999).
            int gapPages = 0;
            for (int p = 0; p < pages.Count; p++)
            {
                long first = pages[p].FirstRowIndex;
                long end = p + 1 < pages.Count ? pages[p + 1].FirstRowIndex : rows;
                bool read = reads.Any(r => r.Offset < pages[p].Offset + pages[p].CompressedPageSize && pages[p].Offset < r.End);
                bool inGap = (first > 1500 && end <= 4050) || (first > 4100 && end <= 9999);
                bool holdsReturned = new long[] { 5, 1500, 4100, 9999 }.Any(t => first <= t && t < end);
                if (inGap)
                {
                    gapPages++;
                    Assert.False(read, $"{string.Join(".", chunk.Path)} page {p} (rows {first}..{end}) lies in a gap but was read");
                }
                if (holdsReturned)
                    Assert.True(read, $"{string.Join(".", chunk.Path)} page {p} (rows {first}..{end}) holds a target but was not read");
            }
            Assert.True(gapPages > 40, $"only {gapPages} pages of {string.Join(".", chunk.Path)} lie in the gaps");
        }
    }

    /// <summary>Point lookups in every file, read by row group in parallel, equal the serial read.</summary>
    [Fact]
    public async Task ReadRowsAsync_PointLookups_InParallel_EqualSerial()
    {
        await CreateTableAsync();
        await using var serial = await OpenAsync(1);
        await using var parallel = await OpenAsync(4);
        var paths = DeltaTable.OrderedActiveFiles(serial.CurrentSnapshot).Select(f => f.Path).ToList();
        var selection = RowSelection.ByPath(Enumerable.Range(0, paths.Count).ToDictionary(
            f => paths[f], f => (IReadOnlyCollection<long>)new long[] { f * 37L % RowsPerFile, RowsPerFile - 1 }));

        var options = new DeltaRowReadOptions
        {
            Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        };
        var expected = await RenderAsync(serial.ReadRowsAsync(selection, options));
        Assert.NotEmpty(expected);
        Assert.Equal(expected, await RenderAsync(parallel.ReadRowsAsync(selection, options)));
    }

    [Theory]
    [InlineData(0, 5, true)]
    [InlineData(0, 6, false)]
    [InlineData(6, 4, true)]
    [InlineData(6, 5, false)]
    [InlineData(10, 1, false)]
    [InlineData(11, 9, true)]
    [InlineData(20, 1, false)]
    [InlineData(21, 1000, true)]
    public void NoTargetIn_SkipsExactlyTheSpansHoldingNoTarget(long first, long count, bool skip)
    {
        Assert.Equal(skip, DeltaTable.NoTargetIn(new HashSet<long> { 20, 5, 10 })(first, count));
        Assert.True(DeltaTable.NoTargetIn(new HashSet<long>())(first, count));
    }
}
