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
/// an early exit or cancellation. The pipeline's own guarantees are tested in <see cref="OrderedFileScanTests"/>.
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
        MaxConcurrentFileReads = window,
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
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeltaTableOptions { MaxConcurrentFileReads = 0 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeltaTableOptions { MaxBufferedReadBytes = 0 });
        Assert.Null(new DeltaTableOptions().MaxConcurrentFileReads);
    }
}
