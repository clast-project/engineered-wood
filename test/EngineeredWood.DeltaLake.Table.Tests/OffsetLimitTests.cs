// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaReadOptions.Offset"/> and <see cref="DeltaReadOptions.Limit"/> (#502 phase 5f). The oracle is the
/// same scan with neither, its rows (and their row addresses) skipped and taken in order. The table has 3 files of 4
/// row groups x 500 rows, a deletion vector deleting every 13th row of files 0 and 1 and a whole row group of file 1,
/// and a partition.
/// </summary>
public class OffsetLimitTests : IDisposable
{
    private const int Files = 3;
    private const int RowsPerFile = 2_000;
    private const int RowsPerGroup = 500;

    private readonly string _tempDir;

    public OffsetLimitTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_offsetlimit_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("cat", StringType.Default, true))
        .Field(new Field("p", StringType.Default, false))
        .Build();

    private static readonly long[] Offsets = [0, 1, 499, 500, 1_001, 2_000, 3_500, 7_000];
    private static readonly long?[] Limits = [null, 0, 1, 10, 501, 2_500];

    public static IEnumerable<object[]> Modes()
    {
        foreach (int concurrency in new[] { 1, 4 })
        {
            foreach (string mode in new[] { "plain", "exact filter", "mask" })
                yield return [concurrency, mode];
        }
    }

    /// <summary>Every offset and limit of the sweep returns the oracle's rows, in its order, with its row addresses.</summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ReturnsTheScansRowsSkippedAndTaken(int concurrency, string mode)
    {
        await using var table = await CreateAsync(concurrency);
        DeltaReadOptions Options() => mode switch
        {
            "exact filter" => new DeltaReadOptions
            {
                Columns = ["id", "cat"], Filter = SparkSqlParser.ParsePredicate("cat <> 'c1'"), FilterRows = true,
                Metadata = DeltaRowMetadata.RowAddress,
            },
            "mask" => new DeltaReadOptions
            {
                Columns = ["id", "cat"], Metadata = DeltaRowMetadata.RowAddress,
                RowMasks = [new DeltaRowMask(["id"], batch =>
                {
                    var ids = (Int64Array)batch.Column(0);
                    var answer = new BooleanArray.Builder().Reserve(ids.Length);
                    for (int i = 0; i < ids.Length; i++)
                        answer.Append(ids.GetValue(i)!.Value % 3 != 0);
                    return answer.Build();
                })],
            },
            _ => new DeltaReadOptions { Columns = ["id", "cat", "p"], Metadata = DeltaRowMetadata.RowAddress },
        };

        var all = await ReadAsync(table, Options());
        Assert.True(all.Count > 3_000);
        foreach (long offset in Offsets)
        {
            foreach (long? limit in Limits)
            {
                var expected = all.Skip((int)Math.Min(offset, all.Count)).Take(limit is { } l ? (int)l : int.MaxValue).ToList();
                var actual = await ReadAsync(table, Options() with { Offset = offset, Limit = limit });
                Assert.True(expected.SequenceEqual(actual),
                    $"{mode}, concurrency {concurrency}, offset {offset}, limit {limit?.ToString(CultureInfo.InvariantCulture) ?? "none"}: "
                    + $"{actual.Count} rows, expected {expected.Count}; first {actual.FirstOrDefault()} vs {expected.FirstOrDefault()}");
            }
        }
    }

    /// <summary>
    /// With no row condition, the units a limit or an offset leaves out are not read: a limit inside the first row group
    /// reads one unit, and an offset covering the first file reads none of its row groups.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task WithNoRowConditionOnlyTheNeededRowGroupsAreRead(int concurrency)
    {
        await using var table = await CreateAsync(concurrency);
        var options = new DeltaReadOptions { Columns = ["id"] };

        int before = table.ScanUnitsRead;
        var ten = await ReadAsync(table, options with { Limit = 10 });
        Assert.Equal(10, ten.Count);
        Assert.Equal(1, table.ScanUnitsRead - before);

        // File 0 holds 2,000 rows less every 13th deleted: an offset of 2,000 is past all of them.
        before = table.ScanUnitsRead;
        var paged = await ReadAsync(table, options with { Offset = 2_000, Limit = 10 });
        Assert.Equal(10, paged.Count);
        Assert.Equal(1, table.ScanUnitsRead - before);

        before = table.ScanUnitsRead;
        int unlimitedUnits = (await ReadAsync(table, options)).Count >= 0 ? table.ScanUnitsRead - before : 0;
        before = table.ScanUnitsRead;
        await ReadAsync(table, options with { Offset = 4_000 });
        Assert.True(table.ScanUnitsRead - before < unlimitedUnits,
            $"an offset of 4,000 read {table.ScanUnitsRead - before} units of {unlimitedUnits}");
    }

    /// <summary>
    /// FilterRows with no Filter is no row condition (review round 2): an offset still skips the row groups it covers
    /// unread, and the rows are those of the same read without FilterRows.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task FilterRowsWithoutAFilterIsCounted(int concurrency)
    {
        await using var table = await CreateAsync(concurrency);
        var plain = new DeltaReadOptions { Columns = ["id"], Offset = 2_000, Limit = 10 };

        int before = table.ScanUnitsRead;
        var rows = await ReadAsync(table, plain with { FilterRows = true });
        Assert.Equal(1, table.ScanUnitsRead - before);
        Assert.Equal(await ReadAsync(table, plain), rows);
    }

    /// <summary>
    /// An offset as large as a long holds returns nothing, rather than wrapping the planner's tally (review round 2).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AnOffsetOfLongMaxValueReturnsNothing(int concurrency)
    {
        await using var table = await CreateAsync(concurrency);
        Assert.Empty(await ReadAsync(table, new DeltaReadOptions { Columns = ["id"], Offset = long.MaxValue, Limit = 5 }));
        Assert.Empty(await ReadAsync(table, new DeltaReadOptions { Columns = ["id"], Offset = long.MaxValue }));
    }

    /// <summary>
    /// With an exact filter, a parallel scan's limit reads one row group first, then two more for each that ends, so a
    /// limit the first row group meets reads at most three, not the window's eight.
    /// </summary>
    [Fact]
    public async Task AFilteredLimitRampsItsWindow()
    {
        await using var table = await CreateAsync(concurrency: 8);
        var options = new DeltaReadOptions
        {
            Columns = ["id"], Filter = SparkSqlParser.ParsePredicate("cat <> 'c1'"), FilterRows = true, Limit = 1,
        };

        int before = table.ScanUnitsRead;
        Assert.Single(await ReadAsync(table, options));
        int units = table.ScanUnitsRead - before;
        Assert.True(units is >= 1 and <= 3, $"read {units} units");
    }

    [Fact]
    public async Task ALimitOfZeroReadsNothing()
    {
        await using var table = await CreateAsync(concurrency: 4);
        int before = table.ScanUnitsRead;
        Assert.Empty(await ReadAsync(table, new DeltaReadOptions { Columns = ["id"], Offset = 5, Limit = 0 }));
        Assert.Equal(before, table.ScanUnitsRead);
    }

    [Fact]
    public async Task ANegativeOffsetOrLimitAndASupersetFilterAreRefused()
    {
        await using var table = await CreateAsync(concurrency: 1);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ReadAsync(table, new DeltaReadOptions { Offset = -1 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ReadAsync(table, new DeltaReadOptions { Limit = -1 }));
        var superset = SparkSqlParser.ParsePredicate("id < 100L");
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(table, new DeltaReadOptions { Filter = superset, Limit = 5 }));
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(table, new DeltaReadOptions { Filter = superset, Offset = 5 }));

        // Without an offset or a limit, the superset filter is as before.
        Assert.NotEmpty(await ReadAsync(table, new DeltaReadOptions { Filter = superset }));
    }

    // ---- The bounded ramp's grants (review round 1) ----------------------------------------------------------------

    private static DeltaTable.ScanLimit Bounded(long offset, long limit) =>
        new(offset, limit, counted: false, ramp: true, window: 8, files: []);

    /// <summary>
    /// CI's failure: a unit that ends before the output wakes for its queued batch is not a starved output. The old
    /// rule granted a unit whenever the output waited with none in flight, and on a slow runner each unit ended before
    /// the output ran, granting the next: 11 units for a limit of 1.
    /// </summary>
    [Fact]
    public void AUnitEndingBeforeTheOutputTakesItsBatchGrantsNothing()
    {
        var limit = Bounded(offset: 0, limit: 1);
        Assert.Equal(1, limit.Granted);
        limit.ConsumerWaiting(waiting: true);
        limit.UnitDone(rows: 500);              // ended; its batch is queued, the output not yet awake
        Assert.Equal(1, limit.Granted);
        limit.ConsumerWaiting(waiting: false, rows: 500);
        Assert.Equal(1, limit.Granted);
    }

    /// <summary>
    /// The deadlock the reserve exists for: the output has received every row the ended units yielded, still wants
    /// more (an accounting disagreement), and nothing is in flight. One unit is granted.
    /// </summary>
    [Fact]
    public void AnOutputThatHasEveryRowAndStillWaitsIsGrantedAUnit()
    {
        var limit = Bounded(offset: 0, limit: 1);
        limit.ConsumerWaiting(waiting: true);
        limit.UnitDone(rows: 500);
        limit.ConsumerWaiting(waiting: false, rows: 500);
        limit.ConsumerWaiting(waiting: true);
        Assert.Equal(2, limit.Granted);
    }

    /// <summary>Units are granted as the rows still needed suggest: a limit far past the first unit's rows opens the window.</summary>
    [Fact]
    public void TheRampGrantsWhatTheRowsStillNeededSuggest()
    {
        var limit = Bounded(offset: 0, limit: 2_000);
        limit.UnitDone(rows: 100);              // 1,900 still needed at 100 a unit: the window, 8
        Assert.Equal(1 + 8, limit.Granted);

        var paged = Bounded(offset: 5, limit: 1);
        Assert.Equal(8, paged.Granted);         // an offset starts with the window
    }

    /// <summary>
    /// A counted limit the first file covers opens no other file (review round 1: the file about to be planned was
    /// opened whatever the lookahead said). Each file is one row group of 2,000 rows here.
    /// </summary>
    [Fact]
    public async Task ACoveredLimitOpensNoFurtherFile()
    {
        string root = Path.Combine(_tempDir, $"onegroup_{Guid.NewGuid():N}");
        var fs = new OpenCountingFileSystem(new LocalTableFileSystem(root));
        await using (var writer = await DeltaTable.CreateAsync(fs, TableSchema,
                         new DeltaTableOptions { TargetFileSize = long.MaxValue }, partitionColumns: ["p"]))
        {
            for (int f = 0; f < Files; f++)
            {
                var id = new Int64Array.Builder();
                var cat = new StringArray.Builder();
                var p = new StringArray.Builder();
                for (long i = f * RowsPerFile; i < (f + 1) * RowsPerFile; i++)
                {
                    id.Append(i);
                    cat.Append("c");
                    p.Append("q" + f.ToString(CultureInfo.InvariantCulture));
                }

                await writer.WriteAsync([new RecordBatch(TableSchema, [id.Build(), cat.Build(), p.Build()], RowsPerFile)]);
            }
        }

        foreach (int concurrency in new[] { 1, 4 })
        {
            await using var table = await DeltaTable.OpenAsync(fs, new DeltaTableOptions { MaxConcurrentReads = concurrency });
            fs.Reset();
            var rows = await ReadAsync(table, new DeltaReadOptions { Columns = ["id"], Limit = 10 });
            Assert.Equal(10, rows.Count);
            Assert.Equal(1, fs.Opened.Count(path => path.EndsWith(".parquet", StringComparison.Ordinal)));
        }
    }

    /// <summary>A table's file system, recording each file opened for reading.</summary>
    private sealed class OpenCountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public System.Collections.Concurrent.ConcurrentQueue<string> Opened { get; private set; } = new();

        // ConcurrentQueue has no Clear on net472.
        public void Reset() => Opened = new();

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            Opened.Enqueue(path);
            return inner.OpenReadAsync(path, cancellationToken);
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
    }

    // ---- The table --------------------------------------------------------------------------------------------------

    private static bool IsDeleted(long id) =>
        (id < 2 * RowsPerFile && id % 13 == 0) || id is >= 2_500 and < 3_000;

    private async Task<DeltaTable> CreateAsync(int concurrency)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, $"t_{Guid.NewGuid():N}")), TableSchema,
            new DeltaTableOptions
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup, DataPageRowCountLimit = 100 },
                MaxConcurrentReads = concurrency,
                TargetFileSize = long.MaxValue,
            },
            partitionColumns: ["p"],
            enableDeletionVectors: true);
        for (int f = 0; f < Files; f++)
        {
            var id = new Int64Array.Builder();
            var cat = new StringArray.Builder();
            var p = new StringArray.Builder();
            for (long i = f * RowsPerFile; i < (f + 1) * RowsPerFile; i++)
            {
                id.Append(i);
                if (i % 11 == 0)
                    cat.AppendNull();
                else
                    cat.Append("c" + (i % 5).ToString(CultureInfo.InvariantCulture));
                p.Append("q" + f.ToString(CultureInfo.InvariantCulture));
            }

            await table.WriteAsync([new RecordBatch(TableSchema, [id.Build(), cat.Build(), p.Build()], RowsPerFile)]);
        }

        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var answer = new BooleanArray.Builder().Reserve(ids.Length);
            for (int i = 0; i < ids.Length; i++)
                answer.Append(IsDeleted(ids.GetValue(i)!.Value));
            return answer.Build();
        });
        Assert.True(deleted > 500);
        return table;
    }

    /// <summary>Each row rendered, every column (metadata included), in the scan's order.</summary>
    private static async Task<List<string>> ReadAsync(DeltaTable table, DeltaReadOptions options)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAsync(options))
        {
            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                {
                    var cells = new List<string>();
                    for (int c = 0; c < batch.ColumnCount; c++)
                    {
                        var column = batch.Column(c);
                        cells.Add(column.IsNull(r) ? "null" : column switch
                        {
                            Int64Array a => a.GetValue(r)!.Value.ToString(CultureInfo.InvariantCulture),
                            StringArray a => a.GetString(r),
                            _ => column.Data.DataType.Name,
                        });
                    }

                    rows.Add(string.Join("|", cells));
                }
            }
        }

        return rows;
    }
}
