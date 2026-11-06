// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// History, VACUUM and compaction used to make their file-system requests one at a time, which against an
/// object store costs a round trip per commit, per deleted file and per source file (measured against Azure
/// Blob: about one round trip per item). They now run up to
/// <see cref="DeltaTable.EffectiveConcurrentRequests"/> / <see cref="DeltaTable.EffectiveConcurrentReads"/> at
/// once. Every test checks both halves: the requests really overlap (under an injected latency), and the result
/// is what the serial path (<see cref="DeltaTableOptions.MaxConcurrentReads"/> of 1) gives.
/// </summary>
public class ParallelTableRequestTests : IDisposable
{
    private const int Hint = 8;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("name", StringType.Default, true))
        .Build();

    private readonly string _root;

    public ParallelTableRequestTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"delta_parallel_requests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    private string Dir(string name)
    {
        string path = Path.Combine(_root, name);
        Directory.CreateDirectory(path);
        return path;
    }

    private static RecordBatch Rows(long first, int count)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (long id = first; id < first + count; id++)
        {
            ids.Append(id);
            if (id % 7 == 0) names.AppendNull(); else names.Append($"row-{id}");
        }
        return new RecordBatch(Schema, [ids.Build(), names.Build()], count);
    }

    /// <summary><paramref name="window"/> null: the file system's hint (<see cref="Hint"/>) decides.</summary>
    private static DeltaTableOptions Options(int? window) => new() { MaxConcurrentReads = window };

    private static async Task<DeltaTable> OpenAsync(LatencyFileSystem fs, int? window)
    {
        var table = await DeltaTable.OpenAsync(fs, Options(window));
        fs.Reset(); // what the open itself read is not under test
        return table;
    }

    // ── History ────────────────────────────────────────────────────────────────────────────────────────────

    private async Task<string> CreateManyCommitsAsync(int commits)
    {
        string dir = Dir("history");
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(dir), Schema);
        for (int c = 0; c < commits; c++)
            await table.WriteAsync([Rows(c * 10L, 10)]);
        return dir;
    }

    private static async Task<List<string>> HistoryAsync(DeltaTable table)
    {
        var entries = new List<string>();
        await foreach (var e in table.GetHistoryAsync())
            entries.Add($"{e.Version}:{e.Operation}:{e.TimestampMs}");
        return entries;
    }

    [Fact]
    public async Task History_ReadsCommitsAhead_AndYieldsThemInVersionOrder()
    {
        string dir = await CreateManyCommitsAsync(30);
        var serialFs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 15);
        var parallelFs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 15);
        await using var serial = await OpenAsync(serialFs, window: 1);
        await using var parallel = await OpenAsync(parallelFs, window: null);

        var expected = await HistoryAsync(serial);
        var actual = await HistoryAsync(parallel);

        Assert.Equal(31, expected.Count); // CREATE TABLE + 30 writes
        Assert.Equal(Enumerable.Range(0, 31).Select(v => (long)v), actual.Select(e => long.Parse(e.Split(':')[0])));
        Assert.Equal(expected, actual);
        Assert.Equal(1, serialFs.MaxCommitReads);
        Assert.InRange(parallelFs.MaxCommitReads, 2, Hint);
        Assert.Equal(0, parallelFs.CommitReadsInFlight);
    }

    [Fact]
    public async Task History_AnEarlyExit_CancelsAndAwaitsTheReadsAhead()
    {
        string dir = await CreateManyCommitsAsync(30);
        var fs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 15);
        await using var table = await OpenAsync(fs, window: null);

        int taken = 0;
        await foreach (var _ in table.GetHistoryAsync())
        {
            if (++taken == 3)
                break;
        }

        Assert.Equal(0, fs.CommitReadsInFlight); // nothing left running after the enumeration ends
        Assert.InRange(fs.CommitReadsStarted, 3, 3 + Hint);
    }

    /// <summary>
    /// Cancelling after the first entry ends the enumeration with a cancellation even when every remaining commit was
    /// already read ahead: the reads that completed never look at the token again (#714 review).
    /// </summary>
    [Fact]
    public async Task History_Cancelled_WithTheRestAlreadyReadAhead_StillThrows()
    {
        string dir = await CreateManyCommitsAsync(4); // 5 versions: all inside one read-ahead window
        var fs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 0);
        await using var table = await OpenAsync(fs, window: null);
        using var cts = new CancellationTokenSource();

        var seen = new List<long>();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var e in table.GetHistoryAsync(cts.Token))
            {
                seen.Add(e.Version);
                if (seen.Count == 1)
                {
                    while (fs.CommitReadsInFlight > 0 || fs.CommitReadsStarted < 5)
                        await Task.Delay(5); // every remaining read has completed
                    cts.Cancel();
                }
            }
        });

        Assert.Equal([0L], seen);
        Assert.Equal(0, fs.CommitReadsInFlight);
    }

    [Fact]
    public async Task History_AFailingCommitRead_ThrowsAtItsVersion_AfterTheEntriesBeforeIt()
    {
        string dir = await CreateManyCommitsAsync(20);
        var fs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 5)
        {
            FailRead = path => path.EndsWith("00000000000000000010.json", StringComparison.Ordinal),
        };
        await using var table = await OpenAsync(fs, window: null);

        var seen = new List<long>();
        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var e in table.GetHistoryAsync())
                seen.Add(e.Version);
        });

        Assert.Equal(Enumerable.Range(0, 10).Select(v => (long)v), seen);
        Assert.Equal(0, fs.CommitReadsInFlight);
    }

    // ── VACUUM ─────────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>Sixteen one-commit files, then an overwrite: the sixteen are unreferenced.</summary>
    private async Task<string> CreateOrphansAsync(string name, int orphans = 16)
    {
        string dir = Dir(name);
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(dir), Schema);
        for (int f = 0; f < orphans; f++)
            await table.WriteAsync([Rows(f * 10L, 10)]);
        await table.WriteAsync([Rows(1_000, 10)], DeltaWriteMode.Overwrite);
        return dir;
    }

    private static int DataFiles(string dir) =>
        Directory.EnumerateFiles(dir, "*.parquet", SearchOption.TopDirectoryOnly).Count();

    [Theory]
    [InlineData(1)]
    [InlineData(null)]
    public async Task Vacuum_DeletesTheUnreferencedFiles_SeveralAtOnce(int? window)
    {
        string dir = await CreateOrphansAsync($"vacuum-{window}");
        Assert.Equal(17, DataFiles(dir));
        var fs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 15);
        await using var table = await OpenAsync(fs, window);

        var result = await table.VacuumAsync(retentionPeriod: TimeSpan.Zero);

        Assert.Equal(16, result.FilesToDelete.Count);
        Assert.Equal(16, result.FilesDeleted);
        Assert.Equal(1, DataFiles(dir));
        if (window == 1)
            Assert.Equal(1, fs.MaxDeletes);
        else
            Assert.InRange(fs.MaxDeletes, 2, Hint);
        Assert.Equal(0, fs.DeletesInFlight);

        var history = await HistoryAsync(table);
        Assert.Contains(":VACUUM END:", history[^1]);
        Assert.Equal(10, await RowCountAsync(table));
    }

    /// <summary>
    /// The third delete fails while the first two are held open (they wait until cancelled), so the outcome does
    /// not depend on scheduling: no delete starts after the failure, the two held ones are cancelled, and so
    /// nothing at all is deleted (#714 review).
    /// </summary>
    [Fact]
    public async Task Vacuum_AFailingDelete_StopsTheRest_AndWritesNoVacuumEnd()
    {
        string dir = await CreateOrphansAsync("vacuum-fail");
        int deletes = 0;
        var fs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 15)
        {
            FailDelete = _ => Interlocked.Increment(ref deletes) == 3,
            HoldDeletes = true,
        };
        await using var table = await OpenAsync(fs, window: null);

        await Assert.ThrowsAsync<IOException>(async () => await table.VacuumAsync(retentionPeriod: TimeSpan.Zero));

        Assert.Equal(3, fs.DeletesStarted);
        Assert.Equal(0, fs.DeletesInFlight);
        Assert.Equal(17, DataFiles(dir));

        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir));
        var history = await HistoryAsync(reopened);
        Assert.Contains(":VACUUM START:", history[^1]);
        Assert.Equal(10, await RowCountAsync(reopened));

        // The next VACUUM collects what was left.
        var again = await reopened.VacuumAsync(retentionPeriod: TimeSpan.Zero);
        Assert.Equal(1, DataFiles(dir));
        Assert.Equal(again.FilesToDelete.Count, again.FilesDeleted);
    }

    private static async Task<long> RowCountAsync(DeltaTable table)
    {
        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }

    // ── Compaction ─────────────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Twelve small files with row tracking, and deletion vectors on two of them (one cutting a file, one
    /// deleting a whole file's rows), built once and copied, so the serial and parallel compactions start from
    /// byte-identical tables.
    /// </summary>
    private async Task<(string Serial, string Parallel)> CreateCompactionTablesAsync()
    {
        string template = Dir("compact-template");
        await using (var table = await DeltaTable.CreateAsync(
                         new LocalTableFileSystem(template), Schema,
                         new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = 50 } },
                         enableDeletionVectors: true, enableRowTracking: true))
        {
            for (int f = 0; f < 12; f++)
                await table.WriteAsync([Rows(f * 200L, 200)]);
            await table.DeleteAsync(Ex.And(
                Ex.GreaterThanOrEqual("id", LiteralValue.Of(130L)), Ex.LessThan("id", LiteralValue.Of(260L))));
            await table.DeleteAsync(Ex.And(
                Ex.GreaterThanOrEqual("id", LiteralValue.Of(1_000L)), Ex.LessThan("id", LiteralValue.Of(1_200L))));
        }

        string serial = Dir("compact-serial"), parallel = Dir("compact-parallel");
        foreach (string target in new[] { serial, parallel })
        {
            foreach (string d in Directory.EnumerateDirectories(template, "*", SearchOption.AllDirectories))
                Directory.CreateDirectory(Path.Combine(target, d.Substring(template.Length + 1)));
            foreach (string f in Directory.EnumerateFiles(template, "*", SearchOption.AllDirectories))
                File.Copy(f, Path.Combine(target, f.Substring(template.Length + 1)));
        }
        return (serial, parallel);
    }

    private static readonly CompactionOptions CompactEverything = new()
    {
        MinFileSize = long.MaxValue,
        TargetFileSize = long.MaxValue,
    };

    /// <summary>Every row in table order, with its stable row id and commit version.</summary>
    private static async Task<string> RenderAsync(DeltaTable table)
    {
        var text = new StringBuilder();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            for (int r = 0; r < batch.Length; r++)
            {
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    string value = batch.Column(c) switch
                    {
                        Int64Array a => a.GetValue(r)?.ToString() ?? "null",
                        StringArray s => s.GetString(r) ?? "null",
                        var other => other.Data.DataType.Name,
                    };
                    text.Append(value).Append(c + 1 < batch.ColumnCount ? "," : "\n");
                }
            }
            batch.Dispose();
        }
        return text.ToString();
    }

    [Fact]
    public async Task Compaction_ReadsTheSourceFilesAtOnce_AndWritesWhatASerialCompactionWrites()
    {
        var (serialDir, parallelDir) = await CreateCompactionTablesAsync();
        var serialFs = new LatencyFileSystem(new LocalTableFileSystem(serialDir), Hint, delayMs: 20);
        var parallelFs = new LatencyFileSystem(new LocalTableFileSystem(parallelDir), Hint, delayMs: 20)
        {
            AwaitSecondOpen = true,
        };
        await using var serial = await OpenAsync(serialFs, window: 1);
        await using var parallel = await OpenAsync(parallelFs, window: null);
        Assert.Equal(Hint, parallel.EffectiveConcurrentReads);

        Assert.NotNull(await serial.CompactAsync(CompactEverything));
        Assert.NotNull(await parallel.CompactAsync(CompactEverything));

        Assert.Equal(1, serialFs.MaxOpening);
        Assert.InRange(parallelFs.MaxOpening, 2, Hint);
        Assert.Equal(0, parallelFs.OpenDataFiles);
        Assert.Equal(0, serialFs.OpenDataFiles);

        // The same rows, ids and commit versions, in the same order, and the same files byte for byte (only their
        // names are random): the batches reached the writer in the serial order.
        Assert.Equal(await RenderAsync(serial), await RenderAsync(parallel));
        Assert.Equal(
            serial.CurrentSnapshot.ActiveFiles.Values.Select(f => f.Size).OrderBy(s => s),
            parallel.CurrentSnapshot.ActiveFiles.Values.Select(f => f.Size).OrderBy(s => s));
        Assert.Equal(12 * 200 - 130 - 200, await RowCountAsync(parallel));
    }

    [Fact]
    public async Task Compaction_ASourceFileThatCannotBeOpened_Fails_AndClosesTheOthers()
    {
        var (_, dir) = await CreateCompactionTablesAsync();
        // A data path, not an ActiveFiles key: a file with a deletion vector is keyed by its path AND the vector.
        string victim;
        await using (var probe = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir)))
        {
            victim = probe.CurrentSnapshot.ActiveFiles.Values.Select(f => f.Path)
                .OrderBy(p => p, StringComparer.Ordinal).ElementAt(5);
        }
        bool injected = false;
        var fs = new LatencyFileSystem(new LocalTableFileSystem(dir), Hint, delayMs: 20)
        {
            FailOpen = path => path == victim && (injected = true),
        };
        await using var table = await OpenAsync(fs, window: null);
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<IOException>(async () => await table.CompactAsync(CompactEverything));

        Assert.True(injected);
        Assert.Equal(0, fs.OpenDataFiles);
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir));
        Assert.Equal(before, reopened.CurrentSnapshot.Version); // nothing committed
        Assert.Equal(12 * 200 - 130 - 200, await RowCountAsync(reopened));
    }

    /// <summary>
    /// The table file system with a latency on the requests under test, measuring how many overlap: commit reads
    /// (<c>_delta_log/*.json</c> through <see cref="ReadAllBytesAsync"/>), deletes, and data-file opens (across
    /// their latency; how long a file then stays open says nothing about overlap, since a small one is read and
    /// closed in well under a millisecond). Open data files are counted too, from open to dispose, to show that
    /// every one is closed. Suggests <paramref name="hint"/> through <see cref="IReadConcurrencyHint"/>, as the
    /// object-store file systems do.
    /// </summary>
    private sealed class LatencyFileSystem(ITableFileSystem inner, int hint, int delayMs)
        : ITableFileSystem, IReadConcurrencyHint
    {
        private int _commitReads, _deletes, _openDataFiles, _opening;
        public int MaxCommitReads, MaxDeletes, MaxOpening, CommitReadsStarted, DeletesStarted;

        public Func<string, bool>? FailRead { get; init; }
        public Func<string, bool>? FailDelete { get; init; }
        public Func<string, bool>? FailOpen { get; init; }

        /// <summary>Every delete that does not fail waits until it is cancelled, so a failing one lands while the
        /// earlier ones are still in flight, whatever the scheduling.</summary>
        public bool HoldDeletes { get; init; }

        /// <summary>The first data-file open waits (up to 10 s) until a second one is in flight: proves overlap
        /// without depending on how fast the machine dispatches them. Only for a run that is expected to overlap.</summary>
        public bool AwaitSecondOpen { get; init; }

        public int CommitReadsInFlight => Volatile.Read(ref _commitReads);
        public int DeletesInFlight => Volatile.Read(ref _deletes);
        public int OpenDataFiles => Volatile.Read(ref _openDataFiles);

        public int? PreferredConcurrentReads => hint;
        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public void Reset()
        {
            MaxCommitReads = MaxDeletes = MaxOpening = CommitReadsStarted = DeletesStarted = 0;
        }

        private static void Enter(ref int counter, ref int max)
        {
            int now = Interlocked.Increment(ref counter);
            int seen;
            while (now > (seen = Volatile.Read(ref max)))
                Interlocked.CompareExchange(ref max, now, seen);
        }

        private static bool IsCommit(string path) =>
            path.Replace('\\', '/').StartsWith("_delta_log/", StringComparison.Ordinal)
            && path.EndsWith(".json", StringComparison.Ordinal);

        private static bool IsDataFile(string path) =>
            path.EndsWith(".parquet", StringComparison.Ordinal) && !path.Contains("_delta_log");

        public async ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            if (!IsCommit(path))
                return await inner.ReadAllBytesAsync(path, cancellationToken);
            Interlocked.Increment(ref CommitReadsStarted);
            Enter(ref _commitReads, ref MaxCommitReads);
            try
            {
                await Task.Delay(delayMs, cancellationToken);
                if (FailRead?.Invoke(path) == true)
                    throw new IOException($"cannot read {path}");
                return await inner.ReadAllBytesAsync(path, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _commitReads);
            }
        }

        public async ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref DeletesStarted);
            Enter(ref _deletes, ref MaxDeletes);
            try
            {
                if (FailDelete?.Invoke(path) == true)
                    throw new IOException($"cannot delete {path}");
                await Task.Delay(HoldDeletes ? Timeout.Infinite : delayMs, cancellationToken);
                await inner.DeleteAsync(path, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _deletes);
            }
        }

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            if (!IsDataFile(path))
                return await inner.OpenReadAsync(path, cancellationToken);
            Enter(ref _opening, ref MaxOpening);
            IRandomAccessFile file;
            try
            {
                if (AwaitSecondOpen)
                {
                    var waited = System.Diagnostics.Stopwatch.StartNew();
                    while (Volatile.Read(ref MaxOpening) < 2 && waited.Elapsed < TimeSpan.FromSeconds(10))
                        await Task.Delay(1, cancellationToken);
                }
                await Task.Delay(delayMs, cancellationToken);
                if (FailOpen?.Invoke(path) == true)
                    throw new IOException($"cannot open {path}");
                file = await inner.OpenReadAsync(path, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _opening);
            }
            Interlocked.Increment(ref _openDataFiles);
            return new CountedFile(file, this);
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
            => inner.ExistsAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);

        private sealed class CountedFile(IRandomAccessFile file, LatencyFileSystem owner) : IRandomAccessFile
        {
            private int _closed;

            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => file.GetLengthAsync(cancellationToken);
            public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
                => file.ReadAsync(range, cancellationToken);
            public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
                => file.ReadRangesAsync(ranges, cancellationToken);

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _closed, 1) == 0)
                    Interlocked.Decrement(ref owner._openDataFiles);
                file.Dispose();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }
        }
    }
}
