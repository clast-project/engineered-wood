// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The change feed reads its files several at once, as a scan does, and opens each with the size its action
/// records rather than asking the store first. On an object store the feed was a chain of one request after
/// another, two of them HEADs per data file. What it returns must not change: the same rows in the same order
/// whatever the window, a vacuumed data file still skipped, and a file that exists but cannot be read still an
/// error.
/// </summary>
public class CdfParallelReadTests : IDisposable
{
    private const int Appends = 6;
    private const int RowsPerAppend = 120;

    private readonly string _tempDir;

    public CdfParallelReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdf_parallel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static DeltaTableOptions Options(int window) => new()
    {
        // Several row groups per file, so each file is several batches and the window has something to reorder.
        ParquetWriteOptions = new EngineeredWood.Parquet.ParquetWriteOptions { RowGroupMaxRows = 50 },
        MaxConcurrentReads = window,
    };

    /// <summary>Six appends, an UPDATE touching every file, and a vector delete in two of them. With the change
    /// feed on, the last two versions are read from change files; with it off, from the remove/add pairs, the
    /// vector edits among them.</summary>
    private async Task CreateTableAsync(bool changeDataFeed)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var configuration = new Dictionary<string, string>
        {
            { EngineeredWood.DeltaLake.DeletionVectors.DeletionVectorConfig.EnableKey, "true" },
        };
        if (changeDataFeed)
            configuration[CdfConfig.EnableKey] = "true";
        await new TransactionLog(fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors"],
                WriterFeatures = changeDataFeed ? ["changeDataFeed", "deletionVectors"] : ["deletionVectors"],
            },
            new MetadataAction
            {
                Id = "cdf-parallel",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = configuration,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs, Options(1));
        for (int a = 0; a < Appends; a++)
        {
            var ids = new Int64Array.Builder();
            var values = new StringArray.Builder();
            for (int i = 0; i < RowsPerAppend; i++)
            {
                ids.Append((long)a * RowsPerAppend + i);
                values.Append($"v{a}-{i}");
            }
            await table.WriteAsync([new RecordBatch(table.ArrowSchema, [ids.Build(), values.Build()], RowsPerAppend)]);
        }

        await table.UpdateAsync(
            predicate: batch =>
            {
                var id = (Int64Array)batch.Column(0);
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(id.GetValue(i)!.Value % 7 == 0);
                return mask.Build();
            },
            updater: batch =>
            {
                var values = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    values.Append("updated");
                return new RecordBatch(batch.Schema, [batch.Column(0), values.Build()], batch.Length);
            });

        var paths = table.CurrentSnapshot.ActiveFiles.Values.Select(f => f.Path).OrderBy(p => p, StringComparer.Ordinal)
            .ToList();
        await table.DeleteRowsAsync(RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [paths[1]] = [3, 60, 61],
            [paths[4]] = [0, 119],
        }));
    }

    private static async Task<List<string>> FeedAsync(ITableFileSystem fs, int window)
    {
        await using var table = await DeltaTable.OpenAsync(fs, Options(window));
        var rows = new List<string>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
                       {
                           StartVersion = 0,
                           EndVersion = table.CurrentSnapshot.Version,
                       }))
        {
            for (int r = 0; r < batch.Length; r++)
            {
                var row = new StringBuilder();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    row.Append(batch.Schema.FieldsList[c].Name).Append('=');
                    row.Append(batch.Column(c) switch
                    {
                        Int64Array a => a.GetValue(r)?.ToString() ?? "null",
                        StringArray s => s.GetString(r) ?? "null",
                        TimestampArray t => t.GetValue(r)?.ToString() ?? "null",
                        var other => other.GetType().Name,
                    }).Append(';');
                }
                rows.Add(row.ToString());
            }
        }
        return rows;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AWindow_ReturnsWhatASerialReadReturns_InTheSameOrder(bool changeDataFeed)
    {
        await CreateTableAsync(changeDataFeed);
        var fs = new LocalTableFileSystem(_tempDir);

        var serial = await FeedAsync(fs, window: 1);
        Assert.True(serial.Count > Appends * RowsPerAppend, $"only {serial.Count} change rows");
        Assert.Equal(serial, await FeedAsync(fs, window: 4));
        Assert.Equal(serial, await FeedAsync(fs, window: 16));
    }

    [Fact]
    public async Task AWindow_ReadsSeveralFilesAtOnce()
    {
        await CreateTableAsync(changeDataFeed: true);
        var fs = new CountingFileSystem(new LocalTableFileSystem(_tempDir), delayMs: 20);

        await FeedAsync(fs, window: 4);
        Assert.InRange(fs.MaxReading, 2, 4);

        fs.Reset();
        await FeedAsync(fs, window: 1);
        Assert.Equal(1, fs.MaxReading);
    }

    /// <summary>Without the change feed, the last two versions are vector edits: their vectors are read, and
    /// a side that changed no rows is skipped, on the file's own read, with no existence check either.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NoFile_IsAskedForItsSizeOrWhetherItExists(bool changeDataFeed)
    {
        await CreateTableAsync(changeDataFeed);
        var fs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));

        await using (var table = await DeltaTable.OpenAsync(fs, Options(4)))
        {
            fs.Reset();
            await foreach (var _ in table.ReadChangesAsync(new DeltaChangeReadOptions
                           {
                               StartVersion = 0,
                               EndVersion = table.CurrentSnapshot.Version,
                           }))
            {
            }
        }

        Assert.True(fs.Opened > Appends, $"only {fs.Opened} files opened");
        Assert.Equal(0, fs.LengthAsks);
        Assert.Equal(0, fs.ExistsAsks);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AVacuumedDataFile_IsStillSkipped(int window)
    {
        await CreateTableAsync(changeDataFeed: true);
        var fs = new LocalTableFileSystem(_tempDir);
        var before = await FeedAsync(fs, window);

        // The first append's file, gone as VACUUM would remove it. Its inserts drop out of the feed, nothing else.
        var add = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single();
        File.Delete(Path.Combine(_tempDir, DeltaPath.Decode(add.Path)));

        var after = await FeedAsync(fs, window);
        var expected = before.Where(r => !(r.Contains("_change_type=insert;") && r.Contains("_commit_version=1;")))
            .ToList();
        Assert.Equal(before.Count - RowsPerAppend, expected.Count);
        Assert.Equal(expected, after);
    }

    /// <summary>
    /// A vector edit whose vector lives in a file. When VACUUM has removed the data file, and the vector file
    /// with it, reading the vectors fails; only then is the data file's existence asked, and the edit
    /// contributes nothing. When the data file is still there, the same failure is an error, not a skip.
    /// </summary>
    [Theory]
    [InlineData(1, true)]
    [InlineData(4, true)]
    [InlineData(1, false)]
    [InlineData(4, false)]
    public async Task AVectorEdit_WhoseVectorsAreGone_IsSkippedOnlyWhenItsFileIsGone(int window, bool dataFileGone)
    {
        await CreateTableAsync(changeDataFeed: false);
        var fs = new LocalTableFileSystem(_tempDir);
        long version;
        AddFile target;
        await using (var table = await DeltaTable.OpenAsync(fs, Options(1)))
        {
            version = table.CurrentSnapshot.Version + 1;
            target = table.CurrentSnapshot.ActiveFiles.Values.First(f => f.DeletionVector is null);
        }

        // A file-backed vector (storage type "u") whose .bin file does not exist: Z85 "0" x 20 is the all-zero uuid.
        var missingVector = new DeletionVector
        {
            StorageType = "u", PathOrInlineDv = new string('0', 20), Offset = 1, SizeInBytes = 34, Cardinality = 2,
        };
        await new TransactionLog(fs).WriteCommitAsync(version, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = target.Path, DataChange = true, DeletionTimestamp = 1, Size = target.Size,
                PartitionValues = target.PartitionValues,
            },
            target with { DeletionVector = missingVector, DataChange = true },
        });
        if (dataFileGone)
            File.Delete(Path.Combine(_tempDir, DeltaPath.Decode(target.Path)));

        await using var reopened = await DeltaTable.OpenAsync(fs, Options(window));
        var feed = reopened.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version });
        if (dataFileGone)
        {
            await foreach (var batch in feed)
                Assert.Equal(0, batch.Length);
        }
        else
        {
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await foreach (var _ in feed)
                {
                }
            });
        }
    }

    /// <summary>Leaving the feed early stops it: the commits after the point it reached are not read. A
    /// cancelled commit read used to be taken for a missing version and skipped, so the dispatcher went on
    /// through every remaining version before the feed could close.</summary>
    [Fact]
    public async Task LeavingEarly_ReadsNoFurtherCommits()
    {
        var fs = new CountingFileSystem(new LocalTableFileSystem(_tempDir));
        const int commits = 40;
        await using (var table = await DeltaTable.CreateAsync(
                         fs, Schema(), configuration: new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" }))
        {
            for (int c = 0; c < commits; c++)
            {
                var ids = new Int64Array.Builder().Append(c).Build();
                var values = new StringArray.Builder().Append("v").Build();
                await table.WriteAsync([new RecordBatch(table.ArrowSchema, [ids, values], 1)]);
            }
        }

        await using (var table = await DeltaTable.OpenAsync(fs, Options(2)))
        {
            fs.Reset();
            await foreach (var _ in table.ReadChangesAsync(new DeltaChangeReadOptions
                           {
                               StartVersion = 0,
                               EndVersion = table.CurrentSnapshot.Version,
                           }))
            {
                break;
            }
        }

        // The window of 2 lets the dispatcher run a few units ahead, never most of the range.
        Assert.InRange(fs.CommitReads, 1, commits / 2);
    }

    private static Apache.Arrow.Schema Schema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Apache.Arrow.Types.Int64Type.Default, false))
        .Field(new Field("value", Apache.Arrow.Types.StringType.Default, true))
        .Build();

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AnUnreadableDataFile_IsAnError_NotSkipped(int window)
    {
        await CreateTableAsync(changeDataFeed: true);
        var fs = new LocalTableFileSystem(_tempDir);

        // There, at its recorded size, but not Parquet: the open fails on a file that exists.
        var add = (await new TransactionLog(fs).ReadCommitAsync(1)).OfType<AddFile>().Single();
        string path = Path.Combine(_tempDir, DeltaPath.Decode(add.Path));
        File.WriteAllBytes(path, new byte[new FileInfo(path).Length]);

        await Assert.ThrowsAnyAsync<Exception>(() => FeedAsync(fs, window));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AMissingChangeFile_IsAnError(int window)
    {
        await CreateTableAsync(changeDataFeed: true);
        var fs = new LocalTableFileSystem(_tempDir);

        var cdc = Directory.GetFiles(Path.Combine(_tempDir, "_change_data")).First();
        File.Delete(cdc);

        await Assert.ThrowsAnyAsync<Exception>(() => FeedAsync(fs, window));
    }

    /// <summary>Counts, for data and change files (Parquet outside the log): the opens, the size asks
    /// (<see cref="IRandomAccessFile.GetLengthAsync"/>), the existence checks, and the most FILES with a read in
    /// flight at once, each read delayed by <paramref name="delayMs"/> so that overlapping reads are seen to overlap.
    /// Files, not reads: a file read as one unit fetches several row groups at once (#846).</summary>
    private sealed class CountingFileSystem(ITableFileSystem inner, int delayMs = 0) : ITableFileSystem
    {
        private int _opened, _lengthAsks, _existsAsks, _reading, _maxReading, _commitReads;

        /// <summary>The commit files read (<see cref="ReadAllBytesAsync"/> of a <c>.json</c> in the log).</summary>
        public int CommitReads => Volatile.Read(ref _commitReads);

        public int Opened => Volatile.Read(ref _opened);
        public int LengthAsks => Volatile.Read(ref _lengthAsks);
        public int ExistsAsks => Volatile.Read(ref _existsAsks);
        public int MaxReading => Volatile.Read(ref _maxReading);

        public void Reset()
        {
            Interlocked.Exchange(ref _opened, 0);
            Interlocked.Exchange(ref _lengthAsks, 0);
            Interlocked.Exchange(ref _existsAsks, 0);
            Interlocked.Exchange(ref _maxReading, 0);
            Interlocked.Exchange(ref _commitReads, 0);
        }

        private static bool IsTracked(string path) =>
            path.EndsWith(".parquet", StringComparison.Ordinal)
            && path.IndexOf("_delta_log", StringComparison.Ordinal) < 0;

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            if (!IsTracked(path))
                return file;
            Interlocked.Increment(ref _opened);
            return new CountingFile(file, this);
        }

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
        {
            if (IsTracked(path))
                Interlocked.Increment(ref _existsAsks);
            return inner.ExistsAsync(path, cancellationToken);
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default)
            => inner.ListAsync(prefix, cancellationToken);
        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            if (path.IndexOf("_delta_log", StringComparison.Ordinal) >= 0
                && path.EndsWith(".json", StringComparison.Ordinal))
                Interlocked.Increment(ref _commitReads);
            return inner.ReadAllBytesAsync(path, cancellationToken);
        }
        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
            => inner.CreateAsync(path, overwrite, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(path, cancellationToken);
        public ValueTask<bool> TryWriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.TryWriteAllBytesAsync(path, data, cancellationToken);
        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
            => inner.WriteAllBytesAsync(path, data, cancellationToken);

        private sealed class CountingFile(IRandomAccessFile file, CountingFileSystem owner) : IRandomAccessFile
        {
            private int _inFlight;

            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref owner._lengthAsks);
                return file.GetLengthAsync(cancellationToken);
            }

            public async ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
            {
                await EnterAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    return await file.ReadAsync(range, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    Leave();
                }
            }

            public async ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                await EnterAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    return await file.ReadRangesAsync(ranges, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    Leave();
                }
            }

            private async Task EnterAsync(CancellationToken cancellationToken)
            {
                if (Interlocked.Increment(ref _inFlight) == 1)
                {
                    int now = Interlocked.Increment(ref owner._reading);
                    int seen;
                    while (now > (seen = Volatile.Read(ref owner._maxReading)))
                        Interlocked.CompareExchange(ref owner._maxReading, now, seen);
                }
                if (owner.DelayMs > 0)
                    await Task.Delay(owner.DelayMs, cancellationToken).ConfigureAwait(false);
            }

            private void Leave()
            {
                if (Interlocked.Decrement(ref _inFlight) == 0)
                    Interlocked.Decrement(ref owner._reading);
            }

            public void Dispose() => file.Dispose();

            public ValueTask DisposeAsync() => file.DisposeAsync();
        }

        private int DelayMs => delayMs;
    }
}
