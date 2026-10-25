// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A predicate DELETE prunes the row groups and pages of a file it opens, as a filtered scan does (#439).
/// Every table here starts as ONE file of 300 rows, <c>id</c> 0..299 in row groups of 100 and pages of 10,
/// so row position equals <c>id</c>. The oracle is the same table deleted through the functional overload,
/// which has nothing to prune by and reads every row: the two must leave the same rows, the same deletion
/// vector and the same change rows. A byte count of the data-file reads shows the predicate's read was
/// actually narrowed, without which every comparison would pass trivially.
/// </summary>
public class DeletePagePruningTests : IDisposable
{
    private const int RowsPerGroup = 100;
    private const int RowsPerPage = 10;
    private const int Rows = 300;

    /// <summary>Every row of the pages [10, 20) and [70, 80) of row group 0, and nothing else.</summary>
    private static readonly Predicate TwoWholePages = Ex.Or(
        Ex.And(Ex.GreaterThanOrEqual("id", 10L), Ex.LessThan("id", 20L)),
        Ex.And(Ex.GreaterThanOrEqual("id", 70L), Ex.LessThan("id", 80L)));

    /// <summary>One row in each of the same two pages.</summary>
    private static readonly Predicate TwoRows = Ex.Or(Ex.Equal("id", 15L), Ex.Equal("id", 75L));

    private static List<long> TwoWholePagesRows =>
        [.. Enumerable.Range(10, RowsPerPage).Select(v => (long)v), .. Enumerable.Range(70, RowsPerPage).Select(v => (long)v)];

    private readonly string _tempDir;

    public DeletePagePruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_delpp_{Guid.NewGuid():N}");
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
        .Field(new Field("payload", StringType.Default, false))
        .Build();

    private static readonly Apache.Arrow.Schema PartitionedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("part", StringType.Default, false))
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("payload", StringType.Default, false))
        .Build();

    /// <summary>The shape of the table both twins are built with.</summary>
    private sealed record Shape(
        ColumnMappingMode Mode = ColumnMappingMode.None, bool Dv = true, bool Cdf = false,
        bool RowTracking = false, bool Partitioned = false);

    /// <summary>A batch size smaller than a page, so a kept range comes back as several batches.</summary>
    private static DeltaTableOptions SmallPages() => new()
    {
        ParquetWriteOptions = ParquetWriteOptions.Default with
        {
            RowGroupMaxRows = RowsPerGroup,
            DataPageRowCountLimit = RowsPerPage,
        },
        ParquetReadOptions = ParquetReadOptions.Default with { BatchSize = 4 },
    };

    private async Task<(DeltaTable Table, CountingFileSystem Fs)> CreateAsync(string name, Shape shape)
    {
        var fs = new CountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var configuration = shape.Cdf
            ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }
            : null;
        var table = await DeltaTable.CreateAsync(
            fs, shape.Partitioned ? PartitionedSchema : TableSchema, SmallPages(),
            partitionColumns: shape.Partitioned ? ["part"] : null,
            columnMappingMode: shape.Mode, enableDeletionVectors: shape.Dv,
            enableRowTracking: shape.RowTracking, configuration: configuration);
        await table.WriteAsync([Batch(shape.Partitioned, 0, Rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        return (table, fs);
    }

    /// <summary>Distinct, poorly compressible payloads, so the data pages outweigh the footer and indexes.</summary>
    private static RecordBatch Batch(bool partitioned, int start, int count)
    {
        var ids = new Int64Array.Builder();
        var payloads = new StringArray.Builder();
        for (int i = start; i < start + count; i++)
        {
            ids.Append(i);
            payloads.Append(string.Concat(Enumerable.Range(0, 6).Select(k => Guid.NewGuid().ToString("N"))));
        }
        if (!partitioned)
            return new RecordBatch(TableSchema, [ids.Build(), payloads.Build()], count);
        var parts = new StringArray.Builder().AppendRange(Enumerable.Repeat("a", count)).Build();
        return new RecordBatch(PartitionedSchema, [parts, ids.Build(), payloads.Build()], count);
    }

    /// <summary>
    /// Builds the table twice, runs <paramref name="setup"/> on each, then deletes <paramref name="predicate"/>
    /// by predicate from one and through the whole-file functional overload from the other.
    /// </summary>
    private async Task<(Twin Pruned, Twin Oracle)> DeleteBothWaysAsync(
        Shape shape, Predicate predicate, Func<DeltaTable, Task>? setup = null)
    {
        var (pruned, prunedFs) = await CreateAsync("pruned", shape);
        var (oracle, oracleFs) = await CreateAsync("oracle", shape);
        if (setup is not null)
        {
            await setup(pruned);
            await setup(oracle);
        }

        prunedFs.Reset();
        oracleFs.Reset();
        var (prunedDeleted, _) = await pruned.DeleteAsync(predicate);
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        return (new Twin(pruned, prunedFs, prunedDeleted, prunedFs.DataBytesRead),
            new Twin(oracle, oracleFs, oracleDeleted, oracleFs.DataBytesRead));
    }

    /// <summary>One side of a <see cref="DeleteBothWaysAsync"/> run.</summary>
    /// <param name="BytesRead">The data-file bytes the DELETE alone read.</param>
    private sealed record Twin(DeltaTable Table, CountingFileSystem Fs, long Deleted, long BytesRead) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => Table.DisposeAsync();
    }

    /// <summary>The surviving ids, and each active file's deletion-vector positions (empty when it has none).</summary>
    private static async Task<(List<long> Ids, List<List<long>> Vectors)> StateAsync(Twin twin)
    {
        var ids = new List<long>();
        await foreach (var batch in twin.Table.ReadAsync(new DeltaReadOptions()))
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();

        var vectors = new List<List<long>>();
        foreach (var addFile in twin.Table.CurrentSnapshot.ActiveFiles.Values.OrderBy(f => f.Path, StringComparer.Ordinal))
        {
            var positions = addFile.DeletionVector is null
                ? []
                : (await new DeletionVectors.DeletionVectorReader(twin.Fs).ReadAsync(addFile.DeletionVector, default)).ToList();
            positions.Sort();
            vectors.Add(positions);
        }
        return (ids, vectors);
    }

    private static async Task AssertSameOutcomeAsync(Twin pruned, Twin oracle)
    {
        Assert.Equal(oracle.Deleted, pruned.Deleted);
        var (prunedIds, prunedVectors) = await StateAsync(pruned);
        var (oracleIds, oracleVectors) = await StateAsync(oracle);
        Assert.Equal(oracleIds, prunedIds);
        Assert.Equal(oracleVectors, prunedVectors);
    }

    /// <summary>The predicate's read took a fraction of the bytes the whole-file read did.</summary>
    private static void AssertNarrowed(Twin pruned, Twin oracle) =>
        Assert.True(pruned.BytesRead * 3 < oracle.BytesRead,
            $"pruned read {pruned.BytesRead} bytes, whole-file read {oracle.BytesRead}");

    [Fact]
    public async Task PartialDelete_MatchesTheWholeFileRead()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(new Shape(), TwoRows);
        await using var _ = pruned;
        await using var __ = oracle;

        Assert.Equal(2, pruned.Deleted);
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>
    /// The predicate matches EVERY row the pruned read sees, but not every row of the file. Counted from the
    /// batches read, the file's rows would equal the rows deleted, and the delete would remove the whole file:
    /// 280 rows lost.
    /// </summary>
    [Fact]
    public async Task EveryRowReadMatches_StaysADeletionVectorDelete()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(new Shape(), TwoWholePages);
        await using var _ = pruned;
        await using var __ = oracle;

        var file = Assert.Single(pruned.Table.CurrentSnapshot.ActiveFiles.Values);
        Assert.NotNull(file.DeletionVector);
        Assert.Equal(2 * RowsPerPage, pruned.Deleted);
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>
    /// The same trap without deletion vectors, where it is louder: the partial delete must be refused, where a
    /// short row count would make it a whole-file remove that deletes the 280 rows it never read.
    /// </summary>
    [Fact]
    public async Task EveryRowReadMatches_WithoutDeletionVectors_IsRefused()
    {
        var (table, _) = await CreateAsync("nodv", new Shape(Dv: false));
        await using var __ = table;
        long version = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<InvalidOperationException>(() => table.DeleteAsync(TwoWholePages).AsTask());

        Assert.Equal(version, table.CurrentSnapshot.Version);
        var count = 0L;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
            count += batch.Length;
        Assert.Equal(Rows, count);
    }

    [Fact]
    public async Task EveryRowOfTheFileMatches_RemovesTheFile()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(new Shape(), Ex.GreaterThanOrEqual("id", 0L));
        await using var _ = pruned;
        await using var __ = oracle;

        Assert.Empty(pruned.Table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(Rows, pruned.Deleted);
        await AssertSameOutcomeAsync(pruned, oracle);
    }

    /// <summary>An existing vector holds rows inside the kept pages and outside them; both stay deleted, and
    /// the rows already gone are not counted again.</summary>
    [Fact]
    public async Task ExistingDeletionVector_IsKeptAndExtended()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(
            new Shape(), TwoWholePages,
            setup: table => table.DeleteAsync(Ex.In("id", [LiteralValue.Of(5L), LiteralValue.Of(12L), LiteralValue.Of(150L)])).AsTask());
        await using var _ = pruned;
        await using var __ = oracle;

        Assert.Equal(2 * RowsPerPage - 1, pruned.Deleted); // 12 was already gone
        var (_, vectors) = await StateAsync(pruned);
        Assert.Equal(new List<long> { 5L }.Concat(TwoWholePagesRows).Append(150L), Assert.Single(vectors));
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>
    /// A vector already holds every row outside the kept pages, so deleting those pages leaves nothing: the
    /// file goes whole, though the read saw only 20 of its 300 rows.
    /// </summary>
    [Fact]
    public async Task ExistingDeletionVector_CoveringTheRest_RemovesTheFile()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(
            new Shape(), TwoWholePages,
            setup: table => table.DeleteAsync(Ex.Not(TwoWholePages)).AsTask());
        await using var _ = pruned;
        await using var __ = oracle;

        Assert.Empty(pruned.Table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(2 * RowsPerPage, pruned.Deleted);
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>The change file gets the deleted rows with the ids a whole-file read gives them.</summary>
    [Fact]
    public async Task ChangeDataFeed_RowsAndRowIds_MatchTheWholeFileRead()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(new Shape(Cdf: true, RowTracking: true), TwoRows);
        await using var _ = pruned;
        await using var __ = oracle;

        var prunedFeed = await DeleteFeedAsync(pruned.Table);
        Assert.Equal(new List<(long, long?)> { (15L, 15L), (75L, 75L) }, prunedFeed); // baseRowId 0 + position
        Assert.Equal(await DeleteFeedAsync(oracle.Table), prunedFeed);
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>
    /// After a compaction the row ids are MATERIALIZED and no longer equal the position: the delete of row 3
    /// shifted every row after it. The change rows must still carry each row's own id.
    /// </summary>
    [Fact]
    public async Task ChangeDataFeed_MaterializedRowIds_StayWithTheirRows()
    {
        var shape = new Shape(Cdf: true, RowTracking: true);
        var (pruned, oracle) = await DeleteBothWaysAsync(shape, TwoRows, setup: async table =>
        {
            await table.WriteAsync([Batch(partitioned: false, Rows, 10)]); // a second file to compact with
            await table.DeleteAsync(Ex.Equal("id", 3L));
            Assert.NotNull(await table.CompactAsync());
            Assert.Single(table.CurrentSnapshot.ActiveFiles);
        });
        await using var _ = pruned;
        await using var __ = oracle;

        var prunedFeed = await DeleteFeedAsync(pruned.Table);
        Assert.Equal(new List<(long, long?)> { (15L, 15L), (75L, 75L) }, prunedFeed); // the original ids, not the shifted positions
        Assert.Equal(await DeleteFeedAsync(oracle.Table), prunedFeed);
        await AssertSameOutcomeAsync(pruned, oracle);
    }

    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task ColumnMapping_TranslatesLogicalNames(ColumnMappingMode mode)
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(new Shape(Mode: mode), TwoWholePages);
        await using var _ = pruned;
        await using var __ = oracle;

        Assert.Equal(2 * RowsPerPage, pruned.Deleted);
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>The partition column is not in the file, so it prunes nothing there; the id range still does.</summary>
    [Fact]
    public async Task PartitionedTable_NarrowsByTheFileColumns()
    {
        var (pruned, oracle) = await DeleteBothWaysAsync(
            new Shape(Partitioned: true), Ex.And(Ex.Equal("part", "a"), TwoWholePages));
        await using var _ = pruned;
        await using var __ = oracle;

        Assert.Equal(2 * RowsPerPage, pruned.Deleted);
        await AssertSameOutcomeAsync(pruned, oracle);
        AssertNarrowed(pruned, oracle);
    }

    /// <summary>The delete rows of the latest commit's change feed, as (id, row id), by id.</summary>
    private static async Task<List<(long Id, long? RowId)>> DeleteFeedAsync(DeltaTable table)
    {
        long version = table.CurrentSnapshot.Version;
        var rows = new List<(long, long?)>();
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
        {
            StartVersion = version,
            EndVersion = version,
            Metadata = DeltaRowMetadata.RowTracking,
        }))
        {
            var change = (StringArray)batch.Column(batch.Schema.GetFieldIndex("_change_type"));
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var rowId = (Int64Array)batch.Column(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal("delete", change.GetString(i));
                rows.Add((id.GetValue(i)!.Value, rowId.IsNull(i) ? null : rowId.GetValue(i)));
            }
        }
        rows.Sort();
        return rows;
    }

    /// <summary>Counts the bytes read from data files (Parquet outside the log); everything else passes through.</summary>
    private sealed class CountingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        private long _dataBytesRead;

        public long DataBytesRead => Interlocked.Read(ref _dataBytesRead);

        public void Reset() => Interlocked.Exchange(ref _dataBytesRead, 0);

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public async ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            var file = await inner.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            bool isData = path.EndsWith(".parquet", StringComparison.Ordinal)
                && path.IndexOf("_delta_log", StringComparison.Ordinal) < 0;
            return isData ? new CountingFile(file, this) : file;
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

        private sealed class CountingFile(IRandomAccessFile inner, CountingFileSystem owner) : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
                => inner.GetLengthAsync(cancellationToken);

            public ValueTask<System.Buffers.IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
            {
                Interlocked.Add(ref owner._dataBytesRead, range.Length);
                return inner.ReadAsync(range, cancellationToken);
            }

            public ValueTask<IReadOnlyList<System.Buffers.IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                foreach (var range in ranges)
                    Interlocked.Add(ref owner._dataBytesRead, range.Length);
                return inner.ReadRangesAsync(ranges, cancellationToken);
            }

            public void Dispose() => inner.Dispose();

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }
}
