// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="RowSelection"/> — the one DML boundary key — and the hazard it exists to remove. A file ordinal
/// is only meaningful in the snapshot it was minted against; resolving it to a PATH at construction turns two
/// classes of silent wrong answer (addressing a different file, addressing nothing) into either a correct
/// answer or an exception naming the file.
/// </summary>
public class RowSelectionTests : IDisposable
{
    private readonly string _tempDir;

    public RowSelectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rowsel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(IdSchema, [ids.Build()], count);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private static List<string> OrderedPaths(Snapshot.Snapshot snapshot) =>
        snapshot.ActiveFiles.Values.Select(a => a.Path)
            .OrderBy(p => p, StringComparer.Ordinal).ToList();

    private async Task<List<long>> ReadIdsFreshAsync()
    {
        await using var reader = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in reader.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    // ── shape ──

    [Fact]
    public void ByPath_ReportsPathsPositionsAndTotal()
    {
        var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            ["a.parquet"] = new long[] { 0, 2, 2, 5 },  // a duplicated position counts once
            ["b.parquet"] = new long[] { 7 },
            ["c.parquet"] = [],                          // an empty entry names no file at all
        });

        Assert.Equal(["a.parquet", "b.parquet"], selection.Paths.OrderBy(p => p, StringComparer.Ordinal));
        Assert.Equal([0L, 2L, 5L], selection.PositionsFor("a.parquet").OrderBy(p => p));
        Assert.Equal([7L], selection.PositionsFor("b.parquet"));
        Assert.Empty(selection.PositionsFor("c.parquet"));
        Assert.Empty(selection.PositionsFor("never-heard-of-it.parquet"));
        Assert.Equal(4, selection.TotalPositions);
        Assert.False(selection.IsEmpty);
    }

    [Fact]
    public void ByPath_NoEntries_IsEmpty()
    {
        var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>());
        Assert.True(selection.IsEmpty);
        Assert.Equal(0, selection.TotalPositions);
        Assert.Empty(selection.Paths);
    }

    [Fact]
    public void ByPath_NegativePosition_Throws() =>
        Assert.Throws<ArgumentException>(() => RowSelection.ByPath(
            new Dictionary<string, IReadOnlyCollection<long>> { ["a.parquet"] = new long[] { -1 } }));

    // ── FromLocatorColumns ──

    private static RecordBatch LocatorBatch(
        string prefix, params (string Path, long Position)[] rows)
    {
        var paths = new StringArray.Builder();
        var positions = new Int64Array.Builder();
        foreach (var (path, position) in rows)
        {
            if (path is null) paths.AppendNull(); else paths.Append(path);
            positions.Append(position);
        }
        return new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field(prefix + RowSelection.FilePathColumnSuffix, StringType.Default, true))
                .Field(new Field(prefix + RowSelection.RowIndexColumnSuffix, Int64Type.Default, true))
                .Build(),
            [paths.Build(), positions.Build()], rows.Length);
    }

    [Fact]
    public void FromLocatorColumns_AccumulatesAcrossBatches()
    {
        var selection = RowSelection.FromLocatorColumns(
        [
            LocatorBatch(RowSelection.DefaultMetadataPrefix, ("a.parquet", 0), ("b.parquet", 4)),
            LocatorBatch(RowSelection.DefaultMetadataPrefix, ("a.parquet", 3), ("a.parquet", 0)),
        ]);

        Assert.Equal(["a.parquet", "b.parquet"], selection.Paths.OrderBy(p => p, StringComparer.Ordinal));
        Assert.Equal([0L, 3L], selection.PositionsFor("a.parquet").OrderBy(p => p));
        Assert.Equal(3, selection.TotalPositions);
    }

    [Fact]
    public void FromLocatorColumns_HonorsACustomPrefix()
    {
        var selection = RowSelection.FromLocatorColumns(
            [LocatorBatch("__meta_", ("a.parquet", 1))], metadataPrefix: "__meta_");
        Assert.Equal([1L], selection.PositionsFor("a.parquet"));
    }

    [Fact]
    public void FromLocatorColumns_WrongPrefix_Throws() =>
        Assert.Throws<ArgumentException>(() => RowSelection.FromLocatorColumns(
            [LocatorBatch("__meta_", ("a.parquet", 1))]));

    // ── the stale-ordinal hazard ──

    /// <summary>
    /// The failure the path key removes: a concurrent commit dropped the FIRST path-sorted file, so every
    /// ordinal after it now names a different file. Addresses minted against the pinned snapshot still name
    /// the rows they were read from — and the same addresses resolved against the CURRENT snapshot demonstrably
    /// name something else, which is what the old ordinal-keyed DML would have deleted.
    /// </summary>
    [Fact]
    public async Task FromRowAddresses_ResolvesAgainstThePinnedSnapshot_NotWhateverIsCurrent()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(10, 2)]);
        await table.WriteAsync([Batch(20, 2)]);
        await table.WriteAsync([Batch(30, 2)]);

        var pinned = table.CurrentSnapshot;
        var orderedThen = OrderedPaths(pinned);
        Assert.Equal(3, orderedThen.Count);

        // Which user ids sit in which path-sorted file — the paths are GUIDs, so the sort order is arbitrary.
        var idsByOrdinal = await IdsByOrdinalAsync(table);

        // A racer drops the first path-sorted file outright, renumbering every ordinal after it.
        await using (var racer = await OpenAsync())
        {
            await racer.DeleteRowsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [orderedThen[0]] = new long[] { 0, 1 },
                }),
                RowDeleteMode.CopyOnWrite);
        }

        await using var reader = await OpenAsync();
        var orderedNow = OrderedPaths(reader.CurrentSnapshot);
        Assert.Equal(2, orderedNow.Count);
        Assert.Equal(orderedThen[1], orderedNow[0]); // ordinal 1 then == ordinal 0 now

        long address = TransientRowAddress.Pack(1, 0); // row 0 of the file that was second when pinned

        // Resolved against the pinned snapshot: the file it was read from.
        Assert.Equal([orderedThen[1]], RowSelection.FromRowAddresses([address], pinned).Paths);
        // Resolved against the current one: a DIFFERENT file — the silent wrong answer, made visible.
        Assert.Equal(
            [orderedThen[2]], RowSelection.FromRowAddresses([address], reader.CurrentSnapshot).Paths);

        // The DML follows the pinned resolution, so exactly the intended row goes.
        await reader.DeleteRowsAsync(
            RowSelection.FromRowAddresses([address], pinned), RowDeleteMode.CopyOnWrite);

        var remaining = await ReadIdsFreshAsync();
        Assert.DoesNotContain(idsByOrdinal[1][0], remaining); // the addressed row
        Assert.Contains(idsByOrdinal[1][1], remaining);       // its file's other row survives
        foreach (long id in idsByOrdinal[2])                  // the file the CURRENT reading would have hit
            Assert.Contains(id, remaining);
    }

    /// <summary>The user ids in each path-sorted file, in absolute-position order.</summary>
    private static async Task<Dictionary<int, List<long>>> IdsByOrdinalAsync(DeltaTable table)
    {
        var byOrdinal = new Dictionary<int, List<long>>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                int ordinal = TransientRowAddress.FileOrdinal(rid.GetValue(i)!.Value);
                if (!byOrdinal.TryGetValue(ordinal, out var ids))
                    byOrdinal[ordinal] = ids = [];
                ids.Add(id.GetValue(i)!.Value);
            }
        }
        return byOrdinal;
    }

    [Fact]
    public async Task FromRowAddresses_OrdinalOutsideTheActiveSet_ThrowsByDefault()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]); // exactly one active file

        var ex = Assert.Throws<ArgumentException>(() => RowSelection.FromRowAddresses(
            [TransientRowAddress.Pack(4, 0)], table.CurrentSnapshot));

        // Names the ordinal and the size of the set it fell outside, so the caller can attribute it.
        Assert.Contains("4", ex.Message);
        Assert.Contains("1 file", ex.Message);
    }

    [Fact]
    public async Task FromRowAddresses_SkipPolicy_DropsTheStaleAddress()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        string path = OrderedPaths(table.CurrentSnapshot)[0];

        var selection = RowSelection.FromRowAddresses(
            [TransientRowAddress.Pack(0, 1), TransientRowAddress.Pack(9, 0)],
            table.CurrentSnapshot, StaleAddressPolicy.Skip);

        // The historical silent-skip contract, now something the caller asked for by name.
        Assert.Equal([path], selection.Paths);
        Assert.Equal([1L], selection.PositionsFor(path));
    }

    /// <summary>The lower-layer primitive keeps its leniency: it is the surface for a host driving its own
    /// retry loop, and its documented contract is the skip.</summary>
    [Fact]
    public async Task ComputeDeletionVectorActionsAsync_StillSkipsAnOrdinalOutsideTheActiveSet()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 3)]);

        var (actions, deleted) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [9] = new long[] { 0 } });

        Assert.Empty(actions);
        Assert.Equal(0, deleted);
    }

    // ── a path the snapshot no longer holds ──

    [Fact]
    public async Task DeleteRowsAsync_PathNoLongerActive_ThrowsNamingIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.DeleteRowsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    ["gone.parquet"] = new long[] { 0 },
                }),
                RowDeleteMode.CopyOnWrite));

        Assert.Contains("gone.parquet", ex.Message);
    }

    [Fact]
    public async Task ReadRowsAsync_PathNoLongerActive_ThrowsNamingIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in table.ReadRowsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    ["gone.parquet"] = new long[] { 0 },
                })))
            {
            }
        });

        Assert.Contains("gone.parquet", ex.Message);
    }

    // ── mode / argument validation ──

    [Fact]
    public async Task DeleteRowsAsync_RowLevelRetryWithCopyOnWrite_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        string path = OrderedPaths(table.CurrentSnapshot)[0];

        var selection = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [path] = new long[] { 0 },
        });
        await Assert.ThrowsAsync<ArgumentException>(() => table
            .DeleteRowsAsync(selection, RowDeleteMode.CopyOnWrite, rowLevelRetry: true).AsTask());
    }
}
