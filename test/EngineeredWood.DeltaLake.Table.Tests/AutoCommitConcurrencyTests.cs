// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Optimistic concurrency for the AUTO-committing write methods — <see cref="DeltaTable.DeleteAsync"/>
/// and the blind-append <see cref="DeltaTable.WriteAsync(IReadOnlyList{RecordBatch}, DeltaWriteMode, CancellationToken, IReadOnlyList{string})"/>
/// — with no explicit transaction in sight. Concurrency is real, not simulated: two independent
/// <see cref="DeltaTable"/> handles are opened on the same directory, and one commits while the other
/// still holds an older snapshot. A single-shot write should rebase-and-retry over a concurrent commit
/// that did not invalidate what it read, and abort only on a genuine conflict — instead of failing on
/// every version collision the way the pre-OCC auto-committer did.
/// </summary>
public class AutoCommitConcurrencyTests : IDisposable
{
    private readonly string _tempDir;

    public AutoCommitConcurrencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_autocommit_{Guid.NewGuid():N}");
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

    private static RecordBatch Batch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private static async Task<List<long>> ReadIds(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }

        ids.Sort();
        return ids;
    }

    /// <summary>A fresh handle so a later read reflects everything both writers committed.</summary>
    private async Task<List<long>> ReadIdsFresh()
    {
        await using var reader = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        return await ReadIds(reader);
    }

    /// <summary>
    /// Two independent handles delete rows in DIFFERENT files. The second committer holds a stale
    /// snapshot, so its commit collides — but nothing it read was removed by the first, so it rebases
    /// onto the winner's version and lands. Both deletes take effect; the auto-committer did NOT throw.
    /// </summary>
    [Fact]
    public async Task TwoHandles_DisjointDeletes_SecondRebasesAndLands()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(5)]); // one file
            await setup.WriteAsync([Batch(7)]); // a second file
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;
        Assert.Equal(baseVersion, tableB.CurrentSnapshot.Version);

        // A commits first (baseVersion + 1). B still holds baseVersion.
        var (rowsA, vA) = await tableA.DeleteAsync(IdEquals(5));
        var (rowsB, vB) = await tableB.DeleteAsync(IdEquals(7));

        Assert.Equal(1, rowsA);
        Assert.Equal(1, rowsB);
        Assert.Equal(baseVersion + 1, vA);
        Assert.Equal(baseVersion + 2, vB); // collided, rebased, landed one version later

        Assert.Empty(await ReadIdsFresh());
    }

    /// <summary>
    /// Two independent handles delete the SAME row of the same file. Row-level concurrency lets a stale
    /// delete rebase its deletion vector onto a concurrent one when the rows are disjoint, but here they
    /// overlap — the row B removes was already removed by A — so the auto-committer aborts with a
    /// <see cref="DeltaConflictException"/> rather than double-delete. (Disjoint rows of the same file both
    /// landing is covered by <see cref="RowLevelConcurrencyTests"/>.)
    /// </summary>
    [Fact]
    public async Task TwoHandles_SameRowDeletes_SecondAborts()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(5, 7)]); // both rows in ONE file
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;

        var (_, vA) = await tableA.DeleteAsync(IdEquals(5));
        Assert.Equal(baseVersion + 1, vA);

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tableB.DeleteAsync(IdEquals(5))); // the same row A just removed
        Assert.Contains("row level", ex.Message, StringComparison.OrdinalIgnoreCase);

        // Only A's delete landed; the aborted delete left the table uncorrupted.
        Assert.Equal([7L], await ReadIdsFresh());
    }

    /// <summary>
    /// Two independent handles each blind-append. A blind append has no read dependency, so the stale
    /// second committer rebases over the first's commit and lands — both rows are present. Pre-OCC this
    /// second append would have thrown on the version collision.
    /// </summary>
    [Fact]
    public async Task TwoHandles_ConcurrentAppends_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1)]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;

        long vA = await tableA.WriteAsync([Batch(2)]);
        long vB = await tableB.WriteAsync([Batch(3)]);

        Assert.Equal(baseVersion + 1, vA);
        Assert.Equal(baseVersion + 2, vB); // collided, rebased, landed

        Assert.Equal([1L, 2L, 3L], await ReadIdsFresh());
    }

    /// <summary>
    /// A blind append rebases over a concurrent DELETE. An append does not depend on which rows exist,
    /// so a concurrent delete is not a conflict for it — the stale appender rebases and lands, and both
    /// the delete and the append are reflected.
    /// </summary>
    [Fact]
    public async Task Append_RebasesPastConcurrentDelete()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1)]); // one file
            await setup.WriteAsync([Batch(2)]); // another file
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        long baseVersion = tableA.CurrentSnapshot.Version;

        var (_, vDel) = await tableA.DeleteAsync(IdEquals(1));
        long vAppend = await tableB.WriteAsync([Batch(3)]); // stale handle, blind append

        Assert.Equal(baseVersion + 1, vDel);
        Assert.Equal(baseVersion + 2, vAppend);

        Assert.Equal([2L, 3L], await ReadIdsFresh());
    }

    /// <summary>
    /// A concurrent metadata change (ADD COLUMN) DOES abort a stale blind append: the append was
    /// prepared against a schema the table no longer has, so rebasing it verbatim could commit
    /// schema-inconsistent data. The checker's unconditional metadata-change rule fires and the append
    /// aborts with a <see cref="DeltaConflictException"/>.
    /// </summary>
    [Fact]
    public async Task Append_AbortsOnConcurrentSchemaChange()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1)]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        await tableA.AddColumnAsync(new Field("name", StringType.Default, nullable: true));

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tableB.WriteAsync([Batch(2)]));
    }

    /// <summary>
    /// A full OVERWRITE is NOT rebase-safe (its remove-set is a read of the active files), so a stale
    /// overwrite racing a concurrent append still fails on the collision — the pre-OCC behavior, kept
    /// deliberately. A concurrent append is exactly the case a rebased overwrite would silently drop.
    /// </summary>
    [Fact]
    public async Task FullOverwrite_ThrowsOnConcurrentAppend()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true))
        {
            await setup.WriteAsync([Batch(1)]);
        }

        await using var tableA = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        await using var tableB = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        await tableA.WriteAsync([Batch(2)]); // concurrent append lands first

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await tableB.WriteAsync([Batch(9)], DeltaWriteMode.Overwrite));
    }
}
