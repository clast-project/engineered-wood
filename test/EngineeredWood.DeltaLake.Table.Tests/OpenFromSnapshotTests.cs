// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #740: <see cref="DeltaTable.OpenAsync(EngineeredWood.IO.ITableFileSystem, Snapshot.Snapshot, DeltaTableOptions?,
/// CancellationToken)"/> opens a table from a snapshot an earlier handle produced. It lists the log once and replays
/// only what is new, and returns what a fresh open would wherever continuing is not provably right.
/// </summary>
public class OpenFromSnapshotTests : IDisposable
{
    private static readonly string Commit0 = $"_delta_log/{0L:D20}.json";

    private readonly string _tempDir = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), $"delta_open_from_snapshot_{Guid.NewGuid():N}")).FullName;

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Local => new(_tempDir);

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        foreach (long i in ids)
            id.Append(i);
        return new RecordBatch(TableSchema, [id.Build()], ids.Length);
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync())
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    private static async Task CreateAsync(EngineeredWood.IO.ITableFileSystem fs, params long[] ids)
    {
        await using var writer = await DeltaTable.CreateAsync(fs, TableSchema, Options);
        foreach (long id in ids)
            await writer.WriteAsync([Rows(id)]);
    }

    private static async Task<Snapshot.Snapshot> HeldSnapshotAsync(EngineeredWood.IO.ITableFileSystem fs)
    {
        await using var earlier = await DeltaTable.OpenAsync(fs, Options);
        return earlier.CurrentSnapshot;
    }

    [Fact]
    public async Task Unchanged_table_costs_one_listing_and_no_reads()
    {
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await CreateAsync(fs, 1, 2);
        var held = await HeldSnapshotAsync(fs);
        int listings = fs.Listings;
        int reads = fs.ReadsOf(Commit0) + fs.ReadsOf($"_delta_log/{2L:D20}.json");

        await using var table = await DeltaTable.OpenAsync(fs, held, Options);

        Assert.Equal(listings + 1, fs.Listings);
        Assert.Equal(reads, fs.ReadsOf(Commit0) + fs.ReadsOf($"_delta_log/{2L:D20}.json"));
        Assert.Same(held.ActiveFiles, table.CurrentSnapshot.ActiveFiles);
        Assert.Equal([1L, 2], await IdsAsync(table));
    }

    [Fact]
    public async Task New_commits_are_replayed_onto_the_held_snapshot()
    {
        var fs = new ListingRewriteFileSystem(Local, f => f);
        await CreateAsync(fs, 1, 2);
        var held = await HeldSnapshotAsync(fs);
        await using (var writer = await DeltaTable.OpenAsync(fs, Options))
        {
            await writer.WriteAsync([Rows(3)]);
            await writer.WriteAsync([Rows(4)]);
        }
        int listings = fs.Listings;
        int reads = fs.ReadsOf(Commit0);

        await using var table = await DeltaTable.OpenAsync(fs, held, Options);

        Assert.Equal(4, table.CurrentSnapshot.Version);
        Assert.Equal(listings + 1, fs.Listings);
        Assert.Equal(reads, fs.ReadsOf(Commit0));                           // incremental, not from the start
        Assert.Equal([1L, 2, 3, 4], await IdsAsync(table));
        Assert.Equal(2, held.Version);                                       // the held snapshot is untouched
    }

    [Fact]
    public async Task One_snapshot_starts_several_independent_handles()
    {
        await CreateAsync(Local, 1);
        var held = await HeldSnapshotAsync(Local);

        await using var a = await DeltaTable.OpenAsync(Local, held, Options);
        await using var b = await DeltaTable.OpenAsync(Local, held, Options);
        await a.WriteAsync([Rows(2)]);

        Assert.Equal(2, a.CurrentSnapshot.Version);
        Assert.Equal(1, b.CurrentSnapshot.Version);                          // b keeps its own view until refreshed
        Assert.Equal([1L], await IdsAsync(b));
    }

    [Fact]
    public async Task Recreated_table_opens_as_the_new_table()
    {
        await CreateAsync(Local, 1, 2, 3);
        var held = await HeldSnapshotAsync(Local);
        foreach (string entry in Directory.EnumerateFileSystemEntries(_tempDir))
        {
            if (Directory.Exists(entry))
                Directory.Delete(entry, recursive: true);
            else
                File.Delete(entry);
        }
        await CreateAsync(Local, 100, 101, 102);                             // same version as the held snapshot

        await using var table = await DeltaTable.OpenAsync(Local, held, Options);
        await using var fresh = await DeltaTable.OpenAsync(Local, Options);

        Assert.Equal(fresh.CurrentSnapshot.Metadata.Id, table.CurrentSnapshot.Metadata.Id);
        Assert.Equal([100L, 101, 102], await IdsAsync(table));
    }

    [Fact]
    public async Task Dropped_table_is_not_found()
    {
        await CreateAsync(Local, 1);
        var held = await HeldSnapshotAsync(Local);
        Directory.Delete(Path.Combine(_tempDir, "_delta_log"), recursive: true);

        await Assert.ThrowsAsync<DeltaTableNotFoundException>(
            async () => await DeltaTable.OpenAsync(Local, held, Options));
    }

    [Fact]
    public async Task Snapshot_of_another_table_opens_this_one()
    {
        string other = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), $"delta_other_{Guid.NewGuid():N}"))
            .FullName;
        try
        {
            await CreateAsync(new LocalTableFileSystem(other), 7, 8);
            var foreign = await HeldSnapshotAsync(new LocalTableFileSystem(other));
            await CreateAsync(Local, 1, 2);

            await using var table = await DeltaTable.OpenAsync(Local, foreign, Options);
            await using var fresh = await DeltaTable.OpenAsync(Local, Options);

            Assert.Equal(fresh.CurrentSnapshot.Metadata.Id, table.CurrentSnapshot.Metadata.Id);
            Assert.Equal([1L, 2], await IdsAsync(table));
        }
        finally
        {
            Directory.Delete(other, recursive: true);
        }
    }

    [Fact]
    public async Task Writer_opened_from_a_held_snapshot_starts_at_the_latest_version()
    {
        // A metadata change committed while the snapshot was held. Opened from the snapshot, the writer's base is the
        // latest version, which already includes it, so the append has no concurrent commit to check against.
        await CreateAsync(Local, 1, 2);
        var held = await HeldSnapshotAsync(Local);
        await using (var other = await DeltaTable.OpenAsync(Local, Options))
            await other.AddColumnAsync(new Field("note", StringType.Default, nullable: true));   // v3

        await using var writer = await DeltaTable.OpenAsync(Local, held, Options);
        Assert.Equal(3, writer.CurrentSnapshot.Version);
        Assert.Contains(writer.ArrowSchema.FieldsList, f => f.Name == "note");

        var schema = writer.ArrowSchema;
        var note = new StringArray.Builder().AppendNull().Build();
        long version = await writer.WriteAsync(
            [new RecordBatch(schema, [new Int64Array.Builder().Append(3).Build(), note], 1)]);

        Assert.Equal(4, version);
        Assert.Equal([1L, 2, 3], await IdsAsync(writer));
    }

    [Fact]
    public async Task Null_snapshot_is_refused() =>
        await Assert.ThrowsAsync<ArgumentNullException>(
            async () => await DeltaTable.OpenAsync(Local, (Snapshot.Snapshot)null!, Options));
}
