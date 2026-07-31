// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The read-side TRANSIENT row-id surface — <see cref="DeltaTable.ReadAllWithRowIdsAsync"/> /
/// <see cref="DeltaTable.ReadAtVersionWithRowIdsAsync"/> append a trailing <c>_ew_row_address</c> =
/// <c>(fileOrdinal &lt;&lt; 40) | absolutePosition</c>, and <see cref="DeltaTable.OrderedActiveBaseRowIdsAsync"/>
/// gives the per-ordinal <c>baseRowId</c>. NOT a stable Delta id — it round-trips WITHIN a snapshot to the
/// row-id DML surface (a host reads rows, keeps the ids, then deletes/updates exactly those rows). This is the
/// maximally reader-compatible copy-on-write path: no deletion vectors or row tracking required on the table.
/// </summary>
public class ReadWithRowIdsTests : IDisposable
{
    private readonly string _tempDir;

    public ReadWithRowIdsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_readrowid_{Guid.NewGuid():N}");
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
        return new RecordBatch(IdSchema, [ids.Build(), ], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>Reads every (id, transientRowId) pair the with-rowids read surfaces.</summary>
    private static async Task<List<(long Id, long RowId)>> ReadWithIds(
        IAsyncEnumerable<RecordBatch> batches)
    {
        var rows = new List<(long, long)>();
        await foreach (var batch in batches)
        {
            var id = (Int64Array)batch.Column("id");
            var rid = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
                rows.Add((id.GetValue(i)!.Value, rid.GetValue(i)!.Value));
        }
        return rows;
    }

    [Fact]
    public async Task ReadAllWithRowIds_SingleFile_EncodesOrdinalZeroAndPosition()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(10, 4)]); // ids 10..13, one file → ordinal 0, positions 0..3

        var rows = await ReadWithIds(table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }));
        rows.Sort();
        Assert.Equal(new long[] { 10, 11, 12, 13 }, rows.Select(r => r.Id).ToArray());
        // ordinal 0 → transient ids are exactly the in-file positions 0..3
        Assert.Equal(new long[] { 0, 1, 2, 3 }, rows.Select(r => r.RowId).ToArray());
    }

    [Fact]
    public async Task ReadAllWithRowIds_MultiFile_EncodesPerFileOrdinal()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 2)]); // file A
        await table.WriteAsync([Batch(100, 3)]); // file B — a second active file

        var rows = await ReadWithIds(table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }));

        // Each transient id decodes to (path-sorted ordinal, in-file position). Two files → ordinals 0 and 1;
        // every id maps back to exactly one file, and positions restart per file.
        var byOrdinal = rows
            .GroupBy(r => TransientRowAddress.FileOrdinal(r.RowId))
            .OrderBy(g => g.Key).ToList();
        Assert.Equal(2, byOrdinal.Count);
        foreach (var g in byOrdinal)
        {
            var positions = g.Select(r => TransientRowAddress.Position(r.RowId)).OrderBy(p => p).ToArray();
            Assert.Equal(Enumerable.Range(0, g.Count()).Select(i => (long)i).ToArray(), positions);
        }
    }

    [Fact]
    public async Task ReadAllWithRowIds_RoundTripsToReadRows()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        var rows = await ReadWithIds(table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }));
        // pick the transient ids of ids 2 and 5 and read them straight back
        var picked = rows.Where(r => r.Id is 2 or 5).Select(r => r.RowId).ToList();
        Assert.Equal(2, picked.Count);

        var readBack = new List<long>();
        var selection = RowSelection.FromRowAddresses(picked, table.CurrentSnapshot);
        await foreach (var batch in table.ReadRowsAsync(selection))
        {
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                readBack.Add(id.GetValue(i)!.Value);
        }
        readBack.Sort();
        Assert.Equal(new long[] { 2, 5 }, readBack);
    }

    [Fact]
    public async Task ReadAllWithRowIds_RoundTripsToDeletionVectorDml()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]); // ids 1..6

        // a host reads the rows, keeps the transient ids, and deletes ids 3 and 4 by those ids
        var rows = await ReadWithIds(table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }));
        var toDelete = rows.Where(r => r.Id is 3 or 4).Select(r => r.RowId).ToList();

        // decode the transient ids into positionsByOrdinal and drive the DV DELETE
        var positionsByOrdinal = toDelete
            .GroupBy(TransientRowAddress.FileOrdinal)
            .ToDictionary(
                g => g.Key,
                g => (IReadOnlyCollection<long>)g.Select(TransientRowAddress.Position).ToList());

        var pinned = table.CurrentSnapshot;
        var (dvActions, deleted) = await table.ComputeDeletionVectorActionsAsync(
            positionsByOrdinal, resolveAgainst: pinned);
        Assert.Equal(2, deleted);
        await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: dvActions, expectedVersion: pinned.Version, operation: "DELETE");

        var remaining = new List<long>();
        await using (var check = await OpenAsync())
        {
            await foreach (var batch in check.ReadAllAsync())
            {
                var id = (Int64Array)batch.Column("id");
                for (int i = 0; i < batch.Length; i++)
                    remaining.Add(id.GetValue(i)!.Value);
            }
        }
        remaining.Sort();
        Assert.Equal(new long[] { 1, 2, 5, 6 }, remaining); // ids 3, 4 gone
    }

    [Fact]
    public async Task OrderedActiveBaseRowIds_RowTrackingTable_ReturnsBaseRowIdPerOrdinal()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]); // file A → baseRowId 0
        await table.WriteAsync([Batch(100, 2)]); // file B → baseRowId 3

        var baseIds = await table.OrderedActiveBaseRowIdsAsync();
        Assert.Equal(2, baseIds.Count);
        // path-sorted ordinal order; the HWM advanced 0 → 3 → 5
        Assert.Equal(new long?[] { 0, 3 }, baseIds.OrderBy(x => x!.Value).ToArray());
    }

    [Fact]
    public async Task ReadAtVersionWithRowIds_PastVersion_ReadsThatVersionsFiles()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]); // v1: ids 1..3
        long v1 = table.CurrentSnapshot.Version;
        await table.WriteAsync([Batch(100, 2)]); // v2 adds more

        var rows = await ReadWithIds(table.ReadAsync(new DeltaReadOptions { AtVersion = v1, Metadata = DeltaRowMetadata.RowAddress }));
        rows.Sort();
        Assert.Equal(new long[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(new long[] { 0, 1, 2 }, rows.Select(r => r.RowId).ToArray()); // one file at v1
    }

    // ── the address is not the identity ──

    /// <summary>
    /// The emitted column must NOT be Spark's <c>_metadata.row_id</c>. The two were once the same string, and
    /// a host reading that name expecting the STABLE row-tracking id instead got a snapshot-scoped address —
    /// a different number, silently. This is the guard on that separation, and on the name
    /// <c>_metadata.row_id</c> staying free for the stable id it belongs to.
    /// </summary>
    [Fact]
    public async Task EmittedColumn_IsTheAddress_NotSparksStableRowIdName()
    {
        Assert.NotEqual(RowTrackingConfig.RowIdColumnName, TransientRowAddress.ColumnName);

        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]);

        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            Assert.True(batch.Schema.GetFieldIndex(TransientRowAddress.ColumnName) >= 0);
            Assert.True(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName) < 0);
        }
    }

    [Fact]
    public void PackAndUnpack_RoundTrip()
    {
        Assert.Equal(0, TransientRowAddress.FileOrdinal(TransientRowAddress.Pack(0, 0)));
        Assert.Equal(0, TransientRowAddress.Position(TransientRowAddress.Pack(0, 0)));

        foreach (var (ordinal, position) in new[]
        {
            (0, 0L), (0, 1L), (1, 0L), (7, 12345L),
            (TransientRowAddress.MaxFileOrdinal, 0L),
            (0, TransientRowAddress.MaxPosition),
            (123, TransientRowAddress.MaxPosition),
        })
        {
            long address = TransientRowAddress.Pack(ordinal, position);
            Assert.True(address >= 0); // the packing must never spill into the sign bit
            Assert.Equal(ordinal, TransientRowAddress.FileOrdinal(address));
            Assert.Equal(position, TransientRowAddress.Position(address));
        }
    }

    /// <summary>The helpers must agree with what the read path actually emits — they are what a host decodes
    /// an address with, so a drift between them would be silent.</summary>
    [Fact]
    public async Task Helpers_DecodeWhatTheReadPathEmits()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(100, 3)]);

        var rows = await ReadWithIds(table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }));
        var byOrdinal = rows.GroupBy(r => TransientRowAddress.FileOrdinal(r.RowId))
            .OrderBy(g => g.Key).ToList();

        Assert.Equal(new[] { 0, 1 }, byOrdinal.Select(g => g.Key).ToArray());
        foreach (var group in byOrdinal)
        {
            var positions = group.Select(r => TransientRowAddress.Position(r.RowId)).OrderBy(p => p).ToArray();
            Assert.Equal(Enumerable.Range(0, positions.Length).Select(i => (long)i).ToArray(), positions);
            foreach (var row in group)
                Assert.Equal(row.RowId, TransientRowAddress.Pack(group.Key, TransientRowAddress.Position(row.RowId)));
        }
    }
}
