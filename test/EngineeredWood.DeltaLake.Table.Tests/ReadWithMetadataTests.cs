// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The read-side LOCATOR surface — <see cref="DeltaTable.ReadAllWithMetadataAsync"/> appends
/// <c>_metadata.file_path</c> and <c>_metadata.row_index</c>, saying where each row physically SITS in this
/// snapshot. It is the UNPACKED, spec-named form of <see cref="DeltaTable.ReadAllWithRowIdsAsync"/>'s
/// <c>_ew_row_address</c>: the same physical address, spelled as the file that holds the row rather than as
/// that file's ordinal in a path-sorted set.
/// </summary>
/// <remarks>
/// These tests pin the properties that make it usable as a DML key rather than the layout of any file: the
/// path names a file that is actually ACTIVE, the index is ABSOLUTE (it counts rows a deletion vector masks,
/// which is what makes repeated DV deletes compose), the pair is unique table-wide, it survives a projection
/// and a pushed filter, and it works on a table with NO row tracking — a row always has a location even when
/// it has no durable identity.
/// </remarks>
public class ReadWithMetadataTests : IDisposable
{
    private readonly string _tempDir;

    public ReadWithMetadataTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_readmeta_{Guid.NewGuid():N}");
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

    private static RecordBatch Batch(long first, int count)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            b.Append(first + i);
        return new RecordBatch(IdSchema, new IArrowArray[] { b.Build() }, count);
    }

    private ValueTask<DeltaTable> OpenAsync() =>
        DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

    private readonly record struct Located(long Id, string FilePath, long RowIndex);

    private static async Task<List<Located>> ReadLocatedAsync(
        IAsyncEnumerable<RecordBatch> stream)
    {
        var rows = new List<Located>();
        await foreach (var batch in stream)
        {
            var ids = (Int64Array)batch.Column("id");
            var path = (StringArray)batch.Column(DeltaTable.MetadataFilePathColumn);
            var idx = (Int64Array)batch.Column(DeltaTable.MetadataRowIndexColumn);
            for (int i = 0; i < batch.Length; i++)
                rows.Add(new Located(ids.GetValue(i)!.Value, path.GetString(i), idx.GetValue(i)!.Value));
        }
        return rows;
    }

    /// <summary>Two flat dot-named columns, both non-null — a row always HAS a location — and the identity
    /// pair is not emitted here, because ReadAllWithRowTrackingAsync owns those.</summary>
    [Fact]
    public async Task Locator_IsTwoFlatNonNullColumns_AndDoesNotCarryIdentity()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        await foreach (var batch in table.ReadAllWithMetadataAsync())
        {
            var path = batch.Schema.GetFieldByName(DeltaTable.MetadataFilePathColumn);
            var idx = batch.Schema.GetFieldByName(DeltaTable.MetadataRowIndexColumn);
            Assert.NotNull(path);
            Assert.NotNull(idx);
            Assert.IsType<StringType>(path!.DataType);
            Assert.IsType<Int64Type>(idx!.DataType);
            Assert.False(path.IsNullable);
            Assert.False(idx.IsNullable);
            Assert.Null(batch.Schema.GetFieldByName(RowTrackingConfig.RowIdColumnName));
            Assert.Null(batch.Schema.GetFieldByName(RowTrackingConfig.RowCommitVersionColumnName));
            return;
        }
        Assert.Fail("no batches");
    }

    /// <summary>Every path names a file that is ACTIVE in the snapshot, and each row is attributed to the file
    /// that actually HOLDS it — asserted against the file's own contents, not against a second read.</summary>
    [Fact]
    public async Task Locator_AttributesEachRowToTheFileThatHoldsIt()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);    // ids 1..3
        await table.WriteAsync([Batch(11, 3)]);   // ids 11..13
        await table.WriteAsync([Batch(21, 3)]);   // ids 21..23

        var rows = await ReadLocatedAsync(table.ReadAllWithMetadataAsync());
        Assert.Equal(9, rows.Count);

        var active = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).ToHashSet(StringComparer.Ordinal);
        Assert.All(rows, r => Assert.Contains(r.FilePath, active));

        // Three files, and each write's ids land together in exactly one of them.
        Assert.Equal(3, rows.Select(r => r.FilePath).Distinct().Count());
        foreach (var group in new[] { new long[] { 1, 2, 3 }, [11, 12, 13], [21, 22, 23] })
        {
            var paths = rows.Where(r => group.Contains(r.Id)).Select(r => r.FilePath).Distinct().ToList();
            Assert.Single(paths);
        }

        // A (file, index) pair identifies exactly one row table-wide — the property a DML key needs.
        Assert.Equal(rows.Count, rows.Select(r => (r.FilePath, r.RowIndex)).Distinct().Count());
    }

    /// <summary>THE SUBTLE ONE: row_index is the ABSOLUTE physical position, COUNTING rows a deletion vector
    /// masks — Spark's <c>_metadata.row_index</c> semantics. A sequential index would renumber survivors after
    /// each delete, so a second DV delete keyed by it would hit the wrong rows.</summary>
    [Fact]
    public async Task RowIndex_IsAbsolute_CountingRowsTheDeletionVectorMasks()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 6)]);   // ids 1..6 at absolute positions 0..5

        var before = await ReadLocatedAsync(table.ReadAllWithMetadataAsync());
        Assert.Equal(Enumerable.Range(0, 6).Select(i => (long)i), before.Select(r => r.RowIndex));

        // DV-delete ids 3 and 4 — absolute positions 2 and 3.
        var positions = before.Where(r => r.Id is 3 or 4).Select(r => r.RowIndex).ToList();
        Assert.Equal(new long[] { 2, 3 }, positions);
        var pinned = table.CurrentSnapshot;
        var (dvActions, deleted) = await table.ComputeDeletionVectorActionsAsync(
            new Dictionary<int, IReadOnlyCollection<long>> { [0] = positions },
            resolveAgainst: pinned);
        Assert.Equal(2, deleted);
        await table.CommitDataFilesAsync([], DeltaWriteMode.Append,
            extraActions: dvActions, expectedVersion: pinned.Version, operation: "DELETE");

        await using var check = await OpenAsync();
        var after = await ReadLocatedAsync(check.ReadAllWithMetadataAsync());
        Assert.Equal(new long[] { 1, 2, 5, 6 }, after.Select(r => r.Id).ToArray());
        // The survivors keep their ORIGINAL positions: 0,1 then 4,5 — the deleted 2,3 are absent, NOT
        // renumbered away. This is what lets a further DV delete address rows by the same index.
        Assert.Equal(new long[] { 0, 1, 4, 5 }, after.Select(r => r.RowIndex).ToArray());
    }

    /// <summary>A row always has a location, so the locator works on a table that tracks no identity at all —
    /// where the row-tracking read surface deliberately refuses.</summary>
    [Fact]
    public async Task Locator_WorksWithoutRowTracking_WhereTheIdentitySurfaceRefuses()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 2)]);
        await table.WriteAsync([Batch(11, 2)]);

        var rows = await ReadLocatedAsync(table.ReadAllWithMetadataAsync());
        Assert.Equal(4, rows.Count);
        Assert.All(rows, r => Assert.False(string.IsNullOrEmpty(r.FilePath)));
        Assert.Equal(2, rows.Select(r => r.FilePath).Distinct().Count());

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await foreach (var _ in table.ReadAllWithRowTrackingAsync(columns: null, filter: null))
            {
            }
        });
    }

    /// <summary>The locator columns are appended whatever the caller projected, so a projected read is still
    /// addressable — and the projection is honoured (the data column asked for is the only one present).</summary>
    [Fact]
    public async Task Locator_SurvivesAProjection()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        await foreach (var batch in table.ReadAllWithMetadataAsync(new[] { "id" }, null))
        {
            Assert.Equal(3, batch.ColumnCount); // id + the two locator columns
            Assert.NotNull(batch.Schema.GetFieldByName("id"));
            Assert.NotNull(batch.Schema.GetFieldByName(DeltaTable.MetadataFilePathColumn));
            Assert.NotNull(batch.Schema.GetFieldByName(DeltaTable.MetadataRowIndexColumn));
            return;
        }
        Assert.Fail("no batches");
    }

    /// <summary>A pushed filter goes through the same planner as any other read: pruning may keep a whole file
    /// (superset-safe) but must not lose a matching row, and every surviving path is still active.</summary>
    [Fact]
    public async Task Locator_HonoursAPushedFilter()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        await table.WriteAsync([Batch(11, 3)]);
        await table.WriteAsync([Batch(21, 3)]);

        var all = await ReadLocatedAsync(table.ReadAllWithMetadataAsync());
        var pred = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", 21L);
        var filtered = await ReadLocatedAsync(table.ReadAllWithMetadataAsync(null, pred));

        Assert.All(new long[] { 21, 22, 23 }, id => Assert.Contains(id, filtered.Select(r => r.Id)));
        Assert.True(filtered.Count <= all.Count);
        var active = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).ToHashSet(StringComparer.Ordinal);
        Assert.All(filtered, r => Assert.Contains(r.FilePath, active));
    }

    /// <summary>The locator agrees with the transient address for the same rows: the path is the file the
    /// ordinal names, and the index is the packed position. Same address, two spellings.</summary>
    [Fact]
    public async Task Locator_AgreesWithTheTransientRowAddress()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Batch(1, 3)]);
        await table.WriteAsync([Batch(11, 3)]);

        var located = (await ReadLocatedAsync(table.ReadAllWithMetadataAsync()))
            .ToDictionary(r => r.Id);

        var byOrdinal = new Dictionary<long, string>();
        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
        for (int i = 0; i < ordered.Count; i++)
            byOrdinal[i] = ordered[i];

        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var addr = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                long id = ids.GetValue(i)!.Value;
                long a = addr.GetValue(i)!.Value;
                Assert.Equal(byOrdinal[TransientRowAddress.FileOrdinal(a)], located[id].FilePath);
                Assert.Equal(TransientRowAddress.Position(a), located[id].RowIndex);
            }
        }
    }
}
