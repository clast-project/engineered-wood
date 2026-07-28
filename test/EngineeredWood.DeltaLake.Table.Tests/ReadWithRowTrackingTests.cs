// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The read-side STABLE row identity — <see cref="DeltaTable.ReadAllWithRowTrackingAsync"/> /
/// <see cref="DeltaTable.ReadAtVersionWithRowTrackingAsync"/> append Delta's two generated row-tracking
/// columns (<c>_metadata.row_id</c>, <c>_metadata.row_commit_version</c>), resolved per row as the file's
/// materialized value where it has one, else <c>add.baseRowId + position</c> /
/// <c>add.defaultRowCommitVersion</c>.
///
/// <para>Distinct from <see cref="ReadWithRowIdsTests"/>, which covers the TRANSIENT address: that says where
/// a row currently sits and is void across snapshots; this says which row it IS and survives every rewrite.
/// The two are asserted against each other below so the distinction cannot quietly erode.</para>
/// </summary>
public class ReadWithRowTrackingTests : IDisposable
{
    private readonly string _tempDir;

    public ReadWithRowTrackingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rtread_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema Schema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static Apache.Arrow.Schema PartitionedSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("part", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }
        return new RecordBatch(Schema, [ids.Build(), values.Build()], rows.Length);
    }

    private static RecordBatch PartitionedRows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        var parts = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
            parts.Append("p1");
        }
        return new RecordBatch(PartitionedSchema, [ids.Build(), values.Build(), parts.Build()], rows.Length);
    }

    private Task<DeltaTable> CreateAsync(bool enableDeletionVectors = false) =>
        DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema,
            enableRowTracking: true, enableDeletionVectors: enableDeletionVectors).AsTask();

    /// <summary>Every (value, rowId, commitVersion) the row-tracking read reports, keyed by 'value'.</summary>
    private static async Task<Dictionary<string, (long? Id, long? Version)>> TrackingByValueAsync(
        IAsyncEnumerable<RecordBatch> batches)
    {
        var result = new Dictionary<string, (long?, long?)>(StringComparer.Ordinal);
        await foreach (var batch in batches)
        {
            var values = (StringArray)batch.Column("value");
            var ids = (Int64Array)batch.Column(RowTrackingConfig.RowIdColumnName);
            var vers = (Int64Array)batch.Column(RowTrackingConfig.RowCommitVersionColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                result[values.GetString(i)!] = (
                    ids.IsNull(i) ? null : ids.GetValue(i),
                    vers.IsNull(i) ? null : vers.GetValue(i));
            }
        }
        return result;
    }

    private static BooleanArray Eq(RecordBatch batch, string target)
    {
        var col = (StringArray)batch.Column("value");
        var b = new BooleanArray.Builder();
        for (int i = 0; i < col.Length; i++)
            b.Append(col.GetString(i) == target);
        return b.Build();
    }

    private static RecordBatch SetValue(RecordBatch batch, string newValue)
    {
        var values = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            values.Append(newValue);
        return new RecordBatch(Schema, [batch.Column("id"), values.Build()], batch.Length);
    }

    // ── the resolution rules ──

    /// <summary>A fresh append carries no materialized column, so every id is add.baseRowId + position and
    /// every commit version is the append's own version.</summary>
    [Fact]
    public async Task Append_ReportsBaseRowIdPlusPosition()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]); // v1, file 0: ids 0,1
        await table.WriteAsync([Rows((30, "c"))]);            // v2, file 1: id 2

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));

        Assert.Equal((0L, 1L), tracking["a"]);
        Assert.Equal((1L, 1L), tracking["b"]);
        Assert.Equal((2L, 2L), tracking["c"]); // second append -> version 2
    }

    /// <summary>
    /// The claim the whole feature exists for: a copy-on-write UPDATE moves every row to a new file and
    /// REORDERS them, and the read still reports each row's ORIGINAL id — the materialized value overriding
    /// the rewritten file's own baseRowId. The changed row's commit version advances; the untouched ones keep
    /// theirs, which is what distinguishes "this row moved" from "this row changed".
    /// </summary>
    [Fact]
    public async Task Update_ReportsPreservedIdsAndAdvancesOnlyTheChangedRowsVersion()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]); // v1: ids 0,1,2

        await table.UpdateAsync(b => Eq(b, "b"), b => SetValue(b, "B")); // v2

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));

        Assert.Equal((0L, 1L), tracking["a"]);
        Assert.Equal((1L, 2L), tracking["B"]); // same id, version advanced to the UPDATE's
        Assert.Equal((2L, 1L), tracking["c"]);
    }

    /// <summary>Compaction mixes rows from several files into one, so a single baseRowId cannot represent
    /// them — the ids must come from the materialized column, and must be the originals.</summary>
    [Fact]
    public async Task Compaction_ReportsPreservedIds()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]);
        await table.WriteAsync([Rows((30, "c"))]);

        Assert.NotNull(await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));
        Assert.Equal(0L, tracking["a"].Id);
        Assert.Equal(1L, tracking["b"].Id);
        Assert.Equal(2L, tracking["c"].Id);
    }

    /// <summary>A deletion vector removes rows without moving the survivors, so their ids stay
    /// baseRowId + PHYSICAL position — the DV does not renumber, and a reader that counted surviving rows
    /// instead would report shifted ids for everything after the hole.</summary>
    [Fact]
    public async Task DeletionVectorDelete_SurvivorsKeepPhysicalPositionIds()
    {
        await using var table = await CreateAsync(enableDeletionVectors: true);
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        await table.DeleteAsync(b => Eq(b, "b"));

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));
        Assert.Equal(["a", "c"], tracking.Keys.OrderBy(k => k).ToArray());
        Assert.Equal(0L, tracking["a"].Id);
        Assert.Equal(2L, tracking["c"].Id); // NOT 1 — the DV hole is not closed up
    }

    /// <summary>A partitioned table reads its files through a different column list; the ids must be the
    /// same. Ties this surface to the read-path fix in <see cref="RowTrackingPartitionedIdLossTests"/> —
    /// without it, the second rewrite below would report fresh ids here.</summary>
    [Fact]
    public async Task Partitioned_AfterTwoRewrites_ReportsPreservedIds()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), PartitionedSchema,
            partitionColumns: ["part"], enableRowTracking: true);
        await table.WriteAsync([PartitionedRows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        await table.UpdateAsync(b => Eq(b, "b"), b => SetPartitionedValue(b, "B"));
        await table.UpdateAsync(b => Eq(b, "a"), b => SetPartitionedValue(b, "A"));

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));
        Assert.Equal(0L, tracking["A"].Id);
        Assert.Equal(1L, tracking["B"].Id);
        Assert.Equal(2L, tracking["c"].Id);
    }

    private static RecordBatch SetPartitionedValue(RecordBatch batch, string newValue)
    {
        var values = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            values.Append(newValue);
        return new RecordBatch(PartitionedSchema,
            [batch.Column("id"), values.Build(), batch.Column("part")], batch.Length);
    }

    // ── shape of the emitted batch ──

    /// <summary>The tracking columns are APPENDED to whatever was projected — a projection does not have to
    /// name them, and naming a column does not cost the identity.</summary>
    [Fact]
    public async Task ProjectedRead_AppendsTrackingColumnsToTheProjection()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]);
        await table.UpdateAsync(b => Eq(b, "a"), b => SetValue(b, "A")); // materialize, so ids are read back

        await foreach (var batch in table.ReadAllWithRowTrackingAsync(["value"], null))
        {
            Assert.Equal(
                ["value", RowTrackingConfig.RowIdColumnName, RowTrackingConfig.RowCommitVersionColumnName],
                batch.Schema.FieldsList.Select(f => f.Name).ToArray());
        }

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(["value"], null));
        Assert.Equal(0L, tracking["A"].Id);
        Assert.Equal(1L, tracking["b"].Id);
    }

    /// <summary>Both columns are nullable Int64. A non-nullable field would be a promise this read cannot
    /// keep for a file that predates row tracking on the table.</summary>
    [Fact]
    public async Task EmittedColumns_AreNullableInt64()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"))]);

        await foreach (var batch in table.ReadAllWithRowTrackingAsync(null, null))
        {
            foreach (string name in new[]
            {
                RowTrackingConfig.RowIdColumnName, RowTrackingConfig.RowCommitVersionColumnName,
            })
            {
                var field = batch.Schema.GetFieldByName(name);
                Assert.Equal(Int64Type.Default, field.DataType);
                Assert.True(field.IsNullable);
            }
        }
    }

    /// <summary>
    /// The stable id and the transient ADDRESS are different numbers with different lifetimes, and this is
    /// the assertion that keeps them from converging. After a rewrite the address of a row is its new
    /// position; its row id is what it always was.
    /// </summary>
    [Fact]
    public async Task StableId_IsNotTheTransientAddress()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        // The UPDATE reorders: the matched row moves to physical position 0, so its ADDRESS becomes 0 while
        // its stable id stays 1.
        await table.UpdateAsync(b => Eq(b, "b"), b => SetValue(b, "B"));

        var tracking = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));
        Assert.Equal(1L, tracking["B"].Id);

        long addressOfB = -1;
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var values = (StringArray)batch.Column("value");
            var addr = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
                if (values.GetString(i) == "B") addressOfB = addr.GetValue(i)!.Value;
        }
        Assert.Equal(0L, addressOfB);
        Assert.NotEqual(tracking["B"].Id, addressOfB);

        // ...and neither read emits the other's column.
        await foreach (var batch in table.ReadAllWithRowTrackingAsync(null, null))
            Assert.True(batch.Schema.GetFieldIndex(TransientRowAddress.ColumnName) < 0);
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
            Assert.True(batch.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName) < 0);
    }

    // ── time travel ──

    /// <summary>
    /// A row read at two versions reports the SAME id — the property that makes the column usable for
    /// following one row through history, and the one a snapshot-scoped address cannot offer.
    /// </summary>
    [Fact]
    public async Task ReadAtVersion_ReportsTheSameIdAsTheCurrentSnapshot()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]); // v1: ids 0,1
        long v1 = table.CurrentSnapshot.Version;

        await table.UpdateAsync(b => Eq(b, "b"), b => SetValue(b, "B")); // v2 rewrites the file

        var atV1 = await TrackingByValueAsync(table.ReadAtVersionWithRowTrackingAsync(v1, null, null));
        var now = await TrackingByValueAsync(table.ReadAllWithRowTrackingAsync(null, null));

        Assert.Equal((1L, 1L), atV1["b"]);  // before the update: id 1, version 1
        Assert.Equal((1L, 2L), now["B"]);   // after: same identity, new commit version
        Assert.Equal(atV1["a"], now["a"]);  // the untouched row is unchanged in both
    }

    // ── refusals ──

    /// <summary>
    /// A table without row tracking is refused rather than served all-null columns: null means "this row has
    /// no derivable id", and answering that for every row of a table that simply does not track identity
    /// would be a different claim wearing the same shape.
    /// </summary>
    [Fact]
    public async Task NonRowTrackingTable_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema);
        await table.WriteAsync([Rows((10, "a"))]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in table.ReadAllWithRowTrackingAsync(null, null)) { }
        });
        Assert.Contains("delta.enableRowTracking", ex.Message);
    }

    /// <summary>A user column named like a generated one would be shadowed by it; refuse instead of picking
    /// a winner silently.</summary>
    [Fact]
    public async Task UserColumnCollidingWithAGeneratedName_Throws()
    {
        var collidingSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field(RowTrackingConfig.RowIdColumnName, Int64Type.Default, true))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), collidingSchema, enableRowTracking: true);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in table.ReadAllWithRowTrackingAsync(null, null)) { }
        });
        Assert.Contains(RowTrackingConfig.RowIdColumnName, ex.Message);
    }
}
