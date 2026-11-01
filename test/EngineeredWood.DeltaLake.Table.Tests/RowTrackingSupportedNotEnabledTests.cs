// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #599: a table whose protocol SUPPORTS the <c>rowTracking</c> writer feature but whose
/// <c>delta.enableRowTracking</c> is off. delta-spark 4.0.0 assigns fresh row ids whenever the feature is supported
/// (<c>RowId.assignFreshRowIds</c> gates on <c>RowTracking.isSupported</c>), and preserves them through a rewrite
/// or reads them only when it is enabled (<c>preserveRowIds</c>, <c>getMaterializedColumnName</c>). So every write
/// here gives its new files a <c>baseRowId</c> + <c>defaultRowCommitVersion</c> and advances the high-water mark,
/// while a rewrite writes no materialized id columns and a row-tracking read is still refused.
/// </summary>
public class RowTrackingSupportedNotEnabledTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingSupportedNotEnabledTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rt_supported_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, true))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Rows(params long[] ids)
    {
        var id = new Int64Array.Builder();
        var value = new StringArray.Builder();
        foreach (long i in ids)
        {
            id.Append(i);
            value.Append($"v{i}");
        }
        return new RecordBatch(Schema, [id.Build(), value.Build()], ids.Length);
    }

    private static RecordBatch SetValue(RecordBatch batch, string value)
    {
        var values = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            values.Append(value);
        return new RecordBatch(Schema, [batch.Column("id"), values.Build()], batch.Length);
    }

    /// <summary>
    /// v0 creates a row-tracking table, v1 appends ids 0..2 while tracking is on, and v2 turns
    /// <c>delta.enableRowTracking</c> off (as Spark's <c>ALTER TABLE ... SET TBLPROPERTIES</c> does), keeping the
    /// feature and the materialized column names. Returns the table open at v2.
    /// </summary>
    private async Task<DeltaTable> SupportedNotEnabledAsync(bool deletionVectors = false)
    {
        var fs = Fs;
        await using (var table = await DeltaTable.CreateAsync(
            fs, Schema, new DeltaTableOptions { CheckpointInterval = 0 },
            enableRowTracking: true, enableDeletionVectors: deletionVectors))
        {
            await table.WriteAsync([Rows(1, 2, 3)]);                                    // v1, ids 0..2
            var metadata = table.CurrentSnapshot.Metadata;
            var configuration = metadata.Configuration!.ToDictionary(p => p.Key, p => p.Value);
            configuration[RowTrackingConfig.EnableKey] = "false";
            await new TransactionLog(fs).WriteCommitAsync(2, [metadata with { Configuration = configuration }]);
        }
        var opened = await DeltaTable.OpenAsync(fs);
        Assert.True(RowTrackingConfig.IsSupported(opened.CurrentSnapshot.Protocol));
        Assert.False(RowTrackingConfig.IsEnabled(opened.CurrentSnapshot.Metadata.Configuration));
        Assert.Equal(3, opened.CurrentSnapshot.RowIdHighWaterMark);
        return opened;
    }

    /// <summary>
    /// Every active file has row ids, no two files' ranges overlap, the high-water mark covers them all, and every
    /// file the commit at <paramref name="version"/> added carries that version (or, for compaction, a source's).
    /// </summary>
    private void AssertRowIdsAssigned(DeltaTable table, long version, bool versionIsCommit = true)
    {
        var snapshot = table.CurrentSnapshot;
        var ranges = new List<(long Start, long End, string Path)>();
        foreach (var add in snapshot.ActiveFiles.Values)
        {
            Assert.True(add.BaseRowId.HasValue, $"{add.Path} has no baseRowId");
            Assert.True(add.DefaultRowCommitVersion.HasValue, $"{add.Path} has no defaultRowCommitVersion");
            long rows = add.GetNumRecords()!.Value;
            ranges.Add((add.BaseRowId!.Value, add.BaseRowId.Value + rows, add.Path));
        }
        ranges.Sort((a, b) => a.Start.CompareTo(b.Start));
        for (int i = 1; i < ranges.Count; i++)
            Assert.True(ranges[i - 1].End <= ranges[i].Start, $"{ranges[i - 1].Path} overlaps {ranges[i].Path}");
        Assert.True(ranges.Count == 0 || ranges[^1].End <= snapshot.RowIdHighWaterMark);
        Assert.True(snapshot.RowIdHighWaterMark >= 3, "the high-water mark went backwards");

        if (versionIsCommit)
        {
            var added = AddsAt(table, version);
            Assert.NotEmpty(added);
            Assert.All(added, add => Assert.Equal(version, add.DefaultRowCommitVersion));
        }
    }

    /// <summary>
    /// The files the commit at <paramref name="version"/> added, read from that commit's own log entry (NOT
    /// picked out by their defaultRowCommitVersion, which would hide a mis-stamped file from the assertions).
    /// Each is returned as the snapshot holds it; every one must still be active.
    /// </summary>
    private List<AddFile> AddsAt(DeltaTable table, long version)
    {
        string log = Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");
        var added = new List<AddFile>();
        foreach (string line in File.ReadAllLines(log))
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            using var doc = System.Text.Json.JsonDocument.Parse(line);
            if (!doc.RootElement.TryGetProperty("add", out var add))
                continue;
            string path = add.GetProperty("path").GetString()!;
            Assert.True(table.CurrentSnapshot.ActiveFiles.TryGetValue(path, out var active),
                $"{path}, added at v{version}, is not active");
            added.Add(active!);
        }
        return added;
    }

    /// <summary>A rewrite does not preserve ids while tracking is off, so it writes no materialized id columns.</summary>
    private async Task AssertNoMaterializedColumnsAsync(DeltaTable table)
    {
        var configuration = table.CurrentSnapshot.Metadata.Configuration!;
        string rowIdColumn = configuration[RowTrackingConfig.MaterializedRowIdColumnNameKey];
        string versionColumn = configuration[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey];
        var fs = Fs;
        foreach (var add in table.CurrentSnapshot.ActiveFiles.Values)
        {
            await using var file = await fs.OpenReadAsync(DeltaPath.Decode(add.Path));
            using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
            await foreach (var batch in reader.ReadAllAsync())
            {
                Assert.Equal(-1, batch.Schema.GetFieldIndex(rowIdColumn));
                Assert.Equal(-1, batch.Schema.GetFieldIndex(versionColumn));
            }
        }
    }

    private static async Task<List<long>> ReadIdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(id.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    [Fact]
    public async Task Append_AssignsRowIdsAndAdvancesTheHighWaterMark()
    {
        await using var table = await SupportedNotEnabledAsync();

        long version = await table.WriteAsync([Rows(4, 5)]);

        var add = Assert.Single(AddsAt(table, version));
        Assert.Equal(3, add.BaseRowId);
        Assert.Equal(5, table.CurrentSnapshot.RowIdHighWaterMark);
        Assert.Equal(4, RowTrackingConfig.TryReadHighWaterMark(table.CurrentSnapshot.DomainMetadata));
        AssertRowIdsAssigned(table, version);
    }

    [Fact]
    public async Task CopyOnWriteDelete_AssignsFreshIdsWithoutMaterializing()
    {
        await using var table = await SupportedNotEnabledAsync();

        var (deleted, version) = await table.DeleteAsync(Ex.Equal("id", 2L));

        Assert.Equal(1, deleted);
        var add = Assert.Single(AddsAt(table, version));
        Assert.Equal(3, add.BaseRowId);
        AssertRowIdsAssigned(table, version);
        await AssertNoMaterializedColumnsAsync(table);
        Assert.Equal([1L, 3L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task CopyOnWriteDeleteRows_AssignsFreshIds()
    {
        await using var table = await SupportedNotEnabledAsync();
        string path = table.CurrentSnapshot.ActiveFiles.Values.Single().Path;

        var (_, version) = await table.DeleteRowsAsync(
            RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>> { [path] = [0L] }),
            RowDeleteMode.CopyOnWrite);

        AssertRowIdsAssigned(table, version);
        await AssertNoMaterializedColumnsAsync(table);
    }

    [Fact]
    public async Task Update_AssignsFreshIdsWithoutMaterializing()
    {
        await using var table = await SupportedNotEnabledAsync();

        var (updated, version) = await table.UpdateAsync(Ex.Equal("id", 2L), b => SetValue(b, "updated"));

        Assert.Equal(1, updated);
        var add = Assert.Single(AddsAt(table, version));
        Assert.Equal(3, add.BaseRowId);
        Assert.Equal(6, table.CurrentSnapshot.RowIdHighWaterMark);
        AssertRowIdsAssigned(table, version);
        await AssertNoMaterializedColumnsAsync(table);
    }

    [Fact]
    public async Task Merge_AssignsIdsToRewrittenAndInsertedFiles()
    {
        await using var table = await SupportedNotEnabledAsync();

        var (_, version) = await table.MergeAsync([Rows(2, 9)], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = MergeAction.Update,
            WhenNotMatched = MergeAction.Insert,
        });

        Assert.True(AddsAt(table, version).Count >= 2);
        AssertRowIdsAssigned(table, version);
        await AssertNoMaterializedColumnsAsync(table);
        Assert.Equal([1L, 2L, 3L, 9L], await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Compaction_AssignsFreshIds()
    {
        await using var table = await SupportedNotEnabledAsync();
        await table.WriteAsync([Rows(4)]);
        long mark = table.CurrentSnapshot.RowIdHighWaterMark;

        long? version = await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        Assert.NotNull(version);
        var add = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal(mark, add.BaseRowId);
        Assert.Equal(1, add.DefaultRowCommitVersion); // OPTIMIZE keeps its sources' earliest version
        AssertRowIdsAssigned(table, version!.Value, versionIsCommit: false);
        await AssertNoMaterializedColumnsAsync(table);
    }

    [Fact]
    public async Task CommitDataFiles_AssignsRowIds()
    {
        await using var table = await SupportedNotEnabledAsync();

        var files = await table.WriteDataFilesAsync([Rows(4, 5)]);
        long version = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append);

        var add = Assert.Single(AddsAt(table, version));
        Assert.Equal(3, add.BaseRowId);
        AssertRowIdsAssigned(table, version);
    }

    /// <summary>A protocol upgrade fused into the same commit counts: the files it publishes alongside must carry
    /// row ids, since the protocol they land under supports the feature.</summary>
    [Fact]
    public async Task CommitDataFiles_WithAFusedProtocolUpgrade_AssignsRowIds()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, Schema, new DeltaTableOptions { CheckpointInterval = 0 });
        Assert.False(RowTrackingConfig.IsSupported(table.CurrentSnapshot.Protocol));

        var files = await table.WriteDataFilesAsync([Rows(1, 2)]);
        long version = await table.CommitDataFilesAsync(files, DeltaWriteMode.Append, extraActions:
        [
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["domainMetadata", "rowTracking", "appendOnly", "invariants"],
            },
        ]);

        Assert.True(RowTrackingConfig.IsSupported(table.CurrentSnapshot.Protocol));
        var add = Assert.Single(AddsAt(table, version));
        Assert.Equal(0, add.BaseRowId);
        Assert.Equal(version, add.DefaultRowCommitVersion);
        Assert.Equal(2, table.CurrentSnapshot.RowIdHighWaterMark);
        Assert.Equal(1, RowTrackingConfig.TryReadHighWaterMark(table.CurrentSnapshot.DomainMetadata));
    }

    [Fact]
    public async Task TransactionalOverwriteWhere_AssignsDisjointIdsToBothHalves()
    {
        await using var table = await SupportedNotEnabledAsync();

        var txn = table.StartTransaction();
        await txn.OverwriteWhereAsync([Rows(2)], Ex.Equal("id", 2L));
        long version = await txn.CommitAsync();

        Assert.Equal(2, AddsAt(table, version).Count); // the appended row and the rewrite of the rest
        AssertRowIdsAssigned(table, version);
        Assert.Equal([1L, 2L, 3L], await ReadIdsAsync(table));
    }

    /// <summary>A stale append rebased past a concurrent one re-derives its ids rather than reusing the ones the
    /// concurrent append took.</summary>
    [Fact]
    public async Task ConcurrentAppends_RebaseTheirRowIds()
    {
        await using (await SupportedNotEnabledAsync())
        {
        }
        await using var a = await DeltaTable.OpenAsync(Fs);
        await using var b = await DeltaTable.OpenAsync(Fs);

        long va = await a.WriteAsync([Rows(4, 5)]);
        long vb = await b.WriteAsync([Rows(6)]);

        Assert.True(vb > va);
        await using var check = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(5, check.CurrentSnapshot.ActiveFiles.Values.Single(f => f.DefaultRowCommitVersion == vb).BaseRowId);
        Assert.Equal(6, check.CurrentSnapshot.RowIdHighWaterMark);
        AssertRowIdsAssigned(check, vb);
    }

    /// <summary>Reads keep gating on enablement, as Spark's (<c>_metadata.row_id</c> is FIELD_NOT_FOUND there).</summary>
    [Fact]
    public async Task RowTrackingRead_IsStillRefused()
    {
        await using var table = await SupportedNotEnabledAsync();
        await table.WriteAsync([Rows(4)]);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
            {
            }
        });
    }

    /// <summary>The issue's case: RESTORE to a version from before tracking was enabled drops the property and
    /// keeps the feature (Spark parity), and the next append still gets row ids.</summary>
    [Fact]
    public async Task AppendAfterRestoreToBeforeEnablement_AssignsRowIds()
    {
        var fs = Fs;
        await using (var table = await DeltaTable.CreateAsync(
            fs, Schema, new DeltaTableOptions { CheckpointInterval = 0 }))
            await table.WriteAsync([Rows(1, 2)]);                                       // v1, no row ids
        await using (var table = await DeltaTable.OpenAsync(fs))
        {
            var snapshot = table.CurrentSnapshot;
            var configuration = (snapshot.Metadata.Configuration ?? new Dictionary<string, string>())
                .ToDictionary(p => p.Key, p => p.Value);
            configuration[RowTrackingConfig.EnableKey] = "true";
            configuration[RowTrackingConfig.MaterializedRowIdColumnNameKey] = "_row_id_x";
            configuration[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey] = "_row_cv_x";
            await new TransactionLog(fs).WriteCommitAsync(2,
            [
                new ProtocolAction
                {
                    MinReaderVersion = 1,
                    MinWriterVersion = 7,
                    WriterFeatures = ["domainMetadata", "rowTracking", "appendOnly", "invariants"],
                },
                snapshot.Metadata with { Configuration = configuration },
            ]);                                                                         // v2
        }
        await using var tracked = await DeltaTable.OpenAsync(fs);
        await tracked.WriteAsync([Rows(3)]);                                           // v3

        await tracked.RestoreAsync(1);
        Assert.False(RowTrackingConfig.IsEnabled(tracked.CurrentSnapshot.Metadata.Configuration));
        Assert.True(RowTrackingConfig.IsSupported(tracked.CurrentSnapshot.Protocol));
        long mark = tracked.CurrentSnapshot.RowIdHighWaterMark;

        long version = await tracked.WriteAsync([Rows(4)]);

        var add = Assert.Single(AddsAt(tracked, version));
        Assert.Equal(mark, add.BaseRowId);
        Assert.Equal(mark + 1, tracked.CurrentSnapshot.RowIdHighWaterMark);
    }

    /// <summary>Compacting files none of which had a commit version (written before the table supported row
    /// tracking, restored into it) gives the compacted file the OPTIMIZE's own version alongside its fresh ids.
    /// </summary>
    [Fact]
    public async Task Compaction_OfFilesWithoutACommitVersion_UsesItsOwn()
    {
        var fs = Fs;
        await using (var table = await DeltaTable.CreateAsync(
            fs, Schema, new DeltaTableOptions { CheckpointInterval = 0 }))
        {
            await table.WriteAsync([Rows(1)]);                                          // v1, no row ids
            await table.WriteAsync([Rows(2)]);                                          // v2, no row ids
            await new TransactionLog(fs).WriteCommitAsync(3,
            [
                new ProtocolAction
                {
                    MinReaderVersion = 1,
                    MinWriterVersion = 7,
                    WriterFeatures = ["domainMetadata", "rowTracking", "appendOnly", "invariants"],
                },
            ]);                                                                         // v3: supported only
        }
        await using var tracked = await DeltaTable.OpenAsync(fs);
        Assert.All(tracked.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DefaultRowCommitVersion));

        long? version = await tracked.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        var add = tracked.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal(0, add.BaseRowId);
        Assert.Equal(version, add.DefaultRowCommitVersion);
        Assert.Equal(2, tracked.CurrentSnapshot.RowIdHighWaterMark);
    }

    /// <summary>
    /// The same compaction, planned on a stale handle and rebased past a concurrent append: the version it gave
    /// the compacted file moves to the version the OPTIMIZE actually lands at (the append took the planned one),
    /// and its ids move past the append's.
    /// </summary>
    [Fact]
    public async Task Compaction_OfFilesWithoutACommitVersion_RebasedPastAnAppend_MovesItsVersion()
    {
        var fs = Fs;
        await using (var table = await DeltaTable.CreateAsync(
            fs, Schema, new DeltaTableOptions { CheckpointInterval = 0 }))
        {
            await table.WriteAsync([Rows(1)]);                                          // v1, no row ids
            await table.WriteAsync([Rows(2)]);                                          // v2, no row ids
            await new TransactionLog(fs).WriteCommitAsync(3,
            [
                new ProtocolAction
                {
                    MinReaderVersion = 1,
                    MinWriterVersion = 7,
                    WriterFeatures = ["domainMetadata", "rowTracking", "appendOnly", "invariants"],
                },
            ]);                                                                         // v3: supported only
        }
        await using var stale = await DeltaTable.OpenAsync(fs);
        await using (var other = await DeltaTable.OpenAsync(fs))
        {
            long appended = await other.WriteAsync([Rows(3, 4)]);                      // v4, ids 0..1
            Assert.Equal(4, appended);
        }

        // The stale plan sees only the two versionless files and stamps the compacted one v4, which the append took.
        long? version = await stale.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });

        Assert.Equal(5, version);
        await using var check = await DeltaTable.OpenAsync(fs);
        var compacted = Assert.Single(AddsAt(check, 5));
        Assert.Equal(5, compacted.DefaultRowCommitVersion);
        var append = check.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Path != compacted.Path);
        Assert.Equal(0, append.BaseRowId);
        Assert.Equal(4, append.DefaultRowCommitVersion);
        Assert.Equal(2, compacted.BaseRowId);
        Assert.Equal(4, check.CurrentSnapshot.RowIdHighWaterMark);
        AssertRowIdsAssigned(check, 5);
    }

    /// <summary>Without the feature nothing changes: no ids, no high-water mark.</summary>
    [Fact]
    public async Task WithoutTheFeature_NoRowIdsAreAssigned()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, Schema);

        await table.WriteAsync([Rows(1, 2)]);

        var add = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Null(add.BaseRowId);
        Assert.Null(add.DefaultRowCommitVersion);
        Assert.Null(RowTrackingConfig.TryReadHighWaterMark(table.CurrentSnapshot.DomainMetadata));
    }
}
