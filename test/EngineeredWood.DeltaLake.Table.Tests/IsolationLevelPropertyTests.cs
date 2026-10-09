// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <c>delta.isolationLevel</c> is the table's demand on every data-changing commit (#472). Unset, the level is
/// <see cref="IsolationLevel.WriteSerializable"/>; set to <c>Serializable</c>, a transaction started without a
/// level runs at Serializable, a stronger request is allowed, and a weaker one is refused. A value naming no level
/// refuses data-changing commits with <c>DELTA_INVALID_ISOLATION_LEVEL</c>, as delta-spark does.
/// </summary>
public class IsolationLevelPropertyTests : IDisposable
{
    private readonly string _tempDir;

    public IsolationLevelPropertyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_isoprop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("region", StringType.Default, false))
        .Build();

    private static RecordBatch Batch(long[] ids, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);
        return new RecordBatch(IdRegionSchema, [idArray, regionBuilder.Build()], ids.Length);
    }

    private LocalTableFileSystem Fs() => new(_tempDir);

    private async Task<DeltaTable> CreateAsync(string? isolationLevel, bool enableRowTracking = false)
    {
        var configuration = isolationLevel is null
            ? null
            : new Dictionary<string, string> { ["delta.isolationLevel"] = isolationLevel };
        var table = await DeltaTable.CreateAsync(
            Fs(), IdRegionSchema, enableDeletionVectors: true, enableRowTracking: enableRowTracking,
            configuration: configuration);
        await table.WriteAsync([Batch([1, 2], ["us", "eu"])]);
        return table;
    }

    // ── Which level a transaction gets ──

    [Fact]
    public async Task Unset_DefaultsToWriteSerializable()
    {
        await using var table = await CreateAsync(isolationLevel: null);
        Assert.Equal(IsolationLevel.WriteSerializable, table.StartTransaction().IsolationLevel);
    }

    [Theory]
    [InlineData("Serializable")]
    [InlineData("serializable")] // delta-spark parses the value case-insensitively
    public async Task SerializableTable_TransactionWithoutLevel_RunsSerializable(string value)
    {
        await using var table = await CreateAsync(value);
        Assert.Equal(IsolationLevel.Serializable, table.StartTransaction().IsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, table.StartTransaction(table.CurrentSnapshot).IsolationLevel);
        var pinned = await table.StartTransactionAsync(table.CurrentSnapshot.Version);
        Assert.Equal(IsolationLevel.Serializable, pinned.IsolationLevel);
    }

    [Fact]
    public async Task SerializableTable_ExplicitWriteSerializable_IsRefused()
    {
        await using var table = await CreateAsync("Serializable");

        var ex = Assert.Throws<ArgumentException>(() => table.StartTransaction(IsolationLevel.WriteSerializable));
        Assert.Equal("isolationLevel", ex.ParamName);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.StartTransactionAsync(table.CurrentSnapshot.Version, IsolationLevel.WriteSerializable));
    }

    [Fact]
    public async Task DefaultTable_ExplicitSerializable_IsAllowed()
    {
        await using var table = await CreateAsync(isolationLevel: null);
        Assert.Equal(IsolationLevel.Serializable, table.StartTransaction(IsolationLevel.Serializable).IsolationLevel);
    }

    [Fact]
    public async Task ExplicitWriteSerializableValue_IsTheDefault()
    {
        await using var table = await CreateAsync("WriteSerializable");
        Assert.Equal(IsolationLevel.WriteSerializable, table.StartTransaction().IsolationLevel);
    }

    /// <summary>An undefined value would compare as stronger than Serializable and then run as
    /// WriteSerializable wherever the checker asks <c>== Serializable</c>.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("Serializable")]
    public async Task UndefinedLevel_IsRefused(string? tableLevel)
    {
        await using var table = await CreateAsync(tableLevel);
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => table.StartTransaction((IsolationLevel)2));
        Assert.Equal("isolationLevel", ex.ParamName);
    }

    // ── What the level changes ──

    /// <summary>
    /// The defect in #472: on a table demanding Serializable, a transaction started the way the auto-committing
    /// DELETE starts one committed past a concurrent blind append matching its predicate. Spark aborts it.
    /// </summary>
    [Fact]
    public async Task SerializableTable_ConcurrentBlindAppendMatchingPredicate_Aborts()
    {
        await using var table = await CreateAsync("Serializable");

        var tx = table.StartTransaction();
        await tx.DeleteAsync(Ex.Equal("region", "us"));

        await using (var other = await DeltaTable.OpenAsync(Fs()))
            await other.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());
        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
    }

    /// <summary>The row-level deletion-vector union is a WriteSerializable behaviour, so the table's level
    /// turns it off too: two deletes of different rows in one file conflict.</summary>
    [Fact]
    public async Task SerializableTable_DisjointRowDeletesOfOneFile_Conflict()
    {
        await using var table = await CreateAsync("Serializable", enableRowTracking: true);

        var tx = table.StartTransaction();
        await tx.StageRowDeletesAsync(RowSelection.FromRowAddresses(
            [TransientRowAddress.Pack(0, 0)], tx.Snapshot));

        await using (var other = await DeltaTable.OpenAsync(Fs()))
        {
            await other.DeleteRowsAsync(RowSelection.FromRowAddresses(
                [TransientRowAddress.Pack(0, 1)], other.CurrentSnapshot));
        }

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx.CommitAsync());
    }

    // ── The auto-committing operations read the level themselves ──
    //
    // Each plans against its handle's current snapshot and commits through the OCC loop with the level it
    // resolved, so a STALE handle races the way a transaction does. Of the auto-committing DML, these two are
    // the ones where the level can change the verdict: the copy-on-write DELETE and the row-level UPDATE read
    // only the files they rewrite, which no concurrent add can match at either level, so for them the level
    // only refuses an invalid value (InvalidValue_RefusesDataChangingCommits).

    /// <summary>
    /// The predicate UPDATE: a concurrent blind append of a row matching its predicate aborts it on a Serializable
    /// table, and is let through by default, as for a transaction's DELETE.
    /// </summary>
    [Theory]
    [InlineData("Serializable", true)]
    [InlineData(null, false)]
    public async Task AutoCommitUpdate_ConcurrentBlindAppendMatchingPredicate(string? level, bool expectConflict)
    {
        await using var created = await CreateAsync(level);
        await using var stale = await DeltaTable.OpenAsync(Fs());

        await using (var other = await DeltaTable.OpenAsync(Fs()))
            await other.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        if (expectConflict)
        {
            var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
                await stale.UpdateAsync(Ex.Equal("region", "us"), batch => batch));
            Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
        }
        else
        {
            var (updated, _) = await stale.UpdateAsync(Ex.Equal("region", "us"), batch => batch);
            Assert.Equal(1, updated); // the appended row linearizes after the update
        }
    }

    /// <summary>
    /// The row DELETE with rowLevelRetry: two deletes of different rows in one file reconcile by default and
    /// conflict on a Serializable table, as the transaction's staged row deletes do.
    /// </summary>
    [Theory]
    [InlineData("Serializable", true)]
    [InlineData(null, false)]
    public async Task AutoCommitRowDelete_DisjointRowsOfOneFile(string? level, bool expectConflict)
    {
        await using var created = await CreateAsync(level, enableRowTracking: true);
        await using var stale = await DeltaTable.OpenAsync(Fs());
        var ours = RowSelection.FromRowAddresses([TransientRowAddress.Pack(0, 0)], stale.CurrentSnapshot);

        await using (var other = await DeltaTable.OpenAsync(Fs()))
        {
            await other.DeleteRowsAsync(RowSelection.FromRowAddresses(
                [TransientRowAddress.Pack(0, 1)], other.CurrentSnapshot));
        }

        if (expectConflict)
        {
            await Assert.ThrowsAsync<DeltaConflictException>(async () =>
                await stale.DeleteRowsAsync(ours, rowLevelRetry: true));
        }
        else
        {
            var (deleted, _) = await stale.DeleteRowsAsync(ours, rowLevelRetry: true);
            Assert.Equal(1, deleted);
        }
    }

    // ── The buffered rebase check ──

    [Fact]
    public async Task SerializableTable_RebaseCheckWithoutLevel_ExaminesBlindAppend()
    {
        await using var table = await CreateAsync("Serializable");
        var baseSnapshot = table.CurrentSnapshot;

        await using var other = await DeltaTable.OpenAsync(Fs());
        await other.WriteAsync([Batch([3], ["us"])]); // concurrent blind append

        await using var reader = await DeltaTable.OpenAsync(Fs());
        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await reader.CheckLogicalRebaseAsync(
                baseSnapshot, plannedActions: [], readPredicates: [Ex.Equal("region", "us")]));
        Assert.Equal(DeltaErrorCodes.ConcurrentAppend, conflict.ErrorCode);
    }

    [Fact]
    public async Task SerializableTable_RebaseCheckAskingWriteSerializable_IsRefused()
    {
        await using var table = await CreateAsync("Serializable");

        // Refused even with nothing concurrent: an objection that appeared only under contention would read
        // as an intermittent failure.
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.CheckLogicalRebaseAsync(table.CurrentSnapshot, plannedActions: [], serializable: false));
        Assert.Equal("serializable", ex.ParamName);
    }

    // ── A value naming no level ──

    /// <summary>
    /// Caller-supplied metaData cannot introduce a bad value either, through any of the seams that take it:
    /// fused into an external-file commit, staged on a transaction, or riding a clustering change. One that
    /// carries the table's own value through unchanged is not judged, so a valid table property update lands.
    /// </summary>
    [Fact]
    public async Task InvalidValue_RefusedInCallerMetadata()
    {
        await using var table = await CreateAsync(isolationLevel: null);
        var metadata = table.CurrentSnapshot.Metadata;
        EngineeredWood.DeltaLake.Actions.MetadataAction WithLevel(string level)
        {
            var configuration = new Dictionary<string, string>();
            foreach (var kv in metadata.Configuration ?? new Dictionary<string, string>())
                configuration[kv.Key] = kv.Value;
            configuration["delta.isolationLevel"] = level;
            return metadata with { Configuration = configuration };
        }
        long version = table.CurrentSnapshot.Version;

        var fused = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.CommitDataFilesAsync(
                [new WrittenDataFile("elsewhere.parquet", 100, 1, null, null)],
                extraActions: [WithLevel("RepeatableRead")]));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, fused.ErrorCode);

        var txn = table.StartTransaction();
        txn.StageActions([WithLevel("RepeatableRead")]);
        var staged = await Assert.ThrowsAsync<DeltaFormatException>(async () => await txn.CommitAsync());
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, staged.ErrorCode);

        var clustering = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.SetClusteringColumnsAsync([], extraActions: [WithLevel("RepeatableRead")]));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, clustering.ErrorCode);

        Assert.Equal(version, (await DeltaTable.OpenAsync(Fs())).CurrentSnapshot.Version);

        // A valid value is a property update like any other.
        await table.CommitDataFilesAsync(
            System.Array.Empty<WrittenDataFile>(), extraActions: [WithLevel("Serializable")]);
        Assert.Equal(IsolationLevel.Serializable, table.StartTransaction().IsolationLevel);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvalidValue_RefusedAtCreate(bool withData)
    {
        var configuration = new Dictionary<string, string> { ["delta.isolationLevel"] = "RepeatableRead" };
        var refused = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await (withData
                ? DeltaTable.CreateOrReplaceAsync(Fs(), IdRegionSchema, [Batch([1], ["us"])], configuration: configuration)
                : DeltaTable.CreateAsync(Fs(), IdRegionSchema, configuration: configuration)));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, refused.ErrorCode);
        Assert.Empty(Directory.GetFiles(_tempDir, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task InvalidValue_RefusesDataChangingCommits()
    {
        await using var table = await CreateAsync(isolationLevel: null);
        // Created valid, so the setup's own append lands; the bad value arrives with a later metadata change.
        // There is no property setter, so it comes in the way another engine's commit would.
        var log = new EngineeredWood.DeltaLake.Log.TransactionLog(Fs());
        var snapshot = table.CurrentSnapshot;
        var configuration = new Dictionary<string, string>();
        foreach (var kv in snapshot.Metadata.Configuration ?? new Dictionary<string, string>())
            configuration[kv.Key] = kv.Value;
        configuration["delta.isolationLevel"] = "RepeatableRead";
        await log.WriteCommitAsync(snapshot.Version + 1, [snapshot.Metadata with { Configuration = configuration }]);

        await using var reopened = await DeltaTable.OpenAsync(Fs());

        var start = Assert.Throws<DeltaFormatException>(() => reopened.StartTransaction());
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, start.ErrorCode);

        var append = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.WriteAsync([Batch([3], ["us"])]));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, append.ErrorCode);

        // The overwrite family makes one direct attempt instead of going through the commit loop, and is
        // refused all the same.
        var overwrite = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.WriteAsync([Batch([3], ["us"])], DeltaWriteMode.Overwrite));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, overwrite.ErrorCode);

        // So is a commit of files written elsewhere.
        var external = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.CommitDataFilesAsync([new WrittenDataFile("elsewhere.parquet", 100, 1, null, null)]));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, external.ErrorCode);

        // So is each auto-committing DML that changes data: the predicate UPDATE, both row DELETE modes and the
        // row-level UPDATE.
        var update = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.UpdateAsync(Ex.Equal("region", "us"), batch => batch));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, update.ErrorCode);
        foreach (var mode in new[] { RowDeleteMode.DeletionVector, RowDeleteMode.CopyOnWrite })
        {
            var rowDelete = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
                await reopened.DeleteRowsAsync(
                    RowSelection.FromRowAddresses([TransientRowAddress.Pack(0, 0)], reopened.CurrentSnapshot),
                    mode));
            Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, rowDelete.ErrorCode);
        }
        var rowUpdate = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.UpdateRowsAsync(
                RowSelection.FromRowAddresses([TransientRowAddress.Pack(0, 0)], reopened.CurrentSnapshot),
                (_, batches, _) => batches));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, rowUpdate.ErrorCode);

        // ...and refused before anything is written: a commit whose file arrives with deleted rows writes their
        // deletion vector while building its actions, and this one is too large to inline.
        var sparse = new long[5000];
        for (int i = 0; i < sparse.Length; i++)
            sparse[i] = 2L * i;
        var withDv = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.CommitDataFilesAsync(
                [new WrittenDataFile("elsewhere.parquet", 100, 10000, null, null)],
                deletedPositionsByFileIndex: new Dictionary<int, IReadOnlyCollection<long>> { [0] = sparse }));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, withDv.ErrorCode);
        Assert.Empty(Directory.GetFiles(_tempDir, "deletion_vector_*", SearchOption.AllDirectories));

        // A file-less overwrite still removes every file, so it changes data and is refused too.
        var emptyOverwrite = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await reopened.CommitDataFilesAsync(System.Array.Empty<WrittenDataFile>(), DeltaWriteMode.Overwrite));
        Assert.Equal(DeltaErrorCodes.InvalidIsolationLevel, emptyOverwrite.ErrorCode);

        // A commit that changes no data never consults the level, so it still lands...
        await reopened.SetDomainMetadataAsync("app.isolation-test", "{}");

        // ...and an append of no rows or an UPDATE matching nothing commits nothing, so neither is refused.
        long version = reopened.CurrentSnapshot.Version;
        Assert.Equal(version, await reopened.WriteAsync([Batch([], [])]));
        var (updated, updateVersion) = await reopened.UpdateAsync(
            Ex.Equal("region", "nowhere"), batch => batch);
        Assert.Equal(0, updated);
        Assert.Equal(version, updateVersion);
    }
}
