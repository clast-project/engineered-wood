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

        // A commit that changes no data never consults the level, so it still lands.
        await reopened.SetDomainMetadataAsync("app.isolation-test", "{}");
    }
}
