// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTransaction.StageAppTransaction"/> — an idempotent producer's version, committed atomically
/// with the transaction's work and guarded by a compare-and-set.
///
/// <para>What justifies a typed method rather than a hand-built <c>txn</c> action through
/// <see cref="DeltaTransaction.StageActions"/> is that the guard has to be re-checked on EVERY commit attempt.
/// The test that shows it is <see cref="TwinProducerWonTheRace_RetryDoesNotDuplicateTheBatch"/>: the read-set
/// check passes on the retry, because nothing this transaction READ was invalidated by its twin — only the
/// producer version was.</para>
/// </summary>
public class AppTransactionStagingTests : IDisposable
{
    private readonly string _tempDir;

    public AppTransactionStagingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_apptxn_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            ids.Append(startId + i);
        return new RecordBatch(BuildSchema(), [ids.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private async Task<DeltaTable> CreateAsync()
    {
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), BuildSchema());
        await table.WriteAsync([Batch(0, 2)]);   // v1: ids 0,1
        return table;
    }

    /// <summary>Stages files the way a host that owns its data plane does: write, then record.</summary>
    private static async Task StageAppendAsync(DeltaTable table, DeltaTransaction txn, RecordBatch batch)
    {
        var files = await table.WriteDataFilesAsync([batch]);
        txn.StageDataFiles(files);
    }

    private async Task<int> RowCountAsync()
    {
        await using var table = await OpenAsync();
        int n = 0;
        await foreach (var batch in table.ReadAllAsync())
            n += batch.Length;
        return n;
    }

    private async Task<long?> RecordedVersionAsync(string appId)
    {
        await using var table = await OpenAsync();
        return table.CurrentSnapshot.AppTransactions.TryGetValue(appId, out var t) ? t.Version : null;
    }

    [Fact]
    public async Task StagedVersion_IsCommittedWithTheWork()
    {
        await using var table = await CreateAsync();
        var txn = table.StartTransaction();
        await StageAppendAsync(table, txn, Batch(10, 3));
        txn.StageAppTransaction("producer-1", 7);
        await txn.CommitAsync();

        Assert.Equal(5, await RowCountAsync());
        Assert.Equal(7, await RecordedVersionAsync("producer-1"));
    }

    /// <summary>A batch already recorded as of the base version fails BEFORE anything is written.</summary>
    [Fact]
    public async Task ExpectedPreviousMismatch_ThrowsAndCommitsNothing()
    {
        await using var table = await CreateAsync();
        var first = table.StartTransaction();
        await StageAppendAsync(table, first, Batch(10, 3));
        first.StageAppTransaction("producer-1", 7);
        await first.CommitAsync();
        long versionAfterFirst = table.CurrentSnapshot.Version;

        // A replay of the same batch: it still expects "no recorded version", but there is one now.
        var replay = table.StartTransaction();
        await StageAppendAsync(table, replay, Batch(10, 3));
        replay.StageAppTransaction("producer-1", 7);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await replay.CommitAsync());
        Assert.Contains("producer-1", ex.Message);

        Assert.Equal(versionAfterFirst, table.CurrentSnapshot.Version);
        Assert.Equal(5, await RowCountAsync());   // the replay's rows are NOT in the table
    }

    /// <summary>
    /// THE CASE THE GUARD EXISTS FOR. Two producers run the same batch. The twin commits first, so this
    /// transaction's optimistic write loses the race and the loop retries — and on that retry the read-set
    /// check has nothing to object to, since a concurrent APPEND invalidates no read of ours. Only the
    /// re-checked producer version stops the batch being committed twice.
    /// </summary>
    [Fact]
    public async Task TwinProducerWonTheRace_RetryDoesNotDuplicateTheBatch()
    {
        await using var created = await CreateAsync();

        // Ours: opened and based BEFORE the twin commits, and not yet committed.
        await using var mine = await OpenAsync();
        var txn = mine.StartTransaction();
        await StageAppendAsync(mine, txn, Batch(10, 3));
        txn.StageAppTransaction("producer-1", 7);

        // The twin runs the identical batch through its own handle and commits first, taking our version.
        await using (var twin = await OpenAsync())
        {
            var twinTxn = twin.StartTransaction();
            await StageAppendAsync(twin, twinTxn, Batch(10, 3));
            twinTxn.StageAppTransaction("producer-1", 7);
            await twinTxn.CommitAsync();
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await txn.CommitAsync());
        Assert.Contains("producer-1", ex.Message);

        // Exactly ONE copy of the batch landed — 2 original + 3 from the twin.
        Assert.Equal(5, await RowCountAsync());
        Assert.Equal(7, await RecordedVersionAsync("producer-1"));
    }

    /// <summary>The producer advances batch by batch: each states the version it expects to be at.</summary>
    [Fact]
    public async Task ChainedExpectedPrevious_Advances()
    {
        await using var table = await CreateAsync();
        var first = table.StartTransaction();
        await StageAppendAsync(table, first, Batch(10, 1));
        first.StageAppTransaction("producer-1", 1);
        await first.CommitAsync();

        var second = table.StartTransaction();
        await StageAppendAsync(table, second, Batch(20, 1));
        second.StageAppTransaction("producer-1", 2, expectedPrevious: 1);
        await second.CommitAsync();

        Assert.Equal(2, await RecordedVersionAsync("producer-1"));
        Assert.Equal(4, await RowCountAsync());
    }

    [Fact]
    public async Task IndependentProducers_DoNotInterfere()
    {
        await using var table = await CreateAsync();
        var txn = table.StartTransaction();
        await StageAppendAsync(table, txn, Batch(10, 1));
        txn.StageAppTransaction("producer-a", 3);
        txn.StageAppTransaction("producer-b", 99);
        await txn.CommitAsync();

        Assert.Equal(3, await RecordedVersionAsync("producer-a"));
        Assert.Equal(99, await RecordedVersionAsync("producer-b"));
    }

    [Fact]
    public async Task EmptyAppId_Throws()
    {
        await using var table = await CreateAsync();
        var txn = table.StartTransaction();
        Assert.Throws<ArgumentException>(() => txn.StageAppTransaction("", 1));
    }
}
