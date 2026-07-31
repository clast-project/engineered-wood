// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Three prefixes on <see cref="DeltaTransaction"/>, three retry contracts, each visible in the name:
///
/// <list type="bullet">
/// <item><b>Stage*</b> — an EFFECT. Staged output, rebased by the commit loop and retried on
/// <see cref="DeltaConflictException"/>.</item>
/// <item><b>Require*</b> — a PRECONDITION. A fact about the base version, re-checked before every attempt.
/// It cannot become true by retrying, so a violation throws <see cref="InvalidOperationException"/> and the
/// loop does not retry it.</item>
/// <item><b>Declare*</b> — a DECLARATION. Widens the read set the conflict checker tests against. Unlike a
/// precondition it is subject to POLICY: the isolation level decides what conflicts with it.</item>
/// </list>
///
/// <para>Plus <see cref="DeltaTransaction.Operation"/>, a property rather than a per-call argument, because
/// Delta's operation field is one string per commit.</para>
/// </summary>
public class TransactionPreconditionTests : IDisposable
{
    private readonly string _tempDir;

    public TransactionPreconditionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_precond_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

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

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(Fs).AsTask();

    private async Task<long?> RecordedVersionAsync(string appId)
    {
        await using var reader = await OpenAsync();
        return reader.CurrentSnapshot.AppTransactions.TryGetValue(appId, out var txn) ? txn.Version : null;
    }

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

    // ── Require*: the precondition ──

    /// <summary>
    /// The point of an idempotent producer: the batch's DATA and the record of having written it land in one
    /// version, so there is no window in which one exists without the other.
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_CommitsTheTxnActionAtomicallyWithTheData()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(1, 3)]);
        txn.RequireAppTransaction("producer", version: 7);
        long version = await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 2, 3 }, await ReadIdsFreshAsync());
        Assert.Equal(7, await RecordedVersionAsync("producer"));

        // One version, carrying both.
        var actions = await new DeltaLake.Log.TransactionLog(Fs).ReadCommitAsync(version);
        Assert.Contains(actions, a => a is AddFile);
        var recorded = Assert.Single(actions.OfType<TransactionId>());
        Assert.Equal(("producer", 7L), (recorded.AppId, recorded.Version));
    }

    [Fact]
    public async Task RequireAppTransaction_SatisfiedPrecondition_Commits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var first = table.StartTransaction();
        await first.WriteAsync([Batch(1, 1)]);
        first.RequireAppTransaction("producer", version: 1);
        await first.CommitAsync();

        await using var reopened = await OpenAsync();
        var second = reopened.StartTransaction();
        await second.WriteAsync([Batch(2, 1)]);
        second.RequireAppTransaction("producer", version: 2, expectedPrevious: 1);
        await second.CommitAsync();

        Assert.Equal(2, await RecordedVersionAsync("producer"));
        Assert.Equal(new long[] { 1, 2 }, await ReadIdsFreshAsync());
    }

    /// <summary>
    /// The reasoning that earned <c>Require</c> its own prefix: a violated precondition is NOT a conflict.
    /// The commit loop retries a <see cref="DeltaConflictException"/>, and retrying cannot make an
    /// already-committed batch un-commit — a producer told "conflict" would keep trying to write a batch the
    /// table already holds.
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_ViolatedPrecondition_ThrowsInvalidOperation_NotConflict()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var first = table.StartTransaction();
        await first.WriteAsync([Batch(1, 1)]);
        first.RequireAppTransaction("producer", version: 5);
        await first.CommitAsync();

        await using var reopened = await OpenAsync();
        var second = reopened.StartTransaction();
        await second.WriteAsync([Batch(2, 1)]);
        second.RequireAppTransaction("producer", version: 6, expectedPrevious: 4); // the table records 5

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await second.CommitAsync());
        Assert.IsNotType<DeltaConflictException>(ex);
        Assert.Contains("producer", ex.Message);
        Assert.Contains("expected the table to record version 4", ex.Message);
        Assert.Contains("records 5", ex.Message);

        // Nothing landed: the data is not in the table and the recorded version is untouched.
        Assert.Equal(new long[] { 1 }, await ReadIdsFreshAsync());
        Assert.Equal(5, await RecordedVersionAsync("producer"));
    }

    [Fact]
    public async Task RequireAppTransaction_ExpectingAPreviousThatWasNeverRecorded_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 1)]);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(2, 1)]);
        txn.RequireAppTransaction("never-seen", version: 2, expectedPrevious: 1);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await txn.CommitAsync());
        Assert.Contains("no transaction at all", ex.Message);
    }

    /// <summary>
    /// The precondition is re-checked on EVERY attempt, not just the first: a concurrent commit that moves
    /// the producer's recorded version while this transaction is open invalidates it, even though the
    /// transaction would otherwise have rebased past a non-conflicting append.
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_IsRecheckedAgainstConcurrentCommits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var setup = table.StartTransaction();
        await setup.WriteAsync([Batch(1, 1)]);
        setup.RequireAppTransaction("producer", version: 1);
        await setup.CommitAsync();

        // Open a transaction whose precondition holds RIGHT NOW.
        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        await txn.WriteAsync([Batch(10, 1)]);
        txn.RequireAppTransaction("producer", version: 2, expectedPrevious: 1);

        // A racer advances the same producer — a blind append the conflict checker would happily rebase past.
        await using (var racer = await OpenAsync())
        {
            var racerTxn = racer.StartTransaction();
            await racerTxn.WriteAsync([Batch(20, 1)]);
            racerTxn.RequireAppTransaction("producer", version: 2);
            await racerTxn.CommitAsync();
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await txn.CommitAsync());
        Assert.Contains("records 2", ex.Message);

        // The racer's work landed; ours did not.
        Assert.Equal(new long[] { 1, 20 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task RequireAppTransaction_NoExpectedPrevious_WritesUnconditionally()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var first = table.StartTransaction();
        await first.WriteAsync([Batch(1, 1)]);
        first.RequireAppTransaction("producer", version: 9);
        await first.CommitAsync();

        await using var reopened = await OpenAsync();
        var second = reopened.StartTransaction();
        await second.WriteAsync([Batch(2, 1)]);
        second.RequireAppTransaction("producer", version: 3); // lower, and unguarded
        await second.CommitAsync();

        Assert.Equal(3, await RecordedVersionAsync("producer"));
    }

    [Fact]
    public async Task RequireAppTransaction_TwiceForOneAppId_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        var txn = table.StartTransaction();
        txn.RequireAppTransaction("producer", version: 1);

        var ex = Assert.Throws<InvalidOperationException>(
            () => txn.RequireAppTransaction("producer", version: 2));
        Assert.Contains("at most one txn action per appId", ex.Message);

        // A DIFFERENT producer in the same transaction is fine.
        txn.RequireAppTransaction("other", version: 1);
    }

    [Fact]
    public async Task RequireAppTransaction_EmptyAppId_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        var txn = table.StartTransaction();
        Assert.Throws<ArgumentException>(() => txn.RequireAppTransaction("", version: 1));
    }

    // ── Declare*: the declaration ──

    /// <summary>
    /// A host's own scan read rows the library never saw, so only the host can say what the transaction
    /// depended on. Declared, a concurrent add matching it conflicts under Serializable.
    /// </summary>
    [Fact]
    public async Task DeclareRead_ConcurrentMatchingAppend_ConflictsUnderSerializable()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction(IsolationLevel.Serializable);
        txn.DeclareRead(Ex.GreaterThan("id", 100L));
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(200, 1)]); // matches the declared predicate

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>The level's own distinction: a blind append is exempt at WriteSerializable. Same
    /// declaration, same racer, different verdict — which is what makes the declaration a matter of POLICY
    /// rather than a fact like a precondition.</summary>
    [Fact]
    public async Task DeclareRead_ConcurrentMatchingAppend_IsExemptUnderWriteSerializable()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction(IsolationLevel.WriteSerializable);
        txn.DeclareRead(Ex.GreaterThan("id", 100L));
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(200, 1)]);

        await txn.CommitAsync(); // no conflict
        Assert.Equal(new long[] { 1, 2, 3, 200, 1000 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task DeclareRead_NonMatchingConcurrentAppend_DoesNotConflict()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction(IsolationLevel.Serializable);
        txn.DeclareRead(Ex.GreaterThan("id", 100L));
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(4, 1)]); // id 4 — cannot match id > 100

        await txn.CommitAsync();
    }

    /// <summary>
    /// The honest declaration when a scan had no pushable predicate: stronger than any set of them, so a
    /// concurrent commit that REMOVES a file is relevant too — which no predicate expresses.
    /// </summary>
    [Fact]
    public async Task DeclareWholeTableRead_ConcurrentDeleteConflicts()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        txn.DeclareWholeTableRead();
        await txn.WriteAsync([Batch(100, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 2L));

        // Honoured at BOTH levels today — the proposal that would drop it for a row-level delete under
        // WriteSerializable is open question 4 and deliberately NOT implemented; see the method's remarks.
        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    [Fact]
    public async Task WithoutADeclaration_TheSameConcurrentDeleteIsIgnored()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        await txn.WriteAsync([Batch(100, 1)]); // a blind append, declaring nothing

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 2L));

        await txn.CommitAsync(); // nothing was declared, so nothing was invalidated
    }

    // ── Operation ──

    private async Task<string?> LastOperationAsync()
    {
        await using var reader = await OpenAsync();
        DeltaTable.DeltaHistoryEntry? last = null;
        await foreach (var entry in reader.GetHistoryAsync())
            last = entry;
        return last?.Operation;
    }

    [Fact]
    public async Task Operation_DefaultsToTheInference()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);

        var append = table.StartTransaction();
        await append.WriteAsync([Batch(1, 2)]);
        await append.CommitAsync();
        Assert.Equal("WRITE", await LastOperationAsync());

        await using var reopened = await OpenAsync();
        var delete = reopened.StartTransaction();
        await delete.DeleteAsync(Ex.Equal("id", 1L));
        await delete.CommitAsync();
        Assert.Equal("DELETE", await LastOperationAsync());
    }

    [Fact]
    public async Task Operation_SetExplicitly_Wins()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(1, 2)]);
        txn.Operation = "MERGE";
        await txn.CommitAsync();

        Assert.Equal("MERGE", await LastOperationAsync());
    }

    /// <summary>A mixed transaction has no single inferred name — Delta's operation field is one string, and
    /// no engine names a fused DELETE+INSERT — so this is exactly when a caller wants to say.</summary>
    [Fact]
    public async Task Operation_NamesAMixedTransactionThatWouldOtherwiseReportWrite()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        await txn.WriteAsync([Batch(100, 1)]);
        await txn.DeleteAsync(Ex.Equal("id", 2L));
        Assert.Null(txn.Operation);        // nothing set — the inference would say "WRITE"
        txn.Operation = "MERGE";
        await txn.CommitAsync();

        Assert.Equal("MERGE", await LastOperationAsync());
    }

    [Fact]
    public async Task Operation_EmptyString_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        var txn = table.StartTransaction();

        Assert.Throws<ArgumentException>(() => txn.Operation = "");
        Assert.Throws<ArgumentException>(() => txn.Operation = "   ");
        txn.Operation = null; // how you ask for the inference back
    }

    // ── the three prefixes, side by side ──

    /// <summary>
    /// One transaction carrying all three kinds, so the retry contracts are visible together: the effect
    /// rebases past the racer's blind append, the declaration decides nothing conflicts at
    /// WriteSerializable, and the precondition still holds — so it commits, once, as "MERGE".
    /// </summary>
    [Fact]
    public async Task EffectPreconditionAndDeclaration_ComposeInOneCommit()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.WriteAsync([Batch(1, 3)]);

        await using var host = await OpenAsync();
        var txn = host.StartTransaction(IsolationLevel.WriteSerializable);
        await txn.WriteAsync([Batch(50, 1)]);                    // effect
        txn.DeclareRead(Ex.GreaterThan("id", 1000L));            // declaration
        txn.RequireAppTransaction("producer", version: 1);       // precondition
        txn.Operation = "MERGE";

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(2000, 1)]);            // matches the declaration — blind, so exempt

        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 2, 3, 50, 2000 }, await ReadIdsFreshAsync());
        Assert.Equal(1, await RecordedVersionAsync("producer"));
        Assert.Equal("MERGE", await LastOperationAsync());
    }
}
