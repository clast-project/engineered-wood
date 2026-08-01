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

    // ── requireAbsent: the precondition a FIRST batch needs ──
    //
    // The test above states the rule this group exists for: an omitted expectedPrevious is "do not check",
    // so it guards nothing. That is fine for batch N, which names N-1 — and leaves batch 1 with NO guard
    // available, since a replay of it has no prior version to name. requireAbsent is that guard.

    /// <summary>A first batch commits: nothing is recorded for the producer, which is what it asserted.</summary>
    [Fact]
    public async Task RequireAppTransaction_RequireAbsent_FirstBatchCommits()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(1, 2)]);
        txn.RequireAppTransaction("producer", version: 1, requireAbsent: true);
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 2 }, await ReadIdsFreshAsync());
        Assert.Equal(1, await RecordedVersionAsync("producer"));
    }

    /// <summary>
    /// THE CASE THE FLAG EXISTS FOR, and the reason it is not a convenience. A replayed FIRST batch — the
    /// ordinary consequence of a crash and restart — is refused instead of committing a second copy.
    ///
    /// <para>Note what the table looks like afterwards if it is NOT refused: the rows are duplicated and the
    /// recorded version is rewritten with the SAME value, so nothing in the table says a batch was applied
    /// twice. That is why an unguarded first batch is worth a parameter.</para>
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_RequireAbsent_ReplayedFirstBatch_IsRefused()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var first = table.StartTransaction();
        await first.WriteAsync([Batch(1, 2)]);
        first.RequireAppTransaction("producer", version: 1, requireAbsent: true);
        await first.CommitAsync();
        long versionAfterFirst = table.CurrentSnapshot.Version;

        await using var reopened = await OpenAsync();
        var replay = reopened.StartTransaction();
        await replay.WriteAsync([Batch(1, 2)]);   // the identical batch, identically guarded
        replay.RequireAppTransaction("producer", version: 1, requireAbsent: true);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await replay.CommitAsync());
        Assert.IsNotType<DeltaConflictException>(ex);   // not retryable — retrying cannot un-commit batch 1
        Assert.Contains("producer", ex.Message);
        Assert.Contains("expected the table to record no transaction at all", ex.Message);
        Assert.Contains("records 1", ex.Message);

        // Nothing landed, and the version did not move.
        Assert.Equal(new long[] { 1, 2 }, await ReadIdsFreshAsync());
        Assert.Equal(versionAfterFirst, table.CurrentSnapshot.Version);
    }

    /// <summary>
    /// Re-checked per attempt like the other precondition, and this is the shape that needs it: a TWIN
    /// producer running the same first batch commits while this transaction is open. Ours then loses the
    /// optimistic race and the loop retries — and on that retry the read-set check has nothing to object to,
    /// because a concurrent APPEND invalidates no read of ours. Only the re-checked precondition stops the
    /// batch landing twice.
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_RequireAbsent_IsRecheckedAgainstConcurrentCommits()
    {
        await using var created = await DeltaTable.CreateAsync(Fs, IdSchema);
        var pinned = created.CurrentSnapshot;

        await using var mine = await OpenAsync();
        var txn = mine.StartTransaction(pinned);
        await txn.WriteAsync([Batch(1, 2)]);
        txn.RequireAppTransaction("producer", version: 1, requireAbsent: true);

        await using (var twin = await OpenAsync())
        {
            var twinTxn = twin.StartTransaction();
            await twinTxn.WriteAsync([Batch(1, 2)]);
            twinTxn.RequireAppTransaction("producer", version: 1, requireAbsent: true);
            await twinTxn.CommitAsync();
        }

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await txn.CommitAsync());
        Assert.Contains("producer", ex.Message);

        // Exactly ONE copy of the batch is in the table.
        Assert.Equal(new long[] { 1, 2 }, await ReadIdsFreshAsync());
        Assert.Equal(1, await RecordedVersionAsync("producer"));
    }

    /// <summary>
    /// The two preconditions contradict each other, so saying both is rejected rather than resolved by a
    /// precedence rule — which would silently make one of them a no-op at the call site that asked for a
    /// guard. Rejected at the CALL, so a transaction that was going to be wrong never gets built.
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_RequireAbsentWithExpectedPrevious_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        var txn = table.StartTransaction();

        var ex = Assert.Throws<ArgumentException>(() =>
            txn.RequireAppTransaction("producer", version: 7, expectedPrevious: 3, requireAbsent: true));
        Assert.Contains("producer", ex.Message);
        Assert.Contains("mutually exclusive", ex.Message);
    }

    /// <summary>
    /// A sentinel value could not have served instead: the recorded version is the APPLICATION's own counter,
    /// not a table version, and nothing constrains its sign — so a negative value is a legitimate record and
    /// reserving one would reinterpret a real caller's guard. Pinned because it is the argument for the flag
    /// existing at all, and it is a property of the action model rather than of this method.
    /// </summary>
    [Fact]
    public async Task RequireAppTransaction_ANegativeVersion_IsALegitimateRecord()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(1, 1)]);
        txn.RequireAppTransaction("producer", version: -1);
        await txn.CommitAsync();

        Assert.Equal(-1, await RecordedVersionAsync("producer"));
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

    // ── DeclareFilesRead: the middle ground ──

    /// <summary>
    /// A table of two files, so "the files I read" and "the whole table" can differ: ids 1..3 in one,
    /// 100..105 in another. Returns a table already holding both.
    /// </summary>
    private async Task<DeltaTable> TwoFileTableAsync()
    {
        var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 3)]);
        await table.WriteAsync([Batch(100, 6)]);
        return table;
    }

    /// <summary>The path of the ONE file holding the low ids, planned exactly as a host would plan it —
    /// through <see cref="DeltaTable.PlanFiles"/> against the transaction's own snapshot. Asserting a single
    /// survivor keeps a pruning regression from quietly turning these tests into whole-table ones.</summary>
    private static string LowIdFile(DeltaTable table, DeltaTransaction txn)
    {
        var planned = table.PlanFiles(Ex.LessThan("id", 50L), snapshot: txn.Snapshot);
        return Assert.Single(planned).File.Path;
    }

    /// <summary>
    /// The declaration's whole reason to exist: a concurrent delete of a file the host DID read invalidates
    /// what it decided, exactly as whole-table would have said — but see the next test for what it no longer
    /// says.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_ConcurrentDeleteOfADeclaredFile_Conflicts()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        txn.DeclareFilesRead([LowIdFile(host, txn)]);
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 2L)); // in the declared file

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>
    /// The narrowing. The same interleaving, the same racer — a delete of a file the host never read — and
    /// the file-level declaration lets it through where whole-table (today's only option, next test) aborts.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_ConcurrentDeleteElsewhere_DoesNotConflict()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        txn.DeclareFilesRead([LowIdFile(host, txn)]);
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 101L)); // the OTHER file — not declared

        await txn.CommitAsync();
        Assert.Equal(
            new long[] { 1, 2, 3, 100, 102, 103, 104, 105, 1000 }, await ReadIdsFreshAsync());
    }

    /// <summary>The same racer against the only declaration a host could make before this: whole-table
    /// aborts on a delete of a file it never read.</summary>
    [Fact]
    public async Task DeclareWholeTableRead_TheSameConcurrentDeleteElsewhere_Conflicts()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        txn.DeclareWholeTableRead();
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 101L));

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>
    /// The implementation trap, made observable: the declared paths must widen the READ set and not the
    /// planned-remove set. This transaction removes nothing, so a concurrent delete of a file it merely read
    /// is <c>concurrentDeleteRead</c>. Were the two sets shared, the delete/delete branch would fire first
    /// and diagnose a removal this transaction never planned.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_DoesNotMakeAReadFileLookLikeAPlannedRemove()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        txn.DeclareFilesRead([LowIdFile(host, txn)]);
        await txn.WriteAsync([Batch(1000, 1)]); // an append — this transaction removes NOTHING

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 2L));

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await txn.CommitAsync());
        Assert.Contains("which this transaction read", conflict.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("also removes", conflict.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Stated up front rather than discovered: a file-level declaration says nothing about concurrent ADDS,
    /// at either level. A file that did not exist when the host scanned was not in its read set. Serializable
    /// here, so the verdict is not the isolation level's blind-append exemption doing the work.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_ConcurrentAppend_DoesNotConflict()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction(IsolationLevel.Serializable);
        txn.DeclareFilesRead([LowIdFile(host, txn)]);
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(2, 1)]); // an id the declared file's range covers

        await txn.CommitAsync();
    }

    /// <summary>
    /// The two declarations compose rather than compete: files for what was read, a predicate for what WOULD
    /// have been read. A host that cares about phantom rows needs both, and this is the pair that gets it —
    /// same transaction, same racer as the test above, opposite verdict.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_ComposesWithDeclareRead_ForThePhantomAppend()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction(IsolationLevel.Serializable);
        txn.DeclareFilesRead([LowIdFile(host, txn)]);   // what it read
        txn.DeclareRead(Ex.LessThan("id", 50L));        // what it would have read
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.WriteAsync([Batch(2, 1)]);      // matches the predicate

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>
    /// Whole-table is strictly stronger, so calling both KEEPS whole-table rather than erroring: everything
    /// the paths would catch, it already catches. Either call order, since neither is "the last word".
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DeclareFilesRead_WithWholeTableRead_KeepsWholeTable(bool wholeTableFirst)
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        string low = LowIdFile(host, txn);
        if (wholeTableFirst)
        {
            txn.DeclareWholeTableRead();
            txn.DeclareFilesRead([low]);
        }
        else
        {
            txn.DeclareFilesRead([low]);
            txn.DeclareWholeTableRead();
        }
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 101L)); // outside the declared files, inside the table

        await Assert.ThrowsAsync<DeltaConflictException>(async () => await txn.CommitAsync());
    }

    /// <summary>
    /// A stale PATH is detectable — unlike a stale ordinal — so it is reported rather than absorbed: a path
    /// no version-N file carries can never match a concurrent remove, so the declaration built from it would
    /// silently protect nothing.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_PathNotInTheBaseSnapshot_Throws()
    {
        await using var table = await TwoFileTableAsync();
        var txn = table.StartTransaction();

        var ex = Assert.Throws<ArgumentException>(() => txn.DeclareFilesRead(["part-nope.parquet"]));
        Assert.Contains("not an active file", ex.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() => txn.DeclareFilesRead([""]));
        Assert.Throws<ArgumentNullException>(() => txn.DeclareFilesRead(null!));
    }

    /// <summary>Validate-then-apply: a list with one bad path declares NOTHING, so the transaction is left
    /// as it was rather than half-declared. Proven by the racer a real declaration would have caught.</summary>
    [Fact]
    public async Task DeclareFilesRead_RejectedList_DeclaresNothing()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        string low = LowIdFile(host, txn);

        Assert.Throws<ArgumentException>(() => txn.DeclareFilesRead([low, "part-nope.parquet"]));
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 2L)); // in `low` — would conflict had it been declared

        await txn.CommitAsync();
    }

    /// <summary>Nothing to declare declares nothing, and an already-committed transaction cannot be told
    /// anything at all.</summary>
    [Fact]
    public async Task DeclareFilesRead_EmptyIsANoOp_AndCommittedIsAnError()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        var txn = table.StartTransaction();
        txn.DeclareFilesRead([]);
        await txn.WriteAsync([Batch(1, 1)]);
        await txn.CommitAsync();

        Assert.Throws<InvalidOperationException>(() => txn.DeclareFilesRead(["whatever.parquet"]));
    }

    /// <summary>
    /// The composition the guide shows, end to end: plan against the transaction's snapshot, declare exactly
    /// what planning returned. Declaring EVERY file is whole-table by another name — and the abort is then
    /// correct, since the host really did read it all.
    /// </summary>
    [Fact]
    public async Task DeclareFilesRead_OfEveryPlannedFile_IsWholeTableByAnotherName()
    {
        await using var table = await TwoFileTableAsync();

        await using var host = await OpenAsync();
        var txn = host.StartTransaction();
        var planned = host.PlanFiles(snapshot: txn.Snapshot);
        Assert.Equal(2, planned.Count);
        txn.DeclareFilesRead(planned.Select(p => p.File.Path).ToList());
        await txn.WriteAsync([Batch(1000, 1)]);

        await using (var racer = await OpenAsync())
            await racer.DeleteAsync(Ex.Equal("id", 101L));

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
