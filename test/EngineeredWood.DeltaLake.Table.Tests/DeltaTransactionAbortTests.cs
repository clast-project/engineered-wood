// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A transaction writes its data files at STAGE time, so one that is refused, aborts, or is simply dropped
/// has already put parquet, deletion vectors and change files on storage. These tests pin what
/// <see cref="DeltaTransaction.AbortAsync"/> and disposal take back — and, more importantly, what they must
/// NOT: a deletion-vector delete's re-add names a LIVE data file, and a host-staged add names a file the host
/// wrote. Deleting either would destroy committed data.
/// </summary>
public class DeltaTransactionAbortTests : IDisposable
{
    private readonly string _tempDir;

    public DeltaTransactionAbortTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_abort_{Guid.NewGuid():N}");
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

    private static RecordBatch Batch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private static RecordBatch Range(long fromInclusive, long toInclusive)
    {
        var ids = new List<long>();
        for (long i = fromInclusive; i <= toInclusive; i++)
            ids.Add(i);
        return Batch([.. ids]);
    }

    private static Func<RecordBatch, BooleanArray> IdEquals(long target) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) == target);
        return mask.Build();
    };

    private static Func<RecordBatch, BooleanArray> IdAtMost(long limit) => batch =>
    {
        var id = (Int64Array)batch.Column("id");
        var mask = new BooleanArray.Builder();
        for (int i = 0; i < id.Length; i++)
            mask.Append(id.GetValue(i) <= limit);
        return mask.Build();
    };

    /// <summary>The table's DATA files — parquet outside the log and the change-data directory.</summary>
    private string[] DataFiles() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal)
                        && !p.Contains("_change_data", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)];

    /// <summary>The on-disk deletion vectors. A vector under the inline threshold has no file, which is why
    /// the tests that count these delete enough rows to push the bitmap over it.</summary>
    private string[] DeletionVectorFiles() =>
        [.. Directory.GetFiles(_tempDir, "deletion_vector_*.bin", SearchOption.AllDirectories)];

    private string[] ChangeDataFiles()
    {
        string dir = Path.Combine(_tempDir, "_change_data");
        return Directory.Exists(dir) ? Directory.GetFiles(dir, "*.parquet") : [];
    }

    private static async Task<List<long>> ReadIds(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }

        ids.Sort();
        return ids;
    }

    // ── What an abort collects ──

    /// <summary>
    /// A staged append's parquet is on storage before the commit. An abort deletes it, and leaves the table
    /// exactly where it was — same version, same rows.
    /// </summary>
    [Fact]
    public async Task AbortAsync_DeletesTheParquetAStagedAppendWrote()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);
        string[] before = DataFiles();
        long version = table.CurrentSnapshot.Version;

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(2), Batch(3)]);
        Assert.Equal(before.Length + 2, DataFiles().Length); // written, not committed

        await txn.AbortAsync();

        Assert.Equal(before, DataFiles());
        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// An UPDATE rewrites the files it touches: the post-image is a NEW file, and the source is the table's
    /// current data. An abort must delete the first and keep the second — deleting the source would take the
    /// rows with it, since the commit that was to replace it never happened.
    /// </summary>
    [Fact]
    public async Task AbortAsync_DeletesTheRewriteAndKeepsWhatItWouldHaveReplaced()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1, 2, 3)]);
        string[] before = DataFiles();
        Assert.Single(before);

        var txn = table.StartTransaction();
        long updated = await txn.UpdateAsync(IdEquals(2), b => Batch(20));
        Assert.Equal(1, updated);
        Assert.Equal(2, DataFiles().Length); // source + post-image

        await txn.AbortAsync();

        Assert.Equal(before, DataFiles()); // the source survived; only the post-image went
        Assert.Equal([1L, 2L, 3L], await ReadIds(table));
    }

    /// <summary>
    /// The case the naive implementation gets catastrophically wrong. A deletion-vector DELETE stages an
    /// <c>add</c> naming an EXISTING data file, re-added with a new vector. Deleting "every staged add's path"
    /// on abort would destroy that file — live table data — so the abort takes only the <c>.bin</c>.
    /// </summary>
    [Fact]
    public async Task AbortAsync_DeletesTheVectorAndNotTheFileItMasks()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Range(1, 1000)]);
        string[] before = DataFiles();
        Assert.Single(before);
        Assert.Empty(DeletionVectorFiles());

        var txn = table.StartTransaction();
        // Enough rows that the roaring bitmap exceeds the inline threshold and becomes a file of its own.
        long deleted = await txn.DeleteAsync(IdAtMost(800));
        Assert.Equal(800, deleted);
        Assert.Single(DeletionVectorFiles());

        await txn.AbortAsync();

        Assert.Empty(DeletionVectorFiles());
        Assert.Equal(before, DataFiles()); // the file the vector masked is untouched
        Assert.Equal(1000, (await ReadIds(table)).Count);
    }

    /// <summary>
    /// A staged DELETE on a Change Data Feed table writes <c>_change_data</c> files too. They are as
    /// uncommitted as the vector, and vacuum does not even sweep that directory — so an abort has to collect
    /// them itself or they are permanent.
    /// </summary>
    [Fact]
    public async Task AbortAsync_DeletesTheChangeDataFilesTheDeleteWrote()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, IdSchema, enableDeletionVectors: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Batch(1, 2, 3)]);

        var txn = table.StartTransaction();
        await txn.DeleteAsync(IdEquals(2));
        Assert.NotEmpty(ChangeDataFiles());

        await txn.AbortAsync();

        Assert.Empty(ChangeDataFiles());
        Assert.Equal([1L, 2L, 3L], await ReadIds(table));
    }

    /// <summary>
    /// Files handed to <see cref="DeltaTransaction.StageDataFiles"/> were written by the HOST before the
    /// transaction ever saw them. An abort of the transaction they were staged on does not delete them: their
    /// lifetime is the host's to decide, and the transaction cannot know whether the host means to stage them
    /// again.
    /// </summary>
    [Fact]
    public async Task AbortAsync_LeavesHostStagedFilesWhereTheHostPutThem()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);

        var files = await table.WriteDataFilesAsync([Batch(2)]);
        Assert.Single(files);
        string hostFile = Path.Combine(_tempDir, files[0].RelativePath);
        Assert.True(File.Exists(hostFile));

        var txn = table.StartTransaction();
        txn.StageDataFiles(files);
        await txn.AbortAsync();

        Assert.True(File.Exists(hostFile), "an aborted transaction deleted a file the host wrote");
        Assert.Equal([1L], await ReadIds(table)); // still uncommitted, as the abort intends
    }

    /// <summary>
    /// The hazard in collecting superseded vectors at a rebase: a transaction can hold a vector that is NOT
    /// superseded. A born-deleted vector belongs to a staged append's add, which the row-level resolution
    /// preserves untouched — so it is still needed by the very commit that lands, even though it was written
    /// by the same transaction whose OTHER vector is being replaced. Deleting it would leave a committed add
    /// naming a vector that does not exist, and every row it hides would come back.
    /// </summary>
    [Fact]
    public async Task ARebasedDelete_KeepsACoStagedBornDeletedVector()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Range(1, 1000)]); // the file the delete below rebases on
        Assert.Empty(DeletionVectorFiles());

        var txn = table.StartTransaction();

        // (a) a host-written append whose rows are born deleted — enough of them to need a vector FILE.
        var appended = await table.WriteDataFilesAsync([Range(2001, 3000)]);
        var bornDeleted = RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
        {
            [appended[0].RelativePath] = [.. Enumerable.Range(0, 600).Select(i => (long)i)],
        });
        await txn.StageDataFilesAsync(appended, bornDeleted);

        // (b) a row-level delete on the existing file, which the concurrent commit below forces to rebase.
        await txn.DeleteAsync(IdAtMost(800));
        Assert.Equal(2, DeletionVectorFiles().Length); // born-deleted + the delete's first attempt

        await using (var other = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
            await other.DeleteAsync(IdEquals(1000)); // a different row — reconciles, so our commit LANDS

        await txn.CommitAsync();

        // The delete's superseded vector went; the born-deleted one — which the commit still names — did not.
        Assert.Equal(2, DeletionVectorFiles().Length); // born-deleted + the union
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var ids = await ReadIds(reopened);
        Assert.Equal(199, ids.Count(i => i <= 1000));   // 801..999 survive the delete
        Assert.Equal(400, ids.Count(i => i >= 2001));   // 600 of the 1000 appended were born deleted
    }

    // ── The paths that reach an abort in practice ──

    /// <summary>
    /// The motivating shape: a conflict aborts the commit AFTER the transaction's files are written. The
    /// loser's append parquet is on storage when <see cref="DeltaTransaction.CommitAsync"/> throws, and the
    /// abort that follows takes it back.
    /// </summary>
    [Fact]
    public async Task AbortAsync_AfterAConflict_TakesBackTheLosersFiles()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(5, 7)]);
        string[] before = DataFiles();

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();

        await tx2.DeleteAsync(IdEquals(7));
        await tx2.CommitAsync();

        await tx1.WriteAsync([Batch(9)]);      // parquet on storage
        await tx1.DeleteAsync(IdEquals(7));    // the row tx2 just removed — a row-level conflict
        await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx1.CommitAsync());
        Assert.Equal(before.Length + 1, DataFiles().Length);

        await tx1.AbortAsync();

        Assert.Equal(before, DataFiles());
        Assert.Equal([5L], await ReadIds(table));
    }

    /// <summary>
    /// A refused app-transaction precondition — routine for a replaying producer, not an error — leaves the
    /// batch's parquet behind exactly as a conflict does. Aborting collects it, which is what keeps a
    /// crash-looping producer from writing a full batch of orphans per restart.
    /// </summary>
    [Fact]
    public async Task AbortAsync_AfterARefusedPrecondition_DeletesTheBatch()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);

        var first = table.StartTransaction();
        await first.WriteAsync([Batch(1)]);
        first.RequireAppTransaction("producer", 1, AppTransactionPrecondition.Absent);
        await first.CommitAsync();
        string[] before = DataFiles();

        // The replay: the same batch again, guarded so it cannot land twice.
        var replay = table.StartTransaction();
        await replay.WriteAsync([Batch(1)]);
        replay.RequireAppTransaction("producer", 1, AppTransactionPrecondition.NotApplied);
        Assert.Equal(before.Length + 1, DataFiles().Length);

        await Assert.ThrowsAsync<AppTransactionPreconditionException>(
            async () => await replay.CommitAsync());
        await replay.AbortAsync();

        Assert.Equal(before, DataFiles());
        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// The auto-committing <see cref="DeltaTable.DeleteAsync(Func{RecordBatch, BooleanArray}, CancellationToken)"/>
    /// builds a transaction internally, so it inherits the cleanup: a delete that loses at row level leaves no
    /// vector behind.
    /// </summary>
    [Fact]
    public async Task AutoCommittingDelete_ThatConflicts_LeavesNoOrphanedVector()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var winner = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await winner.WriteAsync([Range(1, 1000)]);

        // A second handle pinned to the pre-delete version, so its delete is computed against stale state.
        await using var loser = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        await winner.DeleteAsync(IdAtMost(800));
        string[] afterWinner = DeletionVectorFiles();
        Assert.Single(afterWinner);

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await loser.DeleteAsync(IdAtMost(800))); // the same rows — no row-level reconciliation

        Assert.Equal(afterWinner, DeletionVectorFiles());
    }

    // ── Disposal ──

    /// <summary>
    /// A transaction dropped without committing — the shape an exception path produces — is cleaned up by
    /// <c>await using</c>, with the host never naming the files.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_OfAnAbandonedTransaction_DeletesWhatItWrote()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);
        string[] before = DataFiles();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var txn = table.StartTransaction();
            await txn.WriteAsync([Batch(2)]);
            Assert.Equal(before.Length + 1, DataFiles().Length);
            throw new InvalidOperationException("the host's own failure, mid-transaction");
        });

        Assert.Equal(before, DataFiles());
        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// Disposal after a successful commit does nothing — those files are the table's data now. This is what
    /// makes <c>await using</c> safe to write unconditionally.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_AfterACommit_DeletesNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);

        long version;
        await using (var txn = table.StartTransaction())
        {
            await txn.WriteAsync([Batch(1, 2)]);
            version = await txn.CommitAsync();
        }

        Assert.Single(DataFiles());
        Assert.Equal(version, table.CurrentSnapshot.Version);
        Assert.Equal([1L, 2L], await ReadIds(table));
    }

    /// <summary>Abort is idempotent, and so is disposing after one.</summary>
    [Fact]
    public async Task AbortAsync_IsIdempotent()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);
        string[] before = DataFiles();

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(2)]);
        await txn.AbortAsync();
        await txn.AbortAsync();
        await txn.DisposeAsync();

        Assert.Equal(before, DataFiles());
    }

    /// <summary>
    /// An aborted transaction is closed. Committing it afterwards would publish <c>add</c> actions naming
    /// files that no longer exist — a table readers cannot open — so it is refused instead.
    /// </summary>
    [Fact]
    public async Task AfterAbort_StagingAndCommittingBothThrow()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(2)]);
        await txn.AbortAsync();

        var commit = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await txn.CommitAsync());
        Assert.Contains("aborted", commit.Message, StringComparison.OrdinalIgnoreCase);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await txn.WriteAsync([Batch(3)]));

        Assert.Equal([1L], await ReadIds(table));
    }

    /// <summary>
    /// The window a commit cannot retry out of: the version JSON is durable, and the writer's own post-commit
    /// work then fails. `CommitAsync` throws on a commit that LANDED — so a cleanup keyed to "the commit
    /// failed" would be holding files a committed <c>add</c> references, and disposing the transaction would
    /// delete live data. Cancellation reaches the same state with no fault injection: a token cancelled
    /// between the commit write and the snapshot refresh.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_AfterACommitThatLandedButThrew_DeletesNothing()
    {
        var fs = new FailAfterCommitFileSystem(new LocalTableFileSystem(_tempDir));
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        fs.Armed = true; // CreateAsync commits version 0 and reads the log the same way — arm after it

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await using var txn = table.StartTransaction();
            await txn.WriteAsync([Batch(1, 2)]);
            await txn.CommitAsync(); // the commit lands; the snapshot refresh that follows does not
        });
        Assert.True(fs.Committed, "the test needs the commit to have actually landed");

        // Reopened on a healthy filesystem: the version is there, and so are the rows it names.
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Single(DataFiles());
        Assert.Equal([1L, 2L], await ReadIds(reopened));
    }

    /// <summary>
    /// A host aborting on a cancellation path naturally passes the token that was just cancelled. Honouring it
    /// would make every delete fail — and they are swallowed, and the ledger emptied — so the abort would
    /// collect nothing while reporting success. An already-cancelled token cleans up anyway.
    /// </summary>
    [Fact]
    public async Task AbortAsync_WithAnAlreadyCancelledToken_StillCleansUp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        await table.WriteAsync([Batch(1)]);
        string[] before = DataFiles();

        var txn = table.StartTransaction();
        await txn.WriteAsync([Batch(2)]);
        Assert.Equal(before.Length + 1, DataFiles().Length);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await txn.AbortAsync(cts.Token);

        Assert.Equal(before, DataFiles());
    }

    /// <summary>
    /// A commit that THROWS closes the transaction too: a transaction is committed at most once. The error
    /// says that rather than naming a failure mode — a conflict, a refused precondition, an I/O error and a
    /// cancellation all end up here.
    /// </summary>
    [Fact]
    public async Task AfterAFailedCommit_ReCommittingThrowsAndSaysWhy()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(5, 7)]);

        var tx1 = table.StartTransaction();
        var tx2 = table.StartTransaction();
        await tx2.DeleteAsync(IdEquals(7));
        await tx2.CommitAsync();

        await tx1.DeleteAsync(IdEquals(7));
        await Assert.ThrowsAsync<DeltaConflictException>(async () => await tx1.CommitAsync());

        var again = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await tx1.CommitAsync());
        Assert.Contains("did not succeed", again.Message, StringComparison.Ordinal);
    }
}
