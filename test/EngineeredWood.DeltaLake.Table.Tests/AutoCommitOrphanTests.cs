// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The auto-committing surface writes its files BEFORE it attempts the commit, and — unlike
/// <see cref="DeltaTransaction"/> — has no object a host could abort. These tests pin that each such
/// operation collects its own output when the commit does not land, and (the half that matters more) that it
/// never collects a file the table still needs.
///
/// <para>Every conflict here is a real one produced by a second table handle racing the first, not an
/// injected failure: a stale handle's overwrite collides, its blind append meets a concurrent schema change,
/// and the two <c>rebaseSafe:false</c> row-level rewrites lose to ANY concurrent commit at all.</para>
/// </summary>
public class AutoCommitOrphanTests : IDisposable
{
    private readonly string _tempDir;

    public AutoCommitOrphanTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_autocommit_{Guid.NewGuid():N}");
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

    private string[] DataFiles() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal)
                        && !p.Contains("_change_data", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)];

    private string[] DeletionVectorFiles() =>
        [.. Directory.GetFiles(_tempDir, "deletion_vector_*.bin", SearchOption.AllDirectories)];

    private DeltaTable OpenSecondHandle() => DeltaTable.OpenAsync(
        new LocalTableFileSystem(_tempDir)).AsTask().GetAwaiter().GetResult();

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

    private async Task<List<long>> RowAddressesOfAsync(DeltaTable table, params long[] ids)
    {
        var wanted = new HashSet<long>(ids);
        var result = new List<long>();
        await foreach (var b in table.ReadAsync(
            new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var id = (Int64Array)b.Column("id");
            var addr = (Int64Array)b.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < b.Length; i++)
                if (wanted.Contains(id.GetValue(i)!.Value))
                    result.Add(addr.GetValue(i)!.Value);
        }
        return result;
    }

    // ── The append/overwrite family ──

    /// <summary>
    /// A blind append rebases past almost everything, but a concurrent SCHEMA change conflicts
    /// unconditionally — and by then its parquet is written. The operation takes it back.
    /// </summary>
    [Fact]
    public async Task Append_ThatConflictsOnAConcurrentSchemaChange_TakesBackItsParquet()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        await stale.WriteAsync([Batch(1)]);
        string[] before = DataFiles();

        await using (var other = OpenSecondHandle())
            await other.AddColumnAsync(new Field("extra", Int64Type.Default, nullable: true));

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.WriteAsync([Batch(2)]));

        Assert.Equal(before, DataFiles());
    }

    /// <summary>
    /// The overwrite family makes ONE commit attempt at the read version + 1, so any collision at all defeats
    /// it — after the replacement data is already on storage.
    /// </summary>
    [Fact]
    public async Task Overwrite_ThatCollides_TakesBackItsParquetAndKeepsTheOldData()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        await stale.WriteAsync([Batch(1, 2)]);
        string[] before = DataFiles();

        await using (var other = OpenSecondHandle())
            await other.WriteAsync([Batch(3)]); // takes the version the overwrite is aiming at

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.WriteAsync([Batch(99)], DeltaWriteMode.Overwrite));

        // The overwrite's own file is gone; every file it would have REMOVED is untouched, because the
        // commit that was to remove them never landed.
        Assert.Equal(before, DataFiles().Where(before.Contains).ToArray());
        Assert.Equal(before.Length + 1, DataFiles().Length); // only the concurrent writer's file was added
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([1L, 2L, 3L], await ReadIds(reopened));
    }

    // ── Copy-on-write UPDATE ──

    /// <summary>
    /// An UPDATE rewrites the files it touches, then conflicts because a concurrent commit deleted from one
    /// of them. The post-image goes; the source it would have replaced stays, because the commit that was to
    /// replace it never happened.
    /// </summary>
    [Fact]
    public async Task Update_ThatConflicts_TakesBackTheRewriteAndKeepsTheSource()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await stale.WriteAsync([Batch(1, 2, 3)]);
        string[] before = DataFiles();
        Assert.Single(before);

        await using (var other = OpenSecondHandle())
            await other.DeleteAsync(IdEquals(3)); // a dataChange remove of the file the UPDATE reads

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.UpdateAsync(IdEquals(2), _ => Batch(20)));

        Assert.Equal(before, DataFiles());
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([1L, 2L], await ReadIds(reopened)); // only the concurrent delete took effect
    }

    // ── The row-level DML surface ──

    /// <summary>
    /// A deletion-vector <see cref="DeltaTable.DeleteRowsAsync"/> writes its <c>.bin</c> before committing.
    /// A concurrent delete of the same file is a delete/delete conflict; the vector is collected, and the
    /// data file it masked — live table data — is not.
    /// </summary>
    [Fact]
    public async Task DeleteRows_DeletionVector_ThatConflicts_TakesBackTheVectorNotTheData()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await stale.WriteAsync([Range(1, 1000)]);
        string[] beforeData = DataFiles();
        var doomed = RowSelection.FromRowAddresses(
            await RowAddressesOfAsync(stale, [.. Enumerable.Range(1, 800).Select(i => (long)i)]),
            stale.CurrentSnapshot);

        await using (var other = OpenSecondHandle())
            await other.DeleteAsync(IdEquals(1000));
        string[] afterOther = DeletionVectorFiles();

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.DeleteRowsAsync(doomed));

        Assert.Equal(afterOther, DeletionVectorFiles()); // ours went, the winner's stayed
        Assert.Equal(beforeData, DataFiles());
    }

    /// <summary>
    /// The copy-on-write delete is <c>rebaseSafe:false</c>: ANY concurrent commit defeats it, not only a
    /// conflicting one — so its orphan rate used to be the contention rate.
    /// </summary>
    [Fact]
    public async Task DeleteRows_CopyOnWrite_ThatLosesTheRace_TakesBackTheRewrite()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        await stale.WriteAsync([Batch(1, 2, 3)]);
        string[] before = DataFiles();
        var doomed = RowSelection.FromRowAddresses(
            await RowAddressesOfAsync(stale, 2), stale.CurrentSnapshot);

        await using (var other = OpenSecondHandle())
            await other.WriteAsync([Batch(4)]); // a plain append is enough

        await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.DeleteRowsAsync(doomed, RowDeleteMode.CopyOnWrite));

        Assert.Equal(before, DataFiles().Where(before.Contains).ToArray());
        Assert.Equal(before.Length + 1, DataFiles().Length); // only the concurrent append's file is new
    }

    /// <summary>The copy-on-write UPDATE, same shape: <c>rebaseSafe:false</c>, whole rewrite discarded.</summary>
    [Fact]
    public async Task UpdateRows_ThatLosesTheRace_TakesBackTheRewrite()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        await stale.WriteAsync([Batch(1, 2, 3)]);
        string[] before = DataFiles();
        var target = RowSelection.FromRowAddresses(
            await RowAddressesOfAsync(stale, 2), stale.CurrentSnapshot);

        await using (var other = OpenSecondHandle())
            await other.WriteAsync([Batch(4)]);

        await Assert.ThrowsAsync<DeltaConflictException>(async () =>
            await stale.UpdateRowsAsync(target, (_, batches, _) =>
            {
                var outp = new List<RecordBatch>(batches.Count);
                foreach (var b in batches)
                {
                    var id = (Int64Array)b.Column("id");
                    var builder = new Int64Array.Builder();
                    for (int i = 0; i < b.Length; i++)
                        builder.Append(id.GetValue(i)!.Value * 10);
                    outp.Add(new RecordBatch(IdSchema, [builder.Build()], b.Length));
                }
                return outp;
            }));

        Assert.Equal(before, DataFiles().Where(before.Contains).ToArray());
        Assert.Equal(before.Length + 1, DataFiles().Length);
    }

    /// <summary>
    /// The case where the operation SUCCEEDS and still leaves garbage. A row-level delete that loses a race
    /// rebases its vectors onto the winner and retries — writing a fresh <c>.bin</c> per attempt, of which
    /// only the last is ever referenced. The commit that lands empties the ledger wholesale (rightly, to
    /// protect its own files), so the losing attempts' vectors have to be collected at the rebase, which is
    /// the only moment anything knows they are superseded.
    /// </summary>
    [Fact]
    public async Task DeleteRows_ThatRebasesAndLands_LeavesNoSupersededVector()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema, enableDeletionVectors: true);
        await stale.WriteAsync([Range(1, 1000)]);
        // Enough rows that each attempt's bitmap exceeds the inline threshold and becomes a file.
        var doomed = RowSelection.FromRowAddresses(
            await RowAddressesOfAsync(stale, [.. Enumerable.Range(1, 800).Select(i => (long)i)]),
            stale.CurrentSnapshot);

        // A concurrent delete of a DIFFERENT row: reconcilable row by row, so ours rebases and LANDS rather
        // than aborting — which is what makes this the succeeded-with-garbage case.
        await using (var other = OpenSecondHandle())
            await other.DeleteAsync(IdEquals(1000));

        var (deleted, _) = await stale.DeleteRowsAsync(
            doomed, RowDeleteMode.DeletionVector, rowLevelRetry: true);
        Assert.Equal(800, deleted);

        // Exactly one on-disk vector: the union the committed version names. (The concurrent delete's single
        // row fits inline, so it contributes no file of its own.)
        Assert.Single(DeletionVectorFiles());

        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal(199, (await ReadIds(reopened)).Count); // 801..999
    }

    // ── Compaction: the one with the most to lose ──

    /// <summary>
    /// OPTIMIZE rewrites its whole candidate set before it commits, so an aborted commit orphans every
    /// compacted file — potentially the whole table's worth of bytes.
    ///
    /// <para>The conflict has to be a REAL one now (#109): a concurrent commit that merely took the
    /// version no longer defeats OPTIMIZE, it rebases past it. So the racing handle deletes one of the six
    /// candidates, which is the case that genuinely invalidates the rewrite — the compacted file was built
    /// from rows that are no longer in the table — and reaches this commit as delete/delete on a path it
    /// also removes.</para>
    /// </summary>
    [Fact]
    public async Task Compact_ThatConflicts_TakesBackEveryCompactedFile()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var stale = await DeltaTable.CreateAsync(fs, IdSchema);
        for (long i = 1; i <= 6; i++)
            await stale.WriteAsync([Batch(i)]); // six small files, all compaction candidates
        string[] before = DataFiles();
        Assert.Equal(6, before.Length);

        // Copy-on-write, so the file holding id 1 — one of the six — is REMOVED at a version the stale
        // handle has not seen.
        await using (var other = OpenSecondHandle())
            await other.DeleteAsync(IdEquals(1));

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(async () => await stale.CompactAsync(
            new CompactionOptions { TargetFileSize = 1 << 20, MinFileSize = 1 << 20 }));
        Assert.Equal(DeltaErrorCodes.ConcurrentDeleteDelete, ex.ErrorCode);

        // The compacted output is gone, and every source file it was going to replace is still on disk —
        // including the one the concurrent delete de-referenced, which is VACUUM's to collect, not ours.
        Assert.Equal(before, DataFiles());

        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([2L, 3L, 4L, 5L, 6L], await ReadIds(reopened));
    }

    /// <summary>
    /// The other half of every test above: a SUCCESSFUL operation must not collect anything. A compaction
    /// that lands keeps its output — the cleanup is keyed to the commit not landing, not to the operation
    /// finishing.
    /// </summary>
    [Fact]
    public async Task Compact_ThatLands_KeepsItsOutput()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, IdSchema);
        for (long i = 1; i <= 6; i++)
            await table.WriteAsync([Batch(i)]);

        long? version = await table.CompactAsync(
            new CompactionOptions { TargetFileSize = 1 << 20, MinFileSize = 1 << 20 });

        Assert.NotNull(version);
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], await ReadIds(table));
        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], await ReadIds(reopened));
    }
}
