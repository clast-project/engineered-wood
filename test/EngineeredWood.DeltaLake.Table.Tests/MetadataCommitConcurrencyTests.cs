// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Optimistic concurrency for the five commit paths that used to have none (#109): OPTIMIZE, the
/// metadata-only schema changes, clustering, and the two domain-metadata methods. Each computed
/// <c>snapshot.Version + 1</c>, made ONE <c>TransactionLog.WriteCommitAsync</c> attempt, and propagated
/// the collision — so a single concurrent append was enough to fail an ALTER TABLE or throw away an
/// OPTIMIZE that had already rewritten its whole candidate set.
///
/// <para>Concurrency is real, not simulated: two independent <see cref="DeltaTable"/> handles are opened
/// on the same directory and one commits while the other still holds an older snapshot. Every test is a
/// pair — the concurrent commit that is HARMLESS must now rebase and land, and the one that genuinely
/// invalidates the operation must still abort, with the error code that names why.</para>
/// </summary>
public class MetadataCommitConcurrencyTests : IDisposable
{
    private readonly string _tempDir;

    public MetadataCommitConcurrencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_metaocc_{Guid.NewGuid():N}");
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

    private static StructField NullableLong(string name) => new()
    {
        Name = name,
        Type = new PrimitiveType { TypeName = "long" },
        Nullable = true,
    };

    private Task<DeltaTable> OpenAsync() =>
        DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private async Task<List<long>> ReadIdsFresh()
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

    // ── The metadata-only schema changes (CommitMetadataOnlyAsync — seven public methods) ──

    /// <summary>
    /// An ALTER TABLE that loses the version race to an ordinary append rebases onto it and lands. The
    /// append is not a metadata change, so nothing the schema edit was derived from moved.
    /// </summary>
    [Fact]
    public async Task AddColumn_RebasesPastAConcurrentAppend_AndLands()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
            await setup.WriteAsync([Batch(1)]);

        await using var stale = await OpenAsync();
        long baseVersion = stale.CurrentSnapshot.Version;

        await using (var other = await OpenAsync())
            await other.WriteAsync([Batch(2)]); // takes baseVersion + 1

        long committed = await stale.AddColumnAsync(NullableLong("extra"));

        Assert.Equal(baseVersion + 2, committed); // landed one past the winner, not thrown
        await using var reopened = await OpenAsync();
        Assert.Contains(reopened.CurrentSnapshot.Schema.Fields, f => f.Name == "extra");
        Assert.Equal([1L, 2L], await ReadIdsFresh());
    }

    /// <summary>
    /// Two concurrent schema changes do NOT both land. The second was derived from a schema that has
    /// moved, so re-committing it would silently drop the first one's column.
    /// </summary>
    [Fact]
    public async Task AddColumn_AgainstAConcurrentSchemaChange_Conflicts()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
            await setup.WriteAsync([Batch(1)]);

        await using var stale = await OpenAsync();

        await using (var other = await OpenAsync())
            await other.AddColumnAsync(NullableLong("theirs"));

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.AddColumnAsync(NullableLong("mine")));
        Assert.Equal(DeltaErrorCodes.MetadataChanged, ex.ErrorCode);

        // First committer wins, whole: the loser's column is nowhere.
        await using var reopened = await OpenAsync();
        Assert.Contains(reopened.CurrentSnapshot.Schema.Fields, f => f.Name == "theirs");
        Assert.DoesNotContain(reopened.CurrentSnapshot.Schema.Fields, f => f.Name == "mine");
    }

    // ── Domain metadata ──

    /// <summary>A domain write rebases past a commit that named no domain at all.</summary>
    [Fact]
    public async Task SetDomainMetadata_RebasesPastAConcurrentAppend_AndLands()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
            await setup.WriteAsync([Batch(1)]);

        await using var stale = await OpenAsync();
        long baseVersion = stale.CurrentSnapshot.Version;

        await using (var other = await OpenAsync())
            await other.WriteAsync([Batch(2)]);

        long committed = await stale.SetDomainMetadataAsync("acme.retention", """{"days":30}""");

        Assert.Equal(baseVersion + 2, committed);
        await using var reopened = await OpenAsync();
        Assert.Equal("""{"days":30}""", reopened.GetDomainMetadata("acme.retention"));
    }

    /// <summary>
    /// Two writers of the SAME domain conflict rather than last-writer-wins. This is the rule the retry
    /// made necessary: before #109 the version collision itself refused the second commit, and rebasing
    /// without this rule would have let it overwrite an edit its author never saw.
    /// </summary>
    [Fact]
    public async Task SetDomainMetadata_AgainstTheSameDomain_Conflicts()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
        {
            await setup.WriteAsync([Batch(1)]);
            // Declares the domainMetadata writer feature, so the racing commits below carry no protocol
            // action of their own and the rule under test is the only one in play. The first domain write
            // to a table is a protocol upgrade as well as a domain write, and that race is its own case —
            // see DomainMetadataFeatureDeclarationTests.
            await setup.SetDomainMetadataAsync("acme.seed", "{}");
        }

        await using var stale = await OpenAsync();
        long baseVersion = stale.CurrentSnapshot.Version;

        await using (var other = await OpenAsync())
            await other.SetDomainMetadataAsync("acme.retention", """{"days":7}""");

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.SetDomainMetadataAsync("acme.retention", """{"days":30}"""));
        Assert.Equal(DeltaErrorCodes.DomainMetadataConflict, ex.ErrorCode);

        // The reporting the issue asked about (#109), settled by the routing rather than separately: these
        // paths no longer surface a bare lost-version-slot, whose ConflictingVersion is null by design
        // because a taken slot has no single commit to blame. This is a CHECKER verdict, so it names the
        // commit that invalidated us as well as the version we tried for.
        Assert.Equal(baseVersion + 1, ex.ConflictingVersion);
        Assert.Equal(baseVersion + 1, ex.AttemptedVersion);
        Assert.Equal(ConflictRecovery.Replan, ex.Recovery);

        await using var reopened = await OpenAsync();
        Assert.Equal("""{"days":7}""", reopened.GetDomainMetadata("acme.retention")); // first committer wins
    }

    /// <summary>Different domains contest nothing, so both land.</summary>
    [Fact]
    public async Task SetDomainMetadata_AgainstADifferentDomain_BothLand()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
        {
            await setup.WriteAsync([Batch(1)]);
            // Declares the domainMetadata writer feature, so the racing commits below carry no protocol
            // action of their own and the rule under test is the only one in play. The first domain write
            // to a table is a protocol upgrade as well as a domain write, and that race is its own case —
            // see DomainMetadataFeatureDeclarationTests.
            await setup.SetDomainMetadataAsync("acme.seed", "{}");
        }

        await using var stale = await OpenAsync();

        await using (var other = await OpenAsync())
            await other.SetDomainMetadataAsync("acme.lineage", """{"v":1}""");

        await stale.SetDomainMetadataAsync("acme.retention", """{"days":30}""");

        await using var reopened = await OpenAsync();
        Assert.Equal("""{"v":1}""", reopened.GetDomainMetadata("acme.lineage"));
        Assert.Equal("""{"days":30}""", reopened.GetDomainMetadata("acme.retention"));
    }

    /// <summary>
    /// <c>RemoveDomainMetadataAsync</c> asserts the domain exists before it stages a tombstone — a READ,
    /// and a concurrent removal is what invalidates it. It surfaces as the domain conflict rather than as
    /// a tombstone quietly stacked on a tombstone.
    /// </summary>
    [Fact]
    public async Task RemoveDomainMetadata_AgainstAConcurrentRemovalOfTheSameDomain_Conflicts()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
        {
            await setup.WriteAsync([Batch(1)]);
            await setup.SetDomainMetadataAsync("acme.retention", """{"days":30}""");
        }

        await using var stale = await OpenAsync();

        await using (var other = await OpenAsync())
            await other.RemoveDomainMetadataAsync("acme.retention");

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.RemoveDomainMetadataAsync("acme.retention"));
        Assert.Equal(DeltaErrorCodes.DomainMetadataConflict, ex.ErrorCode);
    }

    // ── Clustering (SetClusteringColumnsAsync) ──

    /// <summary>Declaring clustering rebases past an append that touched no domain.</summary>
    [Fact]
    public async Task SetClusteringColumns_RebasesPastAConcurrentAppend_AndLands()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
            await setup.WriteAsync([Batch(1)]);

        await using var stale = await OpenAsync();
        long baseVersion = stale.CurrentSnapshot.Version;

        await using (var other = await OpenAsync())
            await other.WriteAsync([Batch(2)]);

        long committed = await stale.SetClusteringColumnsAsync(["id"]);

        Assert.Equal(baseVersion + 2, committed);
        await using var reopened = await OpenAsync();
        Assert.True(reopened.GetDomainMetadata().ContainsKey("delta.clustering"));
        Assert.Equal([1L, 2L], await ReadIdsFresh());
    }

    /// <summary>
    /// Two writers RE-KEYING the clustering declaration conflict: it lives in the <c>delta.clustering</c>
    /// domain, and the loser's spec would replace the winner's outright.
    ///
    /// <para>Clustering is declared once in setup so that neither racing commit carries the writer-feature
    /// upgrade a first declaration brings — that would conflict as a protocol change, which is correct but
    /// is not the rule under test.</para>
    /// </summary>
    [Fact]
    public async Task SetClusteringColumns_AgainstAConcurrentClusteringChange_Conflicts()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("grp", Int64Type.Default, true))
            .Build();
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema))
        {
            await setup.WriteAsync([new RecordBatch(
                schema,
                [new Int64Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(1).Build()],
                1)]);
            await setup.SetClusteringColumnsAsync(["id"]); // protocol upgrade happens HERE, uncontended
        }

        await using var stale = await OpenAsync();

        await using (var other = await OpenAsync())
            await other.SetClusteringColumnsAsync(["grp"]);

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.SetClusteringColumnsAsync(["id"]));
        Assert.Equal(DeltaErrorCodes.DomainMetadataConflict, ex.ErrorCode);
    }

    // ── OPTIMIZE: the path with the most to lose ──

    private static CompactionOptions CompactEverything() =>
        new() { TargetFileSize = 1 << 20, MinFileSize = 1 << 20 };

    /// <summary>
    /// The headline case. OPTIMIZE rewrote six files and then lost the version race to an append that
    /// touched none of them — which used to throw away the whole rewrite. It now rebases: the compacted
    /// file replaces its six sources, and the concurrently appended file simply is not part of it.
    /// </summary>
    [Fact]
    public async Task Compact_RebasesPastAConcurrentAppend_AndLands()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
        {
            for (long i = 1; i <= 6; i++)
                await setup.WriteAsync([Batch(i)]); // six small files, all candidates
        }

        await using var stale = await OpenAsync();
        long baseVersion = stale.CurrentSnapshot.Version;

        await using (var other = await OpenAsync())
            await other.WriteAsync([Batch(99)]); // takes the version OPTIMIZE was aiming at

        long? committed = await stale.CompactAsync(CompactEverything());

        Assert.Equal(baseVersion + 2, committed);
        await using var reopened = await OpenAsync();

        // The six candidates are one file now; the concurrent append's file is untouched beside it.
        Assert.Equal(2, reopened.CurrentSnapshot.ActiveFiles.Count);
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L, 99L], await ReadIdsFresh());
    }

    /// <summary>
    /// The other direction, and the reason the rebase is sound rather than lucky: a concurrent commit that
    /// removed one of the rewritten files aborts it, because the compacted bytes are no longer the live
    /// rows of the files being removed. The verdict names the rule and the version.
    /// </summary>
    [Fact]
    public async Task Compact_AgainstAConcurrentDeleteOfACandidate_Conflicts()
    {
        await using (var setup = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema))
        {
            for (long i = 1; i <= 6; i++)
                await setup.WriteAsync([Batch(i)]);
        }

        await using var stale = await OpenAsync();

        await using (var other = await OpenAsync())
        {
            await other.DeleteAsync(batch =>
            {
                var id = (Int64Array)batch.Column("id");
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < id.Length; i++)
                    mask.Append(id.GetValue(i) == 3);
                return mask.Build();
            });
        }

        var ex = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.CompactAsync(CompactEverything()));
        Assert.Equal(DeltaErrorCodes.ConcurrentDeleteDelete, ex.ErrorCode);

        // The delete stands and nothing resurrected id 3.
        Assert.Equal([1L, 2L, 4L, 5L, 6L], await ReadIdsFresh());
    }

    /// <summary>
    /// OPTIMIZE on a row-tracking table rebases too, and the ids it hands the compacted file are
    /// re-reserved past what the concurrent append consumed. Overlapping ranges would be the corruption
    /// the old abort was inadvertently protecting against — see <c>CompactionRebaseHandler</c>.
    /// </summary>
    [Fact]
    public async Task Compact_OnARowTrackingTable_RebasesWithFreshlyReservedRowIds()
    {
        await using (var setup = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableRowTracking: true))
        {
            await setup.WriteAsync([Batch(1)]); // ids 0
            await setup.WriteAsync([Batch(2)]); // id  1
        }

        await using var stale = await OpenAsync();
        long baseVersion = stale.CurrentSnapshot.Version;
        long baseMark = stale.CurrentSnapshot.RowIdHighWaterMark;

        await using (var other = await OpenAsync())
            await other.WriteAsync([Batch(98, 99)]); // consumes two ids past the mark the compaction reserved

        long? committed = await stale.CompactAsync(CompactEverything());
        Assert.Equal(baseVersion + 2, committed);

        await using var reopened = await OpenAsync();
        var files = reopened.CurrentSnapshot.ActiveFiles.Values
            .OrderBy(f => f.BaseRowId ?? -1)
            .ToList();
        Assert.Equal(2, files.Count);

        // The concurrent append kept the range it committed at; the compacted file was re-reserved ABOVE
        // it rather than re-using the range it had claimed against the stale mark.
        Assert.Equal(baseMark, files[0].BaseRowId);
        Assert.Equal(baseMark + 2, files[1].BaseRowId);
        Assert.Equal(baseMark + 4, reopened.CurrentSnapshot.RowIdHighWaterMark);

        // And the compaction did NOT re-stamp defaultRowCommitVersion the way an append's rebase does:
        // it stays inherited from the earliest source file.
        Assert.Equal(1L, files[1].DefaultRowCommitVersion);

        Assert.Equal([1L, 2L, 98L, 99L], await ReadIdsFresh());
    }
}
