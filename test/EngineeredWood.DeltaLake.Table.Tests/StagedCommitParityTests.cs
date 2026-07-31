// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The invariant behind slice 5, rather than the two parameters that prompted it:
///
/// <para><b>Anything the auto-committing surface can express, the staged surface can express.</b></para>
///
/// <para>A host that stages its work on a <see cref="DeltaTransaction"/> gets the commit loop — conflict
/// checking, rebase, retry — for free. Every capability that exists ONLY on
/// <see cref="DeltaTable.CommitDataFilesAsync"/> is therefore a capability whose users must hand-roll that
/// loop. Two such gaps were found by a downstream host rather than by us, and the second
/// (<c>identityValuesPreGenerated</c>) meant an identity table's appends could not be staged AT ALL.</para>
///
/// <para>So this walks <see cref="DeltaTable.CommitDataFilesAsync"/>' parameters by reflection and requires
/// each one to be either mapped to a real member of the staged surface or explicitly allow-listed with a
/// reason. The next parameter added without a staged counterpart fails a build instead of waiting for
/// another host to report it.</para>
/// </summary>
public class StagedCommitParityTests : IDisposable
{
    private readonly string _tempDir;

    public StagedCommitParityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_parity_{Guid.NewGuid():N}");
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

    /// <summary>How the staged surface expresses one of <c>CommitDataFilesAsync</c>' capabilities: the member
    /// that carries it, and — for a parameter — the parameter name on that member.</summary>
    private sealed record StagedCounterpart(Type Declaring, string Member, string? Parameter = null);

    /// <summary>
    /// Every <c>CommitDataFilesAsync</c> parameter, mapped to the staged member that expresses it. Each entry
    /// is checked to EXIST by reflection, so a rename on either side breaks this test rather than quietly
    /// leaving a false claim of parity behind.
    /// </summary>
    private static readonly Dictionary<string, StagedCounterpart> Mapped = new(StringComparer.Ordinal)
    {
        ["files"] = new(typeof(DeltaTransaction), nameof(DeltaTransaction.StageDataFilesAsync), "files"),
        ["identityValuesPreGenerated"] = new(
            typeof(DeltaTransaction), nameof(DeltaTransaction.StageDataFilesAsync),
            "identityValuesPreGenerated"),
        // Index-keyed on the committing side, path-keyed here — the key only ever has to identify a file
        // within the same call's list, and WrittenDataFile.RelativePath is what add.path becomes.
        ["deletedPositionsByFileIndex"] = new(
            typeof(DeltaTransaction), nameof(DeltaTransaction.StageDataFilesAsync), "bornDeleted"),
        // Arbitrary pre-built actions fused into the same commit.
        ["extraActions"] = new(typeof(DeltaTransaction), nameof(DeltaTransaction.StageActions), "actions"),
        // What commitInfo records. A PROPERTY since slice 4, not a per-call argument: Delta's operation
        // field is one string per commit, so it describes the transaction rather than any one staged thing.
        ["operation"] = new(typeof(DeltaTransaction), nameof(DeltaTransaction.Operation)),
        // First-committer-wins pinning: a transaction is pinned to the snapshot it started at, by
        // construction, and reports it.
        ["expectedVersion"] = new(typeof(DeltaTransaction), nameof(DeltaTransaction.ReadVersion)),
        ["cancellationToken"] = new(
            typeof(DeltaTransaction), nameof(DeltaTransaction.StageDataFilesAsync), "cancellationToken"),
    };

    /// <summary>
    /// What genuinely cannot be staged, with the reason. Everything here is the overwrite/rewrite family: it
    /// removes files the transaction did not itself add, which is exactly what a rebase cannot re-derive — a
    /// removal computed against the base snapshot is not still correct against a newer one, and a rewrite's
    /// fresh add embeds the attempted version's row-id high-water mark. Use the auto-committing surface.
    /// </summary>
    private static readonly Dictionary<string, string> AllowListed = new(StringComparer.Ordinal)
    {
        ["mode"] = "Overwrite removes the whole active set — a rebase cannot re-derive it.",
        ["dynamicPartitionOverwrite"] = "Removes the active files of the touched partitions; same reason.",
        ["dataChange"] = "dataChange=false is the REWRITE family (compaction / clustering OPTIMIZE), which "
                         + "removes the files it replaces; a rewrite's add is not verbatim-rebase-safe.",
        ["clusteringProvider"] = "Stamped by a clustering OPTIMIZE, which is a rewrite; same reason.",
    };

    [Fact]
    public void EveryCommitDataFilesCapability_IsEitherStageableOrExplicitlyAllowListed()
    {
        var commit = typeof(DeltaTable).GetMethod(nameof(DeltaTable.CommitDataFilesAsync));
        Assert.NotNull(commit);

        var unexplained = commit!.GetParameters()
            .Select(p => p.Name!)
            .Where(name => !Mapped.ContainsKey(name) && !AllowListed.ContainsKey(name))
            .ToList();

        Assert.True(
            unexplained.Count == 0,
            "CommitDataFilesAsync gained capabilities with no staged counterpart and no recorded reason: "
            + string.Join(", ", unexplained)
            + ". Either add them to DeltaTransaction's staged surface, or allow-list them here with why a "
            + "rebase cannot carry them.");
    }

    [Fact]
    public void EveryClaimedStagedCounterpart_ActuallyExists()
    {
        foreach (var (capability, counterpart) in Mapped)
        {
            var member = counterpart.Declaring
                .GetMember(counterpart.Member, BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault();
            Assert.True(
                member is not null,
                $"'{capability}' claims to be staged by {counterpart.Declaring.Name}.{counterpart.Member}, "
                + "which does not exist.");

            if (counterpart.Parameter is null)
                continue;

            var parameters = ((MethodInfo)member!).GetParameters().Select(p => p.Name).ToList();
            Assert.True(
                parameters.Contains(counterpart.Parameter),
                $"'{capability}' claims to be staged by {counterpart.Declaring.Name}."
                + $"{counterpart.Member}'s '{counterpart.Parameter}' parameter, which does not exist. "
                + $"It has: {string.Join(", ", parameters)}.");
        }
    }

    [Fact]
    public void TheAllowList_ExplainsOnlyRealCommitParameters()
    {
        // A stale allow-list entry is a claim that something cannot be staged when it may no longer exist —
        // as misleading as a missing one.
        var commitParams = typeof(DeltaTable).GetMethod(nameof(DeltaTable.CommitDataFilesAsync))!
            .GetParameters().Select(p => p.Name!).ToHashSet(StringComparer.Ordinal);

        foreach (string name in AllowListed.Keys.Concat(Mapped.Keys))
        {
            Assert.True(
                commitParams.Contains(name),
                $"'{name}' is recorded here but is no longer a CommitDataFilesAsync parameter — remove it.");
        }
    }

    // ── the two gaps, exercised end to end ──

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

    private async Task<List<long>> ReadIdsFreshAsync()
    {
        await using var reader = await DeltaTable.OpenAsync(Fs);
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

    /// <summary>
    /// A row inserted and deleted inside one transaction never appears in any committed version — the add is
    /// born with an inline deletion vector rather than the commit carrying an insert the next one undoes.
    /// </summary>
    [Fact]
    public async Task BornDeleted_RowsNeverAppearInAnyCommittedVersion()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);

        var files = await table.WriteDataFilesAsync([Batch(1, 5)]); // ids 1..5, positions 0..4
        var file = Assert.Single(files);

        var txn = table.StartTransaction();
        long live = await txn.StageDataFilesAsync(
            files,
            bornDeleted: RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [file.RelativePath] = new long[] { 1, 3 },   // ids 2 and 4
            }));
        Assert.Equal(3, live);   // five rows staged, two born deleted
        long version = await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 3, 5 }, await ReadIdsFreshAsync());

        // The add carries the vector inline, and its stats are no longer tight — the bounds were computed
        // over rows the vector now hides.
        await using var check = await DeltaTable.OpenAsync(Fs);
        var add = Assert.Single(check.CurrentSnapshot.ActiveFiles.Values);
        Assert.NotNull(add.DeletionVector);
        Assert.Equal(2, add.DeletionVector!.Cardinality);
        Assert.Contains("\"tightBounds\":false", add.GetStatsJson());

        // Nothing was ever visible and then removed: the rows are absent from the version that added them.
        var atCommit = new List<long>();
        await foreach (var batch in check.ReadAtVersionAsync(version))
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                atCommit.Add(col.GetValue(i)!.Value);
        }
        atCommit.Sort();
        Assert.Equal(new long[] { 1, 3, 5 }, atCommit);
    }

    [Fact]
    public async Task BornDeleted_WithoutIt_TheSameStagingIsAPlainAppend()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var files = await table.WriteDataFilesAsync([Batch(1, 3)]);
        var txn = table.StartTransaction();
        Assert.Equal(3, await txn.StageDataFilesAsync(files));
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 2, 3 }, await ReadIdsFreshAsync());
        await using var check = await DeltaTable.OpenAsync(Fs);
        Assert.All(check.CurrentSnapshot.ActiveFiles.Values, a => Assert.Null(a.DeletionVector));
    }

    [Fact]
    public async Task BornDeleted_NamingAFileNotBeingStaged_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        var files = await table.WriteDataFilesAsync([Batch(1, 3)]);
        var txn = table.StartTransaction();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => txn.StageDataFilesAsync(
            files,
            bornDeleted: RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                ["somewhere-else.parquet"] = new long[] { 0 },
            })).AsTask());
        Assert.Contains("somewhere-else.parquet", ex.Message);
    }

    [Fact]
    public async Task BornDeleted_PositionPastTheFilesRowCount_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        var files = await table.WriteDataFilesAsync([Batch(1, 3)]);
        var file = Assert.Single(files);
        var txn = table.StartTransaction();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => txn.StageDataFilesAsync(
            files,
            bornDeleted: RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [file.RelativePath] = new long[] { 99 },
            })).AsTask());
        Assert.Contains("99", ex.Message);
        Assert.Contains("3 row", ex.Message);
    }

    // ── the identity gap ──

    private static string IdentitySchemaString()
    {
        var idMeta = IdentityColumn.CreateMetadata(start: 1, step: 1, allowExplicitInsert: false);
        string idMetaJson = System.Text.Json.JsonSerializer.Serialize(idMeta);
        return $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":true,""metadata"":{idMetaJson}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";
    }

    private async Task<DeltaTable> CreateIdentityTableAsync()
    {
        var log = new TransactionLog(Fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["identityColumns"],
            },
            new MetadataAction
            {
                Id = "id-parity",
                Format = Format.Parquet,
                SchemaString = IdentitySchemaString(),
                PartitionColumns = [],
            },
        });
        return await DeltaTable.OpenAsync(Fs);
    }

    /// <summary>An INSERT-shaped batch: the identity column arrives as NULLs for the engine to fill.</summary>
    private static RecordBatch InsertBatch(params string[] values)
    {
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        foreach (var v in values)
        {
            ids.AppendNull();
            vals.Append(v);
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        return new RecordBatch(schema, [ids.Build(), vals.Build()], values.Length);
    }

    /// <summary>
    /// The gap that mattered most: without <c>identityValuesPreGenerated</c> an identity table's appends
    /// could not be staged AT ALL, so a host on such a table had no transaction to put anything else into
    /// either — it had to drop to CommitDataFilesAsync and hand-roll the commit loop. This is the staged
    /// twin of <c>IdentityTransactionSeamsTests.FusedIdentityCommit_OneMetadataAction_HwmPersists</c>.
    /// </summary>
    [Fact]
    public async Task IdentityTable_AppendsAreStageable_WhenTheValuesWerePreGenerated()
    {
        await using var table = await CreateIdentityTableAsync();
        Assert.False(table.SupportsExternalDataFileCommit); // identity columns: the committing writer's job

        // The host generates the identity values itself — the write-time per-row processing an outside
        // writer would have skipped — and chains them across two statements of one transaction.
        var gen1 = table.GenerateIdentityValues([InsertBatch("a", "b", "c")]);
        var gen2 = table.GenerateIdentityValues([InsertBatch("d", "e")], gen1.HighWaterMarks);
        var files1 = await table.WriteDataFilesAsync(gen1.Batches, identityValuesPreGenerated: true);
        var files2 = await table.WriteDataFilesAsync(gen2.Batches, identityValuesPreGenerated: true);

        var txn = table.StartTransaction();
        Assert.Equal(3, await txn.StageDataFilesAsync(files1, identityValuesPreGenerated: true));
        Assert.Equal(2, await txn.StageDataFilesAsync(files2, identityValuesPreGenerated: true));
        // The final marks ride along as a metaData action, so the fused commit persists them.
        txn.StageActions([table.BuildIdentityMetadataAction(gen2.HighWaterMarks)]);
        await txn.CommitAsync();

        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, await ReadIdsFreshAsync());

        // And the persisted high-water mark drives the NEXT (committing) write, so nothing double-issues.
        await using var check = await DeltaTable.OpenAsync(Fs);
        await check.WriteAsync([InsertBatch("f")]);
        Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6 }, await ReadIdsFreshAsync());
    }

    [Fact]
    public async Task IdentityTable_WithoutThePreGeneratedFlag_IsStillRefused()
    {
        await using var table = await CreateIdentityTableAsync();
        Assert.False(table.SupportsExternalDataFileCommit);

        var txn = table.StartTransaction();
        var stub = new[] { new WrittenDataFile("x.parquet", 1, 1, null, null) };

        // Both the sync and the async form refuse: the flag is an assertion the caller did the work, not a
        // way to skip it.
        await Assert.ThrowsAsync<NotSupportedException>(() => txn.StageDataFilesAsync(stub).AsTask());
        Assert.Throws<NotSupportedException>(() => txn.StageDataFiles(stub));
    }

    /// <summary>Both new arguments compose with the rest of a transaction — the point of staging at all is
    /// that everything lands in ONE version.</summary>
    [Fact]
    public async Task StagedDataFiles_FuseWithOtherStagedWorkIntoOneVersion()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(100, 3)]);            // 100,101,102
        long before = table.CurrentSnapshot.Version;

        var files = await table.WriteDataFilesAsync([Batch(1, 4)]);   // 1..4
        var file = Assert.Single(files);

        var txn = table.StartTransaction();
        await txn.StageDataFilesAsync(
            files,
            bornDeleted: RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
            {
                [file.RelativePath] = new long[] { 0 },     // id 1, born deleted
            }));
        await txn.DeleteAsync(Expressions.Expressions.Equal("id", 101L));
        long version = await txn.CommitAsync();

        Assert.Equal(before + 1, version);                  // ONE version, not two
        Assert.Equal(new long[] { 2, 3, 4, 100, 102 }, await ReadIdsFreshAsync());
    }
}
