// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #598: every public path that can turn in-commit timestamps on for an EXISTING table timestamps that commit and
/// records it as the enablement point. Before, none did, and Spark's <c>DESCRIBE HISTORY</c> and
/// <c>TIMESTAMP AS OF</c> then failed with <c>DELTA_MISSING_COMMIT_TIMESTAMP</c> (see the interop twin in
/// <c>Interop/InCommitTimestampMidLifeInteropTests</c>).
/// </summary>
public class InCommitTimestampMidLifeTests : IDisposable
{
    private readonly string _tempDir;

    public InCommitTimestampMidLifeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ict_midlife_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, nullable: true))
        .Build();

    private static RecordBatch Ids(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private async Task<DeltaTable> CreateWithRowsAsync()
    {
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema);
        await table.WriteAsync([Ids(1, 2)]);
        await table.WriteAsync([Ids(3)]);
        return table;
    }

    /// <summary>The table's metadata with in-commit timestamps on, and a protocol that declares the feature.</summary>
    private static DeltaAction[] Enabling(DeltaTable table)
    {
        var current = table.CurrentSnapshot.Metadata;
        var configuration = new Dictionary<string, string>(StringComparer.Ordinal);
        if (current.Configuration is not null)
        {
            foreach (var kvp in current.Configuration)
                configuration[kvp.Key] = kvp.Value;
        }
        configuration[InCommitTimestamp.EnableKey] = "true";
        var protocol = table.CurrentSnapshot.Protocol;
        var writerFeatures = new List<string>(protocol.WriterFeatures ?? ["appendOnly", "invariants"]);
        writerFeatures.Add("inCommitTimestamp");
        return
        [
            current with { Configuration = configuration },
            new ProtocolAction
            {
                MinReaderVersion = protocol.MinReaderVersion,
                MinWriterVersion = 7,
                ReaderFeatures = protocol.ReaderFeatures,
                WriterFeatures = writerFeatures,
            },
        ];
    }

    private List<JsonElement> CommitOnDisk(long version) =>
        [.. File.ReadAllLines(Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json"))
            .Where(line => line.Length > 0)
            .Select(line => JsonDocument.Parse(line).RootElement)];

    private static IEnumerable<JsonElement> Of(List<JsonElement> actions, string kind) =>
        actions.Where(a => a.TryGetProperty(kind, out _)).Select(a => a.GetProperty(kind));

    /// <summary>The commit carries an inCommitTimestamp, and its metadata names it as the enablement point.</summary>
    private void AssertEnablementStamped(long version)
    {
        var actions = CommitOnDisk(version);
        var commitInfo = Of(actions, "commitInfo").Single();
        Assert.True(commitInfo.TryGetProperty("inCommitTimestamp", out var stamp), "the enabling commit is timestamped");
        var configuration = Of(actions, "metaData").Single().GetProperty("configuration");
        Assert.Equal(version.ToString(), configuration.GetProperty(InCommitTimestamp.EnablementVersionKey).GetString());
        Assert.Equal(
            stamp.GetInt64().ToString(),
            configuration.GetProperty(InCommitTimestamp.EnablementTimestampKey).GetString());
        // The spec allows one protocol action per commit.
        Assert.True(Of(actions, "protocol").Count() <= 1, "at most one protocol action");
    }

    [Theory]
    [InlineData("commit data files")]
    [InlineData("clustering")]
    [InlineData("transaction")]
    public async Task EnablingThroughExtraActions_StampsTheEnablementPoint(string seam)
    {
        await using var table = await CreateWithRowsAsync();
        var enabling = Enabling(table);

        switch (seam)
        {
            case "commit data files":
                await table.CommitDataFilesAsync([], extraActions: enabling, operation: "SET TBLPROPERTIES");
                break;
            case "clustering":
                await table.SetClusteringColumnsAsync(null, extraActions: enabling);
                break;
            case "transaction":
            {
                await using var txn = table.StartTransaction();
                txn.StageActions(enabling);
                await txn.CommitAsync();
                break;
            }
        }

        long version = table.CurrentSnapshot.Version;
        Assert.Equal(3, version);
        AssertEnablementStamped(version);
        Assert.Equal(
            (3L, table.CurrentSnapshot.InCommitTimestamp!.Value),
            InCommitTimestamp.GetEnablement(table.CurrentSnapshot.Metadata.Configuration));

        // The next commit is timestamped too, after the enabling one, and leaves the enablement point alone.
        await table.WriteAsync([Ids(4)]);
        var next = Of(CommitOnDisk(4), "commitInfo").Single();
        long enabledAt = Of(CommitOnDisk(3), "commitInfo").Single().GetProperty("inCommitTimestamp").GetInt64();
        Assert.True(next.GetProperty("inCommitTimestamp").GetInt64() > enabledAt);
        Assert.Equal(
            (3L, enabledAt), InCommitTimestamp.GetEnablement(table.CurrentSnapshot.Metadata.Configuration));
    }

    /// <summary>
    /// Declaring clustering upgrades the protocol, and a caller's protocol in <c>extraActions</c> used to be committed
    /// BESIDE that upgrade: two protocol actions in one commit, which the spec forbids. The caller's now carries the
    /// upgrade.
    /// </summary>
    [Fact]
    public async Task ClusteringWithACallersProtocol_CommitsOneProtocol_CarryingBoth()
    {
        await using var table = await CreateWithRowsAsync();

        await table.SetClusteringColumnsAsync(["id"], extraActions: Enabling(table));

        AssertEnablementStamped(3);
        var features = table.CurrentSnapshot.Protocol.WriterFeatures!;
        Assert.Contains("inCommitTimestamp", features);
        Assert.Contains("clustering", features);
        Assert.Contains("domainMetadata", features);
    }

    [Fact]
    public async Task CreateOrReplace_ThatEnablesThem_StampsTheEnablementPoint()
    {
        await (await CreateWithRowsAsync()).DisposeAsync();

        await using var replaced = await DeltaTable.CreateOrReplaceAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, [Ids(7)],
            configuration: new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" });

        Assert.Equal(3, replaced.CurrentSnapshot.Version);
        AssertEnablementStamped(3);
    }

    [Fact]
    public async Task CreateOrReplace_OfATableThatAlreadyHasThem_KeepsItsEnablementPoint()
    {
        await using (var table = await CreateWithRowsAsync())
            await table.CommitDataFilesAsync([], extraActions: Enabling(table), operation: "SET TBLPROPERTIES");

        // The replacement configuration says nothing about where they began; the commits before v3 still have no
        // timestamp, so the point stays.
        await using var replaced = await DeltaTable.CreateOrReplaceAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, [Ids(7)],
            configuration: new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" });

        Assert.Equal(3L, InCommitTimestamp.GetEnablement(replaced.CurrentSnapshot.Metadata.Configuration)?.Version);
    }

    /// <summary>
    /// #660 review: both VACUUM bookends were floored against the snapshot VACUUM started from, so with the previous
    /// commit's timestamp ahead of this clock, START and END got the same one.
    /// </summary>
    [Fact]
    public async Task VacuumBookends_EachFollowTheCommitBeforeThem()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        long ahead = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 3_600_000;
        await using (var created = await DeltaTable.CreateAsync(
            fs, IdSchema, configuration: new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" }))
        {
            // v1, stamped by a writer whose clock runs an hour ahead.
            await new TransactionLog(fs).WriteCommitAsync(1, [InCommitTimestamp.CreateCommitInfo(ahead, "WRITE")]);
        }
        await using var table = await DeltaTable.OpenAsync(fs);

        await table.VacuumAsync(TimeSpan.FromDays(7));

        long start = Of(CommitOnDisk(2), "commitInfo").Single().GetProperty("inCommitTimestamp").GetInt64();
        long end = Of(CommitOnDisk(3), "commitInfo").Single().GetProperty("inCommitTimestamp").GetInt64();
        Assert.Equal(ahead + 1, start);
        Assert.Equal(start + 1, end);
    }

    [Fact]
    public async Task CreatingWithThem_RecordsNoEnablementPoint()
    {
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema,
            configuration: new Dictionary<string, string> { [InCommitTimestamp.EnableKey] = "true" });
        await table.WriteAsync([Ids(1)]);

        Assert.Null(InCommitTimestamp.GetEnablement(table.CurrentSnapshot.Metadata.Configuration));
        Assert.True(Of(CommitOnDisk(0), "commitInfo").Single().TryGetProperty("inCommitTimestamp", out _));
    }
}
