// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Liquid-clustered tables — INTEROP, not a clustering writer. The `clustering` feature is advisory layout:
/// the spec permits plain unclustered appends and DML by writers that do not implement clustering, and a
/// later clustering OPTIMIZE (Spark) reclusters them. What this library must do is preserve the
/// `delta.clustering` declaration through commits and checkpoints, round-trip `add.clusteringProvider`, and
/// be able to declare clustering at create / ALTER time in the exact shape Spark reads.
/// </summary>
public class ClusteredTableTests : IDisposable
{
    private const string ClusteringDomain = "delta.clustering";
    private readonly string _tempDir;

    public ClusteredTableTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_clust_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("grp", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params (string Grp, long Id)[] rows)
    {
        var grps = new StringArray.Builder();
        var ids = new Int64Array.Builder();
        foreach (var (g, i) in rows)
        {
            grps.Append(g);
            ids.Append(i);
        }
        return new RecordBatch(schema, [grps.Build(), ids.Build()], rows.Length);
    }

    private static DeltaTableOptions Options => new() { CheckpointInterval = 0 };

    private static List<string> ClusteringColumnsOf(DeltaTable table)
    {
        var dm = table.GetDomainMetadata()[ClusteringDomain];
        using var doc = JsonDocument.Parse(dm.Configuration);
        return doc.RootElement.GetProperty("clusteringColumns")
            .EnumerateArray().Select(path => path[0].GetString()!).ToList();
    }

    /// <summary>A clustered table in the exact shape OSS delta-spark writes: protocol v7 with
    /// clustering + domainMetadata, and the delta.clustering domain action in commit 0.</summary>
    private async Task<DeltaTable> CreateSparkShapedClusteredTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["clustering", "domainMetadata"],
            },
            new MetadataAction
            {
                Id = "clustered-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"grp","type":"string","nullable":true,"metadata":{}},{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new DomainMetadata
            {
                Domain = ClusteringDomain,
                Configuration = """{"clusteringColumns":[["grp"],["id"]],"domainName":"delta.clustering"}""",
                Removed = false,
            },
        });

        return await DeltaTable.OpenAsync(fs, Options);
    }

    // Without the allowlist entry, EVERY write to a Databricks/Fabric CLUSTER BY table failed
    // ValidateWriteSupport with "unsupported writer features: [clustering]".
    [Fact]
    public async Task Append_ToAClusteredTable_IsAccepted()
    {
        await using var table = await CreateSparkShapedClusteredTableAsync();
        await table.WriteAsync([Rows(table.ArrowSchema, ("a", 1), ("b", 2))]);

        Assert.Equal(1, table.CurrentSnapshot.FileCount);
        // The declaration survives our commit untouched.
        Assert.Equal(["grp", "id"], ClusteringColumnsOf(table));
    }

    // The sharp edge: a checkpoint that dropped domainMetadata would silently destroy the clustering spec.
    [Fact]
    public async Task ClusteringDomain_SurvivesACheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var seed = await CreateSparkShapedClusteredTableAsync()) { }

        // CheckpointInterval = 1 makes the next commit write a checkpoint.
        await using (var table = await DeltaTable.OpenAsync(
            fs, new DeltaTableOptions { CheckpointInterval = 1 }))
        {
            await table.WriteAsync([Rows(table.ArrowSchema, ("a", 1))]);
        }
        Assert.True(Directory.EnumerateFiles(Path.Combine(_tempDir, "_delta_log"), "*.checkpoint.parquet").Any(),
            "expected a checkpoint to have been written");

        // Reopen so the snapshot is rebuilt FROM the checkpoint.
        await using var reopened = await DeltaTable.OpenAsync(fs, Options);
        Assert.Equal(["grp", "id"], ClusteringColumnsOf(reopened));
    }

    [Fact]
    public async Task ClusteringProvider_RoundTripsThroughLogReplay()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await CreateSparkShapedClusteredTableAsync();
        await table.WriteAsync([Rows(table.ArrowSchema, ("a", 1))]);

        // Simulate a clustering engine's OPTIMIZE output: an add stamped with the provider.
        var existing = table.CurrentSnapshot.ActiveFiles.Values.Single();
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(table.CurrentSnapshot.Version + 1, new List<DeltaAction>
        {
            existing with { Path = "clustered.parquet", ClusteringProvider = "liquid" },
        });

        await using var reopened = await DeltaTable.OpenAsync(fs, Options);
        var clustered = reopened.CurrentSnapshot.ActiveFiles.Values
            .Single(f => f.Path == "clustered.parquet");
        Assert.Equal("liquid", clustered.ClusteringProvider);
    }

    [Fact]
    public async Task Create_WithClusteringColumns_DeclaresTheFeatureAndDomain()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, Schema(), Options, clusteringColumns: ["grp", "id"]);

        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("clustering", protocol.WriterFeatures!);
        Assert.Contains("domainMetadata", protocol.WriterFeatures!);
        // Writer-only: the reader side must NOT gain these, or readers are locked out needlessly.
        Assert.DoesNotContain("clustering", protocol.ReaderFeatures ?? []);

        Assert.Equal(["grp", "id"], ClusteringColumnsOf(table));
        // Spark includes the redundant domainName field; match its byte shape.
        Assert.Contains("\"domainName\":\"delta.clustering\"",
            table.GetDomainMetadata()[ClusteringDomain].Configuration);
    }

    // The spec finding that cost a live crash: the domain stores PHYSICAL names. OSS Delta's
    // ClusteringColumnInfo resolves against physical names and None.get-crashes on a logical one.
    [Fact]
    public async Task Create_WithClusteringColumns_UnderColumnMapping_StoresPhysicalNames()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, Schema(), Options,
            columnMappingMode: ColumnMappingMode.Name, clusteringColumns: ["grp"]);

        var grpField = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "grp");
        string physical = ColumnMapping.GetPhysicalName(grpField, ColumnMappingMode.Name);
        Assert.StartsWith("col-", physical);

        // The domain carries the PHYSICAL name, not "grp".
        Assert.Equal([physical], ClusteringColumnsOf(table));
    }

    [Fact]
    public async Task Create_WithUnknownClusteringColumn_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(fs, Schema(), Options, clusteringColumns: ["nope"]));
    }

    [Fact]
    public async Task Create_WithClusteringAndPartitioning_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(
                fs, Schema(), Options, partitionColumns: ["grp"], clusteringColumns: ["id"]));
        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public async Task SetClusteringColumns_Declares_ReKeys_AndRemoves()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema(), Options);

        // Declare on a previously unclustered table.
        await table.SetClusteringColumnsAsync(["grp"]);
        Assert.Equal(["grp"], ClusteringColumnsOf(table));

        // Re-key.
        await table.SetClusteringColumnsAsync(["id"]);
        Assert.Equal(["id"], ClusteringColumnsOf(table));

        // Remove — a tombstoned domain drops out of the snapshot entirely (SnapshotBuilder removes it
        // on a Removed action rather than retaining it flagged).
        await table.SetClusteringColumnsAsync(null);
        Assert.False(table.GetDomainMetadata().ContainsKey(ClusteringDomain));
    }

    [Fact]
    public async Task SetClusteringColumns_OnAnUnclusteredTable_WithNothingToDo_IsANoOp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema(), Options);

        long before = table.CurrentSnapshot.Version;
        long returned = await table.SetClusteringColumnsAsync(null);

        Assert.Equal(before, returned);
        Assert.Equal(before, table.CurrentSnapshot.Version); // no commit written
    }

    // The writer-only upgrade pin: a legacy reader-1 table becomes writer-7 while STAYING reader-1.
    [Fact]
    public async Task SetClusteringColumns_UpgradesWriterOnly_LeavingTheReaderSideAlone()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema(), Options);

        var before = table.CurrentSnapshot.Protocol;
        Assert.Equal(1, before.MinReaderVersion);
        Assert.Null(before.WriterFeatures);

        await table.SetClusteringColumnsAsync(["grp"]);

        var after = table.CurrentSnapshot.Protocol;
        Assert.Equal(1, after.MinReaderVersion);          // untouched
        Assert.Null(after.ReaderFeatures);                // untouched
        Assert.Equal(7, after.MinWriterVersion);
        Assert.Contains("clustering", after.WriterFeatures!);
        Assert.Contains("domainMetadata", after.WriterFeatures!);
        // The legacy writer-2 features it implied are enumerated, not dropped.
        Assert.Contains("appendOnly", after.WriterFeatures!);
        Assert.Contains("invariants", after.WriterFeatures!);
    }

    [Fact]
    public async Task SetClusteringColumns_OnAPartitionedTable_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, Schema(), Options, partitionColumns: ["grp"]);

        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.SetClusteringColumnsAsync(["id"]));
    }

    [Fact]
    public async Task SetClusteringColumns_FusesExtraActionsIntoTheSameCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, Schema(), Options);

        var newConfig = new Dictionary<string, string> { ["custom.key"] = "v" };
        long version = await table.SetClusteringColumnsAsync(
            ["grp"], [table.CurrentSnapshot.Metadata with { Configuration = newConfig }]);

        // One commit carries both the domain and the metadata update.
        Assert.Equal(["grp"], ClusteringColumnsOf(table));
        Assert.Equal("v", table.CurrentSnapshot.Metadata.Configuration!["custom.key"]);
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }
}
