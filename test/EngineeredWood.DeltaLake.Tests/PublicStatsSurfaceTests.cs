// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Reflection;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// What a consumer OUTSIDE this assembly can reach of a file's statistics. Every test project here
/// has <c>InternalsVisibleTo</c>, so an ordinary functional test cannot tell a public member from an
/// internal one — these assert the visibility explicitly as well as the behaviour.
/// </summary>
public class PublicStatsSurfaceTests : IDisposable
{
    private readonly string _tempDir;

    public PublicStatsSurfaceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_pubstats_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private const string SchemaString =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""";

    private const string StatsJson =
        """{"numRecords":42,"minValues":{"id":1},"maxValues":{"id":99},"nullCount":{"id":0}}""";

    /// <summary>
    /// Builds a table whose checkpoint carries typed statistics and NO JSON string, then reads one
    /// <see cref="AddFile"/> back out of it the way a log-layer host would — snapshot from checkpoint.
    /// </summary>
    private async Task<AddFile> StructOnlyCheckpointedAdd()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "public-stats",
                Format = Format.Parquet,
                SchemaString = SchemaString,
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    ["delta.checkpoint.writeStatsAsJson"] = "false",
                    ["delta.checkpoint.writeStatsAsStruct"] = "true",
                },
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = StatsJson,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        // Rebuilt through the checkpoint, so the AddFile is the one the checkpoint produced rather
        // than the one the commit carried.
        var fromCheckpoint = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));
        return Assert.Single(fromCheckpoint.ActiveFiles).Value;
    }

    /// <summary>
    /// The gap this closes: a host doing its own pruning saw `Stats == null` for every file in such a
    /// checkpoint and had no way to reach the statistics that were demonstrably there. It got no
    /// exception and no empty result — just a pruner that stopped excluding anything.
    /// </summary>
    [Fact]
    public async Task StructOnlyCheckpoint_ExposesStatisticsThroughThePublicSurface()
    {
        var add = await StructOnlyCheckpointedAdd();

        // The literal log field really is absent — this is the situation, not a setup failure.
        Assert.Null(add.Stats);

        Assert.Equal(42L, add.GetNumRecords());

        string? json = add.GetStatsJson();
        Assert.NotNull(json);

        // Round-trips through the public parser, so a host can keep whatever JSON-shaped pruning it
        // already had rather than learning a new statistics model.
        var parsed = ColumnStats.Parse(json);
        Assert.NotNull(parsed);
        Assert.Equal(42L, parsed!.NumRecords);
        Assert.Equal(1L, parsed.MinValues!["id"].GetInt64());
        Assert.Equal(99L, parsed.MaxValues!["id"].GetInt64());
        Assert.Equal(0L, parsed.NullCount!["id"]);
    }

    /// <summary>
    /// A checkpoint that carries BOTH copies must still hand back the log's own string rather than a
    /// re-synthesised one — <see cref="AddFile.Stats"/> stays the literal field, and
    /// <see cref="AddFile.GetStatsJson"/> prefers it.
    /// </summary>
    [Fact]
    public async Task BothCopiesPresent_GetStatsJsonReturnsTheLiteralField()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "public-stats-both",
                Format = Format.Parquet,
                SchemaString = SchemaString,
                PartitionColumns = [],
            },
            new AddFile
            {
                Path = "data.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1000, ModificationTime = 1000, DataChange = true,
                Stats = StatsJson,
            },
        });

        var snapshot = await SnapshotBuilder.BuildAsync(log);
        await new CheckpointWriter(fs).WriteCheckpointAsync(snapshot);

        var fromCheckpoint = await SnapshotBuilder.BuildAsync(log, new CheckpointReader(fs));
        var add = Assert.Single(fromCheckpoint.ActiveFiles).Value;

        Assert.NotNull(add.Stats);
        Assert.Same(add.Stats, add.GetStatsJson());
    }

    /// <summary>
    /// The visibility itself. Without this, re-marking either member <c>internal</c> would leave every
    /// test in this file passing — the test assembly can see internals — while breaking exactly the
    /// consumer the members exist for.
    /// </summary>
    [Theory]
    [InlineData(nameof(AddFile.GetStatsJson))]
    [InlineData(nameof(AddFile.GetNumRecords))]
    public void StatisticsAccessorsArePubliclyVisible(string methodName)
    {
        var method = typeof(AddFile).GetMethod(
            methodName, BindingFlags.Public | BindingFlags.Instance);

        Assert.True(method is not null,
            $"AddFile.{methodName} must stay public: it is the only way a consumer outside this " +
            "assembly can read the statistics of a file in a stats_parsed-only checkpoint.");
    }
}
