// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class VacuumProtocolCheckIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public VacuumProtocolCheckIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_vpc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task Vacuum_WithProtocolCheck_SupportedFeatures_Succeeds()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors", "vacuumProtocolCheck"],
                WriterFeatures = ["deletionVectors", "vacuumProtocolCheck"],
            },
            new MetadataAction
            {
                Id = "vpc-ok",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        // Should succeed — all features are supported
        var result = await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: true);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Vacuum_WithProtocolCheck_UnsupportedFeature_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors", "vacuumProtocolCheck"],
                WriterFeatures = ["deletionVectors", "vacuumProtocolCheck", "liquidClustering"],
            },
            new MetadataAction
            {
                Id = "vpc-fail",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        // Should throw — liquidClustering is not supported
        await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: true).AsTask());
    }

    [Fact]
    public async Task Vacuum_WithoutProtocolCheck_UnknownWriterFeature_Succeeds()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                // No vacuumProtocolCheck in reader features
                WriterFeatures = ["liquidClustering"],
            },
            new MetadataAction
            {
                Id = "vpc-no-check",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        // Should succeed — no vacuumProtocolCheck, so unknown writer features are OK for vacuum
        var result = await table.VacuumAsync(retentionPeriod: TimeSpan.Zero, dryRun: true);
        Assert.NotNull(result);
    }
}
