// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class InCommitTimestampIntegrationTests : IDisposable
{
    private readonly string _tempDir;

    public InCommitTimestampIntegrationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_ict_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateTableWithInCommitTimestamps()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Create table with in-commit timestamps enabled
        var commitInfo = InCommitTimestamp.CreateCommitInfo(
            1700000000000, "CREATE TABLE");

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            commitInfo,
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                WriterFeatures = ["inCommitTimestamp"],
            },
            new MetadataAction
            {
                Id = "ict-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { InCommitTimestamp.EnableKey, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    [Fact]
    public async Task WriteCommit_AddsInCommitTimestamp()
    {
        await using var table = await CreateTableWithInCommitTimestamps();

        var schema = table.ArrowSchema;
        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        var batch = new RecordBatch(schema, [ids, values], 2);

        await table.WriteAsync([batch]);

        // The snapshot should have an in-commit timestamp
        Assert.NotNull(table.CurrentSnapshot.InCommitTimestamp);
        Assert.True(table.CurrentSnapshot.InCommitTimestamp > 0);
    }

    [Fact]
    public async Task TimeTravelByTimestamp_FindsCorrectVersion()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Version 0: create table at t=1000
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(1000, "CREATE TABLE"),
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                WriterFeatures = ["inCommitTimestamp"],
            },
            new MetadataAction
            {
                Id = "tt-ict",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { InCommitTimestamp.EnableKey, "true" },
                },
            },
        });

        // Version 1: add file at t=2000
        await log.WriteCommitAsync(1, new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(2000, "WRITE"),
            new AddFile
            {
                Path = "v1.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100, ModificationTime = 2000, DataChange = true,
            },
        });

        // Version 2: add file at t=3000
        await log.WriteCommitAsync(2, new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(3000, "WRITE"),
            new AddFile
            {
                Path = "v2.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 200, ModificationTime = 3000, DataChange = true,
            },
        });

        // Version 3: add file at t=4000
        await log.WriteCommitAsync(3, new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(4000, "WRITE"),
            new AddFile
            {
                Path = "v3.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 300, ModificationTime = 4000, DataChange = true,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        // Time travel to t=2500 — should get version 1 (2 files: v0 has none, v1 has 1)
        var snapshot = await table.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(2500));
        Assert.Equal(1L, snapshot.Version);
        Assert.Equal(1, snapshot.FileCount);

        // Time travel to t=3000 — should get version 2
        var snapshot2 = await table.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(3000));
        Assert.Equal(2L, snapshot2.Version);
        Assert.Equal(2, snapshot2.FileCount);

        // Time travel to t=5000 — should get version 3 (latest)
        var snapshot3 = await table.GetSnapshotAtTimestampAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(5000));
        Assert.Equal(3L, snapshot3.Version);
        Assert.Equal(3, snapshot3.FileCount);
    }

    [Fact]
    public async Task TimeTravelByTimestamp_BeforeFirstCommit_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(5000, "CREATE TABLE"),
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["inCommitTimestamp"],
            },
            new MetadataAction
            {
                Id = "tt-early",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { InCommitTimestamp.EnableKey, "true" },
                },
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        // Time travel to before the first commit
        await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.GetSnapshotAtTimestampAsync(
                DateTimeOffset.FromUnixTimeMilliseconds(1000)).AsTask());
    }

    [Fact]
    public async Task SnapshotTracksTimestamp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        long expectedTs = 1700000000000;

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            InCommitTimestamp.CreateCommitInfo(expectedTs, "CREATE TABLE"),
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "ts-track",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(expectedTs, table.CurrentSnapshot.InCommitTimestamp);
    }

    [Fact]
    public async Task ProtocolFeature_InCommitTimestamp_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                WriterFeatures = ["inCommitTimestamp"],
            },
            new MetadataAction
            {
                Id = "feat-ict",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }

    [Fact]
    public async Task TableWithoutInCommitTimestamps_CommitInfoTimestampTracked()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var ids = new Int64Array.Builder().Append(1).Build();
        var batch = new RecordBatch(schema, [ids], 1);
        await table.WriteAsync([batch]);

        // commitInfo (with a timestamp) is written on every commit, so timestamp resolution works
        // even without the inCommitTimestamp feature — only the in-protocol monotonic guarantee
        // requires enabling it.
        Assert.NotNull(table.CurrentSnapshot.InCommitTimestamp);
    }
}
