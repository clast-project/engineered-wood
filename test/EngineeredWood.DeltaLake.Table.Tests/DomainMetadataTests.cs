// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class DomainMetadataTests : IDisposable
{
    private readonly string _tempDir;

    public DomainMetadataTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dm_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task SetAndGet_UserDomain()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        // No domain metadata initially
        Assert.Empty(table.GetDomainMetadata());

        // Set a user domain
        await table.SetDomainMetadataAsync("myApp.config", """{"retentionDays":30}""");

        // Verify it's set
        Assert.Single(table.GetDomainMetadata());
        Assert.Equal("""{"retentionDays":30}""", table.GetDomainMetadata("myApp.config"));
    }

    [Fact]
    public async Task SetDomain_UpdatesExisting()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.SetDomainMetadataAsync("myApp", """{"v":1}""");
        Assert.Equal("""{"v":1}""", table.GetDomainMetadata("myApp"));

        await table.SetDomainMetadataAsync("myApp", """{"v":2}""");
        Assert.Equal("""{"v":2}""", table.GetDomainMetadata("myApp"));

        // Still only one domain
        Assert.Single(table.GetDomainMetadata());
    }

    [Fact]
    public async Task RemoveDomain_SetsTombstone()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.SetDomainMetadataAsync("myApp", """{"key":"value"}""");
        Assert.Single(table.GetDomainMetadata());

        await table.RemoveDomainMetadataAsync("myApp");
        Assert.Empty(table.GetDomainMetadata());
        Assert.Null(table.GetDomainMetadata("myApp"));
    }

    [Fact]
    public async Task RemoveDomain_NonExistent_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => table.RemoveDomainMetadataAsync("nonexistent").AsTask());
    }

    [Fact]
    public async Task SetDomain_SystemDomain_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.SetDomainMetadataAsync("delta.myFeature", "{}").AsTask());
    }

    [Fact]
    public async Task RemoveDomain_SystemDomain_Throws()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        // Manually create a table with a system domain
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "dm-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
            new DomainMetadata
            {
                Domain = "delta.someFeature",
                Configuration = """{"enabled":true}""",
                Removed = false,
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);

        // Should be visible
        Assert.Equal("""{"enabled":true}""", table.GetDomainMetadata("delta.someFeature"));

        // But cannot be removed by user
        await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.RemoveDomainMetadataAsync("delta.someFeature").AsTask());
    }

    [Fact]
    public async Task MultipleDomains_IndependentLifecycle()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.SetDomainMetadataAsync("analytics", """{"enabled":true}""");
        await table.SetDomainMetadataAsync("retention", """{"days":90}""");
        await table.SetDomainMetadataAsync("governance", """{"owner":"team-a"}""");

        Assert.Equal(3, table.GetDomainMetadata().Count);

        // Remove one
        await table.RemoveDomainMetadataAsync("retention");
        Assert.Equal(2, table.GetDomainMetadata().Count);
        Assert.Null(table.GetDomainMetadata("retention"));
        Assert.NotNull(table.GetDomainMetadata("analytics"));
        Assert.NotNull(table.GetDomainMetadata("governance"));
    }

    [Fact]
    public async Task DomainMetadata_SurvivesReopen()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using (var table = await DeltaTable.CreateAsync(fs, schema))
        {
            await table.SetDomainMetadataAsync("myApp", """{"setting":"value"}""");
        }

        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal("""{"setting":"value"}""", reopened.GetDomainMetadata("myApp"));
    }

    [Fact]
    public async Task DomainMetadata_SurvivesCheckpoint()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        var options = new DeltaTableOptions { CheckpointInterval = 2 };
        await using var table = await DeltaTable.CreateAsync(fs, schema, options);

        await table.SetDomainMetadataAsync("myApp", """{"key":"val"}""");

        // Write data to trigger checkpoint (version 2, interval=2)
        var ids = new Int64Array.Builder().Append(1).Build();
        var batch = new RecordBatch(schema, [ids], 1);
        await table.WriteAsync([batch]); // This should be version 2 → triggers checkpoint

        // Re-open (will bootstrap from checkpoint)
        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Equal("""{"key":"val"}""", reopened.GetDomainMetadata("myApp"));
    }

    [Fact]
    public async Task Protocol_DomainMetadataFeature_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["deletionVectors"],
                WriterFeatures = ["deletionVectors", "domainMetadata"],
            },
            new MetadataAction
            {
                Id = "dm-feature-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}
