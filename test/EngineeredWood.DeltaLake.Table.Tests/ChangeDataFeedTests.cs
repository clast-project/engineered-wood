// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class ChangeDataFeedTests : IDisposable
{
    private readonly string _tempDir;

    public ChangeDataFeedTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdf_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateCdfTable()
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
                WriterFeatures = ["changeDataFeed", "deletionVectors"],
            },
            new MetadataAction
            {
                Id = "cdf-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { CdfConfig.EnableKey, "true" },
                    { EngineeredWood.DeltaLake.DeletionVectors.DeletionVectorConfig.EnableKey, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    [Fact]
    public async Task ReadChanges_InsertOnly_InfersFromAddActions()
    {
        await using var table = await CreateCdfTable();
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 2)]);

        // Read changes for version 1 (the write)
        var changes = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(1, 1))
            changes.Add(b);

        Assert.NotEmpty(changes);
        int totalRows = changes.Sum(b => b.Length);
        Assert.Equal(2, totalRows);

        // Check _change_type column
        foreach (var b in changes)
        {
            int ctIdx = b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
            Assert.True(ctIdx >= 0, "Should have _change_type column");
            var ct = (StringArray)b.Column(ctIdx);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal("insert", ct.GetString(i));

            // Check _commit_version column
            int cvIdx = b.Schema.GetFieldIndex(CdfConfig.CommitVersionColumn);
            Assert.True(cvIdx >= 0, "Should have _commit_version column");
            var cv = (Int64Array)b.Column(cvIdx);
            for (int i = 0; i < b.Length; i++)
                Assert.Equal(1L, cv.GetValue(i));
        }
    }

    [Fact]
    public async Task Delete_ProducesCdcFiles()
    {
        await using var table = await CreateCdfTable();
        var schema = table.ArrowSchema;

        // Write initial data
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var values = new StringArray.Builder().Append("a").Append("b").Append("c").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 3)]);

        // Delete row with id=2
        await table.DeleteAsync(batch =>
        {
            var idCol = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(idCol.GetValue(i)!.Value == 2);
            return builder.Build();
        });

        // Read changes for the delete version
        var changes = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(2, 2))
            changes.Add(b);

        // Should have CDC file with "delete" change type
        Assert.NotEmpty(changes);

        var deleteChanges = new List<(long Id, string ChangeType)>();
        foreach (var b in changes)
        {
            int ctIdx = b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
            int idIdx = b.Schema.GetFieldIndex("id");
            if (ctIdx >= 0 && idIdx >= 0)
            {
                var ct = (StringArray)b.Column(ctIdx);
                var idCol = (Int64Array)b.Column(idIdx);
                for (int i = 0; i < b.Length; i++)
                    deleteChanges.Add((idCol.GetValue(i)!.Value, ct.GetString(i)));
            }
        }

        Assert.Contains(deleteChanges, c => c.Id == 2 && c.ChangeType == "delete");
    }

    [Fact]
    public async Task Update_ProducesPreimageAndPostimage()
    {
        await using var table = await CreateCdfTable();
        var schema = table.ArrowSchema;

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var values = new StringArray.Builder().Append("old1").Append("old2").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 2)]);

        // Update: set value = "NEW" where id == 1
        await table.UpdateAsync(
            predicate: batch =>
            {
                var idCol = (Int64Array)batch.Column(0);
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    builder.Append(idCol.GetValue(i)!.Value == 1);
                return builder.Build();
            },
            updater: batch =>
            {
                var newValues = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    newValues.Append("NEW");
                return new RecordBatch(batch.Schema,
                    [batch.Column(0), newValues.Build()], batch.Length);
            });

        // Read changes for the update version
        var changes = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(2, 2))
            changes.Add(b);

        // Should have both preimage and postimage CDC entries
        var allChanges = new List<(string ChangeType, string Value)>();
        foreach (var b in changes)
        {
            int ctIdx = b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
            int valIdx = b.Schema.GetFieldIndex("value");
            if (ctIdx >= 0 && valIdx >= 0)
            {
                var ct = (StringArray)b.Column(ctIdx);
                var vals = (StringArray)b.Column(valIdx);
                for (int i = 0; i < b.Length; i++)
                    allChanges.Add((ct.GetString(i), vals.GetString(i)));
            }
        }

        Assert.Contains(allChanges, c =>
            c.ChangeType == "update_preimage" && c.Value == "old1");
        Assert.Contains(allChanges, c =>
            c.ChangeType == "update_postimage" && c.Value == "NEW");
    }

    [Fact]
    public async Task ReadChanges_MultipleVersions()
    {
        await using var table = await CreateCdfTable();
        var schema = table.ArrowSchema;

        // Version 1: insert
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(),
             new StringArray.Builder().Append("a").Build()], 1)]);

        // Version 2: insert more
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(2).Build(),
             new StringArray.Builder().Append("b").Build()], 1)]);

        // Read changes across versions 1-2
        var changes = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(1, 2))
            changes.Add(b);

        int totalRows = changes.Sum(b => b.Length);
        Assert.Equal(2, totalRows);

        // Verify commit versions
        var versions = new HashSet<long>();
        foreach (var b in changes)
        {
            int cvIdx = b.Schema.GetFieldIndex(CdfConfig.CommitVersionColumn);
            var cv = (Int64Array)b.Column(cvIdx);
            for (int i = 0; i < b.Length; i++)
                versions.Add(cv.GetValue(i)!.Value);
        }

        Assert.Contains(1L, versions);
        Assert.Contains(2L, versions);
    }

    [Fact]
    public async Task ReadChanges_WithoutCdf_InfersChanges()
    {
        // Create a table WITHOUT CDF enabled
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1)]);

        // ReadChanges should still work — infers from add actions
        var changes = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(1, 1))
            changes.Add(b);

        Assert.NotEmpty(changes);
        var ct = (StringArray)changes[0].Column(
            changes[0].Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
        Assert.Equal("insert", ct.GetString(0));
    }

    [Fact]
    public async Task ProtocolFeature_ChangeDataFeed_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["changeDataFeed"],
            },
            new MetadataAction
            {
                Id = "cdf-feat",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}
