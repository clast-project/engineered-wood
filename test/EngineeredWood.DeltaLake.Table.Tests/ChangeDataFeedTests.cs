// Copyright (c) clast-project. All rights reserved.
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
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 1 }))
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
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 2, EndVersion = 2 }))
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

    // A commit carrying no cdc action (an OVERWRITE writes none) is read by INFERENCE: the removed file's rows
    // become deletes, the added file's rows become inserts. Rows a file's DELETION VECTOR already marked deleted
    // are not live at that moment — they were reported as deletes when the DV committed — so materializing them
    // again double-counts. Both directions are checked here: the removed file's DV must exclude rows from the
    // inferred deletes, and the added file's DV must exclude rows from the inferred inserts.
    [Fact]
    public async Task ReadChanges_Inferred_HonorsDeletionVectors()
    {
        await using var table = await CreateCdfTable();
        var schema = table.ArrowSchema;

        // v1: five rows in one file.
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Append(4).Append(5).Build();
        var values = new StringArray.Builder()
            .Append("a").Append("b").Append("c").Append("d").Append("e").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, values], 5)]);

        // v2: soft-delete id=2 through a deletion vector. This commit writes its own cdc file (CDF is on), so
        // the delete of id=2 is reported exactly once, here.
        await table.DeleteAsync(batch =>
        {
            var idCol = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(idCol.GetValue(i)!.Value == 2);
            return builder.Build();
        });

        // v3: full overwrite — removes the DV-carrying file and adds a fresh one. The overwrite path emits no
        // cdc action, so version 3 is the inference case.
        var newIds = new Int64Array.Builder().Append(9).Build();
        var newValues = new StringArray.Builder().Append("z").Build();
        await table.WriteAsync(
            [new RecordBatch(schema, [newIds, newValues], 1)], DeltaWriteMode.Overwrite);

        var deleted = new List<long>();
        var inserted = new List<long>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 3, EndVersion = 3 }))
        {
            var idCol = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var ct = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            for (int i = 0; i < b.Length; i++)
            {
                long id = idCol.GetValue(i)!.Value;
                switch (ct.GetString(i))
                {
                    case CdfConfig.Delete: deleted.Add(id); break;
                    case CdfConfig.Insert: inserted.Add(id); break;
                }
            }
        }

        // id=2 was already reported deleted at version 2 — the overwrite must not report it a second time.
        Assert.Equal([1L, 3L, 4L, 5L], deleted.OrderBy(x => x).ToArray());
        Assert.Equal([9L], inserted.ToArray());
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
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 2, EndVersion = 2 }))
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
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 2 }))
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
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 1 }))
            changes.Add(b);

        Assert.NotEmpty(changes);
        var ct = (StringArray)changes[0].Column(
            changes[0].Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
        Assert.Equal("insert", ct.GetString(0));
    }

    // The add side of the same rule, on a table where CDF is OFF so a DV delete produces no cdc file and the
    // whole commit is inferred: the remove carries the file's PRE-image DV (none here, so all five rows are
    // deletes) and the add carries the new DV, so only the four survivors are inserts. Net: id=2 deleted.
    // Without the add-side filter the survivors and the deleted row all re-insert, and the delete disappears.
    //
    // NOTE: this mode is EW-ONLY and has no reference oracle. Measured against Spark 4.0: asking for a change
    // feed over a version where delta.enableChangeDataFeed was never set fails with DELTA_MISSING_CHANGE_DATA
    // rather than inferring anything. EW deliberately answers instead (see ReadChanges_WithoutCdf_InfersChanges)
    // — so these expectations are EW's own semantics, not a conformance claim. The remove side, which DOES have
    // a Spark counterpart, is pinned cross-engine by
    // SparkInteropTests.EwWritten_CdfInference_OverDeletionVector_MatchesSparkFeed.
    [Fact]
    public async Task ReadChanges_Inferred_DvDeleteWithoutCdf_ReportsNetDelete()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, enableDeletionVectors: true);

        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Append(4).Append(5).Build();
        await table.WriteAsync([new RecordBatch(schema, [ids], 5)]);

        // v2: soft-delete id=2 via a deletion vector. CDF is off, so this commit carries no cdc action.
        await table.DeleteAsync(batch =>
        {
            var idCol = (Int64Array)batch.Column(0);
            var builder = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                builder.Append(idCol.GetValue(i)!.Value == 2);
            return builder.Build();
        });

        var deleted = new List<long>();
        var inserted = new List<long>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = 2, EndVersion = 2 }))
        {
            var idCol = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var ct = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            for (int i = 0; i < b.Length; i++)
            {
                long id = idCol.GetValue(i)!.Value;
                switch (ct.GetString(i))
                {
                    case CdfConfig.Delete: deleted.Add(id); break;
                    case CdfConfig.Insert: inserted.Add(id); break;
                }
            }
        }

        Assert.Equal([1L, 2L, 3L, 4L, 5L], deleted.OrderBy(x => x).ToArray());
        Assert.Equal([1L, 3L, 4L, 5L], inserted.OrderBy(x => x).ToArray());
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
