// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row tracking (<c>delta.enableRowTracking=true</c>) is READ-ONLY in this library for now: EngineeredWood
/// cannot yet maintain stable row IDs through appends and copy-on-write rewrites, so a data-changing write
/// to a row-tracking table is refused rather than silently corrupting it (a spec-conformant writer — the
/// deferred Layer 3 (B) work — would lift this). These tests pin the refusal and that reads / non-tracking
/// writes are unaffected.
/// </summary>
public class RowTrackingTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateRowTrackingTable()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["rowTracking"],
            },
            new MetadataAction
            {
                Id = "rt-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { RowTrackingConfig.EnableKey, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs);
    }

    private RecordBatch OneRow(Apache.Arrow.Schema schema) => new(
        schema,
        [new Int64Array.Builder().Append(1).Build(),
         new StringArray.Builder().Append("a").Build()], 1);

    [Fact]
    public async Task Write_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.WriteAsync([OneRow(table.ArrowSchema)]));
        Assert.Contains("row-tracking", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Delete_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();

        // The write-precondition gate fires before any file is read, so an empty table is enough.
        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.DeleteAsync(_ => new BooleanArray.Builder().Build()));
    }

    [Fact]
    public async Task Update_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.UpdateAsync(
                _ => new BooleanArray.Builder().Build(), b => b));
    }

    [Fact]
    public async Task Compaction_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));
    }

    [Fact]
    public async Task NonRowTracking_NoBaseRowId()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);

        var batch = new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build()], 1);
        await table.WriteAsync([batch]);

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.First();
        Assert.Null(addFile.BaseRowId);
        Assert.Null(addFile.DefaultRowCommitVersion);
    }

    [Fact]
    public async Task ProtocolFeature_RowTracking_Accepted()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["rowTracking"],
            },
            new MetadataAction
            {
                Id = "rt-feat",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        });

        await using var table = await DeltaTable.OpenAsync(fs);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
    }
}
