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
/// Row tracking (<c>delta.enableRowTracking=true</c>) supports APPENDS: a freshly-appended file's rows derive
/// their stable ids from <c>add.baseRowId + position</c>, which EngineeredWood assigns and records correctly.
/// A copy-on-write REWRITE (UPDATE / DELETE / OVERWRITE / compaction) is still refused, because it must carry
/// each surviving row's ORIGINAL id in a materialized column — the deferred Layer 3 (B) work. These tests pin
/// the stored enablement metadata, the append behavior, and the rewrite refusal.
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

    private static Apache.Arrow.Schema Schema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private Task<DeltaTable> CreateRowTrackingTable() =>
        DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), Schema, enableRowTracking: true).AsTask();

    private static RecordBatch Rows(params (long Id, string? Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }
        return new RecordBatch(Schema, [ids.Build(), values.Build()], rows.Length);
    }

    [Fact]
    public async Task CreateAsync_StoresRowTrackingMetadataAndFeatures()
    {
        await using var table = await CreateRowTrackingTable();

        var config = table.CurrentSnapshot.Metadata.Configuration!;
        Assert.Equal("true", config[RowTrackingConfig.EnableKey]);

        // The two hidden materialized-column names are fixed at enablement and must be present, non-empty,
        // and distinct (a reader consults them to find the per-row id/version columns a rewrite will write).
        string rowIdCol = config[RowTrackingConfig.MaterializedRowIdColumnNameKey];
        string rowCommitVersionCol = config[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey];
        Assert.False(string.IsNullOrEmpty(rowIdCol));
        Assert.False(string.IsNullOrEmpty(rowCommitVersionCol));
        Assert.NotEqual(rowIdCol, rowCommitVersionCol);

        // rowTracking is a writer-only feature (writer 7) depending on domainMetadata; the reader is untouched.
        var protocol = table.CurrentSnapshot.Protocol;
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("rowTracking", protocol.WriterFeatures!);
        Assert.Contains("domainMetadata", protocol.WriterFeatures!);
        Assert.True(protocol.ReaderFeatures is null || !protocol.ReaderFeatures.Contains("rowTracking"));
    }

    [Fact]
    public async Task Append_AssignsBaseRowIdAndDefaultCommitVersion()
    {
        await using var table = await CreateRowTrackingTable();

        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]);

        var addFile = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values);
        Assert.Equal(0L, addFile.BaseRowId);
        Assert.Equal(1L, addFile.DefaultRowCommitVersion); // create = v0, first append = v1

        // The domain high-water mark records the HIGHEST ASSIGNED id (3 rows: ids 0,1,2 -> highest 2).
        Assert.Equal(2L, RowTrackingConfig.TryReadHighWaterMark(table.GetDomainMetadata()));
    }

    [Fact]
    public async Task Append_WritesNoMaterializedRowIdColumn()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]);

        // A fresh append must NOT add a hidden physical column: a row's id is baseRowId + position. Reading
        // back yields exactly the user schema — no stray _row_id_* / __delta_row_id column leaks through.
        await foreach (var batch in table.ReadAllAsync())
        {
            var names = batch.Schema.FieldsList.Select(f => f.Name).ToArray();
            Assert.Equal(["id", "value"], names);
        }
    }

    [Fact]
    public async Task SecondAppend_ContinuesRowIdsFromHighWaterMark()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]);  // ids 0,1 -> next 2
        await table.WriteAsync([Rows((30, "c"))]);              // id 2   -> next 3

        var second = table.CurrentSnapshot.ActiveFiles.Values
            .OrderByDescending(f => f.BaseRowId ?? -1).First();
        Assert.Equal(2L, second.BaseRowId);
        Assert.Equal(2L, second.DefaultRowCommitVersion); // second append = v2
        Assert.Equal(2L, RowTrackingConfig.TryReadHighWaterMark(table.GetDomainMetadata()));
    }

    [Fact]
    public async Task Append_RoundTripsData()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, null))]);

        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var read = new List<(long, string?)>();
        await foreach (var batch in reopened.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                read.Add((ids.GetValue(i)!.Value, values.GetString(i)));
        }
        read.Sort();
        Assert.Equal([(10L, "a"), (20L, "b"), (30L, null)], read);
    }

    [Fact]
    public async Task Delete_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((1, "a"))]);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.DeleteAsync(_ => new BooleanArray.Builder().Build()));
        Assert.Contains("row-tracking", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Update_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((1, "a"))]);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.UpdateAsync(
                _ => new BooleanArray.Builder().Build(), b => b));
    }

    [Fact]
    public async Task Overwrite_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((1, "a"))]);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await table.WriteAsync([Rows((2, "b"))], DeltaWriteMode.Overwrite));
    }

    [Fact]
    public async Task Compaction_RejectedOnRowTrackingTable()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((1, "a"))]);
        await table.WriteAsync([Rows((2, "b"))]);

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
