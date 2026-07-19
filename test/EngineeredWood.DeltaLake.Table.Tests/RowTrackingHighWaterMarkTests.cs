// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The <c>delta.rowTracking</c> domainMetadata is the spec source of truth for the row-id high-water mark.
/// Deriving it from the active file set alone under-counts after a DELETE removes the highest-id file, which
/// would let a later writer reassign row ids that are still referenced by the table's history.
/// </summary>
public class RowTrackingHighWaterMarkTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingHighWaterMarkTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rthwm_{Guid.NewGuid():N}");
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
                WriterFeatures = ["rowTracking", "domainMetadata"],
            },
            new MetadataAction
            {
                Id = "rt-hwm-table",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string>
                {
                    { RowTrackingConfig.EnableKey, "true" },
                },
            },
        });

        return await DeltaTable.OpenAsync(fs, new DeltaTableOptions { CheckpointInterval = 0 });
    }

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params long[] ids)
    {
        var b = new Int64Array.Builder();
        foreach (var id in ids)
            b.Append(id);
        return new RecordBatch(schema, [b.Build()], ids.Length);
    }

    [Fact]
    public void BuildHighWaterMarkAction_RecordsHighestAssignedId()
    {
        // The domain stores the HIGHEST ASSIGNED id; the writer tracks the NEXT id to assign.
        var action = RowTrackingConfig.BuildHighWaterMarkAction(nextAvailableRowId: 5);

        Assert.Equal(RowTrackingConfig.DomainName, action.Domain);
        Assert.False(action.Removed);
        Assert.Contains("\"rowIdHighWaterMark\":4", action.Configuration);

        var roundTripped = RowTrackingConfig.TryReadHighWaterMark(
            new Dictionary<string, DomainMetadata> { [action.Domain] = action });
        Assert.Equal(4L, roundTripped);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("{\"rowIdHighWaterMark\":\"4\"}")] // string, not a number
    public void TryReadHighWaterMark_ReturnsNullOnUnusableConfiguration(string? configuration)
    {
        var dm = new Dictionary<string, DomainMetadata>
        {
            [RowTrackingConfig.DomainName] = new DomainMetadata
            {
                Domain = RowTrackingConfig.DomainName,
                Configuration = configuration!,
                Removed = false,
            },
        };

        Assert.Null(RowTrackingConfig.TryReadHighWaterMark(dm));
        Assert.Null(RowTrackingConfig.TryReadHighWaterMark(new Dictionary<string, DomainMetadata>()));
    }

    [Fact]
    public async Task Write_EmitsHighWaterMarkDomainMetadata()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows(table.ArrowSchema, 1, 2, 3)]);

        var domains = table.GetDomainMetadata();
        Assert.True(domains.ContainsKey(RowTrackingConfig.DomainName));
        // 3 rows assigned ids 0,1,2 → highest assigned is 2.
        Assert.Equal(2L, RowTrackingConfig.TryReadHighWaterMark(domains));
        Assert.Equal(3L, table.CurrentSnapshot.RowIdHighWaterMark);
    }

    [Fact]
    public async Task Write_AdvancesTheMarkAcrossCommits()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows(table.ArrowSchema, 1, 2)]);
        await table.WriteAsync([Rows(table.ArrowSchema, 3, 4, 5)]);

        Assert.Equal(4L, RowTrackingConfig.TryReadHighWaterMark(table.GetDomainMetadata()));
        Assert.Equal(5L, table.CurrentSnapshot.RowIdHighWaterMark);
    }

    // The case the domainMetadata exists for: once the highest-id file leaves the ACTIVE set (any writer that
    // tombstones a file — compaction, a copy-on-write rewrite, an external engine), the active-file derivation
    // drops back to the surviving files. The committed mark must hold the line, or the next writer reassigns
    // row ids that the table's history still references.
    [Fact]
    public async Task RemovingTheHighestFile_DoesNotRewindTheMark()
    {
        var fs = new LocalTableFileSystem(_tempDir);

        await using (var table = await CreateRowTrackingTable())
        {
            await table.WriteAsync([Rows(table.ArrowSchema, 1, 2, 3)]);
            await table.WriteAsync([Rows(table.ArrowSchema, 4, 5, 6)]);
            Assert.Equal(6L, table.CurrentSnapshot.RowIdHighWaterMark);
        }

        // Tombstone the file holding the highest ids (baseRowId 3) without adding a replacement.
        DeltaTable opened = await DeltaTable.OpenAsync(fs);
        var highest = opened.CurrentSnapshot.ActiveFiles.Values
            .OrderByDescending(f => f.BaseRowId ?? -1).First();
        Assert.Equal(3L, highest.BaseRowId);
        long version = opened.CurrentSnapshot.Version;
        await opened.DisposeAsync();

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(version + 1, new List<DeltaAction>
        {
            new RemoveFile
            {
                Path = highest.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = highest.PartitionValues,
                Size = highest.Size,
            },
        });

        // Reopen so the mark is rebuilt from the log rather than carried in memory.
        await using var reopened = await DeltaTable.OpenAsync(fs);

        Assert.Single(reopened.CurrentSnapshot.ActiveFiles);
        Assert.Equal(5L, RowTrackingConfig.TryReadHighWaterMark(reopened.GetDomainMetadata()));
        // The surviving file alone derives only 3 — the domainMetadata reconciliation keeps it at 6.
        Assert.Equal(6L, reopened.CurrentSnapshot.RowIdHighWaterMark);
    }

    [Fact]
    public async Task NonRowTrackingTable_EmitsNoDomainMetadata()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Apache.Arrow.Types.Int64Type.Default, false))
            .Build();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Rows(schema, 1, 2)]);

        Assert.False(table.GetDomainMetadata().ContainsKey(RowTrackingConfig.DomainName));
    }
}
