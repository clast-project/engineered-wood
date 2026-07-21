// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row tracking (<c>delta.enableRowTracking=true</c>). APPENDS derive a row's stable id from
/// <c>add.baseRowId + position</c> (no hidden column). A copy-on-write REWRITE (UPDATE / OVERWRITE /
/// compaction) MATERIALIZES each surviving row's ORIGINAL id + commit version into the two declared hidden
/// physical columns, so ids survive the rewrite. These tests pin the stored enablement metadata, the append
/// behavior, and — by reading the raw data files — that a rewrite preserves the materialized ids.
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

    private Task<DeltaTable> CreateRowTrackingTable(bool enableDeletionVectors = false) =>
        DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema,
            enableRowTracking: true, enableDeletionVectors: enableDeletionVectors).AsTask();

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

    // Reads the RAW data files (bypassing EngineeredWood's row-id strip) and returns, per physical row, the
    // user 'value' plus the row's stable id + commit version — taken from the materialized hidden columns when
    // the file carries them (a rewrite output), else derived from add.baseRowId + position / defaultRowCommitVersion
    // (a fresh append). This is what a conformant reader (Spark) reconstructs; it lets the tests prove the
    // rewrite preserved ids without the Spark toolchain.
    private async Task<List<(string? Value, long Id, long Version)>> ReadRawWithIdsAsync(DeltaTable table)
    {
        var config = table.CurrentSnapshot.Metadata.Configuration!;
        string idCol = config[RowTrackingConfig.MaterializedRowIdColumnNameKey];
        string verCol = config[RowTrackingConfig.MaterializedRowCommitVersionColumnNameKey];
        var fs = new LocalTableFileSystem(_tempDir);
        var result = new List<(string?, long, long)>();

        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            HashSet<long>? deleted = null;
            if (addFile.DeletionVector is not null)
                deleted = await new DeletionVectors.DeletionVectorReader(fs)
                    .ReadAsync(addFile.DeletionVector, default);

            await using var file = await fs.OpenReadAsync(DeltaPath.Decode(addFile.Path));
            using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
            long pos = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                var values = (StringArray)batch.Column("value");
                var ids = ColumnOrNull(batch, idCol);
                var vers = ColumnOrNull(batch, verCol);
                for (int i = 0; i < batch.Length; i++)
                {
                    long abs = pos + i;
                    if (deleted is not null && deleted.Contains(abs))
                        continue; // soft-deleted row — a reader skips it
                    long id = ids is not null && !ids.IsNull(i)
                        ? ids.GetValue(i)!.Value : addFile.BaseRowId!.Value + abs;
                    long ver = vers is not null && !vers.IsNull(i)
                        ? vers.GetValue(i)!.Value : addFile.DefaultRowCommitVersion!.Value;
                    result.Add((values.GetString(i), id, ver));
                }
                pos += batch.Length;
            }
        }
        return result;
    }

    private static Int64Array? ColumnOrNull(RecordBatch batch, string name)
    {
        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
            if (batch.Schema.FieldsList[i].Name == name)
                return batch.Column(i) as Int64Array;
        return null;
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
    public async Task Update_PreservesRowIdsAndAdvancesVersionOfChangedRow()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2 ; version 1

        // Update the "b" row: value b -> B. Copy-on-write rewrite of the whole file.
        await table.UpdateAsync(
            b => Eq((StringArray)b.Column("value"), "b"),
            b => SetValue(b, "B"));

        // Exactly one active file (the rewrite), carrying the materialized ids for every row.
        var raw = await ReadRawWithIdsAsync(table);
        var byValue = raw.ToDictionary(r => r.Value!, r => (r.Id, r.Version));

        Assert.Equal(3, byValue.Count);
        Assert.Equal(0L, byValue["a"].Id);
        Assert.Equal(1L, byValue["B"].Id); // the changed row KEEPS its original id
        Assert.Equal(2L, byValue["c"].Id);

        // The changed row's commit version advances to the UPDATE's version (v2); untouched rows keep v1.
        Assert.Equal(2L, byValue["B"].Version);
        Assert.Equal(1L, byValue["a"].Version);
        Assert.Equal(1L, byValue["c"].Version);

        // The high-water mark never regresses (ids are never reused).
        Assert.True(RowTrackingConfig.TryReadHighWaterMark(table.GetDomainMetadata()) >= 2L);
    }

    [Fact]
    public async Task Update_ReadsBackCleanSchema_NoHiddenColumns()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]);
        await table.UpdateAsync(
            b => Eq((StringArray)b.Column("value"), "a"),
            b => SetValue(b, "A"));

        var read = new List<(long, string?)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            Assert.Equal(["id", "value"], batch.Schema.FieldsList.Select(f => f.Name).ToArray());
            var ids = (Int64Array)batch.Column("id");
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                read.Add((ids.GetValue(i)!.Value, values.GetString(i)));
        }
        read.Sort();
        Assert.Equal([(10L, "A"), (20L, "b")], read);
    }

    [Fact]
    public async Task SecondUpdate_PreservesIdsMaterializedByFirstUpdate()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        // First update rewrites the file, materializing ids 0,1,2. Second update must READ those materialized
        // ids and preserve them (rather than re-deriving baseRowId + position off the rewritten file).
        await table.UpdateAsync(b => Eq((StringArray)b.Column("value"), "b"), b => SetValue(b, "B"));
        await table.UpdateAsync(b => Eq((StringArray)b.Column("value"), "a"), b => SetValue(b, "A"));

        var byValue = (await ReadRawWithIdsAsync(table)).ToDictionary(r => r.Value!, r => r.Id);
        Assert.Equal(0L, byValue["A"]);
        Assert.Equal(1L, byValue["B"]);
        Assert.Equal(2L, byValue["c"]);
    }

    [Fact]
    public async Task Compaction_PreservesRowIds()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]); // ids 0,1 (file 1)
        await table.WriteAsync([Rows((30, "c"))]);            // id 2   (file 2)

        var version = await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue });
        Assert.NotNull(version);

        var byValue = (await ReadRawWithIdsAsync(table)).ToDictionary(r => r.Value!, r => r.Id);
        Assert.Equal(3, byValue.Count);
        Assert.Equal(0L, byValue["a"]);
        Assert.Equal(1L, byValue["b"]);
        Assert.Equal(2L, byValue["c"]);
    }

    [Fact]
    public async Task Overwrite_AssignsFreshRowIds()
    {
        await using var table = await CreateRowTrackingTable();
        await table.WriteAsync([Rows((10, "a"), (20, "b"))]); // ids 0,1 ; HWM -> 1

        await table.WriteAsync([Rows((30, "c"))], DeltaWriteMode.Overwrite);

        // The overwrite replaces all data; the new file's rows get FRESH ids continuing from the high-water
        // mark (id 2), never reusing the retired 0/1.
        var raw = await ReadRawWithIdsAsync(table);
        var row = Assert.Single(raw);
        Assert.Equal("c", row.Value);
        Assert.Equal(2L, row.Id);
    }

    [Fact]
    public async Task DeletionVectorDelete_KeepsPositionsAndIdsStable()
    {
        // With deletion vectors, a DELETE soft-marks rows without rewriting the file, so surviving rows keep
        // their physical positions — and thus their baseRowId + position ids — with no materialized column.
        await using var table = await CreateRowTrackingTable(enableDeletionVectors: true);
        await table.WriteAsync([Rows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        long baseRowIdBefore = table.CurrentSnapshot.ActiveFiles.Values.Single().BaseRowId!.Value;

        await table.DeleteAsync(b => Eq((StringArray)b.Column("value"), "b"));

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.NotNull(addFile.DeletionVector);            // soft delete, file not rewritten
        Assert.Equal(baseRowIdBefore, addFile.BaseRowId);  // baseRowId unchanged -> ids stable

        var byValue = (await ReadRawWithIdsAsync(table)).ToDictionary(r => r.Value!, r => r.Id);
        Assert.False(byValue.ContainsKey("b"));            // deleted
        Assert.Equal(0L, byValue["a"]);
        Assert.Equal(2L, byValue["c"]);
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

    private static BooleanArray Eq(StringArray col, string target)
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < col.Length; i++)
            b.Append(col.GetString(i) == target);
        return b.Build();
    }

    private static RecordBatch SetValue(RecordBatch batch, string newValue)
    {
        var values = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            values.Append(newValue);
        return new RecordBatch(Schema, [batch.Column("id"), values.Build()], batch.Length);
    }
}
