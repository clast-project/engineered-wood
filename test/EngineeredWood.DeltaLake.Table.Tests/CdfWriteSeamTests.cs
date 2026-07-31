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

/// <summary>
/// The CDF WRITE half of the buffered-transaction seam: <see cref="DeltaTable.WriteChangeDataFileAsync"/> writes a
/// <c>_change_data</c> file WITHOUT committing and returns the <see cref="CdcFile"/> action, which a multi-statement
/// transaction fuses into ONE atomic version via <see cref="DeltaTable.CommitDataFilesAsync"/>' <c>extraActions</c>.
/// The written feed round-trips through <see cref="DeltaTable.ReadChangesAsync"/> (a version carrying cdc actions is
/// read cdc-only).
/// </summary>
public class CdfWriteSeamTests : IDisposable
{
    private readonly string _tempDir;

    public CdfWriteSeamTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfwrite_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // Creates a CDF-enabled table (id: long, value: string) — the public CreateAsync has no CDF switch, so the
    // property is set on the initial metadata directly (same approach as ChangeDataFeedTests).
    private async Task<DeltaTable> CreateCdfTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 4, WriterFeatures = null },
            new MetadataAction
            {
                Id = "cdf-write-seam",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string> { { CdfConfig.EnableKey, "true" } },
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    private static RecordBatch Rows(Apache.Arrow.Schema schema, params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows) { ids.Append(id); values.Append(value); }
        return new RecordBatch(schema, [ids.Build(), values.Build()], rows.Length);
    }

    private static async Task<List<RecordBatch>> ReadChangesAsync(DeltaTable table, long from, long to)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = from, EndVersion = to }))
            list.Add(b);
        return list;
    }

    [Fact]
    public async Task WriteChangeDataFile_FusedWithAppend_ReadCdcOnlyForThatVersion()
    {
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        await table.WriteAsync([Rows(schema, (1, "a"), (2, "b"))]); // v1
        var pinned = table.CurrentSnapshot;

        // Buffered flush: real data files for the new rows + an eager "insert" CDC file for the same rows,
        // fused into ONE version.
        var newRows = Rows(schema, (3, "c"), (4, "d"));
        var files = await table.WriteDataFilesAsync([newRows]);
        var cdc = await table.WriteChangeDataFileAsync(newRows, CdfConfig.Insert);

        long committed = await table.CommitDataFilesAsync(
            files, DeltaWriteMode.Append, extraActions: [cdc],
            expectedVersion: pinned.Version, operation: "WRITE");
        Assert.Equal(pinned.Version + 1, committed);

        // The data landed: the table now has all four rows.
        long total = 0;
        await foreach (var b in table.ReadAllAsync()) total += b.Length;
        Assert.Equal(4, total);

        // The version carries a CdcFile → the feed for it is read cdc-ONLY (the inferred inserts are ignored,
        // so no double-count): exactly the two rows we captured, tagged insert, at the committed version.
        var changes = await ReadChangesAsync(table, committed, committed);
        var seen = new List<(long Id, string Ct, long Ver)>();
        foreach (var b in changes)
        {
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var ct = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var ver = (Int64Array)b.Column(b.Schema.GetFieldIndex(CdfConfig.CommitVersionColumn));
            for (int i = 0; i < b.Length; i++)
                seen.Add((id.GetValue(i)!.Value, ct.GetString(i), ver.GetValue(i)!.Value));
        }
        Assert.Equal(2, seen.Count);
        Assert.All(seen, s => Assert.Equal(CdfConfig.Insert, s.Ct));
        Assert.All(seen, s => Assert.Equal(committed, s.Ver));
        Assert.Contains(seen, s => s.Id == 3);
        Assert.Contains(seen, s => s.Id == 4);
    }

    [Fact]
    public async Task WriteChangeDataFile_DeleteFeed_RoundTripsThroughReadChanges()
    {
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        await table.WriteAsync([Rows(schema, (1, "a"))]); // v1
        var pinned = table.CurrentSnapshot;

        // A CDC-only fused commit (no data files): the helper writes the change rows, the action carries them
        // into the version, and ReadChangesAsync surfaces them verbatim under the requested change type.
        var cdc = await table.WriteChangeDataFileAsync(
            Rows(schema, (7, "gone")), CdfConfig.Delete);
        long committed = await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: [cdc],
            expectedVersion: pinned.Version, operation: "DELETE");

        var changes = await ReadChangesAsync(table, committed, committed);
        var b = Assert.Single(changes);
        Assert.Equal(1, b.Length);
        var ct = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
        Assert.Equal(CdfConfig.Delete, ct.GetString(0));
        var value = (StringArray)b.Column(b.Schema.GetFieldIndex("value"));
        Assert.Equal("gone", value.GetString(0));
    }

    [Fact]
    public async Task WriteChangeDataFile_TagsPartitionValuesOnTheAction()
    {
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        var pv = new Dictionary<string, string> { { "region", "emea" } };

        var cdc = await table.WriteChangeDataFileAsync(
            Rows(schema, (1, "x")), CdfConfig.Insert, partitionValues: pv);

        Assert.True(cdc.PartitionValues.TryGetValue("region", out var region));
        Assert.Equal("emea", region);
        Assert.False(cdc.DataChange); // CDC files never count as a data change
    }

    [Fact]
    public async Task WriteChangeDataFile_RejectsUnknownChangeType()
    {
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        await Assert.ThrowsAsync<ArgumentException>(() =>
            table.WriteChangeDataFileAsync(Rows(schema, (1, "x")), "bogus").AsTask());
    }

    [Fact]
    public async Task WriteChangeDataFile_RejectsWhenCdfDisabled()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema); // CDF NOT enabled

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.WriteChangeDataFileAsync(Rows(schema, (1, "x")), CdfConfig.Insert).AsTask());
    }
}
