// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The write path builds <c>_change_type</c> run-end encoded — one run rather than one value per row,
/// since a change batch carries a single change type. That is an in-memory layout and it must not reach
/// anything outside the writer: the <c>_change_data</c> file holds the plain required UTF8 column the
/// Delta spec names, and the read path hands callers a plain string column.
///
/// <para>These assert the two boundaries the layout is contained by. Without them a change to the writer
/// could leak run-end encoding into the file — where Spark and delta-kernel would not find the column
/// they read the feed through — and nothing else in the suite would notice, because our own reader
/// recovers the values either way.</para>
/// </summary>
public class CdfChangeTypeLayoutTests : IDisposable
{
    private readonly string _tempDir;

    public CdfChangeTypeLayoutTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdftype_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private async Task<DeltaTable> CreateCdfTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 4, WriterFeatures = null },
            new MetadataAction
            {
                Id = "cdf-change-type-layout",
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

    private string SingleChangeDataFile()
    {
        var files = Directory.GetFiles(
            Path.Combine(_tempDir, "_change_data"), "*.parquet", SearchOption.AllDirectories);

        return Assert.Single(files);
    }

    [Fact]
    public async Task TheChangeDataFile_HoldsAPlainRequiredStringColumn()
    {
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        await table.WriteAsync([Rows(schema, (1, "a"))]);
        var pinned = table.CurrentSnapshot;

        var cdc = await table.WriteChangeDataFileAsync(
            Rows(schema, (2, "b"), (3, "c")), CdfConfig.Insert);
        await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: [cdc],
            expectedVersion: pinned.Version, operation: "WRITE");

        await using var file = new LocalRandomAccessFile(SingleChangeDataFile());
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        int index = batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);
        var field = batch.Schema.FieldsList[index];

        // The spec-defined column, exactly as the tiled form wrote it: UTF8 and required.
        Assert.IsType<Apache.Arrow.Types.StringType>(field.DataType);
        Assert.False(field.IsNullable);

        var column = Assert.IsType<StringArray>(batch.Column(index));
        Assert.Equal(2, column.Length);
        Assert.Equal(CdfConfig.Insert, column.GetString(0));
        Assert.Equal(CdfConfig.Insert, column.GetString(1));
    }

    [Fact]
    public async Task TheFeedHandedToCallers_CarriesAPlainStringColumn()
    {
        // The read path synthesizes _change_type for versions with no cdc file, and those batches go to
        // whoever asked for the feed — their schema expectations are not ours to change.
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        await table.WriteAsync([Rows(schema, (1, "a"), (2, "b"))]);

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(1, 1))
            batches.Add(b);

        var batch = Assert.Single(batches);
        int index = batch.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn);

        Assert.IsType<Apache.Arrow.Types.StringType>(batch.Schema.FieldsList[index].DataType);
        var column = Assert.IsType<StringArray>(batch.Column(index));
        Assert.Equal(2, column.Length);
        Assert.Equal(CdfConfig.Insert, column.GetString(0));
    }

    [Fact]
    public async Task AnUpdateFeed_RoundTripsBothChangeTypes()
    {
        // Two cdc files in one commit, each constant in its own change type — the shape the run form is
        // built for.
        await using var table = await CreateCdfTableAsync();
        var schema = table.ArrowSchema;
        await table.WriteAsync([Rows(schema, (1, "before"))]);

        await table.UpdateAsync(
            batch =>
            {
                var all = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++) all.Append(true);
                return all.Build();
            },
            batch =>
            {
                var ids = (Int64Array)batch.Column(0);
                var updated = new StringArray.Builder();
                for (int i = 0; i < batch.Length; i++) updated.Append("after");
                return new RecordBatch(batch.Schema, [ids, updated.Build()], batch.Length);
            });

        var seen = new List<(long Id, string ChangeType, string Value)>();
        await foreach (var b in table.ReadChangesAsync(2, 2))
        {
            var ids = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var values = (StringArray)b.Column(b.Schema.GetFieldIndex("value"));
            var types = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            for (int i = 0; i < b.Length; i++)
                seen.Add((ids.GetValue(i)!.Value, types.GetString(i), values.GetString(i)));
        }

        Assert.Contains(seen, s => s.ChangeType == CdfConfig.UpdatePreimage && s.Value == "before");
        Assert.Contains(seen, s => s.ChangeType == CdfConfig.UpdatePostimage && s.Value == "after");
    }
}
