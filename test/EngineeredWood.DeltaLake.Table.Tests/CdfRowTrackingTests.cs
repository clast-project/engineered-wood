// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row tracking as seen through the Change Data Feed.
///
/// <para><see cref="DeltaTable.ReadChangesAsync"/> opens change files and data files directly rather than
/// going through the main read path, so it never stripped the hidden MATERIALIZED row-tracking columns the
/// way <c>ReadFileAsync</c> does, and they reached the feed as two extra Int64 columns.</para>
///
/// <para>On UNPARTITIONED tables only, as it turns out. A partitioned table re-materializes its partition
/// columns by walking the table schema and consuming data columns positionally, which takes exactly the user
/// columns and leaves the hidden ones off the end — so there they were already being dropped, by accident
/// rather than by intent. Worth recording, because a partitioned test here would pass either way and look
/// like coverage.</para>
/// </summary>
public class CdfRowTrackingTests : IDisposable
{
    private readonly string _tempDir;

    public CdfRowTrackingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdfrt_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private Task<DeltaTable> CreateAsync()
        => DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), TableSchema,
            enableRowTracking: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" })
            .AsTask();

    private static RecordBatch Rows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        foreach (var (id, value) in rows) { ids.Append(id); vals.Append(value); }
        return new RecordBatch(TableSchema, [ids.Build(), vals.Build()], rows.Length);
    }

    // Rewrites the "value" of every row whose id is in `ids`, leaving the rest untouched.
    private static Task<(long RowsUpdated, long Version)> UpdateValueAsync(
        DeltaTable table, string newValue, params long[] ids)
    {
        var target = new HashSet<long>(ids);
        return table.UpdateAsync(
            b =>
            {
                var col = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < b.Length; i++)
                    mask.Append(target.Contains(col.GetValue(i)!.Value));
                return mask.Build();
            },
            b =>
            {
                var vals = new StringArray.Builder();
                for (int i = 0; i < b.Length; i++) vals.Append(newValue);
                var columns = new IArrowArray[b.ColumnCount];
                for (int i = 0; i < b.ColumnCount; i++) columns[i] = b.Column(i);
                columns[b.Schema.GetFieldIndex("value")] = vals.Build();
                return new RecordBatch(b.Schema, columns, b.Length);
            }).AsTask();
    }

    private static async Task<List<RecordBatch>> CollectAsync(IAsyncEnumerable<RecordBatch> source)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in source)
            list.Add(b);
        return list;
    }

    [Fact]
    public async Task ReadChanges_DoesNotLeakTheHiddenMaterializedColumns()
    {
        await using var table = await CreateAsync();
        var (matIdName, matVerName) = RowTrackingConfig.TryGetMaterializedColumnNames(
            table.CurrentSnapshot.Metadata.Configuration);
        Assert.NotNull(matIdName);
        Assert.NotNull(matVerName);

        await table.WriteAsync([Rows((1, "a"), (2, "b"), (3, "c"))]);
        await UpdateValueAsync(table, "B", 2);   // the rewritten file now CARRIES materialized columns
        await table.WriteAsync([Rows((9, "z"))], DeltaWriteMode.Overwrite);

        // The overwrite writes no change file, so its version is INFERRED from the add/remove — which means
        // reading the removed file, the one the update left materialized columns in.
        var batches = await CollectAsync(table.ReadChangesAsync(0, table.CurrentSnapshot.Version));
        Assert.NotEmpty(batches);
        foreach (var b in batches)
        {
            var names = b.Schema.FieldsList.Select(f => f.Name).ToList();
            Assert.DoesNotContain(matIdName!, names);
            Assert.DoesNotContain(matVerName!, names);
            // The feed is the user columns plus exactly the three the spec names.
            Assert.Equal(["id", "value", "_change_type", "_commit_version", "_commit_timestamp"], names);
        }
    }
}
