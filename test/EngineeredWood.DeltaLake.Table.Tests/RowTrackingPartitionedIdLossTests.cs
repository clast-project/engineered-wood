// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row tracking on a PARTITIONED table. <see cref="RowTrackingTests"/> covers the same preservation rules
/// unpartitioned; these exist because the read path builds its file-level column list differently once a table
/// has partitions, and that difference is invisible until a rewrite has to READ BACK a materialized id.
///
/// <para><c>DeltaTable.ReadFileAsync</c> asks the parquet reader for an explicit column list whenever the table
/// is partitioned — the schema's fields minus the partition columns — instead of the null (= all columns) it
/// passes for an unpartitioned unprojected read. The hidden materialized row-tracking columns are not schema
/// fields, so they are never in that list and never come back. <c>StripMaterializedColumns</c> then finds
/// nothing, and each row's stable id silently falls back to <c>add.baseRowId + position</c> — which, on a file
/// that is ITSELF a rewrite output, is a fresh id, not the row's original one.</para>
///
/// <para>Scope: this is a property of <c>ReadFileAsync</c>, so it reaches UPDATE, copy-on-write DELETE, and
/// <c>ReadRowsAsync</c>. Compaction reads raw parquet with no column list and is unaffected.</para>
/// </summary>
public class RowTrackingPartitionedIdLossTests : IDisposable
{
    private readonly string _tempDir;

    public RowTrackingPartitionedIdLossTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rt_part_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // 'part' is a partition column; 'value' is the per-row label the assertions key on. Every row lands in the
    // SAME partition, so the table has exactly one data file — the partitioning is what is under test, not the
    // file fan-out.
    private static Apache.Arrow.Schema PartitionedSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Field(new Field("part", StringType.Default, true))
        .Build();

    private static Apache.Arrow.Schema UnpartitionedSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private Task<DeltaTable> CreatePartitionedAsync() =>
        DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), PartitionedSchema,
            partitionColumns: ["part"], enableRowTracking: true).AsTask();

    private Task<DeltaTable> CreateUnpartitionedAsync() =>
        DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), UnpartitionedSchema,
            enableRowTracking: true).AsTask();

    private static RecordBatch PartitionedRows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        var parts = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
            parts.Append("p1");
        }
        return new RecordBatch(PartitionedSchema, [ids.Build(), values.Build(), parts.Build()], rows.Length);
    }

    private static RecordBatch UnpartitionedRows(params (long Id, string Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }
        return new RecordBatch(UnpartitionedSchema, [ids.Build(), values.Build()], rows.Length);
    }

    // Reads the RAW data files (bypassing the row-id strip) and reconstructs each row's stable id the way a
    // conformant reader does: the materialized hidden column where the file carries one, else
    // add.baseRowId + position. Same technique as RowTrackingTests.ReadRawWithIdsAsync — it proves what is
    // actually ON DISK, independent of whatever the read path does with it.
    private async Task<Dictionary<string, long>> ReadRawIdsByValueAsync(DeltaTable table)
    {
        var config = table.CurrentSnapshot.Metadata.Configuration!;
        string idCol = config[RowTrackingConfig.MaterializedRowIdColumnNameKey];
        var fs = new LocalTableFileSystem(_tempDir);
        var result = new Dictionary<string, long>(StringComparer.Ordinal);

        foreach (var addFile in table.CurrentSnapshot.ActiveFiles.Values)
        {
            await using var file = await fs.OpenReadAsync(DeltaPath.Decode(addFile.Path));
            using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
            long pos = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                var values = (StringArray)batch.Column("value");
                var ids = ColumnOrNull(batch, idCol);
                for (int i = 0; i < batch.Length; i++)
                {
                    long id = ids is not null && !ids.IsNull(i)
                        ? ids.GetValue(i)!.Value
                        : addFile.BaseRowId!.Value + pos + i;
                    result[values.GetString(i)!] = id;
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

    // ── the defect ──

    /// <summary>
    /// FAILING TEST for the partitioned id-loss bug.
    ///
    /// <para>Two UPDATEs on a partitioned row-tracking table. The first rewrite has nothing to read back (the
    /// appended file derives its ids from baseRowId + position) so it materializes 0,1,2 correctly. The SECOND
    /// rewrite must READ those materialized ids off the first rewrite's output — and on a partitioned table it
    /// never asks the parquet reader for that column, so it re-derives ids from the rewritten file's own fresh
    /// baseRowId instead. The rows keep their data and lose their identity.</para>
    ///
    /// <para><see cref="SecondUpdate_Unpartitioned_PreservesIds"/> is the identical sequence without
    /// partitioning and passes, which is what attributes the failure to the partition branch of the read path
    /// rather than to the UPDATE logic.</para>
    /// </summary>
    [Fact]
    public async Task SecondUpdate_Partitioned_PreservesIdsMaterializedByFirstUpdate()
    {
        await using var table = await CreatePartitionedAsync();
        await table.WriteAsync([PartitionedRows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        await table.UpdateAsync(b => Eq(b, "b"), b => SetPartitionedValue(b, "B"));

        // Precondition: the FIRST rewrite got it right, so a failure below is about reading ids back, not
        // writing them.
        var afterFirst = await ReadRawIdsByValueAsync(table);
        Assert.Equal(0L, afterFirst["a"]);
        Assert.Equal(1L, afterFirst["B"]);
        Assert.Equal(2L, afterFirst["c"]);

        await table.UpdateAsync(b => Eq(b, "a"), b => SetPartitionedValue(b, "A"));

        var afterSecond = await ReadRawIdsByValueAsync(table);
        Assert.Equal(0L, afterSecond["A"]);
        Assert.Equal(1L, afterSecond["B"]);
        Assert.Equal(2L, afterSecond["c"]);
    }

    /// <summary>
    /// The read path's own account of a row's stable id, on a partitioned table whose file carries materialized
    /// ids. <c>ReadRowsAsync</c>' <c>sourceRowTrackingOut</c> is the public surface that reports the
    /// RESOLVED id (materialized value where present, else <c>baseRowId + position</c>) — the same resolution a
    /// read-side <c>_metadata.row_id</c> column would expose. It reports fresh ids rather than the row's real
    /// ones, which is the defect stated without a second rewrite in the way.
    /// </summary>
    [Fact]
    public async Task ResolvedStableIds_Partitioned_AfterRewrite_AreTheMaterializedIds()
    {
        await using var table = await CreatePartitionedAsync();
        await table.WriteAsync([PartitionedRows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        // Rewrite the file, materializing ids 0,1,2 into the declared hidden column.
        await table.UpdateAsync(b => Eq(b, "b"), b => SetPartitionedValue(b, "B"));

        // Address every row, then ask the read path for each one's stable id.
        var addresses = new List<long>();
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var rid = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
                addresses.Add(rid.GetValue(i)!.Value);
        }
        Assert.Equal(3, addresses.Count);

        var tracking = new List<(long?[] Ids, long?[] Versions)>();
        var byValue = new Dictionary<string, long?>(StringComparer.Ordinal);
        int bi = 0;
        var selection = RowSelection.FromRowAddresses(addresses, table.CurrentSnapshot);
        await foreach (var batch in table.ReadRowsAsync(selection, sourceRowTrackingOut: tracking))
        {
            var values = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                byValue[values.GetString(i)!] = tracking[bi].Ids[i];
            bi++;
        }

        Assert.Equal(3, byValue.Count);
        Assert.Equal(0L, byValue["a"]);
        Assert.Equal(1L, byValue["B"]);
        Assert.Equal(2L, byValue["c"]);
    }

    /// <summary>
    /// Reading the hidden columns is not the same as exposing them. A partitioned table's read now ASKS the
    /// parquet reader for the materialized row-tracking columns, so the strip that follows is what stands
    /// between the caller and a stray <c>_row_id_&lt;guid&gt;</c> column — on both a projected read and a full
    /// one, over a file that actually carries them (a rewrite output). Leaking one would be a worse bug than
    /// the id loss this fixes.
    /// </summary>
    [Fact]
    public async Task Read_Partitioned_AfterRewrite_DoesNotExposeHiddenColumns()
    {
        await using var table = await CreatePartitionedAsync();
        await table.WriteAsync([PartitionedRows((10, "a"), (20, "b"), (30, "c"))]);
        await table.UpdateAsync(b => Eq(b, "b"), b => SetPartitionedValue(b, "B"));

        await foreach (var batch in table.ReadAllAsync())
            Assert.Equal(["id", "value", "part"], batch.Schema.FieldsList.Select(f => f.Name).ToArray());

        await foreach (var batch in table.ReadAllAsync(["value"]))
            Assert.Equal(["value"], batch.Schema.FieldsList.Select(f => f.Name).ToArray());

        // A projection naming only the partition column takes a different path again (no data column is read
        // from the file at all), and must still come back clean.
        await foreach (var batch in table.ReadAllAsync(["part"]))
            Assert.Equal(["part"], batch.Schema.FieldsList.Select(f => f.Name).ToArray());
    }

    // ── the control ──

    /// <summary>
    /// The same two-UPDATE sequence WITHOUT partitioning: an unprojected read of an unpartitioned table passes
    /// a null column list, so the materialized column comes back and the ids survive. Duplicates
    /// <c>RowTrackingTests.SecondUpdate_PreservesIdsMaterializedByFirstUpdate</c> deliberately — sharing this
    /// file's helpers is what makes the partitioned failure attributable to partitioning alone.
    /// </summary>
    [Fact]
    public async Task SecondUpdate_Unpartitioned_PreservesIds()
    {
        await using var table = await CreateUnpartitionedAsync();
        await table.WriteAsync([UnpartitionedRows((10, "a"), (20, "b"), (30, "c"))]); // ids 0,1,2

        await table.UpdateAsync(b => Eq(b, "b"), b => SetUnpartitionedValue(b, "B"));
        await table.UpdateAsync(b => Eq(b, "a"), b => SetUnpartitionedValue(b, "A"));

        var ids = await ReadRawIdsByValueAsync(table);
        Assert.Equal(0L, ids["A"]);
        Assert.Equal(1L, ids["B"]);
        Assert.Equal(2L, ids["c"]);
    }

    private static BooleanArray Eq(RecordBatch batch, string target)
    {
        var col = (StringArray)batch.Column("value");
        var b = new BooleanArray.Builder();
        for (int i = 0; i < col.Length; i++)
            b.Append(col.GetString(i) == target);
        return b.Build();
    }

    // The read path materializes partition columns into the batch, so the updater receives — and must return —
    // the full logical schema, partition column included.
    private static RecordBatch SetPartitionedValue(RecordBatch batch, string newValue)
    {
        var values = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            values.Append(newValue);
        return new RecordBatch(PartitionedSchema,
            [batch.Column("id"), values.Build(), batch.Column("part")], batch.Length);
    }

    private static RecordBatch SetUnpartitionedValue(RecordBatch batch, string newValue)
    {
        var values = new StringArray.Builder();
        for (int i = 0; i < batch.Length; i++)
            values.Append(newValue);
        return new RecordBatch(UnpartitionedSchema, [batch.Column("id"), values.Build()], batch.Length);
    }
}
