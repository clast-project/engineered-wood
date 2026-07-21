// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The buffered-transaction SCHEMA seam: the compute-only ALTER halves (<see cref="DeltaTable.ComputeAddColumn"/>
/// and the Compute* family) build metaData actions WITHOUT committing, so a schema change fuses into one atomic
/// commit with data changes (chained ALTERs compose on the previous one's PENDING schema); <see
/// cref="DeltaTable.ReconcileBatchToFields"/> overlays a pending schema onto committed reads ("read your own
/// schema"); and <see cref="DeltaTable.SetSchemaAsync"/> adopts an incoming schema wholesale as a metadata-only
/// commit (the CREATE-OR-REPLACE building block), with a logical no-op compare.
/// </summary>
public class BufferedSchemaSeamTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedSchemaSeamTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_bufschema_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdValueSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(IdValueSchema, [ids.Build(), values.Build()], count);
    }

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    private async Task<DeltaTable> CreateWithRowsAsync()
    {
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdValueSchema);
        await table.WriteAsync([Batch(1, 5)]);
        return table;
    }

    [Fact]
    public async Task ChainedComputes_SecondAddComposesOnPendingSchema()
    {
        await using var table = await CreateWithRowsAsync();
        var pinned = table.CurrentSnapshot;

        // two ALTERs in one transaction: the second composes on the FIRST's pending metadata — the fused
        // commit carries only the FINAL metaData action (a commit must not carry two).
        var c1 = table.ComputeAddColumn(new Field("e1", Int32Type.Default, true));
        var c2 = table.ComputeAddColumn(new Field("e2", StringType.Default, true), c1.Metadata, c1.ProtocolUpgrade);

        long committed = await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: c2.Actions,
            expectedVersion: pinned.Version, operation: "ALTER TABLE");
        Assert.Equal(pinned.Version + 1, committed);

        await using var check = await OpenAsync();
        var names = check.ArrowSchema.FieldsList.Select(f => f.Name).ToArray();
        Assert.Contains("e1", names);
        Assert.Contains("e2", names);
    }

    [Fact]
    public async Task ReconcileBatchToFields_BackfillsPendingColumn()
    {
        // "read your own (pending) schema": committed 2-column batches served under a pending 3-column
        // schema — the added column appears as a typed all-NULL array.
        await using var table = await CreateWithRowsAsync();
        var pendingFields = new List<Field>
        {
            new("id", Int64Type.Default, false),
            new("value", StringType.Default, true),
            new("extra", Int32Type.Default, true),
        };

        int seen = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            var reconciled = DeltaTable.ReconcileBatchToFields(batch, pendingFields);
            Assert.Equal(3, reconciled.ColumnCount);
            Assert.Equal(batch.Length, reconciled.Length);
            var extra = Assert.IsType<Int32Array>(reconciled.Column(2));
            for (int i = 0; i < extra.Length; i++)
                Assert.True(extra.IsNull(i));
            seen += reconciled.Length;
        }
        Assert.Equal(5, seen);
    }

    [Fact]
    public async Task SetSchema_AdoptsIncomingSchema_DropAndAdd()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("old", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([new RecordBatch(schema,
        [
            new Int64Array.Builder().Append(1).Build(),
            new StringArray.Builder().Append("x").Build(),
        ], 1)]);

        // adopt EXACTLY the new shape: `old` dropped, `fresh` added — one metadata-only commit
        var newSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("fresh", Int32Type.Default, true))
            .Build();
        long v = await table.SetSchemaAsync(newSchema);
        Assert.Equal(2, v);

        // the CREATE-OR-REPLACE shape: the schema swap pairs with an Overwrite of the data
        await table.WriteAsync([new RecordBatch(newSchema,
        [
            new Int64Array.Builder().Append(10).Build(),
            new Int32Array.Builder().Append(99).Build(),
        ], 1)], DeltaWriteMode.Overwrite);

        var names = table.ArrowSchema.FieldsList.Select(f => f.Name).ToArray();
        Assert.Equal(new[] { "id", "fresh" }, names);
        int rows = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
            Assert.Equal(99, ((Int32Array)batch.Column(1)).GetValue(0));
        }
        Assert.Equal(1, rows);
    }

    [Fact]
    public async Task SetSchema_LogicallyIdentical_IsNoOp()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        long before = table.CurrentSnapshot.Version;

        await table.SetSchemaAsync(schema);
        Assert.Equal(before, table.CurrentSnapshot.Version); // no metadata commit for a no-op
    }
}
