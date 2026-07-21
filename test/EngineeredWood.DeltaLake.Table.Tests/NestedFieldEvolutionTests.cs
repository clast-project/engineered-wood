// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// ADD / RENAME / DROP of a field INSIDE a nested struct column, as metadata-only commits — the nested analogs
/// of the top-level column ALTERs. No data file is rewritten, so files written before the change disagree with
/// the current schema and the recursive read reconcile backfills/removes the member at its nesting level. Covers
/// both the auto-commit forms (<see cref="DeltaTable.AddFieldAsync"/> et al.) and the compute-only buffered
/// halves (<see cref="DeltaTable.ComputeAddField"/> et al.) that fuse a nested ALTER into one atomic commit.
/// </summary>
public class NestedFieldEvolutionTests : IDisposable
{
    private readonly string _tempDir;

    public NestedFieldEvolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nfield_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    // id: Int64 (non-null), s: struct { a: Int64, b: String } (nullable).
    private static Apache.Arrow.Schema NestedSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("a", Int64Type.Default, true),
                new Field("b", StringType.Default, true),
            ]), true))
            .Build();

    private static RecordBatch Batch(Apache.Arrow.Schema schema, long id, long a, string b)
    {
        var structType = (ArrowStructType)schema.FieldsList[1].DataType;
        var nested = new StructArray(structType, 1,
        [
            new Int64Array.Builder().Append(a).Build(),
            new StringArray.Builder().Append(b).Build(),
        ], ArrowBuffer.Empty);
        return new RecordBatch(schema,
            [new Int64Array.Builder().Append(id).Build(), nested], 1);
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var list = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            list.Add(b);
        return list;
    }

    private static ArrowStructType StructOf(DeltaTable table, string column) =>
        (ArrowStructType)table.ArrowSchema.GetFieldByName(column).DataType;

    // ── AddFieldAsync ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AddField_IsMetadataOnly_AndOldFilesBackfillNestedNull()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();

        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Batch(schema, 1, 42, "hello")]);

        long filesBefore = table.CurrentSnapshot.FileCount;
        await table.AddFieldAsync(["s"], new Field("c", DoubleType.Default, true));

        // Metadata-only — no file added/removed — and the struct now has three members.
        Assert.Equal(filesBefore, table.CurrentSnapshot.FileCount);
        var st = StructOf(table, "s");
        Assert.Equal(new[] { "a", "b", "c" }, st.Fields.Select(f => f.Name).ToArray());

        // The pre-existing file lacks s.c — it reads back as an all-NULL child.
        var read = Assert.Single(await ReadAllAsync(table));
        var sArr = Assert.IsType<StructArray>(read.Column(1));
        Assert.Equal(3, sArr.Fields.Count);
        var c = Assert.IsType<DoubleArray>(sArr.Fields[2]);
        Assert.True(c.IsNull(0));
        // Existing members are untouched.
        Assert.Equal(42L, ((Int64Array)sArr.Fields[0]).GetValue(0));
        Assert.Equal("hello", ((StringArray)sArr.Fields[1]).GetString(0));
    }

    [Fact]
    public async Task AddField_UnderColumnMapping_AssignsFreshId_AndBumpsMaxColumnId()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();

        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);

        int before = int.Parse(
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);

        await table.AddFieldAsync(["s"], new Field("c", DoubleType.Default, true));

        var sField = table.CurrentSnapshot.Schema.Fields[1];
        var added = ((EngineeredWood.DeltaLake.Schema.StructType)sField.Type).Fields[2];
        Assert.Equal("c", added.Name);
        Assert.Equal((before + 1).ToString(), added.Metadata![ColumnMapping.FieldIdKey]);
        Assert.StartsWith("col-", added.Metadata![ColumnMapping.PhysicalNameKey]);

        int after = int.Parse(
            table.CurrentSnapshot.Metadata.Configuration![ColumnMapping.MaxColumnIdKey]);
        Assert.Equal(before + 1, after);
    }

    [Fact]
    public async Task AddField_RejectsNonNullable_Duplicate_MissingColumn_AndNonStructPath()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.AddFieldAsync(["s"], new Field("c", DoubleType.Default, false)).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.AddFieldAsync(["s"], new Field("a", DoubleType.Default, true)).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.AddFieldAsync(["nope"], new Field("c", DoubleType.Default, true)).AsTask());
        // "id" is a primitive, not a struct — a path through it is invalid.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.AddFieldAsync(["id"], new Field("c", DoubleType.Default, true)).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() =>
            table.AddFieldAsync([], new Field("c", DoubleType.Default, true)).AsTask());
    }

    [Fact]
    public async Task AddField_TwoLevelsDeep()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        // id, outer: struct { inner: struct { x: Int64 } }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("outer", new ArrowStructType(
            [
                new Field("inner", new ArrowStructType(
                    [new Field("x", Int64Type.Default, true)]), true),
            ]), true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(fs, schema);

        await table.AddFieldAsync(["outer", "inner"], new Field("y", StringType.Default, true));

        var outer = StructOf(table, "outer");
        var inner = (ArrowStructType)outer.Fields[0].DataType;
        Assert.Equal(new[] { "x", "y" }, inner.Fields.Select(f => f.Name).ToArray());
    }

    // ── RenameFieldAsync ───────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RenameField_KeepsData_UnderColumnMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Batch(schema, 1, 7, "keep")]);

        await table.RenameFieldAsync(["s", "b"], "renamed");

        var st = StructOf(table, "s");
        Assert.Equal(new[] { "a", "renamed" }, st.Fields.Select(f => f.Name).ToArray());

        // The physical column is unchanged — the pre-rename file reads under the new logical name.
        var read = Assert.Single(await ReadAllAsync(table));
        var sArr = Assert.IsType<StructArray>(read.Column(1));
        Assert.Equal("renamed", ((ArrowStructType)sArr.Data.DataType).Fields[1].Name);
        Assert.Equal("keep", ((StringArray)sArr.Fields[1]).GetString(0));
    }

    [Fact]
    public async Task RenameField_RequiresColumnMapping_AndRejectsTopLevelPath()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(fs, schema); // no mapping

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.RenameFieldAsync(["s", "b"], "renamed").AsTask());
        // A one-segment path is a top-level column — must use RenameColumnAsync.
        await Assert.ThrowsAsync<ArgumentException>(() =>
            table.RenameFieldAsync(["s"], "renamed").AsTask());
    }

    // ── DropFieldAsync ─────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DropField_RemovesMember_AndReconcilesOldFilesAway()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Batch(schema, 1, 5, "gone")]);

        await table.DropFieldAsync(["s", "b"]);

        var st = StructOf(table, "s");
        Assert.Equal(new[] { "a" }, st.Fields.Select(f => f.Name).ToArray());

        var read = Assert.Single(await ReadAllAsync(table));
        var sArr = Assert.IsType<StructArray>(read.Column(1));
        Assert.Single(sArr.Fields);
        Assert.Equal(5L, ((Int64Array)sArr.Fields[0]).GetValue(0));
    }

    [Fact]
    public async Task DropField_RefusesToEmptyStruct_AndRequiresMapping()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        // s: struct { only: Int64 } — dropping its single member would empty the struct.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", new ArrowStructType(
                [new Field("only", Int64Type.Default, true)]), true))
            .Build();
        await using var mapped = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mapped.DropFieldAsync(["s", "only"]).AsTask());

        var fs2 = new LocalTableFileSystem(Path.Combine(_tempDir, "unmapped"));
        await using var unmapped = await DeltaTable.CreateAsync(fs2, NestedSchema());
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            unmapped.DropFieldAsync(["s", "b"]).AsTask());
    }

    // ── Compute* (buffered seam) ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ComputeAddField_FusesNestedAddIntoOneCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(fs, schema);
        await table.WriteAsync([Batch(schema, 1, 3, "x")]);
        var pinned = table.CurrentSnapshot;

        var change = table.ComputeAddField(["s"], new Field("c", DoubleType.Default, true));
        long committed = await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: change.Actions,
            expectedVersion: pinned.Version, operation: "ALTER TABLE");
        Assert.Equal(pinned.Version + 1, committed);

        await using var check = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var st = (ArrowStructType)check.ArrowSchema.GetFieldByName("s").DataType;
        Assert.Equal(new[] { "a", "b", "c" }, st.Fields.Select(f => f.Name).ToArray());
    }

    [Fact]
    public async Task ComputeAddField_ChainsOnPendingSchema()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Batch(schema, 1, 3, "x")]);
        var pinned = table.CurrentSnapshot;

        // Two nested adds in one transaction: the second composes on the first's PENDING metadata, so the
        // fused commit carries only the final metaData and distinct column ids.
        var c1 = table.ComputeAddField(["s"], new Field("c", DoubleType.Default, true));
        var c2 = table.ComputeAddField(
            ["s"], new Field("d", Int32Type.Default, true), c1.Metadata, c1.ProtocolUpgrade);

        long committed = await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: c2.Actions,
            expectedVersion: pinned.Version, operation: "ALTER TABLE");
        Assert.Equal(pinned.Version + 1, committed);

        await using var check = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var st = (EngineeredWood.DeltaLake.Schema.StructType)check.CurrentSnapshot.Schema.Fields[1].Type;
        Assert.Equal(new[] { "a", "b", "c", "d" }, st.Fields.Select(f => f.Name).ToArray());
        int idC = int.Parse(st.Fields[2].Metadata![ColumnMapping.FieldIdKey]);
        int idD = int.Parse(st.Fields[3].Metadata![ColumnMapping.FieldIdKey]);
        Assert.NotEqual(idC, idD); // the chain bumped maxColumnId between the two adds
    }

    [Fact]
    public async Task ComputeDropField_FusesNestedDropIntoOneCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Batch(schema, 1, 3, "x")]);
        var pinned = table.CurrentSnapshot;

        var change = table.ComputeDropField(["s", "b"]);
        await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: change.Actions,
            expectedVersion: pinned.Version, operation: "ALTER TABLE");

        await using var check = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var st = (ArrowStructType)check.ArrowSchema.GetFieldByName("s").DataType;
        Assert.Equal(new[] { "a" }, st.Fields.Select(f => f.Name).ToArray());
    }

    [Fact]
    public async Task ComputeRenameField_FusesNestedRenameIntoOneCommit()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = NestedSchema();
        await using var table = await DeltaTable.CreateAsync(
            fs, schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Batch(schema, 1, 3, "keep")]);
        var pinned = table.CurrentSnapshot;

        var change = table.ComputeRenameField(["s", "b"], "renamed");
        await table.CommitDataFilesAsync(
            [], DeltaWriteMode.Append, extraActions: change.Actions,
            expectedVersion: pinned.Version, operation: "ALTER TABLE");

        await using var check = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var st = (ArrowStructType)check.ArrowSchema.GetFieldByName("s").DataType;
        Assert.Equal(new[] { "a", "renamed" }, st.Fields.Select(f => f.Name).ToArray());
        var read = Assert.Single(await ReadAllAsync(check));
        var sArr = Assert.IsType<StructArray>(read.Column(1));
        Assert.Equal("keep", ((StringArray)sArr.Fields[1]).GetString(0));
    }
}
