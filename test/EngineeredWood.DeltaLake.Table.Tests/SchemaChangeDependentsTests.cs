// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// RENAME / DROP of a column that something else depends on, and <see cref="DeltaTable.SetSchemaAsync"/>
/// dropping one (#454). Spark refuses each of these; before the fix EW committed them, which either bricked
/// the table for writes or silently moved a CHECK constraint / generation expression onto another column.
/// </summary>
public class SchemaChangeDependentsTests : IDisposable
{
    private readonly string _tempDir;

    public SchemaChangeDependentsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dependents_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema LongSchema(params string[] names)
    {
        var b = new Apache.Arrow.Schema.Builder();
        foreach (var n in names)
            b.Field(new Field(n, Int64Type.Default, true));
        return b.Build();
    }

    private static RecordBatch LongRow(Apache.Arrow.Schema schema, params long[] values)
    {
        var arrays = new List<IArrowArray>();
        foreach (var v in values)
            arrays.Add(new Int64Array.Builder().Append(v).Build());
        return new RecordBatch(schema, arrays, 1);
    }

    private Task<DeltaTable> CreateAsync(
        Apache.Arrow.Schema schema, string? constraint = null, IReadOnlyList<string>? clusteringColumns = null,
        IReadOnlyList<string>? partitionColumns = null,
        ColumnMappingMode mode = ColumnMappingMode.Name) =>
        DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, partitionColumns: partitionColumns,
            columnMappingMode: mode, clusteringColumns: clusteringColumns,
            configuration: constraint is null
                ? null
                : new Dictionary<string, string> { ["delta.constraints.c"] = constraint }).AsTask();

    private static async Task<DeltaFormatException> RefusedAsync(string errorCode, Func<Task> change)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(change);
        Assert.Equal(errorCode, ex.ErrorCode);
        return ex;
    }

    // ── CHECK constraints ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DropColumn_ReferencedByCheckConstraint_IsRefused_AndTheTableStaysWritable()
    {
        var schema = LongSchema("id", "b");
        await using var table = await CreateAsync(schema, "id > 0");
        long before = table.CurrentSnapshot.Version;

        var ex = await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.DropColumnAsync("id"));
        Assert.Contains("delta.constraints.c", ex.Message);

        Assert.Equal(before, table.CurrentSnapshot.Version);
        await table.WriteAsync([LongRow(schema, 1, 2)]);
    }

    [Fact]
    public async Task RenameColumn_ReferencedByCheckConstraint_IsRefused()
    {
        // The bypass this prevents: rename score -> old_score, add a new `score`, and `score > 0` then guards
        // the NEW column, so old_score = -5 commits.
        await using var table = await CreateAsync(LongSchema("id", "score"), "score > 0");

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.RenameColumnAsync("score", "old_score"));
    }

    [Fact]
    public async Task RenameColumn_ReferencedInAnotherCase_IsRefused()
    {
        // Delta column names are case-insensitive, so `SCORE` is the column `score`.
        await using var table = await CreateAsync(LongSchema("id", "score"), "SCORE > 0");

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.RenameColumnAsync("score", "points"));
    }

    [Fact]
    public async Task RenameColumn_ReferencedByQuotedName_IsRefused()
    {
        await using var table = await CreateAsync(LongSchema("id", "my score"), "`my score` > 0");

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.RenameColumnAsync("my score", "points"));
    }

    [Fact]
    public async Task DropColumn_NotReferenced_IsAllowed_AndTheConstraintStillHolds()
    {
        await using var table = await CreateAsync(LongSchema("id", "b", "c"), "id > 0");

        await table.DropColumnAsync("b");
        await table.RenameColumnAsync("c", "d");

        var schema = LongSchema("id", "d");
        await table.WriteAsync([LongRow(schema, 1, 2)]);
        await RefusedAsync(DeltaTableErrorCodes.ConstraintViolated,
            async () => await table.WriteAsync([LongRow(schema, -1, 2)]));
    }

    [Fact]
    public async Task DropColumn_WhenAConstraintCannotBeParsed_IsRefused()
    {
        // Without a parse there is no telling what the constraint reads, and guessing wrong bricks the table.
        await using var table = await CreateAsync(LongSchema("id", "b"), "id >>> 0");

        await RefusedAsync(DeltaTableErrorCodes.UnevaluableTableExpression,
            async () => await table.DropColumnAsync("b"));
    }

    [Fact]
    public async Task ComputeDropAndRename_ReferencedByCheckConstraint_AreRefused()
    {
        await using var table = await CreateAsync(LongSchema("id", "b"), "id > 0");

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            () => { table.ComputeDropColumn("id"); return Task.CompletedTask; });
        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            () => { table.ComputeRenameColumn("id", "x"); return Task.CompletedTask; });
        table.ComputeDropColumn("b");
    }

    // ── Nested fields ─────────────────────────────────────────────────────────────────────────────────

    private static Apache.Arrow.Schema NestedSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("a", Int64Type.Default, true),
                new Field("b", Int64Type.Default, true),
            ]), true))
            .Build();

    [Fact]
    public async Task DropAndRenameField_ReferencedByCheckConstraint_AreRefused()
    {
        await using var table = await CreateAsync(NestedSchema(), "s.a > 0");

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.DropFieldAsync(["s", "a"]));
        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.RenameFieldAsync(["s", "a"], "z"));
        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            () => { table.ComputeDropField(["s", "a"]); return Task.CompletedTask; });
    }

    [Fact]
    public async Task DropAndRenameColumn_ParentOfAReferencedField_AreRefused()
    {
        await using var table = await CreateAsync(NestedSchema(), "s.a > 0");

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.DropColumnAsync("s"));
        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.RenameColumnAsync("s", "t"));
    }

    [Fact]
    public async Task DropField_SiblingOfAReferencedField_IsAllowed()
    {
        await using var table = await CreateAsync(NestedSchema(), "s.a > 0");

        await table.DropFieldAsync(["s", "b"]);
    }

    [Fact]
    public async Task DropField_UnderAColumnReferencedOnlyAsAWhole_IsAllowed()
    {
        // Spark's rule: refuse when the changed column IS the reference or a prefix of it. A constraint on
        // the whole struct does not name its children, and still binds after one of them is dropped.
        await using var table = await CreateAsync(NestedSchema(), "s IS NOT NULL");

        await table.DropFieldAsync(["s", "b"]);
    }

    // ── Generated columns ─────────────────────────────────────────────────────────────────────────────

    private static Apache.Arrow.Schema GeneratedSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", Int64Type.Default, true))
            .Field(new Field("b", Int64Type.Default, true))
            .Field(new Field("g", Int64Type.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = "a * 2" }))
            .Build();

    [Fact]
    public async Task RenameColumn_SourceOfAGeneratedColumn_IsRefused()
    {
        // The bypass this prevents: rename a -> x and b -> a, and `g = a * 2` is computed from the old `b`.
        await using var table = await CreateAsync(GeneratedSchema());

        var ex = await RefusedAsync(DeltaTableErrorCodes.GeneratedColumnsDependentColumnChange,
            async () => await table.RenameColumnAsync("a", "x"));
        Assert.Contains("'g'", ex.Message);
    }

    [Fact]
    public async Task DropColumn_SourceOfAGeneratedColumn_IsRefused()
    {
        await using var table = await CreateAsync(GeneratedSchema());

        await RefusedAsync(DeltaTableErrorCodes.GeneratedColumnsDependentColumnChange,
            async () => await table.DropColumnAsync("a"));
    }

    [Fact]
    public async Task RenameAndDrop_TheGeneratedColumnItself_AreAllowed()
    {
        await using var table = await CreateAsync(GeneratedSchema());

        await table.RenameColumnAsync("g", "doubled");
        await table.DropColumnAsync("doubled");
        await table.DropColumnAsync("a");
    }

    // ── Clustering ────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DropColumn_ClusteringColumn_IsRefused()
    {
        await using var table = await CreateAsync(LongSchema("id", "name"), clusteringColumns: ["name"]);
        string? domain = table.GetDomainMetadata("delta.clustering");

        await RefusedAsync(DeltaTableErrorCodes.UnsupportedDropClusteringColumn,
            async () => await table.DropColumnAsync("name"));
        await RefusedAsync(DeltaTableErrorCodes.UnsupportedDropClusteringColumn,
            () => { table.ComputeDropColumn("name"); return Task.CompletedTask; });

        Assert.Equal(domain, table.GetDomainMetadata("delta.clustering"));
    }

    [Fact]
    public async Task RenameColumn_ClusteringColumn_IsAllowed_AndTheDomainKeepsItsPhysicalName()
    {
        // The domain stores PHYSICAL names, which a rename keeps.
        await using var table = await CreateAsync(LongSchema("id", "name"), clusteringColumns: ["name"]);
        string? domain = table.GetDomainMetadata("delta.clustering");

        await table.RenameColumnAsync("name", "label");

        Assert.Equal(domain, table.GetDomainMetadata("delta.clustering"));
    }

    [Fact]
    public async Task DropColumn_AfterClusteringWasRemoved_IsAllowed()
    {
        await using var table = await CreateAsync(LongSchema("id", "name"), clusteringColumns: ["name"]);
        await table.SetClusteringColumnsAsync(null);

        await table.DropColumnAsync("name");
    }

    // ── SetSchemaAsync ────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SetSchema_WithoutAPartitionColumn_IsRefused()
    {
        // metaData.partitionColumns must name columns of the schema (PROTOCOL.md).
        await using var table = await CreateAsync(
            LongSchema("id", "p"), partitionColumns: ["p"], mode: ColumnMappingMode.None);
        long before = table.CurrentSnapshot.Version;

        await RefusedAsync(DeltaTableErrorCodes.ColumnNotFound,
            async () => await table.SetSchemaAsync(LongSchema("id")));
        Assert.Equal(before, table.CurrentSnapshot.Version);

        await table.SetSchemaAsync(LongSchema("id", "p", "extra"));
    }

    [Fact]
    public async Task SetSchema_WithoutAClusteringColumn_IsRefused()
    {
        await using var table = await CreateAsync(
            LongSchema("id", "name"), clusteringColumns: ["name"], mode: ColumnMappingMode.None);

        await RefusedAsync(DeltaTableErrorCodes.ColumnNotFound,
            async () => await table.SetSchemaAsync(LongSchema("id")));
    }

    [Fact]
    public async Task SetSchema_KeepingAClusteringColumn_RekeysTheDomainToItsNewPhysicalName()
    {
        // Under column mapping SetSchema assigns FRESH physical names, so a domain left alone would name a
        // column the schema no longer has — the state Spark's ClusteringColumnInfo crashes on.
        await using var table = await CreateAsync(LongSchema("id", "name"), clusteringColumns: ["name"]);
        string oldPhysical = ColumnMapping.GetPhysicalName(
            table.CurrentSnapshot.Schema.Fields[1], ColumnMappingMode.Name);

        await table.SetSchemaAsync(LongSchema("id", "name", "extra"));

        string newPhysical = ColumnMapping.GetPhysicalName(
            table.CurrentSnapshot.Schema.Fields[1], ColumnMappingMode.Name);
        Assert.NotEqual(oldPhysical, newPhysical);
        Assert.Equal(
            $"{{\"clusteringColumns\":[[\"{newPhysical}\"]],\"domainName\":\"delta.clustering\"}}",
            table.GetDomainMetadata("delta.clustering"));
    }

    [Fact]
    public async Task SetSchema_WithoutAColumnACheckConstraintReads_IsRefused()
    {
        await using var table = await CreateAsync(
            LongSchema("id", "b"), "id > 0", mode: ColumnMappingMode.None);

        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.SetSchemaAsync(LongSchema("b")));

        await table.SetSchemaAsync(LongSchema("ID", "c"));
    }

    [Fact]
    public async Task SetSchema_KeepingANestedFieldACheckConstraintReads_IsAllowed()
    {
        await using var table = await CreateAsync(NestedSchema(), "s.a > 0", mode: ColumnMappingMode.None);

        var withoutB = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", new ArrowStructType([new Field("a", Int64Type.Default, true)]), true))
            .Build();
        await table.SetSchemaAsync(withoutB);

        var withoutA = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", new ArrowStructType([new Field("b", Int64Type.Default, true)]), true))
            .Build();
        await RefusedAsync(DeltaTableErrorCodes.ConstraintDependentColumnChange,
            async () => await table.SetSchemaAsync(withoutA));
    }
}
