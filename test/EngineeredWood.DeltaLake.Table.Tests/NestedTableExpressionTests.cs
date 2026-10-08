// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #466: a CHECK constraint or generation expression reading a struct's field (<c>s.a</c>) could not be evaluated,
/// so every write to such a table was refused. Spark accepts these, so a Spark-created table like this could not
/// be written by EW at all.
/// </summary>
public class NestedTableExpressionTests : IDisposable
{
    private readonly string _tempDir;

    public NestedTableExpressionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nestedexpr_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    private static readonly ArrowStructType SType = new(
        [new Field("a", Int64Type.Default, true), new Field("b", Int64Type.Default, true)]);

    private static Apache.Arrow.Schema Schema(params Field[] extra)
    {
        var builder = new Apache.Arrow.Schema.Builder().Field(new Field("s", SType, true));
        foreach (var field in extra)
        {
            builder.Field(field);
        }
        return builder.Build();
    }

    private static StructArray S(long a, long b) => new(SType, 1,
        [new Int64Array.Builder().Append(a).Build(), new Int64Array.Builder().Append(b).Build()],
        ArrowBuffer.Empty, nullCount: 0);

    private Task<DeltaTable> CreateAsync(Apache.Arrow.Schema schema, string? constraint = null) =>
        DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema, columnMappingMode: ColumnMappingMode.Name,
            configuration: constraint is null
                ? null
                : new Dictionary<string, string> { ["delta.constraints.c"] = constraint }).AsTask();

    // The issue's probe.
    [Theory]
    [InlineData("s.a > 0")]
    [InlineData("`s`.a > 0")]
    public async Task ACheckConstraintOnAStructField_AcceptsAConformingRow(string constraint)
    {
        var schema = Schema();
        await using var table = await CreateAsync(schema, constraint);

        await table.WriteAsync([new RecordBatch(schema, [S(5, 0)], 1)]);

        long rows = 0;
        await foreach (var batch in table.ReadAllAsync())
        {
            rows += batch.Length;
        }
        Assert.Equal(1, rows);
    }

    [Fact]
    public async Task ACheckConstraintOnAStructField_RefusesAViolatingRow()
    {
        var schema = Schema();
        await using var table = await CreateAsync(schema, "s.a > 0");

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.WriteAsync([new RecordBatch(schema, [S(-1, 0)], 1)]));

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
    }

    [Fact]
    public async Task AGenerationExpressionOverAStructField_IsComputed()
    {
        var schema = Schema(new Field("g", Int64Type.Default, true,
            new Dictionary<string, string> { ["delta.generationExpression"] = "s.a * 2" }));
        await using var table = await CreateAsync(schema);
        var written = new Apache.Arrow.Schema.Builder().Field(new Field("s", SType, true)).Build();

        await table.WriteAsync([new RecordBatch(written, [S(21, 0)], 1)]);

        var rows = new List<long?>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var g = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("g"));
            rows.AddRange(Enumerable.Range(0, batch.Length).Select(i => g.GetValue(i)));
        }
        Assert.Equal([42L], rows);
    }

    // The schema-change guard (#454) reads the same parts: a constraint on s.a depends on s.a and on s, not on s.b.
    [Fact]
    public async Task SchemaChanges_SeeTheNestedReference()
    {
        await using var table = await CreateAsync(Schema(new Field("x", Int64Type.Default, true)), "s.a > 0");

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.DropFieldAsync(["s", "a"]));
        Assert.Equal(DeltaTableErrorCodes.ConstraintDependentColumnChange, ex.ErrorCode);
        ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.RenameColumnAsync("s", "r"));
        Assert.Equal(DeltaTableErrorCodes.ConstraintDependentColumnChange, ex.ErrorCode);

        await table.DropFieldAsync(["s", "b"]);
        await table.RenameColumnAsync("x", "y");
    }
}
