// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Row-level <c>variant_get</c> in CHECK constraints and DML predicates (#681), as Spark 4.1 / delta-spark 4.3.1
/// answer them: a constraint is enforced, a NULL answer (a missing path, a NULL variant) is a violation, and a
/// value that does not convert fails the write under <c>variant_get</c> and is a violation under
/// <c>try_variant_get</c>; a DML predicate matches no row that lacks the path.
/// </summary>
public class VariantRowEvaluationTests : IDisposable
{
    private readonly string _tempDir;

    public VariantRowEvaluationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_rows_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AVariantGetConstraintIsEnforced(bool shredded)
    {
        await using var table = await CreateAsync(shredded);
        await table.WriteAsync([Batch(0, A(VariantValue.FromInt64(3)), A(VariantValue.FromString("8")))]);

        await table.AddConstraintAsync("pos", "variant_get(v, '$.a', 'int') > 0");

        await table.WriteAsync([Batch(10, A(VariantValue.FromInt64(1)))]);

        // NULL is a violation: a missing path, a variant null, a SQL NULL variant.
        await AssertViolatedAsync(table, Batch(20, Obj(("b", VariantValue.FromInt64(1)))));
        await AssertViolatedAsync(table, Batch(20, A(VariantValue.Null)));
        await AssertViolatedAsync(table, Batch(20, [null]));
        await AssertViolatedAsync(table, Batch(20, A(VariantValue.FromInt64(-4))));

        // A value that does not convert fails the cast itself, as Spark's INVALID_VARIANT_CAST does.
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(20, A(VariantValue.FromString("x")))]));
        Assert.Contains("INVALID_VARIANT_CAST", ex.Message);

        Assert.Equal([0, 1, 10], await IdsAsync(table));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ATryVariantGetConstraintCountsAnUnconvertibleValueAsAViolation(bool shredded)
    {
        await using var table = await CreateAsync(shredded);
        await table.AddConstraintAsync("pos", "try_variant_get(v, '$.a', 'int') > 0");

        await AssertViolatedAsync(table, Batch(0, A(VariantValue.FromString("x"))));
        await table.WriteAsync([Batch(0, A(VariantValue.FromString("5")))]);
        Assert.Equal([0], await IdsAsync(table));
    }

    [Fact]
    public async Task AddingAConstraintTheRowsViolateIsRefused()
    {
        await using var table = await CreateAsync(shredded: false);
        await table.WriteAsync([Batch(0, A(VariantValue.FromInt64(3)), Obj(("b", VariantValue.FromInt64(1))))]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.AddConstraintAsync("pos", "cast(v:a AS INT) > 0"));
        Assert.NotEqual(DeltaTableErrorCodes.UnevaluableTableExpression, ex.ErrorCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADeleteOnAVariantPathMatchesOnlyTheRowsWithThePath(bool shredded)
    {
        await using var table = await CreateAsync(shredded);
        await table.WriteAsync([Batch(0,
            A(VariantValue.FromInt64(3)), A(VariantValue.FromInt64(9)), Obj(("b", VariantValue.FromInt64(9))),
            VariantValue.FromString("not an object"), null, A(VariantValue.FromString("12")))]);

        var (deleted, _) = await table.DeleteAsync(SparkSqlParser.ParsePredicate("v:a::int > 5"));

        Assert.Equal(2, deleted);
        Assert.Equal([0, 2, 3, 4], await IdsAsync(table));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUpdateOnAVariantPathMatchesOnlyTheRowsWithThePath(bool shredded)
    {
        await using var table = await CreateAsync(shredded);
        await table.WriteAsync([Batch(0,
            A(VariantValue.FromInt64(3)), A(VariantValue.FromInt64(9)), A(VariantValue.FromString("x")),
            Obj(("b", VariantValue.FromInt64(1))))]);

        var (updated, _) = await table.UpdateAsync(
            SparkSqlParser.ParsePredicate("try_variant_get(v, '$.a', 'int') < 5"),
            batch => batch);

        Assert.Equal(1, updated);
    }

    [Theory]
    [InlineData("variant_get(v, '$.a', 'int')")]
    [InlineData("try_variant_get(v, '$.a', 'int')")]
    [InlineData("CAST(v:a AS INT)")]
    [InlineData("coalesce(v:a::int, 0) + 1")]
    public async Task AGeneratedColumnThatExtractsFromAVariantIsRefusedAsDeltaRefusesIt(string expression)
    {
        // Delta refuses to declare one (DELTA_UNSUPPORTED_EXPRESSION_GENERATED_COLUMN); a table that does anyway is
        // refused at the write rather than computed.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Field(new Field("g", Int32Type.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = expression }))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(0, A(VariantValue.FromInt64(1)))]));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedExpressionGeneratedColumn, ex.ErrorCode);
    }

    [Fact]
    public async Task AVariantGeneratedColumnIsRefusedAsDeltaRefusesIt()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Field(new Field("g", VariantType.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = "v" }))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(0, A(VariantValue.FromInt64(1)))]));
        Assert.Equal(DeltaTableErrorCodes.UnsupportedDataTypeInGeneratedColumn, ex.ErrorCode);
    }

    // ── helpers ──

    private static async Task AssertViolatedAsync(DeltaTable table, RecordBatch batch)
    {
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([batch]));
        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
    }

    private async Task<DeltaTable> CreateAsync(bool shredded)
    {
        var options = shredded
            ? DeltaTableOptions.Default with
            {
                ParquetWriteOptions = ParquetWriteOptions.Default with
                {
                    VariantShredSchemas = new Dictionary<string, ShredSchema>
                    {
                        ["v"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                        {
                            ["a"] = ShredSchema.Primitive(ShredType.Int64),
                        }),
                    },
                },
            }
            : DeltaTableOptions.Default;
        var configuration = shredded
            ? new Dictionary<string, string> { ["delta.enableVariantShredding"] = "true" }
            : null;
        return await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), ArrowSchema(), configuration: configuration, options: options);
    }

    private static Apache.Arrow.Schema ArrowSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    private static RecordBatch Batch(long firstId, params VariantValue?[] values)
    {
        var ids = new Int64Array.Builder();
        var vb = new VariantArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            ids.Append(firstId + i);
            if (values[i] is { } v)
                vb.Append(v);
            else
                vb.AppendNull();
        }
        return new RecordBatch(ArrowSchema(), [ids.Build(), vb.Build(allocator: null)], values.Length);
    }

    private static VariantValue A(VariantValue value) => Obj(("a", value));

    private static VariantValue Obj(params (string Key, VariantValue Value)[] fields) =>
        VariantValue.FromObject(fields.ToDictionary(f => f.Key, f => f.Value));

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync())
        {
            var column = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }
}
