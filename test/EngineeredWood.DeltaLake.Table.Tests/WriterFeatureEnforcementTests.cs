// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Writer-feature ENFORCEMENT (<c>HonorWriterFeatures</c>): a table-features-mode table commonly LISTS
/// legacy writer features (<c>appendOnly</c>/<c>invariants</c>/<c>checkConstraints</c>) without them
/// being ACTIVE — such tables must write normally. But when a feature IS active — an actual
/// <c>delta.appendOnly=true</c>, a declared CHECK constraint, a column invariant or generation
/// expression — this writer cannot evaluate the expressions, so the write is REJECTED with a clear
/// error instead of silently committing possibly-violating data (Delta constraints are write-time-only;
/// a violating commit poisons the table for every reader). The appendOnly arm is covered in
/// <see cref="SchemaWriteModesTests"/>; these are the expression arms.
/// </summary>
public class WriterFeatureEnforcementTests : IDisposable
{
    private readonly string _tempDir;

    public WriterFeatureEnforcementTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_wfe_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateTableAsync(
        string? fieldMetadataJson = null,
        IReadOnlyDictionary<string, string>? configuration = null,
        string[]? writerFeatures = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        string meta = fieldMetadataJson ?? "{}";
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = writerFeatures ?? ["appendOnly", "invariants", "checkConstraints"],
            },
            new MetadataAction
            {
                Id = "wfe-table",
                Format = Format.Parquet,
                SchemaString = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{meta}}}]}}",
                PartitionColumns = [],
                Configuration = configuration?.ToDictionary(kv => kv.Key, kv => kv.Value),
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    /// <summary>A table whose second column is generated from its first.</summary>
    /// <remarks>
    /// The single-column fixture cannot express this: a column generated from itself is
    /// circular, and only looked harmless while every such table was refused outright.
    /// </remarks>
    private async Task<DeltaTable> CreateGeneratedColumnTableAsync(string generation = "id + 1")
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["generatedColumns"],
            },
            new MetadataAction
            {
                Id = "wfe-generated",
                Format = Format.Parquet,
                SchemaString =
                    @"{""type"":""struct"",""fields"":[" +
                    @"{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{}}," +
                    @"{""name"":""derived"",""type"":""long"",""nullable"":true,""metadata"":" +
                    $@"{{""delta.generationExpression"":""{generation}""}}}}]}}",
                PartitionColumns = [],
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    private static RecordBatch IdAndDerived(long id, long derived)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("derived", Int64Type.Default, true))
            .Build();
        return new RecordBatch(
            schema,
            [
                new Int64Array.Builder().Append(id).Build(),
                new Int64Array.Builder().Append(derived).Build(),
            ],
            1);
    }

    /// <summary>Reads the table back, flattened to (id, derived) pairs.</summary>
    private static async Task<List<(long Id, long? Derived)>> ReadAllAsync(DeltaTable table)
    {
        var rows = new List<(long, long?)>();

        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var derived = (Int64Array)batch.Column("derived");

            for (var i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, derived.GetValue(i)));
        }

        return rows;
    }

    private static RecordBatch Batch(long id)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        return new RecordBatch(schema, [new Int64Array.Builder().Append(id).Build()], 1);
    }

    [Fact]
    public async Task ListedButInactiveFeatures_WriteNormally()
    {
        // the common v7-upgrade shape: appendOnly/invariants/checkConstraints ENUMERATED but not active
        await using var table = await CreateTableAsync();
        long v = await table.WriteAsync([Batch(1)]);
        Assert.Equal(1, v);
    }

    [Fact]
    public async Task ActiveColumnInvariant_AdmitsASatisfyingRow()
    {
        // The invariant is now evaluated rather than refused. Its SQL arrives JSON-wrapped, which
        // is how the legacy writer-v2 feature stores it — unlike a CHECK constraint, whose value
        // is the SQL itself.
        await using var table = await CreateTableAsync(
            fieldMetadataJson: @"{""delta.invariants"":""{\""expression\"":{\""expression\"":\""id > 0\""}}""}");

        Assert.Equal(1, await table.WriteAsync([Batch(1)]));
    }

    [Fact]
    public async Task ActiveColumnInvariant_RefusesAViolatingRow()
    {
        await using var table = await CreateTableAsync(
            fieldMetadataJson: @"{""delta.invariants"":""{\""expression\"":{\""expression\"":\""id > 0\""}}""}");

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(-1)]));

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
        Assert.Contains("invariant", ex.Message);
    }

    [Fact]
    public async Task ActiveCheckConstraint_AdmitsASatisfyingRow()
    {
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });

        Assert.Equal(1, await table.WriteAsync([Batch(1)]));
    }

    [Fact]
    public async Task ActiveCheckConstraint_RefusesAViolatingRow()
    {
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(-1)]));

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
        Assert.Contains("delta.constraints.positive_id", ex.Message);
    }

    [Fact]
    public async Task TheTransactionalAppendEnforcesTheSameConstraint()
    {
        // The gate and the enforcer have to agree per path. This one routes through
        // ComputeWriteActionsAsync and therefore validates, so refusing it up front would have
        // left the transactional surface unable to write a constrained table at all — which is
        // exactly what happened before this test existed.
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });

        await using (var ok = table.StartTransaction())
        {
            await ok.WriteAsync([Batch(1)]);
            await ok.CommitAsync();
        }

        await using var bad = table.StartTransaction();
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await bad.WriteAsync([Batch(-1)]));

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
    }

    [Fact]
    public async Task APathThatCannotSeeTheRowsStillRefuses()
    {
        // StageDataFiles takes finished Parquet files, so there is nothing to check them against.
        // Refusing is the only honest answer, and it must survive constraints becoming
        // enforceable elsewhere.
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });

        await using var tx = table.StartTransaction();
        var ex = Assert.Throws<DeltaFormatException>(() => tx.StageDataFiles([]));

        Assert.Equal(DeltaTableErrorCodes.UnevaluableTableExpression, ex.ErrorCode);
    }

    [Fact]
    public async Task AConstraintThisWriterCannotParseStillRefusesTheWrite()
    {
        // The fail-closed guarantee survives evaluation: a constraint outside the parser's grammar
        // refuses exactly as every constraint did before, rather than being skipped as
        // unenforceable. INTERVAL has no representation in the expression tree.
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string>
            {
                ["delta.constraints.exotic"] = "id > INTERVAL 1 DAY",
            });

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(1)]));

        Assert.Equal(DeltaTableErrorCodes.UnevaluableTableExpression, ex.ErrorCode);
    }

    [Fact]
    public async Task AnOmittedGeneratedColumnIsComputed()
    {
        await using var table = await CreateGeneratedColumnTableAsync();

        Assert.Equal(1, await table.WriteAsync([Batch(41)]));

        var rows = await ReadAllAsync(table);
        Assert.Equal(42L, rows.Single().Derived);
    }

    [Fact]
    public async Task ASuppliedGeneratedValueThatAgreesIsAccepted()
    {
        await using var table = await CreateGeneratedColumnTableAsync();

        Assert.Equal(1, await table.WriteAsync([IdAndDerived(41, 42)]));
    }

    [Fact]
    public async Task ASuppliedGeneratedValueThatDisagreesIsRefused()
    {
        await using var table = await CreateGeneratedColumnTableAsync();

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([IdAndDerived(41, 99)]));

        Assert.Equal(DeltaTableErrorCodes.GeneratedColumnMismatch, ex.ErrorCode);
    }

    [Fact]
    public async Task AGenerationExpressionThisWriterCannotParseStillRefusesTheWrite()
    {
        await using var table = await CreateGeneratedColumnTableAsync("id + INTERVAL 1 DAY");

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(1)]));

        Assert.Equal(DeltaTableErrorCodes.UnevaluableTableExpression, ex.ErrorCode);
    }

    [Fact]
    public async Task TheTransactionalAppendComputesGeneratedColumnsToo()
    {
        // The gate asks one question for constraints and generated columns alike, so this path
        // has to be covered for both — it was gated shut for constraints once already.
        await using var table = await CreateGeneratedColumnTableAsync();

        await using (var tx = table.StartTransaction())
        {
            await tx.WriteAsync([Batch(41)]);
            await tx.CommitAsync();
        }

        var rows = await ReadAllAsync(table);
        Assert.Equal(42L, rows.Single().Derived);
    }

    [Fact]
    public async Task ActiveAppendOnly_AllowsAppend_RejectsOverwrite()
    {
        // delta.appendOnly=true: appends are fine, but overwrite/delete/update are rejected.
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.appendOnly"] = "true" });
        long v = await table.WriteAsync([Batch(1)]); // append allowed
        Assert.Equal(1, v);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([Batch(2)], DeltaWriteMode.Overwrite));
        Assert.Contains("append-only", ex.Message);
    }

    /// <summary>A table whose generated column is DECLARED narrower than its expression produces.</summary>
    private async Task<DeltaTable> CreateRescalingTableAsync()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction
            {
                MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["generatedColumns"],
            },
            new MetadataAction
            {
                Id = "wfe-rescale",
                Format = Format.Parquet,
                SchemaString =
                    @"{""type"":""struct"",""fields"":[" +
                    @"{""name"":""amount"",""type"":""decimal(10,4)"",""nullable"":false,""metadata"":{}}," +
                    @"{""name"":""rounded"",""type"":""decimal(10,2)"",""nullable"":true,""metadata"":" +
                    @"{""delta.generationExpression"":""amount""}}]}",
                PartitionColumns = [],
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    private static IArrowArray Decimals(int precision, int scale, decimal value)
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
        b.Append(value);
        return b.Build();
    }

    [Fact]
    public async Task ASuppliedValueIsCheckedAgainstWhatWouldActuallyBeStored()
    {
        // The generated column is declared decimal(10,2) while its expression yields
        // decimal(10,4), so what gets WRITTEN is the rescaled 1.23. A caller supplying exactly
        // that must be accepted: comparing against the raw 1.2345 instead would make the stored
        // value unsuppliable, and the column impossible to write explicitly at all.
        await using var table = await CreateRescalingTableAsync();

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", new Decimal128Type(10, 4), false))
            .Field(new Field("rounded", new Decimal128Type(10, 2), true))
            .Build();
        var batch = new RecordBatch(
            schema, [Decimals(10, 4, 1.2345m), Decimals(10, 2, 1.23m)], 1);

        Assert.Equal(1, await table.WriteAsync([batch]));
    }

    // ── UPDATE: the gate, the validator, the generator and the rewrite must agree ──────────

    /// <summary>Rewrites every row's <c>id</c> to <paramref name="to"/>.</summary>
    private static Func<RecordBatch, RecordBatch> SetId(long to) => batch =>
    {
        var ids = new Int64Array.Builder();
        for (var i = 0; i < batch.Length; i++)
            ids.Append(to);

        var columns = new List<IArrowArray>();
        for (var i = 0; i < batch.ColumnCount; i++)
        {
            columns.Add(string.Equals(batch.Schema.FieldsList[i].Name, "id", StringComparison.Ordinal)
                ? ids.Build()
                : batch.Column(i));
        }

        return new RecordBatch(batch.Schema, columns, batch.Length);
    };

    [Fact]
    public async Task UpdateRefusesAPostImageThatViolatesAConstraint()
    {
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });
        await table.WriteAsync([Batch(5)]);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await table.UpdateAsync(Expressions.Expressions.GreaterThan("id", 0L), SetId(-1)));

        Assert.Equal(DeltaTableErrorCodes.ConstraintViolated, ex.ErrorCode);
    }

    [Fact]
    public async Task UpdateAdmitsAPostImageThatSatisfiesTheConstraint()
    {
        await using var table = await CreateTableAsync(
            configuration: new Dictionary<string, string> { ["delta.constraints.positive_id"] = "id > 0" });
        await table.WriteAsync([Batch(5)]);

        var (rows, _) = await table.UpdateAsync(
            Expressions.Expressions.GreaterThan("id", 0L), SetId(7));

        Assert.Equal(1, rows);
    }

    [Fact]
    public async Task UpdateRecomputesAGeneratedColumnRatherThanCarryingTheStaleValue()
    {
        // The post-image comes out of the data file, so it still holds the OLD derived value.
        // Leaving it there would persist a generated column that disagrees with its own
        // expression — the exact state the feature exists to prevent.
        await using var table = await CreateGeneratedColumnTableAsync();
        await table.WriteAsync([Batch(1)]);

        await table.UpdateAsync(Expressions.Expressions.GreaterThan("id", 0L), SetId(41));

        var rows = await ReadAllAsync(table);
        Assert.Equal(41L, rows.Single().Id);
        Assert.Equal(42L, rows.Single().Derived);
    }

    [Fact]
    public async Task TheTransactionalUpdateAgreesWithTheSingleShotOne()
    {
        // Same table, same edit, different surface. Every defect found in this area so far has
        // been two paths disagreeing about one table rather than either being wrong alone.
        await using var table = await CreateGeneratedColumnTableAsync();
        await table.WriteAsync([Batch(1)]);

        await using (var tx = table.StartTransaction())
        {
            await tx.UpdateAsync(Expressions.Expressions.GreaterThan("id", 0L), SetId(41));
            await tx.CommitAsync();
        }

        var rows = await ReadAllAsync(table);
        Assert.Equal(42L, rows.Single().Derived);
    }

    private static RecordBatch Rows(params long[] ids)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        var builder = new Int64Array.Builder();
        foreach (var id in ids)
            builder.Append(id);

        return new RecordBatch(schema, [builder.Build()], ids.Length);
    }

    /// <summary>Commits a metadata action adding a CHECK constraint to an existing table.</summary>
    private async Task AddConstraintAsync(long version, string name, string sql)
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        await log.WriteCommitAsync(version, new List<DeltaAction>
        {
            new MetadataAction
            {
                Id = "wfe-table",
                Format = Format.Parquet,
                SchemaString =
                    @"{""type"":""struct"",""fields"":[{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{}}]}",
                PartitionColumns = [],
                Configuration = new Dictionary<string, string> { [$"delta.constraints.{name}"] = sql },
            },
        });
    }

    [Fact]
    public async Task UpdateLeavesUntouchedRowsUnvalidated()
    {
        // The scenario has to be BUILT, not assumed: a violating row can only exist in a
        // constrained table if it was written before the constraint was. Writing both rows in one
        // batch puts them in one file, so the UPDATE rewrites that file and carries the violating
        // row through its keep path — which is the path under test. An earlier version of this
        // test created the table already constrained and wrote a satisfying row, so it passed
        // whether or not untouched rows were re-validated.
        await using (var unconstrained = await CreateTableAsync())
        {
            await unconstrained.WriteAsync([Rows(-5, 10)]);
        }

        await AddConstraintAsync(version: 2, "positive_id", "id > 0");

        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var (rows, _) = await table.UpdateAsync(
            Expressions.Expressions.GreaterThan("id", 4L), SetId(11));

        Assert.Equal(1, rows);

        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var column = (Int64Array)batch.Column("id");
            for (var i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }

        // The violating row survived untouched; the matched row was updated and validated.
        Assert.Equal([-5L, 11L], ids.OrderBy(x => x).ToArray());
    }
}
