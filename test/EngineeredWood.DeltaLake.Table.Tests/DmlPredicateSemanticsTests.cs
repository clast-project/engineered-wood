// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO.Local;
using X = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// DML predicates follow the rules the constraint enforcer does: Spark's (#589). A comparison across
/// types casts where Spark casts and refuses where Spark refuses. The registry-free evaluator DML used
/// to have answered NULL for every such pair instead, so a DELETE whose literal was the wrong type
/// committed and deleted nothing.
/// </summary>
public class DmlPredicateSemanticsTests : IDisposable
{
    private readonly string _tempDir;

    private static readonly DateTimeOffset Day = new(2026, 10, 3, 0, 0, 0, TimeSpan.Zero);

    public DmlPredicateSemanticsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dmlsem_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> TableAsync()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("d", Date32Type.Default, true))
            .Build();
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        await table.WriteAsync([new RecordBatch(schema,
        [
            new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build(),
            new Date32Array.Builder().Append(Day.AddDays(-1)).Append(Day).Append(Day.AddDays(1)).Build(),
        ], 3)]);
        return table;
    }

    [Fact]
    public async Task Delete_StringLiteralAgainstADate_IsCastAsSparkCastsIt()
    {
        await using var table = await TableAsync();

        var (deleted, _) = await table.DeleteAsync(X.Equal("d", LiteralValue.Of("2026-10-03")));

        Assert.Equal(1, deleted);
    }

    [Fact]
    public async Task Delete_StringLiteralAgainstADate_IsNotPrunedAwayByStatistics()
    {
        // Two files: the first's date range excludes the literal and the second's holds it. The statistics
        // compare a string against dates, which they cannot order, so the second must not be skipped.
        await using var table = await TableAsync();
        var schema = table.ArrowSchema;
        await table.WriteAsync([new RecordBatch(schema,
        [
            new Int64Array.Builder().AppendRange([4L, 5L]).Build(),
            new Date32Array.Builder().Append(Day.AddDays(10)).Append(Day.AddDays(11)).Build(),
        ], 2)]);

        var (deleted, _) = await table.DeleteAsync(X.Equal("d", LiteralValue.Of("2026-10-13")));

        Assert.Equal(1, deleted);
    }

    [Fact]
    public async Task Delete_StringLiteralAgainstALong_IsCastAsSparkCastsIt()
    {
        await using var table = await TableAsync();

        var (deleted, _) = await table.DeleteAsync(X.GreaterThanOrEqual("id", LiteralValue.Of("2")));

        Assert.Equal(2, deleted);
    }

    [Fact]
    public async Task Update_StringLiteralAgainstADate_IsCastAsSparkCastsIt()
    {
        await using var table = await TableAsync();

        var (updated, _) = await table.UpdateAsync(
            X.LessThan("d", LiteralValue.Of("2026-10-03")), batch => batch);

        Assert.Equal(1, updated);
    }

    [Fact]
    public async Task Delete_IntegerLiteralAgainstADate_IsRefusedAndCommitsNothing()
    {
        await using var table = await TableAsync();
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<ExpressionAnalysisException>(
            async () => await table.DeleteAsync(X.Equal("d", LiteralValue.Of(5))));

        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task Delete_IntegerLiteralAgainstADate_IsRefusedWhenEveryFileIsPruned()
    {
        // The statistics rule every file out before a row is read, so the mask never runs; the refusal
        // must not depend on that.
        await using var table = await TableAsync();
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<ExpressionAnalysisException>(async () => await table.DeleteAsync(X.And(
            X.Equal("id", LiteralValue.Of(100L)), X.Equal("d", LiteralValue.Of(5)))));

        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task Update_IntegerLiteralAgainstADate_IsRefusedOnAnEmptyTable()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("d", Date32Type.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);

        await Assert.ThrowsAsync<ExpressionAnalysisException>(async () => await table.UpdateAsync(
            X.Equal("d", LiteralValue.Of(5)), batch => batch));
    }

#if NET6_0_OR_GREATER
    [Fact]
    public async Task Delete_TypedDateOnlyLiteral_StillMatches()
    {
        await using var table = await TableAsync();

        var (deleted, _) = await table.DeleteAsync(X.And(
            X.GreaterThanOrEqual("d", LiteralValue.Of(new DateOnly(2026, 10, 3))),
            X.LessThan("d", LiteralValue.Of(new DateOnly(2026, 10, 4)))));

        Assert.Equal(1, deleted);
    }
#endif

    [Fact]
    public async Task Delete_UnsignedLiteralAgainstALong_Matches()
    {
        await using var table = await TableAsync();

        var (deleted, _) = await table.DeleteAsync(X.Equal("id", LiteralValue.Of(2u)));

        Assert.Equal(1, deleted);
    }
}
