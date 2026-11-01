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

    [Fact]
    public async Task Delete_TwoLiteralsOfDifferentTypes_AreNotFoldedByTheStatistics()
    {
        // `'5' = 5` is TRUE to Spark (the string is cast). The statistics cannot order a string against a
        // number, and must not read that as "no row matches" and skip every file.
        await using var table = await TableAsync();

        var (deleted, _) = await table.DeleteAsync(
            X.Equal(X.Lit(LiteralValue.Of("5")), X.Lit(LiteralValue.Of(5))));

        Assert.Equal(3, deleted);
    }

    /// <summary>
    /// Two timestamp literals a tick apart are folded by the statistics, and the mask agrees: neither path
    /// materializes them at microseconds to compare (the row evaluator does so only to type a literal), so
    /// both compare the ticks.
    /// </summary>
    [Theory]
    [InlineData(ComparisonOperator.Equal, 0)]
    [InlineData(ComparisonOperator.LessThan, 3)]
    public async Task Delete_TwoTimestampLiteralsATickApart_FoldAsTheMaskCompares(ComparisonOperator op, long expected)
    {
        await using var table = await TableAsync();
        var t = new DateTimeOffset(2026, 10, 3, 0, 0, 0, TimeSpan.Zero);

        var (deleted, _) = await table.DeleteAsync(new ComparisonPredicate(
            X.Lit(LiteralValue.Of(t)), op, X.Lit(LiteralValue.Of(t.AddTicks(1)))));

        Assert.Equal(expected, deleted);
    }

    [Fact]
    public async Task Delete_ACastThatFailsOnARow_IsNotProvenAwayByStatistics()
    {
        // Under ANSI `s = 5` raises CAST_INVALID_INPUT on 'abc', and Spark's OR evaluates it first. The
        // statistics prove `id >= 0` of every row, but trusting that proof would delete the file unread and
        // never raise.
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, new DeltaTableOptions { TrustStatisticsProofs = true });
        await table.WriteAsync([new RecordBatch(schema,
            [new StringArray.Builder().Append("abc").Build(), new Int64Array.Builder().Append(1).Build()], 1)]);
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<EngineeredWood.Expressions.Arrow.Spark.SparkEvaluationException>(async () => await table.DeleteAsync(X.Or(
            X.Equal("s", LiteralValue.Of(5)), X.GreaterThanOrEqual("id", LiteralValue.Of(0L)))));

        Assert.Equal(before, table.CurrentSnapshot.Version);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
    }

    [Fact]
    public async Task Delete_UnsignedInListAgainstAWideDecimal_RoundsAsTheMaskDoes()
    {
        // A uint is typed as a bigint, so `IN (0u)` against decimal(38,20) compares through decimal(38,18) and
        // 4E-19 rounds to 0: a match. The statistics must round through the same type, not skip the file.
        var type = new Decimal128Type(38, 20);
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("p", type, true)).Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        await table.WriteAsync([new RecordBatch(schema,
            [new Decimal128Array.Builder(type).Append(0.0000000000000000004m).Build()], 1)]);

        var (deleted, _) = await table.DeleteAsync(X.In("p", LiteralValue.Of(0u)));

        Assert.Equal(1, deleted);
    }

    private RecordBatch SourceRow(long id, DateTimeOffset d) => new(
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("d", Date32Type.Default, true))
            .Build(),
        [new Int64Array.Builder().Append(id).Build(), new Date32Array.Builder().Append(d).Build()], 1);

    private static Expressions.Expression Side(string side, string column) =>
        UnboundReference.FromParts([side, column]);

    [Fact]
    public async Task Merge_MatchedConditionSparkRefuses_IsRefusedWhenNoKeyMatches()
    {
        // No target row matches id 99, so the matched condition never sees a row; it must still be refused.
        await using var table = await TableAsync();
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<ExpressionAnalysisException>(async () => await table.MergeAsync(
            [SourceRow(99, Day)],
            new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = MergeAction.Update,
                WhenMatchedCondition = X.Equal(Side("target", "d"), X.Lit(LiteralValue.Of(5))),
            }));

        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task Merge_InsertConditionSparkRefuses_IsRefusedForAnEmptySource()
    {
        // An empty source has no row for the condition to see, nor a schema of its own.
        await using var table = await TableAsync();
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<ExpressionAnalysisException>(async () => await table.MergeAsync(
            [],
            new MergeOptions
            {
                Keys = ["id"],
                WhenNotMatched = MergeAction.Insert,
                WhenNotMatchedCondition = X.Equal(Side("source", "d"), X.Lit(LiteralValue.Of(5))),
            }));

        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task Merge_SourcelessConditionSparkRefuses_IsRefusedOnAnEmptyTable()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("d", Date32Type.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);

        await Assert.ThrowsAsync<ExpressionAnalysisException>(async () => await table.MergeAsync(
            [SourceRow(1, Day)],
            new MergeOptions
            {
                Keys = ["id"],
                WhenNotMatchedBySource = MergeAction.Delete,
                WhenNotMatchedBySourceCondition = X.Equal(Side("target", "d"), X.Lit(LiteralValue.Of(5))),
            }));
    }

    [Fact]
    public async Task Merge_ConditionSparkCasts_StillMatches()
    {
        // The check refuses only what the types decide: a string against a date is cast, and matches.
        await using var table = await TableAsync();

        var (metrics, _) = await table.MergeAsync(
            [SourceRow(2, Day)],
            new MergeOptions
            {
                Keys = ["id"],
                WhenMatched = MergeAction.Delete,
                WhenMatchedCondition = X.Equal(Side("target", "d"), X.Lit(LiteralValue.Of("2026-10-03"))),
            });

        Assert.Equal(1, metrics.NumTargetRowsDeleted);
    }

    /// <summary>
    /// A Guid has no Delta or Spark type, so the evaluator cannot type the literal and throws. Two of them
    /// compared folds to "no row matches" in the statistics, and an empty table hands the mask no row, so
    /// both used to commit a no-op where a table with a readable file throws.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Delete_AGuidLiteral_ThrowsWhetherOrNotARowIsRead(bool empty)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        if (!empty)
            await table.WriteAsync([new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build()], 1)]);
        long before = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<NotSupportedException>(async () => await table.DeleteAsync(X.Equal(
            X.Lit(LiteralValue.Of(Guid.Empty)), X.Lit(LiteralValue.Of(new Guid("00000000-0000-0000-0000-000000000001"))))));

        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public async Task Update_ATimeOnlyLiteral_ThrowsOnAnEmptyTable()
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, true)).Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);

        await Assert.ThrowsAsync<NotSupportedException>(async () => await table.UpdateAsync(
            X.Equal(X.Lit(LiteralValue.Of(new TimeOnly(1, 0))), X.Lit(LiteralValue.Of(new TimeOnly(2, 0)))),
            batch => batch));
    }

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
