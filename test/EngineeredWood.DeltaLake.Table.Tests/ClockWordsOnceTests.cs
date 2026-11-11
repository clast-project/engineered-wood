// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #801: an operation reads the clock words of its expressions (<c>CAST('now' AS TIMESTAMP)</c>,
/// <c>CAST('today' AS DATE)</c>) once, when it takes them, so every batch and file it evaluates sees one instant.
/// </summary>
/// <remarks>
/// Each test fixes the operation's clock (<see cref="OperationClock"/>) at <see cref="Now"/>, in 2001, with rows a day
/// either side of it. A word the operation resolved names 2001; one left to the per-evaluation fold would name the
/// real day, decades later, and every row would fall before it: the tests tell the two apart.
/// </remarks>
public class ClockWordsOnceTests : IDisposable
{
    private static readonly DateTimeOffset Now = new(2001, 2, 3, 4, 5, 6, TimeSpan.Zero);
    private static readonly TimestampType Timestamp = new(TimeUnit.Microsecond, "UTC");

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), $"delta_clock_{Guid.NewGuid():N}");

    public ClockWordsOnceTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("ts", Timestamp, true))
        .Build();

    /// <summary>Rows <paramref name="first"/> onward: even ids a day before <see cref="Now"/>, odd ones a day after.</summary>
    private static RecordBatch Rows(Apache.Arrow.Schema schema, long first, int count)
    {
        var ids = new Int64Array.Builder();
        var stamps = new TimestampArray.Builder(Timestamp);
        for (long i = first; i < first + count; i++)
        {
            ids.Append(i);
            stamps.Append(i % 2 == 0 ? Now.AddDays(-1) : Now.AddDays(1));
        }

        return new RecordBatch(schema, [ids.Build(), stamps.Build()], count);
    }

    /// <summary>Three files of four rows: ids 0-11, half before <see cref="Now"/>.</summary>
    private async Task<DeltaTable> CreateAsync(bool deletionVectors = false)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), Schema, enableDeletionVectors: deletionVectors);
        for (int f = 0; f < 3; f++)
            await table.WriteAsync([Rows(table.ArrowSchema, f * 4, 4)]);
        return table;
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Columns = ["id"] }))
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    ids.Add(((Int64Array)batch.Column(0)).GetValue(i)!.Value);
            }
        }

        ids.Sort();
        return ids;
    }

    private static readonly Predicate BeforeNow = SparkSqlParser.ParsePredicate("ts < CAST('now' AS TIMESTAMP)");

    private static IEnumerable<long> Odd => Enumerable.Range(0, 12).Where(i => i % 2 == 1).Select(i => (long)i);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADeleteReadsNowOnceForEveryFile(bool deletionVectors)
    {
        await using var table = await CreateAsync(deletionVectors);
        using (OperationClock.Fix(Now))
        {
            var (deleted, _) = await table.DeleteAsync(BeforeNow);
            Assert.Equal(6, deleted);
        }

        Assert.Equal(Odd, await IdsAsync(table));
    }

    [Fact]
    public async Task AnUpdateReadsNowOnceForEveryFile()
    {
        await using var table = await CreateAsync();
        using (OperationClock.Fix(Now))
        {
            var (updated, _) = await table.UpdateAsync(BeforeNow, batch =>
            {
                var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
                var negated = new Int64Array.Builder();
                for (int i = 0; i < batch.Length; i++)
                    negated.Append(-ids.GetValue(i)!.Value - 100);
                var columns = Enumerable.Range(0, batch.ColumnCount)
                    .Select(c => c == batch.Schema.GetFieldIndex("id") ? negated.Build() : batch.Column(c)).ToArray();
                return new RecordBatch(batch.Schema, columns, batch.Length);
            });
            Assert.Equal(6, updated);
        }

        var ids = await IdsAsync(table);
        Assert.Equal(6, ids.Count(i => i < 0));
        Assert.Equal(Odd, ids.Where(i => i >= 0));
    }

    [Fact]
    public async Task AReplaceWhereReadsNowOnce()
    {
        await using var table = await CreateAsync();
        using (OperationClock.Fix(Now))
        {
            // One replacement row inside the window (before Now), replacing the six there.
            var replacement = new RecordBatch(table.ArrowSchema,
                [new Int64Array.Builder().Append(100).Build(),
                 new TimestampArray.Builder(Timestamp).Append(Now.AddHours(-1)).Build()], 1);
            await table.OverwriteWhereAsync([replacement], BeforeNow);
        }

        Assert.Equal(Odd.Append(100L).OrderBy(i => i), await IdsAsync(table));
    }

    [Fact]
    public async Task AMergeReadsNowOnceForItsConditionsAndValues()
    {
        await using var table = await CreateAsync();
        var source = new RecordBatch(table.ArrowSchema,
            [new Int64Array.Builder().AppendRange(Enumerable.Range(0, 12).Select(i => (long)i)).Build(),
             new TimestampArray.Builder(Timestamp).AppendRange(Enumerable.Repeat(Now, 12)).Build()], 12);

        using (OperationClock.Fix(Now))
        {
            await table.MergeAsync([source], new MergeOptions
            {
                Keys = ["id"],
                WhenMatched =
                [
                    // Before Now: moved to Now + 2 days, a value whose clock word is read at the same instant.
                    MergeAction.UpdateSet(new MergeAssignment("ts",
                        SparkSqlParser.ParseExpression("CAST('tomorrow' AS TIMESTAMP)")))
                        .When(SparkSqlParser.ParsePredicate("target.ts < CAST('now' AS TIMESTAMP)")),
                ],
            });
        }

        var stamps = new Dictionary<long, DateTimeOffset>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                {
                    stamps[((Int64Array)batch.Column(0)).GetValue(i)!.Value] =
                        ((TimestampArray)batch.Column(1)).GetTimestamp(i)!.Value;
                }
            }
        }

        var tomorrow = new DateTimeOffset(2001, 2, 4, 0, 0, 0, TimeSpan.Zero);
        Assert.All(Enumerable.Range(0, 12), i => Assert.Equal(i % 2 == 0 ? tomorrow : Now.AddDays(1), stamps[i]));
    }

    /// <summary>
    /// A CHECK constraint's clock word is read once per write, as Spark reads it once per query: a row a day after
    /// <see cref="Now"/> violates <c>ts &lt;= now</c>, which an unresolved word, read today, would let through.
    /// </summary>
    [Fact]
    public async Task ACheckConstraintReadsNowOncePerWrite()
    {
        await using var table = await CreateAsync();
        using (OperationClock.Fix(Now))
        {
            await table.DeleteAsync(SparkSqlParser.ParsePredicate("ts > CAST('now' AS TIMESTAMP)"));
            await table.AddConstraintAsync("not_future", "ts <= CAST('now' AS TIMESTAMP)");

            var future = new RecordBatch(table.ArrowSchema,
                [new Int64Array.Builder().Append(200).Build(),
                 new TimestampArray.Builder(Timestamp).Append(Now.AddDays(1)).Build()], 1);
            await Assert.ThrowsAsync<DeltaFormatException>(async () => await table.WriteAsync([future]));

            var past = new RecordBatch(table.ArrowSchema,
                [new Int64Array.Builder().Append(201).Build(),
                 new TimestampArray.Builder(Timestamp).Append(Now.AddDays(-1)).Build()], 1);
            await table.WriteAsync([past]);
        }

        Assert.Contains(201L, await IdsAsync(table));
        Assert.DoesNotContain(200L, await IdsAsync(table));
    }

    /// <summary>A generated column defined by a clock word takes the value it names at the write's one instant.</summary>
    [Fact]
    public async Task AGeneratedColumnReadsTodayOncePerWrite()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("ts", Timestamp, true))
            .Field(new Field("day", Date32Type.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = "CAST('today' AS DATE)" }))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);

        using (OperationClock.Fix(Now))
        {
            var rows = Rows(new Apache.Arrow.Schema.Builder().Field(schema.FieldsList[0]).Field(schema.FieldsList[1]).Build(), 0, 4);
            await table.WriteAsync([rows]);
        }

        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Columns = ["day"] }))
        {
            using (batch)
            {
                var days = (Date32Array)batch.Column(0);
                for (int i = 0; i < batch.Length; i++)
                    Assert.Equal(new DateTime(2001, 2, 3), days.GetDateTimeOffset(i)!.Value.Date);
            }
        }
    }
}
