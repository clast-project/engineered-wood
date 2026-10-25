// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTableOptions.DeleteTrustsStatistics"/>: a predicate DELETE removes a file unread when its
/// statistics prove every row matches (#506, stage 2). The table is four files of ten rows:
/// <list type="bullet">
/// <item>F0: <c>id</c> 0..9, <c>v</c> = <c>id</c>, <c>name</c> <c>ab0</c>..</item>
/// <item>F1: <c>id</c> 100..109, likewise;</item>
/// <item>F2: <c>id</c> 200..209, <c>v</c> and <c>name</c> all NULL;</item>
/// <item>F3: <c>id</c> 300..309, <c>v</c> and <c>name</c> set for 300..304 and NULL for the rest.</item>
/// </list>
/// The oracle is the same table deleted through the functional overload, which reads every file. The NULLs are
/// the point: a NULL row answers NULL, which deletes nothing, and the statistics evaluator, built for skipping,
/// does not keep that apart from FALSE.
/// </summary>
public class DeleteTrustsStatisticsTests : IDisposable
{
    private readonly string _tempDir;

    public DeleteTrustsStatisticsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_deltrust_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("v", Int64Type.Default, true))
        .Field(new Field("name", StringType.Default, true))
        .Build();

    private sealed record Shape(bool Trust = true, bool Cdf = false, bool Dv = true);

    /// <summary>A table and the path of each file by label.</summary>
    private sealed record Built(DeltaTable Table, DataFileCountingFileSystem Fs, Dictionary<string, string> Labels);

    private async Task<Built> CreateAsync(string name, Shape shape)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var configuration = shape.Cdf
            ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }
            : null;
        var table = await DeltaTable.CreateAsync(
            fs, TableSchema, new DeltaTableOptions { DeleteTrustsStatistics = shape.Trust },
            enableDeletionVectors: shape.Dv, configuration: configuration);

        // Label -> path, recorded as each file lands; the label is what the assertions name.
        var labels = new Dictionary<string, string>(StringComparer.Ordinal);
        var known = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (label, start, nullsFrom) in new[] { ("F0", 0, 10), ("F1", 100, 10), ("F2", 200, 0), ("F3", 300, 5) })
        {
            await table.WriteAsync([Rows(start, nullsFrom)]);
            string path = table.CurrentSnapshot.ActiveFiles.Keys.Single(p => !known.Contains(p));
            known.Add(path);
            labels[path] = label;
        }
        return new Built(table, fs, labels);
    }

    /// <summary>Ten rows from <paramref name="start"/>; <c>v</c> and <c>name</c> are NULL from row
    /// <paramref name="nullsFrom"/> on.</summary>
    private static RecordBatch Rows(int start, int nullsFrom)
    {
        var ids = new Int64Array.Builder();
        var vs = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < 10; i++)
        {
            ids.Append(start + i);
            if (i < nullsFrom) { vs.Append(start + i); names.Append("ab" + (start + i)); }
            else { vs.AppendNull(); names.AppendNull(); }
        }
        return new RecordBatch(TableSchema, [ids.Build(), vs.Build(), names.Build()], 10);
    }

    private sealed record Outcome(long Deleted, List<string> Opened, List<long> Remaining);

    /// <summary>Deletes by predicate from one table and through the functional overload from a twin, and checks
    /// the two leave the same rows.</summary>
    private async Task<Outcome> DeleteAsync(Shape shape, Predicate predicate, Func<DeltaTable, Task>? setup = null)
    {
        var tested = await CreateAsync("tested", shape);
        var oracle = await CreateAsync("oracle", shape);
        await using var _ = tested.Table;
        await using var __ = oracle.Table;
        if (setup is not null)
        {
            await setup(tested.Table);
            await setup(oracle.Table);
        }

        tested.Fs.Reset();
        var (deleted, _) = await tested.Table.DeleteAsync(predicate);
        var opened = tested.Fs.OpenedDataFiles
            .Select(p => tested.Labels.TryGetValue(p.Replace('\\', '/'), out var label) ? label : p)
            .OrderBy(l => l, StringComparer.Ordinal).ToList();
        var (oracleDeleted, _) = await oracle.Table.DeleteAsync(DeltaTable.MaskFor(predicate));

        var remaining = await IdsAsync(tested.Table);
        Assert.Equal(oracleDeleted, deleted);
        Assert.Equal(await IdsAsync(oracle.Table), remaining);
        return new Outcome(deleted, opened, remaining);
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    [Fact]
    public async Task ProvenFiles_AreRemovedUnread()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.GreaterThanOrEqual("id", 100L));

        Assert.Empty(outcome.Opened);
        Assert.Equal(30, outcome.Deleted);
    }

    /// <summary>The default reads them, as Spark does: only a partition-only condition is decided unread.</summary>
    [Fact]
    public async Task Default_ReadsEveryCandidate()
    {
        Assert.False(new DeltaTableOptions().DeleteTrustsStatistics);
        var outcome = await DeleteAsync(new Shape(Trust: false), Ex.GreaterThanOrEqual("id", 100L));

        Assert.Equal(["F1", "F2", "F3"], outcome.Opened);
        Assert.Equal(30, outcome.Deleted);
    }

    [Fact]
    public async Task PartlyMatchedFile_IsRead()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.GreaterThanOrEqual("id", 105L));

        Assert.Equal(["F1"], outcome.Opened);
        Assert.Equal(25, outcome.Deleted);
    }

    /// <summary>F3's NULL rows answer NULL and stay; the null count keeps it from being proven.</summary>
    [Fact]
    public async Task NullRows_KeepTheFileFromBeingProven()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.GreaterThanOrEqual("v", 100L));

        Assert.Equal(["F3"], outcome.Opened);
        Assert.Equal(15, outcome.Deleted);
        Assert.Contains(305L, outcome.Remaining);
    }

    /// <summary>
    /// <c>v = 5</c> is AlwaysFalse for F1, F2 and F3 to the evaluator, which counts NULL as not TRUE, and so
    /// <c>NOT (v = 5)</c> is AlwaysTrue. For F2 and F3 that is wrong: their NULL rows answer NULL and must stay.
    /// NOT is never proven, so the files are read.
    /// </summary>
    [Fact]
    public async Task Not_IsNeverProven()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.Not(Ex.Equal("v", 5L)));

        Assert.Equal(["F0", "F1", "F2", "F3"], outcome.Opened);
        Assert.Equal(9 + 10 + 5, outcome.Deleted);
        Assert.Contains(200L, outcome.Remaining);
        Assert.Contains(305L, outcome.Remaining);
    }

    /// <summary>The evaluator answers a prefix test without the null count; F3's NULL names must stay.</summary>
    [Fact]
    public async Task StartsWith_NeedsANullCountOfZero()
    {
        var outcome = await DeleteAsync(new Shape(), new ComparisonPredicate(
            new UnboundReference("name"), ComparisonOperator.StartsWith, new LiteralExpression(LiteralValue.Of("ab"))));

        Assert.Equal(["F3"], outcome.Opened);
        Assert.Equal(25, outcome.Deleted);
    }

    /// <summary>
    /// <c>'ab' STARTS WITH name</c> asks whether name is a prefix of <c>'ab'</c>. No <c>ab0</c>.. is, so nothing
    /// goes; read as <c>name STARTS WITH 'ab'</c>, F0 and F1 were removed.
    /// </summary>
    [Fact]
    public async Task LiteralLeftStartsWith_IsNotTurnedRound()
    {
        var outcome = await DeleteAsync(new Shape(), new ComparisonPredicate(
            new LiteralExpression(LiteralValue.Of("ab")), ComparisonOperator.StartsWith, new UnboundReference("name")));

        Assert.Equal(0, outcome.Deleted);
    }

    /// <summary>The evaluator never answers a non-empty IN AlwaysTrue, so IN is read. (F2 is read too: an
    /// all-NULL column has no bounds, and IN, unlike a comparison, does not rule it out from its null count.)</summary>
    [Fact]
    public async Task In_IsRead()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.In("v", LiteralValue.Of(100L), LiteralValue.Of(101L)));

        Assert.Equal(["F1", "F2"], outcome.Opened);
        Assert.Equal(2, outcome.Deleted);
    }

    /// <summary>
    /// The mask cannot evaluate a function, so the read throws. Proving the other OR branch would remove every
    /// file unread and commit instead.
    /// </summary>
    [Fact]
    public async Task OrWithAFunction_StillFails()
    {
        var built = await CreateAsync("fails", new Shape());
        await using var _ = built.Table;
        long version = built.Table.CurrentSnapshot.Version;
        var predicate = Ex.Or(
            Ex.Equal(new FunctionCall("abs", [new UnboundReference("id")]), new LiteralExpression(LiteralValue.Of(0L))),
            Ex.GreaterThanOrEqual("id", 0L));

        await Assert.ThrowsAnyAsync<Exception>(() => built.Table.DeleteAsync(predicate).AsTask());
        Assert.Equal(version, built.Table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData("id", true)]
    [InlineData("missing", false)]
    [InlineData("nested", false)]
    [InlineData("function", false)]
    public void EvaluatesEverywhere_DeclinesWhatTheMaskCannotEvaluate(string kind, bool expected)
    {
        Predicate other = kind switch
        {
            "id" => Ex.Equal("id", 1L),
            "missing" => Ex.Equal("nope", 1L),
            "nested" => Ex.Equal(UnboundReference.FromParts(["v", "x"]), new LiteralExpression(LiteralValue.Of(1L))),
            _ => Ex.Equal(new FunctionCall("abs", [new UnboundReference("id")]), new LiteralExpression(LiteralValue.Of(1L))),
        };
        var schema = DeltaLake.Schema.SchemaConverter.FromArrowSchema(TableSchema);
        Assert.Equal(expected, DeltaTable.EvaluatesEverywhere(
            Ex.Not(Ex.Or(Ex.GreaterThanOrEqual("v", 0L), other)), schema));
    }

    [Fact]
    public async Task Or_IsProvenByAnyChild()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.Or(Ex.LessThan("id", 0L), Ex.GreaterThanOrEqual("id", 100L)));

        Assert.Empty(outcome.Opened);
        Assert.Equal(30, outcome.Deleted);
    }

    [Fact]
    public async Task IsNull_IsProvenForAnAllNullColumn()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.IsNull("v"));

        Assert.Equal(["F3"], outcome.Opened);
        Assert.Equal(15, outcome.Deleted);
    }

    /// <summary>A literal of another type than the column's is left to the read, and its comparison rules.</summary>
    [Fact]
    public async Task LiteralOfAnotherType_IsRead()
    {
        var outcome = await DeleteAsync(new Shape(), Ex.GreaterThanOrEqual("id", LiteralValue.Of(100)));

        Assert.Equal(["F1", "F2", "F3"], outcome.Opened);
        Assert.Equal(30, outcome.Deleted);
    }

    /// <summary>A file's existing deletion vector: its rows are already gone, and are not counted again.</summary>
    [Fact]
    public async Task ExistingDeletionVector_IsNotCountedAgain()
    {
        var outcome = await DeleteAsync(
            new Shape(Dv: true), Ex.GreaterThanOrEqual("id", 100L),
            setup: table => table.DeleteAsync(Ex.Equal("id", 101L)).AsTask());

        Assert.Empty(outcome.Opened);
        Assert.Equal(29, outcome.Deleted);
    }

    [Fact]
    public async Task ChangeDataFeed_StillReads()
    {
        var outcome = await DeleteAsync(new Shape(Cdf: true), Ex.GreaterThanOrEqual("id", 100L));

        Assert.Equal(["F1", "F2", "F3"], outcome.Opened);
        Assert.Equal(30, outcome.Deleted);
    }

    /// <summary>A partition part is decided from the partition values, a data part from the statistics.</summary>
    [Fact]
    public async Task PartitionAndDataParts_AreProvenTogether()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        async Task<(DeltaTable, DataFileCountingFileSystem)> Create(string name)
        {
            var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
            var table = await DeltaTable.CreateAsync(
                fs, schema, new DeltaTableOptions { DeleteTrustsStatistics = true }, partitionColumns: ["part"],
                enableDeletionVectors: true);
            foreach (var (part, start) in new (string?, int)[] { ("a", 0), ("a", 100), ("b", 100), (null, 100) })
            {
                var parts = new StringArray.Builder();
                var ids = new Int64Array.Builder();
                for (int i = start; i < start + 10; i++)
                {
                    if (part is null) parts.AppendNull(); else parts.Append(part);
                    ids.Append(i);
                }
                await table.WriteAsync([new RecordBatch(schema, [parts.Build(), ids.Build()], 10)]);
            }
            return (table, fs);
        }

        var (tested, fs) = await Create("tested");
        var (oracle, _) = await Create("oracle");
        await using var _ = tested;
        await using var __ = oracle;
        var predicate = Ex.And(Ex.Not(Ex.Equal("part", "b")), Ex.GreaterThanOrEqual("id", 100L));

        fs.Reset();
        var (deleted, _) = await tested.DeleteAsync(predicate);
        var opened = fs.OpenedDataFiles;
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        // a/100 goes unread; a/0 and b are ruled out; the NULL partition answers NULL to NOT part = 'b'.
        Assert.Equal(10, deleted);
        Assert.Equal(oracleDeleted, deleted);
        Assert.Equal(await IdsAsync(oracle), await IdsAsync(tested));
        Assert.All(opened, p => Assert.DoesNotContain("part=a", p));
    }

    public static TheoryData<string> Unprovable =>
        ["nested", "column-vs-column", "partition-column", "not-in", "in", "function", "literal-left-starts-with"];

    [Theory]
    [MemberData(nameof(Unprovable))]
    public void ProvesEveryRowTrue_DeclinesWhatItCannotProve(string kind)
    {
        var pruner = new DeltaFilePruner(
            DeltaLake.Schema.SchemaConverter.FromArrowSchema(TableSchema), partitionColumns: ["name"]);
        var addFile = new DeltaLake.Actions.AddFile
        {
            Path = "f.parquet",
            PartitionValues = new Dictionary<string, string> { ["name"] = "x" },
            Size = 1,
            ModificationTime = 0,
            DataChange = true,
            Stats = """{"numRecords":10,"minValues":{"id":5,"v":5},"maxValues":{"id":5,"v":5},"nullCount":{"id":0,"v":0}}""",
        };
        Predicate leaf = kind switch
        {
            "nested" => Ex.Equal(UnboundReference.FromParts(["id", "x"]), new LiteralExpression(LiteralValue.Of(5L))),
            "column-vs-column" => Ex.Equal(new UnboundReference("id"), new UnboundReference("v")),
            "partition-column" => Ex.Equal("name", "x"),
            "not-in" => new SetPredicate(new UnboundReference("id"),
                [new LiteralExpression(LiteralValue.Of(1L))], SetOperator.NotIn),
            "in" => Ex.In("id", LiteralValue.Of(5L)),
            "literal-left-starts-with" => new ComparisonPredicate(
                new LiteralExpression(LiteralValue.Of(5L)), ComparisonOperator.StartsWith, new UnboundReference("id")),
            _ => Ex.Equal(new FunctionCall("abs", [new UnboundReference("id")]), new LiteralExpression(LiteralValue.Of(5L))),
        };

        Assert.True(pruner.ProvesEveryRowTrue(new DeltaFileStats(addFile), Ex.Equal("id", 5L)));
        Assert.False(pruner.ProvesEveryRowTrue(new DeltaFileStats(addFile), leaf));
    }

    /// <summary>
    /// A top-level column literally named <c>s.a</c> beside struct <c>s</c>'s leaf <c>a</c>: the flattened null
    /// count <c>s.a</c> is the struct leaf's. An older file lacks the added <c>s.a</c> column, which reads as all
    /// NULL there, so <c>`s.a` IS NOT NULL</c> must not be proven from the leaf's count of 0.
    /// </summary>
    [Fact]
    public void ProvesEveryRowTrue_CollidedName_UsesNoNullCount()
    {
        var schema = DeltaLake.Schema.SchemaConverter.FromArrowSchema(new Apache.Arrow.Schema.Builder()
            .Field(new Field("s.a", Int64Type.Default, true))
            .Field(new Field("s", new StructType([new Field("a", Int64Type.Default, true)]), true))
            .Build());
        var addFile = new DeltaLake.Actions.AddFile
        {
            Path = "f.parquet",
            PartitionValues = new Dictionary<string, string>(),
            Size = 1,
            ModificationTime = 0,
            DataChange = true,
            Stats = """{"numRecords":3,"nullCount":{"s":{"a":0}}}""",
        };

        Assert.False(new DeltaFilePruner(schema, []).ProvesEveryRowTrue(new DeltaFileStats(addFile), Ex.IsNotNull("s.a")));
    }
}
