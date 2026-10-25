// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A predicate DELETE over partition columns alone removes whole files from the log without opening them
/// (#506). Every table here is partitioned by <c>part</c> into <c>a</c>, <c>b</c> and a null partition, with
/// two files each (ids 0..9 / 100..109 in <c>a</c>, 10..19 / 110..119 in <c>b</c>, 20..29 / 120..129 in null).
/// The oracle is the same table deleted through the functional overload, which reads every file: the two
/// must leave the same rows and report the same count.
/// </summary>
public class DeleteWithoutReadingTests : IDisposable
{
    private readonly string _tempDir;

    public DeleteWithoutReadingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_delnr_{Guid.NewGuid():N}");
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
        .Field(new Field("part", StringType.Default, true))
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private sealed record Shape(ColumnMappingMode Mode = ColumnMappingMode.None, bool Dv = false, bool Cdf = false);

    private async Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateAsync(string name, Shape shape)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var configuration = shape.Cdf
            ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }
            : null;
        var table = await DeltaTable.CreateAsync(
            fs, TableSchema, partitionColumns: ["part"], columnMappingMode: shape.Mode,
            enableDeletionVectors: shape.Dv, configuration: configuration);
        foreach (int offset in new[] { 0, 100 })
        {
            await table.WriteAsync([Rows(("a", offset), ("b", offset + 10), (null, offset + 20))]);
        }
        Assert.Equal(6, table.CurrentSnapshot.ActiveFiles.Count);
        return (table, fs);
    }

    /// <summary>Ten rows per partition, ids from each start.</summary>
    private static RecordBatch Rows(params (string? Part, int Start)[] partitions)
    {
        var parts = new StringArray.Builder();
        var ids = new Int64Array.Builder();
        foreach (var (part, start) in partitions)
        {
            for (int i = start; i < start + 10; i++)
            {
                if (part is null) parts.AppendNull(); else parts.Append(part);
                ids.Append(i);
            }
        }
        return new RecordBatch(TableSchema, [parts.Build(), ids.Build()], 10 * partitions.Length);
    }

    /// <summary>One side of a <see cref="DeleteBothWaysAsync"/> run.</summary>
    /// <param name="Opened">The data files the DELETE alone opened, by partition directory.</param>
    private sealed record Twin(DeltaTable Table, DataFileCountingFileSystem Fs, long Deleted, IReadOnlyList<string> Opened)
        : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => Table.DisposeAsync();
    }

    /// <summary>
    /// Builds the table twice, runs <paramref name="setup"/> on each, then deletes <paramref name="predicate"/>
    /// by predicate from one and through the whole-file functional overload from the other.
    /// </summary>
    private async Task<(Twin Shortcut, Twin Oracle)> DeleteBothWaysAsync(
        Shape shape, Predicate predicate, Func<DeltaTable, Task>? setup = null)
    {
        var (shortcut, shortcutFs) = await CreateAsync("shortcut", shape);
        var (oracle, oracleFs) = await CreateAsync("oracle", shape);
        if (setup is not null)
        {
            await setup(shortcut);
            await setup(oracle);
        }

        shortcutFs.Reset();
        oracleFs.Reset();
        var (shortcutDeleted, _) = await shortcut.DeleteAsync(predicate);
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        return (new Twin(shortcut, shortcutFs, shortcutDeleted, PartitionsOf(shortcutFs.OpenedDataFiles)),
            new Twin(oracle, oracleFs, oracleDeleted, PartitionsOf(oracleFs.OpenedDataFiles)));
    }

    /// <summary>Each opened file's partition directory (<c>part=a</c>, <c>part=__HIVE_DEFAULT_PARTITION__</c>,
    /// or a mapped table's random prefix), sorted.</summary>
    private static List<string> PartitionsOf(IReadOnlyList<string> paths) =>
        paths.Select(p => Path.GetFileName(Path.GetDirectoryName(p)) ?? "")
            .OrderBy(d => d, StringComparer.Ordinal).ToList();

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

    private static async Task AssertSameOutcomeAsync(Twin shortcut, Twin oracle)
    {
        Assert.Equal(oracle.Deleted, shortcut.Deleted);
        Assert.Equal(await IdsAsync(oracle.Table), await IdsAsync(shortcut.Table));
    }

    /// <summary>What the read path opens for <c>part = 'a'</c>.</summary>
    private static readonly string[] ReadPathOpens =
        ["part=__HIVE_DEFAULT_PARTITION__", "part=__HIVE_DEFAULT_PARTITION__", "part=a", "part=a"];

    private static List<long> Ids(params int[] starts) =>
        starts.SelectMany(s => Enumerable.Range(s, 10)).Select(v => (long)v).OrderBy(v => v).ToList();

    [Fact]
    public async Task PartitionPredicate_RemovesTheFilesWithoutOpeningThem()
    {
        var (shortcut, oracle) = await DeleteBothWaysAsync(new Shape(), Ex.Equal("part", "a"));
        await using var _ = shortcut;
        await using var __ = oracle;

        Assert.Empty(shortcut.Opened);
        Assert.NotEmpty(oracle.Opened);
        Assert.Equal(20, shortcut.Deleted);
        Assert.Equal(Ids(10, 20, 110, 120), await IdsAsync(shortcut.Table));
        await AssertSameOutcomeAsync(shortcut, oracle);

        // The commit is whole-file removes, and no deletion vector anywhere.
        Assert.Equal(4, shortcut.Table.CurrentSnapshot.ActiveFiles.Count);
        Assert.All(shortcut.Table.CurrentSnapshot.ActiveFiles.Values, f => Assert.Null(f.DeletionVector));
    }

    /// <summary>
    /// The null partition: <c>NOT part = 'a'</c> is NULL for it, not TRUE, so its rows stay; <c>part IS NULL</c>
    /// takes them. The answer is the row mask's, three-valued logic included.
    /// </summary>
    [Theory]
    [InlineData("not-a")]
    [InlineData("is-null")]
    [InlineData("in")]
    public async Task NullPartition_FollowsThreeValuedLogic(string kind)
    {
        Predicate predicate = kind switch
        {
            "not-a" => Ex.Not(Ex.Equal("part", "a")),
            "is-null" => Ex.IsNull("part"),
            _ => Ex.In("part", LiteralValue.Of("a"), LiteralValue.Of("b")),
        };
        var (shortcut, oracle) = await DeleteBothWaysAsync(new Shape(), predicate);
        await using var _ = shortcut;
        await using var __ = oracle;

        Assert.Empty(shortcut.Opened);
        Assert.Equal(20 * (kind == "in" ? 2 : 1), shortcut.Deleted);
        await AssertSameOutcomeAsync(shortcut, oracle);
    }

    /// <summary>A file's existing deletion vector: its rows are already gone, and are not counted again.</summary>
    [Fact]
    public async Task ExistingDeletionVector_IsNotCountedAgain()
    {
        var (shortcut, oracle) = await DeleteBothWaysAsync(
            new Shape(Dv: true), Ex.Equal("part", "a"),
            setup: table => table.DeleteAsync(Ex.And(Ex.Equal("part", "a"), Ex.In("id", LiteralValue.Of(3L), LiteralValue.Of(104L)))).AsTask());
        await using var _ = shortcut;
        await using var __ = oracle;

        Assert.Empty(shortcut.Opened);
        Assert.Equal(18, shortcut.Deleted);
        await AssertSameOutcomeAsync(shortcut, oracle);
    }

    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task ColumnMapping_ReadsThePhysicalKeyedPartitionValues(ColumnMappingMode mode)
    {
        var (shortcut, oracle) = await DeleteBothWaysAsync(new Shape(Mode: mode), Ex.Equal("part", "b"));
        await using var _ = shortcut;
        await using var __ = oracle;

        Assert.Empty(shortcut.Opened);
        Assert.Equal(20, shortcut.Deleted);
        await AssertSameOutcomeAsync(shortcut, oracle);
    }

    /// <summary>A typed partition column, against a literal of another type: the mask's own promotion decides.</summary>
    [Fact]
    public async Task TypedPartitionColumn_ComparesAsTheMaskDoes()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("p", Int32Type.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        async Task<(DeltaTable, DataFileCountingFileSystem)> Create(string name)
        {
            var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
            var table = await DeltaTable.CreateAsync(fs, schema, partitionColumns: ["p"]);
            var p = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 30).Select(i => i / 10)).Build();
            var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 30).Select(i => (long)i)).Build();
            await table.WriteAsync([new RecordBatch(schema, [p, ids], 30)]);
            return (table, fs);
        }

        var (shortcut, fs) = await Create("shortcut");
        var (oracle, _) = await Create("oracle");
        await using var __ = shortcut;
        await using var ___ = oracle;
        var predicate = Ex.GreaterThanOrEqual("p", 1L);

        fs.Reset();
        var (deleted, _) = await shortcut.DeleteAsync(predicate);
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        Assert.Equal(0, fs.DataFileOpens);
        Assert.Equal(20, deleted);
        Assert.Equal(oracleDeleted, deleted);
        Assert.Equal(await IdsAsync(oracle), await IdsAsync(shortcut));
    }

    /// <summary>
    /// A predicate that also names a data column is not answered from the partition values, even where the
    /// statistics alone could answer it: stage 1 trusts no statistic. The files of <c>a</c> are read.
    /// </summary>
    [Fact]
    public async Task DataColumnInThePredicate_ReadsTheFiles()
    {
        var (shortcut, oracle) = await DeleteBothWaysAsync(
            new Shape(Dv: true), Ex.And(Ex.Equal("part", "a"), Ex.GreaterThanOrEqual("id", 0L)));
        await using var _ = shortcut;
        await using var __ = oracle;

        // The two files of a, and the null partition's two: the statistics cannot rule NULL = 'a' out.
        Assert.Equal(ReadPathOpens, shortcut.Opened);
        Assert.Equal(20, shortcut.Deleted);
        await AssertSameOutcomeAsync(shortcut, oracle);
    }

    /// <summary>The change feed needs the deleted rows, so the files are read and every row reaches it.</summary>
    [Fact]
    public async Task ChangeDataFeed_StillReadsTheFiles()
    {
        var (shortcut, oracle) = await DeleteBothWaysAsync(new Shape(Cdf: true), Ex.Equal("part", "a"));
        await using var _ = shortcut;
        await using var __ = oracle;

        // The two files of a, and the null partition's two: the statistics cannot rule NULL = 'a' out.
        Assert.Equal(ReadPathOpens, shortcut.Opened);
        await AssertSameOutcomeAsync(shortcut, oracle);

        long version = shortcut.Table.CurrentSnapshot.Version;
        var deletedIds = new List<long>();
        await foreach (var batch in shortcut.Table.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var change = (StringArray)batch.Column(batch.Schema.GetFieldIndex("_change_type"));
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal("delete", change.GetString(i));
                deletedIds.Add(id.GetValue(i)!.Value);
            }
        }
        deletedIds.Sort();
        Assert.Equal(Ids(0, 100), deletedIds);
    }

    /// <summary>A file whose statistics carry no row count has none to report, so it alone is read.</summary>
    [Fact]
    public async Task NoRowCount_ReadsThatFile()
    {
        var (shortcut, oracle) = await DeleteBothWaysAsync(
            new Shape(), Ex.Equal("part", "a"), setup: DropStatsOfOneFileOfAAsync);
        await using var _ = shortcut;
        await using var __ = oracle;

        Assert.Equal(["part=a"], shortcut.Opened);
        Assert.Equal(20, shortcut.Deleted);
        await AssertSameOutcomeAsync(shortcut, oracle);
    }

    /// <summary>Re-adds one file of partition <c>a</c> without statistics, as a writer that kept none would.</summary>
    private static async Task DropStatsOfOneFileOfAAsync(DeltaTable table)
    {
        var file = table.CurrentSnapshot.ActiveFiles.Values
            .Where(f => f.PartitionValues.Values.Contains("a"))
            .OrderBy(f => f.Path, StringComparer.Ordinal)
            .First();
        await using var transaction = table.StartTransaction();
        transaction.StageActions(
        [
            new RemoveFile
            {
                Path = file.Path,
                DeletionTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = false,
            },
            file with { Stats = null, TypedStats = null, DataChange = false },
        ]);
        await transaction.CommitAsync();
        Assert.Null(table.CurrentSnapshot.ActiveFiles[file.Path].GetNumRecords());
    }

    [Fact]
    public async Task Transaction_DeletesWithoutOpening()
    {
        var (table, fs) = await CreateAsync("tx", new Shape());
        await using var _ = table;
        fs.Reset();

        await using var transaction = table.StartTransaction();
        Assert.Equal(20, await transaction.DeleteAsync(Ex.Equal("part", "b")));
        await transaction.CommitAsync();

        Assert.Equal(0, fs.DataFileOpens);
        Assert.Equal(Ids(0, 20, 100, 120), await IdsAsync(table));
    }

    public static TheoryData<string, bool> References => new()
    {
        { "partition", true },
        { "and-or-not-in", true },
        { "data", false },
        { "mixed", false },
        { "function", false },
        { "case", false },
    };

    [Theory]
    [MemberData(nameof(References))]
    public void ReferencesOnlyPartitionColumns_DeclinesWhatItCannotSee(string kind, bool expected)
    {
        Predicate predicate = kind switch
        {
            "partition" => Ex.Equal("part", "a"),
            "and-or-not-in" => Ex.And(
                Ex.Or(Ex.IsNull("part"), Ex.Not(Ex.Equal("part", "a"))),
                Ex.In("part", LiteralValue.Of("b"))),
            "data" => Ex.Equal("id", 1L),
            "mixed" => Ex.Or(Ex.Equal("part", "a"), Ex.Equal("id", 1L)),
            "function" => Ex.Equal(
                new FunctionCall("upper", [new UnboundReference("part")]), new LiteralExpression(LiteralValue.Of("A"))),
            // A reference must match the partition column's name exactly.
            _ => Ex.Equal("PART", "a"),
        };
        Assert.Equal(expected, DeltaTable.ReferencesOnlyPartitionColumns(predicate, ["part"]));
    }

    [Fact]
    public void ReferencesOnlyPartitionColumns_UnpartitionedTable_IsFalse() =>
        Assert.False(DeltaTable.ReferencesOnlyPartitionColumns(Ex.True, []));
}
