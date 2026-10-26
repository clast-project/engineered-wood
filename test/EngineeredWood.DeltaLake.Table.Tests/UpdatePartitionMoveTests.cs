// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// An UPDATE that changes a row's partition values moves the row to the partition they name, as delta-spark's
/// UPDATE does (#519). The rewrite used to take the source file's partition values for every row, so the commit
/// succeeded and the new value was lost. Every table here is partitioned by <c>part</c> and seeded with ids 1, 2
/// in <c>a</c>, 3 in <c>b</c> and 4 in the null partition; <c>v</c> is ten times the id.
/// </summary>
public class UpdatePartitionMoveTests : IDisposable
{
    private readonly string _tempDir;

    public UpdatePartitionMoveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_updmove_{Guid.NewGuid():N}");
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
        .Field(new Field("v", Int64Type.Default, true))
        .Build();

    private static RecordBatch Rows(params (string? Part, long Id)[] rows)
    {
        var part = new StringArray.Builder();
        var id = new Int64Array.Builder();
        var v = new Int64Array.Builder();
        foreach (var (p, i) in rows)
        {
            if (p is null) part.AppendNull(); else part.Append(p);
            id.Append(i);
            v.Append(i * 10);
        }
        return new RecordBatch(TableSchema, [part.Build(), id.Build(), v.Build()], rows.Length);
    }

    private static readonly (string? Part, long Id)[] Seed = [("a", 1), ("a", 2), ("b", 3), (null, 4)];

    private async Task<DeltaTable> CreateAsync(
        ColumnMappingMode mapping = ColumnMappingMode.None, bool rowTracking = false, bool cdf = false)
    {
        var configuration = cdf ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" } : null;
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), TableSchema,
            partitionColumns: ["part"], columnMappingMode: mapping, enableRowTracking: rowTracking,
            configuration: configuration);
        await table.WriteAsync([Rows(Seed)]);
        return table;
    }

    /// <summary>id → (part, v), as a full read sees them.</summary>
    private static async Task<SortedDictionary<long, (string? Part, long V)>> ReadAsync(
        DeltaTable table, Expressions.Predicate? filter = null)
    {
        var rows = new SortedDictionary<long, (string?, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = filter }))
        {
            var part = (StringArray)batch.Column(batch.Schema.GetFieldIndex("part"));
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var v = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("v"));
            for (int i = 0; i < batch.Length; i++)
                rows[id.GetValue(i)!.Value] = (part.IsNull(i) ? null : part.GetString(i), v.GetValue(i)!.Value);
        }
        return rows;
    }

    /// <summary>Each active file's partition value, one entry per file.</summary>
    private static List<string?> FilePartitions(DeltaTable table, string key = "part") =>
        table.CurrentSnapshot.ActiveFiles.Values
            .Select(f => f.PartitionValues[key])
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

    private static Func<RecordBatch, BooleanArray> IdIs(long target) => b =>
    {
        var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
        return new BooleanArray.Builder()
            .AppendRange(Enumerable.Range(0, b.Length).Select(i => id.GetValue(i) == target)).Build();
    };

    /// <summary>An updater setting <c>part</c> to <paramref name="value"/> on every row it is given.</summary>
    private static Func<RecordBatch, RecordBatch> SetPart(string? value) => b =>
    {
        var parts = new StringArray.Builder();
        for (int i = 0; i < b.Length; i++)
        {
            if (value is null) parts.AppendNull(); else parts.Append(value);
        }
        int index = b.Schema.GetFieldIndex("part");
        var columns = b.Arrays.ToArray();
        columns[index] = parts.Build();
        return new RecordBatch(b.Schema, columns, b.Length);
    };

    public static TheoryData<string> EntryPoints => new()
    {
        "predicate", "mask", "transaction-predicate", "transaction-mask", "update-rows",
    };

    /// <summary>Moves the row with <paramref name="id"/> to <paramref name="part"/> through one UPDATE entry point.</summary>
    private static async Task MoveAsync(DeltaTable table, string entryPoint, long id, string? part)
    {
        switch (entryPoint)
        {
            case "predicate":
                await table.UpdateAsync(Ex.Equal("id", id), SetPart(part));
                break;
            case "mask":
                await table.UpdateAsync(IdIs(id), SetPart(part));
                break;
            case "transaction-predicate":
            case "transaction-mask":
            {
                await using var transaction = table.StartTransaction();
                if (entryPoint == "transaction-predicate")
                    await transaction.UpdateAsync(Ex.Equal("id", id), SetPart(part));
                else
                    await transaction.UpdateAsync(IdIs(id), SetPart(part));
                await transaction.CommitAsync();
                break;
            }
            case "update-rows":
                await table.UpdateRowsAsync(await PartUpdatesAsync(table, id, part));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(entryPoint));
        }
    }

    /// <summary>An <see cref="DeltaTable.UpdateRowsAsync(RecordBatch, string, CancellationToken)"/> batch setting
    /// the row with <paramref name="id"/>'s <c>part</c>.</summary>
    private static async Task<RecordBatch> PartUpdatesAsync(DeltaTable table, long id, string? part)
    {
        string? path = null;
        long position = -1;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
        {
            var ids = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var paths = (StringArray)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.FilePathSuffix);
            var positions = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                if (ids.GetValue(i) == id)
                    (path, position) = (paths.GetString(i), positions.GetValue(i)!.Value);
            }
        }
        Assert.NotNull(path);

        var parts = new StringArray.Builder();
        if (part is null) parts.AppendNull(); else parts.Append(part);
        return new RecordBatch(
            new Apache.Arrow.Schema.Builder()
                .Field(new Field(RowSelection.DefaultMetadataPrefix + RowSelection.FilePathColumnSuffix, StringType.Default, false))
                .Field(new Field(RowSelection.DefaultMetadataPrefix + RowSelection.RowIndexColumnSuffix, Int64Type.Default, false))
                .Field(new Field("part", StringType.Default, true))
                .Build(),
            [new StringArray.Builder().Append(path).Build(), new Int64Array.Builder().Append(position).Build(), parts.Build()],
            1);
    }

    [Theory]
    [MemberData(nameof(EntryPoints))]
    public async Task ChangedPartitionValue_MovesTheRow(string entryPoint)
    {
        await using var table = await CreateAsync();
        await MoveAsync(table, entryPoint, 1, "c");

        var rows = await ReadAsync(table);
        Assert.Equal(("c", 10L), rows[1]);
        Assert.Equal(("a", 20L), rows[2]); // its file-mate stays
        Assert.Equal(("b", 30L), rows[3]);
        Assert.Equal((null, 40L), rows[4]);

        Assert.Equal([null, "a", "b", "c"], FilePartitions(table));
        var moved = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.PartitionValues["part"] == "c");
        Assert.StartsWith("part=c/", moved.Path, StringComparison.Ordinal);

        // A scan pruned to the new partition finds it there, and the old one no longer holds it.
        Assert.Equal([1L], (await ReadAsync(table, Ex.Equal("part", "c"))).Keys);
        Assert.Equal([2L], (await ReadAsync(table, Ex.Equal("part", "a"))).Keys);
    }

    [Theory]
    [MemberData(nameof(EntryPoints))]
    public async Task MovingIntoAndOutOfTheNullPartition(string entryPoint)
    {
        await using var table = await CreateAsync();
        await MoveAsync(table, entryPoint, 3, null);
        await MoveAsync(table, entryPoint, 4, "d");

        var rows = await ReadAsync(table);
        Assert.Equal((null, 30L), rows[3]);
        Assert.Equal(("d", 40L), rows[4]);
        // Both files whose only row left are gone: nothing stays behind in b, and the null partition holds only
        // id 3's new file.
        Assert.Equal([null, "a", "d"], FilePartitions(table));
    }

    /// <summary>An empty string set by an UPDATE is stored as null, as the write path stores it.</summary>
    [Theory]
    [MemberData(nameof(EntryPoints))]
    public async Task EmptyString_MovesTheRowToTheNullPartition(string entryPoint)
    {
        await using var table = await CreateAsync();
        await MoveAsync(table, entryPoint, 1, "");

        Assert.Equal((null, 10L), (await ReadAsync(table))[1]);
        Assert.Equal(2, FilePartitions(table).Count(p => p is null)); // id 4's file and id 1's
    }

    /// <summary>Rows sharing a file can move to different partitions, each to its own file.</summary>
    [Fact]
    public async Task RowsOfOneFile_MoveToTheirOwnPartitions()
    {
        await using var table = await CreateAsync();
        await table.UpdateAsync(
            b => new BooleanArray.Builder().AppendRange(Enumerable.Repeat(true, b.Length)).Build(),
            b =>
            {
                var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                var parts = new StringArray.Builder();
                for (int i = 0; i < b.Length; i++)
                    parts.Append("p" + id.GetValue(i));
                var columns = b.Arrays.ToArray();
                columns[b.Schema.GetFieldIndex("part")] = parts.Build();
                return new RecordBatch(b.Schema, columns, b.Length);
            });

        var rows = await ReadAsync(table);
        Assert.Equal(["p1", "p2", "p3", "p4"], rows.Values.Select(r => r.Part));
        Assert.Equal(["p1", "p2", "p3", "p4"], FilePartitions(table));
    }

    [Fact]
    public async Task UnchangedPartitionValue_StaysInItsFile()
    {
        await using var table = await CreateAsync();
        var before = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.PartitionValues["part"] == "b");

        await table.UpdateAsync(Ex.Equal("id", 3L), b =>
        {
            var columns = b.Arrays.ToArray();
            columns[b.Schema.GetFieldIndex("v")] = new Int64Array.Builder().AppendRange(Enumerable.Repeat(99L, b.Length)).Build();
            return new RecordBatch(b.Schema, columns, b.Length);
        });

        var after = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.PartitionValues["part"] == "b");
        Assert.NotEqual(before.Path, after.Path);
        Assert.StartsWith("part=b/", after.Path, StringComparison.Ordinal);
        Assert.Equal(("b", 99L), (await ReadAsync(table))[3]);
    }

    /// <summary>
    /// A row whose partition value the UPDATE left alone keeps the value as the log stored it, even when that is not
    /// how this writer would spell it: re-formatting would move it into a new directory, and a reader comparing
    /// values as text would see two partitions.
    /// </summary>
    [Fact]
    public async Task UnchangedPartitionValue_KeepsItsStoredSpelling()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int32Type.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "spelling")), schema, partitionColumns: ["n"]);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int32Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(1).Build()], 1)]);
        var file = table.CurrentSnapshot.ActiveFiles.Values.Single();
        await using (var transaction = table.StartTransaction())
        {
            transaction.StageActions(
            [
                new RemoveFile { Path = file.Path, DeletionTimestamp = 1, DataChange = false },
                file with { PartitionValues = new Dictionary<string, string> { ["n"] = "01" }, DataChange = false },
            ]);
            await transaction.CommitAsync();
        }

        await table.UpdateAsync(Ex.Equal("id", 1L), b =>
        {
            var columns = b.Arrays.ToArray();
            columns[b.Schema.GetFieldIndex("id")] = new Int64Array.Builder().Append(2).Build();
            return new RecordBatch(b.Schema, columns, b.Length);
        });

        var rewritten = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal("01", rewritten.PartitionValues["n"]);
        Assert.Equal(file.Path[..file.Path.LastIndexOf('/')], rewritten.Path[..rewritten.Path.LastIndexOf('/')]);
    }

    /// <summary>Under column mapping the moved file's partition values are keyed by the physical name, as every
    /// other add's are.</summary>
    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task ColumnMapping_MovedFileIsKeyedByThePhysicalName(ColumnMappingMode mode)
    {
        await using var table = await CreateAsync(mode);
        string physical = ColumnMapping.BuildLogicalToPhysicalMap(table.CurrentSnapshot.Schema, mode)["part"];
        Assert.NotEqual("part", physical);

        await table.UpdateAsync(Ex.Equal("id", 1L), SetPart("c"));

        Assert.Contains("c", FilePartitions(table, physical));
        Assert.Equal(("c", 10L), (await ReadAsync(table))[1]);
        Assert.Equal([1L], (await ReadAsync(table, Ex.Equal("part", "c"))).Keys);
    }

    /// <summary>A moved row is the same row: it keeps its row id, and its commit version is the UPDATE's.</summary>
    [Theory]
    [InlineData("predicate")]
    [InlineData("update-rows")]
    public async Task RowTracking_MovedRowKeepsItsId(string entryPoint)
    {
        await using var table = await CreateAsync(rowTracking: true);
        var before = await RowIdsAsync(table);

        await MoveAsync(table, entryPoint, 1, "c");

        var after = await RowIdsAsync(table);
        Assert.Equal(before.Keys.OrderBy(k => k), after.Keys.OrderBy(k => k));
        foreach (long id in before.Keys)
            Assert.Equal(before[id].RowId, after[id].RowId);
        Assert.Equal(table.CurrentSnapshot.Version, after[1].Version);
        Assert.Equal(before[2].Version, after[2].Version);
    }

    private static async Task<Dictionary<long, (long? RowId, long? Version)>> RowIdsAsync(DeltaTable table)
    {
        var map = new Dictionary<long, (long?, long?)>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var rowId = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
            var version = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowCommitVersionColumnName));
            for (int i = 0; i < b.Length; i++)
                map[id.GetValue(i)!.Value] = (rowId.GetValue(i), version.GetValue(i));
        }
        return map;
    }

    /// <summary>The change feed agrees with the table: the pre-image is in the old partition, the post-image in the
    /// new.</summary>
    [Theory]
    [InlineData("predicate")]
    [InlineData("update-rows")]
    public async Task ChangeFeed_PostImageIsInTheNewPartition(string entryPoint)
    {
        await using var table = await CreateAsync(cdf: true);
        await MoveAsync(table, entryPoint, 1, "c");
        long version = table.CurrentSnapshot.Version;

        var feed = new List<(string Change, long Id, string? Part)>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var change = (StringArray)b.Column(b.Schema.GetFieldIndex(CdfConfig.ChangeTypeColumn));
            var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
            var part = (StringArray)b.Column(b.Schema.GetFieldIndex("part"));
            for (int i = 0; i < b.Length; i++)
                feed.Add((change.GetString(i), id.GetValue(i)!.Value, part.IsNull(i) ? null : part.GetString(i)));
        }

        Assert.Equal(
            [(CdfConfig.UpdatePostimage, 1L, "c"), (CdfConfig.UpdatePreimage, 1L, "a")],
            feed.OrderBy(r => r.Change, StringComparer.Ordinal).ToList());
    }

    /// <summary>A generated partition column follows the column it is generated from: before, the recomputed value
    /// was dropped and the row sat in a partition its own expression contradicted.</summary>
    [Fact]
    public async Task GeneratedPartitionColumn_FollowsItsSource()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("source", StringType.Default, true))
            .Field(new Field("part", StringType.Default, true,
                new Dictionary<string, string> { ["delta.generationExpression"] = "source" }))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "generated")), schema, partitionColumns: ["part"]);
        var withoutPart = new Apache.Arrow.Schema.Builder()
            .Field(new Field("source", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await table.WriteAsync([new RecordBatch(withoutPart,
            [new StringArray.Builder().Append("a").Build(), new Int64Array.Builder().Append(1).Build()], 1)]);

        await table.UpdateAsync(Ex.Equal("id", 1L), b =>
        {
            var columns = b.Arrays.ToArray();
            columns[b.Schema.GetFieldIndex("source")] = new StringArray.Builder().Append("z").Build();
            return new RecordBatch(b.Schema, columns, b.Length);
        });

        var file = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal("z", file.PartitionValues["part"]);
        await foreach (var b in table.ReadAllAsync())
            Assert.Equal("z", ((StringArray)b.Column(b.Schema.GetFieldIndex("part"))).GetString(0));
    }

    // ── Partition values the log cannot hold ─────────────────────────────────────────────────────────────

    private static readonly Apache.Arrow.Schema IntPartitioned = new Apache.Arrow.Schema.Builder()
        .Field(new Field("n", Int32Type.Default, true))
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch IntRows(int n, long id) => new(IntPartitioned,
        [new Int32Array.Builder().Append(n).Build(), new Int64Array.Builder().Append(id).Build()], 1);

    /// <summary>The batch with its <c>n</c> column replaced by the string <paramref name="text"/>.</summary>
    private static RecordBatch WithStringN(RecordBatch b, string text)
    {
        var fields = b.Schema.FieldsList.ToArray();
        var columns = b.Arrays.ToArray();
        int index = b.Schema.GetFieldIndex("n");
        fields[index] = new Field("n", StringType.Default, true);
        columns[index] = new StringArray.Builder().AppendRange(Enumerable.Repeat(text, b.Length)).Build();
        return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, b.Length);
    }

    /// <summary>
    /// A partition column retyped by the updater is refused before anything is committed: the value would become
    /// log metadata the read cannot parse (<c>n: "abc"</c> under an integer column). Partition values are the
    /// library's to format whoever writes the data bytes, so a host writer does not lift the check.
    /// </summary>
    [Theory]
    [InlineData("update-rows", false)]
    [InlineData("update-rows", true)]
    [InlineData("predicate", true)]
    public async Task RetypedPartitionColumn_IsRefused(string entryPoint, bool hostWriter)
    {
        // Seeded through the built-in writer, so the UPDATE has a real file to read; the host writer, when there is
        // one, would only have received the rewrite.
        var fs = new LocalTableFileSystem(Path.Combine(_tempDir, "retyped"));
        await using (var seed = await DeltaTable.CreateAsync(fs, IntPartitioned, partitionColumns: ["n"]))
            await seed.WriteAsync([IntRows(1, 1)]);
        await using var table = await DeltaTable.OpenAsync(fs, hostWriter
            ? DeltaTableOptions.Default with { DataFileWriter = new DiscardingWriter() }
            : DeltaTableOptions.Default);
        long version = table.CurrentSnapshot.Version;

        Task act = entryPoint == "predicate"
            ? table.UpdateAsync(Ex.Equal("id", 1L), b => WithStringN(b, "abc")).AsTask()
            : table.UpdateRowsAsync(
                RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>
                {
                    [table.CurrentSnapshot.ActiveFiles.Values.Single().Path] = [0L],
                }),
                (_, batches, _) => batches.Select(b => WithStringN(b, "abc")).ToList()).AsTask();

        await Assert.ThrowsAsync<ArgumentException>(() => act);
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    /// <summary>Writes nothing: the refusal must come before any data file is written.</summary>
    private sealed class DiscardingWriter : IDataFileWriter
    {
        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            await foreach (var _ in batches.WithCancellation(cancellationToken)) { }
            return 4096;
        }
    }

    /// <summary>
    /// A decimal(38,0) partition value wider than <see cref="decimal"/> holds. Every UPDATE of such a file formats its
    /// partition values to decide which rows stay, and formatting through <see cref="decimal"/> overflowed, so even
    /// an UPDATE of another column failed.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WideDecimalPartition_UpdatesAndMoves(bool move)
    {
        const string Wide = "99999999999999999999999999999999999999";
        const string Other = "-12345678901234567890123456789012345678";
        var type = new Decimal128Type(38, 0);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", type, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "wide")), schema, partitionColumns: ["d"]);
        await table.WriteAsync([new RecordBatch(schema,
            [new Decimal128Array.Builder(type).Append(1m).Build(), new Int64Array.Builder().Append(1).Build()], 1)]);
        var file = table.CurrentSnapshot.ActiveFiles.Values.Single();
        await using (var transaction = table.StartTransaction())
        {
            transaction.StageActions(
            [
                new RemoveFile { Path = file.Path, DeletionTimestamp = 1, DataChange = false },
                file with { PartitionValues = new Dictionary<string, string> { ["d"] = Wide }, DataChange = false },
            ]);
            await transaction.CommitAsync();
        }

        await table.UpdateAsync(Ex.Equal("id", 1L), b =>
        {
            var columns = b.Arrays.ToArray();
            columns[b.Schema.GetFieldIndex("id")] = new Int64Array.Builder().Append(2).Build();
            if (move)
                columns[b.Schema.GetFieldIndex("d")] = WideDecimal(type, Other);
            return new RecordBatch(b.Schema, columns, b.Length);
        });

        var rewritten = table.CurrentSnapshot.ActiveFiles.Values.Single();
        Assert.Equal(move ? Other : Wide, rewritten.PartitionValues["d"]);
    }

    /// <summary>The append path's partition split formats with the same code, and overflowed the same way.</summary>
    [Fact]
    public async Task WideDecimalPartition_Appends()
    {
        const string Wide = "-99999999999999999999999999999999.999999";
        var type = new Decimal128Type(38, 6);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", type, true))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "wide-append")), schema, partitionColumns: ["d"]);

        await table.WriteAsync([new RecordBatch(schema,
            [WideDecimal(type, "-99999999999999999999999999999999999999"), new Int64Array.Builder().Append(1).Build()],
            1)]);

        Assert.Equal(Wide, table.CurrentSnapshot.ActiveFiles.Values.Single().PartitionValues["d"]);
    }

    /// <summary>A one-value Decimal128 array holding <paramref name="text"/> exactly, built from its two's-complement
    /// bytes since <see cref="decimal"/> cannot hold it.</summary>
    private static Decimal128Array WideDecimal(Decimal128Type type, string text)
    {
        var unscaled = System.Numerics.BigInteger.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
        var bytes = new byte[16];
        byte[] le = unscaled.ToByteArray();
        System.Array.Copy(le, bytes, le.Length);
        if (unscaled.Sign < 0)
        {
            for (int i = le.Length; i < 16; i++)
                bytes[i] = 0xFF;
        }
        return new Decimal128Array(new ArrayData(type, 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(bytes)]));
    }

    /// <summary>The host-driven UPDATE now holds a moved row to the write path's partition rule.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task UpdateRows_NullOrEmptyInNonNullablePartition_IsRefused(string? value)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", Int64Type.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "notnull")), schema, partitionColumns: ["part"]);
        await table.WriteAsync([new RecordBatch(schema,
            [new StringArray.Builder().Append("a").Build(), new Int64Array.Builder().Append(1).Build(),
                new Int64Array.Builder().Append(10).Build()], 1)]);
        long version = table.CurrentSnapshot.Version;

        var updates = await PartUpdatesAsync(table, 1, value);
        await Assert.ThrowsAsync<ArgumentException>(async () => await table.UpdateRowsAsync(updates));
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }
}
