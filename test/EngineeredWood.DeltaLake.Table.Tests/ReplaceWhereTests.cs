// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaTable.OverwriteWhereAsync"/>: Spark's <c>replaceWhere</c> on any column (#566). Each case is one
/// measured against Spark 4.0 / Delta 4.0.0 (<see cref="Interop.ReplaceWhereInteropTests"/> keeps the comparison).
/// The tables are three files: <c>a</c> (v 1, 2), <c>b</c> (v 3, 4) and <c>c</c> (v 5, 15), so the window
/// <c>3 &lt;= v &lt; 10</c> takes <c>b</c> whole and <c>c</c> in part.
/// </summary>
public class ReplaceWhereTests : IDisposable
{
    private readonly string _tempDir;

    public ReplaceWhereTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_replacewhere_{Guid.NewGuid():N}");
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
        .Field(new Field("part", StringType.Default, true))
        .Field(new Field("v", Int32Type.Default, true))
        .Build();

    private static readonly Expressions.Predicate Window =
        Ex.And(Ex.GreaterThanOrEqual("v", 3), Ex.LessThan("v", 10));

    private static readonly (long, string?, int?)[] A = [(1, "a", 1), (2, "a", 2)];
    private static readonly (long, string?, int?)[] B = [(3, "b", 3), (4, "b", 4)];
    private static readonly (long, string?, int?)[] C = [(5, "c", 5), (6, "c", 15)];

    private static RecordBatch Rows(params (long Id, string? Part, int? V)[] rows)
    {
        var ids = new Int64Array.Builder();
        var parts = new StringArray.Builder();
        var vs = new Int32Array.Builder();
        foreach (var (id, part, v) in rows)
        {
            ids.Append(id);
            if (part is null) parts.AppendNull(); else parts.Append(part);
            if (v is int value) vs.Append(value); else vs.AppendNull();
        }
        return new RecordBatch(TableSchema, [ids.Build(), parts.Build(), vs.Build()], rows.Length);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    /// <summary>One file per write: a, b, c.</summary>
    private async Task<DeltaTable> CreateAsync(
        bool partitioned = false, bool deletionVectors = false, bool changeDataFeed = false,
        Dictionary<string, string>? configuration = null)
    {
        configuration ??= [];
        if (changeDataFeed)
            configuration["delta.enableChangeDataFeed"] = "true";
        var table = await DeltaTable.CreateAsync(
            Fs, TableSchema, new DeltaTableOptions { CheckpointInterval = 0 },
            partitionColumns: partitioned ? ["part"] : null,
            enableDeletionVectors: deletionVectors, configuration: configuration);
        await table.WriteAsync([Rows(A)]);
        await table.WriteAsync([Rows(B)]);
        await table.WriteAsync([Rows(C)]);
        return table;
    }

    private async Task<List<(long Id, string? Part, int? V)>> ReadAsync()
    {
        await using var table = await DeltaTable.OpenAsync(Fs);
        var rows = new List<(long, string?, int?)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var parts = (StringArray)batch.Column("part");
            var vs = (Int32Array)batch.Column("v");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, parts.GetString(i), vs.GetValue(i)));
        }
        rows.Sort();
        return rows;
    }

    private async Task<List<(long Id, string Type)>> FeedAsync(long version)
    {
        await using var table = await DeltaTable.OpenAsync(Fs);
        var changes = new List<(long, string)>();
        await foreach (var batch in table.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var ids = (Int64Array)batch.Column("id");
            var types = (StringArray)batch.Column("_change_type");
            for (int i = 0; i < batch.Length; i++)
                changes.Add((ids.GetValue(i)!.Value, types.GetString(i)));
        }
        changes.Sort();
        return changes;
    }

    private JsonElement CommitInfo(long version)
    {
        string path = Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json");
        foreach (string line in File.ReadAllLines(path))
        {
            using var doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("commitInfo", out var info))
                return info.Clone();
        }
        throw new InvalidOperationException($"No commitInfo in version {version}.");
    }

    private string[] DataFilesOnDisk() =>
        [.. Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(p => !p.Contains("_delta_log", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)];

    private static async Task<IReadOnlyDictionary<string, string>> MetricsAsync(DeltaTable table, long version)
    {
        await foreach (var entry in table.GetHistoryAsync())
        {
            if (entry.Version == version)
                return entry.OperationMetrics ?? throw new InvalidOperationException("No metrics.");
        }
        throw new InvalidOperationException($"No version {version}.");
    }

    // ── A window over a data column ──

    [Fact]
    public async Task ADataWindow_RemovesTheFileItCovers_RewritesTheOneItCuts_AndAppends_InOneCommit()
    {
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;
        var filesBefore = DataFilesOnDisk();

        long version = await table.OverwriteWhereAsync([Rows((20, "x", 7))], Window);

        Assert.Equal(before + 1, version);
        Assert.Equal([(1, "a", 1), (2, "a", 2), (6, "c", 15), (20, "x", 7)], await ReadAsync());
        // Two new files: the new row's, and c rewritten without row 5. b was removed with no rewrite.
        Assert.Equal(2, DataFilesOnDisk().Except(filesBefore).Count());

        var info = CommitInfo(version);
        Assert.Equal("WRITE", info.GetProperty("operation").GetString());
        Assert.False(info.GetProperty("isBlindAppend").GetBoolean());

        // Spark's numbers for this exact layout (oracle case D1, CDF off).
        var metrics = await MetricsAsync(table, version);
        Assert.Equal("2", metrics["numFiles"]);
        Assert.Equal("2", metrics["numRemovedFiles"]);
        Assert.Equal("3", metrics["numDeletedRows"]);
        Assert.Equal("1", metrics["numCopiedRows"]);
        Assert.Equal("2", metrics["numOutputRows"]);
        Assert.Equal("0", metrics["numAddedChangeFiles"]);
    }

    [Fact]
    public async Task ARowThePredicateIsNullFor_IsKept()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Rows((7, "c", null))]);

        await table.OverwriteWhereAsync([Rows((20, "x", 7))], Window);

        Assert.Equal([(1, "a", 1), (2, "a", 2), (6, "c", 15), (7, "c", null), (20, "x", 7)], await ReadAsync());
    }

    [Fact]
    public async Task NoNewRows_DeletesTheWindow()
    {
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;

        long version = await table.OverwriteWhereAsync([], Window);

        Assert.Equal(before + 1, version);
        Assert.Equal([(1, "a", 1), (2, "a", 2), (6, "c", 15)], await ReadAsync());
    }

    [Fact]
    public async Task AWindowThatMatchesNothing_OnlyAppends()
    {
        await using var table = await CreateAsync();

        long version = await table.OverwriteWhereAsync([Rows((20, "x", 100))], Ex.GreaterThanOrEqual("v", 100));

        Assert.Equal(
            [(1, "a", 1), (2, "a", 2), (3, "b", 3), (4, "b", 4), (5, "c", 5), (6, "c", 15), (20, "x", 100)],
            await ReadAsync());
        var info = CommitInfo(version);
        Assert.Equal("WRITE", info.GetProperty("operation").GetString());
        Assert.False(info.GetProperty("isBlindAppend").GetBoolean());
    }

    // ── The new rows must lie inside the window ──

    public static TheoryData<string, (long, string?, int?)[]> RowsOutside => new()
    {
        { "false for one row", [(20, "x", 7), (21, "x", 50)] },
        { "null for a row", [(20, "x", null)] },
    };

    [Theory]
    [MemberData(nameof(RowsOutside))]
    public async Task ANewRowOutsideTheWindow_IsRefused_AndNothingIsWritten(
        string because, (long, string?, int?)[] rows)
    {
        _ = because;
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;
        var filesBefore = DataFilesOnDisk();

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([Rows(rows)], Window).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
        await using var check = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(before, check.CurrentSnapshot.Version);
        Assert.Equal(filesBefore, DataFilesOnDisk());
    }

    [Fact]
    public async Task OnATransaction_ARowOutsideTheWindow_StagesNothing()
    {
        await using var table = await CreateAsync();
        await using var transaction = table.StartTransaction();

        await Assert.ThrowsAsync<DeltaFormatException>(
            () => transaction.OverwriteWhereAsync([Rows((21, "x", 50))], Window).AsTask());
        long version = await transaction.CommitAsync();

        Assert.Equal(table.CurrentSnapshot.Version, version); // nothing staged, nothing committed
    }

    // ── A window over partition columns ──

    [Fact]
    public async Task APartitionWindow_ReplacesThePartition()
    {
        await using var table = await CreateAsync(partitioned: true);
        var bFile = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.PartitionValues["part"] == "b").Path;

        long version = await table.OverwriteWhereAsync([Rows((10, "b", 10))], Ex.Equal("part", "b"));

        Assert.Equal([(1, "a", 1), (2, "a", 2), (5, "c", 5), (6, "c", 15), (10, "b", 10)], await ReadAsync());
        await using var check = await DeltaTable.OpenAsync(Fs);
        Assert.DoesNotContain(bFile, check.CurrentSnapshot.ActiveFiles.Keys);
        var metrics = await MetricsAsync(check, version);
        Assert.Equal("1", metrics["numRemovedFiles"]);
        Assert.Equal("2", metrics["numDeletedRows"]);
        Assert.Equal("0", metrics["numCopiedRows"]);
    }

    public static TheoryData<string, (long, string?, int?)[]> RowsOutsidePartition => new()
    {
        { "another partition", [(10, "b", 10), (11, "c", 11)] },
        { "a NULL partition", [(10, null, 10)] },
    };

    [Theory]
    [MemberData(nameof(RowsOutsidePartition))]
    public async Task APartitionWindow_RefusesARowOutsideIt(string because, (long, string?, int?)[] rows)
    {
        _ = because;
        await using var table = await CreateAsync(partitioned: true);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([Rows(rows)], Ex.Equal("part", "b")).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
    }

    [Fact]
    public async Task AMixedWindow_RewritesWithinThePartition()
    {
        await using var table = await CreateAsync(partitioned: true);

        await table.OverwriteWhereAsync(
            [Rows((30, "c", 20))], Ex.And(Ex.Equal("part", "c"), Ex.GreaterThan("v", 10)));

        Assert.Equal(
            [(1, "a", 1), (2, "a", 2), (3, "b", 3), (4, "b", 4), (5, "c", 5), (30, "c", 20)], await ReadAsync());
    }

    [Fact]
    public async Task AMixedWindow_RefusesARowFailingItsDataHalf()
    {
        await using var table = await CreateAsync(partitioned: true);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.OverwriteWhereAsync(
            [Rows((30, "c", 1))], Ex.And(Ex.Equal("part", "c"), Ex.GreaterThan("v", 10))).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
    }

    // ── Change data feed, deletion vectors ──

    [Fact]
    public async Task TheFeed_RecordsTheReplacedRowsAsDeletes_AndTheNewOnesAsInserts()
    {
        await using var table = await CreateAsync(changeDataFeed: true);

        long version = await table.OverwriteWhereAsync([Rows((20, "x", 7))], Window);

        Assert.Equal([(3, "delete"), (4, "delete"), (5, "delete"), (20, "insert")], await FeedAsync(version));
    }

    [Fact]
    public async Task TheFeed_OfAPartitionWindow()
    {
        await using var table = await CreateAsync(partitioned: true, changeDataFeed: true);

        long version = await table.OverwriteWhereAsync([Rows((10, "b", 10))], Ex.Equal("part", "b"));

        Assert.Equal([(3, "delete"), (4, "delete"), (10, "insert")], await FeedAsync(version));
    }

    [Fact]
    public async Task OnADeletionVectorTable_TheCutFileGetsAVector()
    {
        await using var table = await CreateAsync(deletionVectors: true, changeDataFeed: true);
        string cFile = table.CurrentSnapshot.ActiveFiles.Values.Single(f => f.GetNumRecords() == 2
            && f.Stats!.Contains("\"v\":15", StringComparison.Ordinal)).Path;

        long version = await table.OverwriteWhereAsync([Rows((20, "x", 7))], Window);

        Assert.Equal([(1, "a", 1), (2, "a", 2), (6, "c", 15), (20, "x", 7)], await ReadAsync());
        await using var check = await DeltaTable.OpenAsync(Fs);
        Assert.NotNull(check.CurrentSnapshot.ActiveFiles.Values.Single(f => f.Path == cFile).DeletionVector);
        Assert.Equal([(3, "delete"), (4, "delete"), (5, "delete"), (20, "insert")], await FeedAsync(version));
    }

    // ── Refusals ──

    [Fact]
    public async Task AnAppendOnlyTable_IsRefused_EvenWhenNothingMatches()
    {
        await using var table = await CreateAsync(
            configuration: new Dictionary<string, string> { ["delta.appendOnly"] = "true" });

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(() => table.OverwriteWhereAsync(
            [Rows((20, "x", 100))], Ex.GreaterThanOrEqual("v", 100)).AsTask());

        Assert.Equal(DeltaTableErrorCodes.CannotModifyAppendOnly, ex.ErrorCode);
    }

    [Fact]
    public async Task AnUnknownColumn_IsRefused_EvenOnAnEmptyTable()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([], Ex.Equal("nope", 1)).AsTask());

        Assert.Equal(DeltaTableErrorCodes.UnresolvedColumn, ex.ErrorCode);
    }

    [Fact]
    public async Task ABoundReferenceToAnUnknownColumn_IsRefused_EvenOnAnEmptyTable()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema);
        var predicate = new Expressions.ComparisonPredicate(
            new Expressions.BoundReference(99, "nope"), Expressions.ComparisonOperator.Equal,
            new Expressions.LiteralExpression(1));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([], predicate).AsTask());

        Assert.Equal(DeltaTableErrorCodes.UnresolvedColumn, ex.ErrorCode);
    }

    // ── The rows are judged as they will be stored ──

    /// <summary>A write may leave out a nullable column, which reads back as NULL; the window sees it so.</summary>
    [Fact]
    public async Task AColumnLeftOut_IsJudgedAsTheNullItReadsBackAs()
    {
        await using var table = await CreateAsync();
        var noV = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false)).Field(new Field("part", StringType.Default, true)).Build();
        var batch = new RecordBatch(
            noV, [new Int64Array.Builder().Append(20).Build(), new StringArray.Builder().Append("x").Build()], 1);

        await table.OverwriteWhereAsync([batch], Ex.IsNull("v"));
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([batch], Window).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
        Assert.Contains((20L, "x", (int?)null), await ReadAsync());
    }

    private static readonly Apache.Arrow.Schema ValueOnly = new Apache.Arrow.Schema.Builder()
        .Field(new Field("value", StringType.Default, true)).Build();

    /// <summary>A table whose <c>id</c> is GENERATED ALWAYS AS IDENTITY (start 1, step 1), holding id 1.</summary>
    private async Task<DeltaTable> CreateIdentityTableAsync()
    {
        string idMeta = JsonSerializer.Serialize(IdentityColumn.CreateMetadata(1, 1, allowExplicitInsert: false));
        string schema = $@"{{""type"":""struct"",""fields"":[{{""name"":""id"",""type"":""long"",""nullable"":false,""metadata"":{idMeta}}},{{""name"":""value"",""type"":""string"",""nullable"":true,""metadata"":{{}}}}]}}";
        await new TransactionLog(Fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] },
            new MetadataAction { Id = "identity", Format = Format.Parquet, SchemaString = schema, PartitionColumns = [] },
        });
        var table = await DeltaTable.OpenAsync(Fs);
        await table.WriteAsync([Values("seed")]);
        return table;
    }

    private static RecordBatch Values(string value) =>
        new(ValueOnly, [new StringArray.Builder().Append(value).Build()], 1);

    /// <summary>Spark judges the identity value it generates (measured: a window on the identity column accepts
    /// rows that leave it out when the generated value falls inside, and refuses them when it falls outside).</summary>
    [Fact]
    public async Task AGeneratedIdentityValue_IsWhatTheWindowJudges()
    {
        await using var table = await CreateIdentityTableAsync();

        await table.OverwriteWhereAsync([Values("inside")], Ex.GreaterThanOrEqual("id", 1L)); // generates id 2
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([Values("outside")], Ex.Equal("id", 100L)).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
    }

    /// <summary>A value supplied for a GENERATED ALWAYS column is replaced by a generated one, so a window that the
    /// supplied value satisfies must not let the row in: it would be stored outside the window.</summary>
    [Fact]
    public async Task ASuppliedValueAGeneratedAlwaysIdentityReplaces_CannotSlipIntoTheWindow()
    {
        await using var table = await CreateIdentityTableAsync();
        var withId = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false)).Field(new Field("value", StringType.Default, true)).Build();
        var batch = new RecordBatch(
            withId, [new Int64Array.Builder().Append(0).Build(), new StringArray.Builder().Append("a").Build()], 1);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            () => table.OverwriteWhereAsync([batch], Ex.Equal("id", 0L)).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ReplaceWhereMismatch, ex.ErrorCode);
    }

    // ── Transactions and concurrency ──

    [Fact]
    public async Task OnATransaction_ItComposesWithOtherWork_AndRecordsAWriteWithoutMetrics()
    {
        await using var table = await CreateAsync();
        await using var transaction = table.StartTransaction();

        var (deleted, written) = await transaction.OverwriteWhereAsync([Rows((20, "x", 7))], Window);
        await transaction.DeleteAsync(Ex.Equal("id", 1L));
        long version = await transaction.CommitAsync();

        Assert.Equal((3L, 1L), (deleted, written));
        Assert.Equal([(2, "a", 2), (6, "c", 15), (20, "x", 7)], await ReadAsync());
        Assert.Equal("WRITE", CommitInfo(version).GetProperty("operation").GetString());
        // Two operations' metrics would describe neither.
        Assert.False(CommitInfo(version).TryGetProperty("operationMetrics", out _));
    }

    [Fact]
    public async Task OnATransaction_TouchingAFileAnEarlierOperationRewrote_IsRefused()
    {
        await using var table = await CreateAsync();
        await using var transaction = table.StartTransaction();
        await transaction.DeleteAsync(Ex.Equal("id", 5L));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => transaction.OverwriteWhereAsync([Rows((20, "x", 7))], Window).AsTask());
    }

    /// <summary>The predicate is the read set: under Serializable a concurrent append of a row in the window is a
    /// conflict, since the replacement never saw it.</summary>
    [Fact]
    public async Task AConcurrentAppendIntoTheWindow_Conflicts_UnderSerializable()
    {
        await using var table = await CreateAsync(
            configuration: new Dictionary<string, string> { ["delta.isolationLevel"] = "Serializable" });
        await using var transaction = table.StartTransaction();
        await transaction.OverwriteWhereAsync([Rows((20, "x", 7))], Window);

        await using (var other = await DeltaTable.OpenAsync(Fs))
            await other.WriteAsync([Rows((30, "y", 8))]);

        await Assert.ThrowsAnyAsync<DeltaConflictException>(() => transaction.CommitAsync().AsTask());
    }

    [Fact]
    public async Task AConcurrentAppendOutsideTheWindow_DoesNotConflict()
    {
        await using var table = await CreateAsync(
            configuration: new Dictionary<string, string> { ["delta.isolationLevel"] = "Serializable" });
        await using var transaction = table.StartTransaction();
        await transaction.OverwriteWhereAsync([Rows((20, "x", 7))], Window);

        await using (var other = await DeltaTable.OpenAsync(Fs))
            await other.WriteAsync([Rows((30, "y", 80))]);

        await transaction.CommitAsync();
        Assert.Equal([(1, "a", 1), (2, "a", 2), (6, "c", 15), (20, "x", 7), (30, "y", 80)], await ReadAsync());
    }

    [Fact]
    public async Task WithRowTracking_SurvivorsKeepTheirRowIds_AndEveryIdIsDistinct()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, TableSchema, new DeltaTableOptions { CheckpointInterval = 0 }, enableRowTracking: true);
        await table.WriteAsync([Rows(C)]);
        var idsBefore = await RowIdsAsync();

        await table.OverwriteWhereAsync([Rows((20, "x", 7), (21, "x", 8))], Window);

        var idsAfter = await RowIdsAsync();
        Assert.Equal(idsBefore[6], idsAfter[6]);
        Assert.Equal(idsAfter.Count, idsAfter.Values.Distinct().Count());
        Assert.Equal([6L, 20L, 21L], idsAfter.Keys.OrderBy(k => k));
    }

    private async Task<Dictionary<long, long>> RowIdsAsync()
    {
        await using var reader = await DeltaTable.OpenAsync(Fs);
        var byId = new Dictionary<long, long>();
        await foreach (var batch in reader.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var id = (Int64Array)batch.Column("id");
            var rowId = (Int64Array)batch.Column(
                batch.Schema.GetFieldIndex(EngineeredWood.DeltaLake.RowTracking.RowTrackingConfig.RowIdColumnName));
            for (int i = 0; i < batch.Length; i++)
                byId[id.GetValue(i)!.Value] = rowId.GetValue(i)!.Value;
        }
        return byId;
    }
}
