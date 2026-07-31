// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The one read surface: <see cref="DeltaReadOptions"/> + <see cref="DeltaTable.ReadAsync"/>, and the three
/// things the method-per-combination shape it replaces could not do — request TWO metadata kinds in one pass,
/// rename the emitted metadata columns, and state the output schema without reading.
/// </summary>
public class ReadOptionsTests : IDisposable
{
    private readonly string _tempDir;

    public ReadOptionsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_readopts_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count)
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append("v" + (startId + i));
        }
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    private const string PathCol = DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.FilePathSuffix;
    private const string IndexCol = DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix;
    private const string IdCol = DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix;
    private const string VerCol =
        DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowCommitVersionSuffix;

    private static List<string> NamesOf(RecordBatch batch) =>
        batch.Schema.FieldsList.Select(f => f.Name).ToList();

    // ── two metadata kinds in one pass ──

    /// <summary>
    /// The capability the cross product could not express at all: a host wanting a mutation key AND a stable
    /// identity had to read the table twice, because they were separate private iterators. Asking for all
    /// three now costs one pass — and, more importantly, the three agree row-for-row, which two reads could
    /// not have guaranteed across a concurrent commit.
    /// </summary>
    [Fact]
    public async Task AllThreeMetadataKinds_ComeBackFromOnePass_AndAgree()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]);
        await table.WriteAsync([Batch(11, 3)]); // a second file, so ordinals and paths both vary

        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();

        int rows = 0;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions
        {
            Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator
                       | DeltaRowMetadata.RowTracking,
        }))
        {
            Assert.Equal(
                ["id", "value", TransientRowAddress.ColumnName, PathCol, IndexCol, IdCol, VerCol],
                NamesOf(batch));

            var address = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            var path = (StringArray)batch.Column(PathCol);
            var index = (Int64Array)batch.Column(IndexCol);
            var rowId = (Int64Array)batch.Column(IdCol);

            for (int i = 0; i < batch.Length; i++, rows++)
            {
                long packed = address.GetValue(i)!.Value;
                // The locator is the SAME address, unpacked: the ordinal names the path it resolves to, and
                // the row index is the position the packed value carries.
                Assert.Equal(ordered[TransientRowAddress.FileOrdinal(packed)], path.GetString(i));
                Assert.Equal(TransientRowAddress.Position(packed), index.GetValue(i)!.Value);
                // Row tracking is a different number entirely, and present.
                Assert.False(rowId.IsNull(i));
            }
        }
        Assert.Equal(6, rows);
    }

    [Fact]
    public async Task Metadata_None_EmitsExactlyTheTableSchema()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);

        await foreach (var batch in table.ReadAsync())
            Assert.Equal(["id", "value"], NamesOf(batch));
    }

    // ── GetReadSchema ──

    /// <summary>
    /// <see cref="DeltaTable.GetReadSchema"/> promises the schema <see cref="DeltaTable.ReadAsync"/> will
    /// emit. Both build from one list of metadata fields so they cannot drift, and this walks the
    /// combinations that would catch it if they did — a host advertises this at bind time, before a single
    /// batch exists.
    /// </summary>
    [Theory]
    [InlineData(DeltaRowMetadata.None)]
    [InlineData(DeltaRowMetadata.RowAddress)]
    [InlineData(DeltaRowMetadata.Locator)]
    [InlineData(DeltaRowMetadata.RowTracking)]
    [InlineData(DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator)]
    [InlineData(DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking)]
    [InlineData(DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking)]
    public async Task GetReadSchema_MatchesWhatReadAsyncEmits(DeltaRowMetadata metadata)
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 3)]);

        var options = new DeltaReadOptions { Metadata = metadata };
        var promised = table.GetReadSchema(options);

        int batches = 0;
        await foreach (var batch in table.ReadAsync(options))
        {
            batches++;
            Assert.Equal(FieldNames(promised.FieldsList), NamesOf(batch));
            for (int i = 0; i < promised.FieldsList.Count; i++)
            {
                Assert.Equal(promised.FieldsList[i].DataType.TypeId, batch.Schema.FieldsList[i].DataType.TypeId);
                Assert.Equal(promised.FieldsList[i].IsNullable, batch.Schema.FieldsList[i].IsNullable);
            }
        }
        Assert.True(batches > 0);

        static List<string> FieldNames(IReadOnlyList<Field> fields) => fields.Select(f => f.Name).ToList();
    }

    [Fact]
    public async Task GetReadSchema_HonorsAProjection_InTableFieldOrder()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);

        // Named in reverse; the emitted order follows the TABLE, and GetReadSchema says so.
        var options = new DeltaReadOptions
        {
            Columns = ["value", "id"],
            Metadata = DeltaRowMetadata.RowAddress,
        };
        Assert.Equal(
            ["id", "value", TransientRowAddress.ColumnName],
            table.GetReadSchema(options).FieldsList.Select(f => f.Name));

        await foreach (var batch in table.ReadAsync(options))
            Assert.Equal(["id", "value", TransientRowAddress.ColumnName], NamesOf(batch));
    }

    /// <summary>
    /// The column order must not depend on which FILE a batch came from. The reconciliation that backfills a
    /// column added after a file was written emits in the table's field order, but its no-op fast path used
    /// to return the source batch verbatim — so a projection came back in the CALLER's order from a file that
    /// already matched the schema and in the TABLE's order from one that needed the backfill. Two files of
    /// one table disagreed, which is not a shape a host can bind to and not a schema GetReadSchema could
    /// promise. Caught by GetReadSchema, which is the point of having it.
    /// </summary>
    [Fact]
    public async Task ProjectionOrder_IsTheSame_ForFilesOnBothSidesOfAnAddColumn()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);                       // file A: predates the new column
        await table.AddColumnAsync(new Field("extra", Int32Type.Default, true));

        var withExtra = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        await table.WriteAsync([new RecordBatch(withExtra,
        [
            new Int64Array.Builder().Append(10).Build(),
            new StringArray.Builder().Append("v10").Build(),
            new Int32Array.Builder().Append(7).Build(),
        ], 1)]);                                                     // file B: has it

        // Named in an order that matches neither the table nor itself, so a pass-through would show.
        var options = new DeltaReadOptions { Columns = ["extra", "id"] };
        var promised = table.GetReadSchema(options).FieldsList.Select(f => f.Name).ToList();

        int batches = 0;
        await foreach (var batch in table.ReadAsync(options))
        {
            batches++;
            Assert.Equal(promised, NamesOf(batch));
        }
        Assert.Equal(2, batches); // both files contributed, and both agreed
    }

    [Fact]
    public async Task GetReadSchema_MatchesEmission_OnAPartitionedTable()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema, partitionColumns: ["region"]);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var regions = new StringArray.Builder().Append("east").Append("east").Build();
        await table.WriteAsync([new RecordBatch(schema, [ids, regions], 2)]);

        var options = new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator };
        var promised = table.GetReadSchema(options).FieldsList.Select(f => f.Name).ToList();

        await foreach (var batch in table.ReadAsync(options))
            Assert.Equal(promised, NamesOf(batch));
    }

    [Fact]
    public async Task GetReadSchema_WithoutRowTracking_ThrowsForRowTracking()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema()); // no row tracking
        await table.WriteAsync([Batch(1, 2)]);

        Assert.Throws<InvalidOperationException>(() =>
            table.GetReadSchema(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking }));
    }

    // ── MetadataPrefix: the last open row-tracking gap ──

    [Fact]
    public async Task MetadataPrefix_RenamesTheLocatorAndRowTrackingColumns_ButNotTheAddress()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 2)]);

        var options = new DeltaReadOptions
        {
            Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator
                       | DeltaRowMetadata.RowTracking,
            MetadataPrefix = "ew__",
        };

        await foreach (var batch in table.ReadAsync(options))
        {
            Assert.Equal(
                [
                    "id", "value",
                    TransientRowAddress.ColumnName,   // NOT prefixed — it has no Spark counterpart
                    "ew__file_path", "ew__row_index", "ew__row_id", "ew__row_commit_version",
                ],
                NamesOf(batch));
        }
        Assert.Equal(
            FieldNames(table.GetReadSchema(options).FieldsList),
            ["id", "value", TransientRowAddress.ColumnName,
             "ew__file_path", "ew__row_index", "ew__row_id", "ew__row_commit_version"]);

        static List<string> FieldNames(IReadOnlyList<Field> fields) => fields.Select(f => f.Name).ToList();
    }

    /// <summary>
    /// A table whose own column occupies a metadata name is refused rather than shadowed — and the prefix is
    /// what makes that refusal actionable, since there is now somewhere to move the metadata to. Before the
    /// options object there was nowhere, which is why the names were not configurable.
    /// </summary>
    [Fact]
    public async Task CollidingColumnName_Throws_AndThePrefixResolvesIt()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field(IdCol, StringType.Default, true))   // a user column called _metadata.row_id
            .Build();
        await using var table = await DeltaTable.CreateAsync(Fs, schema, enableRowTracking: true);

        var colliding = new DeltaReadOptions { Metadata = DeltaRowMetadata.RowTracking };
        var ex = Assert.Throws<InvalidOperationException>(() => table.GetReadSchema(colliding));
        Assert.Contains(IdCol, ex.Message);
        Assert.Contains("MetadataPrefix", ex.Message);

        // Moved out of the way, the same read is fine.
        var moved = colliding with { MetadataPrefix = "ew__" };
        Assert.Equal(
            ["id", IdCol, "ew__row_id", "ew__row_commit_version"],
            table.GetReadSchema(moved).FieldsList.Select(f => f.Name));
    }

    // ── the loop with the DML ──

    /// <summary>
    /// What <see cref="DeltaRowMetadata.Locator"/> is for: the read emits exactly what
    /// <see cref="RowSelection.FromLocatorColumns"/> consumes, so a host scans, filters the batches with its
    /// own engine, and hands the survivors straight back with no coordinate arithmetic anywhere.
    /// </summary>
    [Fact]
    public async Task LocatorColumns_FeedRowSelectionDirectly()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);

        // "The host's engine" keeps the even ids.
        var doomed = new List<RecordBatch>();
        await foreach (var batch in table.ReadAsync(
            new DeltaReadOptions { Metadata = DeltaRowMetadata.Locator }))
        {
            var id = (Int64Array)batch.Column("id");
            var keep = new List<int>();
            for (int i = 0; i < batch.Length; i++)
                if (id.GetValue(i)!.Value % 2 == 0)
                    keep.Add(i);
            if (keep.Count > 0)
                doomed.Add(EngineeredWood.Arrow.ArrowCompute.Take(batch, batch.Schema, keep));
        }

        var (deleted, _) = await table.DeleteRowsAsync(
            RowSelection.FromLocatorColumns(doomed), RowDeleteMode.CopyOnWrite);
        Assert.Equal(2, deleted);

        await using var check = await DeltaTable.OpenAsync(Fs);
        var remaining = new List<long>();
        await foreach (var batch in check.ReadAllAsync())
        {
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                remaining.Add(id.GetValue(i)!.Value);
        }
        remaining.Sort();
        Assert.Equal(new long[] { 1, 3, 5 }, remaining);
    }

    // ── time travel and filtering through the options object ──

    [Fact]
    public async Task AtVersion_ReadsTheOlderVersion_WithMetadata()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);
        long v1 = table.CurrentSnapshot.Version;
        await table.WriteAsync([Batch(10, 2)]);

        var seen = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions
        {
            AtVersion = v1,
            Metadata = DeltaRowMetadata.RowAddress,
        }))
        {
            Assert.Contains(TransientRowAddress.ColumnName, NamesOf(batch));
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                seen.Add(id.GetValue(i)!.Value);
        }
        seen.Sort();
        Assert.Equal(new long[] { 1, 2 }, seen);
    }

    [Fact]
    public async Task Filter_PrunesFiles_AndStillEmitsMetadata()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 2)]);    // ids 1,2
        await table.WriteAsync([Batch(100, 2)]);  // ids 100,101

        var seen = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions
        {
            Filter = Ex.GreaterThan("id", 50L),
            Metadata = DeltaRowMetadata.Locator,
        }))
        {
            Assert.Contains(PathCol, NamesOf(batch));
            var id = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                seen.Add(id.GetValue(i)!.Value);
        }
        seen.Sort();
        // Superset-safe pruning: the 1,2 file is proven not to match and is skipped entirely.
        Assert.Equal(new long[] { 100, 101 }, seen);
    }

    // ── change feed options ──

    [Fact]
    public async Task ChangeFeed_RejectsTheAddressMetadataKinds()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(),
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Batch(1, 2)]);

        foreach (var bad in new[] { DeltaRowMetadata.Locator, DeltaRowMetadata.RowAddress })
        {
            var ex = Assert.Throws<ArgumentException>(() => table.ReadChangesAsync(
                new DeltaChangeReadOptions { StartVersion = 1, EndVersion = 1, Metadata = bad }));
            Assert.Contains("_change_data", ex.Message);
        }
    }

    [Fact]
    public async Task ChangeFeed_Columns_ProjectsButKeepsTheFeedsOwnColumns()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(),
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Batch(1, 2)]);
        long v = table.CurrentSnapshot.Version;

        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
        {
            StartVersion = v, EndVersion = v, Columns = ["id"],
        }))
        {
            var names = NamesOf(batch);
            Assert.Contains("id", names);
            Assert.DoesNotContain("value", names);
            // The three feed columns survive any projection — they are what makes a change row a change.
            Assert.Contains(DeltaLake.ChangeDataFeed.CdfConfig.ChangeTypeColumn, names);
            Assert.Contains(DeltaLake.ChangeDataFeed.CdfConfig.CommitVersionColumn, names);
            Assert.Contains(DeltaLake.ChangeDataFeed.CdfConfig.CommitTimestampColumn, names);
        }
    }

    [Fact]
    public async Task ChangeFeed_MetadataPrefix_RenamesTheRowTrackingColumns()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableRowTracking: true,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Batch(1, 2)]);
        long v = table.CurrentSnapshot.Version;

        int seen = 0;
        await foreach (var batch in table.ReadChangesAsync(new DeltaChangeReadOptions
        {
            StartVersion = v, EndVersion = v,
            Metadata = DeltaRowMetadata.RowTracking,
            MetadataPrefix = "ew__",
        }))
        {
            seen += batch.Length;
            var names = NamesOf(batch);
            Assert.Contains("ew__row_id", names);
            Assert.Contains("ew__row_commit_version", names);
            Assert.DoesNotContain(IdCol, names);
        }
        Assert.Equal(2, seen);
    }
}
