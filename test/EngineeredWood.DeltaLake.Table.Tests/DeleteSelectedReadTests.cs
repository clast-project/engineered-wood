// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using X = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A DELETE matches rows through the Parquet selected read (#502 phase 3c): the predicate is the read's row filter,
/// the file's existing deletion vector its excluded rows. A row the vector already deleted is therefore never handed
/// to the predicate, as Spark filters those rows first; before, the predicate saw every row of the file and the
/// vector was applied to its answer, so a value error on a deleted row failed a delete Spark lets through.
/// </summary>
public class DeleteSelectedReadTests : IDisposable
{
    private readonly string _tempDir;

    public DeleteSelectedReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_delsel_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("s", StringType.Default, true))
        .Field(new Field("id", Int64Type.Default, true))
        .Build();

    /// <summary>
    /// One file of four rows: <c>s</c> = '5', 'abc', '7', '5' for ids 1-4, then id 2 (the 'abc' row) deleted with a
    /// deletion vector. Under ANSI, <c>s = 5</c> raises CAST_INVALID_INPUT on 'abc' and on nothing else.
    /// </summary>
    private async Task<DeltaTable> CreateAsync(bool changeDataFeed = false, bool rowTracking = false)
    {
        var configuration = changeDataFeed
            ? new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" }
            : null;
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, Guid.NewGuid().ToString("N"))), Schema,
            enableDeletionVectors: true, enableRowTracking: rowTracking, configuration: configuration);
        await table.WriteAsync([new RecordBatch(Schema,
            [new StringArray.Builder().Append("5").Append("abc").Append("7").Append("5").Build(),
             new Int64Array.Builder().Append(1).Append(2).Append(3).Append(4).Build()], 4)]);
        var (gone, _) = await table.DeleteAsync(X.Equal("id", LiteralValue.Of(2L)));
        Assert.Equal(1, gone);
        Assert.NotNull(Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).DeletionVector);
        return table;
    }

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var b in table.ReadAsync(new DeltaReadOptions { Columns = ["id"] }))
        {
            using (b)
            {
                var id = (Int64Array)b.Column(0);
                for (int i = 0; i < b.Length; i++)
                    ids.Add(id.GetValue(i)!.Value);
            }
        }

        ids.Sort();
        return ids;
    }

    [Theory]
    [InlineData(null)]
    [InlineData(RowDeleteMode.DeletionVector)]
    [InlineData(RowDeleteMode.CopyOnWrite)]
    public async Task AnExpressionDeleteNeverEvaluatesARowTheVectorDeleted(RowDeleteMode? mode)
    {
        await using var table = await CreateAsync();
        var predicate = X.Equal("s", LiteralValue.Of(5));
        var (deleted, _) = mode is RowDeleteMode m
            ? await table.DeleteAsync(predicate, m)
            : await table.DeleteAsync(predicate);

        Assert.Equal(2, deleted);
        Assert.Equal([3L], await IdsAsync(table));
    }

    [Fact]
    public async Task AFunctionDeleteIsNeverHandedARowTheVectorDeleted()
    {
        await using var table = await CreateAsync();
        var seen = new List<long>();
        var (deleted, _) = await table.DeleteAsync(batch =>
        {
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var mask = new BooleanArray.Builder();
            for (int i = 0; i < batch.Length; i++)
            {
                seen.Add(id.GetValue(i)!.Value);
                mask.Append(id.GetValue(i) == 3);
            }

            return mask.Build();
        });

        Assert.Equal(1, deleted);
        Assert.DoesNotContain(2L, seen);
        Assert.Equal([1L, 4L], await IdsAsync(table));
    }

    /// <summary>With a change feed the matched rows are read whole, and only they go to the change file.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WithAChangeFeedTheChangeFileHoldsExactlyTheDeletedRows(bool rowTracking)
    {
        await using var table = await CreateAsync(changeDataFeed: true, rowTracking: rowTracking);
        long version = (await table.DeleteAsync(X.Equal("s", LiteralValue.Of(5)))).Version;

        var changes = new List<string>();
        var options = new DeltaChangeReadOptions
        {
            StartVersion = version,
            EndVersion = version,
            Metadata = rowTracking ? DeltaRowMetadata.RowTracking : DeltaRowMetadata.None,
        };
        await foreach (var b in table.ReadChangesAsync(options))
        {
            using (b)
            {
                var s = (StringArray)b.Column(b.Schema.GetFieldIndex("s"));
                var id = (Int64Array)b.Column(b.Schema.GetFieldIndex("id"));
                for (int i = 0; i < b.Length; i++)
                {
                    string row = $"{s.GetString(i)}/{id.GetValue(i)}";
                    if (rowTracking)
                    {
                        var rowId = (Int64Array)b.Column(b.Schema.GetFieldIndex(RowTrackingConfig.RowIdColumnName));
                        row += $"/rowid={rowId.GetValue(i)}";
                    }
                    changes.Add(row);
                }
            }
        }

        changes.Sort(StringComparer.Ordinal);
        // Row ids follow positions: baseRowId 0, so id n sits at position n - 1.
        Assert.Equal(rowTracking ? ["5/1/rowid=0", "5/4/rowid=3"] : ["5/1", "5/4"], changes);
    }

    /// <summary>
    /// A predicate over no column the file holds (here one added after it was written) is answered once for the file,
    /// and deletes every live row of it, and only those: the rows the vector already deleted are not counted again.
    /// </summary>
    [Fact]
    public async Task APredicateOverNoColumnOfTheFileDeletesItsLiveRows()
    {
        await using var table = await CreateAsync();
        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));
        var withScore = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", StringType.Default, true))
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();
        await table.WriteAsync([new RecordBatch(withScore,
            [new StringArray.Builder().Append("x").Build(), new Int64Array.Builder().Append(9).Build(),
             new DoubleArray.Builder().Append(1.5).Build()], 1)]);

        var (deleted, _) = await table.DeleteAsync(X.IsNull("score"));

        Assert.Equal(3, deleted);
        Assert.Equal([9L], await IdsAsync(table));
    }

    /// <summary>
    /// A file that holds no column the table still has (every data column it was written with since dropped, the one
    /// it has now added later) gives a change-feed DELETE nothing to read for the predicate. Its deleted rows must
    /// still reach the change file, next to those of a newer file the same DELETE matches.
    /// </summary>
    [Fact]
    public async Task WithAChangeFeedAFileHoldingNoCurrentColumnStillRecordsItsDeletedRows()
    {
        var original = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("old", Int64Type.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "evolved")), original,
            partitionColumns: ["region"], columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([new RecordBatch(original,
            [new StringArray.Builder().Append("a").Append("a").Build(), new Int64Array.Builder().Append(1).Append(2).Build()], 2)]);
        await table.AddColumnAsync(new Field("score", DoubleType.Default, true));
        await table.DropColumnAsync("old");
        var current = new Apache.Arrow.Schema.Builder()
            .Field(new Field("region", StringType.Default, true))
            .Field(new Field("score", DoubleType.Default, true))
            .Build();
        await table.WriteAsync([new RecordBatch(current,
            [new StringArray.Builder().Append("a").Append("a").Build(),
             new DoubleArray.Builder().AppendNull().Append(2.5).Build()], 2)]);

        var (deleted, version) = await table.DeleteAsync(X.IsNull("score"));

        Assert.Equal(3, deleted);
        var changes = new List<string>();
        await foreach (var b in table.ReadChangesAsync(new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            using (b)
            {
                var region = (StringArray)b.Column(b.Schema.GetFieldIndex("region"));
                var score = (DoubleArray)b.Column(b.Schema.GetFieldIndex("score"));
                for (int i = 0; i < b.Length; i++)
                    changes.Add($"{region.GetString(i)}/{(score.IsNull(i) ? "null" : score.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture))}");
            }
        }

        Assert.Equal(["a/null", "a/null", "a/null"], changes);
    }

    /// <summary>
    /// A function predicate reads every column of the file, and a file a copy-on-write rewrite produced also holds the
    /// hidden materialized row-tracking columns: the predicate is never handed them, only the table's own.
    /// </summary>
    [Fact]
    public async Task AFunctionDeleteIsNeverHandedTheMaterializedRowTrackingColumns()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(Path.Combine(_tempDir, "rt")), Schema, enableRowTracking: true);
        await using (table)
        {
            await table.WriteAsync([new RecordBatch(Schema,
                [new StringArray.Builder().Append("a").Append("b").Append("c").Build(),
                 new Int64Array.Builder().Append(1).Append(2).Append(3).Build()], 3)]);

            // A copy-on-write UPDATE rewrites the file, materializing each row's id in it.
            await table.UpdateAsync(
                batch =>
                {
                    var mask = new BooleanArray.Builder();
                    for (int i = 0; i < batch.Length; i++)
                        mask.Append(i == 0);
                    return mask.Build();
                },
                batch => batch);
            var (idName, _) = RowTrackingConfig.TryGetMaterializedColumnNames(table.CurrentSnapshot.Metadata.Configuration);
            Assert.NotNull(idName);
            Assert.True(await AnyFileHoldsAsync(table, idName!), "no file holds the materialized row-id column");

            var tableColumns = table.CurrentSnapshot.ArrowSchema.FieldsList.Select(f => f.Name).ToHashSet();
            var (deleted, _) = await table.DeleteAsync(batch =>
            {
                Assert.All(batch.Schema.FieldsList, f => Assert.Contains(f.Name, tableColumns));
                var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
                var mask = new BooleanArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    mask.Append(id.GetValue(i) == 2);
                return mask.Build();
            });

            Assert.Equal(1, deleted);
            Assert.Equal([1L, 3L], await IdsAsync(table));
        }
    }

    private async Task<bool> AnyFileHoldsAsync(DeltaTable table, string column)
    {
        foreach (var add in table.CurrentSnapshot.ActiveFiles.Values)
        {
            string root = Path.Combine(_tempDir, "rt");
            await using var file = new LocalRandomAccessFile(Path.Combine(root, DeltaPath.Decode(add.Path)));
            using var reader = new ParquetFileReader(file, ownsFile: false);
            var schema = await reader.GetSchemaAsync();
            if (schema.Root.Children.Any(c => c.Name == column))
                return true;
        }

        return false;
    }
}
