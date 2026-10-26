// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A transaction's change-data fill-in (#530) beside a schema change staged in the same transaction (#534). An
/// append staged beside the change was written under the NEW schema, so its <c>insert</c> rows must be read
/// under it too: read under the base, a column the transaction added came out NULL on the feed while the table
/// held its value.
/// </summary>
public class CdfFillInSchemaChangeTests : IDisposable
{
    private readonly string _tempDir;

    public CdfFillInSchemaChangeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cdffillschema_{Guid.NewGuid():N}");
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

    private static Apache.Arrow.Schema ArrowSchema(params (string Name, IArrowType Type)[] columns)
    {
        var builder = new Apache.Arrow.Schema.Builder().Field(new Field("id", Int64Type.Default, false));
        foreach (var (name, type) in columns)
            builder.Field(new Field(name, type, true));
        return builder.Build();
    }

    private static readonly Apache.Arrow.Schema BaseSchema = ArrowSchema(("value", StringType.Default));

    private static readonly Apache.Arrow.Schema WithExtra =
        ArrowSchema(("value", StringType.Default), ("extra", Int32Type.Default));

    /// <summary>One row per <paramref name="rows"/> entry, its values in <paramref name="schema"/>'s order.</summary>
    private static RecordBatch Rows(Apache.Arrow.Schema schema, params object?[][] rows)
    {
        var columns = new List<IArrowArray>();
        for (int c = 0; c < schema.FieldsList.Count; c++)
        {
            switch (schema.FieldsList[c].DataType)
            {
                case Int64Type:
                    var longs = new Int64Array.Builder();
                    foreach (var r in rows)
                        longs.Append((long)r[c]!);
                    columns.Add(longs.Build());
                    break;
                case Int32Type:
                    var ints = new Int32Array.Builder();
                    foreach (var r in rows)
                        ints.Append((int)r[c]!);
                    columns.Add(ints.Build());
                    break;
                default:
                    var strings = new StringArray.Builder();
                    foreach (var r in rows)
                        strings.Append((string)r[c]!);
                    columns.Add(strings.Build());
                    break;
            }
        }
        return new RecordBatch(schema, columns, rows.Length);
    }

    /// <summary>A Change Data Feed table holding rows 1-3 in one file.</summary>
    private async Task<DeltaTable> CreateAsync(
        ColumnMappingMode mode = ColumnMappingMode.None, bool rowTracking = false)
    {
        var table = await DeltaTable.CreateAsync(
            Fs, BaseSchema, enableDeletionVectors: true, enableRowTracking: rowTracking, columnMappingMode: mode,
            configuration: new Dictionary<string, string> { ["delta.enableChangeDataFeed"] = "true" });
        await table.WriteAsync([Rows(BaseSchema, [1L, "a"], [2L, "b"], [3L, "c"])]);
        return table;
    }

    /// <summary>The row at <paramref name="position"/> of the table's only file.</summary>
    private static RowSelection At(DeltaTable table, long position) =>
        RowSelection.ByPath(new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal)
        {
            [table.CurrentSnapshot.ActiveFiles.Keys.Single()] = new HashSet<long> { position },
        });

    /// <summary>Stages an ADD COLUMN <c>extra INT</c> and an append of row 6 written under it.</summary>
    private static async Task StageAddColumnAndAppendAsync(DeltaTable table, DeltaTransaction txn)
    {
        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));
        txn.StageSchemaChange(change);
        txn.StageDataFiles(await table.WriteDataFilesAsync(
            [Rows(WithExtra, [6L, "f", 60])], schemaOverride: change.NewSchema));
    }

    /// <summary>Every row of the version's feed as <c>"type col=value ..."</c>, data columns only, sorted.</summary>
    private async Task<List<string>> FeedAsync(long version)
    {
        await using var check = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var lines = new List<string>();
        await foreach (var b in check.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            var types = (StringArray)b.Column("_change_type");
            for (int i = 0; i < b.Length; i++)
            {
                var parts = new List<string> { types.GetString(i) };
                for (int c = 0; c < b.ColumnCount; c++)
                {
                    string name = b.Schema.FieldsList[c].Name;
                    if (name.StartsWith("_", StringComparison.Ordinal))
                        continue;
                    var column = b.Column(c);
                    string value = column.IsNull(i) ? "NULL" : column switch
                    {
                        Int64Array a => a.GetValue(i)!.Value.ToString(),
                        Int32Array a => a.GetValue(i)!.Value.ToString(),
                        StringArray a => a.GetString(i),
                        _ => throw new NotSupportedException(column.GetType().Name),
                    };
                    parts.Add($"{name}={value}");
                }
                lines.Add(string.Join(" ", parts));
            }
        }
        lines.Sort(StringComparer.Ordinal);
        return lines;
    }

    // ── The gap the issue names ──────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(ColumnMappingMode.None)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task AnAppendUnderAnAddedColumn_BesideACopyOnWriteDelete_KeepsTheColumnOnTheFeed(
        ColumnMappingMode mode)
    {
        await using var table = await CreateAsync(mode);

        var txn = table.StartTransaction();
        await StageAddColumnAndAppendAsync(table, txn);
        await txn.StageRowDeletesAsync(At(table, 1), RowDeleteMode.CopyOnWrite); // writes a change file
        long version = await txn.CommitAsync();

        // Was: insert id=6 … extra=NULL. The deleted row never had the column, so its NULL is right.
        Assert.Equal(
            ["delete id=2 value=b extra=NULL", "insert id=6 value=f extra=60"],
            await FeedAsync(version));
    }

    [Theory]
    [InlineData(ColumnMappingMode.None)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task AnAppendUnderAnAddedColumn_BesideHostStagedChangeRows_KeepsTheColumnOnTheFeed(
        ColumnMappingMode mode)
    {
        await using var table = await CreateAsync(mode);

        var txn = table.StartTransaction();
        await StageAddColumnAndAppendAsync(table, txn);
        await txn.StageRowDeletesAsync(At(table, 2)); // a vector delete: filled in under the new schema too
        await txn.StageChangeDataAsync(Rows(BaseSchema, [9L, "z"]), "delete");
        long version = await txn.CommitAsync();

        Assert.Equal(
            ["delete id=3 value=c extra=NULL", "delete id=9 value=z extra=NULL", "insert id=6 value=f extra=60"],
            await FeedAsync(version));
    }

    /// <summary>A rebase on a row-tracking table writes the fill-in's insert files again
    /// (<see cref="DeltaTable.InsertChangeDataFillIn"/>), and must do so under the same schema.</summary>
    [Fact]
    public async Task AnAppendUnderAnAddedColumn_KeepsTheColumnThroughARowTrackingRebase()
    {
        await using var table = await CreateAsync(rowTracking: true);

        var txn = table.StartTransaction();
        await StageAddColumnAndAppendAsync(table, txn);
        await txn.StageRowDeletesAsync(At(table, 1), RowDeleteMode.CopyOnWrite);

        // Takes the ids the transaction reserved for its append, so the commit rewrites its insert files.
        await using (var racer = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
            await racer.WriteAsync([Rows(BaseSchema, [100L, "x"])]);

        long version = await txn.CommitAsync();

        Assert.Equal(
            ["delete id=2 value=b extra=NULL", "insert id=6 value=f extra=60"],
            await FeedAsync(version));
    }

    // ── The other schema changes, which the base schema already read correctly ──────────────────────────

    [Fact]
    public async Task AnAppendUnderARenamedColumn_BesideACopyOnWriteDelete_ReportsTheNewName()
    {
        await using var table = await CreateAsync(ColumnMappingMode.Name);

        var txn = table.StartTransaction();
        var change = table.ComputeRenameColumn("value", "label");
        txn.StageSchemaChange(change);
        txn.StageDataFiles(await table.WriteDataFilesAsync(
            [Rows(ArrowSchema(("label", StringType.Default)), [6L, "f"])], schemaOverride: change.NewSchema));
        await txn.StageRowDeletesAsync(At(table, 1), RowDeleteMode.CopyOnWrite);
        long version = await txn.CommitAsync();

        Assert.Equal(["delete id=2 label=b", "insert id=6 label=f"], await FeedAsync(version));
    }

    [Fact]
    public async Task AnAppendAfterADroppedColumn_BesideACopyOnWriteDelete_ReportsNeither()
    {
        await using var table = await CreateAsync(ColumnMappingMode.Name);

        var txn = table.StartTransaction();
        var change = table.ComputeDropColumn("value");
        txn.StageSchemaChange(change);
        txn.StageDataFiles(await table.WriteDataFilesAsync(
            [Rows(ArrowSchema(), [6L])], schemaOverride: change.NewSchema));
        await txn.StageRowDeletesAsync(At(table, 1), RowDeleteMode.CopyOnWrite);
        long version = await txn.CommitAsync();

        Assert.Equal(["delete id=2", "insert id=6"], await FeedAsync(version));
    }

    /// <summary>No Compute* method widens a type, so the widened <c>metaData</c> is staged raw. The delete's
    /// own change file is still written under the narrow base schema and widens on read.</summary>
    [Fact]
    public async Task AnAppendUnderAWidenedColumn_BesideACopyOnWriteDelete_KeepsTheWideValue()
    {
        var narrow = ArrowSchema(("n", Int32Type.Default));
        await using var table = await DeltaTable.CreateAsync(
            Fs, narrow, enableDeletionVectors: true,
            configuration: new Dictionary<string, string>
            {
                ["delta.enableChangeDataFeed"] = "true",
                [Schema.TypeWidening.EnableKey] = "true",
            });
        await table.WriteAsync([Rows(narrow, [1L, 10], [2L, 20])]);

        var widened = new Schema.StructType
        {
            Fields =
            [
                table.CurrentSnapshot.Schema.Fields[0],
                new StructField
                {
                    Name = "n",
                    Type = new PrimitiveType { TypeName = "long" },
                    Nullable = true,
                    Metadata = new Dictionary<string, string>
                    {
                        [Schema.TypeWidening.TypeChangesKey] = """[{"fromType":"integer","toType":"long"}]""",
                    },
                },
            ],
        };

        var txn = table.StartTransaction();
        txn.StageActions([table.CurrentSnapshot.Metadata with
        {
            SchemaString = DeltaSchemaSerializer.Serialize(widened),
        }]);
        txn.StageDataFiles(await table.WriteDataFilesAsync(
            [Rows(ArrowSchema(("n", Int64Type.Default)), [6L, 5_000_000_000L])], schemaOverride: widened));
        await txn.StageRowDeletesAsync(At(table, 1), RowDeleteMode.CopyOnWrite);
        long version = await txn.CommitAsync();

        Assert.Equal(["delete id=2 n=20", "insert id=6 n=5000000000"], await FeedAsync(version));
    }
}
