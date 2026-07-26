// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// ADD COLUMN and DROP COLUMN are metadata-only commits, so a table's data files span VINTAGES: a file
/// written before an ADD lacks that column, one written before a DROP still carries it. Compaction is where
/// those mixed shapes meet — every batch has to be reconciled to the current schema before they can share one
/// output file, and the reconcile has to pair columns by NAME, since the vintages' column sets differ.
///
/// <para>Compaction must be a pure reorganization: the rows a reader sees afterwards must be exactly the rows
/// it saw before, which is what these tests assert. Checking only that <c>CompactAsync</c> does not throw
/// would miss the failure mode that matters — a positional pairing that lands one column's values under
/// another column's name writes WRONG DATA and raises nothing.</para>
/// </summary>
public class CompactionSchemaEvolutionTests : IDisposable
{
    private readonly string _tempDir;

    public CompactionSchemaEvolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_compact_evo_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema SchemaOf(params Field[] fields) =>
        new(fields.ToList(), null);

    private static Field I64(string name) => new(name, Int64Type.Default, true);
    private static Field Str(string name) => new(name, StringType.Default, true);

    // Every row as "col=value|col=value|…" over the CURRENT logical schema, ordered — the reader's-eye view
    // that compaction must leave untouched.
    private static async Task<List<string>> ReadAllRowsAsync(DeltaTable table)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAllAsync())
        {
            for (int r = 0; r < batch.Length; r++)
            {
                var parts = new List<string>();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    string name = batch.Schema.FieldsList[c].Name;
                    object? v = batch.Column(c) switch
                    {
                        Int64Array a => a.IsNull(r) ? null : a.GetValue(r),
                        StringArray a => a.IsNull(r) ? null : a.GetString(r),
                        var other => throw new InvalidOperationException($"unexpected {other.GetType().Name}"),
                    };
                    parts.Add($"{name}={v ?? "<null>"}");
                }

                rows.Add(string.Join("|", parts));
            }
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    // Forces every file to be a compaction candidate regardless of size.
    private static CompactionOptions AlwaysCompact { get; } =
        new() { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue };

    /// <summary>
    /// The full mixed-vintage case on a column-mapping table: RENAME (physical names stay put while logical
    /// names move), ADD (older files lack the column), then DROP (older files still carry it) — with a write
    /// between each, so the three live files have three different column sets. Compaction must produce one
    /// file whose row groups all share the current shape, with the data intact.
    /// </summary>
    [Fact]
    public async Task Compact_MappedTable_AcrossRenameAddDrop_PreservesRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, SchemaOf(I64("a"), I64("b")), columnMappingMode: ColumnMappingMode.Name);

        // Vintage 1: columns (a, b).
        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), I64("b")),
            [
                new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build(),
                new Int64Array.Builder().AppendRange([10L, 20L, 30L]).Build(),
            ], 3)]);

        await table.RenameColumnAsync("b", "doubled");
        await table.AddColumnAsync(Str("note"));

        // Vintage 2: columns (a, doubled, note).
        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), I64("doubled"), Str("note")),
            [
                new Int64Array.Builder().Append(4L).Build(),
                new Int64Array.Builder().Append(40L).Build(),
                new StringArray.Builder().Append("n4").Build(),
            ], 1)]);

        await table.DropColumnAsync("doubled");

        // Vintage 3: columns (a, note).
        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), Str("note")),
            [
                new Int64Array.Builder().Append(5L).Build(),
                new StringArray.Builder().Append("n5").Build(),
            ], 1)]);

        var before = await ReadAllRowsAsync(table);
        Assert.Equal(
            [
                "a=1|note=<null>",
                "a=2|note=<null>",
                "a=3|note=<null>",
                "a=4|note=n4",
                "a=5|note=n5",
            ],
            before);

        var version = await table.CompactAsync(AlwaysCompact);
        Assert.NotNull(version);

        // One file now, and the rows a reader sees are unchanged.
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(before, await ReadAllRowsAsync(table));
    }

    /// <summary>
    /// The ADD-only half, with NO column mapping — the vintages differ purely by column count. The older
    /// file's rows must come back with the added column NULL, not with a neighbouring column's values shifted
    /// into it.
    /// </summary>
    [Fact]
    public async Task Compact_AcrossAddColumn_PreservesRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, SchemaOf(I64("a")));

        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a")),
            [new Int64Array.Builder().AppendRange([1L, 2L]).Build()], 2)]);

        await table.AddColumnAsync(Str("note"));

        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), Str("note")),
            [
                new Int64Array.Builder().Append(3L).Build(),
                new StringArray.Builder().Append("n3").Build(),
            ], 1)]);

        var before = await ReadAllRowsAsync(table);
        Assert.Equal(["a=1|note=<null>", "a=2|note=<null>", "a=3|note=n3"], before);

        Assert.NotNull(await table.CompactAsync(AlwaysCompact));
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(before, await ReadAllRowsAsync(table));
    }

    /// <summary>
    /// The DROP-only half: the older file still carries the dropped column's bytes. Compaction must leave
    /// them out of the output rather than carrying a column the current schema no longer describes.
    /// </summary>
    [Fact]
    public async Task Compact_AcrossDropColumn_PreservesRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, SchemaOf(I64("a"), I64("gone")), columnMappingMode: ColumnMappingMode.Name);

        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), I64("gone")),
            [
                new Int64Array.Builder().AppendRange([1L, 2L]).Build(),
                new Int64Array.Builder().AppendRange([100L, 200L]).Build(),
            ], 2)]);

        await table.DropColumnAsync("gone");

        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a")),
            [new Int64Array.Builder().Append(3L).Build()], 1)]);

        var before = await ReadAllRowsAsync(table);
        Assert.Equal(["a=1", "a=2", "a=3"], before);

        Assert.NotNull(await table.CompactAsync(AlwaysCompact));
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(before, await ReadAllRowsAsync(table));
    }

    /// <summary>
    /// A RENAME alone changes no type tree — only a field NAME. The rename path must reuse the reader's array
    /// verbatim rather than re-materializing it through the canonical layout, which rejects reader-produced
    /// arrays whose buffer count differs ("Buffer count &lt;2&gt; must be at exactly &lt;3&gt;").
    /// </summary>
    [Fact]
    public async Task Compact_MappedTable_AcrossRenameOnly_PreservesRows()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, SchemaOf(I64("a"), Str("label")), columnMappingMode: ColumnMappingMode.Name);

        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), Str("label")),
            [
                new Int64Array.Builder().AppendRange([1L, 2L]).Build(),
                new StringArray.Builder().Append("x").Append("y").Build(),
            ], 2)]);

        await table.RenameColumnAsync("label", "tag");

        await table.WriteAsync([new RecordBatch(
            SchemaOf(I64("a"), Str("tag")),
            [
                new Int64Array.Builder().Append(3L).Build(),
                new StringArray.Builder().Append("z").Build(),
            ], 1)]);

        var before = await ReadAllRowsAsync(table);
        Assert.Equal(["a=1|tag=x", "a=2|tag=y", "a=3|tag=z"], before);

        Assert.NotNull(await table.CompactAsync(AlwaysCompact));
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(before, await ReadAllRowsAsync(table));
    }
}
