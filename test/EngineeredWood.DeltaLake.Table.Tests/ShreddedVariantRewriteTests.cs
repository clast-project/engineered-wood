// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A file EW shredded reads back reassembled, as <c>(metadata, value)</c>, and must be rewritable: a copy-on-write
/// DELETE or UPDATE and a compaction hand its batches to a writer (#691). And a table holding shredded and unshredded
/// files reads both with one schema.
/// </summary>
public class ShreddedVariantRewriteTests : IDisposable
{
    private readonly string _tempDir;

    public ShreddedVariantRewriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_shredded_rewrite_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema VariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    private static Dictionary<string, string> Enabled() => new() { ["delta.enableVariantShredding"] = "true" };

    private static DeltaTableOptions Shredding(bool shred) => DeltaTableOptions.Default with
    {
        ParquetWriteOptions = ParquetWriteOptions.Default with { ShredVariants = shred ? ShredOptions.Default : null },
    };

    private static VariantValue A(int a) => VariantValue.FromObject(new Dictionary<string, VariantValue>
    {
        ["a"] = VariantValue.FromInt32(a),
    });

    private static RecordBatch Batch(params (long Id, VariantValue Value)[] rows)
    {
        var ids = new Int64Array.Builder();
        var values = new VariantArray.Builder();
        foreach (var (id, value) in rows)
        {
            ids.Append(id);
            values.Append(value);
        }
        return new RecordBatch(VariantSchema(), [ids.Build(), values.Build(allocator: null)], rows.Length);
    }

    private static Func<RecordBatch, BooleanArray> IdIn(params long[] ids) => batch =>
    {
        var column = (Int64Array)batch.Column("id");
        var result = new BooleanArray.Builder();
        for (int i = 0; i < column.Length; i++)
            result.Append(ids.Contains(column.GetValue(i)!.Value));
        return result.Build();
    };

    /// <summary>The table's rows as id → the JSON of its variant, and every batch's schema, described in full.</summary>
    private static async Task<(SortedDictionary<long, string> Rows, HashSet<string> Schemas)> ReadAsync(DeltaTable table)
    {
        var rows = new SortedDictionary<long, string>();
        var schemas = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var batch in table.ReadAllAsync())
        {
            schemas.Add(Describe(batch.Schema));
            var ids = (Int64Array)batch.Column("id");
            var values = (VariantArray)batch.Column("v");
            for (int i = 0; i < batch.Length; i++)
            {
                rows[ids.GetValue(i)!.Value] = VariantJsonWriter.ToJson(values.GetLogicalVariantValue(i), indented: false);
            }
        }
        return (rows, schemas);
    }

    /// <summary>A schema with every type spelled out, an extension's storage included (<c>ToString</c> shows neither
    /// the storage nor a field's nullability, so it cannot tell a shredded variant from a reassembled one).</summary>
    internal static string Describe(Apache.Arrow.Schema schema) =>
        string.Join(", ", schema.FieldsList.Select(Describe));

    private static string Describe(Field field) => $"{field.Name}:{Describe(field.DataType)}{(field.IsNullable ? "" : " not null")}";

    private static string Describe(IArrowType type) => type switch
    {
        ExtensionType e => $"{e.Name}<{Describe(e.StorageType)}>",
        StructType s => $"struct<{string.Join(", ", s.Fields.Select(Describe))}>",
        MapType m => $"map<{Describe(m.KeyField)}, {Describe(m.ValueField)}>",
        ListType l => $"list<{Describe(l.ValueField)}>",
        _ => type.TypeId.ToString(),
    };

    /// <summary>Whether each active file stores <c>v</c> shredded (its storage has <c>typed_value</c>).</summary>
    private async Task<List<bool>> ShreddedFilesAsync(string dir, DeltaTable table)
    {
        var result = new List<bool>();
        foreach (var add in table.CurrentSnapshot.ActiveFiles.Values)
        {
            await using var file = new LocalRandomAccessFile(Path.Combine(dir, add.Path));
            await using var reader = new EngineeredWood.Parquet.ParquetFileReader(file, ownsFile: false);
            var storage = (StructType)(await reader.GetArrowSchemaAsync()).GetFieldByName("v").DataType;
            result.Add(storage.Fields.Any(f => f.Name == "typed_value"));
        }
        result.Sort();
        return result;
    }

    private (string Dir, LocalTableFileSystem Fs) NewTable(string name)
    {
        string dir = Path.Combine(_tempDir, name);
        Directory.CreateDirectory(dir);
        return (dir, new LocalTableFileSystem(dir));
    }

    /// <summary>The issue's repro: a copy-on-write DELETE on a file EW shredded failed in the writer's schema check.</summary>
    [Fact]
    public async Task CopyOnWriteDelete_OfAShreddedFile()
    {
        var (dir, fs) = NewTable("t");
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: Enabled(), options: Shredding(true));
        await table.WriteAsync([Batch((0, A(1)), (1, A(2)))]);
        Assert.Equal([true], await ShreddedFilesAsync(dir, table));

        var (deleted, _) = await table.DeleteAsync(IdIn(0), RowDeleteMode.CopyOnWrite);

        Assert.Equal(1, deleted);
        var (rows, _) = await ReadAsync(table);
        Assert.Equal(new SortedDictionary<long, string> { [1] = """{"a":2}""" }, rows);
    }

    [Fact]
    public async Task CopyOnWriteUpdate_OfAShreddedFile()
    {
        var (_, fs) = NewTable("t");
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: Enabled(), options: Shredding(true));
        await table.WriteAsync([Batch((0, A(1)), (1, A(2)))]);

        var (updated, _) = await table.UpdateAsync(IdIn(0), batch =>
        {
            var values = new VariantArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                values.Append(A(100));
            return new RecordBatch(batch.Schema, [batch.Column("id"), values.Build(allocator: null)], batch.Length);
        });

        Assert.Equal(1, updated);
        var (rows, _) = await ReadAsync(table);
        Assert.Equal(new SortedDictionary<long, string> { [0] = """{"a":100}""", [1] = """{"a":2}""" }, rows);
    }

    [Fact]
    public async Task Compaction_OfShreddedFiles()
    {
        var (_, fs) = NewTable("t");
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: Enabled(), options: Shredding(true));
        await table.WriteAsync([Batch((0, A(1)))]);
        await table.WriteAsync([Batch((1, A(2)))]);

        Assert.NotNull(await table.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));

        var (rows, _) = await ReadAsync(table);
        Assert.Equal(new SortedDictionary<long, string> { [0] = """{"a":1}""", [1] = """{"a":2}""" }, rows);
    }

    /// <summary>
    /// One shredded and one unshredded file in a table: a scan yields one schema for both, and a copy-on-write DELETE
    /// touching both and a compaction of both write them out. The unshredded file keeps the type its footer declares,
    /// the shredded one is reassembled; for EW's own files the two are the same.
    /// </summary>
    [Fact]
    public async Task ATableOfShreddedAndUnshreddedFiles_ReadsAndRewritesAlike()
    {
        var (dir, fs) = NewTable("t");
        await using (var table = await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: Enabled(), options: Shredding(true)))
            await table.WriteAsync([Batch((0, A(1)), (1, A(2)))]);
        await using (var table = await DeltaTable.OpenAsync(fs, Shredding(false)))
            await table.WriteAsync([Batch((2, A(3)), (3, A(4)))]);

        await using var mixed = await DeltaTable.OpenAsync(fs, Shredding(false));
        Assert.Equal([false, true], await ShreddedFilesAsync(dir, mixed));
        var (before, schemas) = await ReadAsync(mixed);
        Assert.Equal(4, before.Count);
        Assert.Single(schemas);

        var (deleted, _) = await mixed.DeleteAsync(IdIn(0, 2), RowDeleteMode.CopyOnWrite);
        Assert.Equal(2, deleted);
        var (afterDelete, deleteSchemas) = await ReadAsync(mixed);
        Assert.Equal(new SortedDictionary<long, string> { [1] = """{"a":2}""", [3] = """{"a":4}""" }, afterDelete);
        Assert.Equal(schemas, deleteSchemas);

        Assert.NotNull(await mixed.CompactAsync(new CompactionOptions { MinFileSize = long.MaxValue }));
        var (afterCompaction, compactionSchemas) = await ReadAsync(mixed);
        Assert.Equal(afterDelete, afterCompaction);
        Assert.Equal(schemas, compactionSchemas);
    }
}
