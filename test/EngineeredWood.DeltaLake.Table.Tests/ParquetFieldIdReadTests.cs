// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowStructType = Apache.Arrow.Types.StructType;
using MapType = Apache.Arrow.Types.MapType;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The Parquet reader carries each field's <c>field_id</c> into Arrow as <c>PARQUET:field_id</c> (issue #446).
/// A Delta read uses it to resolve id-mode columns at every depth, and then removes it: the ids name one data
/// file's PHYSICAL columns, so a caller must not see them.
/// </summary>
public class ParquetFieldIdReadTests : IDisposable
{
    private readonly string _tempDir;

    public ParquetFieldIdReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_fieldid_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // id, struct<inner: int64, label: string>, list<int64>, map<string, int64>.
    private static Apache.Arrow.Schema Schema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("nested", new ArrowStructType(
            [
                new Field("inner", Int64Type.Default, true),
                new Field("label", StringType.Default, true),
            ]), true))
            .Field(new Field("items", new ListType(new Field("element", Int64Type.Default, true)), true))
            .Field(new Field("tags", new MapType(
                new Field("key", StringType.Default, false),
                new Field("value", Int64Type.Default, true)), true))
            .Build();

    private static RecordBatch Batch(Apache.Arrow.Schema schema)
    {
        var structType = (ArrowStructType)schema.FieldsList[1].DataType;
        var nested = new StructArray(
            structType, 2,
            [
                new Int64Array.Builder().Append(10).Append(20).Build(),
                new StringArray.Builder().Append("a").Append("b").Build(),
            ],
            ArrowBuffer.Empty);
        var items = new ListArray(
            schema.FieldsList[2].DataType, 2,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(2).Build(),
            new Int64Array.Builder().Append(7).Append(8).Build(),
            ArrowBuffer.Empty);
        var mapType = (MapType)schema.FieldsList[3].DataType;
        var entries = new StructArray(
            new ArrowStructType([mapType.KeyField, mapType.ValueField]), 2,
            [
                new StringArray.Builder().Append("k1").Append("k2").Build(),
                new Int64Array.Builder().Append(5).Append(6).Build(),
            ],
            ArrowBuffer.Empty);
        var tags = new MapArray(
            mapType, 2, new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(2).Build(), entries,
            ArrowBuffer.Empty);
        return new RecordBatch(
            schema, [new Int64Array.Builder().Append(1).Append(2).Build(), nested, items, tags], 2);
    }

    private static async Task<List<RecordBatch>> ReadAllAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);
        return batches;
    }

    private static IEnumerable<string> FieldIdPaths(IEnumerable<Field> fields, string prefix = "")
    {
        foreach (var field in fields)
        {
            string path = prefix + field.Name;
            if (field.Metadata is { } md && md.ContainsKey(ColumnMapping.ParquetFieldIdKey))
                yield return path;
            IEnumerable<Field> children = field.DataType switch
            {
                ArrowStructType st => st.Fields,
                MapType mt => [mt.KeyField, mt.ValueField],
                ListType lt => [lt.ValueField],
                _ => [],
            };
            foreach (var child in FieldIdPaths(children, path + "."))
                yield return child;
        }
    }

    [Theory]
    [InlineData(ColumnMappingMode.Id)]
    [InlineData(ColumnMappingMode.Name)]
    public async Task Read_HandsBackNoParquetFieldIds(ColumnMappingMode mode)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();
        await using var table = await DeltaTable.CreateAsync(fs, schema, columnMappingMode: mode);
        await table.WriteAsync([Batch(schema)]);

        var read = Assert.Single(await ReadAllAsync(table));

        // The data file carries an id on every mapped field; none of them may reach the caller, in the schema
        // or in the arrays' own types.
        Assert.Empty(FieldIdPaths(read.Schema.FieldsList));
        var arrayFields = read.Schema.FieldsList
            .Select((f, i) => new Field(f.Name, read.Column(i).Data.DataType, f.IsNullable, f.Metadata));
        Assert.Empty(FieldIdPaths(arrayFields));
        Assert.Equal(["id", "nested", "items", "tags"], read.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task IdMode_ResolvesNestedChildrenByFieldId_NotByName()
    {
        // Id mode resolves a file's columns by field id; its physical names need not be the ones the schema
        // records. Rewrite the data file with the struct's two children's names SWAPPED but their ids kept:
        // matching by name binds each child to the other's column (an int64 read as the string column); matching
        // by id reads it right.
        var fs = new LocalTableFileSystem(_tempDir);
        var schema = Schema();
        await using var table = await DeltaTable.CreateAsync(fs, schema, columnMappingMode: ColumnMappingMode.Id);
        await table.WriteAsync([Batch(schema)]);

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
        string path = Path.Combine(_tempDir, DeltaPath.Decode(addFile.Path));

        RecordBatch original;
        await using (var rf = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(rf, ownsFile: false))
            original = await reader.ReadRowGroupAsync(0);

        var structField = original.Schema.FieldsList[1];
        var structType = (ArrowStructType)structField.DataType;
        var (first, second) = (structType.Fields[0], structType.Fields[1]);
        var swappedType = new ArrowStructType(
        [
            new Field(second.Name, first.DataType, first.IsNullable, first.Metadata),
            new Field(first.Name, second.DataType, second.IsNullable, second.Metadata),
        ]);
        var structData = original.Column(1).Data;
        var swapped = new StructArray(new ArrayData(
            swappedType, structData.Length, structData.NullCount, structData.Offset, structData.Buffers,
            structData.Children));

        var fields = original.Schema.FieldsList.ToArray();
        fields[1] = new Field(structField.Name, swappedType, structField.IsNullable, structField.Metadata);
        var arrays = original.Arrays.ToArray();
        arrays[1] = swapped;
        var rewritten = new RecordBatch(new Apache.Arrow.Schema(fields, null), arrays, original.Length);

        File.Delete(path);
        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(rewritten);
            await writer.CloseAsync();
        }

        await using var reopened = await DeltaTable.OpenAsync(fs);
        var read = Assert.Single(await ReadAllAsync(reopened));
        var nested = (StructArray)read.Column(1);
        var nestedType = (ArrowStructType)read.Schema.FieldsList[1].DataType;
        Assert.Equal(["inner", "label"], nestedType.Fields.Select(f => f.Name));
        Assert.Equal([10L, 20L], ((Int64Array)nested.Fields[0]).Values.ToArray());
        Assert.Equal("b", ((StringArray)nested.Fields[1]).GetString(1));
    }
}
