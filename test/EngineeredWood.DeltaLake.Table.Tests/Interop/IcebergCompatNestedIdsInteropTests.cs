// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using ArrowMapType = Apache.Arrow.Types.MapType;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> #467: IcebergCompatV2's nested ids, checked against delta-spark. Spark runs
/// <c>rewriteFieldIdsForIceberg</c> on every metadata update of such a table: it keeps each path that already has
/// an id and mints one for each that has none. So when Spark ALTERs a table EW created, the ids EW recorded come
/// back unchanged only if EW spelled the paths as Spark does; and Spark's own data files then show which Parquet
/// node each id belongs on. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class IcebergCompatNestedIdsInteropTests : IDisposable
{
    private readonly string _tempDir;

    public IcebergCompatNestedIdsInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_nested_ids_interop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Dictionary<string, int> Nested(StructField field) =>
        ColumnMapping.GetNestedIds(field).ToDictionary(e => e.Key, e => e.Value);

    private static string Physical(StructField field) =>
        ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name);

    [SkippableFact]
    public async Task SparkKeepsEwsNestedIds_AndStampsThemOnTheSameParquetNodes()
    {
        Spark.Require();

        var intList = new ListType(new Field("element", Int32Type.Default, true));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, true))
            .Field(new Field("arr", new ListType(new Field("element", Int64Type.Default, true)), true))
            .Field(new Field("m", new ArrowMapType(
                new Field("key", StringType.Default, false), new Field("value", intList, true)), true))
            .Build();
        var fs = new LocalTableFileSystem(_tempDir);

        Dictionary<string, Dictionary<string, int>> ewIds;
        HashSet<string> ewFiles;
        await using (var table = await DeltaTable.CreateAsync(fs, schema,
            columnMappingMode: ColumnMappingMode.Name,
            configuration: new Dictionary<string, string> { [IcebergCompat.EnableV2Key] = "true" }))
        {
            var mapType = (ArrowMapType)schema.FieldsList[2].DataType;
            var mapValue = new ListArray(intList, 1, new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(),
                new Int32Array.Builder().Append(2).Build(), ArrowBuffer.Empty);
            var entries = new StructArray(
                new Apache.Arrow.Types.StructType([mapType.KeyField, mapType.ValueField]), 1,
                [new StringArray.Builder().Append("a").Build(), mapValue], ArrowBuffer.Empty);
            await table.WriteAsync([new RecordBatch(schema,
            [
                new Int64Array.Builder().Append(1).Build(),
                new ListArray(schema.FieldsList[1].DataType, 1,
                    new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(),
                    new Int64Array.Builder().Append(1).Build(), ArrowBuffer.Empty),
                new MapArray(mapType, 1, new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), entries,
                    ArrowBuffer.Empty),
            ], 1)]);

            ewIds = table.CurrentSnapshot.Schema.Fields.ToDictionary(f => f.Name, Nested);
            ewFiles = [.. table.CurrentSnapshot.ActiveFiles.Keys];
        }

        var result = Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[]
            {
                "ALTER TABLE delta.`{path}` ADD COLUMNS (extra ARRAY<INT>)",
                "INSERT INTO delta.`{path}` VALUES (2, array(3), map('b', array(4)), array(5))",
            },
        });
        Assert.Equal(2, result.GetProperty("row_count").GetInt32());

        await using var reopened = await DeltaTable.OpenAsync(fs);
        var fields = reopened.CurrentSnapshot.Schema.Fields;

        // Spark minted nothing for a path EW had already given an id: EW spells the paths as Spark does.
        Assert.Equal(ewIds["arr"], Nested(fields.Single(f => f.Name == "arr")));
        Assert.Equal(ewIds["m"], Nested(fields.Single(f => f.Name == "m")));
        Assert.Equal(3, ewIds["m"].Count); // key, value, value.element

        // Spark's own column, ids from the same sequence: every id in the schema is distinct.
        var extra = fields.Single(f => f.Name == "extra");
        Assert.Equal([Physical(extra) + ".element"], Nested(extra).Keys);
        var allIds = fields.Select(f => ColumnMapping.GetFieldId(f)!.Value)
            .Concat(fields.SelectMany(f => Nested(f).Values)).ToList();
        Assert.Equal(allIds.Count, allIds.Distinct().Count());

        // Spark's data file puts each id where EW does: on the element / key / value node.
        var sparkFile = reopened.CurrentSnapshot.ActiveFiles.Keys.Single(path => !ewFiles.Contains(path));
        await using (var file = await fs.OpenReadAsync(sparkFile))
        {
            using var reader = new EngineeredWood.Parquet.ParquetFileReader(file, ownsFile: false);
            var root = (await reader.GetSchemaAsync()).Root;
            Parquet.Schema.SchemaNode Column(string name) =>
                root.Children.Single(c => c.Name == Physical(fields.Single(f => f.Name == name)));

            string arr = Physical(fields.Single(f => f.Name == "arr"));
            Assert.Equal(ewIds["arr"][arr + ".element"], Column("arr").Children[0].Children[0].Element.FieldId);
            string m = Physical(fields.Single(f => f.Name == "m"));
            var keyValue = Column("m").Children[0];
            Assert.Equal(ewIds["m"][m + ".key"], keyValue.Children[0].Element.FieldId);
            Assert.Equal(ewIds["m"][m + ".value"], keyValue.Children[1].Element.FieldId);
            Assert.Equal(ewIds["m"][m + ".value.element"], keyValue.Children[1].Children[0].Children[0].Element.FieldId);
        }

        // And EW's next column continues past every id Spark recorded.
        await reopened.AddColumnAsync(new Field("more", new ListType(new Field("element", Int64Type.Default, true)), true));
        fields = reopened.CurrentSnapshot.Schema.Fields;
        allIds = fields.Select(f => ColumnMapping.GetFieldId(f)!.Value)
            .Concat(fields.SelectMany(f => Nested(f).Values)).ToList();
        Assert.Equal(allIds.Count, allIds.Distinct().Count());

        int rows = 0;
        await foreach (var batch in reopened.ReadAllAsync())
            rows += batch.Length;
        Assert.Equal(2, rows);
    }
}
