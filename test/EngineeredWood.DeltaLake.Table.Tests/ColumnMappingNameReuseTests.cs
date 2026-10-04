// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowStructType = Apache.Arrow.Types.StructType;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A table upgraded to name-mode column mapping keeps each existing column's ORIGINAL name as its physical
/// name, so a physical name can equal a different column's logical name: rename <c>a</c> to <c>b</c> and then
/// add a new <c>a</c>, or chain renames (<c>x</c> to <c>y</c>, then <c>a</c> to <c>x</c>). A data file names its
/// columns physically, so resolving a file's column must try the PHYSICAL name first. Trying the logical name
/// first binds the old column's data to the new column. Reads, the change feed and compaction (which would
/// rewrite the mix-up into new files) each resolved names somewhere.
/// </summary>
/// <remarks>
/// The table is built with <c>preAssignedSchema</c>, with physical names equal to the logical ones: exactly
/// the metadata Spark leaves after <c>ALTER TABLE ... SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')</c>.
/// </remarks>
public class ColumnMappingNameReuseTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingNameReuseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cmreuse_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static StructField Mapped(string name, DeltaDataType type, int id) => new()
    {
        Name = name,
        Type = type,
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.FieldIdKey] = id.ToString(),
            [ColumnMapping.PhysicalNameKey] = name,
        },
    };

    private static PrimitiveType Long => new() { TypeName = "long" };

    // amount: long; s: struct<a: long, x: string>; every physical name equals its logical name.
    private static Apache.Arrow.Schema ArrowSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", Int64Type.Default, true))
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("a", Int64Type.Default, true),
                new Field("x", StringType.Default, true),
            ]), true))
            .Build();

    private static DeltaStructType UpgradedSchema() => new()
    {
        Fields =
        [
            Mapped("amount", Long, 1),
            Mapped("s", new DeltaStructType
            {
                Fields = [Mapped("a", Long, 3), Mapped("x", new PrimitiveType { TypeName = "string" }, 4)],
            }, 2),
        ],
    };

    private async Task<DeltaTable> CreateAsync(IReadOnlyDictionary<string, string>? configuration = null) =>
        await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), ArrowSchema(), columnMappingMode: ColumnMappingMode.Name,
            configuration: configuration, preAssignedSchema: UpgradedSchema());

    private static RecordBatch Row(long amount, long a, string x)
    {
        var schema = ArrowSchema();
        var structType = (ArrowStructType)schema.FieldsList[1].DataType;
        return new RecordBatch(schema,
            [
                new Int64Array.Builder().Append(amount).Build(),
                new StructArray(structType, 1,
                    [new Int64Array.Builder().Append(a).Build(), new StringArray.Builder().Append(x).Build()],
                    ArrowBuffer.Empty),
            ], 1);
    }

    // Each row as "name=value" pairs in schema order, struct children as "s.child"; a null renders empty.
    private static List<string> Render(IEnumerable<RecordBatch> batches)
    {
        static string Value(IArrowArray array, int i) => array.IsNull(i) ? "" : array switch
        {
            Int64Array l => l.GetValue(i)!.Value.ToString(),
            StringArray s => s.GetString(i),
            _ => "?",
        };

        var rows = new List<string>();
        foreach (var batch in batches)
        {
            for (int r = 0; r < batch.Length; r++)
            {
                var parts = new List<string>();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    var field = batch.Schema.FieldsList[c];
                    if (field.Name.StartsWith("_", StringComparison.Ordinal))
                        continue; // change-feed metadata columns
                    if (batch.Column(c) is StructArray st)
                    {
                        var type = (ArrowStructType)field.DataType;
                        for (int k = 0; k < type.Fields.Count; k++)
                            parts.Add($"{field.Name}.{type.Fields[k].Name}={Value(st.Fields[k], r)}");
                    }
                    else
                    {
                        parts.Add($"{field.Name}={Value(batch.Column(c), r)}");
                    }
                }
                rows.Add(string.Join(" ", parts));
            }
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task<List<string>> ReadAsync(DeltaTable table)
    {
        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadAllAsync())
            batches.Add(b);
        return Render(batches);
    }

    [Fact]
    public async Task Read_ChainedNestedRenames_EachChildKeepsItsOwnData()
    {
        // x -> y, then a -> x: the new logical x is physically "a", and the old file's "x" column is y's.
        await using var table = await CreateAsync();
        await table.WriteAsync([Row(1, 10, "p")]);
        await table.RenameFieldAsync(["s", "x"], "y");
        await table.RenameFieldAsync(["s", "a"], "x");

        Assert.Equal(["amount=1 s.x=10 s.y=p"], await ReadAsync(table));
    }

    [Fact]
    public async Task Read_ChainedTopLevelRenames_EachColumnKeepsItsOwnData()
    {
        // s -> t, then amount -> s. The read must not rename the top level before the nested transform: that
        // would hand it a logical "s" (the long column) to match physical-first — against the struct's "s".
        await using var table = await CreateAsync();
        await table.WriteAsync([Row(1, 10, "p")]);
        await table.RenameColumnAsync("s", "t");
        await table.RenameColumnAsync("amount", "s");

        Assert.Equal(["s=1 t.a=10 t.x=p"], await ReadAsync(table));
    }

    [Fact]
    public async Task Read_NestedFieldAddedUnderARenamedFieldsOldName_StaysEmpty()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([Row(1, 10, "p")]);
        await table.RenameFieldAsync(["s", "a"], "b");
        await table.AddFieldAsync(["s"], new Field("a", Int64Type.Default, true));

        Assert.Equal(["amount=1 s.b=10 s.x=p s.a="], await ReadAsync(table));
    }

    [Fact]
    public async Task ChangeFeed_ChainedNestedRenames_EachChildKeepsItsOwnData()
    {
        // The feed for an insert reads the data file itself, through its own name resolution.
        await using var table = await CreateAsync(
            new Dictionary<string, string> { [CdfConfig.EnableKey] = "true" });
        long inserted = await table.WriteAsync([Row(1, 10, "p")]);
        await table.RenameFieldAsync(["s", "x"], "y");
        await table.RenameFieldAsync(["s", "a"], "x");

        var batches = new List<RecordBatch>();
        await foreach (var b in table.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = inserted, EndVersion = inserted }))
        {
            batches.Add(b);
        }

        Assert.Equal(["amount=1 s.x=10 s.y=p"], Render(batches));
    }

    [Fact]
    public async Task Write_ColumnAddedUnderARenamedColumnsOldName_GetsItsOwnPhysicalColumn()
    {
        // The new logical "amount" must be written under its own physical name, not under "amount" — which is
        // the renamed column's. Binding it there wrote two columns physically named "amount" into one file.
        await using var table = await CreateAsync();
        await table.RenameColumnAsync("amount", "total");
        await table.AddColumnAsync(new Field("amount", Int64Type.Default, true));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("total", Int64Type.Default, true))
            .Field(ArrowSchema().FieldsList[1])
            .Field(new Field("amount", Int64Type.Default, true))
            .Build();
        var s = Row(0, 10, "p").Column(1);
        await table.WriteAsync([new RecordBatch(schema,
            [new Int64Array.Builder().Append(1).Build(), s, new Int64Array.Builder().Append(1000).Build()], 1)]);

        Assert.Equal(["total=1 s.a=10 s.x=p amount=1000"], await ReadAsync(table));

        var addFile = table.CurrentSnapshot.ActiveFiles.Values.Single();
        await using var file = await new LocalTableFileSystem(_tempDir).OpenReadAsync(DeltaPath.Decode(addFile.Path));
        using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
        var names = (await reader.GetSchemaAsync()).Root.Children.Select(c => c.Name).ToList();
        Assert.Equal(names.Count, names.Distinct().Count());
    }

    [Fact]
    public async Task Compaction_ColumnsAddedUnderRenamedColumnsOldNames_KeepEveryValueInPlace()
    {
        // Compaction re-stamps physical names onto batches that are ALREADY physical; binding by logical name
        // first would write the old column's values into the new column's physical slot, for good.
        await using var table = await CreateAsync();
        await table.WriteAsync([Row(1, 10, "p")]);
        await table.RenameColumnAsync("amount", "total");
        await table.AddColumnAsync(new Field("amount", Int64Type.Default, true));
        await table.RenameFieldAsync(["s", "a"], "b");
        await table.AddFieldAsync(["s"], new Field("a", Int64Type.Default, true));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("total", Int64Type.Default, true))
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("b", Int64Type.Default, true),
                new Field("x", StringType.Default, true),
                new Field("a", Int64Type.Default, true),
            ]), true))
            .Field(new Field("amount", Int64Type.Default, true))
            .Build();
        await table.WriteAsync([new RecordBatch(schema,
            [
                new Int64Array.Builder().Append(2).Build(),
                new StructArray((ArrowStructType)schema.FieldsList[1].DataType, 1,
                    [
                        new Int64Array.Builder().Append(20).Build(),
                        new StringArray.Builder().Append("q").Build(),
                        new Int64Array.Builder().Append(200).Build(),
                    ],
                    ArrowBuffer.Empty),
                new Int64Array.Builder().Append(2000).Build(),
            ], 1)]);

        List<string> expected =
        [
            "total=1 s.b=10 s.x=p s.a= amount=",
            "total=2 s.b=20 s.x=q s.a=200 amount=2000",
        ];
        Assert.Equal(expected, await ReadAsync(table));

        Assert.NotNull(await table.CompactAsync(
            new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        Assert.Equal(expected, await ReadAsync(table));
    }

    [Fact]
    public async Task Compaction_NestedFieldAdded_GuidPhysicalNames_KeepsEveryValue()
    {
        // No name is reused here: an ordinary name-mode table with GUID physical names. Compaction reconciles
        // each file against the table schema, which must be physical at EVERY level — the files' struct
        // children are physical-named too.
        var schema = ArrowSchema();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Row(1, 10, "p")]);
        await table.AddFieldAsync(["s"], new Field("c", Int64Type.Default, true));

        var evolved = new Apache.Arrow.Schema.Builder()
            .Field(schema.FieldsList[0])
            .Field(new Field("s", new ArrowStructType(
            [
                new Field("a", Int64Type.Default, true),
                new Field("x", StringType.Default, true),
                new Field("c", Int64Type.Default, true),
            ]), true))
            .Build();
        await table.WriteAsync([new RecordBatch(evolved,
            [
                new Int64Array.Builder().Append(2).Build(),
                new StructArray((ArrowStructType)evolved.FieldsList[1].DataType, 1,
                    [
                        new Int64Array.Builder().Append(20).Build(),
                        new StringArray.Builder().Append("q").Build(),
                        new Int64Array.Builder().Append(200).Build(),
                    ],
                    ArrowBuffer.Empty),
            ], 1)]);

        List<string> expected = ["amount=1 s.a=10 s.x=p s.c=", "amount=2 s.a=20 s.x=q s.c=200"];
        Assert.Equal(expected, await ReadAsync(table));

        Assert.NotNull(await table.CompactAsync(
            new CompactionOptions { MinFileSize = long.MaxValue, TargetFileSize = long.MaxValue }));
        Assert.Equal(expected, await ReadAsync(table));
    }
}
