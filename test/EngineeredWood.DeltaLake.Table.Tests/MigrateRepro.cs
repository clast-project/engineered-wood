using Apache.Arrow;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Xunit;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class MigrateRepro
{
    [Fact]
    public async Task Compact_MappedTable_AfterAddDropRename_Works()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ewrepro_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var fs = new LocalTableFileSystem(dir);
        var schema = new Apache.Arrow.Schema(new List<Field>
        {
            new("a", Apache.Arrow.Types.Int64Type.Default, true),
            new("b", Apache.Arrow.Types.Int64Type.Default, true),
        }, null);
        await using var table = await DeltaTable.CreateAsync(fs, schema,
            columnMappingMode: Schema.ColumnMappingMode.Name);
        var b1 = new RecordBatch(schema, new IArrowArray[]
        {
            new Int64Array.Builder().AppendRange(new long[]{1,2,3}).Build(),
            new Int64Array.Builder().AppendRange(new long[]{10,20,30}).Build(),
        }, 3);
        await table.WriteAsync(new[]{b1});
        await table.RenameColumnAsync("b", "doubled");
        await table.AddColumnAsync(new Field("note", Apache.Arrow.Types.StringType.Default, true));
        var s2 = new Apache.Arrow.Schema(new List<Field>
        {
            new("a", Apache.Arrow.Types.Int64Type.Default, true),
            new("doubled", Apache.Arrow.Types.Int64Type.Default, true),
            new("note", Apache.Arrow.Types.StringType.Default, true),
        }, null);
        var b2 = new RecordBatch(s2, new IArrowArray[]
        {
            new Int64Array.Builder().Append(4).Build(),
            new Int64Array.Builder().Append(40).Build(),
            new StringArray.Builder().Append("n4").Build(),
        }, 1);
        await table.WriteAsync(new[]{b2});
        await table.DropColumnAsync("doubled");
        var b3 = new RecordBatch(new Apache.Arrow.Schema(new List<Field>
        {
            new("a", Apache.Arrow.Types.Int64Type.Default, true),
            new("note", Apache.Arrow.Types.StringType.Default, true),
        }, null), new IArrowArray[]
        {
            new Int64Array.Builder().Append(5).Build(),
            new StringArray.Builder().Append("n5").Build(),
        }, 1);
        await table.WriteAsync(new[]{b3});
        var result = await table.CompactAsync();
        Assert.True(result is null || result >= 0);
    }
}
