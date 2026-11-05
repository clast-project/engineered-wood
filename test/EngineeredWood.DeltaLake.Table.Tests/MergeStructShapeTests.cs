// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The struct a MERGE without <see cref="MergeOptions.MergeSchema"/> writes (#700). Measured against Spark 4.0 /
/// Delta 4.0 without <c>autoMerge</c>: a struct copied or assigned to a column, at any depth (a struct field, an array
/// element, a map value), must have exactly the column's field names, or the MERGE is refused at analysis with
/// <c>DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION</c> ("Cannot cast struct&lt;a:int&gt; to struct&lt;a:int,c:string&gt;.
/// All nested columns must match."), even when no row reaches the clause. Fields in another order are matched by
/// name.
/// </summary>
public class MergeStructShapeTests : IDisposable
{
    private readonly string _tempDir;

    public MergeStructShapeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_merge_struct_shape_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Expression E(string sql) => SparkSqlParser.ParseExpression(sql);

    private async Task<DeltaTable> CreateAsync(Apache.Arrow.Schema schema, RecordBatch seed)
    {
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        await table.WriteAsync([seed]);
        return table;
    }

    private static IArrowArray Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (var v in values)
        {
            if (v is { } x) b.Append(x); else b.AppendNull();
        }
        return b.Build();
    }

    private static IArrowArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values)
        {
            if (v is null) b.AppendNull(); else b.Append(v);
        }
        return b.Build();
    }

    private static StructArray Struct(params (string Name, IArrowArray Values)[] fields) => new(
        new StructType(fields.Select(f => new Field(f.Name, f.Values.Data.DataType, true)).ToList()),
        fields[0].Values.Length, fields.Select(f => f.Values).ToList(), ArrowBuffer.Empty);

    private static RecordBatch Batch(params (string Name, IArrowArray Values)[] columns) => new(
        new Apache.Arrow.Schema(columns.Select(c => new Field(c.Name, c.Values.Data.DataType, true)).ToList(), null),
        columns.Select(c => c.Values).ToList(), columns[0].Values.Length);

    private static string Format(IArrowArray array, int i)
    {
        if (array.IsNull(i))
            return "null";
        return array switch
        {
            Int32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            StringArray a => a.GetString(i),
            StructArray s => "{" + string.Join(", ", ((StructType)s.Data.DataType).Fields.Select(
                (f, k) => f.Name + "=" + Format(s.Fields[k], i))) + "}",
            MapArray m => "{" + string.Join(", ", Enumerable.Range(m.ValueOffsets[i], m.ValueOffsets[i + 1] - m.ValueOffsets[i])
                .Select(j => Format(m.Keys, j) + "->" + Format(m.Values, j))) + "}",
            ListArray l => "[" + string.Join(", ", Enumerable.Range(l.ValueOffsets[i], l.ValueOffsets[i + 1] - l.ValueOffsets[i])
                .Select(j => Format(l.Values, j))) + "]",
            _ => throw new NotSupportedException(array.GetType().Name),
        };
    }

    /// <summary>Every row as <c>"col=value, ..."</c> in the table's column order, sorted.</summary>
    private static async Task<List<string>> RowsAsync(DeltaTable table)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAllAsync())
        {
            for (int i = 0; i < batch.Length; i++)
            {
                rows.Add(string.Join(", ", batch.Schema.FieldsList.Select(
                    (f, c) => f.Name + "=" + Format(batch.Column(c), i))));
            }
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private int DataFileCount() =>
        Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories).Count(f => !f.Contains("_delta_log"));

    // ── A top-level struct column ──

    private static readonly StructType AC =
        new([new Field("a", Int32Type.Default, true), new Field("c", StringType.Default, true)]);

    private static readonly Apache.Arrow.Schema Nested = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, true))
        .Field(new Field("st", AC, true))
        .Build();

    /// <summary>Rows 1 and 2 in one file, so an update of row 1 rewrites it beside an unchanged row.</summary>
    private Task<DeltaTable> CreateNestedAsync() => CreateAsync(
        Nested, Batch(("id", Ints(1, 2)), ("st", Struct(("a", Ints(1, 2)), ("c", Strings("c1", "c2"))))));

    /// <summary>The source struct for a shape: the table's is <c>st(a int, c string)</c>.</summary>
    private static StructArray Shaped(string shape) => shape switch
    {
        "lacks" => Struct(("a", Ints(10))),
        "extra" => Struct(("a", Ints(10)), ("c", Strings("x")), ("b", Strings("y"))),
        "renamed" => Struct(("a", Ints(10)), ("z", Strings("x"))),
        "reordered" => Struct(("c", Strings("x")), ("a", Ints(10))),
        _ => throw new ArgumentOutOfRangeException(nameof(shape)),
    };

    /// <summary>The clause that writes the source's struct column <paramref name="column"/>, and the source row's id.</summary>
    private static (MergeOptions Options, int Id) Writing(string how, string column = "st") => how switch
    {
        "update *" => (new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update] }, 1),
        "update set" => (new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, E("source." + column)))],
        }, 1),
        "update columns" => (new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.UpdateColumns(column)] }, 1),
        "insert *" => (new MergeOptions { Keys = ["id"], WhenNotMatched = [MergeAction.Insert] }, 3),
        "insert values" => (new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatched = [MergeAction.InsertValues(("id", E("source.id")), (column, E("source." + column)))],
        }, 3),
        // No row reaches the clause: Spark refuses at analysis all the same.
        "unreached" => (new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update] }, 9),
        "unreached set" => (new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, E("source." + column)))],
        }, 9),
        _ => throw new ArgumentOutOfRangeException(nameof(how)),
    };

    public static TheoryData<string, string> Mismatches()
    {
        var data = new TheoryData<string, string>();
        foreach (string shape in new[] { "lacks", "extra", "renamed" })
        {
            foreach (string how in new[] { "update *", "update set", "update columns", "insert *", "insert values", "unreached", "unreached set" })
                data.Add(shape, how);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Mismatches))]
    public async Task AStructWhoseFieldsDifferFromTheColumns_IsRefused(string shape, string how)
    {
        await using var table = await CreateNestedAsync();
        long before = table.CurrentSnapshot.Version;
        int files = DataFileCount();
        var (options, id) = Writing(how);

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() =>
            table.MergeAsync([Batch(("id", Ints(id)), ("st", Shaped(shape)))], options).AsTask());

        Assert.Equal(DeltaTableErrorCodes.UpdateSchemaMismatchExpression, e.ErrorCode);
        Assert.Contains("All nested columns must match", e.Message);
        Assert.Equal(before, table.CurrentSnapshot.Version);
        Assert.Equal(files, DataFileCount());
    }

    [Fact]
    public async Task TheRefusal_NamesTheColumnAndBothStructs()
    {
        await using var table = await CreateNestedAsync();

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() =>
            table.MergeAsync([Batch(("id", Ints(1)), ("st", Shaped("lacks")))], Writing("update *").Options).AsTask());

        Assert.Contains("struct<a:int> to struct<a:int,c:string>", e.Message);
        Assert.Contains("'st'", e.Message);
    }

    [Theory]
    [InlineData("update *")]
    [InlineData("update set")]
    [InlineData("update columns")]
    [InlineData("insert *")]
    [InlineData("insert values")]
    public async Task AStructWhoseFieldsComeInAnotherOrder_IsMatchedByName(string how)
    {
        await using var table = await CreateNestedAsync();
        var (options, id) = Writing(how);

        await table.MergeAsync([Batch(("id", Ints(id)), ("st", Shaped("reordered")))], options);

        Assert.Equal(
            id == 1
                ? ["id=1, st={a=10, c=x}", "id=2, st={a=2, c=c2}"]
                : ["id=1, st={a=1, c=c1}", "id=2, st={a=2, c=c2}", "id=3, st={a=10, c=x}"],
            await RowsAsync(table));
    }

    [Fact]
    public async Task ANestedStructsFields_AreCheckedAndMatchedToo()
    {
        var inner = new StructType([new Field("x", Int32Type.Default, true), new Field("y", Int32Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("st", new StructType([new Field("a", Int32Type.Default, true), new Field("in", inner, true)]), true))
            .Build();
        await using var table = await CreateAsync(schema,
            Batch(("id", Ints(1)), ("st", Struct(("a", Ints(1)), ("in", Struct(("x", Ints(1)), ("y", Ints(2))))))));

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync(
            [Batch(("id", Ints(1)), ("st", Struct(("a", Ints(10)), ("in", Struct(("x", Ints(10)))))))],
            Writing("update *").Options).AsTask());
        Assert.Equal(DeltaTableErrorCodes.UpdateSchemaMismatchExpression, e.ErrorCode);

        await table.MergeAsync(
            [Batch(("id", Ints(1)), ("st", Struct(("in", Struct(("y", Ints(20)), ("x", Ints(10)))), ("a", Ints(10)))))],
            Writing("update *").Options);
        Assert.Equal(["id=1, st={a=10, in={x=10, y=20}}"], await RowsAsync(table));
    }

    // ── Inside an array or a map ──

    private static readonly StructType KeyValue =
        new([new Field("key", StringType.Default, false), new Field("value", AC, true)]);

    private static IArrowArray Collection(bool map, StructArray values)
    {
        var offsets = new ArrowBuffer.Builder<int>();
        for (int i = 0; i <= values.Length; i++)
            offsets.Append(i);
        if (!map)
            return new ListArray(new ListType(new Field("element", values.Data.DataType, true)), values.Length,
                offsets.Build(), values, ArrowBuffer.Empty);
        var entries = new StructType(
            [new Field("key", StringType.Default, false), new Field("value", values.Data.DataType, true)]);
        return new MapArray(
            new MapType(new Field("key", StringType.Default, false), new Field("value", values.Data.DataType, true)),
            values.Length, offsets.Build(),
            new StructArray(entries, values.Length, [Strings(Enumerable.Repeat("k", values.Length).ToArray()), values],
                ArrowBuffer.Empty),
            ArrowBuffer.Empty);
    }

    /// <summary>Rows 1 and 2 in one file, each with one element (one entry under key <c>k</c>).</summary>
    private Task<DeltaTable> CreateCollectionAsync(bool map)
    {
        var column = Collection(map, Struct(("a", Ints(1, 2)), ("c", Strings("k1", "k2"))));
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true)).Field(new Field("col", column.Data.DataType, true)).Build();
        return CreateAsync(schema, new RecordBatch(schema, [Ints(1, 2), column], 2));
    }

    [Theory]
    [InlineData(false, "lacks", "update *")]
    [InlineData(false, "extra", "update *")]
    [InlineData(false, "lacks", "insert *")]
    [InlineData(false, "lacks", "update set")]
    [InlineData(true, "lacks", "update *")]
    [InlineData(true, "lacks", "insert *")]
    [InlineData(true, "renamed", "unreached")]
    public async Task AStructInsideAnArrayOrAMap_IsCheckedToo(bool map, string shape, string how)
    {
        await using var table = await CreateCollectionAsync(map);
        long before = table.CurrentSnapshot.Version;
        var (options, id) = Writing(how, "col");

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() =>
            table.MergeAsync([Batch(("id", Ints(id)), ("col", Collection(map, Shaped(shape))))], options).AsTask());

        Assert.Equal(DeltaTableErrorCodes.UpdateSchemaMismatchExpression, e.ErrorCode);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AStructInsideAnArrayOrAMap_InAnotherOrder_IsMatchedByName(bool map)
    {
        await using var table = await CreateCollectionAsync(map);

        await table.MergeAsync(
            [Batch(("id", Ints(1)), ("col", Collection(map, Shaped("reordered"))))], Writing("update *").Options);

        Assert.Equal(
            map
                ? ["id=1, col={k->{a=10, c=x}}", "id=2, col={k->{a=2, c=k2}}"]
                : ["id=1, col=[{a=10, c=x}]", "id=2, col=[{a=2, c=k2}]"],
            await RowsAsync(table));
    }

    // ── What stays as it was ──

    [Theory]
    [InlineData("lacks")]
    [InlineData("reordered")]
    public async Task AnAppend_StillTakesThem(string shape)
    {
        // Measured: a Spark append fills a missing nested field with NULL, with or without mergeSchema. The MERGE's
        // check is its own; the append's type check is not tightened.
        await using var table = await CreateNestedAsync();

        await table.WriteAsync([Batch(("id", Ints(3)), ("st", Shaped(shape)))]);

        Assert.Contains(shape == "lacks" ? "id=3, st={a=10, c=null}" : "id=3, st={a=10, c=x}", await RowsAsync(table));
    }

    [Theory]
    [InlineData("lacks", "id=1, st={a=10, c=c1}")]
    [InlineData("extra", "id=1, st={a=10, c=x, b=y}")]
    public async Task WithMergeSchema_TheStructIsEvolvedInstead(string shape, string row)
    {
        await using var table = await CreateNestedAsync();

        await table.MergeAsync(
            [Batch(("id", Ints(1)), ("st", Shaped(shape)))],
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update], MergeSchema = true });

        Assert.Contains(row, await RowsAsync(table));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnElementTheSourceLacksAFieldOf_UnderMergeSchema_IsNull_BesideAnUnchangedRow(bool map)
    {
        // Measured (array): under autoMerge Spark writes [{a: 10, c: NULL}]. Row 2 shares row 1's file, so the
        // rewrite writes the fitted element beside an unchanged one.
        await using var table = await CreateCollectionAsync(map);

        await table.MergeAsync(
            [Batch(("id", Ints(1)), ("col", Collection(map, Shaped("lacks"))))],
            new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update], MergeSchema = true });

        Assert.Equal(
            map
                ? ["id=1, col={k->{a=10, c=null}}", "id=2, col={k->{a=2, c=k2}}"]
                : ["id=1, col=[{a=10, c=null}]", "id=2, col=[{a=2, c=k2}]"],
            await RowsAsync(table));
    }
}
