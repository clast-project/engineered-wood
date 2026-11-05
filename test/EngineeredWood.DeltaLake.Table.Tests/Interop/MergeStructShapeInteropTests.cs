// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using static EngineeredWood.DeltaLake.Table.Tests.Interop.MergeSchemaInteropTests;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> The struct a MERGE without <see cref="MergeOptions.MergeSchema"/> writes (#700), against
/// Spark's MERGE without <c>autoMerge</c>: Spark creates and seeds two identical tables with a struct column, an
/// array of structs and a map of structs, and each case MERGEs one with <see cref="DeltaTable.MergeAsync"/> and the
/// other with the equivalent <c>MERGE INTO</c>. A struct whose field names differ from its column's must be refused
/// by both with <c>DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION</c>, leaving the same rows; one whose fields only come in
/// another order must leave the same rows. See <see cref="Spark"/> for setup and cost.</para>
/// </summary>
[Collection("Interop")]
public class MergeStructShapeInteropTests : IDisposable
{
    private readonly string _ours;
    private readonly string _theirs;

    public MergeStructShapeInteropTests()
    {
        string root = Path.Combine(Path.GetTempPath(), $"delta_merge_struct_shape_interop_{Guid.NewGuid():N}");
        _ours = Path.Combine(root, "ours");
        _theirs = Path.Combine(root, "theirs");
        Directory.CreateDirectory(_ours);
        Directory.CreateDirectory(_theirs);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        string root = Path.GetDirectoryName(_ours)!;
        if (Directory.Exists(root))
        {
            try { Directory.Delete(root, recursive: true); } catch { }
        }
    }

    private static void CreateAndSeed(string dir) => Sql(dir,
        "CREATE TABLE delta.`{path}` (id INT, st STRUCT<a: INT, c: STRING>, arr ARRAY<STRUCT<a: INT, c: STRING>>, "
        + "m MAP<STRING, STRUCT<a: INT, c: STRING>>) USING delta",
        "INSERT INTO delta.`{path}` VALUES "
        + "(1, named_struct('a', 1, 'c', 'c1'), array(named_struct('a', 1, 'c', 'c1')), map('k', named_struct('a', 1, 'c', 'c1'))), "
        + "(2, named_struct('a', 2, 'c', 'c2'), array(named_struct('a', 2, 'c', 'c2')), map('k', named_struct('a', 2, 'c', 'c2')))");

    /// <summary>A struct of a shape, as Spark SQL and as a one-row array: the table's is <c>struct&lt;a, c&gt;</c>.</summary>
    private static (string Sql, StructArray Array) Shaped(string shape) => shape switch
    {
        "same" => ("named_struct('a', 10, 'c', 'x')", Struct(("a", Ints(10)), ("c", Strings("x")))),
        "lacks" => ("named_struct('a', 10)", Struct(("a", Ints(10)))),
        "extra" => ("named_struct('a', 10, 'c', 'x', 'b', 'y')", Struct(("a", Ints(10)), ("c", Strings("x")), ("b", Strings("y")))),
        "renamed" => ("named_struct('a', 10, 'z', 'x')", Struct(("a", Ints(10)), ("z", Strings("x")))),
        "reordered" => ("named_struct('c', 'x', 'a', 10)", Struct(("c", Strings("x")), ("a", Ints(10)))),
        _ => throw new ArgumentOutOfRangeException(nameof(shape)),
    };

    private static IArrowArray Ints(params int[] values) => new Int32Array.Builder().AppendRange(values).Build();
    private static IArrowArray Strings(params string[] values) => new StringArray.Builder().AppendRange(values).Build();

    private static StructArray Struct(params (string Name, IArrowArray Values)[] fields) => new(
        new StructType(fields.Select(f => new Field(f.Name, f.Values.Data.DataType, true)).ToList()),
        fields[0].Values.Length, fields.Select(f => f.Values).ToList(), ArrowBuffer.Empty);

    private static ArrowBuffer Offsets() => new ArrowBuffer.Builder<int>().Append(0).Append(1).Build();

    private static ListArray ListOf(StructArray element) => new(
        new ListType(new Field("element", element.Data.DataType, true)), 1, Offsets(), element, ArrowBuffer.Empty);

    private static MapArray MapOf(StructArray value) => new(
        new MapType(new Field("key", StringType.Default, false), new Field("value", value.Data.DataType, true)), 1,
        Offsets(),
        new StructArray(
            new StructType([new Field("key", StringType.Default, false), new Field("value", value.Data.DataType, true)]),
            1, [Strings("k"), value], ArrowBuffer.Empty),
        ArrowBuffer.Empty);

    /// <summary>The source row <paramref name="id"/>: every column of the table's shape but <paramref name="column"/>,
    /// which is of <paramref name="shape"/>.</summary>
    private static (string Sql, RecordBatch Batch) Source(int id, string column, string shape)
    {
        var (stSql, st) = Shaped(column == "st" ? shape : "same");
        var (arrSql, arr) = Shaped(column == "arr" ? shape : "same");
        var (mSql, m) = Shaped(column == "m" ? shape : "same");
        var columns = new (string Name, IArrowArray Values)[] { ("id", Ints(id)), ("st", st), ("arr", ListOf(arr)), ("m", MapOf(m)) };
        var batch = new RecordBatch(
            new Apache.Arrow.Schema(columns.Select(c => new Field(c.Name, c.Values.Data.DataType, true)).ToList(), null),
            columns.Select(c => c.Values).ToList(), 1);
        return ($"SELECT {id} AS id, {stSql} AS st, array({arrSql}) AS arr, map('k', {mSql}) AS m", batch);
    }

    /// <summary>A clause writing <paramref name="column"/>, as Spark SQL and as options, and the source row's id.</summary>
    private static (string Sql, MergeOptions Options, int Id) Clause(string how, string column) => how switch
    {
        "update *" => ("WHEN MATCHED THEN UPDATE SET *", new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update] }, 1),
        "update set" => ($"WHEN MATCHED THEN UPDATE SET {column} = s.{column}", new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet((column, SparkSqlParser.ParseExpression("source." + column)))],
        }, 1),
        "insert *" => ("WHEN NOT MATCHED THEN INSERT *", new MergeOptions { Keys = ["id"], WhenNotMatched = [MergeAction.Insert] }, 3),
        "insert values" => ($"WHEN NOT MATCHED THEN INSERT (id, {column}) VALUES (s.id, s.{column})", new MergeOptions
        {
            Keys = ["id"],
            WhenNotMatched = [MergeAction.InsertValues(
                ("id", SparkSqlParser.ParseExpression("source.id")),
                (column, SparkSqlParser.ParseExpression("source." + column)))],
        }, 3),
        "unreached" => ("WHEN MATCHED THEN UPDATE SET *", new MergeOptions { Keys = ["id"], WhenMatched = [MergeAction.Update] }, 9),
        _ => throw new ArgumentOutOfRangeException(nameof(how)),
    };

    public static TheoryData<string, string, string> Cases => new()
    {
        { "st", "lacks", "update *" },
        { "st", "extra", "insert values" },
        { "st", "renamed", "unreached" },
        { "st", "lacks", "update set" },
        { "arr", "lacks", "update *" },
        { "arr", "extra", "insert *" },
        { "m", "lacks", "insert *" },
        { "st", "reordered", "update *" },
        { "st", "reordered", "insert values" },
        { "arr", "reordered", "update *" },
        { "m", "reordered", "insert *" },
    };

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    public async Task AMergeWritingAStruct_IsRefusedOrMatchedByName_AsSparksIs(string column, string shape, string how)
    {
        Spark.Require();
        CreateAndSeed(_ours);
        CreateAndSeed(_theirs);
        var clause = Clause(how, column);
        var source = Source(clause.Id, column, shape);

        var spark = Spark.InvokeRaw("sql", new
        {
            path = _theirs,
            sql = new[] { $"MERGE INTO delta.`{{path}}` t USING ({source.Sql}) s ON t.id = s.id {clause.Sql}" },
        });
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_ours)))
        {
            if (shape == "reordered")
            {
                Assert.True(spark.GetProperty("ok").GetBoolean(), spark.ToString());
                await table.MergeAsync([source.Batch], clause.Options);
            }
            else
            {
                Assert.False(spark.GetProperty("ok").GetBoolean(), "Spark accepted it");
                Assert.Contains(DeltaTableErrorCodes.UpdateSchemaMismatchExpression, spark.GetProperty("error").GetString());
                var e = await Assert.ThrowsAsync<DeltaFormatException>(() =>
                    table.MergeAsync([source.Batch], clause.Options).AsTask());
                Assert.Equal(DeltaTableErrorCodes.UpdateSchemaMismatchExpression, e.ErrorCode);
            }
        }

        Assert.Equal(SparkRows(_theirs), SparkRows(_ours));
    }
}
