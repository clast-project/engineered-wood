// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Schema evolution in MERGE (#604): <see cref="MergeOptions.MergeSchema"/>, and the source columns a MERGE without
/// it may carry. Each behaviour asserted here was measured against Spark 4.0 / Delta 4.0 first (with
/// <c>spark.databricks.delta.schema.autoMerge.enabled</c>; its SQL <c>MERGE WITH SCHEMA EVOLUTION</c> evolves
/// nothing there); <c>MergeSchemaEvolutionInteropTests</c> pins the main shapes against Spark itself.
/// </summary>
public class MergeSchemaEvolutionTests : IDisposable
{
    private readonly string _tempDir;

    public MergeSchemaEvolutionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_merge_evolution_{Guid.NewGuid():N}");
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
    private static Predicate P(string sql) => SparkSqlParser.ParsePredicate(sql);

    private static readonly Apache.Arrow.Schema Base = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, true))
        .Field(new Field("v", StringType.Default, true))
        .Build();

    private async Task<DeltaTable> CreateAsync(
        Apache.Arrow.Schema? schema = null, Dictionary<string, string>? configuration = null,
        params RecordBatch[] seed)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema ?? Base, configuration: configuration ?? []);
        if (seed.Length == 0)
            seed = [Batch(("id", Ints(1, 2)), ("v", Strings("a", "b")))];
        await table.WriteAsync(seed);
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

    private static IArrowArray Longs(params long?[] values)
    {
        var b = new Int64Array.Builder();
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

    /// <summary>The source rows 1 (matching) and 3 (not), with a column <c>extra</c> the table lacks.</summary>
    private static RecordBatch WithExtra() =>
        Batch(("id", Ints(1, 3)), ("v", Strings("x", "z")), ("extra", Ints(10, 30)));

    private static string Format(IArrowArray array, int i)
    {
        if (array.IsNull(i))
            return "null";
        return array switch
        {
            Int32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            StringArray a => a.GetString(i),
            StructArray s => "{" + string.Join(", ", ((StructType)s.Data.DataType).Fields.Select(
                (f, k) => f.Name + "=" + Format(s.Fields[k], i))) + "}",
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

    private static string Columns(DeltaTable table) => string.Join(", ", table.CurrentSnapshot.Schema.Fields.Select(
        f => $"{f.Name}:{Describe(f.Type)}{(f.Nullable ? "" : " not null")}"));

    private static string Describe(Schema.DeltaDataType type) => type switch
    {
        Schema.PrimitiveType p => p.TypeName,
        Schema.StructType s => "struct<" + string.Join(", ", s.Fields.Select(f => f.Name + ":" + Describe(f.Type))) + ">",
        Schema.ArrayType a => "array<" + Describe(a.ElementType) + ">",
        _ => type.ToString()!,
    };

    private static MergeOptions Upsert(bool mergeSchema) => new()
    {
        Keys = ["id"], WhenMatched = [MergeAction.Update], WhenNotMatched = [MergeAction.Insert], MergeSchema = mergeSchema,
    };

    // ── Without MergeSchema: a source column the table lacks is the source's own ──

    [Fact]
    public async Task WithoutMergeSchema_AnUpsert_LeavesOutASourceColumnTheTableLacks()
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], Upsert(mergeSchema: false));

        Assert.Equal("id:integer, v:string", Columns(table));
        Assert.Equal(["id=1, v=x", "id=2, v=b", "id=3, v=z"], await RowsAsync(table));
    }

    [Fact]
    public async Task WithoutMergeSchema_ASourceColumnTheTableLacks_MayBeRead()
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.UpdateSet(("v", E("concat(source.v, source.extra)"))).When(P("source.extra > 5"))],
        });

        Assert.Equal(["id=1, v=x10", "id=2, v=b"], await RowsAsync(table));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ASourceColumnDifferingFromATableColumnOnlyInCase_IsRefused(bool mergeSchema)
    {
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;

        var e = await Assert.ThrowsAnyAsync<Exception>(() => table.MergeAsync(
            [Batch(("id", Ints(1)), ("V", Strings("x")))], Upsert(mergeSchema)).AsTask());

        Assert.True(e is ArgumentException or DeltaFormatException, e.ToString());
        Assert.Contains("differs only in case", e.Message);

        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    // ── Which clauses evolve the schema ──

    [Fact]
    public async Task AnUpdateOfEveryColumn_AddsTheSourcesNewColumn()
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.Update], MergeSchema = true,
        });

        Assert.Equal("id:integer, v:string, extra:integer", Columns(table));
        Assert.Equal(["id=1, v=x, extra=10", "id=2, v=b, extra=null"], await RowsAsync(table));
    }

    [Fact]
    public async Task AnUpsert_AddsTheSourcesNewColumn_InOneCommitWithOneMetaData()
    {
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;

        var (metrics, version) = await table.MergeAsync([WithExtra()], Upsert(mergeSchema: true));

        Assert.Equal(before + 1, version);
        Assert.Equal(["id=1, v=x, extra=10", "id=2, v=b, extra=null", "id=3, v=z, extra=30"], await RowsAsync(table));
        Assert.Equal(1, metrics.NumTargetRowsMatchedUpdated);
        Assert.Equal(1, metrics.NumTargetRowsInserted);
        var commit = File.ReadAllLines(Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json"));
        Assert.Single(commit, l => l.StartsWith("{\"metaData\"", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AnInsertOfEveryColumn_AddsTheSourcesNewColumn()
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"], WhenNotMatched = [MergeAction.Insert], MergeSchema = true,
        });

        Assert.Equal(["id=1, v=a, extra=null", "id=2, v=b, extra=null", "id=3, v=z, extra=30"], await RowsAsync(table));
    }

    /// <summary>An assignment to a source column's name adds it with the SOURCE column's type, whatever the value's
    /// type: measured, <c>SET extra = 5</c> and <c>SET extra = s.extra + 1</c> both add an <c>int</c>.</summary>
    [Theory]
    [InlineData("source.extra + 1", "11")]
    [InlineData("5L", "5")]
    [InlineData("target.id * 100", "100")]
    public async Task AnAssignmentToASourceColumnsName_AddsItWithTheSourceColumnsType(string value, string written)
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("extra", E(value)))], MergeSchema = true,
        });

        Assert.Equal("id:integer, v:string, extra:integer", Columns(table));
        Assert.Equal([$"id=1, v=a, extra={written}", "id=2, v=b, extra=null"], await RowsAsync(table));
    }

    [Fact]
    public async Task AValueThatCannotBeCastToTheNewColumnsType_IsRefused()
    {
        // Measured: SET extra = concat('q', s.extra) adds an int column and fails to cast 'q10' into it.
        await using var table = await CreateAsync();

        var e = await Assert.ThrowsAsync<EngineeredWood.Expressions.Arrow.Spark.SparkEvaluationException>(() =>
            table.MergeAsync([WithExtra()], new MergeOptions
            {
                Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("extra", E("concat('q', source.extra)")))],
                MergeSchema = true,
            }).AsTask());

        Assert.Equal("CAST_INVALID_INPUT", e.ErrorClass);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AColumnListNamingASourceColumn_AddsIt(bool insert)
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = insert ? [] : [MergeAction.UpdateColumns("extra")],
            WhenNotMatched = insert ? [MergeAction.InsertColumns("id", "extra")] : [],
            MergeSchema = true,
        });

        Assert.Equal(
            insert
                ? ["id=1, v=a, extra=null", "id=2, v=b, extra=null", "id=3, v=null, extra=30"]
                : ["id=1, v=a, extra=10", "id=2, v=b, extra=null"],
            await RowsAsync(table));
    }

    public static TheoryData<string> Unevolving => ["condition", "delete", "other-column", "not-matched-by-source"];

    [Theory]
    [MemberData(nameof(Unevolving))]
    public async Task AClauseThatWritesNoNewSourceColumn_LeavesTheSchemaAlone(string shape)
    {
        await using var table = await CreateAsync();

        await table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = shape switch
            {
                "condition" => [MergeAction.UpdateSet(("v", E("source.v"))).When(P("source.extra > 5"))],
                "delete" => [MergeAction.Delete],
                "other-column" => [MergeAction.UpdateColumns("v")],
                _ => [],
            },
            WhenNotMatchedBySource = shape == "not-matched-by-source" ? [MergeAction.Delete] : [],
            MergeSchema = true,
        });

        Assert.Equal("id:integer, v:string", Columns(table));
    }

    [Fact]
    public async Task AnAssignmentToANameNeitherSideHas_IsRefused()
    {
        // Measured: SET newc = s.extra is DELTA_MERGE_UNRESOLVED_EXPRESSION; the name is not a source column, so
        // there is nothing to add.
        await using var table = await CreateAsync();

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([WithExtra()], new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("newc", E("source.extra")))], MergeSchema = true,
        }).AsTask());

        Assert.Equal(DeltaTableErrorCodes.ColumnNotFound, e.ErrorCode);
    }

    [Theory]
    [InlineData("never-matches")]
    [InlineData("no-rows")]
    public async Task ClausesNoRowReaches_StillEvolveTheSchema_InACommitOfItsOwn(string shape)
    {
        // Measured: Spark evolves at analysis, and commits metaData alone when no row changes.
        await using var table = await CreateAsync();
        long before = table.CurrentSnapshot.Version;
        var source = shape == "no-rows"
            ? Batch(("id", Ints()), ("v", Strings()), ("extra", Ints()))
            : WithExtra();

        var (_, version) = await table.MergeAsync([source], new MergeOptions
        {
            Keys = ["id"],
            WhenMatched = [MergeAction.Update.When(P("source.id > 100"))],
            WhenNotMatched = shape == "no-rows" ? [MergeAction.Insert] : [],
            MergeSchema = true,
        });

        Assert.Equal(before + 1, version);
        Assert.Equal("id:integer, v:string, extra:integer", Columns(table));
        Assert.Equal(["id=1, v=a, extra=null", "id=2, v=b, extra=null"], await RowsAsync(table));
        var commit = File.ReadAllLines(Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json"));
        Assert.DoesNotContain(commit, l => l.StartsWith("{\"add\"", StringComparison.Ordinal));
        Assert.Single(commit, l => l.StartsWith("{\"metaData\"", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NewColumns_AreAddedNullable_AtTheEnd_InTheSourcesOrder()
    {
        await using var table = await CreateAsync();
        var source = new RecordBatch(
            new Apache.Arrow.Schema(
                [new Field("b2", Int32Type.Default, false), new Field("v", StringType.Default, true),
                 new Field("id", Int32Type.Default, true), new Field("a1", Int32Type.Default, false)], null),
            [Ints(2), Strings("x"), Ints(1), Ints(1)], 1);

        await table.MergeAsync([source], Upsert(mergeSchema: true));

        Assert.Equal("id:integer, v:string, b2:integer, a1:integer", Columns(table));
    }

    // ── A table column the source lacks ──

    [Fact]
    public async Task ATableColumnTheSourceLacks_IsKeptByTheUpdate_AndNullInTheInsert()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("v", StringType.Default, true))
            .Field(new Field("w", Int32Type.Default, true))
            .Build();
        await using var table = await CreateAsync(
            schema, seed: Batch(("id", Ints(1, 2)), ("v", Strings("a", "b")), ("w", Ints(7, 8))));

        await table.MergeAsync([Batch(("id", Ints(1, 3)), ("v", Strings("x", "z")))], Upsert(mergeSchema: true));

        Assert.Equal(["id=1, v=x, w=7", "id=2, v=b, w=8", "id=3, v=z, w=null"], await RowsAsync(table));
    }

    [Fact]
    public async Task WithoutMergeSchema_ATableColumnTheSourceLacks_IsRefusedByAnUpsert()
    {
        await using var table = await CreateAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => table.MergeAsync(
            [Batch(("id", Ints(1, 3)))], Upsert(mergeSchema: false)).AsTask());
    }

    // ── Struct columns ──

    private static readonly Apache.Arrow.Schema Nested = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int32Type.Default, true))
        .Field(new Field("st", new StructType([new Field("a", Int32Type.Default, true), new Field("c", StringType.Default, true)]), true))
        .Build();

    private Task<DeltaTable> CreateNestedAsync() => CreateAsync(
        Nested, seed: Batch(("id", Ints(1, 2)), ("st", Struct(("a", Ints(1, 2)), ("c", Strings("c1", "c2"))))));

    [Fact]
    public async Task AStructsNewField_IsAdded_AndAFieldTheSourceLacks_IsKeptOrNull()
    {
        // Measured separately: a new field is added (NULL in rows the MERGE does not write), and a field the source
        // lacks keeps the target's value in an update and is NULL in an insert.
        await using var table = await CreateNestedAsync();
        var source = Batch(("id", Ints(1, 3)), ("st", Struct(("b", Strings("x", "z")), ("a", Ints(10, 30)))));

        await table.MergeAsync([source], Upsert(mergeSchema: true));

        Assert.Equal("id:integer, st:struct<a:integer, c:string, b:string>", Columns(table));
        Assert.Equal(
        [
            "id=1, st={a=10, c=c1, b=x}",
            "id=2, st={a=2, c=c2, b=null}",
            "id=3, st={a=30, c=null, b=z}",
        ], await RowsAsync(table));
    }

    [Fact]
    public async Task AStructNullInTheSource_StaysNull()
    {
        await using var table = await CreateNestedAsync();
        var inner = new StructType([new Field("a", Int32Type.Default, true)]);
        var st = new StructArray(inner, 1, [Ints(5)], new ArrowBuffer.BitmapBuilder().Append(false).Build(), nullCount: 1);
        var source = new RecordBatch(
            new Apache.Arrow.Schema([new Field("id", Int32Type.Default, true), new Field("st", inner, true)], null),
            [Ints(1), st], 1);

        await table.MergeAsync([source], Upsert(mergeSchema: true));

        Assert.Equal(["id=1, st=null", "id=2, st={a=2, c=c2}"], await RowsAsync(table));
    }

    [Fact]
    public async Task ANewFieldInsideAnArrayElement_IsRefused()
    {
        // Spark adds it; a mergeSchema write here refuses it, and so does the MERGE, before anything is written.
        var element = new StructType([new Field("a", Int32Type.Default, true)]);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("arr", new ListType(new Field("element", element, true)), true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema);
        long before = table.CurrentSnapshot.Version;
        var wider = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
        var values = new StructArray(wider, 1, [Ints(1), Strings("x")], ArrowBuffer.Empty);
        var list = new ListArray(
            new ListType(new Field("element", wider, true)), 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), values, ArrowBuffer.Empty);
        var source = new RecordBatch(
            new Apache.Arrow.Schema([new Field("id", Int32Type.Default, true), new Field("arr", list.Data.DataType, true)], null),
            [Ints(1), list], 1);

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() =>
            table.MergeAsync([source], Upsert(mergeSchema: true)).AsTask());

        Assert.Equal(DeltaTableErrorCodes.UnsupportedSchemaMerge, e.ErrorCode);
        Assert.Equal(before, table.CurrentSnapshot.Version);
    }

    // ── Types ──

    private static readonly Dictionary<string, string> TypeWidening = new() { ["delta.enableTypeWidening"] = "true" };

    private Task<DeltaTable> CreateNumbersAsync(Dictionary<string, string>? configuration) => CreateAsync(
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true)).Field(new Field("n", Int32Type.Default, true)).Build(),
        configuration, Batch(("id", Ints(1)), ("n", Ints(1))));

    [Fact]
    public async Task AWiderSourceColumn_WidensTheColumn_OnATableThatEnablesTypeWidening()
    {
        await using var table = await CreateNumbersAsync(TypeWidening);

        await table.MergeAsync([Batch(("id", Ints(1, 2)), ("n", Longs(5_000_000_000L, 7)))], Upsert(mergeSchema: true));

        Assert.Equal("id:integer, n:long", Columns(table));
        Assert.Equal(["id=1, n=5000000000", "id=2, n=7"], await RowsAsync(table));
    }

    /// <summary>
    /// Measured: the column is widened by the type of the SOURCE column of its name, not by the assigned value's.
    /// <c>SET n = 5000000000L</c> widens <c>n</c> when the source has a <c>long n</c>; <c>SET n = s.m</c> does not,
    /// and its value overflows the <c>int</c>.
    /// </summary>
    [Theory]
    [InlineData("n", "5000000000L", null)]
    [InlineData("m", "source.m", DeltaTableErrorCodes.CastOverflowInTableWrite)]
    public async Task AnAssignment_WidensByTheSourceColumnOfItsName(string sourceName, string value, string? error)
    {
        await using var table = await CreateNumbersAsync(TypeWidening);
        var options = new MergeOptions
        {
            Keys = ["id"], WhenMatched = [MergeAction.UpdateSet(("n", E(value)))], MergeSchema = true,
        };
        var source = Batch(("id", Ints(1)), (sourceName, Longs(5_000_000_000L)));

        if (error is null)
        {
            await table.MergeAsync([source], options);
            Assert.Equal("id:integer, n:long", Columns(table));
            Assert.Equal(["id=1, n=5000000000"], await RowsAsync(table));
        }
        else
        {
            var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync([source], options).AsTask());
            Assert.Equal(error, e.ErrorCode);
            Assert.Equal("id:integer, n:integer", Columns(table));
        }
    }

    [Fact]
    public async Task AWiderSourceColumn_WithoutTypeWidening_IsRefused()
    {
        // Spark casts it, overflowing only on a value the column cannot hold; a copied column here must already have
        // the table's type, so the MERGE is refused, saying how to widen.
        await using var table = await CreateNumbersAsync(null);

        var e = await Assert.ThrowsAsync<DeltaFormatException>(() => table.MergeAsync(
            [Batch(("id", Ints(1)), ("n", Longs(5)))], Upsert(mergeSchema: true)).AsTask());

        Assert.Equal(DeltaTableErrorCodes.MergeIncompatibleDataType, e.ErrorCode);
    }

    // ── Change feed and column mapping ──

    [Fact]
    public async Task TheChangeFeed_CarriesTheNewColumn()
    {
        await using var table = await CreateAsync(configuration: new() { [CdfConfig.EnableKey] = "true" });

        var (_, version) = await table.MergeAsync([WithExtra()], Upsert(mergeSchema: true));

        var feed = new List<string>();
        await foreach (var batch in table.ReadChangesAsync(
            new DeltaChangeReadOptions { StartVersion = version, EndVersion = version }))
        {
            for (int i = 0; i < batch.Length; i++)
            {
                feed.Add($"{Format(batch.Column("_change_type"), i)} id={Format(batch.Column("id"), i)} "
                    + $"extra={Format(batch.Column("extra"), i)}");
            }
        }
        feed.Sort(StringComparer.Ordinal);
        Assert.Equal(
            ["insert id=3 extra=30", "update_postimage id=1 extra=10", "update_preimage id=1 extra=null"], feed);
    }

    [Fact]
    public async Task UnderColumnMapping_TheNewColumnGetsAnIdAndAPhysicalName()
    {
        await using var table = await CreateAsync(configuration: new()
        {
            ["delta.columnMapping.mode"] = "name", ["delta.minReaderVersion"] = "2", ["delta.minWriterVersion"] = "5",
        });

        await table.MergeAsync([WithExtra()], Upsert(mergeSchema: true));

        var extra = table.CurrentSnapshot.Schema.Fields.Single(f => f.Name == "extra");
        Assert.True(extra.Metadata!.ContainsKey(Schema.ColumnMapping.FieldIdKey));
        Assert.True(extra.Metadata.ContainsKey(Schema.ColumnMapping.PhysicalNameKey));
        Assert.Equal("3", table.CurrentSnapshot.Metadata.Configuration![Schema.ColumnMapping.MaxColumnIdKey]);
        Assert.Equal(["id=1, v=x, extra=10", "id=2, v=b, extra=null", "id=3, v=z, extra=30"], await RowsAsync(table));
    }

    // ── In a transaction ──

    [Fact]
    public async Task InATransaction_AMergingWriteAndAMergingMerge_Compose()
    {
        await using var table = await CreateAsync();
        await using var txn = table.StartTransaction();

        await txn.WriteAsync([Batch(("id", Ints(5)), ("v", Strings("e")), ("w", Strings("w5")))], mergeSchema: true);
        await txn.MergeAsync([WithExtra()], Upsert(mergeSchema: true));
        long version = await txn.CommitAsync();

        Assert.Equal("id:integer, v:string, w:string, extra:integer", Columns(table));
        Assert.Equal(
        [
            "id=1, v=x, w=null, extra=10",
            "id=2, v=b, w=null, extra=null",
            "id=3, v=z, w=null, extra=30",
            "id=5, v=e, w=w5, extra=null",
        ], await RowsAsync(table));
        var commit = File.ReadAllLines(Path.Combine(_tempDir, "_delta_log", $"{version:D20}.json"));
        Assert.Single(commit, l => l.StartsWith("{\"metaData\"", StringComparison.Ordinal));
    }

    [Fact]
    public async Task InATransaction_AMergingMerge_AfterAnotherStagedSchemaChange_IsRefused()
    {
        await using var table = await CreateAsync();
        await using var txn = table.StartTransaction();
        txn.StageSchemaChange(table.ComputeAddColumn(new Field("other", Int32Type.Default, true)));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            txn.MergeAsync([WithExtra()], Upsert(mergeSchema: true)).AsTask());
    }
}
