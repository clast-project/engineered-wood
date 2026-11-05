// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Data.SqlTypes;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Checkpoint;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Skipping files with a variant column's per-path <c>minValues</c> / <c>maxValues</c> (#677): a
/// <c>variant_get</c> predicate is answered from the bound of its path, when the bound's type converts to the
/// predicate's type exactly, and never from a path the file did not bound.
/// </summary>
public class VariantPruningTests : IDisposable
{
    private readonly string _tempDir;

    public VariantPruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_prune_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ── Against hand-made bounds ─────────────────────────────────────────────────────────────────────

    private static readonly EngineeredWood.DeltaLake.Schema.StructType Schema = new()
    {
        Fields =
        [
            new StructField { Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = true },
            new StructField { Name = "v", Type = new PrimitiveType { TypeName = "variant" }, Nullable = true },
        ],
    };

    private static string Z85(params (string Path, VariantValue Value)[] paths)
    {
        var (metadata, value) = new VariantBuilder().Encode(
            VariantValue.FromObject(paths.ToDictionary(p => p.Path, p => p.Value)));
        return VariantStatsEncoding.Encode(metadata, value);
    }

    private static AddFile Add(string min, string max, long nullCount = 0, long numRecords = 10) => new()
    {
        Path = "part-0.parquet",
        PartitionValues = new Dictionary<string, string>(),
        Size = 1,
        ModificationTime = 0,
        DataChange = true,
        Stats = $"{{\"numRecords\":{numRecords},\"minValues\":{{\"id\":0,\"v\":\"{min}\"}},"
            + $"\"maxValues\":{{\"id\":9,\"v\":\"{max}\"}},\"nullCount\":{{\"id\":0,\"v\":{nullCount}}}}}",
    };

    // $.a in [10, 20] (int32), $.s in ["p", "q"], $.c.d in [8.5, 9.5] (double).
    private static readonly AddFile File = Add(
        Z85(("$['a']", VariantValue.FromInt32(10)), ("$['s']", VariantValue.FromString("p")),
            ("$['c']['d']", VariantValue.FromDouble(8.5))),
        Z85(("$['a']", VariantValue.FromInt32(20)), ("$['s']", VariantValue.FromString("q")),
            ("$['c']['d']", VariantValue.FromDouble(9.5))));

    private static bool Keeps(AddFile file, Predicate filter) =>
        new DeltaFilePruner(Schema, []).ShouldInclude(file, filter);

    private static bool Keeps(Predicate filter) => Keeps(File, filter);

    private static bool Keeps(string sql) => Keeps(SparkSqlParser.ParsePredicate(sql));

    private static Expression A => Ex.VariantGet("v", "$.a", "int");

    [Fact]
    public void AComparisonOutsideThePathsBounds_SkipsTheFile()
    {
        Assert.False(Keeps(Ex.GreaterThan(A, Ex.Lit(20))));
        Assert.False(Keeps(Ex.LessThan(A, Ex.Lit(10))));
        Assert.False(Keeps(Ex.Equal(A, Ex.Lit(25))));
        Assert.False(Keeps(Ex.In(A, [LiteralValue.Of(1), LiteralValue.Of(30)])));
    }

    [Fact]
    public void AComparisonTheBoundsAdmit_KeepsTheFile()
    {
        Assert.True(Keeps(Ex.GreaterThan(A, Ex.Lit(19))));
        Assert.True(Keeps(Ex.Equal(A, Ex.Lit(10))));
        Assert.True(Keeps(Ex.LessThanOrEqual(A, Ex.Lit(10))));
        Assert.True(Keeps(Ex.In(A, [LiteralValue.Of(1), LiteralValue.Of(15)])));
    }

    [Fact]
    public void EverySpellingOfTheExtraction_Prunes()
    {
        Assert.False(Keeps("variant_get(v, '$.a', 'int') > 20"));
        Assert.False(Keeps("try_variant_get(v, '$.a', 'INT') > 20"));
        Assert.False(Keeps("variant_get(v, '$[\"a\"]', 'bigint') > 20"));
        Assert.False(Keeps("v:a::int > 20"));
        Assert.False(Keeps("cast(v:a AS int) > 20"));
        Assert.False(Keeps("try_cast(v:['a'] AS long) > 20"));
        Assert.False(Keeps("25 = v:a::int"));
        // (Below the minimum: a NaN row, which no Delta count rules out, would answer TRUE above the maximum.)
        Assert.False(Keeps("v:c.d::double < 8.0D"));
        Assert.False(Keeps("v:c:d::double < 8.0D"));
        Assert.False(Keeps("variant_get(v, '$.c', 'variant'):d::double < 8.0D"));
        Assert.False(Keeps("variant_get(variant_get(v, '$.c'), '$.d', 'double') < 8.0D"));
        Assert.False(Keeps("v:s::string > 'q'"));
        Assert.False(Keeps("v:s::string = 'r'"));
        Assert.True(Keeps("v:s::string = 'p'"));
    }

    [Fact]
    public void APathTheFileDidNotBound_KeepsIt()
    {
        Assert.True(Keeps("v:b::int > 20"));
        Assert.True(Keeps("v:A::int > 20")); // keys are case-sensitive
        Assert.True(Keeps("v:c::int > 20")); // an object, not a leaf
        Assert.True(Keeps("v:a[0]::int > 20")); // an element: never bounded
        Assert.True(Keeps("v:a.x::int > 20"));
        Assert.True(Keeps("variant_get(v, '$. a', 'int') > 20")); // the key " a"
    }

    [Fact]
    public void AVariantResult_IsNotCompared()
    {
        // No type: what a variant compares as is not a bound's business.
        Assert.True(Keeps(Ex.GreaterThan(Ex.Call("variant_get", Ex.Ref("v"), Ex.Lit("$.a")), Ex.Lit(20))));
    }

    [Fact]
    public void ABoundOfAnotherType_AnswersOnlyAnExactConversion()
    {
        // A string bound does not answer int, though Spark converts "7" to 7 (and orders "10" below "9").
        Assert.True(Keeps("v:s::int > 20"));
        // An int bound does not answer string, whose order is not the number's.
        Assert.True(Keeps("v:a::string > '3'"));
        // Nor double, a lossy conversion this does not reason about; a double bound does not answer int.
        Assert.True(Keeps("v:a::double > 30.0D"));
        Assert.True(Keeps("v:c.d::int > 30"));
        Assert.True(Keeps("v:c.d::float > 30.0F"));
        // An int bound answers every integer type it fits, and a decimal one.
        Assert.False(Keeps("v:a::tinyint > 20"));
        Assert.False(Keeps("v:a::smallint > 20"));
        Assert.False(Keeps("v:a::bigint > 20"));
        Assert.False(Keeps("v:a::decimal(5,2) > 20.5"));
        Assert.True(Keeps("v:a::decimal(3,2) > 20.5")); // 20.00 does not fit: those rows fail to convert
    }

    [Fact]
    public void AnIntegerBoundOutsideTheTargetsRange_KeepsTheFile()
    {
        // The rows above 127 fail to convert: variant_get raises on them, try_variant_get makes them NULL.
        var wide = Add(Z85(("$['a']", VariantValue.FromInt64(1))), Z85(("$['a']", VariantValue.FromInt64(300))));
        Assert.True(Keeps(wide, SparkSqlParser.ParsePredicate("v:a::tinyint > 127")));
        Assert.True(Keeps(wide, SparkSqlParser.ParsePredicate("try_variant_get(v, '$.a', 'tinyint') > 127")));
        Assert.False(Keeps(wide, SparkSqlParser.ParsePredicate("v:a::smallint > 300")));
    }

    [Fact]
    public void ADecimalBound_AnswersADecimalOfAtLeastItsScale()
    {
        var file = Add(Z85(("$['d']", VariantValue.FromDecimal4(1.25m))), Z85(("$['d']", VariantValue.FromDecimal4(3.50m))));
        Assert.False(Keeps(file, SparkSqlParser.ParsePredicate("v:d::decimal(9,2) > 3.5")));
        Assert.False(Keeps(file, SparkSqlParser.ParsePredicate("v:d::decimal(10,4) < 1.2")));
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("v:d::decimal(9,2) > 3.49")));
        // Scale 1 rounds 1.25 and 3.50's neighbours: no answer from the bounds.
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("v:d::decimal(9,1) > 3.5")));
        // Nor from a precision the bound overflows.
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("v:d::decimal(2,2) > 0.5")));

        var wide = Add(
            Z85(("$['d']", VariantValue.FromDecimal16(SqlDecimal.Parse("-12345678901234567890.5")))),
            Z85(("$['d']", VariantValue.FromDecimal16(SqlDecimal.Parse("12345678901234567890.5")))));
        Assert.False(Keeps(wide, SparkSqlParser.ParsePredicate("v:d::decimal(30,1) > 12345678901234567890.5")));
        Assert.True(Keeps(wide, SparkSqlParser.ParsePredicate("v:d::decimal(30,1) > 12345678901234567890.4")));
    }

    [Fact]
    public void DatesAndTimestamps_PruneAtTheirOwnType()
    {
        var day = new DateTime(2026, 11, 4);
        var file = Add(
            Z85(("$['dt']", VariantValue.FromDate(day)),
                ("$['ts']", VariantValue.FromTimestamp(new DateTimeOffset(day, TimeSpan.Zero))),
                ("$['ntz']", VariantValue.FromTimestampNtz(day))),
            Z85(("$['dt']", VariantValue.FromDate(day.AddDays(2))),
                ("$['ts']", VariantValue.FromTimestamp(new DateTimeOffset(day.AddHours(1), TimeSpan.Zero))),
                ("$['ntz']", VariantValue.FromTimestampNtz(day.AddHours(1)))));

        Assert.False(Keeps(file, Ex.GreaterThan(Ex.VariantGet("v", "$.dt", "date"),
            Ex.Lit(LiteralValue.Of(new DateTimeOffset(day.AddDays(2), TimeSpan.Zero))))));
        Assert.True(Keeps(file, Ex.GreaterThan(Ex.VariantGet("v", "$.dt", "date"),
            Ex.Lit(LiteralValue.Of(new DateTimeOffset(day.AddDays(1), TimeSpan.Zero))))));
        Assert.False(Keeps(file, Ex.LessThan(Ex.VariantGet("v", "$.ts", "timestamp"),
            Ex.Lit(LiteralValue.Of(new DateTimeOffset(day, TimeSpan.Zero))))));
        Assert.False(Keeps(file, Ex.GreaterThan(Ex.VariantGet("v", "$.ntz", "timestamp_ntz"),
            Ex.Lit(LiteralValue.Of(new DateTimeOffset(day.AddHours(2), TimeSpan.Zero))))));
        // A zoned bound does not answer timestamp_ntz (the session zone decides that conversion), nor the reverse.
        Assert.True(Keeps(file, Ex.LessThan(Ex.VariantGet("v", "$.ts", "timestamp_ntz"),
            Ex.Lit(LiteralValue.Of(new DateTimeOffset(day, TimeSpan.Zero))))));
        Assert.True(Keeps(file, Ex.LessThan(Ex.VariantGet("v", "$.ntz", "timestamp"),
            Ex.Lit(LiteralValue.Of(new DateTimeOffset(day, TimeSpan.Zero))))));
    }

    [Fact]
    public void ATruncatedStringMaximum_StillBounds()
    {
        var file = Add(Z85(("$['s']", VariantValue.FromString("abcd"))), Z85(("$['s']", VariantValue.FromString("abzz\U0010FFFF"))));
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("v:s::string = 'abzzzzzzzz'")));
        Assert.False(Keeps(file, SparkSqlParser.ParsePredicate("v:s::string = 'ac'")));
        Assert.False(Keeps(file, SparkSqlParser.ParsePredicate("v:s::string < 'abc'")));
    }

    [Fact]
    public void AFloatBound_PrunesOnlyWhatAHiddenNaNCouldNotMatch()
    {
        // Delta keeps no NaN count, so a NaN row may sit outside a float bound; NaN sorts above every number.
        Assert.False(Keeps("v:c.d::double < 8.0D"));
        Assert.False(Keeps("v:c.d::double = 10.0D"));
        Assert.True(Keeps("v:c.d::double > 10.0D"));
    }

    [Fact]
    public void NegationAndOr_FollowTheRowsTheBoundsDoNotDescribe()
    {
        // Rows without the path answer NULL, so NOT (a > 20) is not TRUE for them, and no bound proves it TRUE.
        Assert.True(Keeps("NOT (v:a::int > 20)"));
        Assert.True(Keeps("NOT (v:a::int <= 20)"));
        Assert.True(Keeps("v:a::int != 15"));
        Assert.False(Keeps("v:a::int > 20 OR v:s::string > 'q'"));
        Assert.True(Keeps("v:a::int > 20 OR v:s::string = 'q'"));
        Assert.False(Keeps("v:a::int > 20 AND id = 5"));
        Assert.True(Keeps("v:a::int IS NULL"));
        Assert.True(Keeps("v:a::int IS NOT NULL"));
    }

    [Fact]
    public void AnAllNullVariantColumn_AnswersNullForEveryPath()
    {
        var file = Add(Z85(), Z85(), nullCount: 10, numRecords: 10);
        Assert.False(Keeps(file, SparkSqlParser.ParsePredicate("v:anything::int = 1")));
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("v:anything::int IS NULL")));
    }

    [Fact]
    public void BoundsThatDoNotDecode_KeepTheFile()
    {
        Assert.True(Keeps(Add("garbage", "garbage"), SparkSqlParser.ParsePredicate("v:a::int > 20")));
        Assert.True(Keeps(Add("0rr9300000", "0rr9300000"), SparkSqlParser.ParsePredicate("v:a::int > 20"))); // {}
        // A bound that is not an object.
        string scalar = Encode(VariantValue.FromInt32(1));
        Assert.True(Keeps(Add(scalar, scalar), SparkSqlParser.ParsePredicate("v:a::int > 20")));

        static string Encode(VariantValue value)
        {
            var (metadata, bytes) = new VariantBuilder().Encode(value);
            return VariantStatsEncoding.Encode(metadata, bytes);
        }
    }

    [Fact]
    public void AColumnThatIsNotAVariant_IsNotReadAsOne()
    {
        Assert.True(Keeps(SparkSqlParser.ParsePredicate("variant_get(id, '$.a', 'int') > 20")));
        Assert.True(Keeps(SparkSqlParser.ParsePredicate("variant_get(missing, '$.a', 'int') > 20")));
    }

    [Fact]
    public void TheRootPath_IsBoundedUnderDollar()
    {
        var file = Add(Z85(("$", VariantValue.FromInt64(1))), Z85(("$", VariantValue.FromInt64(5))));
        Assert.False(Keeps(file, SparkSqlParser.ParsePredicate("variant_get(v, '$', 'bigint') > 5")));
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("variant_get(v, '$', 'bigint') > 4")));
    }

    [Fact]
    public void AnEscapedKey_IsLookedUpUnderItsNormalizedSpelling()
    {
        var file = Add(Z85(("$['it''s']", VariantValue.FromInt32(1))), Z85(("$['it''s']", VariantValue.FromInt32(2))));
        // Not the spelling: Spark normalizes ' to \' (VariantStatsEncoding.NormalizedPath).
        Assert.True(Keeps(file, SparkSqlParser.ParsePredicate("v:`it's`::int > 5")));

        string key = VariantStatsEncoding.NormalizedPath(["it's"]);
        var escaped = Add(Z85((key, VariantValue.FromInt32(1))), Z85((key, VariantValue.FromInt32(2))));
        Assert.False(Keeps(escaped, SparkSqlParser.ParsePredicate("v:`it's`::int > 5")));
        Assert.False(Keeps(escaped, SparkSqlParser.ParsePredicate("variant_get(v, '$.it\\'s', 'int') > 5")));
        // In Spark SQL 'it''s' is two literals, concatenated: the key "its".
        Assert.True(Keeps(escaped, SparkSqlParser.ParsePredicate("variant_get(v, '$.it''s', 'int') > 5")));
    }

    // ── Written by EW, read back ─────────────────────────────────────────────────────────────────────

    private static Apache.Arrow.Schema ArrowSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    private static RecordBatch Batch(long firstId, params VariantValue?[] values)
    {
        var ids = new Int64Array.Builder();
        var vb = new VariantArray.Builder();
        for (int i = 0; i < values.Length; i++)
        {
            ids.Append(firstId + i);
            if (values[i] is { } v)
                vb.Append(v);
            else
                vb.AppendNull();
        }
        return new RecordBatch(ArrowSchema(), [ids.Build(), vb.Build(allocator: null)], values.Length);
    }

    private static VariantValue Obj(params (string Key, VariantValue Value)[] fields) =>
        VariantValue.FromObject(fields.ToDictionary(f => f.Key, f => f.Value));

    private static async Task<List<long>> IdsAsync(DeltaTable table, Predicate filter)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = filter }))
        {
            var column = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>
    /// Three files: `a` in [1, 3], `a` in [10, 12] with rows that have no `a` or are not objects, and one
    /// where `a` is sometimes a string (so unbounded).
    /// </summary>
    private async Task<DeltaTable> ThreeFilesAsync()
    {
        var options = DeltaTableOptions.Default with
        {
            // An explicit schema: inference shreds nothing in a file mixing objects and scalars.
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                VariantShredSchemas = new Dictionary<string, ShredSchema>
                {
                    ["v"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        ["a"] = ShredSchema.Primitive(ShredType.Int64),
                    }),
                },
            },
        };
        var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), ArrowSchema(),
            configuration: new Dictionary<string, string> { ["delta.enableVariantShredding"] = "true" }, options: options);
        await table.WriteAsync([Batch(0, Obj(("a", VariantValue.FromInt64(1))), Obj(("a", VariantValue.FromInt64(3))))]);
        await table.WriteAsync([Batch(10, Obj(("a", VariantValue.FromInt64(10))), Obj(("a", VariantValue.FromInt64(12))),
            Obj(("b", VariantValue.FromString("no a"))), VariantValue.FromString("not an object"), null)]);
        await table.WriteAsync([Batch(20, Obj(("a", VariantValue.FromInt64(2))), Obj(("a", VariantValue.FromString("x"))))]);
        return table;
    }

    [Fact]
    public async Task AScan_SkipsTheFilesWhoseBoundsExcludeThePredicate()
    {
        await using var table = await ThreeFilesAsync();

        // The filter prunes files only: a kept file's rows all come back.
        Assert.Equal([10, 11, 12, 13, 14, 20, 21], await IdsAsync(table, SparkSqlParser.ParsePredicate("v:a::bigint > 5")));
        Assert.Equal([0, 1, 20, 21], await IdsAsync(table, SparkSqlParser.ParsePredicate("v:a::int < 5")));
        Assert.Equal([20, 21], await IdsAsync(table, SparkSqlParser.ParsePredicate("v:a::bigint = 7")));
        // A path no file bounds keeps them all.
        Assert.Equal(7 + 2, (await IdsAsync(table, SparkSqlParser.ParsePredicate("v:b::string = 'zzz'"))).Count);
    }

    [Fact]
    public async Task AScanFromACheckpoint_PrunesFromStatsParsed()
    {
        await using (var table = await ThreeFilesAsync())
            await table.CheckpointAsync();

        // Answered from the typed copy of the bounds: check that the checkpoint carries them, then that it prunes.
        var reader = new CheckpointReader(new LocalTableFileSystem(_tempDir));
        var adds = (await reader.ReadCheckpointAsync((await reader.ReadLastCheckpointAsync())!)).OfType<AddFile>().ToList();
        Assert.Contains(adds, a => a.TypedStats?.View.HasBound("v") == true);
        var pruner = new DeltaFilePruner(
            (await Snapshot.SnapshotBuilder.BuildAsync(new TransactionLog(new LocalTableFileSystem(_tempDir)))).Schema, []);
        var filter = SparkSqlParser.ParsePredicate("v:a::bigint > 5");
        Assert.Equal(2, adds.Count(a => pruner.ShouldInclude(a with { Stats = null }, filter)));

        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        Assert.Equal([10, 11, 12, 13, 14, 20, 21], await IdsAsync(reopened, filter));
    }
}
