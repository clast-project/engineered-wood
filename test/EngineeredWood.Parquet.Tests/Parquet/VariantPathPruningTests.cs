// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Tests.Parquet.Interop;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Row-group and page pruning on <c>variant_get</c> predicates, by the statistics of the shredded
/// <c>typed_value</c> column that holds the path (#682).
/// </summary>
/// <remarks>
/// Every case asserts the units it expects kept and also that no row the row evaluator matches lies in a
/// unit the pruning dropped, so a wrong bound fails twice.
/// </remarks>
public class VariantPathPruningTests : IDisposable
{
    private readonly string _tempDir;

    public VariantPathPruningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-variantprune-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    // ───── Fixture ─────

    private static readonly ShredSchema Shape = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
    {
        ["a"] = ShredSchema.Primitive(ShredType.Int64),
        ["d"] = ShredSchema.Primitive(ShredType.Double),
        ["o"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["x"] = ShredSchema.Primitive(ShredType.Int32),
        }),
        ["s"] = ShredSchema.Primitive(ShredType.String),
    });

    private static VariantValue Row(long a, double? d = null, int? x = null) =>
        VariantValue.FromObject(new Dictionary<string, VariantValue>
        {
            ["a"] = VariantValue.FromInt64(a),
            ["d"] = VariantValue.FromDouble(d ?? a),
            ["o"] = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                ["x"] = VariantValue.FromInt32(x ?? (int)a),
            }),
            ["s"] = VariantValue.FromString("s" + a.ToString("D4", System.Globalization.CultureInfo.InvariantCulture)),
        });

    /// <summary>A row whose <c>a</c> is a string: the residual of <c>a</c> holds it.</summary>
    private static VariantValue Stray() => VariantValue.FromObject(new Dictionary<string, VariantValue>
    {
        ["a"] = VariantValue.FromString("oops"),
    });

    /// <summary>A row without the path <c>a</c>.</summary>
    private static VariantValue NoA() => VariantValue.FromObject(new Dictionary<string, VariantValue>
    {
        ["s"] = VariantValue.FromString("only s"),
    });

    private static RecordBatch Batch(IEnumerable<VariantValue?> values)
    {
        var builder = new VariantArray.Builder();
        int count = 0;
        foreach (var v in values)
        {
            if (v is { } value)
                builder.Append(value);
            else
                builder.AppendNull();
            count++;
        }
        var array = builder.Build(allocator: null);
        var schema = new Apache.Arrow.Schema(
            new[] { new Field("v", array.Data.DataType, nullable: true) }, metadata: null);
        return new RecordBatch(schema, new IArrowArray[] { array }, count);
    }

    private static IEnumerable<VariantValue?> Range(int from, int to)
    {
        for (int i = from; i < to; i++)
            yield return Row(i);
    }

    private async Task<string> WriteAsync(int? pageRows, params RecordBatch[] rowGroups)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        var options = ParquetWriteOptions.Default with
        {
            VariantShredSchemas = new Dictionary<string, ShredSchema> { ["v"] = Shape },
            DataPageRowCountLimit = pageRows,
        };
        await using var file = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
        foreach (var batch in rowGroups)
            await writer.WriteRowGroupAsync(batch);
        await writer.CloseAsync();
        return path;
    }

    /// <summary>
    /// Four row groups: <c>a</c> in [0, 100), in [100, 200), in [200, 300) with one stray string, and a
    /// group where no row has <c>a</c> (objects without it and NULL variants).
    /// </summary>
    private Task<string> FourRowGroupsAsync() => WriteAsync(
        pageRows: null,
        Batch(Range(0, 100)),
        Batch(Range(100, 200)),
        Batch(Range(200, 250).Append(Stray()).Concat(Range(251, 300))),
        Batch(Enumerable.Range(0, 50).Select(i => i % 2 == 0 ? NoA() : (VariantValue?)null)));

    private static ParquetFileReader Open(string path, out LocalRandomAccessFile file)
    {
        file = new LocalRandomAccessFile(path);
        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        return new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ExtensionRegistry = registry });
    }

    /// <summary>Which rows match, by the row evaluator; null for a row whose evaluation fails.</summary>
    private static async Task<List<bool?>> MatchesAsync(ParquetFileReader reader, int rowGroup, Predicate predicate)
    {
        using var batch = await reader.ReadRowGroupAsync(rowGroup);
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());
        var result = new List<bool?>();
        try
        {
            var matches = evaluator.EvaluatePredicate(predicate, batch);
            for (int i = 0; i < matches.Length; i++)
                result.Add(matches.GetValue(i) == true);
        }
        catch (Exception)
        {
            // A conversion error (the stray string under variant_get): the group must then be kept.
            for (int i = 0; i < batch.Length; i++)
                result.Add(null);
        }
        return result;
    }

    private static async Task<bool[]> CandidatesAsync(string path, string sql)
    {
        using var reader = Open(path, out var file);
        await using var _ = file;
        var predicate = SparkSqlParser.ParsePredicate(sql);
        var candidates = await reader.GetCandidateRowGroupsAsync(predicate);
        var meta = await reader.ReadMetadataAsync();

        var kept = new bool[meta.RowGroups.Count];
        for (int g = 0; g < kept.Length; g++)
        {
            kept[g] = candidates[g];
            if (kept[g])
                continue;
            var matches = await MatchesAsync(reader, g, predicate);
            Assert.True(matches.All(m => m == false),
                $"row group {g} was pruned for {sql} but holds a matching row, or one whose evaluation fails");
        }
        return kept;
    }

    // ───── Row groups ─────

    [Theory]
    [InlineData("variant_get(v, '$.a', 'bigint') < 50", new[] { true, false, true, false })]
    [InlineData("variant_get(v, '$.a', 'bigint') >= 150", new[] { false, true, true, false })]
    [InlineData("variant_get(v, '$.a', 'bigint') = 120", new[] { false, true, true, false })]
    [InlineData("try_variant_get(v, '$.a', 'bigint') > 290", new[] { false, false, true, false })]
    [InlineData("variant_get(v, '$.a', 'int') < 50", new[] { true, false, true, false })]
    [InlineData("v:a::bigint < 50", new[] { true, false, true, false })]
    [InlineData("variant_get(v, '$.a', 'decimal(10,2)') < 50.5", new[] { true, false, true, false })]
    [InlineData("variant_get(v, '$.o.x', 'int') > 180", new[] { false, true, true, false })]
    // Group 3's objects without a still have s, "only s", which sorts below "s0050".
    [InlineData("variant_get(v, '$.s', 'string') < 's0050'", new[] { true, false, false, true })]
    // The stray row of group 2 has no d, so d's residual is null there and the group prunes.
    [InlineData("variant_get(v, '$.d', 'double') < 5.0E1", new[] { true, false, false, false })]
    public async Task RowGroups_PrunedByTheTypedValueBounds(string sql, bool[] expected)
    {
        string path = await FourRowGroupsAsync();
        Assert.Equal(expected, await CandidatesAsync(path, sql));
    }

    /// <summary>
    /// Group 2 holds a stray string at <c>$.a</c>, in its residual. Its <c>typed_value</c> bounds say
    /// [200, 299], but they describe only the typed rows, and <c>variant_get</c> raises on the string: the
    /// group is kept for a predicate its bounds alone would rule out. The <c>$.s</c> path in the same
    /// group has no stray value and prunes it.
    /// </summary>
    [Fact]
    public async Task RowGroup_WithAStrayTypeAtThePath_IsKept()
    {
        string path = await FourRowGroupsAsync();
        Assert.True((await CandidatesAsync(path, "variant_get(v, '$.a', 'bigint') < 10"))[2]);
        Assert.False((await CandidatesAsync(path, "variant_get(v, '$.s', 'string') < 's0010'"))[2]);
    }

    [Theory]
    // No row of group 0 or 1 is without a, so the result is never NULL there; every row of group 3 is.
    [InlineData("variant_get(v, '$.a', 'bigint') IS NULL", new[] { false, false, true, true })]
    [InlineData("variant_get(v, '$.a', 'bigint') IS NOT NULL", new[] { true, true, true, false })]
    // A type the path's values do not convert to: no bounds, and no null count (try_ answers NULL for every
    // row, which a count of zero would contradict).
    [InlineData("try_variant_get(v, '$.a', 'string') IS NULL", new[] { true, true, true, true })]
    [InlineData("try_variant_get(v, '$.a', 'string') = 'x'", new[] { true, true, true, false })]
    public async Task RowGroups_NullCountsOfTheResult(string sql, bool[] expected)
    {
        string path = await FourRowGroupsAsync();
        Assert.Equal(expected, await CandidatesAsync(path, sql));
    }

    [Theory]
    // A path the file does not shred, an array element, a variant result, and types the bounds do not
    // convert to: a string or boolean from an integer, and a tinyint from integers past its range.
    [InlineData("variant_get(v, '$.missing', 'bigint') < 0")]
    [InlineData("variant_get(v, '$.a[0]', 'bigint') < 0")]
    [InlineData("variant_get(v, '$.a') IS NULL")]
    [InlineData("try_variant_get(v, '$.a', 'string') = 'x'")]
    [InlineData("variant_get(v, '$.a', 'boolean') = true")]
    public async Task RowGroups_NotBounded_AreKept(string sql)
    {
        string path = await FourRowGroupsAsync();
        bool[] kept = await CandidatesAsync(path, sql);
        // Group 3 has no a at all; only a path that resolves could prune it, so check groups 0..2.
        Assert.True(kept[0] && kept[1] && kept[2], string.Join(",", kept));
    }

    /// <summary>
    /// A tinyint bound exists only where both ends fit: group 0's [0, 99] does and prunes for <c>&lt; 0</c>;
    /// group 1's [100, 199] does not, so the group is kept rather than pruned on a value that would not convert.
    /// </summary>
    [Fact]
    public async Task RowGroups_NarrowTarget_BoundsOnlyWhereBothEndsFit()
    {
        string path = await FourRowGroupsAsync();
        bool[] kept = await CandidatesAsync(path, "try_variant_get(v, '$.a', 'tinyint') < 0");
        Assert.False(kept[0]);
        Assert.True(kept[1]);
    }

    /// <summary>
    /// A NaN sits above every number in SQL's order, and Parquet leaves it out of min/max. The writer counts
    /// it, so the group with one is kept for <c>&gt;</c> and the group without can still be pruned.
    /// </summary>
    [Fact]
    public async Task DoublePath_KeepsAGroupWithANaN_AndPrunesOneWithout()
    {
        string path = await WriteAsync(
            pageRows: null,
            Batch(Range(0, 10)),
            Batch(Range(10, 19).Append(Row(19, d: double.NaN))));

        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, "variant_get(v, '$.d', 'double') > 1.0E2"));
        Assert.Equal(new[] { false, false }, await CandidatesAsync(path, "variant_get(v, '$.d', 'double') < -1.0E0"));
    }

    [Fact]
    public async Task Filter_SkipsTheRowGroupsItRulesOut()
    {
        string path = await FourRowGroupsAsync();
        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions
        {
            ExtensionRegistry = registry,
            Filter = SparkSqlParser.ParsePredicate("variant_get(v, '$.a', 'bigint') BETWEEN 120 AND 130"),
        });

        long rows = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            using (batch)
                rows += batch.Length;
        }
        Assert.Equal(200, rows); // groups 1 and 2 (the stray keeps 2); 0 and 3 skipped
    }

    // ───── Pages ─────

    /// <summary>
    /// One row group of 1000 rows in pages of 100, <c>a</c> ascending, with a stray string in the sixth
    /// page. A page whose bounds exclude the predicate is dropped; the stray page is kept, since
    /// <c>variant_get</c> raises on the string. Matching rows are found with <c>try_variant_get</c>, which
    /// answers NULL for it instead, so that the rest of the row group can be checked row by row.
    /// </summary>
    [Theory]
    [InlineData("variant_get(v, '$.a', 'bigint') < 50", new long[] { 0, 100, 500, 600 })]
    [InlineData("variant_get(v, '$.a', 'bigint') > 950", new long[] { 500, 600, 900, 1000 })]
    [InlineData("variant_get(v, '$.o.x', 'int') BETWEEN 230 AND 250", new long[] { 200, 300 })]
    public async Task Pages_PrunedByTheTypedValueBounds(string sql, long[] expected)
    {
        string path = await WriteAsync(
            pageRows: 100,
            Batch(Range(0, 550).Append(Stray()).Concat(Range(551, 1000))));

        using var reader = Open(path, out var file);
        await using var _ = file;
        var predicate = SparkSqlParser.ParsePredicate(sql);
        var ranges = await reader.GetCandidateRowRangesAsync(0, predicate);
        Assert.Equal(expected, ranges.SelectMany(r => new[] { r.Start, r.End }).ToArray());

        if (sql.Contains("'$.a'"))
            Assert.Contains(ranges, r => r.Start <= 550 && 550 < r.End); // the stray row raises: never skipped

        var matches = await MatchesAsync(reader, 0, SparkSqlParser.ParsePredicate(sql.Replace("variant_get(", "try_variant_get(")));
        Assert.DoesNotContain(null, matches);
        for (int row = 0; row < matches.Count; row++)
        {
            if (matches[row] == true)
                Assert.True(ranges.Any(r => r.Start <= row && row < r.End), $"row {row} matches {sql} but no range holds it");
        }
    }

    /// <summary>
    /// With no residual value anywhere in the row group, the residual's page index is not needed: its chunk
    /// statistics settle every interval.
    /// </summary>
    [Fact]
    public async Task Pages_ResidualNullThroughout_NeedsNoResidualIndex()
    {
        string path = await WriteAsync(pageRows: 100, Batch(Range(0, 1000)));

        using var reader = Open(path, out var file);
        await using var _ = file;
        var ranges = await reader.GetCandidateRowRangesAsync(
            0, SparkSqlParser.ParsePredicate("variant_get(v, '$.a', 'bigint') BETWEEN 420 AND 430"));
        Assert.Equal(new[] { new RowRange(400, 500) }, ranges);
    }

    // ───── Oracle ─────

    /// <summary>
    /// DuckDB counts the matching rows of the same file, reading it all; EW counts them after pruning. A
    /// bound that drops a matching row makes EW's count the smaller.
    /// </summary>
    [SkippableTheory]
    [InlineData("variant_get(v, '$.a', 'bigint') < 50", "TRY_CAST(v.a AS BIGINT) < 50")]
    [InlineData("variant_get(v, '$.a', 'bigint') > 150", "TRY_CAST(v.a AS BIGINT) > 150")]
    [InlineData("variant_get(v, '$.s', 'string') < 's0120'", "TRY_CAST(v.s AS VARCHAR) < 's0120'")]
    public async Task Oracle_DuckDbCountsTheSameRows(string spark, string duck)
    {
        ExternalParquetReaders.Require();
        // No stray here: variant_get raises on one, and DuckDB's TRY_CAST would not.
        string path = await WriteAsync(
            pageRows: 25,
            Batch(Range(0, 100)),
            Batch(Range(100, 200)),
            Batch(Enumerable.Range(0, 50).Select(i => i % 2 == 0 ? NoA() : (VariantValue?)null)));

        var expected = ExternalParquetReaders.CountWhere("duckdb", path, duck);
        Skip.If(!expected.Installed, $"DuckDB is not installed: {expected.Error}");
        Assert.True(expected.Count is not null, $"DuckDB refused {duck}: {expected.Error}");

        Assert.Equal(expected.Count, await CountAfterPruningAsync(path, spark));
    }

    /// <summary>The same check over a file DuckDB shredded and wrote, in several row groups.</summary>
    [SkippableFact]
    public async Task Oracle_DuckDbWrittenFile()
    {
        ExternalParquetReaders.Require();
        string path = Path.Combine(_tempDir, "duck.parquet");
        var (written, supported, error) = ExternalParquetReaders.DuckDbWriteVariant(
            path, "SELECT i AS id, {'a': i}::VARIANT AS v FROM range(10000) t(i) ORDER BY id",
            orderBy: "id", copyOptions: "ROW_GROUP_SIZE 2048");
        Skip.IfNot(supported, $"DuckDB cannot write shredded VARIANT here: {error}");
        Assert.True(written is not null, $"DuckDB failed to write the file: {error}");
        Assert.Contains("v, typed_value, a, typed_value", written!.Leaves);

        var expected = ExternalParquetReaders.CountWhere("duckdb", path, "v.a::BIGINT > 9500");
        Assert.True(expected.Count is not null, expected.Error);

        using var reader = Open(path, out var file);
        await using var _ = file;
        var candidates = await reader.GetCandidateRowGroupsAsync(
            SparkSqlParser.ParsePredicate("variant_get(v, '$.a', 'bigint') > 9500"));
        Assert.True(candidates.Cast<bool>().Count(k => !k) > 0, "no row group was pruned");

        Assert.Equal(expected.Count, await CountAfterPruningAsync(path, "variant_get(v, '$.a', 'bigint') > 9500"));
    }

    private static async Task<long> CountAfterPruningAsync(string path, string sql)
    {
        var predicate = SparkSqlParser.ParsePredicate(sql);
        using var reader = Open(path, out var file);
        await using var _ = file;
        var candidates = await reader.GetCandidateRowGroupsAsync(predicate);
        long count = 0;
        for (int g = 0; g < candidates.Length; g++)
        {
            if (!candidates[g])
                continue;
            var ranges = await reader.GetCandidateRowRangesAsync(g, predicate);
            var matches = await MatchesAsync(reader, g, predicate);
            for (int row = 0; row < matches.Count; row++)
            {
                if (matches[row] == true && ranges.Any(r => r.Start <= row && row < r.End))
                    count++;
            }
        }
        return count;
    }
}

