// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 4c: a <see cref="DeltaReadOptions.FilterRows"/> scan whose conjuncts over the dictionary-encoded
/// <c>category</c> (16 values) are answered from each chunk's dictionary (<c>Dictionary</c> true), against the same
/// scan asking every row (<see cref="DeltaTable.AnswersFromDictionaries"/> off). The scan returns <c>id</c> only, so
/// <c>category</c> is read for the filter and dropped. A value-wise host mask (a hash-set probe, as a join filter is)
/// is measured the same way.
/// <para>The table is <see cref="ConjunctSplitBenchmarks"/>'s, under <c>%TEMP%\ew-rf502-conjuncts</c>.
/// Run with <c>--filter *ValueWiseBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 12)]
public class ValueWiseBenchmarks
{
    public static IEnumerable<string> Filters =>
    [
        "category = 'cat_03'",                                  // 6% kept
        "category IN ('cat_01', 'cat_05', 'cat_09')",           // 19%
        "category <> 'cat_03'",                                 // 94%
        "lower(category) = 'cat_03'",                           // a function: no direct kernel per row
        "category = 'cat_03' AND amount < 500",                 // a dictionary conjunct, then a row one
    ];

    [ParamsSource(nameof(Filters))]
    public string Filter { get; set; } = "";

    [Params(false, true)]
    public bool Dictionary { get; set; }

    private DeltaTable _table = null!;
    private Expressions.Predicate _predicate = null!;
    private DeltaRowMask _mask = null!;

    [GlobalSetup]
    public void Setup()
    {
        var conjuncts = new ConjunctSplitBenchmarks { Filter = "amount < 990", Split = true };
        conjuncts.Setup(); // builds the table when it isn't there yet
        conjuncts.Cleanup();

        string root = Path.Combine(Path.GetTempPath(), "ew-rf502-conjuncts");
        _table = DeltaTable.OpenAsync(new LocalTableFileSystem(root), new DeltaTableOptions { MaxConcurrentReads = 4 })
            .AsTask().GetAwaiter().GetResult();
        _table.AnswersFromDictionaries = Dictionary;
        _predicate = SparkSqlParser.ParsePredicate(Filter);

        var keys = new HashSet<string>(StringComparer.Ordinal) { "cat_02", "cat_07", "cat_11" };
        _mask = new DeltaRowMask(["category"], batch =>
        {
            var values = (StringArray)batch.Column(0);
            var answer = new BooleanArray.Builder().Reserve(values.Length);
            for (int i = 0; i < values.Length; i++)
                answer.Append(values.GetString(i) is { } v && keys.Contains(v));
            return answer.Build();
        }) { ValueWise = true };

        if (Scan().GetAwaiter().GetResult() == 0 || Mask().GetAwaiter().GetResult() == 0)
            throw new InvalidOperationException($"{Filter}: no row matched");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    [Benchmark(Description = "ReadAsync id, FilterRows")]
    public Task<long> Scan() => CountAsync(new DeltaReadOptions { Columns = ["id"], Filter = _predicate, FilterRows = true });

    /// <summary>The host mask alone (the filter is the param's only to vary the runs; it is not applied).</summary>
    [Benchmark(Description = "ReadAsync id, value-wise mask (19%)")]
    public Task<long> Mask() => CountAsync(new DeltaReadOptions { Columns = ["id"], RowMasks = [_mask] });

    private async Task<long> CountAsync(DeltaReadOptions options)
    {
        long rows = 0;
        await foreach (var batch in _table.ReadAsync(options))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }
}
