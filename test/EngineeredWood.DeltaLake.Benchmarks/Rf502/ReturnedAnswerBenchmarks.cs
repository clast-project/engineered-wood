// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 5d: what answering a predicate from its column's dictionary costs against asking the rows, for a column
/// the scan returns (<c>Returned</c>) and one it reads only for the filter, over an integer column (<c>qty</c>, 100
/// values, 5% NULL) and a string one (<c>category</c>, 16 values). Phase 4d measured a returned dictionary answer
/// 12% slower than a direct kernel at high selectivity; 5b found the same for a returned integer run.
/// <para>On <see cref="ConjunctSplitBenchmarks"/>'s table. Run with <c>--filter *ReturnedAnswerBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 2, warmupCount: 5, iterationCount: 20)]
public class ReturnedAnswerBenchmarks
{
    public static IEnumerable<string> Filters =>
    [
        "qty < 20",                 // 19% kept
        "qty <> 7",                 // 94%
        "category = 'cat_03'",      // 6%
        "category <> 'cat_03'",     // 94%
    ];

    [ParamsSource(nameof(Filters))]
    public string Filter { get; set; } = "";

    [Params(false, true)]
    public bool Dictionary { get; set; }

    [Params(false, true)]
    public bool Returned { get; set; }

    private DeltaTable _table = null!;
    private Expressions.Predicate _predicate = null!;
    private string[] _columns = [];

    [GlobalSetup]
    public void Setup()
    {
        string root = ConjunctSplitBenchmarks.TableRoot;
        if (!Directory.Exists(Path.Combine(root, "_delta_log")))
            ConjunctSplitBenchmarks.Build(root).GetAwaiter().GetResult();

        _table = DeltaTable.OpenAsync(new LocalTableFileSystem(root), new DeltaTableOptions { MaxConcurrentReads = 4 })
            .AsTask().GetAwaiter().GetResult();
        _table.AnswersFromDictionaries = Dictionary;
        _predicate = SparkSqlParser.ParsePredicate(Filter);
        _columns = Returned ? ["id", Filter.Split(' ')[0]] : ["id"];
        if (Scan().GetAwaiter().GetResult() == 0)
            throw new InvalidOperationException($"{Filter}: no row matched");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    [Benchmark(Description = "ReadAsync, FilterRows")]
    public async Task<long> Scan()
    {
        long rows = 0;
        await foreach (var batch in _table.ReadAsync(
            new DeltaReadOptions { Columns = _columns, Filter = _predicate, FilterRows = true }))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }
}
