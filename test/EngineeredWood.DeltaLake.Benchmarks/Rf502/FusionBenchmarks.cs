// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 5b: adjacent conjuncts over one dictionary-encoded column answered from its dictionary together. Before
/// 5b a column two predicates read was never answered from its dictionary, so such a filter was asked row by row.
/// <list type="bullet">
/// <item><description><c>Rows</c>: dictionary answers off.</description></item>
/// <item><description><c>Separate</c>: dictionary answers on, fusion off (5b's baseline: the run is asked row by row).</description></item>
/// <item><description><c>Fused</c>: the run answered together.</description></item>
/// </list>
/// <c>category</c> holds 16 strings and <c>qty</c> 100 integers, both dictionary-encoded; <c>amount</c> is not, so its
/// range is a control that fusion leaves alone. The scan returns <c>id</c>, or <c>id</c> and the filtered column.
/// <para>On <see cref="ConjunctSplitBenchmarks"/>'s table. Run with <c>--filter *FusionBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 15)]
public class FusionBenchmarks
{
    public static IEnumerable<string> Filters =>
    [
        "category <> 'cat_03' AND category <> 'cat_07'",                            // 88% kept
        "category <> 'cat_03' AND category <> 'cat_07' AND category <> 'cat_11'",  // 81%
        "category >= 'cat_04' AND category < 'cat_08'",                             // 25%
        "qty >= 10 AND qty < 20",                                                   // 10% (qty is 5% NULL)
        "amount > 100 AND amount < 900",                                            // 80%, not dictionary-encoded
    ];

    public enum Answering
    {
        Rows,
        Separate,
        Fused,
    }

    [ParamsSource(nameof(Filters))]
    public string Filter { get; set; } = "";

    [Params(Answering.Rows, Answering.Separate, Answering.Fused)]
    public Answering Mode { get; set; }

    /// <summary>Whether the scan returns the filtered column too, rather than reading it only for the filter.</summary>
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
        _table.AnswersFromDictionaries = Mode != Answering.Rows;
        _table.FusesAnswers = Mode == Answering.Fused;
        _predicate = SparkSqlParser.ParsePredicate(Filter);
        string filtered = Filter.Split(' ')[0];
        _columns = Returned ? ["id", filtered] : ["id"];
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
