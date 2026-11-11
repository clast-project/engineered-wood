// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 5a: where asking a conjunct that cannot raise about the whole batch, rather than about a gather of the
/// rows still live, starts to pay. The first conjunct (<c>amount &lt; X</c>) leaves X/10 % of the rows live for the
/// second (<see cref="Second"/>), which is asked either way: <c>Gather</c> never takes the whole batch,
/// <c>Whole</c> always does. Dictionary answers are off, or <c>qty</c>'s dictionary would answer it. The crossover sets <see cref="DeltaTable.WholeBatchLiveShare"/>'s default.
/// <para>On <see cref="ConjunctSplitBenchmarks"/>'s table. Run with <c>--filter *WholeBatchShareBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 2, warmupCount: 5, iterationCount: 25)]
public class WholeBatchShareBenchmarks
{
    [Params(50, 100, 250, 400, 500, 600, 750, 900)]
    public int Amount { get; set; }

    [Params(false, true)]
    public bool Whole { get; set; }

    /// <summary>The second conjunct: a nullable INT32 keeping 94%, or a non-null unique INT64 keeping every row.</summary>
    [Params("qty < 99", "id >= 0")]
    public string Second { get; set; } = "";

    private DeltaTable _table = null!;
    private Expressions.Predicate _predicate = null!;

    [GlobalSetup]
    public void Setup()
    {
        string root = ConjunctSplitBenchmarks.TableRoot;
        if (!Directory.Exists(Path.Combine(root, "_delta_log")))
            ConjunctSplitBenchmarks.Build(root).GetAwaiter().GetResult();

        _table = DeltaTable.OpenAsync(new LocalTableFileSystem(root), new DeltaTableOptions { MaxConcurrentReads = 4 })
            .AsTask().GetAwaiter().GetResult();
        _table.WholeBatchLiveShare = Whole ? 0 : 2;

        // qty holds 100 values, so it is dictionary-encoded and would otherwise be answered from its dictionary
        // (phase 4b), never asked about rows at all.
        _table.AnswersFromDictionaries = false;
        _predicate = SparkSqlParser.ParsePredicate($"amount < {Amount} AND {Second}");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    [Benchmark(Description = "ReadAsync id, FilterRows")]
    public async Task<long> Scan()
    {
        long rows = 0;
        await foreach (var batch in _table.ReadAsync(
            new DeltaReadOptions { Columns = ["id"], Filter = _predicate, FilterRows = true }))
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }
}
