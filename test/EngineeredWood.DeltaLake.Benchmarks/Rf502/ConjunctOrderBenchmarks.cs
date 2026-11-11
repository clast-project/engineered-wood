// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 5e's gate: what the order of a filter's conjuncts costs, when a conjunct that keeps nearly every row
/// stands ahead of a selective one (<c>Unselective first</c>) against the reverse. Only conjuncts that cannot raise may
/// be reordered (doc/row-level-filtering.md §7), and those are all cheap: direct-kernel comparisons and dictionary
/// answers. Measured in doc/row-level-filtering.md §28 (6-16% returning <c>id</c> alone, 0-8% returning every
/// column), and reordering was not built.
/// <para>On <see cref="ConjunctSplitBenchmarks"/>'s table. Run with <c>--filter *ConjunctOrderBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 2, warmupCount: 5, iterationCount: 20)]
public class ConjunctOrderBenchmarks
{
    /// <summary>Pairs: the unselective conjuncts, then the selective one (2%); the reverse is the other order.</summary>
    public static IEnumerable<string> Cases =>
    [
        "id >= 0 | amount < 20",                         // INT64 kernel (100%), DOUBLE kernel (2%)
        "amount < 990 | amount > 980",                   // one column, two kernels (99%, 2%)
        "id >= 0 AND amount < 990 | amount < 20",        // two unselective kernels ahead of one
        "category <> 'cat_03' | amount < 20",            // a dictionary answer (94%), then a kernel
        "amount < 990 | qty = 7",                        // a kernel (99%), then a dictionary answer (1%)
    ];

    [ParamsSource(nameof(Cases))]
    public string Case { get; set; } = "";

    [Params(true, false)]
    public bool UnselectiveFirst { get; set; }

    /// <summary>Whether the scan returns every column rather than <c>id</c> alone.</summary>
    [Params(false, true)]
    public bool Wide { get; set; }

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
        var parts = Case.Split([" | "], StringSplitOptions.None);
        string sql = UnselectiveFirst ? $"{parts[0]} AND {parts[1]}" : $"{parts[1]} AND {parts[0]}";
        _predicate = SparkSqlParser.ParsePredicate(sql);
        if (Scan().GetAwaiter().GetResult() == 0)
            throw new InvalidOperationException($"{sql}: no row matched");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

    [Benchmark(Description = "ReadAsync, FilterRows")]
    public async Task<long> Scan()
    {
        long rows = 0;
        await foreach (var batch in _table.ReadAsync(
            new DeltaReadOptions { Columns = Wide ? null : ["id"], Filter = _predicate, FilterRows = true }))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }
}
