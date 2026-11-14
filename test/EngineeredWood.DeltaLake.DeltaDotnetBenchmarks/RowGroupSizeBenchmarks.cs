// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;

namespace DeltaDotnetComparison;

/// <summary>
/// #878: EW and delta-rs/DataFusion scanning the same 12M-row lineitem tables at four row-group sizes (10K, 100K,
/// 1M, 6M rows; two files each). The tables are the #652 sweep's, written by EW with the <c>rg652 build</c> mode of
/// <c>EngineeredWood.DeltaLake.Benchmarks</c> (branch <c>bench/652-rowgroup-sweep</c>), and are read from
/// <c>EWDD_RG652_ROOT</c> (default <c>%TEMP%/ew-rg652</c>). Local storage only.
///
/// <para>Two queries: every column, and TPC-H Q6's filter returning <c>l_extendedprice, l_discount</c> (about 2%
/// of rows).</para>
/// </summary>
[Config(typeof(ReadConfig))]
public class RowGroupSizeBenchmarks
{
    private const string Q6Filter =
        "l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' "
        + "AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";

    private const string All = "SELECT * FROM deltatable";
    private const string Q6 = "SELECT l_extendedprice, l_discount FROM deltatable WHERE " + Q6Filter;

    [Params("w10k", "w100k", "w1m", "w6m")]
    public string Layout { get; set; } = "";

    private string _path = "";

    [GlobalSetup]
    public void Setup()
    {
        string root = Environment.GetEnvironmentVariable("EWDD_RG652_ROOT")
            ?? Path.Combine(Path.GetTempPath(), "ew-rg652");
        _path = Path.Combine(root, Layout);
        if (!Directory.Exists(Path.Combine(_path, "_delta_log")))
            throw new InvalidOperationException(
                $"No #652 table at {_path}: build it with `rg652 build` (bench/652-rowgroup-sweep) or set EWDD_RG652_ROOT.");

        // The two engines must agree on Q6's rows, or the comparison means nothing.
        long ew = EW_Q6().GetAwaiter().GetResult();
        long dd = DD_Q6().GetAwaiter().GetResult();
        Console.WriteLine($"// {Layout}: Q6 rows EW={ew} DD={dd}");
        if (ew != dd)
            throw new InvalidOperationException($"{Layout}: Q6 row counts differ (EW {ew}, DD {dd}).");
    }

    [BenchmarkCategory("RgAll"), Benchmark(Baseline = true, Description = "EW ReadAllAsync")]
    public async Task<long> EW_All()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path), Ew.ReadOptions);
        return await Ew.CountAsync(table.ReadAllAsync());
    }

    [BenchmarkCategory("RgAll"), Benchmark(Description = "DD SELECT *")]
    public Task<long> DD_All() => Dd.QueryCountAsync(_path, All, Dd.Engine);

    [BenchmarkCategory("RgAll"), Benchmark(Description = "DD SELECT * (64K batches)")]
    public Task<long> DD_AllTuned() => Dd.QueryCountAsync(_path, All, Dd.TunedEngine);

    [BenchmarkCategory("RgQ6"), Benchmark(Baseline = true, Description = "EW FilterRows Q6")]
    public async Task<long> EW_Q6()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path), Ew.ReadOptions);
        var options = new DeltaReadOptions
        {
            Columns = ["l_extendedprice", "l_discount"],
            Filter = SparkSqlParser.ParsePredicate(Q6Filter),
            FilterRows = true,
        };
        return await Ew.CountAsync(table.ReadAsync(options));
    }

    [BenchmarkCategory("RgQ6"), Benchmark(Description = "DD Q6")]
    public Task<long> DD_Q6() => Dd.QueryCountAsync(_path, Q6, Dd.Engine);

    [BenchmarkCategory("RgQ6"), Benchmark(Description = "DD Q6 (64K batches)")]
    public Task<long> DD_Q6Tuned() => Dd.QueryCountAsync(_path, Q6, Dd.TunedEngine);
}
