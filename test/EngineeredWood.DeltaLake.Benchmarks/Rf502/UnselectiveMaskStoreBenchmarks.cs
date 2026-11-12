// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #830: <see cref="UnselectiveMaskBenchmarks"/> over a <see cref="SimulatedObjectStore"/>, where a scan reads in two
/// passes by default, as it did for TPC-H Q13 on S3. Each request waits <see cref="LatencyMs"/> plus its bytes at
/// 200 MB/s. <see cref="TwoPass"/> false forces one pass; true is the store's default, two passes. Measured in
/// doc/row-level-filtering.md §33: a switch to one pass for row groups a mask keeps whole was built and measured
/// here, and lost.
/// <para>On <see cref="UnselectiveMaskBenchmarks"/>'s table. Run with <c>--filter *UnselectiveMaskStoreBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 10)]
public class UnselectiveMaskStoreBenchmarks
{
    private const int Customers = 1_500_000;

    [Params(-1, 100, 95)]
    public int KeptPercent { get; set; }

    [Params(false, true)]
    public bool TwoPass { get; set; }

    [Params(5, 25)]
    public int LatencyMs { get; set; }

    private DeltaTable _table = null!;
    private DeltaRowMask? _mask;

    [GlobalSetup]
    public void Setup()
    {
        UnselectiveMaskBenchmarks.EnsureTable();
        string root = UnselectiveMaskBenchmarks.TableRoot;

        SimulatedObjectStore.BeginHighResolutionTimers();
        var store = new SimulatedObjectStore(
            new LocalTableFileSystem(root), TimeSpan.FromMilliseconds(LatencyMs), bytesPerSecond: 200L << 20);
        _table = DeltaTable.OpenAsync(store, new DeltaTableOptions()).AsTask().GetAwaiter().GetResult();
        _table.TwoPassReads = TwoPass ? null : false;

        _mask = null;
        if (KeptPercent >= 0)
        {
            var keys = new HashSet<long>();
            for (long c = 0; c < Customers; c++)
            {
                if (c % 100 < KeptPercent)
                    keys.Add(c);
            }

            _mask = new DeltaRowMask(["custkey"], batch =>
            {
                var span = ((Int64Array)batch.Column(0)).Values;
                var answer = new BooleanArray.Builder().Reserve(span.Length);
                for (int i = 0; i < span.Length; i++)
                    answer.Append(keys.Contains(span[i]));
                return answer.Build();
            }) { ValueWise = true };
        }

        long before = store.Requests;
        if (Scan().GetAwaiter().GetResult() == 0)
            throw new InvalidOperationException("No row was returned.");
        Console.WriteLine($"// #830 requests per scan: {store.Requests - before} ({KeptPercent}, two passes {TwoPass}, {LatencyMs} ms)");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _table.DisposeAsync().AsTask().GetAwaiter().GetResult();
        SimulatedObjectStore.EndHighResolutionTimers();
    }

    [Benchmark(Description = "ReadAsync orderkey, custkey, comment")]
    public async Task<long> Scan()
    {
        long rows = 0;
        await foreach (var batch in _table.ReadAsync(new DeltaReadOptions
                       {
                           Columns = ["orderkey", "custkey", "comment"],
                           RowMasks = _mask is null ? null : [_mask],
                       }))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }
}
