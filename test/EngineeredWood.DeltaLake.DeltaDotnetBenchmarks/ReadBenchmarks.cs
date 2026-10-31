// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using BenchmarkDotNet.Attributes;
using DeltaLake.Table;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;

namespace DeltaDotnetComparison;

/// <summary>
/// Open the latest snapshot and scan every row of a 1M-row, single-commit table into Arrow
/// (1 file when delta-dotnet wrote it, 16 when EW did).
///
/// <para>delta-dotnet has two scan paths, and both are measured: <c>ReadAsArrowTableAsync</c> is the
/// delta-kernel-rs scan (materializes the whole table), and <c>QueryAsync("SELECT *")</c> is delta-rs's
/// DataFusion path (streams batches).</para>
/// </summary>
[Config(typeof(ReadConfig))]
public class FullScanBenchmarks
{
    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    private string _path = "";

    [GlobalSetup]
    public void Setup() => _path = Fixtures.SingleCommit(Writer);

    [BenchmarkCategory("FullScan"), Benchmark(Baseline = true, Description = "EW ReadAllAsync")]
    public async Task<long> EW()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        return await Ew.CountAsync(table.ReadAllAsync());
    }

    [BenchmarkCategory("FullScan"), Benchmark(Description = "DD ReadAsArrowTable (kernel)")]
    public async Task<long> DD_Kernel()
    {
        using var table = await Dd.OpenAsync(_path);
        using var owned = await table.ReadAsArrowTableAsync(CancellationToken.None);
        return owned.Table.RowCount;
    }

    [BenchmarkCategory("FullScan"), Benchmark(Description = "DD QueryAsync SELECT * (DataFusion)")]
    public async Task<long> DD_DataFusion()
    {
        using var table = await Dd.OpenAsync(_path);
        return await Dd.CountAsync(table.QueryAsync(new SelectQuery("SELECT * FROM deltatable"), CancellationToken.None));
    }
}

/// <summary>
/// A filtered, projected scan (<c>SELECT id, amount WHERE …</c>) over a 20-file table whose files hold
/// disjoint, sorted id ranges.
///
/// <list type="bullet">
/// <item><c>Selective</c> (an id range inside one file): file statistics can skip 19 of 20 files.</item>
/// <item><c>OneCategory</c> (1 row in 16, everywhere): nothing can be skipped; this is decode + filter cost.</item>
/// </list>
///
/// <para>EW's <c>Filter</c> prunes but does not filter rows, so the like-for-like EW method applies the
/// predicate per row too (<see cref="Ew.FilteredCountAsync"/>). The prune-only method shows what that
/// row filter costs, and returns MORE rows than the other two by design.</para>
/// </summary>
[Config(typeof(ReadConfig))]
public class FilteredScanBenchmarks
{
    public enum FilterKind
    {
        Selective,
        OneCategory,
    }

    private static readonly string[] Columns = ["id", "amount"];

    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    [ParamsAllValues]
    public FilterKind Filter { get; set; }

    private string _path = "";
    private string _sql = "";

    [GlobalSetup]
    public void Setup()
    {
        _path = Fixtures.Prunable(Writer);
        _sql = Filter == FilterKind.Selective ? Sql.SelectiveRange : Sql.OneCategory;
    }

    [BenchmarkCategory("FilteredScan"), Benchmark(Baseline = true, Description = "EW ReadAsync + row filter")]
    public async Task<long> EW_Exact()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        return await Ew.FilteredCountAsync(table, _sql, Columns, exact: true);
    }

    [BenchmarkCategory("FilteredScan"), Benchmark(Description = "EW ReadAsync, prune only")]
    public async Task<long> EW_PruneOnly()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        return await Ew.FilteredCountAsync(table, _sql, Columns, exact: false);
    }

    [BenchmarkCategory("FilteredScan"), Benchmark(Description = "DD QueryAsync SELECT … WHERE")]
    public async Task<long> DD_DataFusion()
    {
        using var table = await Dd.OpenAsync(_path);
        return await Dd.CountAsync(table.QueryAsync(
            new SelectQuery($"SELECT id, amount FROM deltatable WHERE {_sql}"), CancellationToken.None));
    }
}

/// <summary>
/// Log replay: open a 100-commit table at its latest version, open it at version 50 (time travel), and list
/// its history; each with and without a checkpoint at the last version.
///
/// <para>EW has no "open at version": it opens the latest snapshot and then builds the older one
/// (<c>GetSnapshotAtVersionAsync</c>), so its time-travel number includes a latest-snapshot load that
/// delta-dotnet's <c>TableOptions.Version</c> does not do.</para>
/// </summary>
[Config(typeof(ReadConfig))]
public class SnapshotBenchmarks
{
    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    [ParamsAllValues]
    public bool Checkpointed { get; set; }

    private string _path = "";

    [GlobalSetup]
    public void Setup() => _path = Fixtures.ManyCommits(Writer, Checkpointed);

    [BenchmarkCategory("OpenLatest"), Benchmark(Baseline = true, Description = "EW OpenAsync")]
    public async Task<long> EW_OpenLatest()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        return table.CurrentSnapshot.Version;
    }

    [BenchmarkCategory("OpenLatest"), Benchmark(Description = "DD LoadTableAsync")]
    public async Task<long> DD_OpenLatest()
    {
        using var table = await Dd.OpenAsync(_path);
        return (long)table.Version()!.Value;
    }

    [BenchmarkCategory("OpenAtVersion"), Benchmark(Baseline = true, Description = "EW OpenAsync + GetSnapshotAtVersionAsync(50)")]
    public async Task<long> EW_OpenAtVersion()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        var snapshot = await table.GetSnapshotAtVersionAsync(50);
        return snapshot.Version;
    }

    [BenchmarkCategory("OpenAtVersion"), Benchmark(Description = "DD LoadTableAsync(Version = 50)")]
    public async Task<long> DD_OpenAtVersion()
    {
        using var table = await Dd.OpenAsync(_path, version: 50);
        return (long)table.Version()!.Value;
    }

    [BenchmarkCategory("History"), Benchmark(Baseline = true, Description = "EW GetHistoryAsync")]
    public async Task<long> EW_History()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        long count = 0;
        await foreach (var _ in table.GetHistoryAsync())
            count++;
        return count;
    }

    [BenchmarkCategory("History"), Benchmark(Description = "DD HistoryAsync")]
    public async Task<long> DD_History()
    {
        using var table = await Dd.OpenAsync(_path);
        var history = await table.HistoryAsync(null, CancellationToken.None);
        return history.Length;
    }
}

/// <summary>Read the whole change data feed (inserts, an UPDATE's pre/post images, a DELETE) of a CDF table.</summary>
[Config(typeof(ReadConfig))]
public class ChangeFeedBenchmarks
{
    [ParamsAllValues]
    public TableWriter Writer { get; set; }

    private string _path = "";

    [GlobalSetup]
    public void Setup() => _path = Fixtures.ChangeFeed(Writer);

    [BenchmarkCategory("ChangeFeed"), Benchmark(Baseline = true, Description = "EW ReadChangesAsync")]
    public async Task<long> EW()
    {
        await using var table = await EwDeltaTable.OpenAsync(Ew.Fs(_path));
        return await Ew.CountAsync(table.ReadChangesAsync(new EngineeredWood.DeltaLake.Table.DeltaChangeReadOptions
        {
            StartVersion = 0,
            EndVersion = table.CurrentSnapshot.Version,
        }));
    }

    [BenchmarkCategory("ChangeFeed"), Benchmark(Description = "DD QueryTableChangesAsync")]
    public async Task<long> DD()
    {
        using var table = await Dd.OpenAsync(_path);
        return await Dd.CountAsync(table.QueryTableChangesAsync(new TableChangesOptions(0), CancellationToken.None));
    }
}
