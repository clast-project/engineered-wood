// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using EwDeltaTable = EngineeredWood.DeltaLake.Table.DeltaTable;
using EwOptions = EngineeredWood.DeltaLake.Table.DeltaTableOptions;
using EwReadOptions = EngineeredWood.DeltaLake.Table.DeltaReadOptions;

namespace DeltaDotnetComparison;

/// <summary>
/// #876 on a real store: scan #652's row-group-size layouts (12M lineitem-shaped rows in 2 files, written by
/// <c>EngineeredWood.DeltaLake.Benchmarks rg652 build</c> to <c>%TEMP%/ew-rg652</c>) with 16 reads at once, under
/// whatever largest coalesced request <c>EWDD_AZURE_MAX_REQUEST_MIB</c> sets for this process. <c>rg876-azure.ps1</c>
/// runs one process per cap.
///
/// <code>dotnet run -c Release -- --rg876-upload [local root] [layout,...]</code>
/// <code>dotnet run -c Release -- --rg876 [layout,...] [query,...] [repeats]</code>
///
/// <para>Queries: <c>all</c> (every column) and <c>q1</c> (TPC-H Q1's seven columns). Prints one row per layout and
/// query: the best and median of the repeats, in seconds.</para>
/// </summary>
internal static class RequestSizeSweep
{
    private static readonly string[] Layouts = ["w100k", "w1m", "w6m"];

    private static readonly string[] Q1 =
        ["l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"];

    /// <summary>On local disk, #652's own tables; on a remote store, <c>rg876</c> under its root.</summary>
    private static string Root => BenchStorage.Current is LocalStorage
        ? ForeignRowGroups.RootFor("rg652")
        : ForeignRowGroups.RootFor("rg876");

    public static Task<int> UploadAsync(string[] args)
    {
        string local = args.Length > 0 ? args[0] : Path.Combine(Path.GetTempPath(), "ew-rg652");
        return ForeignRowGroups.UploadAsync([local, .. args.Skip(1)], Root, Layouts);
    }

    public static async Task<int> RunAsync(string[] args)
    {
        string[] layouts = args.Length > 0 && args[0] != "-" ? args[0].Split(',') : Layouts;
        string[] queries = args.Length > 1 ? args[1].Split(',') : ["all", "q1"];
        int repeats = args.Length > 2 ? int.Parse(args[2]) : 5;
        string cap = Environment.GetEnvironmentVariable("EWDD_AZURE_MAX_REQUEST_MIB") is { Length: > 0 } c ? c : "default";

        foreach (string layout in layouts)
        foreach (string query in queries)
        {
            var times = new List<double>();
            long rows = 0;
            // One unmeasured scan first: it opens connections and warms the JIT, which the first measured scan
            // would otherwise pay for.
            for (int r = -1; r < repeats; r++)
            {
                await using var table = await EwDeltaTable.OpenAsync(
                    Ew.Fs(BenchStorage.Current.Combine(Root, layout)), new EwOptions { MaxConcurrentReads = 16 });
                var read = query == "q1" ? new EwReadOptions { Columns = Q1 } : new EwReadOptions();
                rows = 0;
                var watch = Stopwatch.StartNew();
                await foreach (var batch in table.ReadAsync(read))
                {
                    rows += batch.Length;
                    batch.Dispose();
                }
                if (r >= 0)
                    times.Add(watch.Elapsed.TotalSeconds);
            }

            times.Sort();
            Console.WriteLine($"{cap}\t{layout}\t{query}\t{times[0]:F2}\t{times[times.Count / 2]:F2}\t{rows}");
        }

        return 0;
    }
}
