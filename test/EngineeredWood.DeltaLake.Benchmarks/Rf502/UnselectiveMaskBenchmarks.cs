// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #830: what a host mask that rejects (almost) nothing costs. TPC-H Q13's runtime filter on <c>o_custkey</c> keeps
/// nearly every row of <c>orders</c>, and the scan got slower with masks on. The table is orders-shaped: a sorted
/// <c>orderkey</c>, a random <c>custkey</c> (too many values for its dictionary, so it falls back to PLAIN), and a
/// string <c>comment</c>. The scan returns all three, as Q13's does; the mask is a value-wise hash-set probe of
/// <c>custkey</c> keeping <see cref="KeptPercent"/>% of the rows (-1: no mask). <see cref="TwoPass"/> forces the
/// two-pass read on or off (an object store gets it by default, a local file does not). Measured in
/// doc/row-level-filtering.md §33; <see cref="UnselectiveMaskStoreBenchmarks"/> is the same over a simulated store.
/// <para>The table is built under <c>%TEMP%\ew-830-orders</c>. Run with <c>--filter *UnselectiveMaskBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 12)]
public class UnselectiveMaskBenchmarks
{
    private const int Files = 2;
    private const int GroupsPerFile = 4;
    private const int RowsPerGroup = 1_000_000;
    private const int Customers = 1_500_000;

    internal static readonly string TableRoot = Path.Combine(Path.GetTempPath(), "ew-830-orders");

    [Params(-1, 100, 99, 95, 80)]
    public int KeptPercent { get; set; }

    [Params(false, true)]
    public bool TwoPass { get; set; }

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("orderkey", Apache.Arrow.Types.Int64Type.Default, nullable: false))
        .Field(new Field("custkey", Apache.Arrow.Types.Int64Type.Default, nullable: false))
        .Field(new Field("comment", Apache.Arrow.Types.StringType.Default, nullable: false))
        .Build();

    private DeltaTable _table = null!;
    private DeltaRowMask? _mask;

    [GlobalSetup]
    public void Setup()
    {
        EnsureTable();

        _table = DeltaTable.OpenAsync(new LocalTableFileSystem(TableRoot), new DeltaTableOptions { MaxConcurrentReads = 4 })
            .AsTask().GetAwaiter().GetResult();
        _table.TwoPassReads = TwoPass;

        _mask = null;
        if (KeptPercent >= 0)
        {
            // The build side: the customers whose key falls in the kept share. A join filter probes a hash table.
            var keys = new HashSet<long>();
            for (long c = 0; c < Customers; c++)
            {
                if (c % 100 < KeptPercent)
                    keys.Add(c);
            }

            _mask = new DeltaRowMask(["custkey"], batch =>
            {
                var values = (Int64Array)batch.Column(0);
                var span = values.Values;
                var answer = new BooleanArray.Builder().Reserve(span.Length);
                for (int i = 0; i < span.Length; i++)
                    answer.Append(keys.Contains(span[i]));
                return answer.Build();
            }) { ValueWise = true };
        }

        if (Scan().GetAwaiter().GetResult() == 0)
            throw new InvalidOperationException("No row was returned.");
    }

    [GlobalCleanup]
    public void Cleanup() => _table.DisposeAsync().AsTask().GetAwaiter().GetResult();

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

    /// <summary>
    /// Written beside the table once <see cref="Build"/> has committed every file. The build commits once per file, so
    /// a build interrupted part way leaves a table that has commits but not all its rows (#840 review rounds 2-3).
    /// </summary>
    private static string CompleteMarker => Path.Combine(TableRoot, "_build_complete");

    /// <summary>
    /// Builds the table unless a finished build is there; a partial one is removed and built again.
    /// </summary>
    internal static void EnsureTable()
    {
        if (File.Exists(CompleteMarker))
            return;
        if (Directory.Exists(TableRoot))
            Directory.Delete(TableRoot, recursive: true);
        Build(TableRoot).GetAwaiter().GetResult();
    }

    internal static async Task Build(string root)
    {
        Directory.CreateDirectory(root);
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup },
            TargetFileSize = long.MaxValue,
        };
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(root), Schema, options);
        var random = new Random(830);
        string[] words = ["final", "special", "pending", "requests", "deposits", "accounts", "furiously", "quickly",
            "regular", "express", "ironic", "packages", "theodolites", "carefully", "blithely", "slyly"];
        long next = 0;
        for (int f = 0; f < Files; f++)
        {
            int rows = GroupsPerFile * RowsPerGroup;
            var orderKeys = new Int64Array.Builder().Reserve(rows);
            var custKeys = new Int64Array.Builder().Reserve(rows);
            var comments = new StringArray.Builder();
            var text = new System.Text.StringBuilder();
            for (int i = 0; i < rows; i++, next++)
            {
                orderKeys.Append(next * 4);
                custKeys.Append(random.Next(Customers));
                text.Clear();
                int count = 4 + random.Next(6);
                for (int w = 0; w < count; w++)
                    text.Append(words[random.Next(words.Length)]).Append(' ');
                comments.Append(text.ToString());
            }

            await table.WriteAsync([new RecordBatch(Schema, [orderKeys.Build(), custKeys.Build(), comments.Build()], rows)]);
        }

        // Last, so that it is there only when every file is.
        File.WriteAllText(Path.Combine(root, "_build_complete"), $"{Files * GroupsPerFile * RowsPerGroup} rows");
    }
}
