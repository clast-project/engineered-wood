// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions.Sql;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Benchmarks.Rf502;

/// <summary>
/// #502 phase 4a: what evaluating each conjunct of a <see cref="DeltaReadOptions.FilterRows"/> filter as its own row
/// predicate costs, against evaluating them together as one AND, when no conjunct gains from standing alone. Each
/// predicate after the first is handed a gather of the rows still live, so a split pays one gather per conjunct.
/// The filters are scattered (uniform random columns), so pruning decides nothing, and the scan returns only
/// <c>id</c>: the filter's columns are read for it and dropped.
/// <para>The table (4 files of 4 row groups x 131,072 rows) is built once under <c>%TEMP%\ew-rf502-conjuncts</c>.
/// Run with <c>--filter *ConjunctSplitBenchmarks*</c>.</para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 12)]
public class ConjunctSplitBenchmarks
{
    private const int RowsPerGroup = 131_072;
    private const int Files = 4;
    private const int GroupsPerFile = 4;

    public static IEnumerable<string> Filters =>
    [
        "amount < 500 AND qty < 50",                            // 50% then 50%: 25% kept
        "amount < 900 AND qty < 90 AND category <> 'cat_03'",   // 90%, 90%, 94%: 76% kept
        "amount < 100 AND qty < 90",                            // 10% then 90%: 9% kept
        "amount < 990 AND qty < 99 AND id >= 0",                // 99%, 99%, 100%: 94% kept
        "amount < 990",                                         // one conjunct: the split changes nothing
    ];

    [ParamsSource(nameof(Filters))]
    public string Filter { get; set; } = "";

    [Params(false, true)]
    public bool Split { get; set; }

    private DeltaTable _table = null!;
    private Expressions.Predicate _predicate = null!;

    private static readonly Apache.Arrow.Schema Schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
        .Field(f => f.Name("qty").DataType(Int32Type.Default).Nullable(true))
        .Field(f => f.Name("category").DataType(StringType.Default).Nullable(false))
        .Build();

    [GlobalSetup]
    public void Setup()
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-rf502-conjuncts");
        if (!Directory.Exists(Path.Combine(root, "_delta_log")))
            Build(root).GetAwaiter().GetResult();

        _table = DeltaTable.OpenAsync(new LocalTableFileSystem(root), new DeltaTableOptions { MaxConcurrentReads = 4 })
            .AsTask().GetAwaiter().GetResult();
        _table.EvaluatesConjunctsTogether = !Split;
        _predicate = SparkSqlParser.ParsePredicate(Filter);
        if (Scan().GetAwaiter().GetResult() == 0)
            throw new InvalidOperationException($"{Filter}: no row matched");
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

    private static async Task Build(string root)
    {
        Directory.CreateDirectory(root);
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup },
            TargetFileSize = long.MaxValue,
        };
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(root), Schema, options);
        var random = new Random(502);
        long next = 0;
        for (int f = 0; f < Files; f++)
        {
            int rows = GroupsPerFile * RowsPerGroup;
            var ids = new Int64Array.Builder().Reserve(rows);
            var amounts = new DoubleArray.Builder().Reserve(rows);
            var qtys = new Int32Array.Builder().Reserve(rows);
            var categories = new StringArray.Builder();
            for (int i = 0; i < rows; i++, next++)
            {
                ids.Append(next);
                amounts.Append(random.NextDouble() * 1000);
                if (random.Next(20) == 0)
                    qtys.AppendNull();
                else
                    qtys.Append(random.Next(100));
                categories.Append("cat_" + random.Next(16).ToString("00"));
            }

            await table.WriteAsync([new RecordBatch(Schema,
                [ids.Build(), amounts.Build(), qtys.Build(), categories.Build()], rows)]);
        }
    }
}
