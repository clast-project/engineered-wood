// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using EngineeredWood.Arrow;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Benchmarks.Dv610;

/// <summary>Deletion patterns over one 1M-row data file.</summary>
public enum DvPattern
{
    /// <summary>0.1% of rows, scattered (array containers).</summary>
    Sparse,
    /// <summary>50% of rows, scattered (bitmap containers).</summary>
    Dense,
    /// <summary>10 runs of 10,000 rows, serialized as RUN containers.</summary>
    Runs,
    /// <summary>The same rows as <see cref="Runs"/>, serialized without runs (what EW and Spark write).</summary>
    RunsNoRun,
    /// <summary>A 2,000-row run straddling every 65,536-row boundary.</summary>
    Boundary,
}

internal static class DvFixtures
{
    public const int FileRows = 1 << 20;

    public static long[] Positions(DvPattern pattern)
    {
        var rng = new Random(610);
        var set = new SortedSet<long>();
        switch (pattern)
        {
            case DvPattern.Sparse:
                while (set.Count < FileRows / 1000) set.Add(rng.Next(FileRows));
                break;
            case DvPattern.Dense:
                for (int i = 0; i < FileRows; i++) if (rng.Next(2) == 0) set.Add(i);
                break;
            case DvPattern.Runs:
            case DvPattern.RunsNoRun:
                for (int r = 0; r < 10; r++)
                {
                    int s = r * (FileRows / 10) + rng.Next(FileRows / 10 - 10_000);
                    for (int i = 0; i < 10_000; i++) set.Add(s + i);
                }
                break;
            case DvPattern.Boundary:
                for (int b = 65536; b < FileRows; b += 65536)
                    for (int i = -1000; i < 1000; i++) set.Add(b + i);
                break;
        }
        return set.ToArray();
    }

    public static RecordBatch Batch(int rows)
    {
        var ids = new Int64Array.Builder().Reserve(rows);
        var amounts = new DoubleArray.Builder().Reserve(rows);
        var cats = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(i);
            amounts.Append(i * 0.5);
            cats.Append("category_" + (i & 15));
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default))
            .Field(f => f.Name("amount").DataType(Apache.Arrow.Types.DoubleType.Default))
            .Field(f => f.Name("category").DataType(Apache.Arrow.Types.StringType.Default))
            .Build();
        return new RecordBatch(schema, [ids.Build(), amounts.Build(), cats.Build()], rows);
    }
}

/// <summary>
/// #610: applying a deletion vector to every batch of one 1M-row file, before (the DV expanded into a
/// <see cref="HashSet{T}"/>, probed per row) and after (<see cref="DeletionVectorBitmap"/> read in place,
/// clearing a selection mask). Each Scan includes loading the DV from its serialized bytes, as a scan does once
/// per file. Run with <c>--filter *DeletionVectorBenchmarks*</c>.
/// </summary>
[MemoryDiagnoser]
public class DeletionVectorBenchmarks
{
    [ParamsAllValues]
    public DvPattern Pattern { get; set; }

    [Params(65536, 1 << 20)]
    public int BatchRows { get; set; }

    private byte[] _dv = null!;
    private RecordBatch _batch = null!;
    private long _expectedRows;

    [GlobalSetup]
    public void Setup()
    {
        var positions = DvFixtures.Positions(Pattern);
        _dv = Pattern == DvPattern.Runs
            ? RunAwareWriter.Serialize(positions, allowRuns: true)
            : RoaringBitmapWriter.Serialize(positions);
        _batch = DvFixtures.Batch(BatchRows);
        _expectedRows = DvFixtures.FileRows - positions.Length;

        // Cross-check both against the truth before timing anything.
        Check(nameof(Scan_HashSet), Scan_HashSet());
        Check(nameof(Scan_Bitmap), Scan_Bitmap());
    }

    private void Check(string name, long rows)
    {
        if (rows != _expectedRows)
            throw new InvalidOperationException($"{name}: {rows} rows, expected {_expectedRows}");
    }

    [Benchmark]
    public int Load_HashSet() => new HashSet<long>(RoaringBitmapReader.Deserialize(_dv)).Count;

    [Benchmark]
    public long Load_Bitmap() => RoaringBitmapReader.Deserialize(_dv).Count;

    /// <summary>Before #610: the per-row HashSet probe.</summary>
    [Benchmark(Baseline = true)]
    public long Scan_HashSet()
    {
        var deleted = new HashSet<long>(RoaringBitmapReader.Deserialize(_dv));
        long rows = 0;
        for (long start = 0; start < DvFixtures.FileRows; start += BatchRows)
            rows += DeletionVectorFilter.Filter(_batch, deleted, start).Length;
        return rows;
    }

    /// <summary>After #610: the scan's own path.</summary>
    [Benchmark]
    public long Scan_Bitmap()
    {
        var deleted = RoaringBitmapReader.Deserialize(_dv);
        long rows = 0;
        for (long start = 0; start < DvFixtures.FileRows; start += BatchRows)
            rows += DeletionVectorFilter.Filter(_batch, deleted, start).Length;
        return rows;
    }
}

/// <summary>
/// #610: merging a deletion vector with two predicate masks to decide which rows of a third column to
/// materialize. "TwoStep" drops the DV's rows, then filters the survivors again by the predicates (each mask
/// gathered down to the survivors first); "Combined" ANDs the DV and both Arrow masks into one selection and
/// gathers once. Both combine masks word-wise, so the difference is the extra gathers alone.
/// </summary>
[MemoryDiagnoser]
public class SelectionMergeBenchmarks
{
    [Params(DvPattern.Sparse, DvPattern.Dense)]
    public DvPattern Pattern { get; set; }

    private const int BatchRows = 65536;
    private byte[] _dv = null!;
    private RecordBatch _batch = null!;
    private BooleanArray[] _predA = null!, _predB = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dv = RoaringBitmapWriter.Serialize(DvFixtures.Positions(Pattern));
        _batch = DvFixtures.Batch(BatchRows);
        var rng = new Random(502);
        int batches = DvFixtures.FileRows / BatchRows;
        _predA = new BooleanArray[batches];
        _predB = new BooleanArray[batches];
        for (int b = 0; b < batches; b++)
        {
            _predA[b] = Mask(rng);
            _predB[b] = Mask(rng);
        }
        if (TwoStep() != Combined())
            throw new InvalidOperationException("TwoStep and Combined disagree");
    }

    private static BooleanArray Mask(Random rng)
    {
        var b = new BooleanArray.Builder().Reserve(BatchRows);
        for (int i = 0; i < BatchRows; i++)
        {
            if (rng.Next(100) == 0) b.AppendNull(); else b.Append(rng.Next(2) == 0);
        }
        return b.Build();
    }

    [Benchmark(Baseline = true)]
    public long TwoStep()
    {
        var deleted = RoaringBitmapReader.Deserialize(_dv);
        var selection = new ulong[SelectionMask.WordCount(BatchRows)];
        var idx = new int[BatchRows];
        long rows = 0;
        for (int b = 0; b * BatchRows < DvFixtures.FileRows; b++)
        {
            long start = (long)b * BatchRows;
            SelectionMask.SelectAll(selection, BatchRows);
            deleted.ClearDeleted(start, BatchRows, selection);
            int n = SelectionMask.ToIndices(selection, idx);
            var live = ArrowCompute.Take(_batch, _batch.Schema, idx.AsSpan(0, n));
            var a = (BooleanArray)ArrowCompute.Take(_predA[b], idx.AsSpan(0, n));
            var c = (BooleanArray)ArrowCompute.Take(_predB[b], idx.AsSpan(0, n));

            var second = selection.AsSpan(0, SelectionMask.WordCount(n));
            SelectionMask.SelectAll(second, n);
            SelectionMask.And(second, n, a);
            SelectionMask.And(second, n, c);
            int m = SelectionMask.ToIndices(second, idx);
            rows += ArrowCompute.Take(live, live.Schema, idx.AsSpan(0, m)).Length;
        }
        return rows;
    }

    [Benchmark]
    public long Combined()
    {
        var deleted = RoaringBitmapReader.Deserialize(_dv);
        var selection = new ulong[SelectionMask.WordCount(BatchRows)];
        var idx = new int[BatchRows];
        long rows = 0;
        for (int b = 0; b * BatchRows < DvFixtures.FileRows; b++)
        {
            long start = (long)b * BatchRows;
            SelectionMask.SelectAll(selection, BatchRows);
            deleted.ClearDeleted(start, BatchRows, selection);
            SelectionMask.And(selection, BatchRows, _predA[b]);
            SelectionMask.And(selection, BatchRows, _predB[b]);
            int n = SelectionMask.ToIndices(selection, idx);
            rows += ArrowCompute.Take(_batch, _batch.Schema, idx.AsSpan(0, n)).Length;
        }
        return rows;
    }
}
