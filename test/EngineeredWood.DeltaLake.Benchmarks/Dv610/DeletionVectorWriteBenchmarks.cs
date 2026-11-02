// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Table;

namespace EngineeredWood.DeltaLake.Benchmarks.Dv610;

/// <summary>What a DML adds to a file's existing deletion vector.</summary>
public enum DvAddition
{
    /// <summary>1,000 rows scattered over the file.</summary>
    Scattered,
    /// <summary>One contiguous run of 100,000 rows.</summary>
    Range,
}

/// <summary>
/// #610 part 2: a DML's write of a deletion vector over a 1M-row file that already has one: read the existing
/// vector, add the new rows (noting which were newly deleted, as the conflict check needs), and serialize.
/// "HashSet" is the path before (expand into a set, add, serialize through a SortedDictionary entry per row);
/// "Bitmap" is the container-at-a-time union. CdfDiff is the change feed's pair of set differences between
/// the old vector and the new. Run with <c>--filter *DeletionVectorWriteBenchmarks*</c>.
/// </summary>
[MemoryDiagnoser]
public class DeletionVectorWriteBenchmarks
{
    [Params(DvPattern.Sparse, DvPattern.Dense, DvPattern.RunsNoRun)]
    public DvPattern Pattern { get; set; }

    [ParamsAllValues]
    public DvAddition Addition { get; set; }

    private byte[] _existing = null!, _updated = null!;
    private long[] _added = null!;

    [GlobalSetup]
    public void Setup()
    {
        long[] existing = DvFixtures.Positions(Pattern);
        _existing = LegacyRoaringBitmapWriter.Serialize(existing);
        var rng = new Random(611);
        _added = Addition == DvAddition.Scattered
            ? Enumerable.Range(0, 1000).Select(_ => (long)rng.Next(DvFixtures.FileRows)).Distinct().OrderBy(p => p).ToArray()
            : Enumerable.Range(0, 100_000).Select(i => 300_000L + i).ToArray();
        _updated = LegacyRoaringBitmapWriter.Serialize(existing.Concat(_added));

        // Same answer both ways, and the sizes each way writes (bytes stored inline or in a .bin).
        var before = Union_HashSet();
        var after = Union_Bitmap();
        if (!RoaringBitmapReader.Deserialize(before).SequenceEqual(RoaringBitmapReader.Deserialize(after)))
            throw new InvalidOperationException("Union_HashSet and Union_Bitmap disagree");
        if (CdfDiff_HashSet() != CdfDiff_Bitmap())
            throw new InvalidOperationException("CdfDiff_HashSet and CdfDiff_Bitmap disagree");
        Console.WriteLine($"// {Pattern}/{Addition}: serialized {before.Length} bytes before, {after.Length} after");
    }

    [Benchmark(Baseline = true)]
    public byte[] Union_HashSet()
    {
        var deleted = new HashSet<long>(RoaringBitmapReader.Deserialize(_existing));
        var newlyDeleted = new List<long>();
        foreach (long p in _added)
            if (deleted.Add(p))
                newlyDeleted.Add(p);
        return LegacyRoaringBitmapWriter.Serialize(deleted);
    }

    [Benchmark]
    public byte[] Union_Bitmap()
    {
        var newlyDeleted = new List<long>();
        return RoaringBitmapReader.Deserialize(_existing)
            .Union(DeletionVectorBuilder.SortedDistinct(_added), newlyDeleted)
            .SerializedBytes();
    }

    [Benchmark]
    public long CdfDiff_HashSet()
    {
        var oldRows = new HashSet<long>(RoaringBitmapReader.Deserialize(_existing));
        var newRows = new HashSet<long>(RoaringBitmapReader.Deserialize(_updated));
        var newlyDeleted = new HashSet<long>(newRows);
        newlyDeleted.ExceptWith(oldRows);
        var reinserted = new HashSet<long>(oldRows);
        reinserted.ExceptWith(newRows);
        return newlyDeleted.Count * 1_000_000L + reinserted.Count;
    }

    [Benchmark]
    public long CdfDiff_Bitmap()
    {
        var oldRows = RoaringBitmapReader.Deserialize(_existing);
        var newRows = RoaringBitmapReader.Deserialize(_updated);
        return newRows.Except(oldRows).Count * 1_000_000L + oldRows.Except(newRows).Count;
    }
}

/// <summary>
/// #610 part 2: a predicate DELETE's per-batch step over a 1M-row file with an existing deletion vector: the
/// rows the predicate matches (10% TRUE, 1% NULL) that the vector does not already delete. "HashSet" probes a
/// set and the mask row by row; "Mask" clears the vector's rows from a selection and ANDs the predicate mask in
/// a word at a time. Run with <c>--filter *DeleteMergeBenchmarks*</c>.
/// </summary>
[MemoryDiagnoser]
public class DeleteMergeBenchmarks
{
    [Params(DvPattern.Sparse, DvPattern.Dense, DvPattern.RunsNoRun)]
    public DvPattern Pattern { get; set; }

    private const int BatchRows = 65536;
    private byte[] _dv = null!;
    private BooleanArray[] _masks = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dv = LegacyRoaringBitmapWriter.Serialize(DvFixtures.Positions(Pattern));
        var rng = new Random(439);
        _masks = new BooleanArray[DvFixtures.FileRows / BatchRows];
        for (int b = 0; b < _masks.Length; b++)
        {
            var builder = new BooleanArray.Builder().Reserve(BatchRows);
            for (int i = 0; i < BatchRows; i++)
            {
                int r = rng.Next(100);
                if (r == 0) builder.AppendNull(); else builder.Append(r <= 10);
            }
            _masks[b] = builder.Build();
        }
        if (Scan_HashSet() != Scan_Mask())
            throw new InvalidOperationException("Scan_HashSet and Scan_Mask disagree");
    }

    [Benchmark(Baseline = true)]
    public long Scan_HashSet()
    {
        var deleted = new HashSet<long>(RoaringBitmapReader.Deserialize(_dv));
        var newDeleted = new List<long>();
        for (int b = 0; b < _masks.Length; b++)
        {
            long firstRow = (long)b * BatchRows;
            var mask = _masks[b];
            var matchRows = new List<int>();
            for (int i = 0; i < BatchRows; i++)
            {
                long abs = firstRow + i;
                if (deleted.Contains(abs))
                    continue;
                if (!mask.IsNull(i) && mask.GetValue(i) == true)
                {
                    newDeleted.Add(abs);
                    matchRows.Add(i);
                }
            }
        }
        return newDeleted.Count;
    }

    [Benchmark]
    public long Scan_Mask()
    {
        var deleted = RoaringBitmapReader.Deserialize(_dv);
        var newDeleted = new List<long>();
        for (int b = 0; b < _masks.Length; b++)
        {
            long firstRow = (long)b * BatchRows;
            var matchRows = DeltaTable.MatchingLiveRows(deleted, firstRow, BatchRows, _masks[b]);
            foreach (int i in matchRows)
                newDeleted.Add(firstRow + i);
        }
        return newDeleted.Count;
    }
}
