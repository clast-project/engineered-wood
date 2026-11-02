// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Writing deletion vectors: <see cref="DeletionVectorBitmap.FromPositions"/>, <see cref="DeletionVectorBitmap.Union(IEnumerable{long})"/>
/// and <see cref="DeletionVectorBitmap.Except"/> against plain sets, and the bytes they write against CRoaring's.
/// </summary>
public class DeletionVectorBuilderTests
{
    // pyroaring 1.2.0 (CRoaring) goldens, BitMap(values); run_optimize(); serialize(), as in
    // EngineeredWood.Core.Tests.Encodings.RoaringBitmapTests: a "run" container c holds [c*65536+100, +1100) and
    // [c*65536+5000, +5003); an "array" container holds c*65536 + {7, 300, 65535}.
    private const string Runs3 =
        "3b300200050000ea03010002000200ea0302006400e7038813020007002c01ffff02006400e70388130200";
    private const string Runs4 =
        "3b300300050000ea03010002000200ea0303000200250000002f000000350000003f00000002006400e7038813020007002c01ffff02006400e7038813020007002c01ffff";
    private const string Runs5 =
        "3b30040016000002000100ea030200ea03030002000400ea032d000000330000003d000000470000004d00000007002c01ffff02006400e7038813020002006400e7038813020007002c01ffff02006400e70388130200";

    public static TheoryData<string, string[]> Goldens => new()
    {
        { Runs3, ["run", "array", "run"] },
        { Runs4, ["run", "array", "run", "array"] },
        { Runs5, ["array", "run", "run", "array", "run"] },
    };

    [Theory]
    [MemberData(nameof(Goldens))]
    public void WritesTheBytesCRoaringWrites(string hex, string[] kinds)
    {
        // The builder picks each container's form as runOptimize does and lays the bitmap out as CRoaring does,
        // so the portable bitmap after the RoaringBitmapArray framing is CRoaring's, byte for byte.
        byte[] blob = RoaringBitmapWriter.Serialize(Values(kinds));

        Assert.Equal(1681511377u, BinaryPrimitives.ReadUInt32LittleEndian(blob));
        Assert.Equal(1L, BinaryPrimitives.ReadInt64LittleEndian(blob.AsSpan(4)));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(blob.AsSpan(12)));
        Assert.Equal(hex, BitConverter.ToString(blob, 16).Replace("-", "").ToLowerInvariant());
    }

    [Fact]
    public void WithoutRuns_WritesTheNoRunCookie()
    {
        byte[] blob = RoaringBitmapWriter.Serialize(Values(["run", "array", "run", "array"]), allowRuns: false);
        Assert.Equal(12346u, BinaryPrimitives.ReadUInt32LittleEndian(blob.AsSpan(16)));
        Assert.Equal(Values(["run", "array", "run", "array"]), RoaringBitmapReader.Deserialize(blob).ToArray());
    }

    [Theory]
    [InlineData(0, 1)]            // one value
    [InlineData(0, 4096)]         // a range is a run whatever its length:
    [InlineData(0, 4097)]         //   6 bytes, against 8 KB as an array or a bitmap
    [InlineData(65535, 2)]        // a run across a container boundary
    [InlineData(0, 65536)]        // a full container: one run
    [InlineData(0, 3 * 65536)]    // three full containers
    public void ARangeIsOneRunPerContainer(long start, int count)
    {
        long[] positions = Enumerable.Range(0, count).Select(i => start + i).ToArray();
        var bitmap = DeletionVectorBitmap.FromPositions(positions);
        Assert.Equal(positions, bitmap.ToArray());
        Assert.Equal(count, bitmap.Count);
        int containers = (int)(((start + count - 1) >> 16) - (start >> 16) + 1);
        // magic + count + key + run cookie + run bitmap + per container (descriptor + 2-byte count + one run)
        // (+ offsets from 4 containers): a range costs bytes per container, not per row.
        Assert.True(bitmap.SerializedBytes().Length <= 16 + 4 + 1 + containers * (4 + 2 + 4 + 4),
            $"{bitmap.SerializedBytes().Length} bytes for a {count}-row range");
    }

    [Theory]
    [InlineData(1, "array")]
    [InlineData(4096, "array")]   // the largest array
    [InlineData(4097, "bitmap")]  // the smallest bitmap
    [InlineData(30000, "bitmap")]
    public void ScatteredValuesCrossFromArrayToBitmapAt4096(int count, string kind)
    {
        // Every other value: no two adjacent, so runs never win and the array/bitmap boundary decides.
        long[] positions = Enumerable.Range(0, count).Select(i => 2L * i).ToArray();
        byte[] blob = RoaringBitmapWriter.Serialize(positions);
        Assert.Equal(12346u, BinaryPrimitives.ReadUInt32LittleEndian(blob.AsSpan(16))); // no runs at all

        var containers = new List<EngineeredWood.Encodings.RoaringContainer>();
        EngineeredWood.Encodings.RoaringBitmap.ParseContainers(
            blob.AsSpan(16), EngineeredWood.Encodings.RoaringBitmap.RoaringFormat.CRoaring, containers);
        Assert.Equal(kind, Assert.Single(containers).Kind.ToString().ToLowerInvariant());
        Assert.Equal(positions, RoaringBitmapReader.Deserialize(blob).ToArray());
    }

    public static TheoryData<int, bool> Seeds()
    {
        var data = new TheoryData<int, bool>();
        for (int seed = 1; seed <= 12; seed++)
        {
            data.Add(seed, true);
            data.Add(seed, false);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void UnionAndExceptMatchSets(int seed, bool allowRuns)
    {
        var rng = new Random(seed);
        var a = RandomPositions(rng);
        var b = RandomPositions(rng);
        var bitmapA = RoaringBitmapReader.Deserialize(RoaringBitmapWriter.Serialize(a, allowRuns));
        var bitmapB = RoaringBitmapReader.Deserialize(RoaringBitmapWriter.Serialize(b, allowRuns));

        var union = new SortedSet<long>(a);
        union.UnionWith(b);
        Assert.Equal(union, bitmapA.Union(b).ToArray());
        Assert.Equal(union.Count, bitmapA.Union(b).Count);

        var newly = new List<long>();
        long[] bSorted = b.Distinct().OrderBy(p => p).ToArray();
        var viaSpan = bitmapA.Union(bSorted, newly);
        Assert.Equal(union, viaSpan.ToArray());
        Assert.Equal(bSorted.Where(p => !a.Contains(p)), newly);

        var aMinusB = new SortedSet<long>(a);
        aMinusB.ExceptWith(b);
        Assert.Equal(aMinusB, bitmapA.Except(bitmapB).ToArray());
        Assert.Equal(aMinusB.Count, bitmapA.Except(bitmapB).Count);
        var bMinusA = new SortedSet<long>(b);
        bMinusA.ExceptWith(a);
        Assert.Equal(bMinusA, bitmapB.Except(bitmapA).ToArray());

        // Every result is a well-formed vector: it re-reads to itself from its own bytes.
        foreach (var result in new[] { viaSpan, bitmapA.Except(bitmapB), bitmapB.Except(bitmapA) })
            Assert.Equal(result.ToArray(), RoaringBitmapReader.Deserialize(result.SerializedBytes()).ToArray());
    }

    [Fact]
    public void FromPositionsTakesAnyOrderAndDuplicates()
    {
        var bitmap = DeletionVectorBitmap.FromPositions([70000, 5, 5, 3, 70000, 1L << 32]);
        Assert.Equal([3L, 5, 70000, 1L << 32], bitmap.ToArray());
        Assert.Equal(4, bitmap.Count);
    }

    [Fact]
    public void EmptyCases()
    {
        Assert.Same(DeletionVectorBitmap.Empty, DeletionVectorBitmap.FromPositions([]));
        Assert.Equal(12, DeletionVectorBitmap.Empty.SerializedBytes().Length);
        Assert.Empty(RoaringBitmapReader.Deserialize(DeletionVectorBitmap.Empty.SerializedBytes()));

        var some = DeletionVectorBitmap.FromPositions([1, 2, 3]);
        Assert.Same(some, some.Union([]));
        Assert.Same(some, some.Except(DeletionVectorBitmap.Empty));
        Assert.True(some.Except(some).IsEmpty);
        Assert.Equal([1L, 2, 3], DeletionVectorBitmap.Empty.Union([3, 2, 1]).ToArray());
    }

    [Fact]
    public void RefusesPositionsADeltaVectorCannotHold()
    {
        long max = ((long)(int.MaxValue - 1) << 32) | 0xFFFFFFFFL;
        Assert.Equal([max], DeletionVectorBitmap.FromPositions([max]).ToArray());
        // Each names the argument its caller passed, not an internal one.
        Assert.Equal("positions",
            Assert.Throws<ArgumentOutOfRangeException>(() => DeletionVectorBitmap.FromPositions([max + 1])).ParamName);
        Assert.Equal("positions",
            Assert.Throws<ArgumentOutOfRangeException>(() => DeletionVectorBitmap.FromPositions([-1])).ParamName);
        Assert.Equal("positions",
            Assert.Throws<ArgumentOutOfRangeException>(() => DeletionVectorBitmap.Empty.Union([5, -1])).ParamName);
        var writer = new DeletionVectorWriter(new EngineeredWood.IO.Local.LocalTableFileSystem(Path.GetTempPath()));
        Assert.Equal("deletedRowIndices", Assert.Throws<ArgumentOutOfRangeException>(
            () => writer.CreateAsync(new long[] { -1 }).AsTask().GetAwaiter().GetResult()).ParamName);
    }

    [Fact]
    public void TheSpanUnionRequiresAscendingDistinctPositions()
    {
        var some = DeletionVectorBitmap.FromPositions([1]);
        Assert.Throws<ArgumentException>(() => some.Union(new long[] { 70000, 5 }, null));
        Assert.Throws<ArgumentException>(() => some.Union(new long[] { 5, 5 }, null));
        Assert.Throws<ArgumentException>(() => some.Union(new long[] { 5, 4 }, null));
    }

    [Fact]
    public void UnionKeepsTheExistingContainersForm()
    {
        // Each kind of existing container (array, bitmap, run) ORed into the scratch correctly, with new
        // positions inside and outside it.
        var array = Enumerable.Range(0, 100).Select(i => (long)i * 7);                      // container 0
        var dense = Enumerable.Range(0, 30000).Select(i => 65536L + 2 * i);                  // container 1
        var run = Enumerable.Range(0, 20000).Select(i => 2 * 65536L + 100 + i);              // container 2
        long[] existing = [.. array, .. dense, .. run];
        var bitmap = DeletionVectorBitmap.FromPositions(existing);
        long[] added = [1, 7, 65537, 65538, 2 * 65536 + 99, 2 * 65536 + 150, 2 * 65536 + 20100, 5 * 65536];

        var newly = new List<long>();
        var union = bitmap.Union(added, newly);

        var expected = new SortedSet<long>(existing);
        expected.UnionWith(added);
        Assert.Equal(expected, union.ToArray());
        Assert.Equal(added.Where(p => !existing.Contains(p)), newly);
    }

    private static HashSet<long> RandomPositions(Random rng)
    {
        var set = new HashSet<long>();
        int shape = rng.Next(4);
        for (int k = 0; k < 6; k++)
        {
            long b = (long)rng.Next(0, 8) * 65536 + (rng.Next(4) == 0 ? 1L << 32 : 0);
            switch ((shape + k) % 4)
            {
                case 0: // sparse
                    for (int i = 0; i < rng.Next(1, 300); i++) set.Add(b + rng.Next(65536));
                    break;
                case 1: // dense
                    for (int i = 0; i < 20000; i++) set.Add(b + rng.Next(65536));
                    break;
                case 2: // runs
                    for (int r = 0; r < rng.Next(1, 20); r++)
                    {
                        int s = rng.Next(65536), len = rng.Next(1, 3000);
                        for (int i = s; i < Math.Min(65536, s + len); i++) set.Add(b + i);
                    }
                    break;
                default: // full container
                    for (int i = 0; i < 65536; i++) set.Add(b + i);
                    break;
            }
        }
        return set;
    }

    private static long[] Values(string[] kinds)
    {
        var values = new List<long>();
        for (int c = 0; c < kinds.Length; c++)
        {
            long b = c * 65536L;
            if (kinds[c] == "run")
            {
                for (long v = b + 100; v < b + 1100; v++) values.Add(v);
                for (long v = b + 5000; v < b + 5003; v++) values.Add(v);
            }
            else
            {
                values.Add(b + 7);
                values.Add(b + 300);
                values.Add(b + 65535);
            }
        }
        return values.ToArray();
    }
}
