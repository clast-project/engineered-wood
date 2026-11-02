// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.DeletionVectors;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <see cref="DeletionVectorBitmap"/> against a plain set of positions, over every container kind (array,
/// bitmap, run), several sub-bitmaps, and query windows that start and end anywhere: mid-word, mid-container,
/// across container and sub-bitmap boundaries.
/// </summary>
public class DeletionVectorBitmapTests
{
    public enum Shape { Sparse, Dense, Runs, Mixed, Boundary, Full }

    public static TheoryData<Shape, RunMode, int> Cases()
    {
        var data = new TheoryData<Shape, RunMode, int>();
        foreach (Shape shape in Enum.GetValues(typeof(Shape)))
            foreach (RunMode mode in Enum.GetValues(typeof(RunMode)))
                for (int seed = 1; seed <= 3; seed++)
                    data.Add(shape, mode, seed);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void MatchesASetOfPositions(Shape shape, RunMode mode, int seed)
    {
        var rng = new Random(seed * 7919 + (int)shape * 31 + (int)mode);
        long[] deleted = Positions(shape, rng);
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize(deleted, mode));
        var oracle = new HashSet<long>(deleted);

        Assert.Equal(deleted.LongLength, bitmap.Count);
        Assert.Equal(deleted.Length == 0, bitmap.IsEmpty);
        Assert.Equal(deleted, bitmap.ToArray());

        foreach (long p in Probes(deleted, rng))
        {
            Assert.Equal(oracle.Contains(p), bitmap.Contains(p));
            long expectedNext = NextAtOrAfter(deleted, p);
            Assert.Equal(expectedNext != long.MaxValue, bitmap.TryGetNext(p, out long next));
            if (expectedNext != long.MaxValue)
                Assert.Equal(expectedNext, next);
        }

        for (int trial = 0; trial < 300; trial++)
        {
            long start = PickStart(deleted, rng);
            int length = rng.Next(4) switch
            {
                0 => rng.Next(0, 70),
                1 => rng.Next(0, 5000),
                2 => rng.Next(60000, 140000),
                _ => rng.Next(0, 300000),
            };
            CheckWindow(bitmap, deleted, start, length, rng);
        }
    }

    [Fact]
    public void SubBitmapsAboveTwoToThe32()
    {
        // Keys 0, 1 and 5: positions straddle the 2^32 boundary and skip whole sub-bitmaps.
        var rng = new Random(42);
        var deleted = new SortedSet<long>();
        foreach (long high in new[] { 0L, 1L, 5L })
        {
            long b = high << 32;
            for (int i = 0; i < 3000; i++) deleted.Add(b + rng.Next(0, 300000));
            for (long v = b + 65000; v < b + 66500; v++) deleted.Add(v);
        }
        for (long v = (1L << 32) - 700; v < (1L << 32) + 700; v++) deleted.Add(v);
        long[] sorted = deleted.ToArray();

        foreach (RunMode mode in Enum.GetValues(typeof(RunMode)))
        {
            var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize(sorted, mode));
            Assert.Equal(sorted, bitmap.ToArray());
            CheckWindow(bitmap, sorted, (1L << 32) - 1000, 2000, rng);
            CheckWindow(bitmap, sorted, (1L << 32) - 70000, 140001, rng);
            CheckWindow(bitmap, sorted, (2L << 32) - 5, 100, rng); // an absent sub-bitmap
            CheckWindow(bitmap, sorted, (5L << 32) + 64999, 1503, rng);
            for (int trial = 0; trial < 200; trial++)
                CheckWindow(bitmap, sorted, PickStart(sorted, rng), rng.Next(0, 200000), rng);
            Assert.False(bitmap.TryGetNext((5L << 32) + 400000, out _));
        }
    }

    [Fact]
    public void Empty()
    {
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize([], RunMode.Never));
        Assert.Same(DeletionVectorBitmap.Empty, bitmap);
        Assert.Equal(0, bitmap.Count);
        Assert.True(bitmap.IsEmpty);
        Assert.Empty(bitmap);
        Assert.False(bitmap.Contains(0));
        Assert.False(bitmap.TryGetNext(0, out _));
        Assert.False(bitmap.Intersects(0, int.MaxValue));
        var selection = new[] { ulong.MaxValue };
        bitmap.ClearDeleted(0, 64, selection);
        Assert.Equal(ulong.MaxValue, selection[0]);
    }

    [Fact]
    public void NegativePositionsAreNeverDeleted()
    {
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize([0, 5], RunMode.Never));
        Assert.False(bitmap.Contains(-1));
        Assert.True(bitmap.TryGetNext(-10, out long next));
        Assert.Equal(0, next);
        Assert.True(bitmap.Intersects(-1, 2));
        Assert.False(bitmap.Intersects(-1, 1));
    }

    [Fact]
    public void AWindowStartingFarBelowZeroSeesNoRow()
    {
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize([0, 5], RunMode.Never));
        Assert.False(bitmap.Intersects(long.MinValue, 1));
        Assert.False(bitmap.Intersects(long.MinValue + 1, long.MaxValue)); // [MinValue + 1, 0): no row 0
        Assert.True(bitmap.Intersects(long.MinValue + 2, long.MaxValue));  // [MinValue + 2, 1): holds row 0
    }

    [Theory]
    [InlineData(RunMode.Never)]
    [InlineData(RunMode.Always)]
    public void AContainerAtTheTopOfTheRangeDoesNotHideTheOthers(RunMode mode)
    {
        // The highest position a Delta DV can hold (high key int.MaxValue - 1, low bits all set): the container
        // search must still find the rows far below it.
        long top = ((long)(int.MaxValue - 1) << 32) | 0xFFFFFFFFL;
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize([0, 70000, top], mode));
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Intersects(0, 1));
        Assert.True(bitmap.Contains(70000));
        Assert.True(bitmap.Contains(top));
        Assert.True(bitmap.TryGetNext(1, out long next));
        Assert.Equal(70000, next);
        Assert.True(bitmap.TryGetNext(70001, out next));
        Assert.Equal(top, next);
        var selection = new[] { ulong.MaxValue, ulong.MaxValue };
        bitmap.ClearDeleted(0, 128, selection.AsSpan());
        Assert.Equal(new[] { ulong.MaxValue - 1, ulong.MaxValue }, selection);
        Assert.Equal(new long[] { 0, 70000, top }, bitmap.ToArray());
    }

    [Fact]
    public void AWindowRunningOffTheEndOfTheLongRange()
    {
        // start + length wraps; no Delta position lies there, so nothing is cleared and nothing intersects.
        long top = ((long)(int.MaxValue - 1) << 32) | 0xFFFFFFFFL;
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize([0, top], RunMode.Never));
        var selection = new[] { ulong.MaxValue };
        bitmap.ClearDeleted(long.MaxValue - 10, 64, selection);
        Assert.Equal(ulong.MaxValue, selection[0]);
        Assert.False(bitmap.Intersects(long.MaxValue - 10, 64));
        Assert.False(bitmap.TryGetNext(top + 1, out _));

        selection[0] = ulong.MaxValue;
        bitmap.ClearDeleted(top - 3, 64, selection);
        Assert.Equal(ulong.MaxValue & ~(1UL << 3), selection[0]);
    }

    [Fact]
    public void ClearDeletedValidatesArguments()
    {
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize([3], RunMode.Never));
        Assert.Throws<ArgumentOutOfRangeException>(() => bitmap.ClearDeleted(-1, 10, new ulong[1]));
        Assert.Throws<ArgumentOutOfRangeException>(() => bitmap.ClearDeleted(0, -1, new ulong[1]));
        Assert.Throws<ArgumentOutOfRangeException>(() => bitmap.ClearDeleted(0, 65, new ulong[1]));
        bitmap.ClearDeleted(0, 0, Span<ulong>.Empty);
    }

    [Fact]
    public void KeptRowsAndFilterMatchTheSet()
    {
        var rng = new Random(5);
        long[] deleted = Positions(Shape.Mixed, rng);
        var bitmap = RoaringBitmapReader.Deserialize(TestRoaringWriter.Serialize(deleted, RunMode.Optimal));
        var oracle = new HashSet<long>(deleted);

        for (int trial = 0; trial < 100; trial++)
        {
            long start = PickStart(deleted, rng);
            int length = rng.Next(0, 3000);
            var expected = Enumerable.Range(0, length).Where(i => !oracle.Contains(start + i)).ToArray();

            using (var kept = KeptRows.For(bitmap, start, length))
            {
                Assert.Equal(expected.Length, kept.Count);
                Assert.Equal(expected.Length == length, kept.AllKept);
                for (int n = 0; n < kept.Count; n++)
                    Assert.Equal(expected[n], kept[n]);
                if (!kept.AllKept)
                    Assert.Equal(expected, kept.Indices.ToArray());
            }

            var batch = IdBatch(length);
            var filtered = DeletionVectorFilter.Filter(batch, bitmap, start);
            if (expected.Length == length)
                Assert.Same(batch, filtered);
            Assert.Equal(expected, Ids(filtered));
            Assert.Equal(expected, Ids(DeletionVectorFilter.Filter(batch, oracle, start)));
        }
    }

    // ---- malformed input -------------------------------------------------------------------------------

    [Theory]
    [InlineData(RunMode.Never)]
    [InlineData(RunMode.Always)]
    public void EveryTruncationIsAFormatError(RunMode mode)
    {
        var rng = new Random(11);
        long[] deleted = Positions(Shape.Mixed, rng).Where(p => p < 4 * 65536).ToArray();
        byte[] blob = TestRoaringWriter.Serialize(deleted, mode);
        for (int length = 0; length < blob.Length; length++)
        {
            byte[] prefix = blob.AsSpan(0, length).ToArray();
            Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(prefix));
        }
    }

    [Fact]
    public void CorruptBytesAreRefusedOrReadConsistently()
    {
        // Whatever a flipped byte does, the reader either refuses it as a format error or yields a bitmap whose
        // queries stay in bounds: never an IndexOutOfRange, never an unsorted enumeration.
        var rng = new Random(3);
        foreach (RunMode mode in Enum.GetValues(typeof(RunMode)))
        {
            long[] deleted = Positions(Shape.Mixed, new Random(9)).Where(p => p < 6 * 65536).ToArray();
            byte[] blob = TestRoaringWriter.Serialize(deleted, mode);
            for (int trial = 0; trial < 1500; trial++)
            {
                byte[] bad = (byte[])blob.Clone();
                int flips = rng.Next(1, 4);
                for (int f = 0; f < flips; f++)
                {
                    // Bias toward the headers, where the structure lives.
                    int at = rng.Next(2) == 0 ? rng.Next(Math.Min(bad.Length, 80)) : rng.Next(bad.Length);
                    bad[at] = (byte)rng.Next(256);
                }

                DeletionVectorBitmap bitmap;
                try
                {
                    bitmap = RoaringBitmapReader.Deserialize(bad);
                }
                catch (DeltaFormatException)
                {
                    continue;
                }

                long[] values = bitmap.ToArray();
                Assert.Equal(values.LongLength, bitmap.Count);
                for (int i = 1; i < values.Length; i++)
                    Assert.True(values[i - 1] < values[i]);
                for (int w = 0; w < 5; w++)
                    CheckWindow(bitmap, values, PickStart(values, rng), rng.Next(0, 140000), rng);
            }
        }
    }

    [Fact]
    public void SubBitmapKeysMustAscend()
    {
        byte[] one = TestRoaringWriter.Serialize([1], RunMode.Never);
        byte[] sub = one.AsSpan(16).ToArray(); // one sub-bitmap's portable bitmap, after magic + count + key
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(Array((1, sub), (1, sub))));
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(Array((2, sub), (1, sub))));
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(Array((0x80000000u, sub))));
        // int.MaxValue is past Delta's MAX_REPRESENTABLE_VALUE; int.MaxValue - 1 is the top key it uses.
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(Array(((uint)int.MaxValue, sub))));
        Assert.Equal([((long)(int.MaxValue - 1) << 32) | 1],
            RoaringBitmapReader.Deserialize(Array(((uint)(int.MaxValue - 1), sub))).ToArray());
        Assert.Equal([1L, (3L << 32) | 1], RoaringBitmapReader.Deserialize(Array((0, sub), (3, sub))).ToArray());
    }

    [Theory]
    // key 0, card 2: values out of order.
    [InlineData("3a30000001000000000001001000000005000300")]
    // key 0, card 2: a duplicate value.
    [InlineData("3a30000001000000000001001000000005000500")]
    // two containers with keys 1 then 0.
    [InlineData("3a30000002000000010000000000000018000000" + "1a00000007000700")]
    // run container: overlapping runs [5,10] and [8,9].
    [InlineData("3b30000001" + "0000" + "0500" + "0200" + "05000500" + "08000100")]
    // run container: runs out of order, [8,9] then [5,6].
    [InlineData("3b30000001" + "0000" + "0300" + "0200" + "08000100" + "05000100")]
    // run container: a run past 65535.
    [InlineData("3b30000001" + "0000" + "0100" + "0100" + "ffff0100")]
    // run container: one run [5,7] (3 values) under a header claiming 100.
    [InlineData("3b30000001" + "0000" + "6300" + "0100" + "05000200")]
    public void MalformedContainersAreFormatErrors(string hex)
    {
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(Array((0, Hex(hex)))));
    }

    [Theory]
    [InlineData(5000, 4999)] // a header one short of the payload
    [InlineData(5000, 5001)] // and one over
    public void ABitmapContainerMustHoldWhatItsHeaderSays(int values, int headerCardinality)
    {
        // A bitmap's count is what Java's RoaringBitmap reports for it, so a header that disagrees with the
        // payload is refused rather than read one way here and another there.
        var words = new ulong[1024];
        for (int v = 0; v < values; v++)
            words[v >> 6] |= 1UL << (v & 63);
        using var ms = new MemoryStream();
        using (var bw = new BinaryWriter(ms))
        {
            bw.Write(12346u);                           // no-run cookie
            bw.Write(1u);                               // one container
            bw.Write((ushort)0);                        // key
            bw.Write((ushort)(headerCardinality - 1));  // header cardinality - 1
            bw.Write(16u);                              // offset of the payload
            foreach (ulong w in words) bw.Write(w);
        }
        Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(Array((0, ms.ToArray()))));
    }

    [Fact]
    public void TouchingRunsAreRead()
    {
        // [5,6] then [7,7]: CRoaring would have merged them, but they name no row twice.
        var bitmap = RoaringBitmapReader.Deserialize(
            Array((0, Hex("3b30000001" + "0000" + "0200" + "0200" + "05000100" + "07000000"))));
        Assert.Equal([5L, 6, 7], bitmap.ToArray());
        Assert.Equal(3, bitmap.Count);
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    // `sorted` is the truth: the deleted positions in ascending order.
    private static void CheckWindow(DeletionVectorBitmap bitmap, long[] sorted, long start, int length, Random rng)
    {
        int first = LowerBound(sorted, start), end = LowerBound(sorted, start + length);
        Assert.Equal(end > first, bitmap.Intersects(start, length));

        // Prefill with noise: ClearDeleted must only clear deleted rows' bits and leave every other bit alone,
        // including the bits past `length` in the last word and the guard word after it.
        int words = (length + 63) >> 6;
        var selection = new ulong[words + 1];
        for (int w = 0; w < selection.Length; w++)
            selection[w] = ((ulong)(uint)rng.Next() << 33) ^ ((ulong)(uint)rng.Next() << 2) ^ (ulong)(uint)rng.Next(4);
        var expected = (ulong[])selection.Clone();
        for (int k = first; k < end; k++)
        {
            long i = sorted[k] - start;
            expected[i >> 6] &= ~(1UL << (int)(i & 63));
        }

        bitmap.ClearDeleted(start, length, selection.AsSpan(0, words));

        for (int w = 0; w < selection.Length; w++)
            if (selection[w] != expected[w])
                Assert.Fail($"window [{start}, +{length}): word {w} is {selection[w]:x16}, expected {expected[w]:x16}");
    }

    private static int LowerBound(long[] sorted, long value)
    {
        int i = System.Array.BinarySearch(sorted, value);
        if (i < 0) return ~i;
        return i; // positions are distinct, so this is the only match
    }

    private static long[] Positions(Shape shape, Random rng)
    {
        var set = new SortedSet<long>();
        switch (shape)
        {
            case Shape.Sparse:
                for (int i = 0; i < 2000; i++) set.Add(rng.Next(0, 1 << 20));
                break;
            case Shape.Dense:
                for (int i = 0; i < 300000; i++) set.Add(rng.Next(0, 1 << 19));
                break;
            case Shape.Runs:
                for (int r = 0; r < 40; r++)
                {
                    long s = rng.Next(0, 1 << 20);
                    int len = rng.Next(1, 20000);
                    for (long v = s; v < s + len; v++) set.Add(v);
                }
                break;
            case Shape.Mixed:
                // One container of each kind and some that mix runs and singletons.
                for (int i = 0; i < 1000; i++) set.Add(rng.Next(0, 65536));                    // array
                for (int i = 0; i < 40000; i++) set.Add(65536 + rng.Next(0, 65536));           // bitmap
                for (long v = 2 * 65536 + 10; v < 3 * 65536 - 10; v++) set.Add(v);            // one run
                for (int i = 0; i < 200; i++)                                                   // runs + points
                {
                    long s = 3 * 65536 + rng.Next(0, 65536);
                    for (long v = s; v < s + rng.Next(1, 40) && v < 4 * 65536; v++) set.Add(v);
                }
                for (long v = 5 * 65536; v < 6 * 65536; v++) set.Add(v);                        // full
                set.Add(7 * 65536 + 65535);                                                     // last value
                break;
            case Shape.Boundary:
                for (long k = 1; k < 16; k++)
                    for (long v = k * 65536 - rng.Next(1, 1500); v < k * 65536 + rng.Next(1, 1500); v++)
                        set.Add(v);
                break;
            case Shape.Full:
                for (long v = 0; v < 3 * 65536; v++) set.Add(v);
                break;
        }
        return set.ToArray();
    }

    private static IEnumerable<long> Probes(long[] deleted, Random rng)
    {
        for (long p = 0; p < 20; p++) yield return p;
        foreach (long k in new long[] { 1, 2, 3, 5, 8, 16 })
            for (long d = -2; d <= 2; d++)
                yield return k * 65536 + d;
        for (int i = 0; i < 400; i++)
        {
            if (deleted.Length > 0)
            {
                long v = deleted[rng.Next(deleted.Length)];
                yield return v - 1;
                yield return v;
                yield return v + 1;
            }
            yield return rng.Next(0, 1 << 21);
        }
    }

    private static long PickStart(long[] deleted, Random rng) => rng.Next(4) switch
    {
        0 when deleted.Length > 0 => Math.Max(0, deleted[rng.Next(deleted.Length)] - rng.Next(0, 200)),
        1 => rng.Next(0, 32) * 65536L + rng.Next(-70, 70) is var p && p >= 0 ? p : 0,
        _ => rng.Next(0, 1 << 21),
    };

    private static long NextAtOrAfter(long[] sorted, long position)
    {
        int i = System.Array.BinarySearch(sorted, position);
        if (i < 0) i = ~i;
        return i < sorted.Length ? sorted[i] : long.MaxValue;
    }

    private static RecordBatch IdBatch(int rows)
    {
        var ids = new Int32Array.Builder();
        for (int i = 0; i < rows; i++) ids.Append(i);
        var schema = new Apache.Arrow.Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();
        return new RecordBatch(schema, [ids.Build()], rows);
    }

    private static int[] Ids(RecordBatch batch)
    {
        var col = (Int32Array)batch.Column(0);
        return Enumerable.Range(0, col.Length).Select(i => col.GetValue(i)!.Value).ToArray();
    }

    private static byte[] Array(params (uint Key, byte[] Bitmap)[] subBitmaps)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write(1681511377u);
        bw.Write((long)subBitmaps.Length);
        foreach (var (key, bitmap) in subBitmaps)
        {
            bw.Write(key);
            bw.Write(bitmap);
        }
        bw.Flush();
        return ms.ToArray();
    }

    private static byte[] Hex(string hex) =>
        Enumerable.Range(0, hex.Length / 2).Select(i => Convert.ToByte(hex.Substring(2 * i, 2), 16)).ToArray();
}

/// <summary>Whether <see cref="TestRoaringWriter"/> writes run containers.</summary>
public enum RunMode
{
    /// <summary>Array and bitmap containers only, as EW and Delta Spark write.</summary>
    Never,
    /// <summary>A run container wherever it is the smallest form, as CRoaring's <c>runOptimize</c> picks.</summary>
    Optimal,
    /// <summary>A run container for every container, however large.</summary>
    Always,
}

/// <summary>
/// Writes sorted positions as a Delta RoaringBitmapArray in the portable CRoaring format, choosing each
/// container's kind by <see cref="RunMode"/>. The library's own writer never emits runs.
/// </summary>
internal static class TestRoaringWriter
{
    public static byte[] Serialize(long[] sorted, RunMode mode)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        bw.Write(1681511377u);

        var groups = sorted.GroupBy(p => (uint)(p >> 32)).OrderBy(g => g.Key).ToList();
        bw.Write((long)groups.Count);
        foreach (var g in groups)
        {
            bw.Write(g.Key);
            var containers = g.GroupBy(p => (ushort)((p >> 16) & 0xFFFF)).OrderBy(c => c.Key)
                .Select(c => (c.Key, Values: c.Select(p => (ushort)(p & 0xFFFF)).ToArray())).ToList();
            int n = containers.Count;
            var runs = containers.Select(c => Runs(c.Values)).ToList();
            var kinds = new int[n]; // 0 array, 1 bitmap, 2 run
            for (int i = 0; i < n; i++)
            {
                int card = containers[i].Values.Length;
                int arr = 2 * card, bmp = 8192, run = 2 + 4 * runs[i].Count;
                bool useRun = mode == RunMode.Always || (mode == RunMode.Optimal && run < Math.Min(arr, bmp));
                kinds[i] = useRun ? 2 : card > 4096 ? 1 : 0;
            }
            bool anyRun = kinds.Contains(2);
            long start = ms.Position;
            bool offsets;
            if (anyRun)
            {
                bw.Write((uint)(12347 | ((n - 1) << 16)));
                var rb = new byte[(n + 7) / 8];
                for (int i = 0; i < n; i++)
                    if (kinds[i] == 2)
                        rb[i >> 3] |= (byte)(1 << (i & 7));
                bw.Write(rb);
                offsets = n >= 4;
            }
            else
            {
                bw.Write(12346u);
                bw.Write((uint)n);
                offsets = true;
            }
            for (int i = 0; i < n; i++)
            {
                bw.Write(containers[i].Key);
                bw.Write((ushort)(containers[i].Values.Length - 1));
            }
            var sizes = new int[n];
            for (int i = 0; i < n; i++)
                sizes[i] = kinds[i] switch { 2 => 2 + 4 * runs[i].Count, 1 => 8192, _ => 2 * containers[i].Values.Length };
            if (offsets)
            {
                long at = ms.Position + 4L * n - start;
                for (int i = 0; i < n; i++)
                {
                    bw.Write((uint)at);
                    at += sizes[i];
                }
            }
            for (int i = 0; i < n; i++)
            {
                var v = containers[i].Values;
                switch (kinds[i])
                {
                    case 2:
                        bw.Write((ushort)runs[i].Count);
                        foreach (var (s, len) in runs[i])
                        {
                            bw.Write(s);
                            bw.Write(len);
                        }
                        break;
                    case 1:
                        var words = new ulong[1024];
                        foreach (var x in v) words[x >> 6] |= 1UL << (x & 63);
                        foreach (var w in words) bw.Write(w);
                        break;
                    default:
                        foreach (var x in v) bw.Write(x);
                        break;
                }
            }
        }
        bw.Flush();
        return ms.ToArray();
    }

    private static List<(ushort Start, ushort LengthMinus1)> Runs(ushort[] v)
    {
        var r = new List<(ushort, ushort)>();
        int i = 0;
        while (i < v.Length)
        {
            int j = i;
            while (j + 1 < v.Length && v[j + 1] == v[j] + 1) j++;
            r.Add((v[i], (ushort)(v[j] - v[i])));
            i = j + 1;
        }
        return r;
    }
}
