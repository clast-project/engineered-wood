// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Benchmarks.Dv610;

/// <summary>
/// Serializes sorted positions as a Delta RoaringBitmapArray, picking the smallest container
/// per chunk the way CRoaring's runOptimize would (array / bitmap / run). EW's own writer never emits runs;
/// this exists so the benchmark can exercise run containers (as range-adding writers produce them).
/// </summary>
internal static class RunAwareWriter
{
    public static byte[] Serialize(long[] sorted, bool allowRuns)
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
                .Select(c => (Key: c.Key, Values: c.Select(p => (ushort)(p & 0xFFFF)).ToArray())).ToList();
            int n = containers.Count;
            var runs = containers.Select(c => Runs(c.Values)).ToList();
            var kinds = new int[n]; // 0 array, 1 bitmap, 2 run
            for (int i = 0; i < n; i++)
            {
                int card = containers[i].Values.Length;
                int arr = 2 * card, bmp = 8192, run = 2 + 4 * runs[i].Count;
                kinds[i] = allowRuns && run < Math.Min(arr, bmp) ? 2 : card > 4096 ? 1 : 0;
            }
            bool anyRun = kinds.Contains(2);
            long start = ms.Position;
            bool offsets;
            if (anyRun)
            {
                bw.Write((uint)(12347 | ((n - 1) << 16)));
                var rb = new byte[(n + 7) / 8];
                for (int i = 0; i < n; i++) if (kinds[i] == 2) rb[i >> 3] |= (byte)(1 << (i & 7));
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
                for (int i = 0; i < n; i++) { bw.Write((uint)at); at += sizes[i]; }
            }
            for (int i = 0; i < n; i++)
            {
                var v = containers[i].Values;
                switch (kinds[i])
                {
                    case 2:
                        bw.Write((ushort)runs[i].Count);
                        foreach (var (s, len) in runs[i]) { bw.Write(s); bw.Write(len); }
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
