// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;

namespace EngineeredWood.Core.Tests.Encodings;

/// <summary>
/// Portable roaring bitmaps that use RUN containers, as CRoaring writes them. The goldens were produced by
/// pyroaring 1.2.0 (CRoaring): <c>BitMap(values); run_optimize(); serialize()</c>, where container
/// <c>c</c> holds, for a "run" container, the runs [c*65536+100, +1100) and [c*65536+5000, +5003), and for an
/// "array" container the values c*65536 + {7, 300, 65535}.
///
/// <para>The run cookie carries an offset header once there are 4 or more containers (the spec's
/// NO_OFFSET_THRESHOLD). The reader used to assume it never did, so every run-cookie bitmap with 4 or more
/// containers was misread from the first container on.</para>
/// </summary>
public class RoaringBitmapTests
{
    // 3 containers [run, array, run]: no offset header.
    internal const string Runs3 =
        "3b300200050000ea03010002000200ea0302006400e7038813020007002c01ffff02006400e70388130200";

    // 4 containers [run, array, run, array]: the threshold, offset header present.
    internal const string Runs4 =
        "3b300300050000ea03010002000200ea0303000200250000002f000000350000003f00000002006400e7038813020007002c01ffff02006400e7038813020007002c01ffff";

    // 5 containers [array, run, run, array, run].
    internal const string Runs5 =
        "3b30040016000002000100ea030200ea03030002000400ea032d000000330000003d000000470000004d00000007002c01ffff02006400e7038813020002006400e7038813020007002c01ffff02006400e70388130200";

    internal static readonly string[] Runs3Kinds = ["run", "array", "run"];
    internal static readonly string[] Runs4Kinds = ["run", "array", "run", "array"];
    internal static readonly string[] Runs5Kinds = ["array", "run", "run", "array", "run"];

    public static TheoryData<string, string> Goldens => new()
    {
        { nameof(Runs3), Runs3 },
        { nameof(Runs4), Runs4 },
        { nameof(Runs5), Runs5 },
    };

    [Theory]
    [MemberData(nameof(Goldens))]
    public void RunContainers_DecodeEveryValue_InBothFormats(string name, string hex)
    {
        byte[] data = FromHex(hex);
        long[] expected = Expected(KindsFor(name));

        foreach (var format in new[] { RoaringBitmap.RoaringFormat.CRoaring, RoaringBitmap.RoaringFormat.SpecCompliant })
        {
            var actual = new List<long>();
            int consumed = RoaringBitmap.DeserializePortable(data, actual.Add, format);

            Assert.Equal(expected, actual);
            // The whole blob, exactly: a RoaringBitmapArray reader advances to the next sub-bitmap by this.
            Assert.Equal(data.Length, consumed);
        }
    }

    internal static string[] KindsFor(string name) => name switch
    {
        nameof(Runs3) => Runs3Kinds,
        nameof(Runs4) => Runs4Kinds,
        nameof(Runs5) => Runs5Kinds,
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    /// <summary>The values the golden generator put in each container, ascending.</summary>
    internal static long[] Expected(string[] kinds)
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
        return [.. values];
    }

    internal static byte[] FromHex(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = Convert.ToByte(hex.Substring(2 * i, 2), 16);
        return bytes;
    }
}
