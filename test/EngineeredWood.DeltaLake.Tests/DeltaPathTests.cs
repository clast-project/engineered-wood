// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <para>Pins <see cref="DeltaPath.EscapePathName(string)"/> against Spark's
/// <c>ExternalCatalogUtils.escapePathName</c> — including the fact that Spark's escape table is
/// PLATFORM-DEPENDENT, adding <c>' '</c>, <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c> under
/// <c>if (Shell.WINDOWS)</c>.</para>
///
/// <para>Both sets are asserted on every machine by driving the internal overload directly, so the
/// POSIX expectations do not stop being checked just because CI runs on Windows (or vice versa). The
/// one platform-conditional test is the one that must be: that the PUBLIC entry point picks the set
/// matching the machine it is running on.</para>
///
/// <para>The expectations here are transcribed from a direct enumeration of
/// <c>escapePathName</c> over the whole ASCII range on pyspark 4.0.1 / delta-spark 4.0.0. The live
/// comparison against a running Spark lives in <c>SparkInteropTests</c>; this file is the fast,
/// toolchain-free copy of the same ground truth.</para>
/// </summary>
public class DeltaPathTests
{
    /// <summary>Not <c>OperatingSystem.IsWindows()</c>: this project also targets net472.</summary>
    private static bool IsWindows => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    /// <summary>Spark's escape set on POSIX: Hive's table, minus NUL, and with no closing brace.</summary>
    [Theory]
    // Left alone — the characters Windows objects to but POSIX does not.
    [InlineData("a b", "a b")]
    [InlineData("a b ", "a b ")]
    [InlineData("a<b", "a<b")]
    [InlineData("a>b", "a>b")]
    [InlineData("a|b", "a|b")]
    // Left alone everywhere. '}' is the one EW used to escape and Spark never has.
    [InlineData("a}b", "a}b")]
    [InlineData("café", "café")]
    [InlineData("日本", "日本")]
    [InlineData("a-b_c.d~e", "a-b_c.d~e")]
    // Escaped everywhere.
    [InlineData("a\"b", "a%22b")]
    [InlineData("a#b", "a%23b")]
    [InlineData("a%b", "a%25b")]
    [InlineData("a'b", "a%27b")]
    [InlineData("a*b", "a%2Ab")]
    [InlineData("a/b", "a%2Fb")]
    [InlineData("a:b", "a%3Ab")]
    [InlineData("a=b", "a%3Db")]
    [InlineData("a?b", "a%3Fb")]
    [InlineData("a[b", "a%5Bb")]
    [InlineData("a\\b", "a%5Cb")]
    [InlineData("a]b", "a%5Db")]
    [InlineData("a^b", "a%5Eb")]
    [InlineData("a{b", "a%7Bb")]
    [InlineData("a\tb", "a%09b")]
    [InlineData("ab", "a%7Fb")]
    [InlineData("a b#c?d", "a b%23c%3Fd")]
    public void EscapePathName_PosixRules_MatchesSpark(string value, string expected) =>
        Assert.Equal(expected, DeltaPath.EscapePathName(value, windowsRules: false));

    /// <summary>Spark's escape set on Windows: the POSIX set plus <c>' '</c>, <c>'&lt;'</c>,
    /// <c>'&gt;'</c> and <c>'|'</c>. Only those four differ; nothing else moves.</summary>
    [Theory]
    [InlineData("a b", "a%20b")]
    [InlineData("a b ", "a%20b%20")]
    [InlineData("a<b", "a%3Cb")]
    [InlineData("a>b", "a%3Eb")]
    [InlineData("a|b", "a%7Cb")]
    // Unchanged from the POSIX set.
    [InlineData("a}b", "a}b")]
    [InlineData("café", "café")]
    [InlineData("日本", "日本")]
    [InlineData("a:b", "a%3Ab")]
    [InlineData("a{b", "a%7Bb")]
    [InlineData("a b#c?d", "a%20b%23c%3Fd")]
    public void EscapePathName_WindowsRules_MatchesSpark(string value, string expected) =>
        Assert.Equal(expected, DeltaPath.EscapePathName(value, windowsRules: true));

    /// <summary>
    /// The two sets differ in EXACTLY four characters. Written as a sweep rather than a list so that
    /// adding a character to one set and forgetting the other cannot pass.
    /// </summary>
    [Fact]
    public void EscapeSets_DifferInExactlyTheFourWindowsCharacters()
    {
        var differing = new List<char>();
        for (int cp = 0; cp < 128; cp++)
        {
            string s = ((char)cp).ToString();
            if (DeltaPath.EscapePathName(s, windowsRules: false)
                != DeltaPath.EscapePathName(s, windowsRules: true))
            {
                differing.Add((char)cp);
            }
        }

        Assert.Equal(new[] { ' ', '<', '>', '|' }, differing);
    }

    /// <summary>
    /// The public entry point applies the set for the machine it is running on. This is the one
    /// assertion that has to branch, because the behaviour under test is the branch.
    /// </summary>
    [Fact]
    public void EscapePathName_PublicEntryPoint_FollowsThePlatform()
    {
        bool windows = IsWindows;
        Assert.Equal(windows ? "a%20b" : "a b", DeltaPath.EscapePathName("a b"));
        Assert.Equal(windows ? "a%3Cb" : "a<b", DeltaPath.EscapePathName("a<b"));
        Assert.Equal(windows ? "a%3Eb" : "a>b", DeltaPath.EscapePathName("a>b"));
        Assert.Equal(windows ? "a%7Cb" : "a|b", DeltaPath.EscapePathName("a|b"));

        // Everything outside the four is platform-independent, so it can be asserted flat.
        Assert.Equal("a%3Ab", DeltaPath.EscapePathName("a:b"));
        Assert.Equal("a}b", DeltaPath.EscapePathName("a}b"));
        Assert.Equal("café", DeltaPath.EscapePathName("café"));
    }

    /// <summary>
    /// <para>Layer 2 — the on-disk relative path to <c>add.path</c>. Spark's is
    /// <c>new Path(rel).toUri().toString()</c>: Java URI quoting of the ASCII characters illegal in a
    /// URI path, with non-ASCII left LITERAL because it is <c>toString()</c> and not
    /// <c>toASCIIString()</c>.</para>
    ///
    /// <para><c>}</c> and the backtick are the ones worth staring at: neither is in layer 1's table on
    /// any platform, so they arrive here literally and this is the only place they get escaped. EW
    /// missed both until <c>EwPartitionPaths_AreIdenticalToSparks</c> was pointed at a <c>}</c>.</para>
    /// </summary>
    [Theory]
    // Escaped.
    [InlineData("region=a b/f.parquet", "region=a%20b/f.parquet")]
    [InlineData("region=a\"b/f.parquet", "region=a%22b/f.parquet")]
    [InlineData("region=a#b/f.parquet", "region=a%23b/f.parquet")]
    [InlineData("region=a%b/f.parquet", "region=a%25b/f.parquet")]
    [InlineData("region=a<b/f.parquet", "region=a%3Cb/f.parquet")]
    [InlineData("region=a>b/f.parquet", "region=a%3Eb/f.parquet")]
    [InlineData("region=a?b/f.parquet", "region=a%3Fb/f.parquet")]
    [InlineData("region=a[b/f.parquet", "region=a%5Bb/f.parquet")]
    [InlineData("region=a]b/f.parquet", "region=a%5Db/f.parquet")]
    [InlineData("region=a^b/f.parquet", "region=a%5Eb/f.parquet")]
    [InlineData("region=a`b/f.parquet", "region=a%60b/f.parquet")]
    [InlineData("region=a{b/f.parquet", "region=a%7Bb/f.parquet")]
    [InlineData("region=a|b/f.parquet", "region=a%7Cb/f.parquet")]
    [InlineData("region=a}b/f.parquet", "region=a%7Db/f.parquet")]
    [InlineData("region=a\tb/f.parquet", "region=a%09b/f.parquet")]
    // Left literal: the separator, the Hive '=', non-ASCII, and the sub-delims a URI permits.
    [InlineData("region=café/f.parquet", "region=café/f.parquet")]
    [InlineData("region=日本/f.parquet", "region=日本/f.parquet")]
    [InlineData("region=a'b/f.parquet", "region=a'b/f.parquet")]
    [InlineData("region=a*b/f.parquet", "region=a*b/f.parquet")]
    [InlineData("region=a!$&()+,;@~b/f.parquet", "region=a!$&()+,;@~b/f.parquet")]
    // Layer 1's output re-encoded: every '%' it produced becomes '%25'.
    [InlineData("region=a%20b%23c%3Fd/f.parquet", "region=a%2520b%2523c%253Fd/f.parquet")]
    public void Encode_MatchesSparksLayerTwo(string onDisk, string expected) =>
        Assert.Equal(expected, DeltaPath.Encode(onDisk));

    /// <summary>
    /// The two layers compose to what Spark writes, for a value that needs both of them. Windows-only
    /// because on POSIX Spark leaves the four alone at layer 1 and catches them at layer 2 instead —
    /// asserted here as the pair, since that is the observable contract.
    /// </summary>
    [Theory]
    [InlineData("a<b", "region=a%3Cb", "region=a%253Cb")]
    [InlineData("a b ", "region=a%20b%20", "region=a%2520b%2520")]
    [InlineData("a}b", "region=a}b", "region=a%7Db")]
    [InlineData("café", "region=café", "region=café")]
    public void BothLayers_ComposeToSparksOutput_OnWindows(string value, string dir, string logged)
    {
        if (!IsWindows) return;

        string onDisk = "region=" + DeltaPath.EscapePathName(value);
        Assert.Equal(dir, onDisk);
        Assert.Equal(logged + "/f.parquet", DeltaPath.Encode(onDisk + "/f.parquet"));
    }

    /// <summary>
    /// Every escaped form survives the round trip a reader performs, which is the property that
    /// actually has to hold: layer 2 URL-encodes the directory name into <c>add.path</c>, and the
    /// reader <see cref="DeltaPath.Decode"/>s it back to the name on disk.
    /// </summary>
    [Theory]
    [InlineData("a b ")]
    [InlineData("a<b")]
    [InlineData("a|b")]
    [InlineData("a%b")]
    [InlineData("a#b")]
    [InlineData("café")]
    [InlineData("日本")]
    public void EscapedName_RoundTripsThroughTheLogEncoding(string value)
    {
        foreach (bool windows in new[] { false, true })
        {
            string dir = "region=" + DeltaPath.EscapePathName(value, windows);
            string logged = DeltaPath.Encode(dir + "/part-0.parquet");
            Assert.Equal(dir + "/part-0.parquet", DeltaPath.Decode(logged));
        }
    }

    /// <summary>
    /// <para>The invariant layer 2 exists for, swept over every ASCII code point and over BOTH layer-1
    /// escape sets rather than sampled: for any partition value, the composed <c>add.path</c> contains no
    /// character that would make Java's <c>new URI(…)</c> throw or silently truncate, and every <c>%</c>
    /// starts a well-formed <c>%XX</c> triple.</para>
    ///
    /// <para>This is the assertion that would have caught the four-character layer-2 set. Round-tripping
    /// through <see cref="DeltaPath.Decode"/> could not: EW decodes an under-escaped path back to the
    /// right name quite happily, while Spark's <c>CanonicalPathFunction</c> throws
    /// <c>URISyntaxException</c> and fails the read of the whole table. MEASURED on pyspark 4.0.1 /
    /// delta-spark 4.0.0.</para>
    /// </summary>
    [Fact]
    public void Encode_LeavesNoUriIllegalCharacter_ForAnyAsciiPartitionValue()
    {
        // RFC 2396 "excluded" characters, plus '#' and '?' — legal in a URI, but they delimit the
        // fragment and query, so leaving them literal truncates the path instead of throwing.
        const string mustNotAppearLiterally = " \"<>[]^`{|}#?";
        var offenders = new List<string>();

        for (int cp = 0; cp < 128; cp++)
        {
            foreach (bool windows in new[] { false, true })
            {
                string dir = "region=" + DeltaPath.EscapePathName("a" + (char)cp + "b", windows);
                string logged = DeltaPath.Encode(dir + "/f.parquet");

                foreach (char bad in mustNotAppearLiterally)
                {
                    if (logged.IndexOf(bad) >= 0)
                        offenders.Add($"U+{cp:X4} windows={windows}: '{logged}' has literal '{bad}'");
                }

                for (int i = 0; i < logged.Length; i++)
                {
                    if (logged[i] != '%')
                        continue;
                    if (i + 2 >= logged.Length || !IsHex(logged[i + 1]) || !IsHex(logged[i + 2]))
                        offenders.Add($"U+{cp:X4} windows={windows}: '{logged}' has a bare '%'");
                }
            }
        }

        Assert.Empty(offenders);

        static bool IsHex(char c) =>
            (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
    }

    /// <summary>
    /// On Windows the escaped directory name must be something Win32 can actually hold — which is the
    /// whole reason Spark's table grows those four characters. Asserted by building the directory for
    /// real, because the trailing-space case is invisible to a string comparison: Win32 accepts
    /// <c>CreateDirectory("region=a b ")</c> and silently gives you <c>region=a b</c> instead.
    /// </summary>
    [Theory]
    [InlineData("a<b")]
    [InlineData("a>b")]
    [InlineData("a|b")]
    [InlineData("a b ")]
    [InlineData("a b")]
    public void EscapedName_IsWritableOnWindows(string value)
    {
        if (!IsWindows) return;   // the escaping under test is Windows-specific

        string root = Path.Combine(Path.GetTempPath(), "ew-deltapath-" + Guid.NewGuid().ToString("N"));
        try
        {
            string name = "region=" + DeltaPath.EscapePathName(value);
            string dir = Path.Combine(root, name);
            Directory.CreateDirectory(dir);
            File.WriteAllBytes(Path.Combine(dir, "part-0.parquet"), new byte[] { 1, 2, 3 });

            // The name Win32 kept has to be the name we asked for; a stripped trailing space or dot
            // would show up here and nowhere else.
            Assert.Equal(name, Path.GetFileName(Directory.GetDirectories(root).Single()));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>
    /// <see cref="DeltaPath.BuildPartitionPath"/> escapes the column name as well as the value, and
    /// joins in enumeration order. The values here are the ones that used to be unwritable on Windows.
    /// </summary>
    [Fact]
    public void BuildPartitionPath_EscapesBothSides()
    {
        var values = new Dictionary<string, string> { ["region"] = "a<b", ["dt"] = "2024-01-01" };
        Assert.Equal(
            IsWindows ? "region=a%3Cb/dt=2024-01-01" : "region=a<b/dt=2024-01-01",
            DeltaPath.BuildPartitionPath(values));
    }
}
