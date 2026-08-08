// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <para>Pins both of <see cref="DeltaPath"/>'s layers.</para>
///
/// <para><b>Layer 1</b> (<see cref="DeltaPath.EscapePathName"/>) is a local naming choice with two
/// declared promises — see <see cref="PartitionPathSpelling"/>. Under
/// <see cref="PartitionPathSpelling.SparkCompatible"/> the expectations are transcribed from a direct
/// enumeration of <c>ExternalCatalogUtils.escapePathName</c> over the whole ASCII range on pyspark 4.0.1
/// / delta-spark 4.0.0, including Spark's own <c>if (Shell.WINDOWS)</c> branch. The live comparison
/// against a running Spark lives in <c>SparkInteropTests</c>; this file is the fast, toolchain-free copy
/// of the same ground truth.</para>
///
/// <para><b>Every escape set is asserted on every machine</b>, by passing the storage constraints
/// explicitly rather than letting the host platform pick. That is not merely a testing convenience: the
/// constraints are a property of the TARGET STORAGE, so a Windows machine writing to a store with no
/// restrictions must produce the unrestricted spelling. There is no longer any behaviour that depends on
/// the OS of the running process, and so no test here needs to branch on it — the two that do are
/// checking what Win32 itself does to a name, not what EW chooses to spell.</para>
/// </summary>
public class DeltaPathTests
{
    /// <summary>Not <c>OperatingSystem.IsWindows()</c>: this project also targets net472.</summary>
    private static bool IsWindows => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    private static string Spark(string value, PathNameConstraints constraints) =>
        DeltaPath.EscapePathName(value, PartitionPathSpelling.SparkCompatible, constraints);

    private static string Portable(string value) =>
        DeltaPath.EscapePathName(value, PartitionPathSpelling.Portable, PathNameConstraints.None);

    // ---------------------------------------------------------------- layer 1, SparkCompatible

    /// <summary>
    /// Spark's escape set on storage with no restrictions — Hive's table, minus NUL, and with no closing
    /// brace. This is what Spark writes on POSIX, and now also what EW writes to an object store from any
    /// machine: MEASURED, all three object stores hold <c>&lt; &gt; |</c> and a trailing space literally.
    /// </summary>
    [Theory]
    // Left alone — the characters Win32 objects to and unrestricted storage does not.
    [InlineData("a b", "a b")]
    [InlineData("a b ", "a b ")]
    [InlineData("a<b", "a<b")]
    [InlineData("a>b", "a>b")]
    [InlineData("a|b", "a|b")]
    // Left alone on every storage. '}' is the one EW used to escape and Spark never has.
    [InlineData("a}b", "a}b")]
    [InlineData("café", "café")]
    [InlineData("日本", "日本")]
    [InlineData("a-b_c.d~e", "a-b_c.d~e")]
    // Escaped on every storage.
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
    public void SparkCompatible_UnrestrictedStorage_MatchesSpark(string value, string expected) =>
        Assert.Equal(expected, Spark(value, PathNameConstraints.None));

    /// <summary>Spark's escape set on Win32-constrained storage: the unrestricted set plus <c>' '</c>,
    /// <c>'&lt;'</c>, <c>'&gt;'</c> and <c>'|'</c>. Only those four differ; nothing else moves.</summary>
    [Theory]
    [InlineData("a b", "a%20b")]
    [InlineData("a b ", "a%20b%20")]
    [InlineData("a<b", "a%3Cb")]
    [InlineData("a>b", "a%3Eb")]
    [InlineData("a|b", "a%7Cb")]
    // Unchanged from the unrestricted set.
    [InlineData("a}b", "a}b")]
    [InlineData("café", "café")]
    [InlineData("日本", "日本")]
    [InlineData("a:b", "a%3Ab")]
    [InlineData("a{b", "a%7Bb")]
    [InlineData("a b#c?d", "a%20b%23c%3Fd")]
    public void SparkCompatible_Win32Storage_MatchesSpark(string value, string expected) =>
        Assert.Equal(expected, Spark(value, PathNameConstraints.Win32));

    /// <summary>
    /// The two sets differ in EXACTLY four characters. Written as a sweep rather than a list so that
    /// adding a character to one set and forgetting the other cannot pass.
    /// </summary>
    [Fact]
    public void SparkCompatibleSets_DifferInExactlyTheFourWin32Characters()
    {
        var differing = new List<char>();
        for (int cp = 0; cp < 128; cp++)
        {
            string s = ((char)cp).ToString();
            if (Spark(s, PathNameConstraints.None) != Spark(s, PathNameConstraints.Win32))
                differing.Add((char)cp);
        }

        Assert.Equal(new[] { ' ', '<', '>', '|' }, differing);
    }

    /// <summary>
    /// <para>The regression test for GitHub issue #84's REDESIGN: the escape set follows the target
    /// storage, never the writing process. Both halves are asserted on every machine, so this test proves
    /// the process OS is not consulted no matter which platform runs it.</para>
    ///
    /// <para>Only the <see cref="PathNameConstraints.Win32ReservedCharacters"/> flag participates, because
    /// it is the only one Spark itself branches on. A backend declaring some OTHER constraint does not
    /// move this spelling — that is the documented cost of asking for Spark parity, and
    /// <see cref="Portable_HonoursConstraintsSparkCompatibleIgnores"/> pins the difference.</para>
    /// </summary>
    [Fact]
    public void SparkCompatible_FollowsTheStorage_NotTheProcess()
    {
        // Unrestricted storage (every object store measured) — the four stay literal even on Windows.
        Assert.Equal("a b", Spark("a b", PathNameConstraints.None));
        Assert.Equal("a<b", Spark("a<b", PathNameConstraints.None));

        // Win32-constrained storage — the four are escaped even on Linux.
        Assert.Equal("a%20b", Spark("a b", PathNameConstraints.Win32));
        Assert.Equal("a%3Cb", Spark("a<b", PathNameConstraints.Win32));

        // Only the reserved-character flag is read, so a store that merely bans trailing dots gets the
        // unrestricted spelling for the four.
        Assert.Equal("a b", Spark("a b", PathNameConstraints.NoTrailingDot));
        Assert.Equal(
            Spark("a<b", PathNameConstraints.None),
            Spark("a<b", PathNameConstraints.NoControlCharacters | PathNameConstraints.NoTrailingDot));
    }

    /// <summary>
    /// <see cref="LocalTableFileSystem"/> is the one type allowed to consult the host OS, because for a
    /// local volume the host OS IS the storage. Everything else in the seam reads this property.
    /// </summary>
    [Fact]
    public void LocalFileSystem_ReportsWin32ConstraintsOnlyOnWindows()
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(Path.GetTempPath());
        Assert.Equal(
            IsWindows ? PathNameConstraints.Win32 : PathNameConstraints.None,
            fs.PathConstraints);
    }

    // ---------------------------------------------------------------- layer 1, Portable

    /// <summary>
    /// Portable escapes everything that is not RFC 3986 <i>unreserved</i> — the rule delta-rs applies
    /// unconditionally — and leaves non-ASCII literal, since non-ASCII is legal on every backend measured.
    /// </summary>
    [Theory]
    // Unreserved: kept.
    [InlineData("abcXYZ019", "abcXYZ019")]
    [InlineData("a-b_c.d~e", "a-b_c.d~e")]
    [InlineData("café", "café")]
    [InlineData("日本", "日本")]
    // Escaped here but NOT by SparkCompatible — the divergence this mode buys legality with.
    [InlineData("a b", "a%20b")]
    [InlineData("a<b", "a%3Cb")]
    [InlineData("a|b", "a%7Cb")]
    [InlineData("a}b", "a%7Db")]
    [InlineData("a+b", "a%2Bb")]
    [InlineData("a&b", "a%26b")]
    [InlineData("a!b", "a%21b")]
    [InlineData("a@b", "a%40b")]
    // Escaped by both.
    [InlineData("a/b", "a%2Fb")]
    [InlineData("a%b", "a%25b")]
    [InlineData("a=b", "a%3Db")]
    public void Portable_EscapesEverythingNotRfc3986Unreserved(string value, string expected) =>
        Assert.Equal(expected, Portable(value));

    /// <summary>
    /// <para>The trailing dot — the one rule no character set can express, and the reason this mode is not
    /// simply "what delta-rs does". <c>.</c> is RFC 3986 unreserved, so delta-rs leaves it alone
    /// everywhere and a value ending in one still names a directory Win32 silently renames.</para>
    ///
    /// <para>Escaping only the FINAL dot also disposes of a component that is exactly <c>.</c> or
    /// <c>..</c>, which would otherwise read as relative-path navigation rather than naming anything.</para>
    /// </summary>
    [Theory]
    [InlineData("a.b", "a.b")]        // interior: legal everywhere, left alone
    [InlineData("a.", "a%2E")]
    [InlineData("a..", "a.%2E")]
    [InlineData(".", "%2E")]
    [InlineData("..", ".%2E")]
    [InlineData("2024-01-01", "2024-01-01")]
    public void Portable_EscapesATrailingDot_ButNotAnInteriorOne(string value, string expected) =>
        Assert.Equal(expected, Portable(value));

    /// <summary>
    /// Portable's whole promise: ONE spelling, whatever the storage says. Swept over every ASCII code
    /// point in both leading and trailing position, against every constraint combination that matters.
    /// </summary>
    [Fact]
    public void Portable_IsIdenticalOnEveryStorage()
    {
        var constraints = new[]
        {
            PathNameConstraints.None,
            PathNameConstraints.Win32,
            PathNameConstraints.NoTrailingDot,
            PathNameConstraints.Win32ReservedCharacters | PathNameConstraints.NoDotOnlySegments,
        };

        for (int cp = 0; cp < 128; cp++)
        {
            foreach (string value in new[] { "a" + (char)cp + "b", "a" + (char)cp, ((char)cp).ToString() })
            {
                string baseline = DeltaPath.EscapePathName(
                    value, PartitionPathSpelling.Portable, PathNameConstraints.None);
                foreach (var c in constraints)
                {
                    Assert.Equal(baseline,
                        DeltaPath.EscapePathName(value, PartitionPathSpelling.Portable, c));
                }
            }
        }
    }

    /// <summary>
    /// Portable satisfies every constraint <see cref="PathNameConstraints"/> can declare, for any ASCII
    /// value in any position — which is what makes a tree written this way copyable onto a Win32 volume.
    /// Asserted as a sweep, because the promise is universal and a table of examples cannot express it.
    /// </summary>
    [Fact]
    public void Portable_OutputSatisfiesEveryDeclaredConstraint()
    {
        const string win32Reserved = "<>|:*?\"";
        var offenders = new List<string>();

        for (int cp = 0; cp < 128; cp++)
        {
            foreach (string value in new[] { "a" + (char)cp + "b", "a" + (char)cp, ((char)cp).ToString() })
            {
                // The component as it appears on disk, which is what the constraints apply to.
                string component = "region=" + Portable(value);

                foreach (char bad in win32Reserved)
                {
                    if (component.IndexOf(bad) >= 0)
                        offenders.Add($"U+{cp:X4} '{component}': Win32-reserved '{bad}'");
                }

                if (component.Any(ch => ch < ' ' || ch == ''))
                    offenders.Add($"U+{cp:X4} '{component}': control character");
                if (component.EndsWith(".", StringComparison.Ordinal))
                    offenders.Add($"U+{cp:X4} '{component}': trailing dot");
                if (component.EndsWith(" ", StringComparison.Ordinal))
                    offenders.Add($"U+{cp:X4} '{component}': trailing space");
                if (component is "." or "..")
                    offenders.Add($"U+{cp:X4} '{component}': dot-only segment");
            }
        }

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The concrete difference between the two promises, on the case that motivated the mode: a partition
    /// value ending in a dot. SparkCompatible inherits Spark's behaviour and does not escape it; Portable
    /// does. <see cref="TrailingDot_IsLostOnWin32_UnlessPortable"/> shows what that costs on a real
    /// volume.
    /// </summary>
    [Fact]
    public void Portable_HonoursConstraintsSparkCompatibleIgnores()
    {
        // Declared by Azure Blob and enforced by Win32, escaped by neither Spark, delta-rs nor kernel.
        Assert.Equal("a.", Spark("a.", PathNameConstraints.Win32 | PathNameConstraints.NoTrailingDot));
        Assert.Equal("a%2E", Portable("a."));
    }

    // ---------------------------------------------------------------- layer 2 and composition

    /// <summary>
    /// <para>Layer 2 — the on-disk relative path to <c>add.path</c>. Spark's is
    /// <c>new Path(rel).toUri().toString()</c>: Java URI quoting of the ASCII characters illegal in a URI
    /// path, with non-ASCII left LITERAL because it is <c>toString()</c> and not
    /// <c>toASCIIString()</c>.</para>
    ///
    /// <para>Unlike layer 1 this is NOT a local choice: every reader decodes <c>add.path</c> to locate the
    /// file, and Spark rejects a malformed one outright — see
    /// <see cref="Encode_LeavesNoUriIllegalCharacter_UnderEitherSpelling"/>.</para>
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
    /// <para>The invariant layer 2 exists for, swept over every ASCII code point and over every layer-1
    /// spelling rather than sampled: the composed <c>add.path</c> contains no character that would make
    /// Java's <c>new URI(…)</c> throw or silently truncate, and every <c>%</c> starts a well-formed
    /// <c>%XX</c> triple.</para>
    ///
    /// <para>This is the assertion that catches an under-escaped layer 2. Round-tripping through
    /// <see cref="DeltaPath.Decode"/> cannot: EW decodes an under-escaped path back to the right name
    /// quite happily, while Spark's <c>CanonicalPathFunction</c> throws <c>URISyntaxException</c> and
    /// fails the read of the WHOLE table. MEASURED on pyspark 4.0.1 / delta-spark 4.0.0.</para>
    /// </summary>
    [Fact]
    public void Encode_LeavesNoUriIllegalCharacter_UnderEitherSpelling()
    {
        // RFC 2396 "excluded" characters, plus '#' and '?' — legal in a URI, but they delimit the
        // fragment and query, so leaving them literal truncates the path instead of throwing.
        const string mustNotAppearLiterally = " \"<>[]^`{|}#?";
        var offenders = new List<string>();

        for (int cp = 0; cp < 128; cp++)
        {
            foreach (var (spelling, constraints) in Spellings())
            {
                string dir = "region=" + DeltaPath.EscapePathName(
                    "a" + (char)cp + "b", spelling, constraints);
                string logged = DeltaPath.Encode(dir + "/f.parquet");
                string where = $"U+{cp:X4} {spelling}/{constraints}";

                foreach (char bad in mustNotAppearLiterally)
                {
                    if (logged.IndexOf(bad) >= 0)
                        offenders.Add($"{where}: '{logged}' has literal '{bad}'");
                }

                for (int i = 0; i < logged.Length; i++)
                {
                    if (logged[i] != '%')
                        continue;
                    if (i + 2 >= logged.Length || !IsHex(logged[i + 1]) || !IsHex(logged[i + 2]))
                        offenders.Add($"{where}: '{logged}' has a bare '%'");
                }
            }
        }

        Assert.Empty(offenders);

        static bool IsHex(char c) =>
            (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
    }

    /// <summary>
    /// Every spelling survives the round trip a reader performs, which is the property that actually has
    /// to hold: layer 2 URL-encodes the directory name into <c>add.path</c>, and the reader
    /// <see cref="DeltaPath.Decode"/>s it back to the name on disk.
    /// </summary>
    [Fact]
    public void EscapedName_RoundTripsThroughTheLogEncoding()
    {
        var mismatches = new List<string>();

        for (int cp = 0; cp < 128; cp++)
        {
            foreach (var (spelling, constraints) in Spellings())
            {
                string onDisk = "region="
                    + DeltaPath.EscapePathName("a" + (char)cp + "b", spelling, constraints)
                    + "/part-0.parquet";
                string decoded = DeltaPath.Decode(DeltaPath.Encode(onDisk));
                if (decoded != onDisk)
                    mismatches.Add($"U+{cp:X4} {spelling}/{constraints}: '{onDisk}' -> '{decoded}'");
            }
        }

        Assert.Empty(mismatches);
    }

    /// <summary>
    /// The two layers compose to what Spark writes on a Win32 volume, for values that need both.
    /// </summary>
    [Theory]
    [InlineData("a<b", "region=a%3Cb", "region=a%253Cb")]
    [InlineData("a b ", "region=a%20b%20", "region=a%2520b%2520")]
    [InlineData("a}b", "region=a}b", "region=a%7Db")]
    [InlineData("café", "region=café", "region=café")]
    public void BothLayers_ComposeToSparksOutput_OnWin32Storage(string value, string dir, string logged)
    {
        string onDisk = "region=" + Spark(value, PathNameConstraints.Win32);
        Assert.Equal(dir, onDisk);
        Assert.Equal(logged + "/f.parquet", DeltaPath.Encode(onDisk + "/f.parquet"));
    }

    // ---------------------------------------------------------------- against a real volume

    /// <summary>
    /// The Win32-constrained spelling must be something Win32 can actually hold — which is the whole
    /// reason Spark's table grows those four characters. Asserted by building the directory for real,
    /// because the trailing-space case is invisible to a string comparison: Win32 accepts
    /// <c>CreateDirectory("region=a b ")</c> and silently gives you <c>region=a b</c> instead.
    /// </summary>
    [Theory]
    [InlineData("a<b")]
    [InlineData("a>b")]
    [InlineData("a|b")]
    [InlineData("a b ")]
    [InlineData("a b")]
    public void Win32Spelling_IsWritableOnWindows(string value)
    {
        if (!IsWindows) return;   // only a Win32 volume can disagree

        AssertDirectoryKeepsItsName("region=" + Spark(value, PathNameConstraints.Win32), keptExactly: true);
    }

    /// <summary>
    /// <para>The trailing dot, MEASURED against a real NTFS volume rather than argued: the
    /// SparkCompatible spelling is silently RENAMED by Win32 — which is how a writer ends up opening a
    /// directory it did not ask for — and the Portable spelling is not.</para>
    ///
    /// <para>This is the gap that justifies <see cref="PartitionPathSpelling.Portable"/> existing at all
    /// rather than deferring to delta-rs's character set, which does not cover it either.</para>
    /// </summary>
    [Fact]
    public void TrailingDot_IsLostOnWin32_UnlessPortable()
    {
        if (!IsWindows) return;

        AssertDirectoryKeepsItsName(
            "region=" + Spark("a.", PathNameConstraints.Win32), keptExactly: false);
        AssertDirectoryKeepsItsName("region=" + Portable("a."), keptExactly: true);
    }

    private static void AssertDirectoryKeepsItsName(string name, bool keptExactly)
    {
        string root = Path.Combine(Path.GetTempPath(), "ew-deltapath-" + Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(Path.Combine(root, name));
            string actual = Path.GetFileName(Directory.GetDirectories(root).Single());
            if (keptExactly)
                Assert.Equal(name, actual);
            else
                Assert.NotEqual(name, actual);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    // ---------------------------------------------------------------- BuildPartitionPath

    /// <summary>
    /// <see cref="DeltaPath.BuildPartitionPath"/> escapes the column name as well as the value, and joins
    /// in enumeration order.
    /// </summary>
    [Fact]
    public void BuildPartitionPath_EscapesBothSides()
    {
        var values = new Dictionary<string, string> { ["region"] = "a<b", ["dt"] = "2024-01-01" };

        Assert.Equal("region=a<b/dt=2024-01-01", DeltaPath.BuildPartitionPath(
            values, PartitionPathSpelling.SparkCompatible, PathNameConstraints.None));
        Assert.Equal("region=a%3Cb/dt=2024-01-01", DeltaPath.BuildPartitionPath(
            values, PartitionPathSpelling.SparkCompatible, PathNameConstraints.Win32));
        Assert.Equal("region=a%3Cb/dt=2024-01-01", DeltaPath.BuildPartitionPath(
            values, PartitionPathSpelling.Portable, PathNameConstraints.None));
    }

    /// <summary>A null value becomes Hive's sentinel, which is never escaped in either spelling.</summary>
    [Fact]
    public void BuildPartitionPath_NullValueBecomesTheHiveSentinel()
    {
        var values = new Dictionary<string, string> { ["region"] = null! };

        foreach (var (spelling, constraints) in Spellings())
        {
            Assert.Equal(
                "region=__HIVE_DEFAULT_PARTITION__",
                DeltaPath.BuildPartitionPath(values, spelling, constraints));
        }
    }

    /// <summary>Every (spelling, constraints) pair whose behaviour differs.</summary>
    private static IEnumerable<(PartitionPathSpelling Spelling, PathNameConstraints Constraints)> Spellings()
    {
        yield return (PartitionPathSpelling.SparkCompatible, PathNameConstraints.None);
        yield return (PartitionPathSpelling.SparkCompatible, PathNameConstraints.Win32);
        yield return (PartitionPathSpelling.Portable, PathNameConstraints.None);
    }
}
