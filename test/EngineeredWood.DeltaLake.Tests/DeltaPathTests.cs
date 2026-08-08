// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <para>Pins <see cref="DeltaPath.Encode"/> — layer 2, the <c>add.path</c> form — against Spark's
/// <c>new Path(rel).toUri().toString()</c>.</para>
///
/// <para>The expectations are transcribed from a direct enumeration of <c>Path.toUri().toString()</c>
/// over the ASCII range on pyspark 4.0.1 / delta-spark 4.0.0.</para>
///
/// <para><b>Why this layer is worth its own test class.</b> Under-escaping here is invisible to EW: our
/// own <see cref="DeltaPath.Decode"/> recovers the correct name from an under-escaped path, because
/// every character except <c>%</c> decodes to itself. Spark instead runs <c>add.path</c> through
/// <c>new URI(…)</c> in its <c>CanonicalPathFunction</c> UDF, where one literal URI-illegal character
/// throws <c>URISyntaxException</c> and fails the read of the WHOLE table. So the assertions that
/// matter here are not round-trip assertions — those passed before the fix — but the ones that pin the
/// exact output spelling and the absence of URI-illegal characters.</para>
/// </summary>
public class DeltaPathTests
{
    /// <summary>
    /// ASCII characters that may not appear literally in the path component of a URI. The first group is
    /// RFC 2396 "excluded"; <c>#</c> and <c>?</c> are legal in a URI but delimit the fragment and query,
    /// so leaving them literal silently truncates the path instead of throwing.
    /// </summary>
    private const string MustNotAppearLiterally = " \"<>[]^`{|}#?";

    /// <summary>
    /// Layer 2's exact output. The interesting rows are the six characters that can reach here at all:
    /// layer 1 escapes most of this set first, but <c>&lt;</c>, <c>&gt;</c>, <c>|</c> and the backtick are
    /// in no layer-1 escape table, and space and <c>%</c> are handled by both layers for different
    /// reasons.
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
    // Left literal: the separator, the Hive '=', non-ASCII, and the sub-delims a URI permits. Spark uses
    // toString() rather than toASCIIString(), which is why non-ASCII stays raw here and delta-rs (which
    // percent-encodes it as UTF-8 bytes) diverges from both.
    [InlineData("region=café/f.parquet", "region=café/f.parquet")]
    [InlineData("region=日本/f.parquet", "region=日本/f.parquet")]
    [InlineData("region=a'b/f.parquet", "region=a'b/f.parquet")]
    [InlineData("region=a*b/f.parquet", "region=a*b/f.parquet")]
    [InlineData("region=a!$&()+,;@~b/f.parquet", "region=a!$&()+,;@~b/f.parquet")]
    // Layer 1's output re-encoded: every '%' it produced becomes '%25', so a single decode by the reader
    // yields the literal directory name back.
    [InlineData("region=a%20b%23c%3Fd/f.parquet", "region=a%2520b%2523c%253Fd/f.parquet")]
    public void Encode_MatchesSparksLayerTwo(string onDisk, string expected) =>
        Assert.Equal(expected, DeltaPath.Encode(onDisk));

    /// <summary>
    /// The invariant the fix exists for, asserted over every ASCII code point rather than a sample: for
    /// ANY partition value, the composed <c>add.path</c> contains no character that would make Java's
    /// <c>new URI(…)</c> throw or silently truncate. A <c>%</c> may appear only as a well-formed
    /// <c>%XX</c> triple.
    /// </summary>
    [Fact]
    public void Encode_LeavesNoUriIllegalCharacter_ForAnyAsciiPartitionValue()
    {
        var offenders = new List<string>();

        for (int cp = 0; cp < 128; cp++)
        {
            string value = "a" + (char)cp + "b";
            string logged = DeltaPath.Encode("region=" + DeltaPath.EscapePathName(value) + "/f.parquet");

            foreach (char bad in MustNotAppearLiterally)
            {
                if (logged.IndexOf(bad) >= 0)
                    offenders.Add($"U+{cp:X4} -> '{logged}' contains literal '{bad}'");
            }

            for (int i = 0; i < logged.Length; i++)
            {
                if (logged[i] != '%')
                    continue;
                if (i + 2 >= logged.Length || !IsHex(logged[i + 1]) || !IsHex(logged[i + 2]))
                    offenders.Add($"U+{cp:X4} -> '{logged}' has a '%' that does not start a %XX triple");
            }
        }

        Assert.Empty(offenders);

        static bool IsHex(char c) =>
            (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
    }

    /// <summary>
    /// A reader recovers the on-disk name by decoding <c>add.path</c> once. Held before the fix too — it
    /// is here to prove the widened set did not break it, since escaping MORE is the kind of change that
    /// could double-encode.
    /// </summary>
    [Fact]
    public void EncodedPath_RoundTripsBackToTheNameOnDisk()
    {
        var mismatches = new StringBuilder();

        for (int cp = 0; cp < 128; cp++)
        {
            string onDisk = "region=" + DeltaPath.EscapePathName("a" + (char)cp + "b") + "/part-0.parquet";
            string decoded = DeltaPath.Decode(DeltaPath.Encode(onDisk));
            if (decoded != onDisk)
                mismatches.Append($"U+{cp:X4}: '{onDisk}' -> '{decoded}'; ");
        }

        Assert.Equal("", mismatches.ToString());
    }
}
