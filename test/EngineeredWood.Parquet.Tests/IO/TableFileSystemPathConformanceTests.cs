// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.IO;

namespace EngineeredWood.Tests.IO;

/// <summary>
/// <para>One conformance suite over every <see cref="ITableFileSystem"/> implementation, asserting that a
/// key survives create -> list -> read -> delete <b>unchanged</b>. Step 3 of issue #79.</para>
///
/// <para><b>Why this is a storage-layer suite and not a Delta one.</b> "Can this backend carry these
/// bytes under this name" is not a question about any file format, and three formats --
/// <c>EngineeredWood.DeltaLake</c>, <c>EngineeredWood.Iceberg</c> and <c>EngineeredWood.Lance.Table</c> --
/// commit through this interface. Written once here, it covers all three. It lives beside the other
/// <c>ITableFileSystem</c> suites in this project's <c>IO/</c> folder, which is where the backend tests and
/// the <see cref="CloudEmulator"/> gate already are; splitting the storage tests across two test projects
/// would cost more than the folder's name saves.</para>
///
/// <para><b>The characters are not arbitrary.</b> They are what Delta actually hands a backend. A reader
/// takes an <c>add.path</c> of <c>region=a%20b%2523c%253Fd/part-0.parquet</c> and DECODES it
/// (<c>DeltaPath.Decode</c>) before calling <see cref="ITableFileSystem"/>, so the key that arrives here is
/// <c>region=a b%23c%3Fd/part-0.parquet</c> -- a literal space and literal <c>%</c>. The backend must then
/// URL-encode that for its HTTP request, turning <c>%</c> into <c>%25</c>; and for S3, SigV4 must sign the
/// same encoding it sends. A mismatch is <c>SignatureDoesNotMatch</c>, a 404, or -- worst, because nothing
/// reports it -- a successful read of the WRONG object. None of that is reachable from a local-filesystem
/// test, because <c>LocalTableFileSystem</c> never builds a URL.</para>
///
/// <para><b>Constraints are honoured, not asserted around.</b> Each case is skipped on a backend whose
/// <see cref="ITableFileSystem.PathConstraints"/> declares it cannot hold the name -- that is the contract
/// working, not a gap. On Windows <c>LocalTableFileSystem</c> declares
/// <see cref="PathNameConstraints.Win32"/> and so skips <c>?</c>; every object store declares it can hold
/// all of these, and is held to it.</para>
/// </summary>
public abstract class TableFileSystemPathConformanceTests : IAsyncLifetime
{
    /// <summary>Long enough that a ranged read is a real read and not the whole object.</summary>
    private const int PayloadLength = 600;

    /// <summary>
    /// <para>The path segments a table format actually produces, each paired with the hazard it exists to
    /// catch. These are DIRECTORY segments: every one is used as <c>{segment}/part-00000.parquet</c>.</para>
    ///
    /// <list type="table">
    /// <item><term><c>=</c></term><description>every Hive partition directory contains one, and before
    /// this suite no cloud test path had a single <c>=</c> in it -- the existing keys are
    /// <c>blob.bin</c>, <c>commit.json</c>, <c>data/part-000.parquet</c>.</description></item>
    /// <item><term>space</term><description>Hive escaping leaves it literal in the directory
    /// name.</description></item>
    /// <item><term><c>%</c></term><description>the double-encoding hazard: the decoded <c>add.path</c>
    /// contains literal <c>%23</c>/<c>%3F</c>, which must reach the wire as <c>%2523</c>/<c>%253F</c> and
    /// come back as <c>%23</c>/<c>%3F</c>.</description></item>
    /// <item><term><c>#</c> <c>?</c> <c>&amp;</c></term><description>fragment and query delimiters; a
    /// backend that pastes a key into a URL loses everything after them.</description></item>
    /// <item><term><c>+</c></term><description>some decoders turn it back into a space, which would
    /// silently alias two different partitions.</description></item>
    /// <item><term>non-ASCII</term><description>Spark and EW both leave these literal -- see #72 /
    /// PR #77.</description></item>
    /// </list>
    /// </summary>
    public static TheoryData<string, string> PathCases =>
        new()
        {
            { "equals", "region=us-east-1" },
            { "space", "region=a b" },
            { "percent", "region=a%23c" },
            { "percent-space", "region=a%20b" },
            { "hash", "region=a#c" },
            { "question", "region=a?d" },
            { "plus", "region=a+b" },
            { "ampersand", "region=a&b" },
            { "second-equals", "region=a=b" },
            { "latin-1", "region=café" },
            { "cjk", "region=日本" },
            { "delta-decoded", "region=a b%23c%3Fd" },
        };

    /// <summary>
    /// <para>Names that a careless encode/decode round would fold onto each other. Each is a way to spell
    /// "a" then something then a letter, and all of them MUST be separate objects: if <c>%20</c> is decoded
    /// on the way out, or <c>+</c> is decoded as a space on the way in, two partitions become one and a
    /// read returns another partition's rows with no error anywhere.</para>
    /// </summary>
    private static readonly string[] AliasingSegments =
    [
        "region=a b",       // literal space
        "region=a%20b",     // literal percent-two-zero
        "region=a+b",       // literal plus
        "region=a#c",       // literal hash
        "region=a%23c",     // literal percent-two-three
    ];

    /// <summary>The emulator this backend needs, phrased so the skip message is actionable on its own.</summary>
    protected abstract string Emulator { get; }

    /// <summary>The probe result. Always <see langword="true"/> for a backend with no emulator.</summary>
    protected abstract bool Available { get; }

    /// <summary>What the probe failed with, when it did.</summary>
    protected abstract string? UnavailableReason { get; }

    /// <summary>The instance under test. Only read after <see cref="Require"/> has passed.</summary>
    protected abstract ITableFileSystem FileSystem { get; }

    /// <inheritdoc/>
    public abstract Task InitializeAsync();

    /// <inheritdoc/>
    public abstract Task DisposeAsync();

    /// <summary>Skips (or fails, under <see cref="CloudEmulator.RequireEnvVar"/>) when the backend's
    /// emulator is not there.</summary>
    protected void Require() => CloudEmulator.Require(Emulator, Available, UnavailableReason);

    [SkippableTheory]
    [MemberData(nameof(PathCases), MemberType = typeof(TableFileSystemPathConformanceTests))]
    public async Task PathSurvivesEveryOperation(string label, string segment)
    {
        Require();
        var fs = FileSystem;
        SkipIfInexpressible(fs, segment);

        string path = segment + "/part-00000.parquet";
        byte[] payload = PayloadFor(path);

        // Create-if-absent is the commit primitive; exercise it under the awkward name rather than a
        // plain one, because it is the operation whose request carries a precondition alongside the key.
        Assert.True(
            await fs.TryWriteAllBytesAsync(path, payload),
            $"[{label}] TryWriteAllBytesAsync reported the object already existed in a fresh store.");

        Assert.True(await fs.ExistsAsync(path), $"[{label}] the object does not exist after writing it.");
        Assert.Equal(payload, await fs.ReadAllBytesAsync(path));

        // Listing is where a decode mismatch shows up as a CHANGED name rather than as an error: the
        // backend encoded the key on the way in and decodes whatever the store hands back on the way out,
        // and those two are separate code paths in all three cloud implementations.
        var listed = await ListAsync(fs, segment + "/");
        Assert.Equal([path], listed.Select(static info => info.Path).ToArray());
        Assert.Equal(payload.Length, listed[0].Size);

        // The sequential writer builds its own request (block list, multipart, resumable upload), so it
        // gets its own pass over the name rather than inheriting ReadAllBytes' verdict.
        byte[] replacement = PayloadFor("replaced:" + path);
        await using (var file = await fs.CreateAsync(path, overwrite: true))
        {
            await file.WriteAsync(replacement);
        }

        Assert.Equal(replacement, await fs.ReadAllBytesAsync(path));

        await fs.DeleteAsync(path);
        Assert.False(await fs.ExistsAsync(path), $"[{label}] the object still exists after deleting it.");
    }

    [SkippableFact]
    public async Task RangedRead_UnderAwkwardName_ReturnsTheRequestedBytes()
    {
        Require();
        var fs = FileSystem;

        // One representative name is enough here: a ranged read differs from a whole-object read only in
        // the range header, and the key is spelled by the same code either way. What this covers that
        // PathSurvivesEveryOperation does not is OpenReadAsync's separate handle, which several backends
        // build a distinct request from.
        const string Segment = "region=a b%23c%3Fd";
        SkipIfInexpressible(fs, Segment);

        string path = Segment + "/part-00000.parquet";
        byte[] payload = PayloadFor(path);
        await fs.WriteAllBytesAsync(path, payload);

        await using var handle = await fs.OpenReadAsync(path);
        Assert.Equal(payload.Length, await handle.GetLengthAsync());

        using var owner = await handle.ReadAsync(new FileRange(100, 50));
        Assert.True(
            payload.AsSpan(100, 50).SequenceEqual(owner.Memory.Span),
            "a ranged read returned different bytes than were written.");
    }

    [SkippableFact]
    public async Task NamesDifferingOnlyByEncoding_AreDistinctObjects()
    {
        Require();
        var fs = FileSystem;

        string[] segments = AliasingSegments
            .Where(segment => IsExpressible(fs.PathConstraints, segment))
            .ToArray();
        Skip.If(segments.Length < 2, $"{fs.GetType().Name} can hold fewer than two of the aliasing names.");

        var written = segments.ToDictionary(
            segment => segment + "/part-00000.parquet",
            segment => PayloadFor(segment + "/part-00000.parquet"),
            StringComparer.Ordinal);

        foreach (var pair in written)
            await fs.WriteAllBytesAsync(pair.Key, pair.Value);

        // Each name must still hold its OWN bytes. This is the failure the issue calls the worst case,
        // because it has no error path: an encoding that folds two names together reads the wrong
        // partition's data back and nothing anywhere reports a problem.
        foreach (var pair in written)
        {
            byte[] actual = await fs.ReadAllBytesAsync(pair.Key);
            Assert.True(
                pair.Value.AsSpan().SequenceEqual(actual),
                $"reading '{pair.Key}' returned the bytes written under '{NameIn(written, actual)}'.");
        }

        // Deleting one must not take a sibling with it -- the same aliasing, on the request that has no
        // response body to notice it with.
        string first = written.Keys.First();
        await fs.DeleteAsync(first);
        Assert.False(await fs.ExistsAsync(first));

        foreach (string path in written.Keys.Skip(1))
        {
            Assert.True(
                await fs.ExistsAsync(path),
                $"deleting '{first}' also removed '{path}'.");
        }
    }

    [SkippableFact]
    public async Task ListReturnsEveryNameVerbatim_InLexicographicOrder()
    {
        Require();
        var fs = FileSystem;

        string[] expected = PathCases
            .Select(static row => (string)row[1]!)
            .Where(segment => IsExpressible(fs.PathConstraints, segment))
            .Select(static segment => "listing/" + segment + "/part-00000.parquet")
            .OrderBy(static path => path, StringComparer.Ordinal)
            .ToArray();

        foreach (string path in expected)
            await fs.WriteAllBytesAsync(path, PayloadFor(path));

        var listed = await ListAsync(fs, "listing/");
        var actual = listed.Select(static info => info.Path).ToArray();

        // Set difference first: it names the specific key that came back altered, where a sequence
        // comparison would only say the two lists differ.
        Assert.Empty(actual.Except(expected, StringComparer.Ordinal));
        Assert.Empty(expected.Except(actual, StringComparer.Ordinal));

        // Ordinal on UTF-16 and byte order on UTF-8 agree for every character used here (all BMP, no
        // surrogates), so the store's own ordering is directly comparable.
        Assert.Equal(expected, actual);
    }

    [SkippableFact]
    public async Task TryWriteAllBytes_UnderAwkwardName_DoesNotOverwrite()
    {
        Require();
        var fs = FileSystem;

        // The composite from the issue: literal space, literal '%23', literal '%3F' -- exactly what
        // DeltaPath.Decode hands a backend for a Spark-written partition.
        const string Segment = "region=a b%23c%3Fd";
        SkipIfInexpressible(fs, Segment);

        string path = Segment + "/00000000000000000000.json";

        Assert.True(await fs.TryWriteAllBytesAsync(path, Encoding.UTF8.GetBytes("winner")));
        Assert.False(
            await fs.TryWriteAllBytesAsync(path, Encoding.UTF8.GetBytes("loser")),
            "a second create-if-absent under the same awkward name reported that it created the object; " +
            "two concurrent committers would both believe they won version N.");

        Assert.Equal("winner", Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(path)));
    }

    private static void SkipIfInexpressible(ITableFileSystem fs, string segment) =>
        Skip.IfNot(
            IsExpressible(fs.PathConstraints, segment),
            $"{fs.GetType().Name} declares PathConstraints={fs.PathConstraints}, which cannot hold " +
            $"'{segment}'. Honouring the declaration is the contract working, not a gap.");

    /// <summary>
    /// Whether a backend declaring <paramref name="constraints"/> claims it can hold
    /// <paramref name="segment"/>. Derived from the documented meaning of each flag rather than tabulated
    /// per case, so a new flag or a new case cannot fall through silently.
    /// </summary>
    internal static bool IsExpressible(PathNameConstraints constraints, string segment)
    {
        if ((constraints & PathNameConstraints.Win32ReservedCharacters) != 0
            && segment.IndexOfAny(['<', '>', '|', ':', '*', '?', '"']) >= 0)
        {
            return false;
        }

        if ((constraints & (PathNameConstraints.Win32ReservedCharacters
                | PathNameConstraints.NoControlCharacters)) != 0
            && segment.Any(char.IsControl))
        {
            return false;
        }

        if ((constraints & PathNameConstraints.NoTrailingDot) != 0
            && segment.Length > 0 && segment[segment.Length - 1] == '.')
        {
            return false;
        }

        if ((constraints & PathNameConstraints.NoTrailingSpace) != 0
            && segment.Length > 0 && segment[segment.Length - 1] == ' ')
        {
            return false;
        }

        return (constraints & PathNameConstraints.NoDotOnlySegments) == 0
            || (segment != "." && segment != "..");
    }

    private static async Task<List<TableFileInfo>> ListAsync(ITableFileSystem fs, string prefix)
    {
        var listed = new List<TableFileInfo>();
        await foreach (var info in fs.ListAsync(prefix))
            listed.Add(info);
        return listed;
    }

    /// <summary>A payload unique to, and derived from, the path it is written under, so a read of the
    /// wrong object can name the object it actually got.</summary>
    private static byte[] PayloadFor(string path)
    {
        byte[] tag = Encoding.UTF8.GetBytes(path);
        var payload = new byte[PayloadLength];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = tag[i % tag.Length];
        return payload;
    }

    private static string NameIn(Dictionary<string, byte[]> written, byte[] payload)
    {
        foreach (var pair in written)
        {
            if (pair.Value.AsSpan().SequenceEqual(payload))
                return pair.Key;
        }

        return "(no name in this test)";
    }
}
