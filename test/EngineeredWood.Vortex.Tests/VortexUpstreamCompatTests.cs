// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using EngineeredWood.Vortex.Tests.TestHelpers;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// <para>Reads upstream vortex's published backward-compatibility fixtures — the files its own
/// <c>vortex-test/compat-gen</c> writes for every release, covering each encoding plus TPC-H and
/// ClickBench samples — and checks EW decodes each one to the same values vortex's reader does.</para>
///
/// <para><b>The oracle.</b> The fixtures carry no expected values; upstream's check rebuilds them
/// from Rust code. Here the pinned reference reader stands in: <c>vortex-oracle</c> (the Rust crate
/// beside this project) decodes the same file to Arrow IPC, and both results are compared cell by
/// cell in <see cref="ArrowValues"/>'s canonical form. Upstream's own CI asserts that reader matches
/// the fixtures' definitions for every published release.</para>
///
/// <para><b>Inputs.</b> <c>UpstreamCompat/fixtures.lock.json</c> pins the releases and files, and
/// <c>UpstreamCompat/fetch_fixtures.py</c> downloads them. Without the files or the oracle every
/// <see cref="EwReadsLikeUpstream"/> case skips; <see cref="RequireEnvVar"/> turns that into a
/// failure in the CI job that provides both.</para>
///
/// <para><b>Committed samples.</b> So that every job — Windows and net472 included — exercises
/// the decoding these fixtures demand, a few small ones from the newest pinned release live in
/// <c>TestData/upstream-compat/</c> beside the oracle's output for each
/// (<c>vortex-oracle x.vortex x.arrow</c>); <see cref="EwReadsCommittedSampleLikeUpstream"/>
/// needs neither Rust nor the network.</para>
/// </summary>
public class VortexUpstreamCompatTests
{
    private const string RequireEnvVar = "EW_REQUIRE_VORTEX_UPSTREAM_COMPAT";

    /// <summary>
    /// Fixtures EW is known not to read yet, each with the reason. A listed case must still fail:
    /// once it passes, the entry has to go, so this list cannot outlive the gap it describes.
    /// </summary>
    private static readonly Dictionary<string, string> KnownGaps = new(StringComparer.Ordinal)
    {
        // Empty: every pinned fixture reads correctly. An entry is one fixture file name mapped
        // to the reason, e.g. ["map.vortex"] = "the Map dtype is not supported yet (#366)".
    };

    /// <summary>The release the committed samples were copied from.</summary>
    private const string SampleVersion = "0.86.1";

    private static readonly Lazy<Dictionary<string, Dictionary<string, string>>> Lock = new(() =>
    {
        var path = Path.Combine(AppContext.BaseDirectory, "UpstreamCompat", "fixtures.lock.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return doc.RootElement.GetProperty("versions").EnumerateObject().ToDictionary(
            v => v.Name,
            v => v.Value.EnumerateObject().ToDictionary(f => f.Name, f => f.Value.GetString()!));
    });

    public static IEnumerable<object[]> Fixtures() =>
        Lock.Value.SelectMany(v => v.Value.Keys.Select(name => new object[] { v.Key, name }));

    public static IEnumerable<object[]> Samples() =>
        Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "TestData", "upstream-compat"), "*.vortex")
            .Select(p => new object[] { Path.GetFileName(p) })
            .OrderBy(a => (string)a[0], StringComparer.Ordinal);

    [SkippableTheory]
    [MemberData(nameof(Fixtures))]
    public async Task EwReadsLikeUpstream(string version, string fixture)
    {
        var oracle = RustTools.Require("vortex-oracle", RequireEnvVar);
        var path = RequireFixture(version, fixture);

        Exception? failure = null;
        var expectedPath = Path.GetTempFileName();
        try
        {
            // Outside the known-gap catch: the oracle failing is never EW's expected failure.
            var (code, stdout, stderr) = RustTools.Run(oracle, path, expectedPath);
            Assert.True(code == 0, $"vortex-oracle failed (exit {code}): {stderr}{stdout}");
            try
            {
                await CompareAsync(expectedPath, path);
            }
            catch (Exception e) when (KnownGaps.ContainsKey(fixture))
            {
                failure = e;
            }
        }
        finally
        {
            try { File.Delete(expectedPath); } catch { }
        }

        if (KnownGaps.TryGetValue(fixture, out var gap))
        {
            Assert.True(failure is not null,
                $"{fixture} from {version} now reads correctly; remove its KnownGaps entry (\"{gap}\").");
        }
    }

    [Theory]
    [MemberData(nameof(Samples))]
    public async Task EwReadsCommittedSampleLikeUpstream(string fixture)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", "upstream-compat", fixture);

        // The sample must still be the published file the lock pins, byte for byte.
        using (var sha = System.Security.Cryptography.SHA256.Create())
        {
            var hash = BitConverter.ToString(sha.ComputeHash(File.ReadAllBytes(path))).Replace("-", "").ToLowerInvariant();
            Assert.Equal(Lock.Value[SampleVersion][fixture], hash);
        }

        await CompareAsync(Path.ChangeExtension(path, ".arrow"), path);
    }

    private static string RequireFixture(string version, string fixture)
    {
        var root = Environment.GetEnvironmentVariable("EW_VORTEX_COMPAT_FIXTURES")
            ?? (RustTools.ProjectDirectory is { } dir ? Path.Combine(dir, "UpstreamCompat", "fixtures") : null);
        var path = root is null ? null : Path.Combine(root, version, fixture);
        if (path is not null && File.Exists(path))
            return path;

        const string fetch = "python test/EngineeredWood.Vortex.Tests/UpstreamCompat/fetch_fixtures.py";
        if (Environment.GetEnvironmentVariable(RequireEnvVar) == "1")
            throw new InvalidOperationException($"{RequireEnvVar}=1 but {version}/{fixture} is not downloaded ({fetch}).");
        Skip.If(true, $"Upstream compat fixtures are not downloaded ({fetch}).");
        return null!;
    }

    /// <summary>
    /// Asserts EW reads <paramref name="path"/> as the same schema and values as the Arrow IPC file
    /// the oracle wrote for it.
    /// </summary>
    private static async Task CompareAsync(string expectedPath, string path)
    {
        List<RecordBatch> expected;
        Apache.Arrow.Schema expectedSchema;
        using (var stream = File.OpenRead(expectedPath))
        using (var reader = new ArrowFileReader(stream))
        {
            expectedSchema = reader.Schema;
            expected = new List<RecordBatch>();
            while (await reader.ReadNextRecordBatchAsync() is { } batch)
                expected.Add(batch);
        }

        await using var ew = await VortexFileReader.OpenAsync(path);
        var actual = new List<RecordBatch>();
        await foreach (var batch in ew.ReadAllAsync())
            actual.Add(batch);

        Assert.Equal(
            expectedSchema.FieldsList.Select(f => $"{f.Name}: {ArrowValues.FieldName(f)}"),
            ew.Schema.FieldsList.Select(f => $"{f.Name}: {ArrowValues.FieldName(f)}"));
        Assert.Equal(expected.Sum(b => (long)b.Length), actual.Sum(b => (long)b.Length));

        for (int c = 0; c < expectedSchema.FieldsList.Count; c++)
        {
            using var want = Cells(expected, c).GetEnumerator();
            using var got = Cells(actual, c).GetEnumerator();
            long row = 0;
            while (want.MoveNext() && got.MoveNext())
            {
                var w = ArrowValues.Render(want.Current.Array, want.Current.Index);
                var g = ArrowValues.Render(got.Current.Array, got.Current.Index);
                if (w != g)
                    Assert.Fail($"Column {expectedSchema.FieldsList[c].Name}, row {row}: vortex reads {w}, EW reads {g}.");
                row++;
            }
        }
    }

    private static IEnumerable<(IArrowArray Array, int Index)> Cells(List<RecordBatch> batches, int column)
    {
        foreach (var batch in batches)
        {
            var array = batch.Column(column);
            for (int i = 0; i < batch.Length; i++)
                yield return (array, i);
        }
    }
}
