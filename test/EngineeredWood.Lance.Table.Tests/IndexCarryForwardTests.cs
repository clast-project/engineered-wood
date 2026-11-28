// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Apache.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Lance.Table.Manifest;
using static EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// EW's commits carry the base manifest's index section forward (#1002), on the pylance 14.0.3 fixtures of
/// <see cref="IndexSectionTests"/> (<c>index_vacuumed_v21</c>: fragments 0 = [1, 2, 3], 1 = [10, 20, 30],
/// 2 = [100, 200], with <c>x_idx</c> covering fragments 0 and 1) and <c>btree_index_v21</c> (fragments 0 = 1..5,
/// 1 = 10..50, 2 = 100..500, with <c>x_idx</c> covering all three). A read yields one batch per fragment it scans.
/// </summary>
public sealed class IndexCarryForwardTests : IDisposable
{
    private readonly string _root;

    public IndexCarryForwardTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "ew-lance-index-carry-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    [Fact]
    public async Task Append_CarriesTheIndex_AndScansTheNewFragment()
    {
        string dataset = Copy("index_vacuumed_v21");
        var before = await IndicesAsync(dataset);
        await AppendAsync(dataset, 7, 20);

        await using var table = await LanceTable.OpenAsync(dataset);
        var info = Assert.Single(await table.GetIndicesAsync());
        Assert.Equal(before.Single().Uuid, info.Uuid);
        Assert.Equal(new uint[] { 0, 1 }, info.FragmentIds);

        var (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(7)));
        Assert.Equal([7], values);
        Assert.Equal(2, batches);  // fragments 2 and 3, which no segment covers; 0 and 1 pruned

        (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(20)));
        Assert.Equal([20, 20], values);
        Assert.Equal(3, batches);  // fragment 1 by the index, and 2 and 3
    }

    [Fact]
    public async Task DeleteUpdateAndCompact_CarryTheIndex()
    {
        string dataset = Copy("index_vacuumed_v21");
        var uuid = (await IndicesAsync(dataset)).Single().Uuid;

        await LanceDatasetWriter.DeleteAsync(dataset, Equal("x", LiteralValue.Of(20)));
        Assert.Equal(uuid, (await IndicesAsync(dataset)).Single().Uuid);

        var (rows, _) = await LanceDatasetWriter.UpdateAsync(dataset, Equal("x", LiteralValue.Of(10)),
            new Dictionary<string, Expression> { ["x"] = new LiteralExpression(LiteralValue.Of(11)) });
        Assert.Equal(1L, rows);
        Assert.Equal(uuid, (await IndicesAsync(dataset)).Single().Uuid);

        var compaction = await LanceDatasetWriter.CompactAsync(dataset);
        Assert.Equal(new ulong[] { 1 }, compaction.SourceFragmentIds);
        var info = (await IndicesAsync(dataset)).Single();
        Assert.Equal(uuid, info.Uuid);
        // The bitmap still names fragment 1, which is gone; that only means nothing is pruned for it.
        Assert.Equal(new uint[] { 0, 1 }, info.FragmentIds);

        await using var table = await LanceTable.OpenAsync(dataset);
        Assert.Empty((await ReadXAsync(table, Equal("x", LiteralValue.Of(20)))).Values);
        Assert.Empty((await ReadXAsync(table, Equal("x", LiteralValue.Of(10)))).Values);
        Assert.Equal(new[] { 11 }, (await ReadXAsync(table, Equal("x", LiteralValue.Of(11)))).Values);
        Assert.Equal(new[] { 30 }, (await ReadXAsync(table, Equal("x", LiteralValue.Of(30)))).Values);
        Assert.Equal(new[] { 2 }, (await ReadXAsync(table, Equal("x", LiteralValue.Of(2)))).Values);
        var all = await ReadXAsync(table, filter: null);
        Assert.Equal([1, 2, 3, 11, 30, 100, 200], all.Values);
    }

    [Fact]
    public async Task AFragmentIdIsNeverReused_SoACarriedIndexNeverClaimsNewRows()
    {
        // x_idx covers fragments 0, 1 and 2. Deleting every row of fragment 2 and compacting drops it, leaving 1 the
        // largest fragment id. Were the next append numbered from there, it would be fragment 2 again, which x_idx
        // covers without having seen its rows, and x = 555 would be pruned away.
        string dataset = Copy("btree_index_v21");
        await LanceDatasetWriter.DeleteAsync(dataset, GreaterThanOrEqual("x", LiteralValue.Of(100)));
        var compaction = await LanceDatasetWriter.CompactAsync(dataset);
        Assert.Empty(compaction.NewFragmentIds);
        var afterCompact = await LatestManifestAsync(dataset);
        Assert.Equal(new ulong[] { 0, 1 }, afterCompact.Fragments.Select(f => f.Id));
        Assert.Equal(2u, afterCompact.MaxFragmentId);

        await AppendAsync(dataset, 555);

        var manifest = await LatestManifestAsync(dataset);
        Assert.Equal(new ulong[] { 0, 1, 3 }, manifest.Fragments.Select(f => f.Id));
        Assert.Equal(3u, manifest.MaxFragmentId);
        await using var table = await LanceTable.OpenAsync(dataset);
        Assert.Equal(new uint[] { 0, 1, 2 }, Assert.Single(await table.GetIndicesAsync()).FragmentIds);
        var (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(555)));
        Assert.Equal([555], values);
        Assert.Equal(1, batches);
    }

    [Fact]
    public async Task Overwrite_DropsEveryIndex_AndKeepsNumberingFragments()
    {
        string dataset = Copy("index_vacuumed_v21");
        await using (var writer = await LanceDatasetWriter.OverwriteAsync(dataset))
        {
            await writer.FileWriter.WriteInt32ColumnAsync("x", new[] { 5 });
            await writer.FinishAsync();
        }

        Assert.Empty(await IndicesAsync(dataset));
        var manifest = await LatestManifestAsync(dataset);
        Assert.False(manifest.HasIndexSection);
        Assert.Equal(3UL, Assert.Single(manifest.Fragments).Id);
    }

    [Fact]
    public async Task ASegmentOnAFieldTheSchemaLacks_IsDropped()
    {
        string dataset = Copy("index_segments_v21");
        // The latest version's two segments of x_idx; the older versions' sections are left as they are.
        Guid kept = default;
        IndexSectionTests.RewriteIndexSection(dataset, section =>
        {
            if (section.Indices.Count != 2)
                return;
            section.Indices[1].Fields[0] = 99;
            Assert.True(Indices.IndexCatalog.TryGetUuid(section.Indices[0], out kept));
        });
        Assert.Equal(2, (await IndicesAsync(dataset)).Count);
        await AppendAsync(dataset, 7);

        var info = Assert.Single(await IndicesAsync(dataset));
        Assert.Equal(kept, info.Uuid);
        await using var table = await LanceTable.OpenAsync(dataset);
        Assert.Equal(new[] { 7 }, (await ReadXAsync(table, Equal("x", LiteralValue.Of(7)))).Values);
    }

    [Theory]
    [InlineData("__lance_frag_reuse")]
    [InlineData("__lance_mem_wal")]
    public async Task ASystemIndex_DropsTheWholeSection(string name)
    {
        // Their state follows the fragments lance rewrites, which EW does not maintain, so EW keeps none of the
        // indices rather than leave them inconsistent: reads are then unpruned, never wrong.
        string dataset = Copy("index_vacuumed_v21");
        IndexSectionTests.RewriteIndexSection(dataset, section =>
        {
            var system = section.Indices[0].Clone();
            system.Name = name;
            system.Fields.Clear();
            system.Uuid = new Proto.UUID { Uuid = Google.Protobuf.ByteString.CopyFrom(Guid.NewGuid().ToByteArray()) };
            section.Indices.Add(system);
        });
        await AppendAsync(dataset, 7);

        Assert.False((await LatestManifestAsync(dataset)).HasIndexSection);
    }

    [Fact]
    public async Task Vacuum_KeepsACarriedIndex_AndDeletesADroppedOneOnceNoVersionListsIt()
    {
        string dataset = Copy("index_vacuumed_v21");
        string indexDir = Path.Combine(dataset, "_indices", (await IndicesAsync(dataset)).Single().Uuid.ToString("D"));
        // Not an index's directory: left alone.
        string other = Path.Combine(dataset, "_indices", "not-a-uuid");
        Directory.CreateDirectory(other);

        await AppendAsync(dataset, 7);
        var result = await LanceDatasetWriter.VacuumAsync(dataset);
        Assert.Empty(result.IndexDirectoriesDeleted);
        Assert.True(Directory.Exists(indexDir));

        await using (var writer = await LanceDatasetWriter.OverwriteAsync(dataset))
        {
            await writer.FileWriter.WriteInt32ColumnAsync("x", new[] { 5 });
            await writer.FinishAsync();
        }
        // The version before the overwrite still lists it.
        result = await LanceDatasetWriter.VacuumAsync(dataset, new LanceVacuumOptions { RetainVersions = 2 });
        Assert.Empty(result.IndexDirectoriesDeleted);
        Assert.True(Directory.Exists(indexDir));

        result = await LanceDatasetWriter.VacuumAsync(dataset, new LanceVacuumOptions { DryRun = true });
        Assert.Equal([Path.GetFileName(indexDir)], result.IndexDirectoriesDeleted);
        Assert.True(Directory.Exists(indexDir));

        result = await LanceDatasetWriter.VacuumAsync(dataset);
        Assert.Equal([Path.GetFileName(indexDir)], result.IndexDirectoriesDeleted);
        Assert.False(Directory.Exists(indexDir));
        Assert.True(Directory.Exists(other));
        Assert.True(result.BytesDeleted > 0);
    }

    [Fact]
    public async Task Append_CarriedIndex_CrossValidatedAgainstPylance()
    {
        if (!IsPylanceAvailable()) return;
        string dataset = Copy("index_vacuumed_v21");
        await AppendAsync(dataset, 7, 20);

        string script = "import sys, json, lance\n" +
            $"ds = lance.dataset(r'{dataset}')\n" +
            "idx = [i for i in ds.describe_indices()]\n" +
            "out = { 'names': sorted(i.name for i in idx), " +
            "'seven': ds.to_table(filter='x = 7').column('x').to_pylist(), " +
            "'twenty': ds.to_table(filter='x = 20').column('x').to_pylist(), " +
            "'rows': ds.count_rows() }\n" +
            "sys.stdout.write(json.dumps(out))\n";
        string stdout = await RunPythonAsync(script);

        using var json = System.Text.Json.JsonDocument.Parse(stdout);
        Assert.Equal(["x_idx"], json.RootElement.GetProperty("names").EnumerateArray().Select(e => e.GetString()));
        Assert.Equal([7], json.RootElement.GetProperty("seven").EnumerateArray().Select(e => e.GetInt32()));
        Assert.Equal([20, 20], json.RootElement.GetProperty("twenty").EnumerateArray().Select(e => e.GetInt32()));
        Assert.Equal(10, json.RootElement.GetProperty("rows").GetInt32());
    }

    [Fact]
    public async Task CompactThenAppend_CrossValidatedAgainstPylance()
    {
        // pylance reads the high-water mark EW keeps, and its index answers right for the appended fragment.
        if (!IsPylanceAvailable()) return;
        string dataset = Copy("btree_index_v21");
        await LanceDatasetWriter.DeleteAsync(dataset, GreaterThanOrEqual("x", LiteralValue.Of(100)));
        await LanceDatasetWriter.CompactAsync(dataset);
        await AppendAsync(dataset, 555);

        string script = "import sys, json, lance\n" +
            $"ds = lance.dataset(r'{dataset}')\n" +
            "out = { 'names': sorted(i.name for i in ds.describe_indices()), " +
            "'hit': ds.to_table(filter='x = 555').column('x').to_pylist(), " +
            "'frags': sorted(f.fragment_id for f in ds.get_fragments()) }\n" +
            "sys.stdout.write(json.dumps(out))\n";
        string stdout = await RunPythonAsync(script);

        using var json = System.Text.Json.JsonDocument.Parse(stdout);
        Assert.Equal(["x_idx"], json.RootElement.GetProperty("names").EnumerateArray().Select(e => e.GetString()));
        Assert.Equal([555], json.RootElement.GetProperty("hit").EnumerateArray().Select(e => e.GetInt32()));
        Assert.Equal([0, 1, 3], json.RootElement.GetProperty("frags").EnumerateArray().Select(e => e.GetInt32()));
    }

    // --- helpers ----------------------------------------------------------------------------------

    private string Copy(string fixture)
    {
        string target = Path.Combine(_root, fixture);
        LanceBasePathsTests.CopyDirectory(TestDataPath.Resolve(fixture), target);
        return target;
    }

    private static async Task AppendAsync(string dataset, params int[] values)
    {
        await using var writer = await LanceDatasetWriter.AppendAsync(dataset);
        await writer.FileWriter.WriteInt32ColumnAsync("x", values);
        await writer.FinishAsync();
    }

    private static async Task<IReadOnlyList<Indices.IndexInfo>> IndicesAsync(string dataset)
    {
        await using var table = await LanceTable.OpenAsync(dataset);
        return await table.GetIndicesAsync();
    }

    private static async Task<Proto.Manifest> LatestManifestAsync(string dataset)
    {
        var fs = new LocalTableFileSystem(dataset);
        var entry = await ManifestPathResolver.ResolveLatestAsync(fs);
        return await ManifestReader.ReadAsync(fs, entry.Path);
    }

    private static async Task<(int Batches, int[] Values)> ReadXAsync(LanceTable table, Predicate? filter)
    {
        int batches = 0;
        var values = new List<int>();
        await foreach (var batch in table.ReadAsync(columns: null, filter: filter))
        {
            batches++;
            values.AddRange(((Int32Array)batch.Column("x")).Values.ToArray());
        }
        values.Sort();
        return (batches, values.ToArray());
    }

    private static bool IsPylanceAvailable()
    {
        try
        {
            var psi = new ProcessStartInfo("python", "-c \"import lance\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi);
            if (proc is null) return false;
            proc.WaitForExit(5_000);
            return proc.ExitCode == 0;
        }
        catch { return false; }
    }

    private static async Task<string> RunPythonAsync(string script)
    {
        var psi = new ProcessStartInfo("python", "-c \"" + script.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var proc = Process.Start(psi)!;
        string stdout = await proc.StandardOutput.ReadToEndAsync();
        string stderr = await proc.StandardError.ReadToEndAsync();
        proc.WaitForExit();
        Assert.True(proc.ExitCode == 0, $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");
        return stdout;
    }
}
