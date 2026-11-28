// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Diagnostics;
using Apache.Arrow;
using EngineeredWood.Expressions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using Google.Protobuf;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Datasets whose manifests have <c>base_paths</c> (#963): a pylance 14.0.3 shallow clone and a multi-base write
/// (<c>TestData/base_paths_v21</c>, see <c>generate_test_data.py</c>). pylance stores each base as the absolute local
/// path of the machine that wrote it, so every test works on a copy whose manifests point the bases at the copy:
/// the bytes before the manifest message and the rest of the manifest are pylance's.
/// </summary>
public sealed class LanceBasePathsTests : IDisposable
{
    private static readonly int[] CloneIds =
        Enumerable.Range(0, 20).Where(i => i is not (3 or 5 or 13)).Concat([100, 101]).ToArray();

    private static readonly int[] MultiIds = Enumerable.Range(0, 30).Where(i => i % 10 != 0).ToArray();

    private readonly string _root;

    public LanceBasePathsTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "ew-lance-bases-" + Guid.NewGuid().ToString("N"));
        CopyDirectory(TestDataPath.Resolve("base_paths_v21"), _root);
        foreach (string dataset in new[] { "clone", "multi" })
            RewriteBases(Path.Combine(_root, dataset), b => b.Path = Path.Combine(_root, LastSegment(b.Path)));
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    private string Clone => Path.Combine(_root, "clone");
    private string Source => Path.Combine(_root, "clone_source");
    private string Multi => Path.Combine(_root, "multi");

    // --- reads ------------------------------------------------------------------------------------

    [Fact]
    public async Task ShallowClone_ReadsSourceFilesThroughTheResolver()
    {
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });

        Assert.Equal(CloneIds.Length, table.NumberOfRows);
        Assert.Equal(CloneIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task ShallowClone_FilteredRead_MatchesUnfiltered()
    {
        // The clone's inherited index lives in the source with base_id 0, and EW discovers indices from
        // _transactions/ (the clone's has only a Clone op), so no index prunes here; the rows must still be right.
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });

        var ids = new List<int>();
        await foreach (var batch in table.ReadAsync(null,
                           EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", LiteralValue.Of(10))))
            ids.AddRange(((Int32Array)batch.Column(0)).Values.ToArray());

        Assert.Equal(CloneIds.Where(i => i >= 10), ids.OrderBy(i => i));
    }

    [Fact]
    public async Task ShallowClone_UnregisteredSource_OpensButRefusesToRead()
    {
        await using var table = await LanceTable.OpenAsync(Clone);
        Assert.Equal(CloneIds.Length, table.NumberOfRows);

        var ex = await Assert.ThrowsAsync<UnregisteredStorageLocationException>(() => ReadIdsAsync(table));
        Assert.Contains("clone_source", ex.Location.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task MultiBase_ReadsDatasetRootAndFileDirectoryBases()
    {
        var resolver = new StorageResolver()
            .Register(new LocalTableFileSystem(Path.Combine(_root, "rootbase")))
            .Register(new LocalTableFileSystem(Path.Combine(_root, "filebase")));
        await using var table = await LanceTable.OpenAsync(Multi, new LanceTableOptions { StorageResolver = resolver });

        Assert.Equal(3, table.NumberOfFragments);
        Assert.Equal(MultiIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task MultiBase_OneBaseUnregistered_RefusesItsFragment()
    {
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Path.Combine(_root, "rootbase")));
        await using var table = await LanceTable.OpenAsync(Multi, new LanceTableOptions { StorageResolver = resolver });

        var ex = await Assert.ThrowsAsync<UnregisteredStorageLocationException>(() => ReadIdsAsync(table));
        Assert.Contains("filebase", ex.Location.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BaseUnderTheDatasetItself_NeedsNoRegistration()
    {
        // Move both bases inside the dataset's own directory: they are then the dataset's own files.
        Directory.Move(Path.Combine(_root, "rootbase"), Path.Combine(Multi, "inner_root"));
        Directory.Move(Path.Combine(_root, "filebase"), Path.Combine(Multi, "inner_files"));
        RewriteBases(Multi, b => b.Path = Path.Combine(Multi, b.IsDatasetRoot ? "inner_root" : "inner_files"));

        await using var table = await LanceTable.OpenAsync(Multi);
        Assert.Equal(MultiIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task FileUri_IsPercentDecoded()
    {
        // lance parses a base with a scheme as a URI and percent-decodes its path (Path::from_url_path).
        string spaced = Path.Combine(_root, "clone source");
        Directory.Move(Source, spaced);
        RewriteBases(Clone, b => b.Path = FileUri(_root) + "/clone%20source");

        var resolver = new StorageResolver().Register(new LocalTableFileSystem(spaced));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        Assert.Equal(CloneIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task BarePath_IsLiteral()
    {
        // A base with no scheme is a local path, taken literally: %20 is three characters of the name.
        string literal = Path.Combine(_root, "clone%20source");
        Directory.Move(Source, literal);
        RewriteBases(Clone, b => b.Path = literal);

        var resolver = new StorageResolver().Register(new LocalTableFileSystem(literal));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        Assert.Equal(CloneIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task BarePath_DotSegmentsCollapse()
    {
        // A local path's . and .. navigate, as lance's path_abs does.
        RewriteBases(Clone, b => b.Path = Path.Combine(_root, "nowhere", "..", "clone_source"));

        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        Assert.Equal(CloneIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task WindowsMixedSeparators_AsPylanceWritesThem()
    {
        // pylance 14.0.3's shallow_clone on Windows stored 'C:/…/m1\src': forward slashes, then a backslash.
        if (Path.DirectorySeparatorChar != '\\')
            return;
        RewriteBases(Clone, b => b.Path = _root.Replace('\\', '/') + "\\clone_source");

        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        Assert.Equal(CloneIds, await ReadIdsAsync(table));
    }

    // --- refusals ---------------------------------------------------------------------------------

    [Fact]
    public async Task PosixBackslashInBarePath_IsRefused()
    {
        // Off Windows, '{clone}\clone_source' is ONE directory name beside the clone (lance reads it literally); read
        // with '\' as a separator it would name a directory INSIDE the clone, reached without the resolver.
        if (Path.DirectorySeparatorChar == '\\')
            return;
        RewriteBases(Clone, b => b.Path = Clone + "\\clone_source");

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () => await LanceTable.OpenAsync(Clone));
        Assert.Contains("'\\'", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("clone_source")]
    [InlineData("../clone_source")]
    [InlineData("./clone_source")]
    public async Task RelativeBase_IsRefused(string relative)
    {
        RewriteBases(Clone, b => b.Path = relative);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () => await LanceTable.OpenAsync(Clone));
        Assert.Contains("relative", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("s3://bucket/a/../clone_source")]
    [InlineData("s3://bucket/a/./clone_source")]
    [InlineData("gs://bucket/../clone_source")]
    public async Task ObjectStoreDotSegment_IsRefused(string location)
    {
        RewriteBases(Clone, b => b.Path = location);
        await Assert.ThrowsAsync<NotSupportedException>(async () => await LanceTable.OpenAsync(Clone));
    }

    [Fact]
    public async Task ObjectStoreBase_Unregistered_IsRefusedOnRead()
    {
        RewriteBases(Clone, b => b.Path = "s3://bucket/datasets/clone_source");
        await using var table = await LanceTable.OpenAsync(Clone);

        var ex = await Assert.ThrowsAsync<UnregisteredStorageLocationException>(() => ReadIdsAsync(table));
        Assert.Equal("s3", ex.Location.Scheme);
    }

    [Fact]
    public async Task EmptyBase_IsRefused()
    {
        RewriteBases(Clone, b => b.Path = "");
        await Assert.ThrowsAsync<LanceTableFormatException>(async () => await LanceTable.OpenAsync(Clone));
    }

    [Fact]
    public async Task UnknownBaseId_IsRefused()
    {
        RewriteBases(Clone, b => b.Id = 7);
        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(async () => await LanceTable.OpenAsync(Clone));
        Assert.Contains("base path id 0", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DuplicateBaseId_IsRefused()
    {
        RewriteManifests(Clone, m => m.BasePaths.Add(m.BasePaths[0].Clone()));
        await Assert.ThrowsAsync<LanceTableFormatException>(async () => await LanceTable.OpenAsync(Clone));
    }

    [Fact]
    public async Task DeletionFileUnderAFileDirectoryBase_IsRefused()
    {
        // lance 14.0.3: "base_path id {} is not a dataset root for deletion_file".
        RewriteBases(Clone, b => b.IsDatasetRoot = false);
        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(async () => await LanceTable.OpenAsync(Clone));
        Assert.Contains("not a dataset root", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("../escape.lance")]
    [InlineData("/etc/escape.lance")]
    public async Task DataFilePathLeavingItsBase_IsRefused(string path)
    {
        RewriteManifests(Clone, m =>
        {
            foreach (var fragment in m.Fragments)
                foreach (var file in fragment.Files)
                    if (file.HasBaseId)
                        file.Path = path;
        });

        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        await Assert.ThrowsAsync<LanceTableFormatException>(() => ReadIdsAsync(table));
    }

    // --- writes (#990) ----------------------------------------------------------------------------

    [Fact]
    public async Task Append_CarriesBasesForward_AndLeavesTheSourceUnchanged()
    {
        string[] source = HashTree(Source);
        await AppendIdsAsync(Clone, 200, 201);

        var manifest = await ReadLatestManifestAsync(Clone);
        Assert.Single(manifest.BasePaths);
        Assert.Equal(FlagBasePaths, manifest.ReaderFeatureFlags & FlagBasePaths);
        Assert.Equal(FlagBasePaths, manifest.WriterFeatureFlags & FlagBasePaths);
        var appended = manifest.Fragments[manifest.Fragments.Count - 1];
        Assert.False(appended.Files[0].HasBaseId);
        Assert.True(File.Exists(Path.Combine(Clone, "data", appended.Files[0].Path)));

        Assert.Equal(CloneIds.Concat([200, 201]).OrderBy(i => i), await ReadCloneIdsAsync());
        Assert.Equal(source, HashTree(Source));
    }

    [Fact]
    public async Task Overwrite_KeepsTheBases_AsLanceDoes()
    {
        // Measured, pylance 14.0.3: an overwrite of a shallow clone keeps its base_paths.
        await using (var writer = await LanceDatasetWriter.OverwriteAsync(Clone))
        {
            await writer.FileWriter.WriteInt32ColumnAsync("id", new[] { 7 });
            await writer.FinishAsync();
        }

        var manifest = await ReadLatestManifestAsync(Clone);
        Assert.Single(manifest.BasePaths);
        Assert.Equal(FlagBasePaths, manifest.ReaderFeatureFlags & FlagBasePaths);
        Assert.All(manifest.Fragments, f => Assert.False(f.Files[0].HasBaseId));

        await using var table = await LanceTable.OpenAsync(Clone);
        Assert.Equal(new[] { 7 }, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Delete_ReadsSourceFiles_WritesUnderTheRoot()
    {
        string[] source = HashTree(Source);
        await LanceDatasetWriter.DeleteAsync(Clone, EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", LiteralValue.Of(15)),
            CloneWrite);

        var manifest = await ReadLatestManifestAsync(Clone);
        Assert.Single(manifest.BasePaths);
        // Fragment 1 is the source's, and its previous deletion file was the source's too (base 0); the merged one
        // replacing it is the clone's own. Its data file still names the source.
        var fragment1 = manifest.Fragments.Single(f => f.Id == 1);
        Assert.True(fragment1.Files[0].HasBaseId);
        Assert.False(fragment1.DeletionFile.HasBaseId);
        Assert.True(File.Exists(Path.Combine(Clone, "_deletions",
            Deletions.DeletionFileReader.FileName(1, fragment1.DeletionFile))));

        Assert.Equal(CloneIds.Where(i => i < 15), await ReadCloneIdsAsync());
        Assert.Equal(source, HashTree(Source));
    }

    [Fact]
    public async Task DeleteRows_MergesADeletionFileUnderTheSource()
    {
        // Fragment 1 (ids 10-19) carries the source's deletion file, which deletes id 13 (offset 3).
        string[] source = HashTree(Source);
        await LanceDatasetWriter.DeleteRowsAsync(Clone,
            new Dictionary<ulong, IReadOnlyList<int>> { [1] = new[] { 0 } }, CloneWrite);

        Assert.Equal(CloneIds.Where(i => i != 10), await ReadCloneIdsAsync());
        Assert.Equal(source, HashTree(Source));
    }

    [Fact]
    public async Task Update_RewritesSourceRowsUnderTheRoot()
    {
        string[] source = HashTree(Source);
        var (updated, _) = await LanceDatasetWriter.UpdateAsync(Clone,
            EngineeredWood.Expressions.Expressions.LessThan("id", LiteralValue.Of(12)),
            new Dictionary<string, Expression> { ["v"] = new LiteralExpression(LiteralValue.Of("new")) },
            CloneWrite);

        Assert.Equal(CloneIds.Count(i => i < 12), updated);
        var rows = await ReadCloneRowsAsync();
        Assert.Equal(CloneIds, rows.Keys.OrderBy(i => i));
        Assert.All(rows, kv => Assert.Equal(kv.Key < 12 ? "new" : $"v{kv.Key}", kv.Value));
        Assert.Equal(source, HashTree(Source));
    }

    [Fact]
    public async Task Compact_MovesSourceSurvivorsUnderTheRoot()
    {
        string[] source = HashTree(Source);
        var result = await LanceDatasetWriter.CompactAsync(Clone, CloneWrite);

        Assert.Equal(new ulong[] { 0, 1 }, result.SourceFragmentIds.OrderBy(i => i));
        var manifest = await ReadLatestManifestAsync(Clone);
        Assert.Single(manifest.BasePaths);
        Assert.All(manifest.Fragments, f => Assert.False(f.Files[0].HasBaseId));
        Assert.Equal(CloneIds, await ReadCloneIdsAsync());
        Assert.Equal(source, HashTree(Source));
    }

    [Fact]
    public async Task MultiBase_DeleteAndCompact_ReadEveryBase()
    {
        string[] bases = HashTree(Path.Combine(_root, "rootbase")).Concat(HashTree(Path.Combine(_root, "filebase")))
            .ToArray();
        var resolver = new StorageResolver()
            .Register(new LocalTableFileSystem(Path.Combine(_root, "rootbase")))
            .Register(new LocalTableFileSystem(Path.Combine(_root, "filebase")));
        var options = new LanceWriteOptions { StorageResolver = resolver };

        await LanceDatasetWriter.DeleteAsync(Multi, EngineeredWood.Expressions.Expressions.Equal("id", LiteralValue.Of(15)), options);
        await LanceDatasetWriter.CompactAsync(Multi, options);

        var manifest = await ReadLatestManifestAsync(Multi);
        Assert.Equal(2, manifest.BasePaths.Count);
        await using var table = await LanceTable.OpenAsync(Multi);
        Assert.Equal(MultiIds.Where(i => i != 15), await ReadIdsAsync(table));
        Assert.Equal(bases,
            HashTree(Path.Combine(_root, "rootbase")).Concat(HashTree(Path.Combine(_root, "filebase"))).ToArray());
    }

    [Fact]
    public async Task ReadingOps_WithoutAResolver_RefuseBeforeWriting()
    {
        string[] before = ListFiles(_root);
        var pred = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", LiteralValue.Of(0));

        await Assert.ThrowsAsync<UnregisteredStorageLocationException>(async () =>
            await LanceDatasetWriter.DeleteAsync(Clone, pred));
        await Assert.ThrowsAsync<UnregisteredStorageLocationException>(async () =>
            await LanceDatasetWriter.DeleteRowsAsync(Clone,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0 }, [1] = new[] { 0 } }));
        await Assert.ThrowsAsync<UnregisteredStorageLocationException>(async () =>
            await LanceDatasetWriter.UpdateAsync(Clone, pred,
                new Dictionary<string, Expression> { ["v"] = new LiteralExpression(LiteralValue.Of("x")) }));
        await Assert.ThrowsAsync<UnregisteredStorageLocationException>(async () =>
            await LanceDatasetWriter.CompactAsync(Clone));

        Assert.Equal(before, ListFiles(_root));
    }

    [Fact]
    public async Task Vacuum_NeverDeletesABaseFile()
    {
        await LanceDatasetWriter.CompactAsync(Clone, CloneWrite);
        string[] source = HashTree(Source);

        // Retaining one version leaves no reference to the source's files; they are still not the clone's to delete.
        var result = await LanceDatasetWriter.VacuumAsync(Clone, new LanceVacuumOptions { RetainVersions = 1 });

        Assert.NotEmpty(result.ManifestsDeleted);
        Assert.Equal(source, HashTree(Source));
        Assert.Equal(CloneIds, await ReadCloneIdsAsync());
    }

    [Fact]
    public async Task Vacuum_KeepsAFileABaseInsideTheRootNames()
    {
        // A file-directory base at the dataset's own data/ (and a dataset-root base at the dataset itself) names
        // files that vacuum lists as the dataset's own data/ files. They are live.
        string filebase = Path.Combine(_root, "filebase");
        foreach (string file in Directory.GetFiles(filebase))
            File.Move(file, Path.Combine(Multi, "data", Path.GetFileName(file)));
        foreach (string file in Directory.GetFiles(Path.Combine(_root, "rootbase", "data")))
            File.Move(file, Path.Combine(Multi, "data", Path.GetFileName(file)));
        RewriteBases(Multi, b => b.Path = b.IsDatasetRoot ? Multi : Path.Combine(Multi, "data"));

        var result = await LanceDatasetWriter.VacuumAsync(Multi, new LanceVacuumOptions { RetainVersions = 1 });

        Assert.Empty(result.DataFilesDeleted);
        await using var table = await LanceTable.OpenAsync(Multi);
        Assert.Equal(MultiIds, await ReadIdsAsync(table));
    }

    [Fact]
    public async Task Vacuum_StillDeletesAnUnreferencedRootFile()
    {
        // An overwrite drops every fragment of the clone, its own included; vacuum then reclaims the clone's files.
        string ownFile = Directory.GetFiles(Path.Combine(Clone, "data")).Single();
        await using (var writer = await LanceDatasetWriter.OverwriteAsync(Clone))
        {
            await writer.FileWriter.WriteInt32ColumnAsync("id", new[] { 7 });
            await writer.FinishAsync();
        }
        string[] source = HashTree(Source);

        var result = await LanceDatasetWriter.VacuumAsync(Clone, new LanceVacuumOptions { RetainVersions = 1 });

        Assert.Contains(Path.GetFileName(ownFile), result.DataFilesDeleted);
        Assert.False(File.Exists(ownFile));
        Assert.Equal(source, HashTree(Source));
    }

    [Fact]
    public async Task Vacuum_RefusesABaseItCannotPlace()
    {
        RewriteBases(Clone, b => b.Path = "clone_source");
        string[] before = ListFiles(_root);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await LanceDatasetWriter.VacuumAsync(Clone, new LanceVacuumOptions { RetainVersions = 1 }));
        Assert.Equal(before, ListFiles(_root));
    }

    [Fact]
    public async Task Pylance_ReadsTheWrittenClone_AndAgreesWithEW()
    {
        if (!IsPylanceAvailable())
            return;

        // Every fragment EW writes here holds an even number of rows, and the update writes two 4-character
        // values: pylance 14.0.3 refuses an EW Flat buffer whose length is not a multiple of 8 (EW declares the
        // size rounded up to 8; lance's value.rs now wants it exact), which is a defect of the file writer, not of
        // base paths.
        await AppendIdsAsync(Clone, 200, 201);
        await LanceDatasetWriter.DeleteAsync(Clone, EngineeredWood.Expressions.Expressions.Equal("id", LiteralValue.Of(11)), CloneWrite);
        await LanceDatasetWriter.UpdateAsync(Clone,
            EngineeredWood.Expressions.Expressions.In("id", LiteralValue.Of(1), LiteralValue.Of(2)),
            new Dictionary<string, Expression> { ["v"] = new LiteralExpression(LiteralValue.Of("vvvv")) }, CloneWrite);
        var afterUpdate = await ReadCloneRowsAsync();
        Assert.Equal(afterUpdate, await PylanceRowsAsync(Clone));

        await LanceDatasetWriter.CompactAsync(Clone, CloneWrite);
        var afterCompact = await ReadCloneRowsAsync();
        Assert.Equal(afterUpdate, afterCompact);
        Assert.Equal(afterCompact, await PylanceRowsAsync(Clone));
    }

    // --- helpers ----------------------------------------------------------------------------------

    private const ulong FlagBasePaths = 16;

    private LanceWriteOptions CloneWrite =>
        new() { StorageResolver = new StorageResolver().Register(new LocalTableFileSystem(Source)) };

    private async Task<int[]> ReadCloneIdsAsync()
    {
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        return await ReadIdsAsync(table);
    }

    private async Task<SortedDictionary<int, string>> ReadCloneRowsAsync()
    {
        var resolver = new StorageResolver().Register(new LocalTableFileSystem(Source));
        await using var table = await LanceTable.OpenAsync(Clone, new LanceTableOptions { StorageResolver = resolver });
        var rows = new SortedDictionary<int, string>();
        await foreach (var batch in table.ReadAsync())
        {
            var ids = (Int32Array)batch.Column("id");
            var values = (StringArray)batch.Column("v");
            for (int i = 0; i < batch.Length; i++)
                rows.Add(ids.GetValue(i)!.Value, values.GetString(i));
        }

        return rows;
    }

    private static async Task AppendIdsAsync(string dataset, params int[] ids)
    {
        await using var writer = await LanceDatasetWriter.AppendAsync(dataset);
        await writer.FileWriter.WriteInt32ColumnAsync("id", ids);
        var values = new StringArray.Builder();
        foreach (int id in ids)
            values.Append($"v{id}");
        await writer.FileWriter.WriteColumnAsync("v", values.Build());
        await writer.FinishAsync();
    }

    private static async Task<Proto.Manifest> ReadLatestManifestAsync(string dataset)
    {
        var fs = new LocalTableFileSystem(dataset);
        var entry = await Manifest.ManifestPathResolver.ResolveLatestAsync(fs);
        return await Manifest.ManifestReader.ReadAsync(fs, entry.Path);
    }

    /// <summary>Every file under <paramref name="root"/> with a hash of its bytes.</summary>
    private static string[] HashTree(string root)
    {
        using var sha = System.Security.Cryptography.SHA256.Create();
        return Directory.GetFiles(root, "*", SearchOption.AllDirectories)
            .Select(f => f.Substring(root.Length) + ":" + Convert.ToBase64String(sha.ComputeHash(File.ReadAllBytes(f))))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();
    }

    private static bool IsPylanceAvailable()
    {
        try
        {
            using var proc = Process.Start(new ProcessStartInfo("python", "-c \"import lance\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            });
            if (proc is null) return false;
            proc.WaitForExit(30_000);
            return proc.ExitCode == 0;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return false;
        }
    }

    /// <summary>The rows pylance reads from <paramref name="dataset"/>, as id to v.</summary>
    private static async Task<SortedDictionary<int, string>> PylanceRowsAsync(string dataset)
    {
        string script = "import sys, json, lance\n"
            + "t = lance.dataset(sys.argv[1]).to_table()\n"
            + "sys.stdout.write(json.dumps([t['id'].to_pylist(), t['v'].to_pylist()]))\n";
        string scriptPath = Path.Combine(Path.GetTempPath(), "ew-lance-read-" + Guid.NewGuid().ToString("N") + ".py");
        File.WriteAllText(scriptPath, script);
        try
        {
            var psi = new ProcessStartInfo("python", $"-I \"{scriptPath}\" \"{dataset}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            var stdout = proc.StandardOutput.ReadToEndAsync();
            var stderr = proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0, $"pylance exited {proc.ExitCode}: {await stderr}");

            using var json = System.Text.Json.JsonDocument.Parse(await stdout);
            var ids = json.RootElement[0].EnumerateArray().Select(e => e.GetInt32()).ToArray();
            var values = json.RootElement[1].EnumerateArray().Select(e => e.GetString()!).ToArray();
            var rows = new SortedDictionary<int, string>();
            for (int i = 0; i < ids.Length; i++)
                rows.Add(ids[i], values[i]);
            return rows;
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }

    private static async Task<int[]> ReadIdsAsync(LanceTable table)
    {
        var ids = new List<int>();
        await foreach (var batch in table.ReadAsync())
            ids.AddRange(((Int32Array)batch.Column(0)).Values.ToArray());
        ids.Sort();
        return ids.ToArray();
    }

    private static string LastSegment(string path) =>
        path.Split('/', '\\').Last(s => s.Length > 0);

    private static string FileUri(string localPath)
    {
        string slashed = localPath.Replace('\\', '/');
        return "file://" + (slashed.StartsWith("/", StringComparison.Ordinal) ? "" : "/") + slashed;
    }

    private static string[] ListFiles(string root) =>
        Directory.GetFiles(root, "*", SearchOption.AllDirectories)
            .Select(f => f.Substring(root.Length) + ":" + new FileInfo(f).Length)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();

    private static void RewriteBases(string dataset, Action<Proto.BasePath> edit) =>
        RewriteManifests(dataset, m =>
        {
            foreach (var basePath in m.BasePaths)
                edit(basePath);
        });

    /// <summary>
    /// Rewrites the manifest message of every manifest under <paramref name="dataset"/>'s <c>_versions/</c>, keeping
    /// every byte before it (so any section offsets stay valid) and the footer's version and magic.
    /// </summary>
    private static void RewriteManifests(string dataset, Action<Proto.Manifest> edit)
    {
        foreach (string file in Directory.GetFiles(Path.Combine(dataset, "_versions"), "*.manifest"))
        {
            byte[] bytes = File.ReadAllBytes(file);
            const int footerSize = 16;
            int footer = bytes.Length - footerSize;
            int lengthPos = checked((int)BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(footer)));
            int length = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(lengthPos)));
            var manifest = Proto.Manifest.Parser.ParseFrom(bytes, lengthPos + 4, length);
            edit(manifest);
            byte[] body = manifest.ToByteArray();

            using var output = new MemoryStream();
            output.Write(bytes, 0, lengthPos);
            var lengthBytes = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(lengthBytes, (uint)body.Length);
            output.Write(lengthBytes, 0, 4);
            output.Write(body, 0, body.Length);
            var position = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(position, lengthPos);
            output.Write(position, 0, 8);
            output.Write(bytes, footer + 8, footerSize - 8);
            File.WriteAllBytes(file, output.ToArray());
        }
    }

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (string dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(to, dir.Substring(from.Length + 1)));
        foreach (string file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
            File.Copy(file, Path.Combine(to, file.Substring(from.Length + 1)));
    }
}
