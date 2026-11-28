// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
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

    // --- writes -----------------------------------------------------------------------------------

    [Fact]
    public async Task Writer_RefusesEveryOperation_AndLeavesTheDatasetUnchanged()
    {
        string[] before = ListFiles(_root);
        var pred = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual("id", LiteralValue.Of(0));

        await AssertWriterRefusesAsync(async () => await LanceDatasetWriter.AppendAsync(Clone));
        await AssertWriterRefusesAsync(async () => await LanceDatasetWriter.OverwriteAsync(Clone));
        await AssertWriterRefusesAsync(async () => await LanceDatasetWriter.DeleteAsync(Clone, pred));
        await AssertWriterRefusesAsync(async () =>
            await LanceDatasetWriter.DeleteRowsAsync(Clone,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0 } }));
        await AssertWriterRefusesAsync(async () =>
            await LanceDatasetWriter.UpdateAsync(Clone, pred,
                new Dictionary<string, Expression> { ["v"] = new LiteralExpression(LiteralValue.Of("x")) }));
        await AssertWriterRefusesAsync(async () => await LanceDatasetWriter.CompactAsync(Clone));
        await AssertWriterRefusesAsync(async () => await LanceDatasetWriter.VacuumAsync(Multi));

        Assert.Equal(before, ListFiles(_root));
    }

    // --- helpers ----------------------------------------------------------------------------------

    private static async Task AssertWriterRefusesAsync(Func<Task> write)
    {
        var ex = await Assert.ThrowsAsync<NotSupportedException>(write);
        Assert.Contains("base path(s)", ex.Message, StringComparison.Ordinal);
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
