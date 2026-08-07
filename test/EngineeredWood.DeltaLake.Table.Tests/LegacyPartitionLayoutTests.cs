// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <para>The upgrade path for GitHub issue #84. Making <see cref="DeltaPath.EscapePathName(string)"/>
/// platform-dependent — as Spark's is — changed the directory EW builds on Windows for a partition
/// value containing a space: <c>region=a b</c> before, <c>region=a%20b</c> after. Tables written by an
/// older EW on Windows, and tables written by ANY Delta writer on POSIX, carry the old spelling.</para>
///
/// <para>Both must still read, and an append must still work against them. That holds because a reader
/// resolves files through <c>add.path</c> and never by parsing directory names — so the old directory
/// keeps being found while new files land beside it under the new name. MEASURED to be exactly what
/// Spark itself does in the same situation: pointed at a POSIX-written table on Windows it reads it,
/// then appends into a SECOND directory next to the first, and reads both back as one logical
/// partition value.</para>
///
/// <para>Windows-only, because on POSIX the two spellings coincide and there is nothing to test.</para>
/// </summary>
public class LegacyPartitionLayoutTests : IDisposable
{
    private readonly string _tempDir;

    public LegacyPartitionLayoutTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_legacypart_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdRegionSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("region").DataType(StringType.Default).Nullable(false))
        .Build();

    private static RecordBatch Batch(long[] ids, string[] regions)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var regionBuilder = new StringArray.Builder();
        foreach (string r in regions)
            regionBuilder.Append(r);
        return new RecordBatch(IdRegionSchema, [idArray, regionBuilder.Build()], ids.Length);
    }

    private async Task<List<(long Id, string Region)>> ReadAll()
    {
        await using var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));
        var rows = new List<(long, string)>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var regions = (StringArray)batch.Column("region");
            for (int i = 0; i < batch.Length; i++)
                rows.Add((ids.GetValue(i)!.Value, regions.GetString(i)));
        }
        rows.Sort();
        return rows;
    }

    private static List<string> PartitionDirectories(string root) =>
        Directory.GetDirectories(root)
            .Select(d => Path.GetFileName(d)!)
            .Where(d => d != "_delta_log")
            .OrderBy(d => d, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Rewrites the table EW just wrote into the layout an older EW (or a POSIX writer) would have
    /// produced: the partition directory renamed to its unescaped spelling, and every <c>add.path</c>
    /// re-encoded from that name. Fabricating the old layout this way rather than checking in a fixture
    /// keeps the parquet and the log self-consistent without freezing a writer version into the repo.
    /// </summary>
    private void RewriteToLegacyLayout(string escapedDir, string legacyDir)
    {
        Directory.Move(Path.Combine(_tempDir, escapedDir), Path.Combine(_tempDir, legacyDir));

        string commit = Path.Combine(_tempDir, "_delta_log", $"{1:D20}.json");
        var rewritten = new List<string>();
        foreach (string line in File.ReadAllLines(commit))
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            using var doc = JsonDocument.Parse(line);
            if (!doc.RootElement.TryGetProperty("add", out var add))
            {
                rewritten.Add(line);
                continue;
            }

            // Re-encode the add.path off the legacy directory name. Uses the same layer-2 encoder the
            // writer uses, so only the directory spelling differs from what EW produced.
            string decoded = DeltaPath.Decode(add.GetProperty("path").GetString()!);
            string fileName = decoded[(decoded.IndexOf('/') + 1)..];
            var node = System.Text.Json.Nodes.JsonNode.Parse(line)!;
            node["add"]!["path"] = DeltaPath.Encode(legacyDir + "/" + fileName);
            rewritten.Add(node.ToJsonString());
        }
        File.WriteAllLines(commit, rewritten);
    }

    [Fact]
    public async Task LegacyDirectorySpelling_StillReadsAndAppends()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return;

        await using (var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdRegionSchema, partitionColumns: ["region"]))
        {
            await table.WriteAsync([Batch([1], ["a b"])]);
        }

        // Precondition: the NEW spelling is what a current EW writes.
        Assert.Equal(["region=a%20b"], PartitionDirectories(_tempDir));

        RewriteToLegacyLayout("region=a%20b", "region=a b");
        Assert.Equal(["region=a b"], PartitionDirectories(_tempDir));

        // The legacy table reads.
        Assert.Equal([(1L, "a b")], await ReadAll());

        // An append to it works, and lands under the new spelling beside the old directory.
        await using (var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)))
        {
            await table.WriteAsync([Batch([2], ["a b"])]);
        }

        Assert.Equal(["region=a b", "region=a%20b"], PartitionDirectories(_tempDir));

        // Two physical directories, one logical partition value, both rows visible.
        Assert.Equal([(1L, "a b"), (2L, "a b")], await ReadAll());
    }
}
