// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Expressions;
using Google.Protobuf;
using static EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Indices listed by the manifest's index section (#991), with fixtures pylance 14.0.3 wrote
/// (<c>generate_test_data.py index_section</c>). Both have fragments 0 = [1, 2, 3], 1 = [10, 20, 30] and
/// 2 = [100, 200], and a BTREE index <c>x_idx</c> built before fragment 2 was appended:
/// <list type="bullet">
/// <item><c>index_vacuumed_v21</c>: then vacuumed, which deleted the <c>CreateIndex</c> transaction file. The index's
/// one segment covers fragments 0 and 1 only.</item>
/// <item><c>index_segments_v21</c>: then <c>optimize_indices</c> without merging, which added a second segment of
/// <c>x_idx</c> covering fragment 2.</item>
/// </list>
/// A read yields one batch per fragment it scans, so a batch count is the number of fragments not pruned.
/// </summary>
public sealed class IndexSectionTests : IDisposable
{
    private readonly string _root;

    public IndexSectionTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "ew-lance-index-section-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    // --- discovery --------------------------------------------------------------------------------

    [Fact]
    public async Task VacuumedCreateIndexTransaction_StillListsTheIndex()
    {
        string dataset = TestDataPath.Resolve("index_vacuumed_v21");
        // The fixture is what the test says it is: the only transaction file left is the append's.
        Assert.Single(Directory.GetFiles(Path.Combine(dataset, "_transactions"), "*.txn"));

        await using var table = await LanceTable.OpenAsync(dataset);
        var info = Assert.Single(await table.GetIndicesAsync());
        Assert.Equal("x_idx", info.Name);
        Assert.Equal(["x"], info.ColumnNames);
        Assert.Equal(new uint[] { 0, 1 }, info.FragmentIds);
        Assert.Null(info.BaseId);
        Assert.Equal($"_indices/{info.Uuid:D}", info.DirectoryPath);
        Assert.Equal(2UL, info.BuiltFromVersion);
        Assert.True(Directory.Exists(Path.Combine(dataset, "_indices", info.Uuid.ToString("D"))));
    }

    [Fact]
    public async Task Segments_AreListedOnePerEntry_OldestFirst()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("index_segments_v21"));
        var indices = await table.GetIndicesAsync();

        Assert.Equal(2, indices.Count);
        Assert.All(indices, i => Assert.Equal("x_idx", i.Name));
        Assert.Equal(new uint[] { 0, 1 }, indices[0].FragmentIds);
        Assert.Equal(new uint[] { 2 }, indices[1].FragmentIds);
        Assert.True(indices[0].BuiltFromVersion < indices[1].BuiltFromVersion);
        Assert.NotEqual(indices[0].Uuid, indices[1].Uuid);
    }

    [Fact]
    public async Task OpeningASegmentedIndexByName_IsRefused_ButEachSegmentOpens()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("index_segments_v21"));

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => table.OpenBTreeIndexAsync("x_idx").AsTask());
        Assert.Contains("2 segments", ex.Message);

        var indices = await table.GetIndicesAsync();
        await using (var first = await table.OpenBTreeIndexAsync(indices[0]))
        {
            Assert.Equal(new uint[] { 1 }, (await first.QueryEqualAsync(20)).Select(h => h.FragmentId));
            Assert.Empty(await first.QueryEqualAsync(200));
        }
        await using (var second = await table.OpenBTreeIndexAsync(indices[1]))
            Assert.Equal(new uint[] { 2 }, (await second.QueryEqualAsync(200)).Select(h => h.FragmentId));
    }

    [Fact]
    public async Task OpeningAnIndexOfAnotherTable_IsRefused()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("index_vacuumed_v21"));
        await using var other = await LanceTable.OpenAsync(TestDataPath.Resolve("index_vacuumed_v21"));
        var info = Assert.Single(await other.GetIndicesAsync());

        await Assert.ThrowsAsync<ArgumentException>(() => table.OpenBTreeIndexAsync(info).AsTask());
    }

    // --- pruning ----------------------------------------------------------------------------------

    [Fact]
    public async Task AFragmentNoSegmentCovers_IsAlwaysScanned()
    {
        // x_idx covers fragments 0 and 1. It has no entry for 200, but that says nothing about fragment 2.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("index_vacuumed_v21"));

        var (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(200)));
        Assert.Equal([200], values);
        Assert.Equal(1, batches);  // fragments 0 and 1 pruned

        (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(20)));
        Assert.Equal([20], values);
        Assert.Equal(2, batches);  // fragment 1 by the index, fragment 2 because it covers it not

        (batches, values) = await ReadXAsync(table, GreaterThanOrEqual("x", LiteralValue.Of(30)));
        Assert.Equal([30, 100, 200], values);
        Assert.Equal(2, batches);

        (batches, values) = await ReadXAsync(table,
            new SetPredicate(new UnboundReference("x"), [LiteralValue.Of(2), LiteralValue.Of(100)], SetOperator.In));
        Assert.Equal([2, 100], values);
        Assert.Equal(2, batches);
    }

    [Fact]
    public async Task EverySegment_AnswersForItsOwnFragments()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("index_segments_v21"));

        var (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(200)));
        Assert.Equal([200], values);
        Assert.Equal(1, batches);

        (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(20)));
        Assert.Equal([20], values);
        Assert.Equal(1, batches);  // the second segment covers fragment 2 and has no 20

        (batches, values) = await ReadXAsync(table, LessThan("x", LiteralValue.Of(5)));
        Assert.Equal([1, 2, 3], values);
        Assert.Equal(1, batches);

        (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(7)));
        Assert.Empty(values);
        Assert.Equal(0, batches);
    }

    // --- writer and refusals ----------------------------------------------------------------------

    [Fact]
    public async Task EWAppend_WritesNoIndexSection_SoTheIndexIsNoLongerListed()
    {
        // EW's writer does not carry indices forward (yet): its manifest has no index section, which lance reads as
        // "no indices" too. Reads stay right; they are just not pruned.
        string dataset = Copy("index_vacuumed_v21");
        await using (var writer = await LanceDatasetWriter.AppendAsync(dataset))
        {
            await writer.FileWriter.WriteInt32ColumnAsync("x", new[] { 7 });
            await writer.FinishAsync();
        }

        await using var table = await LanceTable.OpenAsync(dataset);
        Assert.Empty(await table.GetIndicesAsync());
        var (_, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(7)));
        Assert.Equal([7], values);
    }

    [Theory]
    [InlineData(1UL << 7)]   // FLAG_COVERED_INDEX_METADATA (lance-table 14.0.3)
    [InlineData(1UL << 13)]  // FLAG_INDEPENDENT_COVERING_FIELDS
    public async Task CoveringIndexFlags_AreRefused(ulong flag)
    {
        // Both change what IndexMetadata.fields means: some of its fields are only carried, not keyed on, so reading
        // the index as keyed on every one would prune by a column it does not order.
        string dataset = Copy("index_vacuumed_v21");
        LanceBasePathsTests.RewriteManifests(dataset, m => m.ReaderFeatureFlags |= flag);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => LanceTable.OpenAsync(dataset).AsTask());
        Assert.Contains("feature flags", ex.Message);
    }

    [Fact]
    public async Task AnIndexSectionOutsideTheFile_IsRefused()
    {
        string dataset = Copy("index_vacuumed_v21");
        LanceBasePathsTests.RewriteManifests(dataset, m => m.IndexSection = 1UL << 40);

        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(() => LanceTable.OpenAsync(dataset).AsTask());
        Assert.Contains("index section", ex.Message);
    }

    [Fact]
    public async Task AnIndexSectionPointingAtTheTransaction_IsRefused()
    {
        // pylance 14.0.3 writes the index section at 0 and the transaction after it, pointed at by field 23. The
        // transaction lies before the manifest, so only the overlap check keeps it from being read as an index section.
        string dataset = Copy("index_vacuumed_v21");
        LanceBasePathsTests.RewriteManifests(dataset, m =>
        {
            Assert.True(m.HasTransactionSection);
            m.IndexSection = m.TransactionSection;
        });

        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(() => LanceTable.OpenAsync(dataset).AsTask());
        Assert.Contains("overlaps the transaction section", ex.Message);
    }

    [Fact]
    public async Task AnIndexSectionRunningIntoTheManifest_IsRefused()
    {
        string dataset = Copy("index_vacuumed_v21");
        LanceBasePathsTests.RewriteManifests(dataset, m => m.ClearTransactionSection());
        string file = Assert.Single(Directory.GetFiles(Path.Combine(dataset, "_versions"), "*.manifest"));
        byte[] bytes = File.ReadAllBytes(file);
        int manifestPos = checked((int)BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(bytes.Length - 16)));
        // The section at 0 now claims every byte up to and including the manifest's length prefix.
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, (uint)manifestPos);
        File.WriteAllBytes(file, bytes);

        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(() => LanceTable.OpenAsync(dataset).AsTask());
        Assert.Contains("runs into the manifest", ex.Message);
    }

    [Fact]
    public async Task AFragmentIdBeyond32Bits_ReadsUnpruned()
    {
        // lance never writes one (max_fragment_id is a u32), and an index's bitmap cannot name it; such a manifest is
        // read without pruning rather than refused.
        string dataset = Copy("index_vacuumed_v21");
        LanceBasePathsTests.RewriteManifests(dataset, m => m.Fragments[2].Id = 1UL << 33);

        await using var table = await LanceTable.OpenAsync(dataset);
        var (batches, values) = await ReadXAsync(table, Equal("x", LiteralValue.Of(200)));
        Assert.Equal([200], values);
        Assert.Equal(3, batches);
    }

    [Fact]
    public async Task AnIndexNamingABaseTheManifestDoesNotList_IsRefused()
    {
        string dataset = Copy("index_vacuumed_v21");
        RewriteIndexSection(dataset, section => section.Indices[0].BaseId = 7);

        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(() => LanceTable.OpenAsync(dataset).AsTask());
        Assert.Contains("base path id 7", ex.Message);
    }

    [Fact]
    public async Task AnIndexWithoutAUuid_IsRefused()
    {
        string dataset = Copy("index_vacuumed_v21");
        RewriteIndexSection(dataset, section => section.Indices[0].Uuid = null);

        var ex = await Assert.ThrowsAsync<LanceTableFormatException>(() => LanceTable.OpenAsync(dataset).AsTask());
        Assert.Contains("UUID", ex.Message);
    }

    // --- helpers ----------------------------------------------------------------------------------

    private string Copy(string fixture)
    {
        string target = Path.Combine(_root, fixture);
        LanceBasePathsTests.CopyDirectory(TestDataPath.Resolve(fixture), target);
        return target;
    }

    private static async Task<(int Batches, int[] Values)> ReadXAsync(LanceTable table, Predicate filter)
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

    /// <summary>
    /// Rewrites the index section of every manifest of <paramref name="dataset"/> that has one: the edited section is
    /// written after the bytes that precede the manifest message (so every other section offset stays valid), and the
    /// manifest, pointed at it, after that.
    /// </summary>
    internal static void RewriteIndexSection(string dataset, Action<Proto.IndexSection> edit)
    {
        foreach (string file in Directory.GetFiles(Path.Combine(dataset, "_versions"), "*.manifest"))
        {
            byte[] bytes = File.ReadAllBytes(file);
            const int footerSize = 16;
            int footer = bytes.Length - footerSize;
            int lengthPos = checked((int)BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(footer)));
            int length = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(lengthPos)));
            var manifest = Proto.Manifest.Parser.ParseFrom(bytes, lengthPos + 4, length);
            if (!manifest.HasIndexSection)
                continue;

            int sectionPos = checked((int)manifest.IndexSection);
            int sectionLength = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(sectionPos)));
            var section = Proto.IndexSection.Parser.ParseFrom(bytes, sectionPos + 4, sectionLength);
            edit(section);

            using var output = new MemoryStream();
            output.Write(bytes, 0, lengthPos);
            manifest.IndexSection = (ulong)output.Position;
            WriteMessage(output, section.ToByteArray());
            long manifestPos = output.Position;
            WriteMessage(output, manifest.ToByteArray());
            var position = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(position, manifestPos);
            output.Write(position, 0, 8);
            output.Write(bytes, footer + 8, footerSize - 8);
            File.WriteAllBytes(file, output.ToArray());
        }
    }

    private static void WriteMessage(Stream output, byte[] message)
    {
        var lengthBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lengthBytes, (uint)message.Length);
        output.Write(lengthBytes, 0, 4);
        output.Write(message, 0, message.Length);
    }
}
