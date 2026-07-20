// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Table.Indices;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Tests for <see cref="BTreeIndex"/>. The fixture
/// <c>btree_index_v21</c> has 3 fragments with disjoint integer ranges
/// (1..5, 10..50, 100..500) and a BTREE scalar index on the column.
/// Row ids are encoded as <c>(fragment_id &lt;&lt; 32) | row_offset</c>.
/// </summary>
public class BTreeIndexTests
{
    private static string FindIndexDir()
    {
        string indicesRoot = Path.Combine(TestDataPath.Resolve("btree_index_v21"), "_indices");
        // Each index lives in its own UUID-named subdirectory; pylance
        // assigns a fresh UUID at index-creation time so we can't hardcode
        // the path. The fixture has exactly one index.
        var dirs = Directory.GetDirectories(indicesRoot);
        Assert.Single(dirs);
        return dirs[0];
    }

    [Fact]
    public async Task QueryEqual_FindsSingleRow()
    {
        await using var idx = await BTreeIndex.OpenAsync(FindIndexDir());
        var hits = await idx.QueryEqualAsync(200);
        Assert.Single(hits);
        // 200 is in fragment 2 (the third), at row offset 1 ([100, 200, 300, 400, 500]).
        Assert.Equal(2u, hits[0].FragmentId);
        Assert.Equal(1u, hits[0].RowOffset);
    }

    [Fact]
    public async Task QueryEqual_NoMatch()
    {
        await using var idx = await BTreeIndex.OpenAsync(FindIndexDir());
        var hits = await idx.QueryEqualAsync(7);  // 7 is between the value ranges
        Assert.Empty(hits);
    }

    [Fact]
    public async Task QueryRange_OpenLow()
    {
        await using var idx = await BTreeIndex.OpenAsync(FindIndexDir());
        // x > 100 → 200, 300, 400, 500 → all fragment 2 rows beyond row 0.
        var hits = await idx.QueryRangeAsync(min: 100, max: null, includeMin: false);
        Assert.Equal(4, hits.Count);
        foreach (var h in hits) Assert.Equal(2u, h.FragmentId);
        var offsets = hits.Select(h => h.RowOffset).OrderBy(o => o).ToList();
        Assert.Equal(new uint[] { 1, 2, 3, 4 }, offsets);
    }

    [Fact]
    public async Task QueryRange_StraddlesFragments()
    {
        await using var idx = await BTreeIndex.OpenAsync(FindIndexDir());
        // x in [3, 30] → 3, 4, 5 (fragment 0), 10, 20, 30 (fragment 1).
        var hits = await idx.QueryRangeAsync(min: 3, max: 30);
        Assert.Equal(6, hits.Count);
        var fragmentIds = hits.Select(h => h.FragmentId).Distinct().OrderBy(f => f).ToList();
        Assert.Equal(new uint[] { 0, 1 }, fragmentIds);
    }

    [Fact]
    public async Task ValueType_IsInt32()
    {
        await using var idx = await BTreeIndex.OpenAsync(FindIndexDir());
        Assert.IsType<Apache.Arrow.Types.Int32Type>(idx.ValueType);
    }

    [Fact]
    public async Task LanceTable_GetIndices_FindsBTreeIndex()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("btree_index_v21"));
        var indices = await table.GetIndicesAsync();
        Assert.Single(indices);
        var info = indices[0];
        Assert.Equal("x_idx", info.Name);             // pylance default name = "{column}_idx"
        Assert.Equal(new[] { "x" }, info.ColumnNames);
        Assert.Equal(new uint[] { 0, 1, 2 }, info.FragmentIds);
        Assert.Contains("BTreeIndexDetails", info.TypeUrl);
        Assert.Equal($"_indices/{info.Uuid:D}", info.DirectoryPath);
    }

    [Fact]
    public async Task LanceTable_OpenBTreeIndex_ByName()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("btree_index_v21"));
        await using var idx = await table.OpenBTreeIndexAsync("x_idx");
        var hits = await idx.QueryEqualAsync(300);
        Assert.Single(hits);
        Assert.Equal(2u, hits[0].FragmentId);
        Assert.Equal(2u, hits[0].RowOffset);
    }

    [Fact]
    public async Task LanceTable_OpenBTreeIndex_UnknownNameThrows()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("btree_index_v21"));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.OpenBTreeIndexAsync("nonexistent_idx"));
    }

    [Fact]
    public async Task LanceTable_GetIndices_NoIndicesReturnsEmpty()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("simple_v21"));
        var indices = await table.GetIndicesAsync();
        Assert.Empty(indices);
    }

    [Fact]
    public async Task ReadAsync_WithFilter_PrunesFragmentsViaIndex()
    {
        // The fixture's three fragments hold disjoint ranges: 0=[1..5],
        // 1=[10..50], 2=[100..500]. Filter `x > 100` matches only
        // fragment 2 (x=200,300,400,500). The index pruner must
        // recognise this and skip fragments 0 and 1.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("btree_index_v21"));
        var filter = EngineeredWood.Expressions.Expressions.GreaterThan(
            "x", EngineeredWood.Expressions.LiteralValue.Of(100));

        int batches = 0;
        var values = new List<int>();
        await foreach (var batch in table.ReadAsync(columns: null, filter: filter))
        {
            batches++;
            var col = (Apache.Arrow.Int32Array)batch.Column(0);
            for (int i = 0; i < col.Length; i++)
                values.Add(col.GetValue(i)!.Value);
        }
        // Without pruning we'd get 3 batches (one per fragment) with
        // post-decode filtering. With pruning, only fragment 2 gets read.
        Assert.Equal(1, batches);
        Assert.Equal(new[] { 200, 300, 400, 500 }, values);
    }

    [Fact]
    public async Task ReadAsync_WithFilter_StraddlesFragments()
    {
        // Filter `x BETWEEN 3 AND 30` matches fragment 0 (3,4,5) and
        // fragment 1 (10,20,30). Pruner should yield candidate set
        // {0, 1} and skip fragment 2.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("btree_index_v21"));
        var filter = new EngineeredWood.Expressions.AndPredicate(new EngineeredWood.Expressions.Predicate[] {
            EngineeredWood.Expressions.Expressions.GreaterThanOrEqual(
                "x", EngineeredWood.Expressions.LiteralValue.Of(3)),
            EngineeredWood.Expressions.Expressions.LessThanOrEqual(
                "x", EngineeredWood.Expressions.LiteralValue.Of(30)),
        });

        int batches = 0;
        var values = new List<int>();
        await foreach (var batch in table.ReadAsync(columns: null, filter: filter))
        {
            batches++;
            var col = (Apache.Arrow.Int32Array)batch.Column(0);
            for (int i = 0; i < col.Length; i++)
                values.Add(col.GetValue(i)!.Value);
        }
        Assert.Equal(2, batches);
        Assert.Equal(new[] { 3, 4, 5, 10, 20, 30 }, values);
    }

    [Fact]
    public async Task ReadAsync_WithEqualFilter_HitsOneFragment()
    {
        // Single point equality — only fragment 1 contains x=20.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("btree_index_v21"));
        var filter = EngineeredWood.Expressions.Expressions.Equal(
            "x", EngineeredWood.Expressions.LiteralValue.Of(20));

        int batches = 0;
        var values = new List<int>();
        await foreach (var batch in table.ReadAsync(columns: null, filter: filter))
        {
            batches++;
            var col = (Apache.Arrow.Int32Array)batch.Column(0);
            for (int i = 0; i < col.Length; i++)
                values.Add(col.GetValue(i)!.Value);
        }
        Assert.Equal(1, batches);
        Assert.Equal(new[] { 20 }, values);
    }
}
