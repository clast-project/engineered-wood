// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;
using static EngineeredWood.Expressions.Expressions;
using EngineeredWood.Lance.Table.Indices;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Tests for <see cref="BitmapIndex"/>. The fixture
/// <c>bitmap_index_v21</c> has 3 fragments with this layout:
/// <list type="bullet">
///   <item>fragment 0: [1, 2, 3]</item>
///   <item>fragment 1: [1, 2, 1]</item>
///   <item>fragment 2: [3, 1, 2]</item>
/// </list>
/// All three unique values appear in all three fragments — except
/// when querying with a filter we still expect index lookup to
/// short-circuit obvious things (no value 99 anywhere).
/// </summary>
public class BitmapIndexTests
{
    private static string FindIndexDir()
    {
        string indicesRoot = Path.Combine(TestDataPath.Resolve("bitmap_index_v21"), "_indices");
        var dirs = Directory.GetDirectories(indicesRoot);
        Assert.Single(dirs);
        return dirs[0];
    }

    [Fact]
    public async Task QueryEqual_ValuePresentInAllFragments()
    {
        await using var idx = await BitmapIndex.OpenAsync(FindIndexDir());
        var hits = await idx.QueryEqualAsync(1);
        // x=1 occurs at: frag0 row0; frag1 rows 0,2; frag2 row 1. Total 4.
        Assert.Equal(4, hits.Count);
        var byFragment = hits
            .GroupBy(h => h.FragmentId)
            .ToDictionary(g => g.Key, g => g.Select(h => h.RowOffset).OrderBy(r => r).ToArray());
        Assert.Equal(new uint[] { 0 }, byFragment[0u]);
        Assert.Equal(new uint[] { 0, 2 }, byFragment[1u]);
        Assert.Equal(new uint[] { 1 }, byFragment[2u]);
    }

    [Fact]
    public async Task QueryEqual_AbsentValue()
    {
        await using var idx = await BitmapIndex.OpenAsync(FindIndexDir());
        var hits = await idx.QueryEqualAsync(99);
        Assert.Empty(hits);
    }

    [Fact]
    public async Task QueryEqual_ValueInOneFragment()
    {
        await using var idx = await BitmapIndex.OpenAsync(FindIndexDir());
        // x=3 occurs at: frag0 row2; frag2 row0. So 2 hits across 2 fragments.
        var hits = await idx.QueryEqualAsync(3);
        Assert.Equal(2, hits.Count);
        var fragments = hits.Select(h => h.FragmentId).OrderBy(f => f).ToArray();
        Assert.Equal(new uint[] { 0, 2 }, fragments);
    }

    [Fact]
    public async Task LanceTable_GetIndices_RecognisesBitmap()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("bitmap_index_v21"));
        var indices = await table.GetIndicesAsync();
        Assert.Single(indices);
        Assert.Equal(new[] { "x" }, indices[0].ColumnNames);
        Assert.Contains("BitmapIndex", indices[0].TypeUrl);
    }

    [Fact]
    public async Task LanceTable_OpenBitmapIndexByName()
    {
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("bitmap_index_v21"));
        await using var idx = await table.OpenBitmapIndexAsync("x_idx");
        var hits = await idx.QueryEqualAsync(2);
        // x=2 occurs at: frag0 row1; frag1 row1; frag2 row2. 3 hits.
        Assert.Equal(3, hits.Count);
    }

    [Fact]
    public async Task ReadAsync_WithEqualFilter_BitmapPrunesAbsentFragments()
    {
        // None of our fragments lack values 1/2/3, so let's pick a
        // construction that DOES exercise the pruner: only fragment 1
        // contains x=1 with row offset 2, and the equality filter
        // `x = 99` should prune ALL fragments.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("bitmap_index_v21"));
        var filter = Equal("x", LiteralValue.Of(99));
        int batches = 0, rows = 0;
        await foreach (var batch in table.ReadAsync(columns: null, filter: filter))
        {
            batches++;
            rows += batch.Length;
        }
        Assert.Equal(0, batches);
        Assert.Equal(0, rows);
    }

    [Fact]
    public async Task ReadAsync_WithInFilter_UnionsBitmapHits()
    {
        // `x IN (3)`: x=3 lives only in fragments 0 and 2 (per probe data).
        // The pruner should fetch fragment 1 zero times.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("bitmap_index_v21"));
        var filter = new SetPredicate(
            new UnboundReference("x"),
            new[] { LiteralValue.Of(3) },
            SetOperator.In);
        int batches = 0;
        var values = new List<int>();
        await foreach (var batch in table.ReadAsync(columns: null, filter: filter))
        {
            batches++;
            var col = (Apache.Arrow.Int32Array)batch.Column(0);
            for (int i = 0; i < col.Length; i++)
                values.Add(col.GetValue(i)!.Value);
        }
        Assert.Equal(2, batches);    // fragments 0 and 2 only
        Assert.Equal(new[] { 3, 3 }, values.OrderBy(v => v).ToArray());
    }
}
