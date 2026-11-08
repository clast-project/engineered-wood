// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Expressions;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// An index answers only for the column it covers, found by its path, not by its own name (#771). The fixture
/// <c>nested_index_v21</c> has a BTREE index on struct field <c>s.x</c> beside an unindexed top-level <c>x</c>, in
/// two fragments: <c>x</c> holds 0..99 and 1000..1099, <c>s.x</c> 100..199 and 1100..1199. Keyed by the field's
/// own name, the index answered for <c>x</c>, found no fragment holding 5, and the scan read nothing.
/// </summary>
public class NestedIndexPruningTests
{
    private static ValueTask<LanceTable> OpenAsync() => LanceTable.OpenAsync(TestDataPath.Resolve("nested_index_v21"));

    [Fact]
    public async Task AnIndexOnAStructField_IsListedUnderItsPath()
    {
        await using var table = await OpenAsync();
        var info = Assert.Single(await table.GetIndicesAsync());
        Assert.Equal(["s.x"], info.ColumnNames);
        var path = Assert.Single(info.ColumnPaths);
        Assert.Equal(["s", "x"], path);
    }

    [Theory]
    [InlineData(5, 1)]
    [InlineData(1005, 1)]
    [InlineData(150, 0)]
    public async Task AFilterOnATopLevelColumn_IsNotJudgedByAStructFieldsIndex(int value, int expected)
    {
        await using var table = await OpenAsync();
        long rows = 0;
        await foreach (var batch in table.ReadAsync(
            columns: ["x"], filter: Expressions.Expressions.Equal("x", LiteralValue.Of(value))))
        {
            rows += batch.Length;
        }
        Assert.Equal(expected, rows);
    }
}
