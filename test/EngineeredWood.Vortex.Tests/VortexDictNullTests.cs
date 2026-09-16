// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Tests.TestData;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// A <c>vortex.dict</c> layout can carry a column's nulls in its dictionary values: a null
/// entry that null rows point at, with non-nullable codes. vortex 0.86's default writer does
/// this for low-cardinality nullable columns (<c>write_dict_nullable_values</c> in
/// <c>Rust/src/main.rs</c>).
/// </summary>
public class VortexDictNullTests
{
    private const string Fixture = "dict_nullable_values_20000rows.vortex";
    private const int Rows = 20_000;

    [Fact]
    public async Task BothColumnsUseADictLayout()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        Assert.All(reader.ColumnPlans, plan => Assert.IsType<DictColumnPlan>(plan));
    }

    [Fact]
    public async Task NullDictionaryEntryReadsAsNullNumber()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        var u = Assert.IsType<UInt16Array>(await reader.ReadColumnAsync(0));
        Assert.Equal(Rows, u.Length);
        Assert.Equal(Enumerable.Range(0, Rows).Count(i => i % 7 == 0), u.NullCount);
        for (int i = 0; i < Rows; i++)
            Assert.Equal(i % 7 == 0 ? null : (ushort)(i % 50), u.GetValue(i));
    }

    [Fact]
    public async Task NullDictionaryEntryReadsAsNullString()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        var tag = Assert.IsType<StringArray>(await reader.ReadColumnAsync(1));
        Assert.Equal(Rows, tag.Length);
        Assert.Equal(Enumerable.Range(0, Rows).Count(i => i % 11 == 0), tag.NullCount);
        for (int i = 0; i < Rows; i++)
        {
            if (i % 11 == 0)
                Assert.True(tag.IsNull(i), $"row {i}");
            else
                Assert.Equal($"t{i % 30}", tag.GetString(i));
        }
    }

    [Fact]
    public async Task StreamedBatchesCarryTheNulls()
    {
        await using var reader = await VortexFileReader.OpenAsync(TestDataPath.Resolve(Fixture));

        long row = 0;
        await foreach (var batch in reader.ReadAllAsync())
        {
            var u = (UInt16Array)batch.Column(0);
            var tag = (StringArray)batch.Column(1);
            for (int i = 0; i < batch.Length; i++, row++)
            {
                Assert.Equal(row % 7 == 0, u.IsNull(i));
                Assert.Equal(row % 11 == 0, tag.IsNull(i));
            }
        }
        Assert.Equal(Rows, row);
    }
}
