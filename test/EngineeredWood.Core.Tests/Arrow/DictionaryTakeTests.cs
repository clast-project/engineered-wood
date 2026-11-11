// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// #794: <see cref="ArrowCompute.Take(IArrowArray, ReadOnlySpan{int})"/> gathers a dictionary column by its indices and
/// keeps its dictionary, as Arrow's own <c>take</c> does: the result is dictionary-encoded over the same dictionary,
/// its nulls follow the indices', and a sliced source gathers from its own logical rows.
/// </summary>
public class DictionaryTakeTests
{
    private static readonly DictionaryType Type = new(Int32Type.Default, StringType.Default, false);

    private static DictionaryArray Column()
    {
        var dictionary = new StringArray.Builder().Append("red").Append("green").Append("blue").Build();
        var indices = new Int32Array.Builder().Append(2).Append(0).AppendNull().Append(1).Append(2).Append(0).Build();
        return new DictionaryArray(Type, indices, dictionary);
    }

    private static string?[] Values(DictionaryArray array) =>
        [.. Enumerable.Range(0, array.Length).Select(i => array.IsNull(i)
            ? null
            : ((StringArray)array.Dictionary).GetString(((Int32Array)array.Indices).GetValue(i)!.Value))];

    [Fact]
    public void GathersTheIndicesAndSharesTheDictionary()
    {
        var source = Column();

        var taken = Assert.IsType<DictionaryArray>(ArrowCompute.Take(source, new[] { 4, 2, 0, 3, 3 }));

        Assert.Same(Type, taken.Data.DataType);
        Assert.Same(source.Dictionary, taken.Dictionary);
        Assert.Equal(["blue", null, "blue", "green", "green"], Values(taken));
        Assert.Equal(1, taken.NullCount);
    }

    [Fact]
    public void GathersFromASlicedSourcesOwnRows()
    {
        var sliced = (DictionaryArray)Column().Slice(2, 3); // null, green, blue

        var taken = Assert.IsType<DictionaryArray>(ArrowCompute.Take(sliced, new[] { 2, 1, 0 }));

        Assert.Equal(["blue", "green", null], Values(taken));
    }

    [Fact]
    public void GathersADictionaryColumnOfABatch()
    {
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("c", Type, true)], null), [Column()], 6);

        var taken = ArrowCompute.Take(batch, batch.Schema, new[] { 5, 1 });

        Assert.Equal(["red", "red"], Values((DictionaryArray)taken.Column(0)));
    }
}
