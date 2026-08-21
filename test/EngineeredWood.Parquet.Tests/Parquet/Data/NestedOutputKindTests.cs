// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// A leaf array's Arrow type comes from <c>ArrowSchemaConverter</c> reading the read options, while
/// the struct/list/map that wraps it gets its child field from <c>NestedAssembler</c>. Both have to
/// see the same options, or the wrapper declares a type its own data does not have — an array that
/// is internally inconsistent in exactly the way issue #165 was about.
/// </summary>
public class NestedOutputKindTests
{
    private static IArrowType ElementTypeOf(IArrowType listType) =>
        Assert.IsType<ListType>(listType).ValueDataType;

    // ViewType was missing here until issue #194: a nested column is assembled through
    // ArrowCompute.Take, which had no view-array gather, so that combination threw before it reached any
    // of these assertions. It is a full member of the theory now — this file is where the fix has to keep
    // holding, since the gap was invisible until a nested column with a null in the nesting was read.
    [Theory]
    [InlineData(ByteArrayOutputKind.Default, typeof(StringType), typeof(StringArray))]
    [InlineData(ByteArrayOutputKind.LargeOffsets, typeof(LargeStringType), typeof(LargeStringArray))]
    [InlineData(ByteArrayOutputKind.ViewType, typeof(StringViewType), typeof(StringViewArray))]
    public async Task ListOfString_ElementTypeFollowsTheOutputKind(
        ByteArrayOutputKind kind, Type expectedElementType, Type expectedElementArray)
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("list_columns.parquet"));
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = kind });

        using var batch = await reader.ReadRowGroupAsync(0, ["utf8_list"]);

        // What the schema advertises...
        Assert.IsType(
            expectedElementType, ElementTypeOf(batch.Schema.GetFieldByName("utf8_list").DataType));

        // ...what the array says it holds, and what it actually holds.
        var list = Assert.IsType<ListArray>(batch.Column(0));
        Assert.IsType(expectedElementType, ElementTypeOf(list.Data.DataType));
        Assert.IsType(expectedElementArray, list.Values);
    }

    [Theory]
    [InlineData(ByteArrayOutputKind.LargeOffsets, typeof(LargeStringType), typeof(LargeStringArray))]
    [InlineData(ByteArrayOutputKind.ViewType, typeof(StringViewType), typeof(StringViewArray))]
    public async Task DeeplyNestedString_ElementTypeFollowsTheOutputKind(
        ByteArrayOutputKind kind, Type expectedElementType, Type expectedElementArray)
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("nested_lists.snappy.parquet"));
        using var reader = new ParquetFileReader(
            file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = kind });

        using var batch = await reader.ReadRowGroupAsync(0, ["a"]);

        Assert.IsType(expectedElementType, ElementTypeOf(ElementTypeOf(
            ElementTypeOf(batch.Schema.GetFieldByName("a").DataType))));

        var outer = Assert.IsType<ListArray>(batch.Column(0));
        var middle = Assert.IsType<ListArray>(outer.Values);
        var inner = Assert.IsType<ListArray>(middle.Values);
        Assert.IsType(expectedElementType, ElementTypeOf(inner.Data.DataType));
        Assert.IsType(expectedElementArray, inner.Values);
    }

    /// <summary>
    /// The type check above would still pass on a gather that produced view entries pointing at the wrong
    /// rows, so this reads the same column twice — once as plain strings, once as views — and compares the
    /// VALUES. Three levels of list over a column whose nesting carries nulls is what made issue #194
    /// reachable in the first place; a run that only ever saw a non-null nesting never gathered at all.
    /// </summary>
    [Fact]
    public async Task ViewTypeReadsTheSameNestedValuesAsTheDefaultKind()
    {
        static async Task<List<string?>> LeavesAsync(ByteArrayOutputKind kind)
        {
            await using var file = new LocalRandomAccessFile(TestData.GetPath("list_columns.parquet"));
            using var reader = new ParquetFileReader(
                file, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = kind });

            using var batch = await reader.ReadRowGroupAsync(0, ["utf8_list"]);
            var list = Assert.IsType<ListArray>(batch.Column(0));

            var leaves = new List<string?>();
            for (int row = 0; row < list.Length; row++)
            {
                if (list.IsNull(row))
                {
                    leaves.Add(null);
                    continue;
                }

                // The offsets are logical positions in the shared child, so the child is read directly
                // rather than through a per-row slice.
                int start = list.ValueOffsets[row];
                int end = list.ValueOffsets[row + 1];
                leaves.Add($"[{end - start}]");
                for (int i = start; i < end; i++)
                {
                    leaves.Add(list.Values switch
                    {
                        StringArray s => s.GetString(i),
                        StringViewArray v => v.GetString(i),
                        _ => throw new Xunit.Sdk.XunitException(
                            $"unexpected element array {list.Values.GetType().Name}"),
                    });
                }
            }

            return leaves;
        }

        var expected = await LeavesAsync(ByteArrayOutputKind.Default);
        var actual = await LeavesAsync(ByteArrayOutputKind.ViewType);

        // A file with no rows would make the comparison vacuous, and this one does carry nulls in the
        // nesting — which is the condition that makes the assembler gather at all.
        Assert.NotEmpty(expected);
        Assert.Contains(null, expected);
        Assert.Equal(expected, actual);
    }
}
