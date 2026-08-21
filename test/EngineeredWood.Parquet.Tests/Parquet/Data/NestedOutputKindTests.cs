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

    // ViewType is missing here on purpose: a nested column is assembled through
    // ArrowCompute.Take, which has no StringView gather, so that combination throws before it
    // reaches any of these assertions. That gap predates this test and is not INT96's to fix.
    [Theory]
    [InlineData(ByteArrayOutputKind.Default, typeof(StringType), typeof(StringArray))]
    [InlineData(ByteArrayOutputKind.LargeOffsets, typeof(LargeStringType), typeof(LargeStringArray))]
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

    [Fact]
    public async Task DeeplyNestedString_ElementTypeFollowsTheOutputKind()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("nested_lists.snappy.parquet"));
        using var reader = new ParquetFileReader(
            file, ownsFile: false,
            new ParquetReadOptions { ByteArrayOutput = ByteArrayOutputKind.LargeOffsets });

        using var batch = await reader.ReadRowGroupAsync(0, ["a"]);

        Assert.IsType<LargeStringType>(ElementTypeOf(ElementTypeOf(
            ElementTypeOf(batch.Schema.GetFieldByName("a").DataType))));

        var outer = Assert.IsType<ListArray>(batch.Column(0));
        var middle = Assert.IsType<ListArray>(outer.Values);
        var inner = Assert.IsType<ListArray>(middle.Values);
        Assert.IsType<LargeStringType>(ElementTypeOf(inner.Data.DataType));
        Assert.IsType<LargeStringArray>(inner.Values);
    }
}
