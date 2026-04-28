// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Tests.TestData;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Phase 5 cross-validation — nested types (struct, list, fixed-size list)
/// against pylance-produced v2.0 files.
/// </summary>
public class PylanceNestedTests
{
    [Fact]
    public async Task List_Int32_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_int.lance"));
        Assert.IsType<Apache.Arrow.Types.ListType>(reader.Schema.FieldsList[0].DataType);

        var arr = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, arr.Length);
        Assert.Equal(0, arr.NullCount);

        Assert.Equal(new int?[] { 1, 2, 3 }, ((Int32Array)arr.GetSlicedValues(0)).ToArray());
        Assert.Equal(new int?[] { 4 }, ((Int32Array)arr.GetSlicedValues(1)).ToArray());
        Assert.Equal(new int?[] { }, ((Int32Array)arr.GetSlicedValues(2)).ToArray());
        Assert.Equal(new int?[] { 5, 6 }, ((Int32Array)arr.GetSlicedValues(3)).ToArray());
    }

    [Fact]
    public async Task List_Int32_WithNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_nulls.lance"));
        var arr = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, arr.NullCount);

        Assert.Equal(new int?[] { 1, 2 }, ((Int32Array)arr.GetSlicedValues(0)).ToArray());
        Assert.True(arr.IsNull(1));
        Assert.Equal(new int?[] { 3, 4 }, ((Int32Array)arr.GetSlicedValues(2)).ToArray());
    }

    [Fact]
    public async Task FixedSizeList_Int32()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("fsl_int.lance"));
        Assert.IsType<FixedSizeListType>(reader.Schema.FieldsList[0].DataType);

        var arr = (FixedSizeListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(2, arr.Length);
        Assert.Equal(3, ((FixedSizeListType)arr.Data.DataType).ListSize);

        var values = (Int32Array)arr.Values;
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5, 6 }, values.ToArray());
    }

    [Fact]
    public async Task Struct_TwoInt32_NoNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_2i32.lance"));
        Assert.IsType<StructType>(reader.Schema.FieldsList[0].DataType);

        var arr = (StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(2, arr.Length);
        Assert.Equal(0, arr.NullCount);

        var a = (Int32Array)arr.Fields[0];
        var b = (Int32Array)arr.Fields[1];
        Assert.Equal(new int?[] { 1, 2 }, a.ToArray());
        Assert.Equal(new int?[] { 10, 20 }, b.ToArray());
    }

    [Fact]
    public async Task Struct_NullsLostInV20()
    {
        // v2.0 SimpleStruct has no validity bitmap, so the "null struct" row
        // round-trips as {a:0, b:0} (pylance-confirmed behavior). We assert
        // this known-loss semantic rather than trying to recover nulls.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("struct_nulls.lance"));
        var arr = (StructArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, arr.NullCount);

        var a = (Int32Array)arr.Fields[0];
        var b = (Int32Array)arr.Fields[1];
        Assert.Equal(new int?[] { 1, 0, 3 }, a.ToArray());
        Assert.Equal(new int?[] { 10, 0, 30 }, b.ToArray());
    }

    [Fact]
    public async Task ListOfStruct()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("list_struct.lance"));
        // Arrow type: List<Struct<a: Int32>>
        var listType = Assert.IsType<Apache.Arrow.Types.ListType>(reader.Schema.FieldsList[0].DataType);
        Assert.IsType<StructType>(listType.ValueDataType);

        var list = (ListArray)await reader.ReadColumnAsync(0);
        Assert.Equal(2, list.Length);
        Assert.Equal(0, list.NullCount);

        // Row 0: [{a:1}, {a:2}]
        var row0 = (StructArray)list.GetSlicedValues(0);
        Assert.Equal(2, row0.Length);
        Assert.Equal(new int?[] { 1, 2 }, ((Int32Array)row0.Fields[0]).ToArray());

        // Row 1: [{a:3}]
        var row1 = (StructArray)list.GetSlicedValues(1);
        Assert.Equal(1, row1.Length);
        Assert.Equal(new int?[] { 3 }, ((Int32Array)row1.Fields[0]).ToArray());
    }
}
