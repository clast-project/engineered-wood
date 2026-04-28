// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Tests.TestData;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Phase 2 cross-validation: read pylance-produced Lance v2.0 files and
/// assert the decoded Arrow array matches the original Python input.
/// </summary>
public class PylanceCrossValidationTests
{
    [Fact]
    public async Task Int32_NonNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int32_nonull.lance"));
        Assert.Equal(5L, reader.NumberOfRows);
        Assert.IsType<Int32Type>(reader.Schema.FieldsList[0].DataType);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(5, arr.Length);
        Assert.Equal(0, arr.NullCount);
        Assert.Equal(new int?[] { 1, 2, 3, 4, 5 }, arr.ToArray());
    }

    [Fact]
    public async Task Int32_WithNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int32_nulls.lance"));
        Assert.Equal(5L, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(5, arr.Length);
        Assert.Equal(2, arr.NullCount);
        Assert.Equal(1, arr.GetValue(0));
        Assert.Null(arr.GetValue(1));
        Assert.Equal(3, arr.GetValue(2));
        Assert.Null(arr.GetValue(3));
        Assert.Equal(5, arr.GetValue(4));
    }

    [Fact]
    public async Task Int32_AllNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int32_allnull.lance"));
        Assert.Equal(3L, reader.NumberOfRows);

        var arr = (Int32Array)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(3, arr.NullCount);
        Assert.Null(arr.GetValue(0));
        Assert.Null(arr.GetValue(1));
        Assert.Null(arr.GetValue(2));
    }

    [Fact]
    public async Task Int64_NonNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("int64_nonull.lance"));
        var arr = (Int64Array)await reader.ReadColumnAsync(0);
        Assert.Equal(new long?[] { 10, 20, 30, 40, 50 }, arr.ToArray());
        Assert.Equal(0, arr.NullCount);
    }

    [Fact]
    public async Task Double_NonNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("double_nonull.lance"));
        var arr = (DoubleArray)await reader.ReadColumnAsync(0);
        Assert.Equal(new double?[] { 1.5, 2.5, 3.5 }, arr.ToArray());
        Assert.Equal(0, arr.NullCount);
    }

    [Fact]
    public async Task Float_NonNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("float_nonull.lance"));
        var arr = (FloatArray)await reader.ReadColumnAsync(0);
        Assert.Equal(new float?[] { 1.5f, 2.5f, 3.5f }, arr.ToArray());
        Assert.Equal(0, arr.NullCount);
    }

    [Fact]
    public async Task Bool_NonNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("bool_nonull.lance"));
        var arr = (BooleanArray)await reader.ReadColumnAsync(0);
        Assert.Equal(4, arr.Length);
        Assert.True(arr.GetValue(0));
        Assert.False(arr.GetValue(1));
        Assert.True(arr.GetValue(2));
        Assert.True(arr.GetValue(3));
    }

    [Fact]
    public async Task String_NonNull()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("string_nonull.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, arr.NullCount);
        Assert.Equal("foo", arr.GetString(0));
        Assert.Equal("bar", arr.GetString(1));
        Assert.Equal("baz", arr.GetString(2));
    }

    [Fact]
    public async Task String_WithNulls()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("string_nulls.lance"));
        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, arr.NullCount);
        Assert.Equal("foo", arr.GetString(0));
        Assert.True(arr.IsNull(1));
        Assert.Equal("baz", arr.GetString(2));
    }

    [Fact]
    public async Task Dictionary_RepetitiveStrings()
    {
        // 2 unique values × 50 rows each → pylance emits an ArrayEncoding.Dictionary
        // with u8 indices and a Binary items sub-encoding. Exercises the real
        // DictionaryDecoder + BinaryDecoder composition end-to-end.
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("repetitive_strings.lance"));
        Assert.Equal(100L, reader.NumberOfRows);

        var arr = (StringArray)await reader.ReadColumnAsync(0);
        Assert.Equal(100, arr.Length);
        Assert.Equal(0, arr.NullCount);
        for (int i = 0; i < 50; i++)
            Assert.Equal("hello world", arr.GetString(i));
        for (int i = 50; i < 100; i++)
            Assert.Equal("goodbye world", arr.GetString(i));
    }

    [Fact]
    public async Task FixedSizeBinary_4()
    {
        await using var reader = await LanceFileReader.OpenAsync(TestDataPath.Resolve("fsb_nonull.lance"));
        // pylance writes fixed-size binary(4) as a FixedSizeBinary Arrow field.
        Assert.IsType<FixedSizeBinaryType>(reader.Schema.FieldsList[0].DataType);

        var arr = (FixedSizeBinaryArray)await reader.ReadColumnAsync(0);
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, arr.NullCount);
        Assert.Equal("abcd"u8.ToArray(), arr.GetBytes(0).ToArray());
        Assert.Equal("efgh"u8.ToArray(), arr.GetBytes(1).ToArray());
        Assert.Equal("ijkl"u8.ToArray(), arr.GetBytes(2).ToArray());
    }
}
