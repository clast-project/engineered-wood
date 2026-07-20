// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

public class DecimalReadTests
{
    [Fact]
    public async Task Int32Decimal_ReadsAsDecimal32()
    {
        // int32_decimal.parquet: 24 rows, precision=4, scale=2, physical INT32
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int32_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(24, batch.Length);
        var field = batch.Schema.GetFieldByName("value");
        var decType = Assert.IsType<Decimal32Type>(field.DataType);
        Assert.Equal(4, decType.Precision);
        Assert.Equal(2, decType.Scale);

        var array = (Decimal32Array)batch.Column("value");
        Assert.Equal(24, array.Length);

        // Values are 1.00 .. 24.00 (stored as unscaled ints 100..2400)
        // Decimal32Array stores 4-byte LE values; read raw and verify
        for (int i = 0; i < 24; i++)
        {
            Assert.False(array.IsNull(i), $"Row {i} should not be null");
        }
    }

    [Fact]
    public async Task Int64Decimal_ReadsAsDecimal64()
    {
        // int64_decimal.parquet: 24 rows, precision=10, scale=2, physical INT64
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int64_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(24, batch.Length);
        var field = batch.Schema.GetFieldByName("value");
        var decType = Assert.IsType<Decimal64Type>(field.DataType);
        Assert.Equal(10, decType.Precision);
        Assert.Equal(2, decType.Scale);

        var array = (Decimal64Array)batch.Column("value");
        Assert.Equal(24, array.Length);

        for (int i = 0; i < 24; i++)
        {
            Assert.False(array.IsNull(i), $"Row {i} should not be null");
        }
    }

    [Fact]
    public async Task FixedLengthDecimal_ReadsAsDecimal128()
    {
        // fixed_length_decimal.parquet: 24 rows, precision=25, scale=2, physical FLBA(11)
        await using var file = new LocalRandomAccessFile(TestData.GetPath("fixed_length_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.Equal(24, batch.Length);
        var field = batch.Schema.GetFieldByName("value");
        var decType = Assert.IsType<Decimal128Type>(field.DataType);
        Assert.Equal(25, decType.Precision);
        Assert.Equal(2, decType.Scale);

        var array = (Decimal128Array)batch.Column("value");
        Assert.Equal(24, array.Length);

        for (int i = 0; i < 24; i++)
        {
            Assert.False(array.IsNull(i), $"Row {i} should not be null");
        }
    }

    [Fact]
    public async Task FixedLengthDecimalLegacy_ReadsAsDecimal128()
    {
        // fixed_length_decimal_legacy.parquet: legacy FLBA decimal
        await using var file = new LocalRandomAccessFile(TestData.GetPath("fixed_length_decimal_legacy.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.True(batch.Length > 0);
        var field = batch.Schema.GetFieldByName("value");
        Assert.True(
            field.DataType is Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type,
            $"Expected a decimal type but got {field.DataType.Name}");
    }

    [Fact]
    public async Task ByteArrayDecimal_ReadsAsDecimalType()
    {
        // byte_array_decimal.parquet: variable-length byte array decimal
        await using var file = new LocalRandomAccessFile(TestData.GetPath("byte_array_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);

        Assert.True(batch.Length > 0);
        var field = batch.Schema.GetFieldByName("value");
        Assert.True(
            field.DataType is Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type,
            $"Expected a decimal type but got {field.DataType.Name}");

        // Verify no exceptions and non-null values
        var array = batch.Column("value");
        for (int i = 0; i < array.Length; i++)
        {
            Assert.False(array.IsNull(i), $"Row {i} should not be null");
        }
    }

    [Fact]
    public async Task Int32Decimal_ValuesAreCorrect()
    {
        // int32_decimal.parquet: values 1.00..24.00 stored as int32 with scale=2
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int32_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        var array = (Decimal32Array)batch.Column("value");

        for (int i = 0; i < 24; i++)
        {
            decimal? val = array.GetValue(i);
            Assert.NotNull(val);
            Assert.Equal((i + 1) * 1.00m, val.Value);
        }
    }

    [Fact]
    public async Task Int64Decimal_ValuesAreCorrect()
    {
        // int64_decimal.parquet: values 1.00..24.00 stored as int64 with scale=2
        await using var file = new LocalRandomAccessFile(TestData.GetPath("int64_decimal.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);

        var batch = await reader.ReadRowGroupAsync(0);
        var array = (Decimal64Array)batch.Column("value");

        for (int i = 0; i < 24; i++)
        {
            decimal? val = array.GetValue(i);
            Assert.NotNull(val);
            Assert.Equal((i + 1) * 1.00m, val.Value);
        }
    }
}
