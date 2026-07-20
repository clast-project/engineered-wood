// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table.TypeWidening;

namespace EngineeredWood.DeltaLake.Table.Tests;

public class DecimalWideningTests
{
    /// <summary>
    /// Helper to build a Decimal128Array with the given values.
    /// </summary>
    private static Decimal128Array BuildDecimal128Array(
        Decimal128Type type, params decimal?[] values)
    {
        int byteWidth = 16;
        var bytes = new byte[values.Length * byteWidth];
        var nullBitmap = new ArrowBuffer.BitmapBuilder();

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
            {
                nullBitmap.Append(false);
            }
            else
            {
                nullBitmap.Append(true);
                // Convert decimal to scaled BigInteger
                var scaledValue = (long)(values[i]!.Value * (decimal)Math.Pow(10, type.Scale));
                var bi = new BigInteger(scaledValue);
                var dest = bytes.AsSpan(i * byteWidth, byteWidth);
                // Sign-extend first
                dest.Fill(bi < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
                bi.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
                var biBytes = bi.ToByteArray();
                biBytes.AsSpan(0, Math.Min(biBytes.Length, byteWidth)).CopyTo(dest);
#endif
            }
        }

        int nullCount = values.Count(v => v is null);
        var data = new ArrayData(type, values.Length,
            nullCount, 0,
            [nullBitmap.Build(), new ArrowBuffer(bytes)]);
        return new Decimal128Array(data);
    }

    /// <summary>
    /// Reads a Decimal128Array value back as a decimal for assertion.
    /// </summary>
    private static decimal? ReadDecimal128Value(Decimal128Array array, int index)
    {
        if (array.IsNull(index)) return null;
        var bytes = array.GetBytes(index);
        var type = (Decimal128Type)array.Data.DataType;
#if NET6_0_OR_GREATER
        var bi = new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#else
        var bi = new BigInteger(bytes.ToArray());
#endif
        return (decimal)bi / (decimal)Math.Pow(10, type.Scale);
    }

    [Fact]
    public void WidenDecimal128_ScaleIncrease()
    {
        // decimal(10,2) → decimal(10,4): multiply each value by 100
        var sourceType = new Decimal128Type(10, 2);
        var targetType = new Decimal128Type(10, 4);

        var source = BuildDecimal128Array(sourceType, 12.34m, 56.78m, null);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(3, result.Length);
        Assert.Equal(targetType.Scale, ((Decimal128Type)result.Data.DataType).Scale);

        Assert.Equal(12.34m, ReadDecimal128Value(result, 0));
        Assert.Equal(56.78m, ReadDecimal128Value(result, 1));
        Assert.True(result.IsNull(2));
    }

    [Fact]
    public void WidenDecimal128_PrecisionOnlyIncrease()
    {
        // decimal(10,2) → decimal(20,2): same binary representation
        var sourceType = new Decimal128Type(10, 2);
        var targetType = new Decimal128Type(20, 2);

        var source = BuildDecimal128Array(sourceType, 99.99m, 0.01m);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(2, result.Length);
        var resultType = (Decimal128Type)result.Data.DataType;
        Assert.Equal(20, resultType.Precision);
        Assert.Equal(2, resultType.Scale);

        Assert.Equal(99.99m, ReadDecimal128Value(result, 0));
        Assert.Equal(0.01m, ReadDecimal128Value(result, 1));
    }

    [Fact]
    public void WidenDecimal128_BothPrecisionAndScaleIncrease()
    {
        // decimal(10,2) → decimal(15,5): multiply each value by 1000
        var sourceType = new Decimal128Type(10, 2);
        var targetType = new Decimal128Type(15, 5);

        var source = BuildDecimal128Array(sourceType, 1.50m, -3.25m);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(2, result.Length);
        Assert.Equal(1.50m, ReadDecimal128Value(result, 0));
        Assert.Equal(-3.25m, ReadDecimal128Value(result, 1));
    }

    [Fact]
    public void WidenDecimal128_NegativeValues()
    {
        var sourceType = new Decimal128Type(10, 2);
        var targetType = new Decimal128Type(10, 4);

        var source = BuildDecimal128Array(sourceType, -99.99m, -0.01m);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(-99.99m, ReadDecimal128Value(result, 0));
        Assert.Equal(-0.01m, ReadDecimal128Value(result, 1));
    }

    [Fact]
    public void WidenDecimal128_Zero()
    {
        var sourceType = new Decimal128Type(10, 2);
        var targetType = new Decimal128Type(10, 4);

        var source = BuildDecimal128Array(sourceType, 0m);
        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(0m, ReadDecimal128Value(result, 0));
    }

    [Fact]
    public void WidenIntToDecimal_Int32()
    {
        var source = new Int32Array.Builder().Append(42).Append(-7).AppendNull().Build();
        var targetType = new Decimal128Type(12, 2);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(3, result.Length);
        Assert.Equal(42m, ReadDecimal128Value(result, 0));
        Assert.Equal(-7m, ReadDecimal128Value(result, 1));
        Assert.True(result.IsNull(2));
    }

    [Fact]
    public void WidenIntToDecimal_Int64()
    {
        var source = new Int64Array.Builder().Append(1000000L).Append(-1L).Build();
        var targetType = new Decimal128Type(22, 3);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(2, result.Length);
        Assert.Equal(1000000m, ReadDecimal128Value(result, 0));
        Assert.Equal(-1m, ReadDecimal128Value(result, 1));
    }

    [Fact]
    public void WidenIntToDecimal_Int8()
    {
        var source = new Int8Array.Builder().Append(127).Append(-128).Build();
        var targetType = new Decimal128Type(10, 0);

        var result = (Decimal128Array)ValueWidener.WidenArray(source, targetType);

        Assert.Equal(2, result.Length);
        Assert.Equal(127m, ReadDecimal128Value(result, 0));
        Assert.Equal(-128m, ReadDecimal128Value(result, 1));
    }

    [Fact]
    public void WidenBatch_DecimalScaleChange()
    {
        var sourceType = new Decimal128Type(10, 2);
        var targetType = new Decimal128Type(10, 4);

        var sourceSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", sourceType, true))
            .Build();
        var targetSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("amount", targetType, true))
            .Build();

        var source = BuildDecimal128Array(sourceType, 12.34m, 56.78m);
        var batch = new RecordBatch(sourceSchema, [source], 2);

        var result = ValueWidener.WidenBatch(batch, targetSchema);

        Assert.Equal(2, result.Length);
        var resultArray = (Decimal128Array)result.Column(0);
        Assert.Equal(12.34m, ReadDecimal128Value(resultArray, 0));
        Assert.Equal(56.78m, ReadDecimal128Value(resultArray, 1));
    }
}
