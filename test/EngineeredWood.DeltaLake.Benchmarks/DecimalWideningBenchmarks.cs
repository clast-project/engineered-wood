// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using EngineeredWood.DeltaLake.Table.TypeWidening;

namespace EngineeredWood.DeltaLake.Benchmarks;

/// <summary>
/// Widening a column read from a file written before a decimal type widening (#503) — runs once per such
/// column per batch of every scan that reaches an old file.
///
/// <para>The <c>BigInteger</c> methods replicate what <c>ValueWidener</c> did before the <c>Int128</c> rewrite:
/// a <see cref="BigInteger"/> per row, and the validity bitmap rebuilt bit by bit. The <c>Int128</c> methods
/// call the current <see cref="ValueWidener.WidenArray"/>. One in ten rows is null.</para>
/// </summary>
[MemoryDiagnoser]
public class DecimalWideningBenchmarks
{
    [Params(65_536, 1_048_576)]
    public int Length { get; set; }

    private static readonly Decimal128Type Decimal18_2 = new(18, 2);
    private static readonly Decimal128Type Decimal22_4 = new(22, 4);
    private static readonly Decimal128Type Decimal12_2 = new(12, 2);

    private Decimal128Array _decimals = null!;
    private Int32Array _ints = null!;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(503);
        var decimals = new Decimal128Array.Builder(Decimal18_2);
        var ints = new Int32Array.Builder();
        for (int i = 0; i < Length; i++)
        {
            if (i % 10 == 3)
            {
                decimals.AppendNull();
                ints.AppendNull();
                continue;
            }
            // Any 18-digit value at scale 2.
            long unscaled = (long)(rng.NextDouble() * 2e17) - (long)1e17;
            decimals.Append(unscaled / 100m);
            ints.Append(rng.Next(int.MinValue, int.MaxValue));
        }
        _decimals = decimals.Build();
        _ints = ints.Build();
    }

    [Benchmark(Baseline = true, Description = "decimal(18,2)->(22,4) via BigInteger")]
    public IArrowArray DecimalRescaleBigInteger() => LegacyWidenDecimal128(_decimals, Decimal22_4);

    [Benchmark(Description = "decimal(18,2)->(22,4) via Int128")]
    public IArrowArray DecimalRescaleInt128() => ValueWidener.WidenArray(_decimals, Decimal22_4);

    [Benchmark(Description = "int->decimal(12,2) via BigInteger")]
    public IArrowArray IntToDecimalBigInteger() => LegacyWidenIntToDecimal(_ints, Decimal12_2);

    [Benchmark(Description = "int->decimal(12,2) via Int128")]
    public IArrowArray IntToDecimalInt128() => ValueWidener.WidenArray(_ints, Decimal12_2);

    #region The BigInteger implementation, as it was before #503

    private static Decimal128Array LegacyWidenDecimal128(Decimal128Array source, Decimal128Type targetType)
    {
        var sourceType = (Decimal128Type)source.Data.DataType;
        var multiplier = BigInteger.Pow(10, targetType.Scale - sourceType.Scale);
        var resultBytes = new byte[source.Length * 16];
        var nullBitmap = new ArrowBuffer.BitmapBuilder();
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i))
            {
                nullBitmap.Append(false);
            }
            else
            {
                nullBitmap.Append(true);
                var value = ReadInt128LE(source.GetBytes(i));
                WriteInt128LE(value * multiplier, resultBytes.AsSpan(i * 16, 16));
            }
        }
        return new Decimal128Array(new ArrayData(targetType, source.Length, source.NullCount, 0,
            [nullBitmap.Build(), new ArrowBuffer(resultBytes)]));
    }

    private static Decimal128Array LegacyWidenIntToDecimal(Int32Array source, Decimal128Type targetType)
    {
        var scaleFactor = BigInteger.Pow(10, targetType.Scale);
        var resultBytes = new byte[source.Length * 16];
        var nullBitmap = new ArrowBuffer.BitmapBuilder();
        Func<int, long> toLong = v => v;
        for (int i = 0; i < source.Length; i++)
        {
            if (source.IsNull(i))
            {
                nullBitmap.Append(false);
            }
            else
            {
                nullBitmap.Append(true);
                var scaled = new BigInteger(toLong(source.GetValue(i)!.Value)) * scaleFactor;
                WriteInt128LE(scaled, resultBytes.AsSpan(i * 16, 16));
            }
        }
        return new Decimal128Array(new ArrayData(targetType, source.Length, source.NullCount, 0,
            [nullBitmap.Build(), new ArrowBuffer(resultBytes)]));
    }

    private static BigInteger ReadInt128LE(ReadOnlySpan<byte> bytes)
    {
#if NET6_0_OR_GREATER
        return new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
#else
        return new BigInteger(bytes.ToArray());
#endif
    }

    private static void WriteInt128LE(BigInteger value, Span<byte> dest)
    {
        dest.Fill(value < 0 ? (byte)0xFF : (byte)0x00);
#if NET6_0_OR_GREATER
        value.TryWriteBytes(dest, out _, isUnsigned: false, isBigEndian: false);
#else
        var bytes = value.ToByteArray();
        bytes.AsSpan(0, Math.Min(bytes.Length, dest.Length)).CopyTo(dest);
#endif
    }

    #endregion
}
