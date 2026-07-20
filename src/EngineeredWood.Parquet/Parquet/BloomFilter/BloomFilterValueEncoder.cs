// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Converts .NET values to the Parquet plain-encoding byte representation
/// used for bloom filter hashing.
/// </summary>
internal static class BloomFilterValueEncoder
{
    /// <summary>
    /// Encodes a typed value into Parquet plain-encoding bytes suitable for bloom filter hashing.
    /// </summary>
    /// <param name="value">The value to encode.</param>
    /// <param name="physicalType">The Parquet physical type of the target column.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="ArgumentException">The value type is incompatible with the column's physical type.</exception>
    public static byte[] Encode(object value, PhysicalType physicalType)
    {
        return physicalType switch
        {
            PhysicalType.Boolean => EncodeBoolean(value),
            PhysicalType.Int32 => EncodeInt32(value),
            PhysicalType.Int64 => EncodeInt64(value),
            PhysicalType.Float => EncodeFloat(value),
            PhysicalType.Double => EncodeDouble(value),
            PhysicalType.ByteArray => EncodeByteArray(value),
            PhysicalType.FixedLenByteArray => EncodeByteArray(value),
            PhysicalType.Int96 => EncodeInt96(value),
            _ => throw new ArgumentException($"Unsupported physical type: {physicalType}", nameof(physicalType)),
        };
    }

    private static byte[] EncodeBoolean(object value)
    {
        if (value is bool b) return [b ? (byte)1 : (byte)0];
        throw new ArgumentException(
            $"Cannot encode value of type {value.GetType().Name} as Boolean.", nameof(value));
    }

    private static byte[] EncodeInt32(object value)
    {
        int i = value switch
        {
            int v => v,
            short v => v,
            sbyte v => v,
            ushort v => v,
            byte v => v,
            _ => throw new ArgumentException(
                $"Cannot encode value of type {value.GetType().Name} as Int32.", nameof(value)),
        };
        var buf = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buf, i);
        return buf;
    }

    private static byte[] EncodeInt64(object value)
    {
        long l = value switch
        {
            long v => v,
            int v => v,
            short v => v,
            sbyte v => v,
            uint v => v,
            ushort v => v,
            byte v => v,
            _ => throw new ArgumentException(
                $"Cannot encode value of type {value.GetType().Name} as Int64.", nameof(value)),
        };
        var buf = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buf, l);
        return buf;
    }

    private static byte[] EncodeFloat(object value)
    {
        float f = value switch
        {
            float v => v,
            _ => throw new ArgumentException(
                $"Cannot encode value of type {value.GetType().Name} as Float.", nameof(value)),
        };
        var buf = new byte[4];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteSingleLittleEndian(buf, f);
#else
        BitConverter.GetBytes(f).CopyTo(buf, 0);
#endif
        return buf;
    }

    private static byte[] EncodeDouble(object value)
    {
        double d = value switch
        {
            double v => v,
            float v => v,
            _ => throw new ArgumentException(
                $"Cannot encode value of type {value.GetType().Name} as Double.", nameof(value)),
        };
        var buf = new byte[8];
#if NET8_0_OR_GREATER
        BinaryPrimitives.WriteDoubleLittleEndian(buf, d);
#else
        BitConverter.GetBytes(d).CopyTo(buf, 0);
#endif
        return buf;
    }

    private static byte[] EncodeByteArray(object value)
    {
        return value switch
        {
            byte[] b => b,
            string s => System.Text.Encoding.UTF8.GetBytes(s),
            _ => throw new ArgumentException(
                $"Cannot encode value of type {value.GetType().Name} as ByteArray.", nameof(value)),
        };
    }

    private static byte[] EncodeInt96(object value)
    {
        if (value is byte[] b && b.Length == 12) return b;
        throw new ArgumentException(
            $"Cannot encode value of type {value.GetType().Name} as Int96. Expected a 12-byte array.",
            nameof(value));
    }
}
