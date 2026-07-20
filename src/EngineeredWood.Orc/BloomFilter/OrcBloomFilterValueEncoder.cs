// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using OrcTypeKind = EngineeredWood.Orc.Proto.Type.Types.Kind;

namespace EngineeredWood.Orc.BloomFilter;

/// <summary>
/// Determines the hashing strategy for ORC bloom filter probing based on column type.
/// ORC uses Thomas Wang's hash for numeric types and murmur3 for byte data.
/// </summary>
internal enum OrcBloomHashKind
{
    /// <summary>Hash as a long using Thomas Wang's hash (integers, dates, timestamps).</summary>
    Long,

    /// <summary>Hash as double bits using Thomas Wang's hash (float, double).</summary>
    Double,

    /// <summary>Hash as bytes using murmur3 (strings, binary).</summary>
    Bytes,
}

/// <summary>
/// Converts .NET values to the appropriate form for ORC bloom filter probing,
/// and determines which hash function to use.
/// </summary>
internal static class OrcBloomFilterValueEncoder
{
    /// <summary>
    /// Determines the hash kind for a given ORC type.
    /// </summary>
    public static OrcBloomHashKind GetHashKind(OrcTypeKind kind)
    {
        return kind switch
        {
            OrcTypeKind.Boolean or OrcTypeKind.Byte or OrcTypeKind.Short
                or OrcTypeKind.Int or OrcTypeKind.Long
                or OrcTypeKind.Date or OrcTypeKind.Timestamp or OrcTypeKind.TimestampInstant
                => OrcBloomHashKind.Long,
            OrcTypeKind.Float or OrcTypeKind.Double
                => OrcBloomHashKind.Double,
            OrcTypeKind.String or OrcTypeKind.Varchar or OrcTypeKind.Char
                or OrcTypeKind.Binary or OrcTypeKind.Decimal
                => OrcBloomHashKind.Bytes,
            _ => throw new ArgumentException($"Unsupported ORC type kind for bloom filter: {kind}", nameof(kind)),
        };
    }

    /// <summary>
    /// Converts a value to a long for Thomas Wang hashing.
    /// </summary>
    public static long ToLong(object value)
    {
        return value switch
        {
            long v => v,
            int v => v,
            short v => v,
            sbyte v => v,
            byte v => v,
            ushort v => v,
            uint v => v,
            bool b => b ? 1L : 0L,
            _ => throw new ArgumentException(
                $"Cannot convert value of type {value.GetType().Name} to long for bloom filter.", nameof(value)),
        };
    }

    /// <summary>
    /// Converts a value to a double for Thomas Wang hashing (via DoubleToInt64Bits).
    /// </summary>
    public static double ToDouble(object value)
    {
        return value switch
        {
            double v => v,
            float v => v,
            _ => throw new ArgumentException(
                $"Cannot convert value of type {value.GetType().Name} to double for bloom filter.", nameof(value)),
        };
    }

    /// <summary>
    /// Converts a value to bytes for murmur3 hashing.
    /// </summary>
    public static byte[] ToBytes(object value)
    {
        return value switch
        {
            string s => System.Text.Encoding.UTF8.GetBytes(s),
            byte[] b => b,
            _ => throw new ArgumentException(
                $"Cannot convert value of type {value.GetType().Name} to bytes for bloom filter.", nameof(value)),
        };
    }
}
