// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Serializes a Parquet bloom filter block (Thrift header + raw bitset).
/// Inverse of <see cref="BloomFilterReader"/>.
/// </summary>
internal static class BloomFilterSerializer
{
    /// <summary>
    /// Serializes a bloom filter bitset into a complete bloom filter block
    /// with Thrift-encoded header.
    /// </summary>
    /// <param name="bitset">The raw bloom filter bitset bytes (must be a multiple of 32).</param>
    /// <returns>Complete bloom filter block: [Thrift header][bitset].</returns>
    public static byte[] Serialize(byte[] bitset)
    {
        // Estimate header size: numBytes(~3) + 3 union structs (~9) + stop(1) ≈ 16 bytes max.
        var writer = new ThriftCompactWriter(32);

        // Field 1: numBytes (i32)
        writer.WriteFieldHeader(ThriftType.I32, (short)1);
        writer.WriteZigZagInt32(bitset.Length);

        // Field 2: algorithm = SplitBlockAlgorithm (union as struct, field 1 = empty struct)
        writer.WriteFieldHeader(ThriftType.Struct, (short)2);
        WriteEmptyUnionVariant(ref writer);

        // Field 3: hash = XxHash (union as struct, field 1 = empty struct)
        writer.WriteFieldHeader(ThriftType.Struct, (short)3);
        WriteEmptyUnionVariant(ref writer);

        // Field 4: compression = Uncompressed (union as struct, field 1 = empty struct)
        writer.WriteFieldHeader(ThriftType.Struct, (short)4);
        WriteEmptyUnionVariant(ref writer);

        writer.WriteStructStop();

        int headerLength = writer.Length;

        // Combine header + bitset.
        var result = new byte[headerLength + bitset.Length];
        writer.WrittenSpan.CopyTo(result);
        bitset.CopyTo(result.AsSpan(headerLength));

        return result;
    }

    /// <summary>
    /// Writes a Thrift union variant as an outer struct containing field 1 = empty struct.
    /// Produces: [field header for field 1] [stop (inner struct)] [stop (outer struct)]
    /// </summary>
    private static void WriteEmptyUnionVariant(ref ThriftCompactWriter writer)
    {
        writer.PushStruct();
        writer.WriteFieldHeader(ThriftType.Struct, (short)1);
        writer.WriteStructStop(); // inner empty struct
        writer.WriteStructStop(); // outer union struct
        writer.PopStruct();
    }
}
