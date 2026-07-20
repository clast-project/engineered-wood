// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Clast.BloomFilter;
using EngineeredWood.Parquet.Thrift;

namespace EngineeredWood.Parquet.BloomFilter;

/// <summary>
/// Reads a Parquet bloom filter block (Thrift header + raw bitset) from a byte span.
/// </summary>
internal static class BloomFilterReader
{
    /// <summary>
    /// Parses a bloom filter block and returns the SBBF instance.
    /// </summary>
    /// <param name="data">
    /// The raw bloom filter block bytes starting at the bloom filter offset.
    /// Contains a Thrift-encoded <c>BloomFilterHeader</c> followed by the raw bitset.
    /// </param>
    /// <returns>A <see cref="SplitBlockBloomFilter"/> ready for probing.</returns>
    public static SplitBlockBloomFilter Parse(ReadOnlySpan<byte> data)
    {
        var reader = new ThriftCompactReader(data);
        int numBytes = 0;
        bool hasSplitBlock = false;
        bool hasXxHash = false;
        bool isUncompressed = false;

        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            switch (fid)
            {
                case 1: // numBytes: i32
                    numBytes = checked((int)reader.ReadZigZagInt32());
                    break;
                case 2: // algorithm: BloomFilterAlgorithm (union as struct)
                    hasSplitBlock = ReadUnionTag(ref reader) == 1; // field 1 = BLOCK
                    break;
                case 3: // hash: BloomFilterHash (union as struct)
                    hasXxHash = ReadUnionTag(ref reader) == 1; // field 1 = XXHASH
                    break;
                case 4: // compression: BloomFilterCompression (union as struct)
                    isUncompressed = ReadUnionTag(ref reader) == 1; // field 1 = UNCOMPRESSED
                    break;
                default:
                    reader.Skip(type);
                    break;
            }
        }

        if (numBytes <= 0)
            throw new ParquetFormatException("Bloom filter header missing or invalid numBytes.");

        if (!hasSplitBlock)
            throw new ParquetFormatException("Unsupported bloom filter algorithm (expected SPLIT_BLOCK).");

        if (!hasXxHash)
            throw new ParquetFormatException("Unsupported bloom filter hash (expected XXHASH).");

        if (!isUncompressed)
            throw new ParquetFormatException("Unsupported bloom filter compression (expected UNCOMPRESSED).");

        // The bitset immediately follows the Thrift header.
        var bitset = data.Slice(reader.Position, numBytes).ToArray();
        return new SplitBlockBloomFilter(bitset);
    }

    /// <summary>
    /// Reads a Thrift union encoded as a struct and returns the field ID of the set variant.
    /// Each variant is an empty struct, so this reads: field header → empty struct (stop) → stop.
    /// </summary>
    private static int ReadUnionTag(ref ThriftCompactReader reader)
    {
        reader.PushStruct();

        int tag = -1;
        while (true)
        {
            var (type, fid) = reader.ReadFieldHeader();
            if (type == ThriftType.Stop) break;

            if (type == ThriftType.Struct)
            {
                tag = fid;
                // Read the empty struct's stop byte.
                reader.PushStruct();
                var (innerType, _) = reader.ReadFieldHeader();
                if (innerType != ThriftType.Stop)
                    reader.Skip(innerType);
                reader.PopStruct();
            }
            else
            {
                reader.Skip(type);
            }
        }

        reader.PopStruct();
        return tag;
    }
}
