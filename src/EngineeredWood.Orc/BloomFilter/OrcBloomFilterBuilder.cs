// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.Orc.Proto;
using Google.Protobuf;

namespace EngineeredWood.Orc.BloomFilter;

/// <summary>
/// Builds an ORC bloom filter by inserting values. Supports both the C++ and Java
/// hash function variants. Produces a <see cref="Proto.BloomFilter"/> protobuf message
/// with the <c>utf8bitset</c> field (bloom_encoding=1).
/// </summary>
internal sealed class OrcBloomFilterBuilder
{
    private readonly long[] _bitset;
    private readonly int _numHashFunctions;
    private readonly int _numBits;
    private readonly OrcWriterKind _writerKind;

    /// <summary>
    /// Creates a bloom filter builder sized for the expected number of entries.
    /// </summary>
    /// <param name="expectedEntries">Expected number of distinct values (e.g., RowIndexStride).</param>
    /// <param name="fpp">Target false positive probability.</param>
    /// <param name="writerKind">Which hash variant to use.</param>
    public OrcBloomFilterBuilder(int expectedEntries, double fpp, OrcWriterKind writerKind)
    {
        double ln2 = Math.Log(2);
        long numBits = (long)Math.Ceiling(-expectedEntries * Math.Log(fpp) / (ln2 * ln2));
        numBits = Math.Max(64, numBits);
        int numLongs = (int)((numBits + 63) / 64);

        _bitset = new long[numLongs];
        _numBits = numLongs * 64;
        _numHashFunctions = Math.Max(1, (int)Math.Round((double)_numBits / expectedEntries * ln2));
        _writerKind = writerKind;
    }

    /// <summary>Number of hash functions used by this bloom filter.</summary>
    public int NumHashFunctions => _numHashFunctions;

    /// <summary>
    /// Inserts a byte value (string, binary, decimal) into the bloom filter.
    /// </summary>
    public void AddBytes(ReadOnlySpan<byte> data)
    {
        long hash64 = _writerKind == OrcWriterKind.Cpp
            ? Murmur3.Hash64Cpp(data)
            : Murmur3.Hash64Java(data);
        InsertHash(hash64);
    }

    /// <summary>
    /// Inserts a long value (int, long, date, timestamp, boolean, byte, short) into the bloom filter.
    /// </summary>
    public void AddLong(long value)
    {
        long hash64 = _writerKind == OrcWriterKind.Cpp
            ? Murmur3.GetLongHashCpp(value)
            : Murmur3.GetLongHashJava(value);
        InsertHash(hash64);
    }

    /// <summary>
    /// Inserts a double value (float, double) into the bloom filter.
    /// </summary>
    public void AddDouble(double value)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);
        long hash64 = _writerKind == OrcWriterKind.Cpp
            ? Murmur3.GetLongHashCpp(bits)
            : Murmur3.GetLongHashJava(bits);
        InsertHash(hash64);
    }

    /// <summary>
    /// Serializes the current state as a protobuf <see cref="Proto.BloomFilter"/>
    /// with the <c>utf8bitset</c> field (little-endian byte encoding).
    /// </summary>
    public Proto.BloomFilter ToProto()
    {
        var bf = new Proto.BloomFilter
        {
            NumHashFunctions = (uint)_numHashFunctions,
        };

        // Encode bitset as little-endian bytes in utf8bitset field.
        var bytes = new byte[_bitset.Length * 8];
        for (int i = 0; i < _bitset.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 8), _bitset[i]);
        bf.Utf8Bitset = ByteString.CopyFrom(bytes);

        return bf;
    }

    /// <summary>
    /// Resets the bitset for the next row group while keeping the same sizing.
    /// </summary>
    public void Reset()
    {
        Array.Clear(_bitset, 0, _bitset.Length);
    }

    private void InsertHash(long hash64)
    {
        int hash1 = (int)hash64;
        int hash2 = (int)((ulong)hash64 >> 32);

        for (int i = 1; i <= _numHashFunctions; i++)
        {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0)
                combinedHash = ~combinedHash;
            int pos = combinedHash % _numBits;
            _bitset[pos >> 6] |= (1L << pos);
        }
    }
}
