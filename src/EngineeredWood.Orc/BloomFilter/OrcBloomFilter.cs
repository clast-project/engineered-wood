// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc.BloomFilter;

/// <summary>
/// Identifies which ORC implementation wrote the file, determining
/// which hash functions to use for bloom filter probing.
/// </summary>
internal enum OrcWriterKind
{
    /// <summary>ORC Java (Hive, Spark, Presto): murmur3_x64_128 + unsigned-shift Wang hash.</summary>
    Java,

    /// <summary>ORC C++ (PyArrow, Arrow): murmur3_64 + signed-shift Wang hash.</summary>
    Cpp,
}

/// <summary>
/// Probes an ORC bloom filter for the presence of a value.
/// </summary>
/// <remarks>
/// <para>
/// The Java and C++ ORC implementations use incompatible hash functions:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Java</b> (writer=0,2,4): <c>murmur3_x64_128</c> returning h1 for strings;
/// Thomas Wang hash with unsigned right shifts for integers.
/// </description></item>
/// <item><description>
/// <b>C++</b> (writer=1): simplified <c>murmur3_64</c> (single accumulator) for strings;
/// Thomas Wang hash with signed right shifts for integers.
/// </description></item>
/// </list>
/// <para>
/// The writer kind is determined from the <c>Footer.Writer</c> field.
/// Bloom filters that fail sanity checks (unreasonable numHashFunctions,
/// all-zero or all-ones bitset) are rejected.
/// </para>
/// </remarks>
internal sealed class OrcBloomFilter
{
    /// <summary>Maximum reasonable number of hash functions for a well-configured bloom filter.</summary>
    private const int MaxReasonableHashFunctions = 30;

    /// <summary>Fill ratio above which a bloom filter is considered saturated and useless.</summary>
    private const double MaxFillRatio = 0.95;

    private readonly long[] _bitset;
    private readonly int _numHashFunctions;
    private readonly int _numBits;
    private readonly OrcWriterKind _writerKind;

    /// <summary>
    /// Creates a bloom filter from a protobuf message, or returns null if the data
    /// is unsupported or fails sanity checks.
    /// </summary>
    /// <param name="proto">The protobuf BloomFilter message.</param>
    /// <param name="writerKind">Which ORC implementation wrote the file.</param>
    public static OrcBloomFilter? TryCreate(Proto.BloomFilter proto, OrcWriterKind writerKind)
    {
        int numHashFunctions = (int)proto.NumHashFunctions;
        long[]? bitset = null;

        if (proto.Utf8Bitset != null && !proto.Utf8Bitset.IsEmpty)
        {
            // UTF-8 variant: bitset stored as raw bytes in little-endian order
            // (per ORC spec; the C++ writer converts to LE on big-endian platforms).
            var bytes = proto.Utf8Bitset.Span;
            int count = bytes.Length / 8;
            bitset = new long[count];
            for (int i = 0; i < count; i++)
                bitset[i] = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(i * 8));
        }
        else if (proto.Bitset.Count > 1)
        {
            // Standard format: bitset in repeated fixed64 field 2.
            bitset = new long[proto.Bitset.Count];
            for (int i = 0; i < proto.Bitset.Count; i++)
                bitset[i] = unchecked((long)proto.Bitset[i]);
        }

        if (bitset == null || bitset.Length == 0)
            return null;

        // Sanity checks
        if (numHashFunctions <= 0 || numHashFunctions > MaxReasonableHashFunctions)
            return null; // e.g., old Hive format stores expectedEntries in this field

        if (IsAllZeros(bitset) || IsAllOnes(bitset))
            return null; // empty or fully saturated — useless

        if (FillRatio(bitset) > MaxFillRatio)
            return null; // nearly saturated — unreliable

        return new OrcBloomFilter(bitset, numHashFunctions, writerKind);
    }

    private OrcBloomFilter(long[] bitset, int numHashFunctions, OrcWriterKind writerKind)
    {
        _bitset = bitset;
        _numHashFunctions = numHashFunctions;
        _numBits = bitset.Length * 64;
        _writerKind = writerKind;
    }

    internal int NumHashFunctions => _numHashFunctions;
    internal int NumBits => _numBits;
    internal int BitsetLength => _bitset.Length;
    internal long GetBitsetWord(int index) => _bitset[index];

    /// <summary>
    /// Tests whether the given byte data (string/binary) might be present.
    /// </summary>
    public bool MightContain(ReadOnlySpan<byte> data)
    {
        long hash64 = _writerKind == OrcWriterKind.Cpp
            ? Murmur3.Hash64Cpp(data)
            : Murmur3.Hash64Java(data);
        return TestHash(hash64);
    }

    /// <summary>
    /// Tests whether the given long value might be present.
    /// </summary>
    public bool MightContainLong(long value)
    {
        long hash64 = _writerKind == OrcWriterKind.Cpp
            ? Murmur3.GetLongHashCpp(value)
            : Murmur3.GetLongHashJava(value);
        return TestHash(hash64);
    }

    /// <summary>
    /// Tests whether the given double value might be present.
    /// </summary>
    public bool MightContainDouble(double value)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);
        long hash64 = _writerKind == OrcWriterKind.Cpp
            ? Murmur3.GetLongHashCpp(bits)
            : Murmur3.GetLongHashJava(bits);
        return TestHash(hash64);
    }

    private bool TestHash(long hash64)
    {
        int hash1 = (int)hash64;
        int hash2 = (int)((ulong)hash64 >> 32);

        for (int i = 1; i <= _numHashFunctions; i++)
        {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0)
                combinedHash = ~combinedHash;
            int pos = combinedHash % _numBits;
            if ((_bitset[pos >> 6] & (1L << pos)) == 0)
                return false;
        }

        return true;
    }

    private static bool IsAllZeros(long[] bitset)
    {
        foreach (long word in bitset)
            if (word != 0) return false;
        return true;
    }

    private static bool IsAllOnes(long[] bitset)
    {
        foreach (long word in bitset)
            if (word != ~0L) return false;
        return true;
    }

    private static double FillRatio(long[] bitset)
    {
        long setBits = 0;
        foreach (long word in bitset)
            setBits += PopCount(unchecked((ulong)word));
        return (double)setBits / (bitset.Length * 64);
    }

    private static int PopCount(ulong value)
    {
#if NET8_0_OR_GREATER
        return System.Numerics.BitOperations.PopCount(value);
#else
        // Kernighan's bit counting
        int count = 0;
        while (value != 0)
        {
            value &= value - 1;
            count++;
        }
        return count;
#endif
    }
}
