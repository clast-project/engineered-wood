// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro.Schema;

/// <summary>
/// Computes the CRC-64-AVRO (Rabin) fingerprint as defined by the Avro specification.
/// Uses the ECMA-182 polynomial 0xC96C5795D7870F42.
/// </summary>
internal static class RabinFingerprint
{
    private const ulong Polynomial = 0xC96C5795D7870F42UL;

    /// <summary>
    /// The fingerprint of the empty byte sequence, used as the initial value.
    /// </summary>
    private const ulong Empty = 0xC15D213AA4D7A795UL;

    private static readonly ulong[] Table = BuildTable();

    private static ulong[] BuildTable()
    {
        var table = new ulong[256];
        for (int i = 0; i < 256; i++)
        {
            ulong fp = (ulong)i;
            for (int j = 0; j < 8; j++)
            {
                ulong mask = (ulong)(-(long)(fp & 1)); // all 1s if bit 0 set, else all 0s
                fp = (fp >> 1) ^ (Polynomial & mask);
            }
            table[i] = fp;
        }
        return table;
    }

    /// <summary>
    /// Computes the 64-bit Rabin fingerprint of the given data.
    /// </summary>
    public static ulong Compute(ReadOnlySpan<byte> data)
    {
        ulong fingerprint = Empty;
        for (int i = 0; i < data.Length; i++)
        {
            fingerprint = (fingerprint >> 8) ^ Table[(int)(fingerprint ^ data[i]) & 0xFF];
        }
        return fingerprint;
    }
}
