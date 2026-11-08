// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro.Tests;

/// <summary>
/// Builds an object container file by hand. The writer re-serializes the parsed schema, so a file it
/// writes never carries an annotation the parser drops, and it cannot write a schema the Arrow side
/// cannot express. This keeps the <c>avro.schema</c> JSON exactly as given.
/// </summary>
internal static class RawOcfFile
{
    /// <summary>
    /// A null-codec object container file holding one block of records. Each value is written as a
    /// length-prefixed <c>bytes</c>, or with <paramref name="lengthPrefixed"/> false as given: a
    /// <c>fixed</c>, or a record encoded by hand.
    /// </summary>
    public static MemoryStream Create(string schemaJson, byte[][] values, bool lengthPrefixed = true)
    {
        var ms = new MemoryStream();
        ms.Write([(byte)'O', (byte)'b', (byte)'j', 1], 0, 4);
        WriteLong(ms, 1);
        WriteBytes(ms, "avro.schema"u8.ToArray());
        WriteBytes(ms, System.Text.Encoding.UTF8.GetBytes(schemaJson));
        WriteLong(ms, 0);
        byte[] sync = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        ms.Write(sync, 0, sync.Length);

        using var block = new MemoryStream();
        foreach (var v in values)
        {
            if (lengthPrefixed)
                WriteBytes(block, v);
            else
                block.Write(v, 0, v.Length);
        }
        WriteLong(ms, values.Length);
        WriteLong(ms, block.Length);
        block.WriteTo(ms);
        ms.Write(sync, 0, sync.Length);

        ms.Position = 0;
        return ms;

        static void WriteBytes(Stream s, byte[] b)
        {
            WriteLong(s, b.Length);
            s.Write(b, 0, b.Length);
        }

        static void WriteLong(Stream s, long v)
        {
            ulong z = (ulong)((v << 1) ^ (v >> 63));
            while (z >= 0x80)
            {
                s.WriteByte((byte)(z | 0x80));
                z >>= 7;
            }
            s.WriteByte((byte)z);
        }
    }
}
