// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>Helpers for the hand-rolled protobuf metadata parsers.</summary>
internal static class ProtobufWire
{
    /// <summary>
    /// Advances <paramref name="pos"/> past the value of a field with wire type
    /// <paramref name="wireType"/> whose tag has already been read.
    /// </summary>
    /// <remarks>
    /// A length-delimited field's length is read into a local before advancing: in
    /// <c>pos += ReadUnsigned(bytes, ref pos)</c> the left-hand <c>pos</c> is read before the call
    /// moves it past the length varint, so that form skips from the wrong place.
    /// </remarks>
    public static void SkipField(ReadOnlySpan<byte> bytes, ref int pos, ulong wireType, string context)
    {
        switch (wireType)
        {
            case 0:
                Varint.ReadUnsigned(bytes, ref pos);
                return;
            case 1:
                Advance(bytes, ref pos, 8, context);
                return;
            case 2:
                {
                    long length = Varint.ReadUnsigned(bytes, ref pos);
                    Advance(bytes, ref pos, length, context);
                    return;
                }
            case 5:
                Advance(bytes, ref pos, 4, context);
                return;
            default:
                throw new VortexFormatException($"Unsupported protobuf wire type {wireType} in {context}.");
        }
    }

    /// <summary>
    /// Moves <paramref name="pos"/> forward <paramref name="count"/> bytes, which must be within
    /// the message. Compared against the bytes remaining rather than added first, so a huge or
    /// negative count can't wrap <paramref name="pos"/> past the check.
    /// </summary>
    private static void Advance(ReadOnlySpan<byte> bytes, ref int pos, long count, string context)
    {
        if (count < 0 || count > bytes.Length - pos)
            throw new VortexFormatException(
                $"A protobuf field of {count} bytes at offset {pos} in {context} runs past the end of its {bytes.Length}-byte message.");
        pos += (int)count;
    }
}
