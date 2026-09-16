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
                break;
            case 1:
                pos += 8;
                break;
            case 2:
                {
                    int length = checked((int)Varint.ReadUnsigned(bytes, ref pos));
                    pos += length;
                    break;
                }
            case 5:
                pos += 4;
                break;
            default:
                throw new VortexFormatException($"Unsupported protobuf wire type {wireType} in {context}.");
        }
        if (pos > bytes.Length)
            throw new VortexFormatException($"A protobuf field in {context} runs past the end of its message.");
    }
}
