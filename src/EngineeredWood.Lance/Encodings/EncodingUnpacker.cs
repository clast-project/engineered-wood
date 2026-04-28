// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Proto.Encodings.V20;
using EngineeredWood.Lance.Proto.V2;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace EngineeredWood.Lance.Encodings;

/// <summary>
/// Lance v2.x wraps encoding protos in two layers:
/// <list type="number">
/// <item><c>Encoding</c> (from file2.proto) — <c>direct | indirect | none</c> oneof.</item>
/// <item><c>DirectEncoding.encoding</c> is the bytes of a <c>google.protobuf.Any</c>
///       whose type URL (e.g. <c>/lance.encodings.ArrayEncoding</c>) distinguishes
///       <see cref="ArrayEncoding"/> from <see cref="ColumnEncoding"/>.</item>
/// </list>
/// This helper peels those layers.
/// </summary>
internal static class EncodingUnpacker
{
    /// <summary>
    /// Extract the inner message bytes from an <see cref="Encoding"/>. Throws
    /// for <c>indirect</c> (DeferredEncoding) and <c>none</c>, which are not
    /// supported in Phase 2.
    /// </summary>
    public static ByteString UnwrapDirect(Encoding encoding)
    {
        return encoding.LocationCase switch
        {
            Encoding.LocationOneofCase.Direct => UnpackAny(encoding.Direct.Encoding),
            Encoding.LocationOneofCase.Indirect => throw new NotImplementedException(
                "DeferredEncoding (shared/indirect encodings) are not supported yet."),
            Encoding.LocationOneofCase.None => throw new LanceFormatException(
                "Encoding location is 'none'; the reader does not know how to decode this data."),
            _ => throw new LanceFormatException(
                $"Unknown Encoding.location case {encoding.LocationCase}."),
        };
    }

    public static ArrayEncoding UnpackArrayEncoding(Encoding encoding)
    {
        var inner = UnwrapDirect(encoding);
        try
        {
            return ArrayEncoding.Parser.ParseFrom(inner);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceFormatException(
                "Failed to parse page ArrayEncoding.", ex);
        }
    }

    public static ColumnEncoding UnpackColumnEncoding(Encoding encoding)
    {
        var inner = UnwrapDirect(encoding);
        try
        {
            return ColumnEncoding.Parser.ParseFrom(inner);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceFormatException(
                "Failed to parse column ColumnEncoding.", ex);
        }
    }

    public static Proto.Encodings.V21.PageLayout UnpackPageLayout(Encoding encoding)
    {
        var inner = UnwrapDirect(encoding);
        try
        {
            return Proto.Encodings.V21.PageLayout.Parser.ParseFrom(inner);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceFormatException(
                "Failed to parse page PageLayout (v2.1).", ex);
        }
    }

    private static ByteString UnpackAny(ByteString anyBytes)
    {
        // DirectEncoding.encoding is the serialized bytes of a google.protobuf.Any
        // whose .value field is the actual ArrayEncoding/ColumnEncoding message.
        try
        {
            var any = Any.Parser.ParseFrom(anyBytes);
            return any.Value;
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new LanceFormatException(
                "Failed to parse DirectEncoding body as google.protobuf.Any.", ex);
        }
    }
}
