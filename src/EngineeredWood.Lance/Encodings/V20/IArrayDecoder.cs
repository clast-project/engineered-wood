// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Phase 2 decoder interface. Given a pre-loaded <see cref="PageContext"/>
/// and a target Arrow type, produces a decoded array of <paramref name="numRows"/>
/// values.
/// </summary>
internal interface IArrayDecoder
{
    IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context);
}

/// <summary>
/// Builds an <see cref="IArrayDecoder"/> tree from an <see cref="ArrayEncoding"/>
/// protobuf for the Lance v2.0 encoding set.
/// </summary>
internal static class V20ArrayEncodingDispatcher
{
    public static IArrayDecoder Create(ArrayEncoding encoding)
    {
        return encoding.ArrayEncodingCase switch
        {
            ArrayEncoding.ArrayEncodingOneofCase.Flat => new FlatDecoder(encoding.Flat),
            ArrayEncoding.ArrayEncodingOneofCase.Nullable => NullableDecoder.Create(encoding.Nullable),
            ArrayEncoding.ArrayEncodingOneofCase.Binary => new BinaryDecoder(encoding.Binary),
            ArrayEncoding.ArrayEncodingOneofCase.Constant => new ConstantDecoder(encoding.Constant),
            ArrayEncoding.ArrayEncodingOneofCase.FixedSizeBinary => new FixedSizeBinaryDecoder(encoding.FixedSizeBinary),
            ArrayEncoding.ArrayEncodingOneofCase.Bitpacked => new BitpackedDecoder(encoding.Bitpacked),
            ArrayEncoding.ArrayEncodingOneofCase.Dictionary => new DictionaryDecoder(encoding.Dictionary),
            ArrayEncoding.ArrayEncodingOneofCase.FixedSizeList => new FixedSizeListDecoder(encoding.FixedSizeList),

            ArrayEncoding.ArrayEncodingOneofCase.BitpackedForNonNeg =>
                new BitpackedForNonNegDecoder(encoding.BitpackedForNonNeg),

            // The following encodings appear in the v2.0 ArrayEncoding oneof
            // but have no Buffer reference in the proto — they can only be
            // decoded inside a v2.1 MiniBlockLayout that provides chunks.
            // Phase 6 lands both the layout wrapper and these decoders.
            ArrayEncoding.ArrayEncodingOneofCase.InlineBitpacking or
            ArrayEncoding.ArrayEncodingOneofCase.OutOfLineBitpacking or
            ArrayEncoding.ArrayEncodingOneofCase.Rle or
            ArrayEncoding.ArrayEncodingOneofCase.GeneralMiniBlock or
            ArrayEncoding.ArrayEncodingOneofCase.PackedStructFixedWidthMiniBlock or
            ArrayEncoding.ArrayEncodingOneofCase.ByteStreamSplit or
            ArrayEncoding.ArrayEncodingOneofCase.Block =>
                throw new NotImplementedException(
                    $"'{encoding.ArrayEncodingCase}' is a miniblock-format encoding with no standalone " +
                    "buffer reference. It will be decoded via the v2.1 MiniBlockLayout path in Phase 6."),

            ArrayEncoding.ArrayEncodingOneofCase.Fsst =>
                throw new NotImplementedException(
                    "Fsst requires porting the FSST symbol-table format used by Lance (VLDB 2020 algorithm). " +
                    "Planned as a dedicated phase alongside Fastlanes."),

            // SimpleStruct (field name: `Struct`) and List span multiple columns
            // and are handled at the LanceFileReader level, not here. PackedStruct
            // is a single-column packed layout but not yet implemented.
            ArrayEncoding.ArrayEncodingOneofCase.Struct =>
                throw new InvalidOperationException(
                    "SimpleStruct is a multi-column encoding; the reader handles it before dispatching a single page."),
            ArrayEncoding.ArrayEncodingOneofCase.List =>
                throw new InvalidOperationException(
                    "List is a multi-column encoding; the reader handles it before dispatching a single page."),
            ArrayEncoding.ArrayEncodingOneofCase.PackedStruct =>
                throw new NotImplementedException(
                    "PackedStruct (single-column packed-struct layout) is not yet supported."),

            _ => throw new NotImplementedException(
                $"Lance v2.0 ArrayEncoding case '{encoding.ArrayEncodingCase}' is not supported yet."),
        };
    }
}
