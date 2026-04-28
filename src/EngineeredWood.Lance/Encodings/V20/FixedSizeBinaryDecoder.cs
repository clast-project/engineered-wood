// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="FixedSizeBinary"/> v2.0 ArrayEncoding. Delegates to
/// an inner bytes decoder and reinterprets the payload as a fixed-width
/// binary buffer.
///
/// <para>Note: pylance 4.x emits <see cref="Flat"/> with
/// <c>bits_per_value = byte_width * 8</c> for fixed-size binary columns
/// instead of this encoding. It is implemented here for spec completeness.</para>
/// </summary>
internal sealed class FixedSizeBinaryDecoder : IArrayDecoder
{
    private readonly FixedSizeBinary _encoding;

    public FixedSizeBinaryDecoder(FixedSizeBinary encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        if (targetType is not FixedSizeBinaryType fsb)
            throw new LanceFormatException(
                $"FixedSizeBinary encoding used with non-fsb target type {targetType}.");
        if (fsb.ByteWidth != (int)_encoding.ByteWidth)
            throw new LanceFormatException(
                $"FixedSizeBinary byte_width mismatch: schema has {fsb.ByteWidth}, encoding has {_encoding.ByteWidth}.");

        if (_encoding.Bytes is null)
            throw new LanceFormatException("FixedSizeBinary encoding is missing the inner bytes encoding.");

        // The inner encoding produces a flat byte buffer of (num_rows × byte_width) bytes.
        ReadOnlyMemory<byte> bytes = FlatDecoder.ResolveFlatBuffer(_encoding.Bytes, context, out _);
        int expected = checked((int)numRows * fsb.ByteWidth);
        if (bytes.Length < expected)
            throw new LanceFormatException(
                $"FixedSizeBinary bytes buffer too small: got {bytes.Length}, need {expected}.");

        var valueBuffer = new ArrowBuffer(bytes.Slice(0, expected).ToArray());
        var data = new ArrayData(
            targetType, checked((int)numRows),
            nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, valueBuffer });
        return ArrowArrayFactory.BuildArray(data);
    }
}
