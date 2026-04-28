// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="Constant"/> v2.0 ArrayEncoding: every value is the
/// same literal. The literal is stored as raw little-endian bytes in
/// <see cref="Constant.Value"/> and must match the fixed byte width of the
/// target Arrow type. pylance 4.x does not currently emit this encoding for
/// simple types, but the spec defines it and round-tripping it is trivial.
/// </summary>
internal sealed class ConstantDecoder : IArrayDecoder
{
    private readonly Constant _encoding;

    public ConstantDecoder(Constant encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        int length = checked((int)numRows);
        var scalar = _encoding.Value.Span;

        if (targetType is BooleanType)
        {
            if (scalar.Length != 1)
                throw new LanceFormatException(
                    $"Constant for Boolean expects a 1-byte value, got {scalar.Length}.");
            bool bit = scalar[0] != 0;
            int bytes = (length + 7) / 8;
            var buf = new byte[bytes];
            if (bit)
            {
                // Fill length bits with 1.
                for (int i = 0; i < length; i++) buf[i >> 3] |= (byte)(1 << (i & 7));
            }
            return new BooleanArray(new ArrayData(
                BooleanType.Default, length, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, new ArrowBuffer(buf) }));
        }

        if (targetType is FixedWidthType fw && targetType is not BooleanType)
        {
            int bytesPerValue = fw.BitWidth / 8;
            if (bytesPerValue == 0)
                throw new NotImplementedException($"Constant decoding for {targetType} is not supported yet.");
            if (scalar.Length != bytesPerValue)
                throw new LanceFormatException(
                    $"Constant value size ({scalar.Length}) does not match {targetType} width ({bytesPerValue}).");

            var dest = new byte[bytesPerValue * length];
            for (int i = 0; i < length; i++)
                scalar.CopyTo(dest.AsSpan(i * bytesPerValue, bytesPerValue));

            var data = new ArrayData(
                targetType, length, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, new ArrowBuffer(dest) });
            return ArrowArrayFactory.BuildArray(data);
        }

        throw new NotImplementedException(
            $"Constant decoding for Arrow type {targetType} is not supported yet.");
    }
}
