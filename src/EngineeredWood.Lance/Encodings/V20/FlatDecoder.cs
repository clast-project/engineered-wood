// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="Flat"/> v2.0 ArrayEncoding: a fixed number of bits
/// per value stored contiguously in a single buffer.
///
/// <para>Handles every common primitive width: <c>1</c> (bool bitmap), <c>8</c>,
/// <c>16</c>, <c>32</c>, <c>64</c>, <c>128</c> (decimal128), <c>256</c>
/// (decimal256). bits_per_value is allowed to be non-byte-aligned by the
/// spec, but Phase 2 only handles <c>1</c> and multiples of 8.</para>
///
/// <para>Compression is rejected: Phase 2 only reads uncompressed data.</para>
/// </summary>
internal sealed class FlatDecoder : IArrayDecoder
{
    private readonly Flat _encoding;

    public FlatDecoder(Flat encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        if (_encoding.Compression is not null && !string.IsNullOrEmpty(_encoding.Compression.Scheme))
            throw new NotImplementedException(
                $"Flat encoding with compression='{_encoding.Compression.Scheme}' is not supported yet.");

        if (_encoding.Buffer is null)
            throw new LanceFormatException("Flat encoding has no buffer reference.");

        ReadOnlyMemory<byte> buffer = context.Resolve(_encoding.Buffer);
        return DecodeBuffer(_encoding.BitsPerValue, numRows, targetType, buffer.Span);
    }

    /// <summary>
    /// Raw buffer shape access for composite decoders (e.g., <see cref="BinaryDecoder"/>)
    /// that want to consume the flat payload themselves rather than build an Arrow array.
    /// </summary>
    public static ReadOnlyMemory<byte> ResolveFlatBuffer(
        ArrayEncoding encoding, in PageContext context, out ulong bitsPerValue)
    {
        // Accept a bare Flat or a Nullable(NoNull(Flat)) wrapper. Used by Binary
        // which embeds untyped buffers via nested ArrayEncodings.
        while (true)
        {
            switch (encoding.ArrayEncodingCase)
            {
                case ArrayEncoding.ArrayEncodingOneofCase.Flat:
                    if (encoding.Flat.Buffer is null)
                        throw new LanceFormatException("Flat encoding has no buffer reference.");
                    bitsPerValue = encoding.Flat.BitsPerValue;
                    return context.Resolve(encoding.Flat.Buffer);

                case ArrayEncoding.ArrayEncodingOneofCase.Nullable
                    when encoding.Nullable.NullabilityCase
                        == Proto.Encodings.V20.Nullable.NullabilityOneofCase.NoNulls:
                    encoding = encoding.Nullable.NoNulls.Values;
                    continue;

                default:
                    throw new NotImplementedException(
                        $"Expected a Flat or Nullable(NoNull(Flat)) encoding inside a composite, got '{encoding.ArrayEncodingCase}'.");
            }
        }
    }

    private static IArrowArray DecodeBuffer(
        ulong bitsPerValue, long numRows, IArrowType targetType, ReadOnlySpan<byte> buffer)
    {
        if (numRows > int.MaxValue)
            throw new NotImplementedException(
                $"Pages with more than {int.MaxValue} rows are not supported in Phase 2.");
        int length = (int)numRows;

        // Bool: bits_per_value = 1 → bitmap.
        if (bitsPerValue == 1)
        {
            if (targetType is not BooleanType)
                throw new LanceFormatException(
                    $"Flat(bits_per_value=1) used with non-bool target type {targetType}.");
            int bytesNeeded = (length + 7) / 8;
            if (buffer.Length < bytesNeeded)
                throw new LanceFormatException(
                    $"Flat buffer too small for {length} bool values: got {buffer.Length} bytes, need {bytesNeeded}.");
            var valueBuffer = new ArrowBuffer(buffer.Slice(0, bytesNeeded).ToArray());
            var data = new ArrayData(BooleanType.Default, length, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
            return new BooleanArray(data);
        }

        // Byte-aligned widths.
        if (bitsPerValue % 8 != 0)
            throw new NotImplementedException(
                $"Flat(bits_per_value={bitsPerValue}) is not byte-aligned; bitpacking is a later phase.");

        int bytesPerValue = checked((int)(bitsPerValue / 8));
        int totalBytes = checked(bytesPerValue * length);
        if (buffer.Length < totalBytes)
            throw new LanceFormatException(
                $"Flat buffer too small: got {buffer.Length} bytes, need {totalBytes} ({length} × {bytesPerValue}).");

        var valuesBuffer = new ArrowBuffer(buffer.Slice(0, totalBytes).ToArray());
        var arrayData = new ArrayData(
            targetType, length, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, valuesBuffer });
        return ArrowArrayFactory.BuildArray(arrayData);
    }
}
