// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Dispatches <see cref="Nullable"/> v2.0 encodings — <c>NoNull</c>,
/// <c>SomeNull</c>, or <c>AllNull</c>.
/// </summary>
internal static class NullableDecoder
{
    public static IArrayDecoder Create(Proto.Encodings.V20.Nullable encoding)
    {
        return encoding.NullabilityCase switch
        {
            Proto.Encodings.V20.Nullable.NullabilityOneofCase.NoNulls => V20ArrayEncodingDispatcher.Create(encoding.NoNulls.Values),
            Proto.Encodings.V20.Nullable.NullabilityOneofCase.SomeNulls => new SomeNullDecoder(encoding.SomeNulls),
            Proto.Encodings.V20.Nullable.NullabilityOneofCase.AllNulls => AllNullDecoder.Instance,
            _ => throw new LanceFormatException(
                $"Unknown Nullable.nullability case {encoding.NullabilityCase}."),
        };
    }

    private sealed class SomeNullDecoder : IArrayDecoder
    {
        private readonly IArrayDecoder _validity;
        private readonly IArrayDecoder _values;
        private readonly Proto.Encodings.V20.Nullable.Types.SomeNull _raw;

        public SomeNullDecoder(Proto.Encodings.V20.Nullable.Types.SomeNull some)
        {
            _raw = some;
            _validity = V20ArrayEncodingDispatcher.Create(some.Validity);
            _values = V20ArrayEncodingDispatcher.Create(some.Values);
        }

        public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
        {
            // Validity is always a bitmap (bool). The values decoder produces a
            // non-null array of the target type; we splice its value buffer into
            // a new ArrayData with the validity bitmap attached.
            var validityArr = _validity.Decode(numRows, BooleanType.Default, in context);
            if (validityArr is not BooleanArray validityBool)
                throw new LanceFormatException(
                    $"SomeNull validity decoded to {validityArr.GetType().Name}, expected BooleanArray.");

            var valuesArr = _values.Decode(numRows, targetType, in context);

            int length = checked((int)numRows);
            int nullCount = length - CountSetBits(validityBool.Values, length);

            // The Arrow validity bitmap lives at buffer index 0 across array layouts.
            // Replace whatever was there (typically ArrowBuffer.Empty) with the
            // real bitmap and recompute nullCount.
            ArrowBuffer[] buffers = valuesArr.Data.Buffers.ToArray();
            buffers[0] = validityBool.ValueBuffer;

            var newData = new ArrayData(
                valuesArr.Data.DataType,
                length,
                nullCount,
                offset: 0,
                buffers,
                children: valuesArr.Data.Children,
                dictionary: valuesArr.Data.Dictionary);

            // Dispose the intermediate values array (we've taken ownership of its buffers).
            return ArrowArrayFactory.BuildArray(newData);
        }

        private static int CountSetBits(ReadOnlySpan<byte> bitmap, int length)
        {
            int count = 0;
            int fullBytes = length / 8;
            for (int i = 0; i < fullBytes; i++)
                count += PopCount8(bitmap[i]);
            int tailBits = length & 7;
            if (tailBits > 0 && fullBytes < bitmap.Length)
            {
                byte tail = bitmap[fullBytes];
                for (int b = 0; b < tailBits; b++)
                    if ((tail & (1 << b)) != 0)
                        count++;
            }
            return count;
        }

        // Hand-rolled byte popcount to avoid System.Numerics.BitOperations
        // on netstandard2.0. Hot path is short (one byte per 8 rows), so the
        // scalar SWAR trick is fine.
        private static int PopCount8(byte b)
        {
            int x = b;
            x = x - ((x >> 1) & 0x55);
            x = (x & 0x33) + ((x >> 2) & 0x33);
            return (x + (x >> 4)) & 0x0f;
        }
    }

    private sealed class AllNullDecoder : IArrayDecoder
    {
        public static readonly AllNullDecoder Instance = new();

        public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
        {
            int length = checked((int)numRows);

            // Validity bitmap of all zeros.
            int bitmapBytes = (length + 7) / 8;
            var validity = new ArrowBuffer(new byte[bitmapBytes]);

            // Fabricate zero-filled value buffers matching the target type's layout.
            var buffers = NullValueBuffers(targetType, length, validity);
            var data = new ArrayData(
                targetType, length,
                nullCount: length,
                offset: 0,
                buffers);
            return ArrowArrayFactory.BuildArray(data);
        }

        private static ArrowBuffer[] NullValueBuffers(IArrowType targetType, int length, ArrowBuffer validity)
        {
            switch (targetType)
            {
                case NullType:
                    return new[] { validity };

                case BooleanType:
                    return new[] { validity, new ArrowBuffer(new byte[(length + 7) / 8]) };

                case StringType or BinaryType:
                    // offsets: (length + 1) × 4, values: empty.
                    return new[] { validity, new ArrowBuffer(new byte[(length + 1) * sizeof(int)]), ArrowBuffer.Empty };

                case LargeStringType or LargeBinaryType:
                    return new[] { validity, new ArrowBuffer(new byte[(length + 1) * sizeof(long)]), ArrowBuffer.Empty };

                case FixedSizeBinaryType fsb:
                    return new[] { validity, new ArrowBuffer(new byte[length * fsb.ByteWidth]) };

                case FixedWidthType fw:
                    {
                        int bytesPerValue = fw.BitWidth / 8;
                        return new[] { validity, new ArrowBuffer(new byte[length * bytesPerValue]) };
                    }

                default:
                    throw new NotImplementedException(
                        $"AllNull decoding for Arrow type {targetType} is not supported yet.");
            }
        }
    }
}
