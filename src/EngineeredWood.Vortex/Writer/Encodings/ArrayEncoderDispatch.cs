// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Vortex.Writer.Encodings;

/// <summary>
/// Indices in the file's <c>array_specs</c> registry. Threaded through the
/// encoders so each can emit its <c>encoding</c> field with the right value.
/// The values must match the writer's registry order (see VortexFileWriter).
/// </summary>
internal readonly record struct EncodingIndices(
    ushort Primitive, ushort Bool, ushort VarBin, ushort List, ushort FixedSizeList,
    ushort BitPacked, ushort Decimal);

/// <summary>
/// Routes an Arrow array to its matching encoder's recursive <c>Emit</c>
/// method. Used both at the top level (one column → one segment via
/// VortexFileWriter) and recursively from List/FixedSizeList encoders to
/// embed their child element subtrees.
/// </summary>
internal static class ArrayEncoderDispatch
{
    /// <summary>
    /// <param name="compress">When true, eligible columns auto-route through
    /// compressing encodings (currently <c>fastlanes.bitpacked</c> for
    /// non-nullable unsigned ints with MaxBits &lt; native).</param>
    /// </summary>
    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx,
        int? statsTicket = null, bool compress = false)
    {
        if (compress && BitPackedArrayEncoder.IsApplicable(array))
            return BitPackedArrayEncoder.Emit(sb, array, idx.BitPacked, idx.Bool, statsTicket);

        return array switch
        {
            ListArray => ListArrayEncoder.Emit(sb, array, idx, statsTicket),
            FixedSizeListArray => FixedSizeListArrayEncoder.Emit(sb, array, idx, statsTicket),
            // Decimal128/256Array inherit from FixedSizeBinaryArray, so they MUST
            // be matched before any FixedSizeBinary case (none yet, but mind it).
            Decimal128Array or Decimal256Array => DecimalArrayEncoder.Emit(sb, array, idx.Decimal, idx.Bool, statsTicket),
            StringArray or BinaryArray => VarBinArrayEncoder.Emit(sb, array, idx.VarBin, idx.Primitive, idx.Bool, statsTicket),
            BooleanArray => BoolArrayEncoder.Emit(sb, array, idx.Bool, statsTicket),
            _ => PrimitiveArrayEncoder.Emit(sb, array, idx.Primitive, idx.Bool, statsTicket),
        };
    }
}
