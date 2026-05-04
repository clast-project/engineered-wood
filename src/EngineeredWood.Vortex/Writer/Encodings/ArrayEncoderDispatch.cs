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
    ushort BitPacked, ushort Decimal, ushort Constant, ushort For, ushort Delta,
    ushort Dict, ushort Rle, ushort Struct_, ushort Alp, ushort RunEnd, ushort Sparse,
    ushort FsstString, ushort AlpRd);

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
    /// compressing encodings in dispatch order. The order matters: each gate
    /// rejects the column if its niche doesn't fit, so cheaper / more
    /// specialised encodings are checked first. Order:
    /// constant > dict > alp > rle > runend > delta > FoR > bitpacked.
    /// Constant strictly subsumes everything when the column is uniform.
    /// Dict is StringArray-only. ALP and RLE are float-only. RunEnd handles
    /// long-run integer columns that bitpacked alone wouldn't compress as
    /// hard. Delta is gated on <c>stats.IsStrictSorted</c>. FoR is checked
    /// before plain bitpacked because it strictly subsumes bitpacked for
    /// columns where it applies (FoR with min=0 would be identical, but we
    /// only enable FoR when min != 0 or signed-with-negatives).</param>
    /// <param name="stats">Top-level column statistics (passed by ref to avoid
    /// copying). Encoders consult fields like <c>IsStrictSorted</c> to decide
    /// profitability without re-scanning.</param>
    /// </summary>
    public static int Emit(
        SegmentBuilder sb, IArrowArray array, EncodingIndices idx,
        int? statsTicket = null, bool compress = false,
        ArrayStatsValues stats = default)
    {
        if (compress && ConstantArrayEncoder.IsApplicable(array))
            return ConstantArrayEncoder.Emit(sb, array, idx.Constant, statsTicket);
        if (compress && DictArrayEncoder.IsApplicable(array))
            return DictArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && FsstArrayEncoder.IsApplicable(array))
            return FsstArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && AlpArrayEncoder.IsApplicable(array))
            return AlpArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && AlpRdArrayEncoder.IsApplicable(array))
            return AlpRdArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && RleArrayEncoder.IsApplicable(array))
            return RleArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && SparseArrayEncoder.IsApplicable(array))
            return SparseArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && RunEndArrayEncoder.IsApplicable(array))
            return RunEndArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && DeltaArrayEncoder.IsApplicable(array))
            return DeltaArrayEncoder.Emit(sb, array, idx.Delta, idx.Primitive, idx.BitPacked, idx.Bool, statsTicket);
        if (compress && ForArrayEncoder.IsApplicable(array))
            return ForArrayEncoder.Emit(sb, array, idx.For, idx.BitPacked, idx.Primitive, idx.Bool, statsTicket);
        if (compress && BitPackedArrayEncoder.IsApplicable(array))
            return BitPackedArrayEncoder.Emit(sb, array, idx.BitPacked, idx.Primitive, idx.Bool, statsTicket);

        return array switch
        {
            StructArray => StructArrayEncoder.Emit(sb, array, idx, statsTicket),
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
