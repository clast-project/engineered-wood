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
    ushort Dict, ushort Rle);

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
    /// compressing encodings in this order:
    /// <list type="number">
    ///   <item><c>vortex.constant</c> — fully-uniform columns (one ScalarValue, no buffers).</item>
    ///   <item><c>fastlanes.delta</c> — strictly-sorted unsigned-integer columns
    ///     (delta-encoded successive differences inside FastLanes-transposed lanes).</item>
    ///   <item><c>fastlanes.for</c> — integer columns where shifting by min tightens
    ///     the bit width (or is required because the column has negative values).</item>
    ///   <item><c>fastlanes.bitpacked</c> — non-nullable integers with MaxBits &lt; native.</item>
    /// </list>
    /// Constant is checked first because it's strictly smaller. Delta is gated
    /// on <c>stats.IsStrictSorted</c> — that tells us within-lane successive
    /// differences will be small without an O(n) probe scan. FoR is checked
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
        if (compress && RleArrayEncoder.IsApplicable(array))
            return RleArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && DeltaArrayEncoder.IsApplicable(array))
            return DeltaArrayEncoder.Emit(sb, array, idx.Delta, idx.Primitive, idx.BitPacked, idx.Bool, statsTicket);
        if (compress && ForArrayEncoder.IsApplicable(array))
            return ForArrayEncoder.Emit(sb, array, idx.For, idx.BitPacked, idx.Bool, statsTicket);
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
