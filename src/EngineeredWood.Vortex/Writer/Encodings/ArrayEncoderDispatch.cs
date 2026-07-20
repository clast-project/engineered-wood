// Copyright (c) clast-project. All rights reserved.
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
    ushort FsstString, ushort AlpRd, ushort VarBinView, ushort Pco,
    ushort DateTimeParts, ushort Ext);

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
        ArrayStatsValues stats = default,
        bool preferVarBinView = false,
        bool preferPco = false,
        bool preferDateTimeParts = false)
    {
        // Extension-typed columns (TimestampArray / Date32Array / Date64Array)
        // need to be wrapped in a vortex.ext ArrayNode whose single child is
        // the storage encoding. Upstream's wire shape: 0 buffers, 1 child,
        // empty metadata (per vortex-array/src/arrays/extension/vtable/mod.rs).
        // Dispatched ahead of the compress chain so the wrapper sits on the
        // OUTSIDE — stats attach to the wrapper, the storage encoding has
        // statsTicket=null.
        if (array is Apache.Arrow.TimestampArray)
        {
            int storageTicket;
            if (preferDateTimeParts && DateTimePartsArrayEncoder.IsApplicable(array))
                storageTicket = DateTimePartsArrayEncoder.Emit(sb, array, idx, statsTicket: null);
            else
                storageTicket = PrimitiveArrayEncoder.Emit(sb, array, idx.Primitive, idx.Bool, statsTicket: null);
            return WrapExtension(sb, idx.Ext, storageTicket, statsTicket);
        }
        if (array is Apache.Arrow.Date32Array or Apache.Arrow.Date64Array
            or Apache.Arrow.Time32Array or Apache.Arrow.Time64Array)
        {
            int storageTicket = PrimitiveArrayEncoder.Emit(
                sb, array, idx.Primitive, idx.Bool, statsTicket: null);
            return WrapExtension(sb, idx.Ext, storageTicket, statsTicket);
        }
        // FixedSizeBinaryArray (NOT Decimal128/256, which inherit from FSB
        // and are matched ahead of this in the type switch below) — emitted
        // as vortex.uuid wrapping FSL<U8, byteWidth>. Today the DType
        // serializer only emits the vortex.uuid Extension for byteWidth=16,
        // matching upstream's UUID convention; other widths fail at the
        // schema level so we never reach here for them.
        if (array is Apache.Arrow.Arrays.FixedSizeBinaryArray fsb
            && array is not Decimal128Array and not Decimal256Array)
        {
            var fslArr = WrapFixedSizeBinaryAsListOfBytes(fsb);
            int storageTicket = FixedSizeListArrayEncoder.Emit(
                sb, fslArr, idx, statsTicket: null);
            return WrapExtension(sb, idx.Ext, storageTicket, statsTicket);
        }

        if (compress && ConstantArrayEncoder.IsApplicable(array))
            return ConstantArrayEncoder.Emit(sb, array, idx.Constant, statsTicket);
        if (compress && DictArrayEncoder.IsApplicable(array))
            return DictArrayEncoder.Emit(sb, array, idx, statsTicket);
        if (compress && FsstArrayEncoder.IsApplicable(array))
            return FsstArrayEncoder.Emit(sb, array, idx, statsTicket);
        // Pco supersedes the float/integer compressing chain when the user
        // opts in. Keep it after constant/dict/fsst (those are strictly
        // better for their niches) but before ALP/RLE/FoR/bitpacked so a
        // numeric column with preferPco=true reliably lands on pco.
        if (compress && preferPco && PcoArrayEncoder.IsApplicable(array))
            return PcoArrayEncoder.Emit(sb, array, idx, statsTicket);
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
            // StringArray inherits from BinaryArray, so the BinaryArray
            // pattern below catches both string + binary columns when the
            // user opts into varbinview.
            BinaryArray when preferVarBinView => VarBinViewArrayEncoder.Emit(sb, array, idx, statsTicket),
            StringArray or BinaryArray => VarBinArrayEncoder.Emit(sb, array, idx.VarBin, idx.Primitive, idx.Bool, statsTicket),
            BooleanArray => BoolArrayEncoder.Emit(sb, array, idx.Bool, statsTicket),
            _ => PrimitiveArrayEncoder.Emit(sb, array, idx.Primitive, idx.Bool, statsTicket),
        };
    }

    /// <summary>
    /// Wraps <paramref name="storageTicket"/> in a <c>vortex.ext</c> ArrayNode.
    /// Upstream's wire shape: 0 buffers, 1 child (the storage encoding),
    /// empty metadata. Stats attach to the wrapper, not the storage.
    /// </summary>
    private static int WrapExtension(SegmentBuilder sb, ushort extEncodingIdx, int storageTicket, int? statsTicket)
    {
        var children = new[] { storageTicket };
        return statsTicket is null
            ? ArrayNodeEmitter.EmitWithChildrenOnly(sb.Builder, extEncodingIdx, children)
            : ArrayNodeEmitter.EmitWithChildrenAndStats(sb.Builder, extEncodingIdx, children, statsTicket.Value);
    }

    /// <summary>
    /// Reinterprets a <see cref="FixedSizeBinaryArray"/> as the corresponding
    /// <c>FixedSizeList&lt;UInt8, byteWidth&gt;</c> needed for vortex.uuid
    /// storage. FSB lays out <c>n × byteWidth</c> contiguous bytes in
    /// <c>Buffers[1]</c> exactly the way an FSL of u8 would, so the
    /// rewrap is a pure metadata change — no value-buffer copy.
    /// </summary>
    private static FixedSizeListArray WrapFixedSizeBinaryAsListOfBytes(
        Apache.Arrow.Arrays.FixedSizeBinaryArray fsb)
    {
        var fsbType = (Apache.Arrow.Types.FixedSizeBinaryType)fsb.Data.DataType;
        int byteWidth = fsbType.ByteWidth;
        var fsbData = fsb.Data;
        int rowCount = fsb.Length;

        // Inner UInt8Array carries the contiguous bytes. Length = rowCount *
        // byteWidth so the FSL's child invariant holds; offset = 0 because we
        // adjust visibility through the parent FSL's offset instead.
        int innerOffset = fsbData.Offset * byteWidth;
        int innerLength = rowCount * byteWidth;
        var innerData = new ArrayData(
            Apache.Arrow.Types.UInt8Type.Default,
            length: fsbData.Buffers[1].Length, // covers the full underlying buffer
            nullCount: 0,
            offset: 0,
            buffers: new[] { ArrowBuffer.Empty, fsbData.Buffers[1] });
        // Leave the inner array's full underlying buffer in place; the FSL's
        // outer offset will steer the visible window. (Slicing inner here
        // would force a copy via Apache.Arrow.Array.Slice.)
        _ = innerLength; _ = innerOffset;

        var elemField = new Field("item", Apache.Arrow.Types.UInt8Type.Default, nullable: false);
        var fslType = new Apache.Arrow.Types.FixedSizeListType(elemField, byteWidth);
        var fslData = new ArrayData(
            fslType,
            length: rowCount,
            nullCount: fsbData.NullCount,
            offset: fsbData.Offset,
            buffers: new[] { fsbData.NullCount > 0 ? fsbData.Buffers[0] : ArrowBuffer.Empty },
            children: new[] { innerData });
        return new FixedSizeListArray(fslData);
    }
}
