// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Top-level dispatch for converting a parsed <see cref="SerializedArray"/>
/// into an Apache Arrow <see cref="IArrowArray"/>. Encoding-specific
/// implementations live in sibling files (e.g. <c>PrimitiveArrayDecoder</c>);
/// this dispatcher just resolves the encoding id and routes.
/// </summary>
internal static class ArrayDecoder
{
    public static IArrowArray Decode(
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        var rootNode = serialized.Message.Root;
        return DecodeNode(rootNode, serialized, arraySpecs, expectedType, expectedRowCount);
    }

    public static IArrowArray DecodeNode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        var encId = ResolveEncoding(node.EncodingIndex, arraySpecs);
        return encId switch
        {
            VortexArrayEncodings.Primitive => PrimitiveArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.Sequence => SequenceArrayDecoder.Decode(
                node, expectedType, expectedRowCount),
            VortexArrayEncodings.Constant => ConstantArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.Bool => BoolArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.VarBinView => VarBinViewArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.FsstString => FsstArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.RunEnd => RunEndArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.FastlanesBitPacked => BitPackedArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.FastlanesFor => ForArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.FastlanesDelta => DeltaArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.FastlanesRle => RleArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.Pco => PcoArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.Alp => AlpArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.AlpRD => AlpRdArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.Null => NullArrayDecoder.Decode(
                node, expectedType, expectedRowCount),
            VortexArrayEncodings.ByteBool => ByteBoolArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.VarBin => VarBinArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.Dict => DictArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.Sparse => SparseArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.Decimal => DecimalArrayDecoder.Decode(
                node, serialized, expectedType, expectedRowCount),
            VortexArrayEncodings.DecimalByteParts => DecimalBytePartsArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.DateTimeParts => DateTimePartsArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.Extension => ExtensionArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.FixedSizeList => FixedSizeListArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.List => ListArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            VortexArrayEncodings.ListView => ListViewArrayDecoder.Decode(
                node, serialized, arraySpecs, expectedType, expectedRowCount),
            _ => throw new NotSupportedException(
                $"Vortex array encoding '{encId}' is not yet implemented. " +
                "Add a decoder and a fixture that exercises it."),
        };
    }

    private static string ResolveEncoding(ushort idx, IReadOnlyList<string> arraySpecs)
    {
        if (idx >= arraySpecs.Count)
            throw new VortexFormatException(
                $"Array encoding index {idx} is out of range (registry has {arraySpecs.Count} entries).");
        return arraySpecs[idx];
    }
}
