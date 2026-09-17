// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.map</c>: the canonical encoding of the <c>Map</c> dtype (vortex 0.86+).
///
/// <para>Wire format (per <c>arrays/map/vtable/mod.rs</c>): 0 buffers, empty metadata, and a
/// single child holding the entries — a <c>vortex.listview</c> of the non-nullable
/// <c>{key, value}</c> entry struct. The map's own validity is the entry list's: a null map row
/// is a null list row, which is why the encoding carries no validity of its own.</para>
///
/// <para>Arrow models a map the same way — <c>List&lt;Struct{key, value}&gt;</c> with i32
/// offsets — so the entries child decodes straight into a <see cref="ListArray"/> whose buffers
/// and struct child become the <see cref="MapArray"/>'s. Entry order is preserved, duplicate keys
/// included; <c>keys_sorted</c> travels in the type, not the data.</para>
/// </summary>
internal static class MapArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node,
        SerializedArray serialized,
        IReadOnlyList<string> arraySpecs,
        IArrowType expectedType,
        long expectedRowCount)
    {
        if (expectedType is not MapType mapType)
            throw new VortexFormatException(
                $"vortex.map requires MapType, got {expectedType}.");
        if (node.BufferRefCount != 0)
            throw new VortexFormatException(
                $"vortex.map expects 0 buffers, got {node.BufferRefCount}.");
        if (node.ChildCount != 1)
            throw new VortexFormatException(
                $"vortex.map expects 1 child (entries), got {node.ChildCount}.");
        if (node.Metadata.Length != 0)
            throw new VortexFormatException(
                $"vortex.map expects empty metadata, got {node.Metadata.Length} bytes.");

        // mapType.Fields[0] is Arrow's non-nullable "entries" struct field.
        var entries = ArrayDecoder.DecodeNode(
            node.Child(0), serialized, arraySpecs, new ListType(mapType.Fields[0]), expectedRowCount);
        if (entries is not ListArray list)
            throw new VortexFormatException(
                $"vortex.map entries decoded to {entries.GetType().Name}, expected a list.");

        var d = list.Data;
        return new MapArray(new ArrayData(
            mapType, d.Length, d.NullCount, d.Offset, d.Buffers, d.Children));
    }
}
