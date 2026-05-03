// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Format;

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Decoder for <c>vortex.null</c>: every row is null. No buffers, no children,
/// empty metadata — only the length matters.
/// </summary>
internal static class NullArrayDecoder
{
    public static IArrowArray Decode(
        ArrayNode node, IArrowType expectedType, long expectedRowCount)
    {
        if (node.BufferRefCount != 0 || node.ChildCount != 0)
            throw new VortexFormatException(
                $"vortex.null expects 0 buffers and 0 children, got {node.BufferRefCount} / {node.ChildCount}.");

        if (expectedType is not NullType)
            throw new NotSupportedException(
                $"vortex.null only produces NullType arrays, got expected {expectedType}.");

        return new NullArray(checked((int)expectedRowCount));
    }
}
