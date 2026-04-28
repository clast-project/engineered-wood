// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V21;

namespace EngineeredWood.Lance.Encodings.V21;

/// <summary>
/// Top-level dispatcher for Lance v2.1 <see cref="PageLayout"/> protos.
/// Phase 6 supports <see cref="MiniBlockLayout"/>; Full-zip and Blob layouts
/// land in Phase 8, and <see cref="ConstantLayout"/> is a narrow inline case
/// handled here.
/// </summary>
internal static class PageLayoutDispatcher
{
    public static IArrowArray Decode(
        PageLayout layout, long numRowsFromPage,
        IArrowType targetType, in PageContext context)
    {
        switch (layout.LayoutCase)
        {
            case PageLayout.LayoutOneofCase.MiniBlockLayout:
                return MiniBlockLayoutDecoder.Decode(
                    layout.MiniBlockLayout, targetType, context);

            case PageLayout.LayoutOneofCase.ConstantLayout:
                throw new NotImplementedException(
                    "ConstantLayout decoding is not yet supported (Phase 6 carryover; implement when pylance emits it).");

            case PageLayout.LayoutOneofCase.FullZipLayout:
                return FullZipLayoutDecoder.Decode(
                    layout.FullZipLayout, targetType, context);

            case PageLayout.LayoutOneofCase.BlobLayout:
                throw new NotImplementedException(
                    "BlobLayout is not yet supported (two-level layout with external blob storage).");

            default:
                throw new LanceFormatException(
                    $"PageLayout has no layout case set (got {layout.LayoutCase}).");
        }
    }
}
