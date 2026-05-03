// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// Well-known layout encoding ids registered by <c>vortex_file::register_default_encodings</c>.
/// Match exactly the strings produced by the canonical Rust impl as of vortex 0.70.
/// </summary>
internal static class VortexLayoutEncodings
{
    /// <summary>Single-buffer leaf: one segment, holds a serialized Vortex array message + raw buffers.</summary>
    public const string Flat = "vortex.flat";

    /// <summary>Columnar wrapper: one child per Arrow field of the parent struct dtype.</summary>
    public const string Struct = "vortex.struct";

    /// <summary>Row-wise partitioned wrapper: children are row chunks, materialized in order.</summary>
    public const string Chunked = "vortex.chunked";

    /// <summary>Stats-wrapper layout: typically a child carrying a stats table plus the data layout.</summary>
    public const string Stats = "vortex.stats";

    /// <summary>Dictionary-sharing layout: one child for indices, sibling for the dictionary array.</summary>
    public const string Dictionary = "vortex.dict";

    /// <summary>Zone-map layout for filter pruning. Not yet implemented.</summary>
    public const string Zoned = "vortex.zoned";
}
