// Copyright (c) clast-project. All rights reserved.
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

    /// <summary>
    /// Zoned layout (upstream Rust calls this <c>ZonedLayout</c> but
    /// serializes the encoding id as the legacy string <c>vortex.stats</c>;
    /// per <c>vortex-layout/src/layouts/zoned/mod.rs</c>: "For legacy
    /// reasons the serialized layout encoding ID is still vortex.stats.").
    /// Two children — child[0] = data, child[1] = zones table — plus
    /// metadata <c>{ zone_len: u32 LE, present_stats: bitset }</c>. Used
    /// for filter pruning via the shared
    /// <see cref="EngineeredWood.Expressions.Predicate"/> API
    /// (see <see cref="VortexFileReader.ReadAllAsync(EngineeredWood.Expressions.Predicate, System.Threading.CancellationToken)"/>).
    /// </summary>
    public const string Stats = "vortex.stats";

    /// <summary>Dictionary-sharing layout: one child for indices, sibling for the dictionary array.</summary>
    public const string Dictionary = "vortex.dict";
}
