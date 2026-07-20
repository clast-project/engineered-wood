// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex;

/// <summary>
/// Per-zone statistics that may be carried by the <c>vortex.stats</c>
/// (zoned) layout. Enum values match upstream vortex's
/// <c>vortex_array::expr::stats::Stat</c>: positions are stable and used as
/// bit indices in the layout-metadata bitset.
/// </summary>
public enum Stat : byte
{
    IsConstant = 0,
    IsSorted = 1,
    IsStrictSorted = 2,
    Max = 3,
    Min = 4,
    Sum = 5,
    NullCount = 6,
    UncompressedSizeInBytes = 7,
    NaNCount = 8,
}
