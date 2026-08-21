// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Whether a table declares anything that has to be evaluated against the rows being written.
/// </summary>
/// <remarks>
/// One place, because the write gate and the write path have to agree about it. A gate that asks
/// a narrower question than the path enforces refuses tables the path could have handled — which
/// is exactly how the transactional append came to be gated shut against its own validator.
/// </remarks>
internal static class WriteTimeExpressions
{
    /// <summary>
    /// True when the snapshot carries a CHECK constraint, an invariant, or a generated column.
    /// </summary>
    /// <remarks>Deliberately does not parse; the parse belongs where the rows are.</remarks>
    public static bool Declares(Snapshot.Snapshot snapshot) =>
        DeltaConstraintEnforcer.Declares(snapshot) || DeltaGeneratedColumns.Declares(snapshot);
}
