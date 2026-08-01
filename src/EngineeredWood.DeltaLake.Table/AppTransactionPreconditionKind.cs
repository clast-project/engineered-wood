// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>Which precondition an <see cref="AppTransactionPrecondition"/> states.</summary>
public enum AppTransactionPreconditionKind
{
    /// <summary>No check: record the version unconditionally. The default.</summary>
    None = 0,

    /// <summary>The table must record NO version at all for the producer.</summary>
    Absent,

    /// <summary>The table must record exactly <see cref="AppTransactionPrecondition.Version"/>.</summary>
    Exactly,

    /// <summary>The table must record nothing, or a version LOWER than the one being committed.</summary>
    NotApplied,
}
