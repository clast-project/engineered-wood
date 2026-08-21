// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// How a write path accounts for the table's CHECK constraints, invariants and generated columns.
/// </summary>
/// <remarks>
/// Three states rather than a boolean, because two of them are not equally safe and a boolean
/// would let a reader mistake one for the other. <see cref="ValidatedHere"/> is a fact this
/// library establishes; <see cref="AssertedByCaller"/> is a claim it cannot check. Naming them
/// apart is what makes the unverifiable one greppable — every place a write proceeds on
/// somebody's word says so in its own call.
/// </remarks>
internal enum WriteTimeExpressionHandling
{
    /// <summary>
    /// The path cannot see the rows, so a table declaring any write-time expression is refused.
    /// </summary>
    /// <remarks>
    /// The default, and deliberately the value a new call site inherits by saying nothing: Delta
    /// enforces these at write time only, so one unvalidated commit poisons the table for every
    /// later reader.
    /// </remarks>
    Refuse = 0,

    /// <summary>This path evaluates the expressions against the batches itself.</summary>
    ValidatedHere,

    /// <summary>
    /// A host declared it enforced them over rows this library never saw.
    /// </summary>
    /// <remarks>
    /// Only the external-file seams, which are handed finished Parquet. Nothing verifies it, and a
    /// false claim commits rows every later reader will trust.
    /// </remarks>
    AssertedByCaller,
}
