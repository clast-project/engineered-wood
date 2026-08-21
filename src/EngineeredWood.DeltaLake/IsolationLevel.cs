// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// The isolation level a transaction is validated at when it commits concurrently with others.
///
/// <para>The two levels differ in two places, both following from the same rule — that under
/// <see cref="Serializable"/> commit order IS the logical order:</para>
/// <list type="number">
/// <item>Whether a concurrent <b>blind append</b> (a commit that only adds files — no removes, no
/// metadata/protocol change, hence no read dependency) can conflict with a transaction whose read
/// predicates its new files happen to match. Under <see cref="Serializable"/> it can; under
/// <see cref="WriteSerializable"/> it cannot.</item>
/// <item>Whether a concurrent DELETE of <b>different rows of a file this transaction also deletes from</b>
/// reconciles at row granularity (deletion-vector union) instead of conflicting at file granularity. That
/// reconciliation silences a <c>dataChange=true</c> remove of a file this transaction read, so it runs only
/// under <see cref="WriteSerializable"/>. Relocating rows across a concurrent <i>compaction</i> is not
/// affected: its removes are <c>dataChange=false</c>, exempt at both levels.</item>
/// </list>
/// <para>Everything else — concurrent metadata/protocol changes, delete/delete, and a concurrent
/// <c>dataChange=true</c> remove of a file this transaction read — conflicts at both levels.</para>
/// </summary>
public enum IsolationLevel
{
    /// <summary>
    /// The Delta default. A concurrent blind append never conflicts, on the reasoning that its rows did
    /// not depend on anything this transaction might invalidate — so the two commits can be linearized
    /// as "append happened after". Strictly weaker than <see cref="Serializable"/> only for that one
    /// case, and it is the level Spark and delta-rs use unless told otherwise.
    /// </summary>
    WriteSerializable = 0,

    /// <summary>
    /// Full serializability: a concurrent blind append whose added files match this transaction's read
    /// predicates is treated as a conflict, because under a strict serial order this transaction might
    /// have been required to read those rows. For the same reason a concurrent DELETE from a file this
    /// transaction also deletes from conflicts at FILE granularity, however disjoint the rows — the
    /// row-level deletion-vector union is a <see cref="WriteSerializable"/> behaviour.
    /// </summary>
    Serializable = 1,
}
