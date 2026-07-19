// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The isolation level a transaction is validated at when it commits concurrently with others.
///
/// <para>The two levels differ in exactly one place: whether a concurrent <b>blind append</b> (a commit
/// that only adds files — no removes, no metadata/protocol change, hence no read dependency) can
/// conflict with a transaction whose read predicates its new files happen to match. Under
/// <see cref="Serializable"/> it can; under <see cref="WriteSerializable"/> it cannot. Everything else —
/// concurrent metadata/protocol changes, delete/delete, and a concurrent remove of a file this
/// transaction read — conflicts at both levels.</para>
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
    /// have been required to read those rows.
    /// </summary>
    Serializable = 1,
}
