// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Log;

/// <summary>What a commit landed on.</summary>
/// <param name="Version">The version the actions were committed at — NOT necessarily
/// <c>BaseSnapshot.Version + 1</c>, because a rebase past a non-conflicting concurrent commit lands
/// later. When <paramref name="Committed"/> is false this is the base version, unchanged.</param>
/// <param name="Snapshot">The table state after the commit, built incrementally from the caller's. Worth
/// keeping: it is the newest snapshot in hand, and re-reading the log to get one would only repeat the
/// work.</param>
/// <param name="Committed">False when there was nothing to commit — an empty action list is a no-op, not
/// an empty version. Distinguishes "landed at version 7" from "was already at version 7".</param>
public readonly record struct LogCommitResult(
    long Version,
    Snapshot.Snapshot Snapshot,
    bool Committed);
