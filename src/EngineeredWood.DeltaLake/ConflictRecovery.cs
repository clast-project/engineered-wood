// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// What a caller can do about a <see cref="DeltaConflictException"/>: re-attempt the same work, or
/// rebuild it against the table as it now stands.
///
/// <para>The distinction is the one thing a retry loop needs and could not previously ask for. Every
/// conflict message ends in some form of "try again", but they do not mean the same thing — one means
/// the actions are still valid and only their version number is wrong, and the other means the plan
/// was computed against a table state that no longer holds.</para>
/// </summary>
/// <remarks>
/// <para><b>Why there is no third value.</b> "Give up" was considered and rejected: no conflict kind
/// justifies it. A concurrent <c>metaData</c> change looks fatal but is not — the checker raises it for
/// ANY concurrent metadata action, including an <c>AddColumnAsync</c> adding a nullable column, after
/// which a re-planned commit succeeds trivially. A protocol change is the same: if the new protocol
/// genuinely requires a writer feature this library does not implement, the caller learns that on the
/// NEXT attempt as a <see cref="DeltaFormatException"/> from the protocol gate, not as a conflict. The
/// unrecoverable case already has its own exception type and never arrives here.</para>
/// <para>So whether to stop is a property of the caller's retry budget and its own schema assumptions,
/// not of the conflict — and a library asserting it would be overreaching. An enum rather than a bool
/// nonetheless, so that a genuinely new recovery (catalog-managed commits are the plausible source) can
/// be added without widening a <c>bool</c> and breaking every caller.</para>
/// </remarks>
public enum ConflictRecovery
{
    /// <summary>
    /// The staged actions are still valid; only the version they were aimed at was taken. Re-attempt
    /// them at a newer version.
    ///
    /// <para>Rare to observe in practice, and that is by design: <see cref="Log.LogCommitter"/> does
    /// exactly this internally, so it escapes to a caller only when the attempt budget runs out
    /// (<c>MaxAttempts</c>), or when a caller drives <see cref="Log.TransactionLog.WriteCommitAsync"/>
    /// itself and owns the loop.</para>
    /// </summary>
    Replay = 0,

    /// <summary>
    /// The plan is stale — a concurrent commit touched something it read, removed, or assumed. Re-read
    /// the table, recompute what to write, and commit that.
    ///
    /// <para>Re-attempting the same actions is wrong or pointless here, which is the failure mode this
    /// whole type exists to prevent: a host that treats every conflict as retryable can replay work
    /// whose premise has gone.</para>
    /// </summary>
    Replan = 1,
}
