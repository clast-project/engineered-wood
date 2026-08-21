// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// A commit did not land because another writer got there first — either by taking the version it
/// aimed at, or by changing something it depended on.
///
/// <para>Those are different failures demanding different responses, and until this type carried
/// <see cref="Recovery"/> a caller could not tell them apart: every conflict arrived as this exception
/// with a message and nothing else, so a host either retried all of them (and could replay work whose
/// premise had gone) or none of them (and failed commits a rebase would have landed).</para>
///
/// <para><see cref="ErrorCode"/> names the condition, in the same flat <c>DELTA_*</c> namespace as
/// <see cref="DeltaErrorCodes"/> and <c>DeltaTableErrorCodes</c>, so a caller switches on one kind of
/// identifier for every Delta failure rather than on a message. Six of the codes are delta-spark's own
/// names for the same conditions — see <see cref="DeltaErrorCodes"/> — so a host bridging engines can
/// treat them as equivalent.</para>
///
/// <example>
/// <code>
/// catch (DeltaConflictException e) when (e.Recovery == ConflictRecovery.Replay)
/// {
///     // Same actions, newer version.
/// }
/// catch (DeltaConflictException e)
/// {
///     // Replan: re-read and recompute. e.ErrorCode says what moved, e.ConflictingVersion says where.
/// }
/// </code>
/// </example>
/// </summary>
public class DeltaConflictException : Exception
{
    /// <summary>
    /// The version this commit tried to take, or null when it never got that far — an
    /// optimistic-concurrency verdict aborts before any version is attempted.
    /// </summary>
    /// <remarks>
    /// Was a non-nullable <c>long</c> carrying <c>-1</c> for "not applicable", which was the only way
    /// to tell the two failures apart and was documented nowhere. Null now means what <c>-1</c> meant.
    /// </remarks>
    public long? AttemptedVersion { get; }

    /// <summary>
    /// The version of the concurrent commit that caused this, when one commit is responsible. Null when
    /// no single version is to blame, or when the conflict is a lost version slot rather than a verdict
    /// about a specific commit.
    /// </summary>
    /// <remarks>
    /// Distinct from <see cref="AttemptedVersion"/> and rarely equal to it: one is where we tried to
    /// go, the other is what stopped us. delta-spark reports the same thing — every message in its
    /// concurrency error family carries a <c>conflictingCommit</c> placeholder.
    /// </remarks>
    public long? ConflictingVersion { get; }

    /// <summary>
    /// The stable identifier for this condition — a <c>DELTA_*</c> constant from
    /// <see cref="DeltaErrorCodes"/> or the table layer's <c>DeltaTableErrorCodes</c>. Null only for an
    /// exception built through the message-only constructor, which predates this property.
    /// </summary>
    public string? ErrorCode { get; }

    /// <summary>What to do about it. See <see cref="ConflictRecovery"/>.</summary>
    public ConflictRecovery Recovery { get; }

    /// <summary>
    /// The version slot was taken: another writer created this version first. The staged actions
    /// themselves are untouched, so this is <see cref="ConflictRecovery.Replay"/>.
    /// </summary>
    public DeltaConflictException(long attemptedVersion)
        : base($"Commit conflict: version {attemptedVersion} already exists.")
    {
        AttemptedVersion = attemptedVersion;
        ErrorCode = DeltaErrorCodes.ConcurrentWrite;
        Recovery = ConflictRecovery.Replay;
    }

    /// <summary>
    /// A conflict with a named condition. The normal constructor for everything that is not a lost
    /// version slot.
    /// </summary>
    /// <param name="errorCode">A <c>DELTA_*</c> constant naming the condition.</param>
    /// <param name="message">Human-readable detail. Free to change — <paramref name="errorCode"/> is
    /// the part callers are meant to match on.</param>
    /// <param name="recovery">What the caller can do about it.</param>
    /// <param name="conflictingVersion">The concurrent commit responsible, when one is.</param>
    /// <param name="attemptedVersion">The version this commit was trying to take, when it got that far.</param>
    public DeltaConflictException(
        string errorCode,
        string message,
        ConflictRecovery recovery = ConflictRecovery.Replan,
        long? conflictingVersion = null,
        long? attemptedVersion = null)
        : base(message)
    {
        ErrorCode = errorCode;
        Recovery = recovery;
        ConflictingVersion = conflictingVersion;
        AttemptedVersion = attemptedVersion;
    }

    /// <summary>
    /// Raised when optimistic-concurrency validation aborts a transaction: a concurrent commit
    /// invalidated something this transaction read or removed. The message names the specific conflict.
    /// </summary>
    /// <remarks>
    /// Leaves <see cref="ErrorCode"/> null, so it cannot be matched on. Prefer the overload taking a
    /// code; this one remains because it is public API and because a caller constructing the exception
    /// itself has no code to give.
    /// </remarks>
    public DeltaConflictException(string message)
        : base(message)
    {
        Recovery = ConflictRecovery.Replan;
    }
}
