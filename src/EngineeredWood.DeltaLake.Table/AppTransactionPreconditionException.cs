// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// A <see cref="DeltaTransaction.RequireAppTransaction"/> precondition did not hold: the table records
/// something other than what the transaction required for the producer, so nothing was committed.
///
/// <para>Derives from <see cref="InvalidOperationException"/> — NOT <see cref="DeltaConflictException"/> —
/// deliberately, and the distinction is the whole point: the commit loop RETRIES a conflict, and no amount
/// of retrying makes an already-committed batch un-commit. A producer told "conflict" would keep trying to
/// write a batch the table already holds.</para>
///
/// <para>The dedicated type exists so a HOST can report this one case in its own vocabulary without matching
/// on message text. The base type alone cannot be told from any other invalid-operation failure raised while
/// committing, which leaves a caller choosing between string matching and mislabelling unrelated errors.</para>
///
/// <para><b>Why this is an exception even for <see cref="AppTransactionPreconditionKind.NotApplied"/>,</b>
/// where "already applied" is an outcome a replaying producer expects: a transaction's scope is wider than
/// its producer — a delete, a schema change or staged actions can be committed alongside the <c>txn</c>
/// record — so the failure means NOTHING the caller staged happened, which must not be silent. A producer
/// that expects to hit this routinely should ask
/// <see cref="DeltaTransaction.IsAppTransactionApplied"/> BEFORE staging, leaving the throw for the race it
/// cannot rule out.</para>
/// </summary>
public class AppTransactionPreconditionException : InvalidOperationException
{
    /// <param name="message">The failure, phrased for a human.</param>
    /// <param name="appId">The producer whose precondition failed.</param>
    /// <param name="requiredVersion">The version that was NOT committed.</param>
    /// <param name="precondition">What the transaction required the table to already record.</param>
    /// <param name="actualPrevious">The version the table actually records; null when it records none —
    /// never a sentinel, which is why it is nullable.</param>
    public AppTransactionPreconditionException(
        string message,
        string appId,
        long requiredVersion,
        AppTransactionPrecondition precondition,
        long? actualPrevious)
        : base(message)
    {
        AppId = appId;
        RequiredVersion = requiredVersion;
        Precondition = precondition;
        ActualPrevious = actualPrevious;
    }

    /// <summary>The producer whose precondition failed.</summary>
    public string AppId { get; }

    /// <summary>The version that was NOT committed.</summary>
    public long RequiredVersion { get; }

    /// <summary>What the transaction required the table to already record.</summary>
    public AppTransactionPrecondition Precondition { get; }

    /// <summary>The version the table actually records — null = none at all.</summary>
    public long? ActualPrevious { get; }
}
