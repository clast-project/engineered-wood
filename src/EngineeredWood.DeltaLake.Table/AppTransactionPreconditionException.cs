// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// A <see cref="DeltaTransaction.RequireAppTransaction"/> precondition did not hold: the table records a
/// different version for the producer than the transaction required, so nothing was committed.
///
/// <para>Derives from <see cref="InvalidOperationException"/> — NOT
/// <see cref="DeltaConflictException"/> — deliberately, and the distinction is the whole point: the commit
/// loop RETRIES a conflict, and no amount of retrying makes an already-committed batch un-commit. A producer
/// told "conflict" would keep trying to write a batch the table already holds.</para>
///
/// <para>The dedicated type exists so a HOST can report this one case in its own vocabulary without matching
/// on message text. The base type alone cannot be told from any other invalid-operation failure raised while
/// committing, which leaves a caller choosing between string matching and mislabelling unrelated errors.</para>
/// </summary>
public class AppTransactionPreconditionException : InvalidOperationException
{
    /// <param name="appId">The producer whose precondition failed.</param>
    /// <param name="requiredVersion">The version that was NOT committed.</param>
    /// <param name="expectedPrevious">The version the table was required to record; null when the
    /// requirement was that it record none at all.</param>
    /// <param name="actualPrevious">The version the table actually records; null when it records none.</param>
    public AppTransactionPreconditionException(
        string message, string appId, long requiredVersion, long? expectedPrevious, long? actualPrevious)
        : base(message)
    {
        AppId = appId;
        RequiredVersion = requiredVersion;
        ExpectedPrevious = expectedPrevious;
        ActualPrevious = actualPrevious;
    }

    /// <summary>The producer whose precondition failed.</summary>
    public string AppId { get; }

    /// <summary>The version that was NOT committed.</summary>
    public long RequiredVersion { get; }

    /// <summary>The version the table was required to record — null = "none at all".</summary>
    public long? ExpectedPrevious { get; }

    /// <summary>The version the table actually records — null = none.</summary>
    public long? ActualPrevious { get; }
}
