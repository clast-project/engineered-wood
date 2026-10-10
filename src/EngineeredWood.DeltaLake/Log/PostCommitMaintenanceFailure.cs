// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Log;

/// <summary>The piece of post-commit maintenance a <see cref="PostCommitMaintenanceFailure"/> is about.</summary>
public enum PostCommitMaintenanceStep
{
    /// <summary>Writing the <c>_delta_log/&lt;version&gt;.crc</c> version checksum.</summary>
    VersionChecksum,

    /// <summary>
    /// Writing the checkpoint a commit on the checkpoint interval owes. Log cleanup is skipped when this
    /// fails, since it may delete only what a durable checkpoint covers.
    /// </summary>
    Checkpoint,

    /// <summary>
    /// Deleting log files, sidecars and checksums that a checkpoint has made redundant. One report per
    /// failure, so a pass that skips several files reports several.
    /// </summary>
    LogCleanup,
}

/// <summary>
/// A post-commit maintenance step that failed after its commit was already durable, and was therefore
/// swallowed rather than thrown. Delivered through <see cref="LogCommitOptions.OnPostCommitMaintenanceFailure"/>.
/// </summary>
/// <remarks>
/// <para><b>The write this follows succeeded.</b> A version checksum, an interval checkpoint and log cleanup
/// are all work the table does not need in order to be correct, and a later commit does each of them again —
/// so, as in delta-spark (whose post-commit hooks run inside a catch that logs and records
/// <c>delta.commit.hook.failure</c>), their failure costs a report rather than the caller's write.</para>
///
/// <para><b>Why it is worth listening for.</b> One failure is harmless. A checkpoint that fails EVERY time
/// is not: log cleanup only deletes what a checkpoint covers, so such a table never reclaims anything and
/// every open replays from its last good checkpoint, or from version 0. Without a listener nothing says so.</para>
///
/// <para>Cancellation is not reported here: a cancelled step still throws, as it always has.</para>
/// </remarks>
/// <param name="Step">Which step failed.</param>
/// <param name="Version">The version the step was for: the version a checksum or checkpoint describes, or
/// the checkpoint version a cleanup pass deletes below.</param>
/// <param name="Exception">What it failed with.</param>
public sealed record PostCommitMaintenanceFailure(
    PostCommitMaintenanceStep Step, long Version, Exception Exception)
{
    /// <summary>
    /// Delivers a failure to <paramref name="listener"/>, if there is one. A listener that throws is ignored:
    /// letting it escape would fail a write that has already committed, which is the outcome this whole
    /// channel exists to avoid.
    /// </summary>
    /// <remarks>
    /// Only a synchronous throw can be caught here. An <c>async</c> lambda passed as the listener is
    /// <c>async void</c>, and what it throws after an <c>await</c> never comes back through this call. The
    /// option documents that the listener must be synchronous; an awaitable callback type would let this
    /// catch cover it, but it would also put the listener's latency on every commit.
    /// </remarks>
    internal static void Report(
        Action<PostCommitMaintenanceFailure>? listener,
        PostCommitMaintenanceStep step, long version, Exception exception)
    {
        if (listener is null)
            return;
        try
        {
            listener(new PostCommitMaintenanceFailure(step, version, exception));
        }
        catch (Exception)
        {
            // See above: a broken listener must not turn into a failed write.
        }
    }
}
