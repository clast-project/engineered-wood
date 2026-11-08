// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>How two listings' views of one log file compare (<see cref="LogFileIdentity.Compare"/>).</summary>
internal enum LogFileMatch
{
    /// <summary>Certainly the same object: equal ETags, or, without them, equal size and a known, equal time.</summary>
    Same,

    /// <summary>Neither can be shown. Covers an unknown time, and a time that differs with nothing else to go on:
    /// a time is a weak signal, which copying or touching a file changes without replacing it.</summary>
    Unknown,

    /// <summary>Certainly a different object: a different path, size or ETag.</summary>
    Different,
}

/// <summary>
/// One log file as a listing reported it: enough to tell, on a later listing, whether the object at that path is
/// still the same one (#740).
/// </summary>
/// <remarks>
/// <para>A Delta log only ever grows, and a file in it is never rewritten, so a different object at a path a snapshot
/// was built from means the log itself was replaced: the table was dropped and re-created at the same location.
/// Version numbers cannot show that, because the new log can reach the same version or pass it.</para>
///
/// <para>The backend's ETag decides where both listings carry one. Without it, size and modification time do, and
/// the two are not equally strong. A different size is proof. A different time is not: it changes when a file is
/// touched or copied in place, and a committer refusing on it would fail a write that is already durable. So a time
/// can confirm a match but never prove a replacement; a reader rebuilds on it (that is always safe), and a committer
/// continues.</para>
/// </remarks>
internal readonly record struct LogFileIdentity(string Path, long Size, DateTimeOffset LastModified, string? ETag)
{
    public static LogFileIdentity Of(TableFileInfo file) =>
        new(file.Path, file.Size, file.LastModified, file.ETag);

    /// <summary>How <paramref name="other"/>, a later listing's view of the same path, compares with this one.</summary>
    public LogFileMatch Compare(LogFileIdentity other)
    {
        if (!string.Equals(Path, other.Path, StringComparison.Ordinal) || Size != other.Size)
            return LogFileMatch.Different;

        if (ETag is not null && other.ETag is not null)
            return string.Equals(ETag, other.ETag, StringComparison.Ordinal) ? LogFileMatch.Same : LogFileMatch.Different;

        // A tag on one side only means the two listings did not describe the object the same way, so they cannot be
        // compared. The time is no substitute: a coarse one can match across a same-size replacement (#746 review).
        if (ETag is not null || other.ETag is not null)
            return LogFileMatch.Unknown;

        return LastModified != default && LastModified == other.LastModified ? LogFileMatch.Same : LogFileMatch.Unknown;
    }
}

/// <summary>
/// Every log file that named a snapshot's version in the listing it was built from: the commit, and each checkpoint
/// file at that version (#740, #746 review).
/// </summary>
/// <param name="Files">The files, ordered by path.</param>
/// <param name="HasCommit">Whether the commit file was among them. Log cleanup can leave a checkpoint with no commit
/// beside it; a commit can never appear at that version afterwards, because a commit is written before any
/// checkpoint of it and is never written again.</param>
internal sealed record LogVersionAnchor(IReadOnlyList<LogFileIdentity> Files, bool HasCommit)
{
    /// <summary>
    /// The <c>commitInfo.txnId</c> of the commit at this version, when the snapshot was built by applying that commit
    /// and it carries one (this library writes one on every commit, as delta-spark does); otherwise null.
    /// </summary>
    /// <remarks>What decides when the listing cannot (#755 follow-up): without ETags, a re-created commit of exactly
    /// the old size shows only a different modification time, and a table re-created with the same data produces
    /// exactly that (measured: 478 bytes both times). A commit is never rewritten, so a different id, or none, at the
    /// same path is a different commit.</remarks>
    public string? CommitTxnId { get; init; }
}
