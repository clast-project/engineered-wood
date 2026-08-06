// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// What versions a table's <c>_delta_log</c> holds, from one classified pass over the directory.
/// </summary>
/// <remarks>
/// <para>Answering "what versions are there?" from the commit files alone is wrong on any table whose
/// log has been cleaned: metadata cleanup deletes commit files and keeps the checkpoint that subsumes
/// them, so the newest version can have no commit file at all, and the oldest surviving commit file
/// can name a version nothing can reconstruct.</para>
///
/// <para>The three lists answer different questions and are deliberately not interchangeable — see
/// each one. <see cref="ReadableVersions"/> is the one a history view, a version picker or a
/// time-travel bound wants.</para>
/// </remarks>
public sealed class LogVersions
{
    internal LogVersions(
        long latestVersion,
        IReadOnlyList<long> commitVersions,
        IReadOnlyList<long> checkpointVersions,
        IReadOnlyList<long> readableVersions)
    {
        LatestVersion = latestVersion;
        CommitVersions = commitVersions;
        CheckpointVersions = checkpointVersions;
        ReadableVersions = readableVersions;
    }

    /// <summary>
    /// The newest version the log names, from commits and checkpoints alike, or <c>-1</c> when the
    /// table has neither.
    /// </summary>
    public long LatestVersion { get; }

    /// <summary>
    /// Versions with a commit file (<c>&lt;n&gt;.json</c>), ascending. This is a statement about which
    /// FILES exist, not about what can be read: a commit whose predecessors were cleaned away, with no
    /// checkpoint covering them, appears here and cannot be built.
    /// </summary>
    public IReadOnlyList<long> CommitVersions { get; }

    /// <summary>
    /// Versions carrying a checkpoint this implementation can actually bootstrap from, ascending.
    /// </summary>
    /// <remarks>
    /// Excludes a multi-part checkpoint whose parts are incomplete — a writer that died midway leaves
    /// a prefix, and reading it would silently drop the files in the missing parts. Also excludes
    /// checkpoint forms this version does not decode, so this is what THIS reader can use rather than
    /// what the directory contains.
    /// </remarks>
    public IReadOnlyList<long> CheckpointVersions { get; }

    /// <summary>
    /// Versions at which a snapshot can actually be built, ascending. Usually contiguous, but not
    /// guaranteed to be: a log cleaned in the middle leaves readable ranges either side of the hole.
    /// </summary>
    /// <remarks>
    /// A version is readable when replay can cover every version up to it — either from <c>0</c> with
    /// every commit present, or from a usable checkpoint with every commit after it present. This is
    /// the honest answer to "what can I open?", and it is a SUBSET of
    /// <see cref="CommitVersions"/> ∪ <see cref="CheckpointVersions"/>.
    /// </remarks>
    public IReadOnlyList<long> ReadableVersions { get; }

    /// <summary>
    /// Computes the three views from one directory listing. The commit versions must be ascending,
    /// which <see cref="LogListing"/> guarantees.
    /// </summary>
    internal static LogVersions FromListing(LogListing listing)
    {
        var commits = listing.CommitVersions;
        var checkpoints = listing.UsableCheckpointVersionsAscending().ToList();

        // Maximal runs of consecutive commit versions. Replay can cross a run but never a gap, so a
        // run is exactly how far one starting point can reach.
        var runs = new List<(long Start, long End)>();
        foreach (long version in commits)
        {
            if (runs.Count > 0 && runs[^1].End + 1 == version)
                runs[^1] = (runs[^1].Start, version);
            else
                runs.Add((version, version));
        }

        var readable = new SortedSet<long>();

        void AddThrough(long from, long through)
        {
            for (long v = from; v <= through; v++)
                readable.Add(v);
        }

        // A full replay from the beginning, available only while commit 0 itself survives.
        if (runs.Count > 0 && runs[0].Start == 0)
            AddThrough(0, runs[0].End);

        // Every usable checkpoint is readable on its own, and carries replay as far as the commits
        // immediately after it run unbroken. Both sequences ascend, so one pointer walks the runs.
        int r = 0;
        foreach (long anchor in checkpoints)
        {
            readable.Add(anchor);

            long next = anchor + 1;
            while (r < runs.Count && runs[r].End < next)
                r++;

            if (r < runs.Count && runs[r].Start <= next)
                AddThrough(next, runs[r].End);
        }

        return new LogVersions(
            listing.LatestVersion, commits.ToList(), checkpoints, readable.ToList());
    }
}
