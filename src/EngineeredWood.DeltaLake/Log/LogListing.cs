// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// One pass over <c>_delta_log</c>, classified. Building a snapshot needs four different views of that
/// directory — the latest version, the commits to replay, the newest checkpoint, the compaction files —
/// and each used to walk it separately, which on an object store is four round-trips to answer questions
/// about the same set of names.
/// </summary>
/// <remarks>
/// Reading them from one listing also makes the four views consistent with each other. Separate listings
/// could disagree about a concurrent commit: the target version came from one moment and the replay set
/// from another, and only the <c>v &lt;= targetVersion</c> filter kept that from mattering.
/// </remarks>
internal sealed class LogListing
{
    private const string CheckpointMarker = ".checkpoint.";

    /// <summary>Commit versions present, ascending. The listing order is filesystem-dependent.</summary>
    public List<long> CommitVersions { get; } = [];

    /// <summary>Versions with a <c>&lt;version&gt;.checkpoint.parquet</c>.</summary>
    public HashSet<long> ClassicCheckpoints { get; } = [];

    /// <summary>Version to the path of its <c>&lt;version&gt;.checkpoint.&lt;uuid&gt;.json</c>.</summary>
    public Dictionary<long, string> V2Checkpoints { get; } = [];

    /// <summary>Version to declared part count to the part numbers actually present.</summary>
    public Dictionary<long, Dictionary<int, HashSet<int>>> MultiPartCheckpoints { get; } = [];

    /// <summary>Compaction files, as (start, end, path).</summary>
    public List<(long Start, long End, string Path)> CompactedRanges { get; } = [];

    /// <summary>
    /// The newest version the log names, or -1. A checkpoint names one just as a commit does — cleanup
    /// deletes commit files and keeps the checkpoint that subsumes them, so on an idle table the newest
    /// version may have no commit file left at all.
    /// </summary>
    public long LatestVersion { get; private set; } = -1;

    public static async ValueTask<LogListing> ReadAsync(
        ITableFileSystem fileSystem, CancellationToken cancellationToken = default)
    {
        var listing = new LogListing();

        await foreach (var file in fileSystem.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            listing.Add(Path.GetFileName(file.Path), file.Path);
        }

        listing.CommitVersions.Sort();
        return listing;
    }

    private void Add(string fileName, string path)
    {
        if (DeltaVersion.TryParseCommitVersion(fileName, out long commit))
        {
            CommitVersions.Add(commit);
            Observe(commit);
            return;
        }

        if (DeltaVersion.TryParseCompactedRange(fileName, out long start, out long end))
        {
            // A compaction file summarizes commits that must exist in their own right, so it names no
            // version of its own.
            CompactedRanges.Add((start, end, path));
            return;
        }

        if (!DeltaVersion.TryParseCheckpointVersion(fileName, out long version))
            return;

        Observe(version);

        string[] suffix = fileName[(fileName.IndexOf(CheckpointMarker, StringComparison.OrdinalIgnoreCase)
            + CheckpointMarker.Length)..].Split('.');

        // <version>.checkpoint.parquet
        if (suffix is ["parquet"])
        {
            ClassicCheckpoints.Add(version);
        }
        // <version>.checkpoint.<part>.<total>.parquet
        else if (suffix is [var p, var t, "parquet"] &&
                 int.TryParse(p, out int part) && int.TryParse(t, out int total) && total > 0)
        {
            if (!MultiPartCheckpoints.TryGetValue(version, out var byTotal))
                MultiPartCheckpoints[version] = byTotal = [];
            if (!byTotal.TryGetValue(total, out var seen))
                byTotal[total] = seen = [];
            seen.Add(part);
        }
        // <version>.checkpoint.<uuid>.json. The parquet-bodied V2 form is deliberately not claimed:
        // CheckpointReader only decodes NDJSON, so claiming it would just fail the read.
        else if (suffix is [_, "json"])
        {
            V2Checkpoints[version] = DeltaVersion.LogPrefix + fileName;
        }
    }

    private void Observe(long version)
    {
        if (version > LatestVersion)
            LatestVersion = version;
    }

    /// <summary>Commit versions in <c>[from, through]</c>, ascending.</summary>
    public IEnumerable<long> CommitsInRange(long from, long through)
    {
        foreach (long v in CommitVersions)
        {
            if (v >= from && v <= through)
                yield return v;
        }
    }

    /// <summary>Every version that carries a checkpoint of any form, descending.</summary>
    /// <remarks>
    /// Includes multi-part versions whose parts are INCOMPLETE — callers that intend to read the
    /// checkpoint must confirm with <see cref="CompleteMultiPartCount"/>. This exists as the candidate
    /// order for <c>SelectLatestCheckpoint</c>, which does exactly that as it walks.
    /// </remarks>
    public IEnumerable<long> CheckpointVersionsDescending()
    {
        var all = new SortedSet<long>(ClassicCheckpoints);
        all.UnionWith(V2Checkpoints.Keys);
        all.UnionWith(MultiPartCheckpoints.Keys);
        return all.Reverse();
    }

    /// <summary>
    /// The part count of a COMPLETE multi-part checkpoint at this version, or null when there is none.
    /// </summary>
    /// <remarks>
    /// A writer that died midway leaves a prefix of the parts, and bootstrapping from that would
    /// silently drop the files in the missing ones — so a multi-part checkpoint counts only when every
    /// part is present. One version can declare more than one part count (a re-checkpoint that split
    /// differently); any complete one will do.
    /// </remarks>
    public int? CompleteMultiPartCount(long version)
    {
        if (!MultiPartCheckpoints.TryGetValue(version, out var byTotal))
            return null;

        foreach (var (total, seen) in byTotal)
        {
            if (seen.Count == total)
                return total;
        }

        return null;
    }

    /// <summary>Every version carrying a checkpoint that can actually be read, ascending.</summary>
    public IEnumerable<long> UsableCheckpointVersionsAscending()
    {
        var all = new SortedSet<long>(ClassicCheckpoints);
        all.UnionWith(V2Checkpoints.Keys);

        foreach (long version in MultiPartCheckpoints.Keys)
        {
            if (CompleteMultiPartCount(version) is not null)
                all.Add(version);
        }

        return all;
    }
}
