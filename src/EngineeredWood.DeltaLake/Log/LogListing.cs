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

    /// <summary>
    /// Version to the path of its <c>&lt;version&gt;.checkpoint.&lt;uuid&gt;.{json,parquet}</c> — the two
    /// bodies PROTOCOL.md defines for a UUID-named V2 checkpoint, both of which are decoded.
    /// </summary>
    public Dictionary<long, string> V2Checkpoints { get; } = [];

    /// <summary>
    /// Version to the path of a checkpoint this reader RECOGNISES but cannot decode — a UUID-named V2
    /// checkpoint whose body is neither of the two the spec defines.
    /// </summary>
    /// <remarks>
    /// Deliberately kept out of <see cref="V2Checkpoints"/> and out of checkpoint SELECTION: a table
    /// carrying one may still be perfectly readable from an older checkpoint or from its commits, and
    /// refusing it would be wrong. Recorded only so that a replay which fails for want of the versions
    /// this checkpoint covers can name the real cause instead of blaming the log.
    /// </remarks>
    public Dictionary<long, string> UndecodableCheckpoints { get; } = [];

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
        // <version>.checkpoint.<uuid>.{json,parquet} — a UUID-named V2 checkpoint, in either of the two
        // bodies the spec defines. A UUID name is the only 2-part suffix: classic is 1 part and
        // multi-part is 3.
        else if (suffix is [_, "json" or "parquet"])
        {
            AddV2Checkpoint(version, fileName);
        }
        // <version>.checkpoint.<uuid>.<anything else> — named like a UUID-named V2 checkpoint, in a body
        // the spec does not define and this reader has no decoder for. Recognised rather than ignored so
        // that a replay which fails for want of the versions it covers can name it, instead of blaming
        // log retention for a decoding limitation.
        else if (suffix.Length == 2)
        {
            UndecodableCheckpoints[version] = DeltaVersion.LogPrefix + fileName;
        }
    }

    /// <summary>
    /// Records a UUID-named V2 checkpoint, keeping one deterministic choice per version.
    /// </summary>
    /// <remarks>
    /// Two writers racing to checkpoint the same version produce two UUID names, and both are valid —
    /// the spec has them carrying the same information, so a reader may use either. Which one arrives
    /// first is filesystem-dependent, though, so the choice is pinned rather than left to listing order:
    /// a JSON body wins over a Parquet one (it is read without a Parquet footer round-trip), and
    /// otherwise the ordinally smallest name wins. Snapshots then do not vary between two reads of an
    /// unchanged table. The others are kept too, for when the chosen one cannot be read.
    /// </remarks>
    private void AddV2Checkpoint(long version, string fileName)
    {
        string path = DeltaVersion.LogPrefix + fileName;
        if (!V2Checkpoints.TryGetValue(version, out string? existing) || CompareV2Preference(path, existing) < 0)
            V2Checkpoints[version] = path;

        if (!_allV2Checkpoints.TryGetValue(version, out var all))
            _allV2Checkpoints[version] = all = [];
        all.Add(path);
    }

    private readonly Dictionary<long, List<string>> _allV2Checkpoints = [];

    /// <summary>Negative when <paramref name="a"/> is the V2 checkpoint to prefer over <paramref name="b"/>.</summary>
    private static int CompareV2Preference(string a, string b)
    {
        bool aIsJson = a.EndsWith(".json", StringComparison.OrdinalIgnoreCase);
        if (aIsJson != b.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            return aIsJson ? -1 : 1;
        return string.CompareOrdinal(a, b);
    }

    /// <summary>
    /// Every UUID-named V2 checkpoint at this version, the one in <see cref="V2Checkpoints"/> first.
    /// </summary>
    public IEnumerable<string> V2CheckpointsInPreferenceOrder(long version)
    {
        if (!_allV2Checkpoints.TryGetValue(version, out var all))
            return [];

        var sorted = new List<string>(all);
        sorted.Sort(CompareV2Preference);
        return sorted;
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

    /// <summary>
    /// How a replay of <c>[from, through]</c> would use this log: the one compaction file it applies, if
    /// any, and whether the commit files around it supply every other version.
    /// </summary>
    /// <remarks>
    /// <para>The one place that decides which compaction file a replay uses — the longest that lies
    /// wholly inside the range — so that the snapshot build applies the same plan it judged a checkpoint
    /// by. Judging with a looser rule than replay applies is how a checkpoint got chosen whose replay
    /// then crossed a hole: a compaction file that starts before the replay does, or ends past the
    /// target, covers versions the replay never takes from it.</para>
    ///
    /// <para>Complete is a statement about the LISTING. A commit that is listed but cannot be read still
    /// leaves a hole, which the replay reports when it gets there.</para>
    /// </remarks>
    public ReplayPlan PlanReplay(long from, long through)
    {
        (long Start, long End, string Path)? best = null;
        foreach (var cf in CompactedRanges)
        {
            if (cf.Start >= from && cf.End <= through &&
                (best is null || (cf.End - cf.Start) > (best.Value.End - best.Value.Start)))
            {
                best = cf;
            }
        }

        bool complete = best is { } b
            ? AllCommitsListed(from, b.Start - 1) && AllCommitsListed(b.End + 1, through)
            : AllCommitsListed(from, through);
        return new ReplayPlan(best, complete);
    }

    /// <summary>Whether a commit file is listed for every version in <c>[from, through]</c>.</summary>
    private bool AllCommitsListed(long from, long through) =>
        from > through || CountCommitsBelow(through + 1) - CountCommitsBelow(from) == through - from + 1;

    /// <summary>The number of listed commits below <paramref name="version"/>; the list is sorted.</summary>
    private int CountCommitsBelow(long version)
    {
        int i = CommitVersions.BinarySearch(version);
        return i >= 0 ? i : ~i;
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
        foreach (int total in CompleteMultiPartCounts(version))
            return total;

        return null;
    }

    /// <summary>
    /// The part count of every COMPLETE multi-part checkpoint at this version, smallest first, so that
    /// the choice does not depend on listing order.
    /// </summary>
    public IEnumerable<int> CompleteMultiPartCounts(long version)
    {
        if (!MultiPartCheckpoints.TryGetValue(version, out var byTotal))
            return [];

        var totals = new List<int>();
        foreach (var (total, seen) in byTotal)
        {
            if (seen.Count == total)
                totals.Add(total);
        }

        totals.Sort();
        return totals;
    }

    /// <summary>
    /// A multi-part checkpoint inside <c>[from, through]</c> that has SOME of its parts but not all, and
    /// which nothing else at that version covers — or null when there is none.
    /// </summary>
    /// <remarks>
    /// The other way a replay ends up with a hole nothing accounts for. A writer that died midway leaves
    /// a prefix of the parts, and that prefix is skipped for bootstrapping because loading it would
    /// silently drop the files in the parts that never landed. If the commits it was meant to subsume
    /// have since been cleaned away, the history really is gone — so this is still a truncated log rather
    /// than an unsupported format — but the torn checkpoint is the cause, and saying so is the difference
    /// between "your retention is too aggressive" and "a checkpoint write did not finish".
    /// </remarks>
    public (long Version, int Present, int Declared)? TornMultiPartCheckpoint(long from, long through)
    {
        foreach (var (version, byTotal) in MultiPartCheckpoints)
        {
            if (version < from || version > through)
                continue;
            if (CompleteMultiPartCount(version) is not null)
                continue;
            if (ClassicCheckpoints.Contains(version) || V2Checkpoints.ContainsKey(version))
                continue;

            // The most nearly complete declaration, when a version somehow carries more than one.
            int bestTotal = 0, bestPresent = -1;
            foreach (var (total, seen) in byTotal)
            {
                if (seen.Count > bestPresent)
                    (bestTotal, bestPresent) = (total, seen.Count);
            }

            return (version, bestPresent, bestTotal);
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

/// <summary>What <see cref="LogListing.PlanReplay"/> decided for one range.</summary>
/// <param name="Compacted">The compaction file the replay applies, or null when it reads commits only.</param>
/// <param name="IsComplete">Whether the listing supplies every version in the range.</param>
internal readonly record struct ReplayPlan((long Start, long End, string Path)? Compacted, bool IsComplete);
