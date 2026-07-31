// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// The row-level DML boundary key: per data file, keyed by its <c>add.path</c> exactly as the snapshot
/// records it, the ABSOLUTE in-file positions selected. Every row-level entry point takes one —
/// <see cref="DeltaTable.DeleteRowsAsync"/>, <see cref="DeltaTable.UpdateRowsAsync(RowSelection, Func{string, IReadOnlyList{RecordBatch}, IReadOnlyList{Int64Array}, IReadOnlyList{RecordBatch}}, CancellationToken)"/>,
/// <see cref="DeltaTable.ReadRowsAsync"/>, <see cref="DeltaTransaction.StageRowDeletesAsync"/> — so there is
/// one spelling of "which row is this" at the boundary rather than one per method.
///
/// <para><b>Positions are ABSOLUTE</b>: the parquet row index, counting rows a deletion vector hides. That is
/// what makes repeated deletes against one file compose, and it is the same number
/// <c>_metadata.row_index</c> carries on the read side (and Spark's column of that name).</para>
///
/// <para><b>Why a path and not a file ordinal.</b> A path-sorted ordinal is only meaningful in the snapshot it
/// was minted against: a concurrent append inserts a path into the sort order and renumbers everything after
/// it, so a stale-but-in-range ordinal silently addresses a DIFFERENT file and an out-of-range one silently
/// selects nothing. Resolving the ordinal HERE — at construction, against the snapshot the addresses came
/// from (<see cref="FromRowAddresses"/>) — means a stale address fails where the caller still holds the
/// context to explain it, instead of four <c>continue</c>s deep inside the DML.</para>
/// </summary>
public sealed class RowSelection
{
    /// <summary>The default prefix of the locator column pair, matching Spark's <c>_metadata</c> struct.</summary>
    public const string DefaultMetadataPrefix = "_metadata.";

    /// <summary>The locator column carrying the file's <c>add.path</c>, after the metadata prefix.</summary>
    public const string FilePathColumnSuffix = "file_path";

    /// <summary>The locator column carrying the row's ABSOLUTE in-file position, after the metadata
    /// prefix.</summary>
    public const string RowIndexColumnSuffix = "row_index";

    private static readonly long[] NoPositions = [];

    private readonly Dictionary<string, IReadOnlyCollection<long>> _byPath;

    private RowSelection(Dictionary<string, IReadOnlyCollection<long>> byPath)
    {
        _byPath = byPath;
        long total = 0;
        foreach (var positions in byPath.Values)
            total += positions.Count;
        TotalPositions = total;
    }

    /// <summary>
    /// The selection stated directly: absolute in-file positions per <c>add.path</c>. The path must be the
    /// snapshot's own — <see cref="Actions.AddFile.Path"/> / <see cref="PlannedFile.File"/>'s, or
    /// <see cref="WrittenDataFile.RelativePath"/> for files not yet committed — not a decoded or absolutized
    /// form of it.
    /// </summary>
    public static RowSelection ByPath(IReadOnlyDictionary<string, IReadOnlyCollection<long>> positionsByPath)
    {
        if (positionsByPath is null)
            throw new ArgumentNullException(nameof(positionsByPath));

        var byPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var kvp in positionsByPath)
        {
            if (kvp.Key is null)
                throw new ArgumentException("positionsByPath contains a null path.", nameof(positionsByPath));
            if (kvp.Value is null)
                throw new ArgumentException(
                    $"positionsByPath['{kvp.Key}'] is null.", nameof(positionsByPath));
            if (kvp.Value.Count == 0)
                continue;
            // Deduplicate: a caller joining its own scan output can legitimately produce a position twice, and
            // a DV union would silently absorb it while a copy-on-write rewrite would count it twice.
            byPath[kvp.Key] = Dedupe(kvp.Value);
        }
        return new RowSelection(byPath);
    }

    /// <summary>
    /// The selection read straight back off batches carrying <c>DeltaRowMetadata.Locator</c>: the
    /// <c>{prefix}file_path</c> (Utf8) and <c>{prefix}row_index</c> (Int64) pair. This is the loop the read
    /// and DML sides close — a host scans with <c>DeltaRowMetadata.Locator</c>, filters the batches
    /// with its own engine, and hands the survivors straight back.
    /// </summary>
    public static RowSelection FromLocatorColumns(
        IEnumerable<RecordBatch> batches, string metadataPrefix = DefaultMetadataPrefix)
    {
        if (batches is null)
            throw new ArgumentNullException(nameof(batches));
        if (metadataPrefix is null)
            throw new ArgumentNullException(nameof(metadataPrefix));

        string pathName = metadataPrefix + FilePathColumnSuffix;
        string indexName = metadataPrefix + RowIndexColumnSuffix;

        var accumulated = new Dictionary<string, HashSet<long>>(StringComparer.Ordinal);
        foreach (var batch in batches)
        {
            if (batch is null)
                continue;
            var (paths, indexes) = LocatorColumns(batch, pathName, indexName);
            for (int i = 0; i < batch.Length; i++)
            {
                if (paths.IsNull(i) || indexes.IsNull(i))
                    continue;
                string path = paths.GetString(i);
                if (!accumulated.TryGetValue(path, out var set))
                    accumulated[path] = set = [];
                set.Add(indexes.GetValue(i)!.Value);
            }
        }

        var byPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var kvp in accumulated)
            if (kvp.Value.Count > 0)
                byPath[kvp.Key] = kvp.Value;
        return new RowSelection(byPath);
    }

    /// <summary>
    /// The ordinal adapter, for a host whose own rowid is a single <c>BIGINT</c> — a packed
    /// <see cref="TransientRowAddress"/>. Resolves each address's file ordinal to its <c>add.path</c> NOW,
    /// against <paramref name="snapshot"/>, which MUST be the snapshot the addresses were minted against
    /// (<see cref="DeltaTransaction.Snapshot"/>, or whatever the read was pinned to).
    ///
    /// <para>An address whose ordinal falls outside that snapshot's active set is stale.
    /// <see cref="StaleAddressPolicy.Throw"/> — the default, and the recommended setting — reports it here,
    /// where the caller still knows which of its own rows produced it;
    /// <see cref="StaleAddressPolicy.Skip"/> drops it, reproducing the historical silent behaviour of the
    /// old <c>ByRowIds</c> methods. Neither can detect an ordinal that is stale but still IN range: that is
    /// exactly why the snapshot is a required argument rather than an implied <c>CurrentSnapshot</c>.</para>
    /// </summary>
    public static RowSelection FromRowAddresses(
        IReadOnlyCollection<long> addresses,
        Snapshot.Snapshot snapshot,
        StaleAddressPolicy policy = StaleAddressPolicy.Throw)
    {
        if (addresses is null)
            throw new ArgumentNullException(nameof(addresses));
        if (snapshot is null)
            throw new ArgumentNullException(nameof(snapshot));

        var byOrdinal = new Dictionary<int, IReadOnlyCollection<long>>();
        foreach (long address in addresses)
        {
            int ordinal = TransientRowAddress.FileOrdinal(address);
            if (!byOrdinal.TryGetValue(ordinal, out var set))
                byOrdinal[ordinal] = set = new HashSet<long>();
            ((HashSet<long>)set).Add(TransientRowAddress.Position(address));
        }
        return FromOrdinals(byOrdinal, snapshot, policy, nameof(addresses));
    }

    /// <summary>
    /// The ordinal adapter for callers already holding positions grouped by file ordinal — the lower-layer
    /// primitives' key (<see cref="DeltaTable.ComputeDeletionVectorActionsAsync"/>,
    /// <see cref="DeltaTable.RebaseDvDmlActionsAsync"/>), which keep it because they are the hand-rolled
    /// commit-loop surface rather than the DML boundary.
    /// </summary>
    internal static RowSelection FromOrdinals(
        IReadOnlyDictionary<int, IReadOnlyCollection<long>> positionsByOrdinal,
        Snapshot.Snapshot snapshot,
        StaleAddressPolicy policy,
        string paramName)
    {
        var ordered = DeltaTable.OrderedActiveFiles(snapshot);
        var byPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);

        foreach (var kvp in positionsByOrdinal)
        {
            int ordinal = kvp.Key;
            if (ordinal < 0 || ordinal >= ordered.Count)
            {
                if (policy == StaleAddressPolicy.Skip)
                    continue;
                throw new ArgumentException(
                    $"Row address file ordinal {ordinal} is outside version {snapshot.Version}'s active set "
                    + $"of {ordered.Count} file(s), so the address is stale — it was minted against a "
                    + "different snapshot. Pass the snapshot the addresses were read against, or "
                    + "StaleAddressPolicy.Skip to drop stale addresses.",
                    paramName);
            }
            if (kvp.Value is null || kvp.Value.Count == 0)
                continue;

            string path = ordered[ordinal].Path;
            // Two ordinals cannot map to one path within a snapshot, so no merge is needed — but a caller
            // could hand ByPath-shaped duplicates in, so dedupe on the same terms as ByPath.
            byPath[path] = Dedupe(kvp.Value);
        }
        return new RowSelection(byPath);
    }

    /// <summary>The paths this selection names. Never contains a path with no positions.</summary>
    public IReadOnlyCollection<string> Paths => _byPath.Keys;

    /// <summary>
    /// The absolute in-file positions selected in <paramref name="path"/>, or an empty collection when the
    /// selection does not name it.
    /// </summary>
    public IReadOnlyCollection<long> PositionsFor(string path)
    {
        if (path is null)
            throw new ArgumentNullException(nameof(path));
        return _byPath.TryGetValue(path, out var positions) ? positions : NoPositions;
    }

    /// <summary>The total number of positions across every path — the upper bound on rows this selection can
    /// affect (a position a deletion vector already hides affects nothing).</summary>
    public long TotalPositions { get; }

    /// <summary>True when the selection names no rows at all.</summary>
    public bool IsEmpty => _byPath.Count == 0;

    internal IEnumerable<KeyValuePair<string, IReadOnlyCollection<long>>> Entries => _byPath;

    /// <summary>The positions as a set, for the per-row membership tests the DML paths run.</summary>
    internal static HashSet<long> AsSet(IReadOnlyCollection<long> positions) =>
        positions as HashSet<long> ?? [.. positions];

    // Always copies, never aliases the caller's collection: a RowSelection is immutable once built, and a
    // caller holding a mutable set it also passed in could otherwise change what the DML addresses after the
    // fact. (No capacity ctor here — HashSet<T>(int) does not exist on netstandard2.0.)
    private static IReadOnlyCollection<long> Dedupe(IReadOnlyCollection<long> positions)
    {
        var deduped = new HashSet<long>();
        foreach (long p in positions)
        {
            if (p < 0)
                throw new ArgumentException($"Row position {p} is negative.", nameof(positions));
            deduped.Add(p);
        }
        return deduped;
    }

    private static (StringArray Paths, Int64Array Indexes) LocatorColumns(
        RecordBatch batch, string pathName, string indexName)
    {
        int pathIdx = batch.Schema.GetFieldIndex(pathName);
        if (pathIdx < 0)
            throw new ArgumentException(
                $"Batch has no locator column '{pathName}'. Read with "
                + "DeltaReadOptions { Metadata = DeltaRowMetadata.Locator } (and the same MetadataPrefix).",
                nameof(batch));
        int indexIdx = batch.Schema.GetFieldIndex(indexName);
        if (indexIdx < 0)
            throw new ArgumentException(
                $"Batch has no locator column '{indexName}'. Read with "
                + "DeltaReadOptions { Metadata = DeltaRowMetadata.Locator } (and the same MetadataPrefix).",
                nameof(batch));

        if (batch.Column(pathIdx) is not StringArray paths)
            throw new ArgumentException(
                $"Locator column '{pathName}' must be Utf8.", nameof(batch));
        if (batch.Column(indexIdx) is not Int64Array indexes)
            throw new ArgumentException(
                $"Locator column '{indexName}' must be Int64.", nameof(batch));
        return (paths, indexes);
    }
}

/// <summary>
/// What <see cref="RowSelection.FromRowAddresses"/> does with an address whose file ordinal is not in the
/// snapshot's active set — i.e. one minted against a different version.
/// </summary>
public enum StaleAddressPolicy
{
    /// <summary>Throw, naming the ordinal and the size of the active set. The default, and the recommended
    /// setting: a stale address is a bug in the caller's version pinning, and it is cheaper to see it here
    /// than as a wrong answer later.</summary>
    Throw,

    /// <summary>Skip the address. The historical behaviour of the <c>ByRowIds</c> methods this type
    /// replaces, available explicitly for a caller that genuinely tolerates it.</summary>
    Skip,
}
