// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Provides time-travel operations for navigating and manipulating an Iceberg table's snapshot history.
/// </summary>
public static class TimeTravel
{
    /// <summary>
    /// Returns metadata with the current snapshot set to the specified snapshot ID.
    /// </summary>
    public static TableMetadata AtSnapshot(TableMetadata metadata, long snapshotId)
    {
        if (!metadata.Snapshots.Any(s => s.SnapshotId == snapshotId))
            throw new ArgumentException($"Snapshot not found: {snapshotId}");

        return metadata with { CurrentSnapshotId = snapshotId };
    }

    /// <summary>
    /// Returns metadata with the current snapshot set to the latest snapshot at or before the given timestamp.
    /// </summary>
    public static TableMetadata AsOfTimestamp(TableMetadata metadata, long timestampMs)
    {
        var snapshot = metadata.Snapshots
            .Where(s => s.TimestampMs <= timestampMs)
            .OrderByDescending(s => s.TimestampMs)
            .FirstOrDefault()
            ?? throw new ArgumentException(
                $"No snapshot found at or before timestamp: {timestampMs}");

        return metadata with { CurrentSnapshotId = snapshot.SnapshotId };
    }

    /// <summary>
    /// Walks the parent chain from the current snapshot and returns the full snapshot history.
    /// </summary>
    public static IReadOnlyList<Snapshot> SnapshotHistory(TableMetadata metadata)
    {
        if (metadata.CurrentSnapshotId is null)
            return [];

        var byId = metadata.Snapshots.ToDictionary(s => s.SnapshotId);
        var history = new List<Snapshot>();
        long? current = metadata.CurrentSnapshotId;

        while (current is not null && byId.TryGetValue(current.Value, out var snapshot))
        {
            history.Add(snapshot);
            current = snapshot.ParentSnapshotId;
        }

        return history;
    }

    /// <summary>
    /// Rolls back the table to a previous snapshot, updating the current snapshot and appending to the snapshot log.
    /// </summary>
    public static TableMetadata Rollback(TableMetadata metadata, long snapshotId)
    {
        var snapshot = metadata.Snapshots.FirstOrDefault(s => s.SnapshotId == snapshotId)
            ?? throw new ArgumentException($"Snapshot not found: {snapshotId}");

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var snapshotLog = metadata.SnapshotLog.ToList();
        snapshotLog.Add(new SnapshotLogEntry(now, snapshotId));

        return metadata with
        {
            CurrentSnapshotId = snapshotId,
            LastUpdatedMs = now,
            SnapshotLog = snapshotLog,
        };
    }

    /// <summary>
    /// Cherry-picks a snapshot's manifest list into a new snapshot on the current branch.
    /// </summary>
    public static TableMetadata CherryPick(TableMetadata metadata, long snapshotId)
    {
        var snapshot = metadata.Snapshots.FirstOrDefault(s => s.SnapshotId == snapshotId)
            ?? throw new ArgumentException($"Snapshot not found: {snapshotId}");

        if (metadata.CurrentSnapshotId is null)
            throw new InvalidOperationException("Cannot cherry-pick into a table with no current snapshot");

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var newSnapshotId = SnapshotIdFactory.Generate();

        var cherryPicked = new Snapshot(
            SnapshotId: newSnapshotId,
            ParentSnapshotId: metadata.CurrentSnapshotId,
            SequenceNumber: metadata.LastSequenceNumber + 1,
            TimestampMs: now,
            ManifestList: snapshot.ManifestList,
            Summary: CherryPickSummary(snapshot.Summary, snapshotId),
            SchemaId: snapshot.SchemaId);

        var snapshots = metadata.Snapshots.ToList();
        snapshots.Add(cherryPicked);

        var snapshotLog = metadata.SnapshotLog.ToList();
        snapshotLog.Add(new SnapshotLogEntry(now, newSnapshotId));

        return metadata with
        {
            LastSequenceNumber = metadata.LastSequenceNumber + 1,
            LastUpdatedMs = now,
            CurrentSnapshotId = newSnapshotId,
            Snapshots = snapshots,
            SnapshotLog = snapshotLog,
        };
    }

    private static Dictionary<string, string> CherryPickSummary(
        IReadOnlyDictionary<string, string> source, long snapshotId)
    {
        var dict = source.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        dict["operation"] = "replace";
        dict["cherry-pick-snapshot-id"] = snapshotId.ToString();
        return dict;
    }
}
