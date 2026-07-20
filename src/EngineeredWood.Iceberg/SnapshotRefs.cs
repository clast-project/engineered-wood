// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Operations for managing snapshot references (branches and tags) on a table.
/// </summary>
public static class SnapshotRefs
{
    /// <summary>The well-known name of the default branch.</summary>
    public const string MainBranch = "main";

    /// <summary>
    /// Create or update a branch pointing to a snapshot.
    /// </summary>
    public static TableMetadata SetBranch(
        TableMetadata metadata,
        string name,
        long snapshotId,
        int? minSnapshotsToKeep = null,
        long? maxSnapshotAgeMs = null)
    {
        ValidateSnapshotExists(metadata, snapshotId);

        var refs = metadata.Refs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        refs[name] = SnapshotRef.Branch(snapshotId, minSnapshotsToKeep, maxSnapshotAgeMs);

        var result = metadata with
        {
            Refs = refs,
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        // Setting the main branch also updates current-snapshot-id
        if (name == MainBranch)
            result = result with { CurrentSnapshotId = snapshotId };

        return result;
    }

    /// <summary>
    /// Create a tag pointing to a snapshot. Tags are immutable — setting a tag
    /// that already exists throws.
    /// </summary>
    public static TableMetadata SetTag(
        TableMetadata metadata,
        string name,
        long snapshotId,
        long? maxRefAgeMs = null)
    {
        ValidateSnapshotExists(metadata, snapshotId);

        if (metadata.Refs.ContainsKey(name))
            throw new ArgumentException($"Ref already exists: {name}. Tags are immutable — drop it first to reassign.");

        var refs = metadata.Refs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        refs[name] = SnapshotRef.Tag(snapshotId, maxRefAgeMs);

        return metadata with
        {
            Refs = refs,
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };
    }

    /// <summary>
    /// Remove a snapshot reference. Cannot remove the main branch.
    /// </summary>
    public static TableMetadata RemoveRef(TableMetadata metadata, string name)
    {
        if (name == MainBranch)
            throw new ArgumentException("Cannot remove the main branch");

        if (!metadata.Refs.ContainsKey(name))
            throw new ArgumentException($"Ref not found: {name}");

        var refs = metadata.Refs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        refs.Remove(name);

        return metadata with
        {
            Refs = refs,
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };
    }

    /// <summary>
    /// Fast-forward a branch to a new snapshot.
    /// </summary>
    public static TableMetadata FastForwardBranch(
        TableMetadata metadata, string name, long newSnapshotId)
    {
        if (!metadata.Refs.TryGetValue(name, out var existing))
            throw new ArgumentException($"Branch not found: {name}");

        if (existing.Type != SnapshotRefType.Branch)
            throw new ArgumentException($"Ref '{name}' is a tag, not a branch");

        ValidateSnapshotExists(metadata, newSnapshotId);

        return SetBranch(metadata, name, newSnapshotId,
            existing.MinSnapshotsToKeep, existing.MaxSnapshotAgeMs);
    }

    /// <summary>
    /// Replace the main branch with a different snapshot (e.g. for WAP — write-audit-publish).
    /// </summary>
    public static TableMetadata PublishBranch(TableMetadata metadata, string sourceBranch)
    {
        if (!metadata.Refs.TryGetValue(sourceBranch, out var source))
            throw new ArgumentException($"Branch not found: {sourceBranch}");

        if (source.Type != SnapshotRefType.Branch)
            throw new ArgumentException($"Ref '{sourceBranch}' is a tag, not a branch");

        return SetBranch(metadata, MainBranch, source.SnapshotId);
    }

    /// <summary>
    /// Resolve a ref name to its snapshot ID.
    /// </summary>
    public static long? ResolveRef(TableMetadata metadata, string name)
    {
        return metadata.Refs.TryGetValue(name, out var r) ? r.SnapshotId : null;
    }

    /// <summary>
    /// Ensure the "main" branch ref is consistent with current-snapshot-id.
    /// Call after operations that update current-snapshot-id directly
    /// (e.g. AppendFiles, Rollback).
    /// </summary>
    public static TableMetadata SyncMainBranch(TableMetadata metadata)
    {
        if (metadata.CurrentSnapshotId is null)
            return metadata;

        var refs = metadata.Refs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

        if (refs.TryGetValue(MainBranch, out var existing) &&
            existing.SnapshotId == metadata.CurrentSnapshotId.Value)
            return metadata;

        refs[MainBranch] = existing is not null
            ? existing with { SnapshotId = metadata.CurrentSnapshotId.Value }
            : SnapshotRef.Branch(metadata.CurrentSnapshotId.Value);

        return metadata with { Refs = refs };
    }

    private static void ValidateSnapshotExists(TableMetadata metadata, long snapshotId)
    {
        if (!metadata.Snapshots.Any(s => s.SnapshotId == snapshotId))
            throw new ArgumentException($"Snapshot not found: {snapshotId}");
    }
}
