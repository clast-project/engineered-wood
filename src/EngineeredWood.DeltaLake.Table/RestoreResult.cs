// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// What a RESTORE did: the same six figures Spark's <c>RESTORE TABLE</c> returns and records as the commit's
/// <c>operationMetrics</c>.
/// </summary>
public sealed record RestoreResult
{
    /// <summary>The version the restore committed, or the table's current version when it committed nothing.</summary>
    public required long Version { get; init; }

    /// <summary>
    /// False when the table was already at the requested version, which commits nothing, as Spark does.
    /// Restoring to an earlier version whose files match the current ones still commits.
    /// </summary>
    public required bool Committed { get; init; }

    /// <summary>The version whose state was restored.</summary>
    public required long RestoredVersion { get; init; }

    /// <summary>Files re-added: active in the restored version but not before the restore. A file whose
    /// deletion vector differs counts once here and once in <see cref="NumRemovedFiles"/>.</summary>
    public required long NumRestoredFiles { get; init; }

    /// <summary>Files removed: active before the restore but not in the restored version.</summary>
    public required long NumRemovedFiles { get; init; }

    /// <summary>The total size in bytes of the re-added files.</summary>
    public required long RestoredFilesSize { get; init; }

    /// <summary>The total size in bytes of the removed files.</summary>
    public required long RemovedFilesSize { get; init; }

    /// <summary>The number of files active after the restore.</summary>
    public required long NumOfFilesAfterRestore { get; init; }

    /// <summary>The total size in bytes of the files active after the restore.</summary>
    public required long TableSizeAfterRestore { get; init; }
}
