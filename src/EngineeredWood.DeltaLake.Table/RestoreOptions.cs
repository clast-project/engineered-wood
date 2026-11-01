// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Options for <see cref="DeltaTable.RestoreAsync(long, RestoreOptions?, System.Threading.CancellationToken, CommitProperties?)"/>.
/// </summary>
public sealed record RestoreOptions
{
    /// <summary>The defaults: a restore whose files are missing is refused.</summary>
    public static RestoreOptions Default { get; } = new();

    /// <summary>
    /// Whether to restore without checking that the files the restore re-adds still exist. Off by default, and
    /// the restore is then refused, naming the missing files, when VACUUM has deleted any of them.
    ///
    /// <para>On, the check is skipped and nothing else changes: a missing file is still re-added, so the table
    /// then names a file that is not there, and reading it fails. This is what Spark's
    /// <c>spark.sql.files.ignoreMissingFiles</c> and delta-rs's <c>ignore_missing_files</c> do to a RESTORE.</para>
    /// </summary>
    public bool IgnoreMissingFiles { get; init; }
}
