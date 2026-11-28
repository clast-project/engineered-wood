// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table;

/// <summary>How <see cref="LanceTable.OpenAsync(EngineeredWood.IO.ITableFileSystem, LanceTableOptions, CancellationToken)"/>
/// opens a dataset.</summary>
public sealed class LanceTableOptions
{
    /// <summary>The version to open (time travel); null for the latest. Cannot be combined with <see cref="AsOf"/>.</summary>
    public ulong? Version { get; init; }

    /// <summary>Opens the latest version committed at or before this instant; null for the latest. Cannot be combined
    /// with <see cref="Version"/>.</summary>
    public DateTimeOffset? AsOf { get; init; }

    /// <summary>
    /// <para>Reaches the files a manifest's <c>base_paths</c> name outside the dataset: a shallow clone's source
    /// dataset, or the directories an import or a multi-base write put files in. Register each location such files
    /// may lie under. Default: none, so a dataset naming such a file refuses to read it (with
    /// <see cref="EngineeredWood.IO.UnregisteredStorageLocationException"/>) rather than reach storage it was not
    /// given.</para>
    /// <para>A base under the dataset's own location needs no registration when the dataset's file system says where
    /// it is rooted (<see cref="EngineeredWood.IO.ILocatedFileSystem"/>, which every built-in backend implements). A
    /// base given as a relative path, or with a <c>.</c> or <c>..</c> segment outside a local path, is refused.</para>
    /// </summary>
    public EngineeredWood.IO.StorageResolver? StorageResolver { get; init; }
}
