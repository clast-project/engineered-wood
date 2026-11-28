// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table;

/// <summary>
/// How a <see cref="LanceDatasetWriter"/> operation that reads the dataset's existing files (delete, update, compact)
/// reaches them.
/// </summary>
public sealed class LanceWriteOptions
{
    /// <summary>
    /// <para>Reaches the existing files a manifest's <c>base_paths</c> name outside the dataset: a shallow clone's
    /// source dataset, or the directories an import or a multi-base write put files in. Register each location such
    /// files may lie under. Default: none, so an operation that must read such a file refuses (with
    /// <see cref="EngineeredWood.IO.UnregisteredStorageLocationException"/>) before it writes anything, rather than
    /// reach storage it was not given.</para>
    /// <para>The resolver is only read through. Every file the writer creates goes under the dataset root, and the
    /// files under other bases are left as they are, as lance does when a writer names no target base.</para>
    /// </summary>
    public EngineeredWood.IO.StorageResolver? StorageResolver { get; init; }
}
