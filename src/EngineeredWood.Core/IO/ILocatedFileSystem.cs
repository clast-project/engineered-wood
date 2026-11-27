// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Implemented by an <see cref="ITableFileSystem"/> that knows the absolute location it is rooted at, so that a table
/// format which names some of its files by absolute location (Delta's <c>add.path</c>, a shallow clone's files) can
/// tell a file under its own root, which it reaches through this file system, from one elsewhere, which it reaches
/// through a <see cref="StorageResolver"/> or not at all.
/// </summary>
/// <remarks>
/// A wrapper around another file system should implement it and forward its inner one's location only when the
/// wrapper serves the same paths; otherwise a reader treats the table's location as unknown, and an operation that
/// needs it to stay safe (Delta's VACUUM of a table holding absolute paths) refuses.
/// </remarks>
public interface ILocatedFileSystem
{
    /// <summary>
    /// The location this file system is rooted at: the path <c>""</c> names it, and every path it is handed is
    /// relative to it. Throws <see cref="NotSupportedException"/> when the root cannot be named as a
    /// <see cref="StorageLocation"/> (an object-store root with a <c>.</c> or <c>..</c> segment).
    /// </summary>
    StorageLocation Location { get; }
}
