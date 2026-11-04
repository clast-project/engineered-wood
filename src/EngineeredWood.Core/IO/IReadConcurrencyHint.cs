// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO;

/// <summary>
/// Implemented by an <see cref="IRandomAccessFile"/> that knows how many reads it serves well at once, so that
/// a reader which can split its work (the row groups of a Parquet file, #612) can size its default window to
/// the storage rather than to the CPU; and by an <see cref="ITableFileSystem"/> whose files do, so that a table
/// scan, which picks its window before it opens any file, can do the same (#632).
/// </summary>
/// <remarks>
/// <para>A local file does not implement it: its reads are cheap, the work is decoding, and the readers'
/// CPU-based default is the right one. An object-store file does: there the time goes to waiting on requests,
/// and more of them in flight keeps paying off well past the core count's sweet spot for decoding.</para>
/// <para>It is a hint, not a limit. A reader's explicit setting (for example
/// <c>ParquetReadOptions.MaxConcurrentRowGroups</c>) overrides it, and the memory a read may hold is bounded
/// separately. A wrapper around another file or filesystem should implement it and forward its inner one's hint
/// (null when the inner one gives none), or the reader falls back to its default.</para>
/// </remarks>
public interface IReadConcurrencyHint
{
    /// <summary>
    /// How many independent reads this file (or a file of this filesystem) serves efficiently at once — for an
    /// object store, the requests it lets be in flight. At least 1, or null for no preference.
    /// </summary>
    int? PreferredConcurrentReads { get; }
}
