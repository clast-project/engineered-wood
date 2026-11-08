// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>The one way to list the Delta log's own files (#755 review).</summary>
/// <remarks>
/// <para>A listing of <see cref="DeltaVersion.LogPrefix"/> is recursive on some backends (the local file system, S3),
/// so it also returns files in directories inside the log: <c>_sidecars/</c>, or a backup copy someone made. Those are
/// not commits, checkpoints or checksums whatever their names, and every scan that classifies log files by name must
/// skip them. Counting <c>_delta_log/backup/&lt;v&gt;.json</c> as commit v invents a version for a reader, a restore or
/// a time-travel search, and makes log cleanup delete the copy as an expired commit.</para>
///
/// <para>Sidecars are not lost by this: they are found by their own prefix, <see cref="DeltaVersion.SidecarPrefix"/>.
/// A path that does not start with the log prefix at all is passed through rather than guessed at.</para>
/// </remarks>
internal static class LogDirectory
{
    /// <summary>The files directly in <c>_delta_log/</c>, as <paramref name="fileSystem"/> lists them.</summary>
    public static async IAsyncEnumerable<TableFileInfo> ListAsync(
        ITableFileSystem fileSystem,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var file in fileSystem.ListAsync(DeltaVersion.LogPrefix, cancellationToken)
            .ConfigureAwait(false))
        {
            if (!IsNested(file.Path))
                yield return file;
        }
    }

    /// <summary>Whether <paramref name="path"/> is in a directory under the log rather than in the log itself.</summary>
    internal static bool IsNested(string path) =>
        path.StartsWith(DeltaVersion.LogPrefix, StringComparison.Ordinal)
        && path.IndexOf('/', DeltaVersion.LogPrefix.Length) >= 0;
}
