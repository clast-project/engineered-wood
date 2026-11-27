// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for local filesystems.
/// Uses <see cref="LocalRandomAccessFile"/> and <see cref="LocalSequentialFile"/>
/// for file handle operations.
/// </summary>
public sealed class LocalTableFileSystem : ITableFileSystem, ILocatedFileSystem
{
    private readonly string _rootPath;
    private readonly BufferAllocator? _allocator;

    /// <summary>
    /// Creates a new local filesystem rooted at the specified directory.
    /// All paths are resolved relative to this root.
    /// </summary>
    public LocalTableFileSystem(string rootPath, BufferAllocator? allocator = null)
    {
        string full = Path.GetFullPath(rootPath);
        // Without a trailing separator ("C:\t\" -> "C:\t"), since that is how GetFullPath spells the root when a path
        // collapses back to it ("sub/.."); a volume root ("C:\", "/") keeps its separator.
        string trimmed = full.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        _rootPath = trimmed.Length == 0 || Path.GetPathRoot(full) == full ? full : trimmed;
        _allocator = allocator;
    }

    /// <summary>The directory this file system is rooted at, as a storage location.</summary>
    public StorageLocation Location => StorageLocation.Parse(_rootPath);

    /// <summary>
    /// <para>The naming rules of the VOLUME this instance is rooted on, which is the only place in the
    /// codebase where the host operating system is the right thing to ask about.</para>
    ///
    /// <para>Windows is reported as the full <see cref="PathNameConstraints.Win32"/> set. On other
    /// platforms this reports <see cref="PathNameConstraints.None"/>, which is a KNOWN
    /// UNDER-REPORT — reasoned, not measured: a POSIX process can have an NTFS, exFAT or SMB volume
    /// mounted, the constraints belong to that volume rather than to the kernel reaching it, and .NET
    /// exposes no portable way to ask which filesystem backs a path. A caller that needs a name safe on
    /// a volume it cannot interrogate should ask for a portable spelling explicitly rather than trust
    /// this property to know.</para>
    /// </summary>
    public PathNameConstraints PathConstraints =>
#if NET5_0_OR_GREATER
        OperatingSystem.IsWindows()
#else
        System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(
            System.Runtime.InteropServices.OSPlatform.Windows)
#endif
            ? PathNameConstraints.Win32
            : PathNameConstraints.None;

    /// <inheritdoc/>
    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string directory;
        string searchPattern;

        if (string.IsNullOrEmpty(prefix))
        {
            directory = _rootPath;
            searchPattern = "*";
        }
        else
        {
            string fullPrefix = ResolvePath(prefix);
            if (fullPrefix.Length == _rootPath.Length || EndsWithDotSegment(prefix))
            {
                // "." or "a/..", or "wh/." from a subtree over this one: a directory, which GetFullPath spells
                // without a trailing separator. Taken as a file-name prefix it would list the directory's siblings
                // whose names start with its name, and for the root itself those are outside the root (#962).
                directory = fullPrefix;
                searchPattern = "*";
            }
            else
            {
                directory = Path.GetDirectoryName(fullPrefix) ?? _rootPath;
                searchPattern = Path.GetFileName(fullPrefix) + "*";
            }
        }

        if (!Directory.Exists(directory))
            yield break;

        // DirectoryInfo's enumeration fills each FileInfo from the directory entry it read, so the length and
        // time below cost no further call per file (a FileInfo built from a path stats the file again).
        var entries = new DirectoryInfo(directory)
            .EnumerateFiles(searchPattern, SearchOption.AllDirectories)
            .OrderBy(f => f.FullName, StringComparer.Ordinal);

        foreach (var info in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // A streamed write in flight is not a file yet, as on an object store.
            if (LocalSequentialFile.IsTempFileName(info.Name))
                continue;

            string fullPath = info.FullName;
            string relativePath = GetRelativePath(_rootPath, fullPath)
                .Replace('\\', '/');

            yield return new TableFileInfo(
                relativePath,
                info.Length,
                new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero));
        }
    }

    /// <summary>Whether the last segment of <paramref name="prefix"/> is <c>.</c> or <c>..</c>, so that it names a
    /// directory however <see cref="Path.GetFullPath(string)"/> spells it.</summary>
    private static bool EndsWithDotSegment(string prefix)
    {
        // A trailing separator ("sub/./") does not end the dot segment before it.
        int end = prefix.Length;
        while (end > 0 && (prefix[end - 1] == '/' || prefix[end - 1] == Path.DirectorySeparatorChar))
            end--;
        int start = end;
        while (start > 0 && prefix[start - 1] != '/' && prefix[start - 1] != Path.DirectorySeparatorChar)
            start--;
        return end - start is 1 or 2 && prefix.Substring(start, end - start).Trim('.').Length == 0;
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);
        return new ValueTask<IRandomAccessFile>(
            new LocalRandomAccessFile(fullPath, _allocator));
    }

    /// <inheritdoc/>
    public ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);

        if (!overwrite && File.Exists(fullPath))
            throw new IOException($"File already exists: {path}");

        string? directory = Path.GetDirectoryName(fullPath);
        if (directory is not null)
            Directory.CreateDirectory(directory);

        return new ValueTask<ISequentialFile>(new LocalSequentialFile(fullPath, overwrite));
    }

    /// <inheritdoc/>
    public ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);

        if (File.Exists(fullPath))
            File.Delete(fullPath);

        return default;
    }

    /// <inheritdoc/>
    public ValueTask<bool> ExistsAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<bool>(File.Exists(ResolvePath(path)));
    }

    /// <inheritdoc/>
    /// <remarks>
    /// On Windows, a file that was just published (renamed into place, as <see cref="TryWriteAllBytesAsync"/>
    /// does) can briefly refuse to open with a sharing or lock violation, even though nothing in the process
    /// holds it: MEASURED (#960), about 1 read in 300 when eight writers race to commit table versions while
    /// reading the previous one. It persists with every in-process writer serialized, so the holder is
    /// outside the process (the OS or a scanner). Those errors are retried for up to about half a second
    /// before they surface. Any other failure is thrown at once.
    /// </remarks>
    public async ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);
        for (int delayMs = 1; ; delayMs *= 2)
        {
            try
            {
                return File.ReadAllBytes(fullPath);
            }
            catch (IOException e) when (IsTransientSharingViolation(e) && delayMs <= 256)
            {
                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ERROR_SHARING_VIOLATION (32) and ERROR_LOCK_VIOLATION (33), as HRESULTs.
    private static bool IsTransientSharingViolation(IOException e) =>
        e.HResult is unchecked((int)0x80070020) or unchecked((int)0x80070021);

    /// <inheritdoc/>
    public ValueTask<bool> TryWriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (directory is not null)
        {
            Directory.CreateDirectory(directory);
        }

        string tempPath = fullPath + ".tmp." + Guid.NewGuid().ToString("N");
        try
        {
            File.WriteAllBytes(tempPath, data.ToArray());
            try
            {
                File.Move(tempPath, fullPath);
                return new ValueTask<bool>(true);
            }
            catch (IOException) when (File.Exists(fullPath))
            {
                return new ValueTask<bool>(false);
            }
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }

    /// <inheritdoc/>
    public ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);

        string? directory = Path.GetDirectoryName(fullPath);
        if (directory is not null)
            Directory.CreateDirectory(directory);

#if NET6_0_OR_GREATER
        File.WriteAllBytes(fullPath, data.Span.ToArray());
#else
        File.WriteAllBytes(fullPath, data.ToArray());
#endif
        return default;
    }

    /// <summary>
    /// The full path <paramref name="relativePath"/> names under the root, or an <see cref="ArgumentException"/>
    /// when it names anything else (#962). Dot segments collapse, but may not climb above the root even to come
    /// back down; a rooted path (either separator on Windows, a drive letter, UNC, <c>\\?\</c>) is refused; and the
    /// full path is checked to be under the root at the end, which also refuses what Windows maps elsewhere (the
    /// device name <c>NUL</c> resolves to <c>\\.\NUL</c>, MEASURED on Windows 11).
    /// </summary>
    private string ResolvePath(string relativePath)
    {
        bool windows = Path.DirectorySeparatorChar == '\\';
        RootRelativePath.ThrowIfEscapesRoot(relativePath, backslashSeparates: windows);

        string native = relativePath.Replace('/', Path.DirectorySeparatorChar);
        if (Path.IsPathRooted(native))
            throw RootRelativePath.Rooted(relativePath); // "C:x": drive-relative, resolved against that drive's cwd

        string fullPath = Path.GetFullPath(Path.Combine(_rootPath, native));
        if (!IsUnderRoot(fullPath))
            throw RootRelativePath.Outside(relativePath);
        return fullPath;
    }

    private bool IsUnderRoot(string fullPath)
    {
        // GetFullPath of the root joined with a relative path starts with the root's own spelling, so an ordinal
        // comparison is exact here, case-insensitive volume or not.
        if (!fullPath.StartsWith(_rootPath, StringComparison.Ordinal))
            return false;
        if (fullPath.Length == _rootPath.Length)
            return true;
        char last = _rootPath[_rootPath.Length - 1];
        return last == Path.DirectorySeparatorChar || last == Path.AltDirectorySeparatorChar
            || fullPath[_rootPath.Length] == Path.DirectorySeparatorChar;
    }

    private static string GetRelativePath(string relativeTo, string path)
    {
#if NET6_0_OR_GREATER
        return Path.GetRelativePath(relativeTo, path);
#else
        // Simple polyfill for netstandard2.0 / net472
        var rootUri = new Uri(relativeTo.EndsWith(Path.DirectorySeparatorChar.ToString())
            ? relativeTo
            : relativeTo + Path.DirectorySeparatorChar);
        var pathUri = new Uri(path);
        return Uri.UnescapeDataString(rootUri.MakeRelativeUri(pathUri).ToString())
            .Replace('/', Path.DirectorySeparatorChar);
#endif
    }
}
