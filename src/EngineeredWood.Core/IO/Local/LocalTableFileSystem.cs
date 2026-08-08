// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for local filesystems.
/// Uses <see cref="LocalRandomAccessFile"/> and <see cref="LocalSequentialFile"/>
/// for file handle operations.
/// </summary>
public sealed class LocalTableFileSystem : ITableFileSystem
{
    private readonly string _rootPath;
    private readonly BufferAllocator? _allocator;

    /// <summary>
    /// Creates a new local filesystem rooted at the specified directory.
    /// All paths are resolved relative to this root.
    /// </summary>
    public LocalTableFileSystem(string rootPath, BufferAllocator? allocator = null)
    {
        _rootPath = Path.GetFullPath(rootPath);
        _allocator = allocator;
    }

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
            directory = Path.GetDirectoryName(fullPrefix) ?? _rootPath;
            searchPattern = Path.GetFileName(fullPrefix) + "*";
        }

        if (!Directory.Exists(directory))
            yield break;

        var entries = Directory.EnumerateFiles(directory, searchPattern, SearchOption.AllDirectories)
            .OrderBy(p => p, StringComparer.Ordinal);

        foreach (string fullPath in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var info = new FileInfo(fullPath);
            string relativePath = GetRelativePath(_rootPath, fullPath)
                .Replace('\\', '/');

            yield return new TableFileInfo(
                relativePath,
                info.Length,
                new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero));
        }
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

        return new ValueTask<ISequentialFile>(new LocalSequentialFile(fullPath));
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
    public ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<byte[]>(File.ReadAllBytes(ResolvePath(path)));
    }

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

    private string ResolvePath(string relativePath) =>
        Path.GetFullPath(Path.Combine(_rootPath, relativePath.Replace('/', Path.DirectorySeparatorChar)));

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
