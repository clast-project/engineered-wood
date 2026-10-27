// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.IO.Local;

/// <summary>
/// <see cref="ISequentialFile"/> implementation for local files using <see cref="FileStream"/>.
/// </summary>
/// <remarks>
/// <para>Writes go to a hidden temp file beside the target, <c>.&lt;name&gt;.&lt;guid&gt;.tmp</c>, which
/// <see cref="CompleteAsync"/> moves into place and an abort deletes. The leading dot keeps it out of
/// what table formats treat as their own: Delta's log listing and VACUUM both skip hidden names, and the
/// stem no longer parses as a commit or checkpoint version.</para>
///
/// <para>Without overwrite the move is the create-if-absent step: it fails when the target exists, so a
/// file another writer published after this one was created is never replaced. With overwrite, an
/// existing target is replaced in one step. On Windows that fails while another process has the target
/// open without delete sharing.</para>
/// </remarks>
public sealed class LocalSequentialFile : ISequentialFile
{
    private readonly string _path;
    private readonly string _tempPath;
    private readonly bool _overwrite;
    private readonly FileStream _stream;
    private long _position;
    private bool _completed;
    private bool _aborted;
    private bool _disposed;

    /// <summary>
    /// Creates a new file for writing, published at <paramref name="path"/> by <see cref="CompleteAsync"/>.
    /// </summary>
    /// <param name="path">The file to create.</param>
    /// <param name="overwrite">
    /// Whether completion may replace an existing file. When false, <see cref="CompleteAsync"/> throws
    /// <see cref="IOException"/> if <paramref name="path"/> exists by then.
    /// </param>
    public LocalSequentialFile(string path, bool overwrite = true)
    {
        _path = Path.GetFullPath(path);
        _overwrite = overwrite;
        string directory = Path.GetDirectoryName(_path) ?? ".";
        // The target's name is kept only as a hint for whoever finds a temp file a crash left behind, and is
        // cut short so the temp name stays within a 255-byte component limit however long the target's is.
        string name = Path.GetFileName(_path);
        if (name.Length > MaxNameHintLength)
            name = name.Substring(0, MaxNameHintLength);
        _tempPath = Path.Combine(directory, "." + name + "." + Guid.NewGuid().ToString("N") + TempSuffix);
        _stream = new FileStream(_tempPath, FileMode.CreateNew, FileAccess.Write,
            FileShare.None, bufferSize: 81920, useAsync: false);
    }

    private const string TempSuffix = ".tmp";

    /// <summary>
    /// How much of the target's name the temp name carries, in UTF-16 units. A unit is at most 3 bytes of
    /// UTF-8, so the hint is at most 192 bytes, and with the ASCII dots, guid and suffix (38 bytes) the temp
    /// name stays under the common 255-byte component limit.
    /// </summary>
    private const int MaxNameHintLength = 64;

    /// <summary>
    /// Whether <paramref name="fileName"/> is the temp file of a write still in flight (or abandoned by a
    /// crash): <c>.&lt;name&gt;.&lt;32 hex digits&gt;.tmp</c>.
    /// </summary>
    internal static bool IsTempFileName(string fileName)
    {
        const int GuidLength = 32;
        // ".", at least one character of name, ".", the guid, ".tmp".
        if (fileName.Length < 1 + 1 + 1 + GuidLength + TempSuffix.Length ||
            fileName[0] != '.' ||
            !fileName.EndsWith(TempSuffix, StringComparison.Ordinal))
            return false;

        int guidStart = fileName.Length - TempSuffix.Length - GuidLength;
        if (fileName[guidStart - 1] != '.')
            return false;
        for (int i = guidStart; i < guidStart + GuidLength; i++)
        {
            char c = fileName[i];
            if (!(c is >= '0' and <= '9' || c is >= 'a' and <= 'f'))
                return false;
        }
        return true;
    }

    /// <inheritdoc/>
    public long Position => _position;

    /// <inheritdoc/>
    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ThrowIfNotWritable();
        cancellationToken.ThrowIfCancellationRequested();

#if NET6_0_OR_GREATER
        _stream.Write(data.Span);
#else
        byte[] array = data.ToArray();
        _stream.Write(array, 0, array.Length);
#endif
        _position += data.Length;
        return default;
    }

    /// <inheritdoc/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfNotWritable();
        cancellationToken.ThrowIfCancellationRequested();

        _stream.Flush();
        return default;
    }

    /// <inheritdoc/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_completed)
            return default;
        ThrowIfAborted();

        try
        {
            // Inside the try: a completion that is cancelled fails like any other, and aborts the write.
            cancellationToken.ThrowIfCancellationRequested();

            // The stream must be closed before the move: Windows cannot rename a file that is open.
            _stream.Dispose();
            Publish();
        }
        catch
        {
            _aborted = true;
            _stream.Dispose();
            DeleteTemp();
            throw;
        }
        _completed = true;
        return default;
    }

    private void Publish()
    {
        if (!_overwrite)
        {
            try
            {
                File.Move(_tempPath, _path);
            }
            catch (IOException) when (File.Exists(_path))
            {
                throw new IOException($"File already exists: {_path}");
            }
            return;
        }

#if NET6_0_OR_GREATER
        File.Move(_tempPath, _path, overwrite: true);
#else
        // .NET Framework has no overwriting move. File.Replace swaps an existing target in one step but
        // requires it to exist, so a target that is absent (or deleted in between) takes the plain move.
        // Either can lose a race with another writer that creates or deletes the target in between, so the
        // pair is retried a few times; overwrite means the last writer wins, not that this one gives up.
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                if (File.Exists(_path))
                    File.Replace(_tempPath, _path, destinationBackupFileName: null);
                else
                    File.Move(_tempPath, _path);
                return;
            }
            catch (IOException) when (attempt < MaxOverwriteAttempts && File.Exists(_tempPath))
            {
            }
        }
#endif
    }

#if !NET6_0_OR_GREATER
    private const int MaxOverwriteAttempts = 3;
#endif

    /// <summary>Aborts the write unless it was completed: the temp file is deleted and nothing is published.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (_completed || _aborted) return;

        _stream.Dispose();
        DeleteTemp();
    }

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    private void DeleteTemp()
    {
        try
        {
            File.Delete(_tempPath);
        }
        catch (IOException)
        {
            // Best-effort: the write has already failed or been abandoned, and a leftover hidden temp file
            // is invisible to the table formats.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private void ThrowIfDisposed()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
    }

    private void ThrowIfAborted()
    {
        if (_aborted)
            throw new InvalidOperationException("Cannot use a file whose completion failed; the write was aborted.");
    }

    private void ThrowIfNotWritable()
    {
        ThrowIfDisposed();
        ThrowIfAborted();
        if (_completed)
            throw new InvalidOperationException("Cannot write to a file that has already been completed.");
    }
}
